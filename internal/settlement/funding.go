package settlement

import (
	"context"
	"log/slog"
	"time"

	"github.com/dex/matching-engine/internal/config"
	"github.com/dex/matching-engine/internal/events"
	"github.com/dex/matching-engine/internal/marketdata"
	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

// fundingRateCap bounds the funding rate per interval to avoid runaway payments.
var fundingRateCap = decimal.NewFromFloat(0.0075) // 0.75%

// FundingScheduler periodically applies funding payments between longs and
// shorts on every registered futures market, based on the premium of mark
// price over index (spot) price.
type FundingScheduler struct {
	futures    *FuturesSettlement
	marketdata *marketdata.Service
	symbols    *config.Registry
	bus        *events.Bus
	log        *slog.Logger
}

// NewFundingScheduler creates a FundingScheduler.
func NewFundingScheduler(futures *FuturesSettlement, md *marketdata.Service, symbols *config.Registry, bus *events.Bus) *FundingScheduler {
	return &FundingScheduler{
		futures:    futures,
		marketdata: md,
		symbols:    symbols,
		bus:        bus,
		log:        slog.Default(),
	}
}

// Run starts the funding loop; call in a goroutine. Stops when ctx is cancelled.
// interval is the wall-clock tick rate for checking whether a symbol's funding
// interval has elapsed (typically much shorter than the funding interval itself).
func (f *FundingScheduler) Run(ctx context.Context, checkInterval time.Duration) {
	lastRun := make(map[string]time.Time)
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			f.tick(lastRun)
		case <-ctx.Done():
			return
		}
	}
}

func (f *FundingScheduler) tick(lastRun map[string]time.Time) {
	for _, cfg := range f.symbols.All() {
		if cfg.Market != models.Futures || cfg.FundingIntervalHours <= 0 {
			continue
		}
		due := lastRun[cfg.Symbol].Add(time.Duration(cfg.FundingIntervalHours) * time.Hour)
		if time.Now().Before(due) {
			continue
		}
		f.settleFunding(cfg)
		lastRun[cfg.Symbol] = time.Now()
	}
}

func (f *FundingScheduler) settleFunding(cfg *config.SymbolConfig) {
	ticker, err := f.marketdata.Ticker(cfg.Symbol, models.Futures)
	if err != nil || ticker.MidPrice.IsZero() {
		return
	}
	indexTicker, err := f.marketdata.Ticker(cfg.UnderlyingSymbol, models.Spot)
	indexPrice := ticker.MidPrice
	if err == nil && !indexTicker.MidPrice.IsZero() {
		indexPrice = indexTicker.MidPrice
	}

	rate := ticker.MidPrice.Sub(indexPrice).Div(indexPrice)
	if rate.GreaterThan(fundingRateCap) {
		rate = fundingRateCap
	} else if rate.LessThan(fundingRateCap.Neg()) {
		rate = fundingRateCap.Neg()
	}
	if rate.IsZero() {
		return
	}

	for _, pos := range f.futures.AllPositions() {
		if pos.Symbol != cfg.Symbol || pos.Size.IsZero() {
			continue
		}
		notional := ticker.MidPrice.Mul(pos.Size.Abs())
		payment := notional.Mul(rate)
		// Longs pay shorts when rate is positive (mark > index); shorts pay longs when negative.
		if pos.Side == models.Buy {
			payment = payment.Neg()
		}
		if err := f.futures.ApplyFunding(pos.AccountID, pos.Symbol, payment, cfg.QuoteCurrency); err != nil {
			f.log.Error("funding settlement failed", "account", pos.AccountID, "symbol", pos.Symbol, "error", err)
			continue
		}
		if f.bus != nil {
			f.bus.Publish(&models.Event{
				Type:   models.EventFunding,
				Symbol: pos.Symbol,
				Market: string(models.Futures),
				Funding: &models.Funding{
					AccountID: pos.AccountID,
					Symbol:    pos.Symbol,
					Rate:      rate,
					Payment:   payment,
				},
			})
		}
	}
}
