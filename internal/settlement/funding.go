package settlement

import (
	"context"
	"log/slog"
	"time"

	"github.com/dex/matching-engine/internal/config"
	"github.com/dex/matching-engine/internal/events"
	"github.com/dex/matching-engine/internal/marketdata"
	"github.com/dex/matching-engine/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
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
	pool       *pgxpool.Pool // optional: used to persist/load last-run times
	log        *slog.Logger
}

// NewFundingScheduler creates a FundingScheduler. pool may be nil when
// Postgres is disabled (funding run-times then start fresh on every restart).
func NewFundingScheduler(futures *FuturesSettlement, md *marketdata.Service, symbols *config.Registry, bus *events.Bus, pool *pgxpool.Pool) *FundingScheduler {
	return &FundingScheduler{
		futures:    futures,
		marketdata: md,
		symbols:    symbols,
		bus:        bus,
		pool:       pool,
		log:        slog.Default(),
	}
}

// Run starts the funding loop; call in a goroutine. Stops when ctx is cancelled.
// interval is the wall-clock tick rate for checking whether a symbol's funding
// interval has elapsed (typically much shorter than the funding interval itself).
func (f *FundingScheduler) Run(ctx context.Context, checkInterval time.Duration) {
	lastRun := f.loadLastRun(ctx)
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

// loadLastRun retrieves the most recent funding settlement timestamp per
// symbol from Postgres so that a restart does not reset the funding clock
// (which would either skip or double-apply an interval boundary). Returns an
// empty map (all symbols "due") when Postgres is unavailable.
func (f *FundingScheduler) loadLastRun(ctx context.Context) map[string]time.Time {
	out := make(map[string]time.Time)
	if f.pool == nil {
		return out
	}
	rows, err := f.pool.Query(ctx, `
		SELECT symbol, MAX(created_at) AS last_run
		FROM funding_payments
		GROUP BY symbol`)
	if err != nil {
		f.log.Error("load funding last-run from DB; starting fresh", "error", err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var symbol string
		var lastRun time.Time
		if err := rows.Scan(&symbol, &lastRun); err == nil && !lastRun.IsZero() {
			out[symbol] = lastRun
		}
	}
	return out
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
	if err != nil || ticker.MarkPrice.IsZero() {
		return
	}
	indexTicker, err := f.marketdata.Ticker(cfg.UnderlyingSymbol, models.Spot)
	indexPrice := ticker.MarkPrice
	if err == nil && !indexTicker.MarkPrice.IsZero() {
		indexPrice = indexTicker.MarkPrice
	}

	rate := ticker.MarkPrice.Sub(indexPrice).Div(indexPrice)
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
		notional := ticker.MarkPrice.Mul(pos.Size.Abs())
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
