// Package liquidation periodically sweeps open futures positions and force-
// closes any that have breached their maintenance margin requirement.
package liquidation

import (
	"context"
	"log/slog"
	"time"

	"github.com/dex/matching-engine/internal/config"
	"github.com/dex/matching-engine/internal/events"
	"github.com/dex/matching-engine/internal/marketdata"
	"github.com/dex/matching-engine/internal/matching"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/dex/matching-engine/internal/settlement"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// Engine sweeps futures positions on a timer and force-closes any that have
// fallen below their maintenance margin requirement at the current mark price.
type Engine struct {
	registry   *matching.Registry
	settlement *settlement.FuturesSettlement
	marketdata *marketdata.Service
	symbols    *config.Registry
	checker    *risk.Checker
	bus        *events.Bus
	log        *slog.Logger
}

// New creates a liquidation Engine.
func New(registry *matching.Registry, fs *settlement.FuturesSettlement, md *marketdata.Service,
	symbols *config.Registry, checker *risk.Checker, bus *events.Bus) *Engine {
	return &Engine{
		registry:   registry,
		settlement: fs,
		marketdata: md,
		symbols:    symbols,
		checker:    checker,
		bus:        bus,
		log:        slog.Default(),
	}
}

// Run starts the sweep loop; call in a goroutine. Stops when ctx is cancelled.
func (e *Engine) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.sweep()
		case <-ctx.Done():
			return
		}
	}
}

func (e *Engine) sweep() {
	for _, pos := range e.settlement.AllPositions() {
		if pos.Size.IsZero() {
			continue
		}
		cfg, err := e.symbols.Get(pos.Symbol, models.Futures)
		if err != nil || cfg.MaintenanceMarginRate.IsZero() {
			continue
		}
		ticker, err := e.marketdata.Ticker(pos.Symbol, models.Futures)
		if err != nil || ticker.MidPrice.IsZero() {
			continue
		}
		if pos.MarginRatio(ticker.MidPrice).GreaterThanOrEqual(cfg.MaintenanceMarginRate) {
			continue
		}
		e.forceClose(pos, ticker.MidPrice, cfg)
	}
}

func (e *Engine) forceClose(pos *settlement.Position, markPrice decimal.Decimal, cfg *config.SymbolConfig) {
	// closingSide is the opposite side of the held position.
	closingSide := models.Sell
	if pos.Side == models.Sell {
		closingSide = models.Buy
	}

	order := &models.Order{
		ID:                  uuid.NewString(),
		AccountID:           pos.AccountID,
		Symbol:              pos.Symbol,
		Market:              models.Futures,
		Side:                closingSide,
		Type:                models.Market,
		Quantity:            pos.Size.Abs(),
		ReduceOnly:          true,
		TimeInForce:         models.GTC,
		Status:              models.StatusPending,
		CreatedAt:           time.Now(),
		InternalLiquidation: true,
	}

	if err := e.checker.Check(order); err != nil {
		e.log.Error("liquidation risk check failed", "account", pos.AccountID, "symbol", pos.Symbol, "error", err)
		return
	}
	if _, err := e.registry.Submit(order); err != nil {
		e.log.Error("liquidation submit failed", "account", pos.AccountID, "symbol", pos.Symbol, "error", err)
		return
	}

	// The reduce-only market order above already closes the filled portion of
	// the position through Settle/applyFill at the actual fill prices. Only
	// force-close the remainder at the mark price if the position still exists
	// (i.e. the market order did not fully fill it). This avoids realizing PnL
	// twice or at inconsistent prices for the same quantity.
	if remaining := e.settlement.GetPosition(pos.AccountID, pos.Symbol); remaining != nil && !remaining.Size.IsZero() {
		e.settlement.ClosePosition(pos.AccountID, pos.Symbol, cfg.QuoteCurrency, markPrice)
	}

	e.log.Warn("position liquidated", "account", pos.AccountID, "symbol", pos.Symbol, "size", pos.Size.String())
	if e.bus != nil {
		e.bus.Publish(&models.Event{
			Type:   models.EventLiquidation,
			Symbol: pos.Symbol,
			Market: string(models.Futures),
			Liquidation: &models.Liquidation{
				AccountID: pos.AccountID,
				Symbol:    pos.Symbol,
				Side:      pos.Side,
				Size:      pos.Size,
			},
		})
	}
}
