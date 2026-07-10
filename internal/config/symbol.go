// Package config loads and hot-reloads symbol configuration.
// All values are loaded into RAM at startup; no per-order network calls are made.
package config

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

// SymbolConfig holds the static parameters for a single trading pair.
type SymbolConfig struct {
	Symbol      string
	Market      models.MarketType
	BaseCurrency  string
	QuoteCurrency string
	TickSize    decimal.Decimal // minimum price increment
	LotSize     decimal.Decimal // minimum quantity increment
	MinNotional decimal.Decimal // minimum order value
	MaxPrice    decimal.Decimal // upper price limit
	MakerFee    decimal.Decimal // fraction (e.g. 0.001 = 0.1%)
	TakerFee    decimal.Decimal
	Active      bool
}

// Registry is the in-memory symbol configuration store.
// Configs are loaded from Postgres at startup and refreshed on a slow interval.
type Registry struct {
	mu      sync.RWMutex
	configs map[string]*SymbolConfig // key: symbol+":"+market
	pool    *pgxpool.Pool
	log     *slog.Logger
}

// NewRegistry creates a Registry and loads initial configuration from Postgres.
func NewRegistry(ctx context.Context, pool *pgxpool.Pool) (*Registry, error) {
	r := &Registry{
		configs: make(map[string]*SymbolConfig),
		pool:    pool,
		log:     slog.Default(),
	}
	if err := r.reload(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// Get returns the config for symbol/market, or an error if not found.
func (r *Registry) Get(symbol string, market models.MarketType) (*SymbolConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cfg, ok := r.configs[symbol+":"+string(market)]
	if !ok {
		return nil, fmt.Errorf("no config for %s/%s", symbol, market)
	}
	return cfg, nil
}

// All returns all active symbol configurations.
func (r *Registry) All() []*SymbolConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*SymbolConfig, 0, len(r.configs))
	for _, c := range r.configs {
		if c.Active {
			out = append(out, c)
		}
	}
	return out
}

// StartHotReload begins periodic reloading at the given interval.
// Call in a goroutine; stops when ctx is cancelled.
func (r *Registry) StartHotReload(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := r.reload(ctx); err != nil {
				r.log.Error("symbol config reload failed", "error", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (r *Registry) reload(ctx context.Context) error {
	rows, err := r.pool.Query(ctx, `
		SELECT symbol, market, base_currency, quote_currency,
		       tick_size, lot_size, min_notional, max_price,
		       maker_fee, taker_fee, active
		FROM symbol_configs
		WHERE active = true`)
	if err != nil {
		return fmt.Errorf("query symbol_configs: %w", err)
	}
	defer rows.Close()

	fresh := make(map[string]*SymbolConfig)
	for rows.Next() {
		var c SymbolConfig
		var market string
		var tickSize, lotSize, minNotional, maxPrice, makerFee, takerFee string
		if err := rows.Scan(&c.Symbol, &market, &c.BaseCurrency, &c.QuoteCurrency,
			&tickSize, &lotSize, &minNotional, &maxPrice,
			&makerFee, &takerFee, &c.Active); err != nil {
			return fmt.Errorf("scan symbol_config row: %w", err)
		}
		c.Market = models.MarketType(market)
		c.TickSize, _ = decimal.NewFromString(tickSize)
		c.LotSize, _ = decimal.NewFromString(lotSize)
		c.MinNotional, _ = decimal.NewFromString(minNotional)
		c.MaxPrice, _ = decimal.NewFromString(maxPrice)
		c.MakerFee, _ = decimal.NewFromString(makerFee)
		c.TakerFee, _ = decimal.NewFromString(takerFee)
		fresh[c.Symbol+":"+market] = &c
	}
	if err := rows.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	r.configs = fresh
	r.mu.Unlock()
	r.log.Info("symbol configs reloaded", "count", len(fresh))
	return nil
}

// EnsureSchema creates the symbol_configs table if it does not exist.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS symbol_configs (
		    symbol         TEXT        NOT NULL,
		    market         TEXT        NOT NULL,
		    base_currency  TEXT        NOT NULL,
		    quote_currency TEXT        NOT NULL,
		    tick_size      NUMERIC     NOT NULL DEFAULT '0.01',
		    lot_size       NUMERIC     NOT NULL DEFAULT '0.00001',
		    min_notional   NUMERIC     NOT NULL DEFAULT '1',
		    max_price      NUMERIC     NOT NULL DEFAULT '1000000',
		    maker_fee      NUMERIC     NOT NULL DEFAULT '0.001',
		    taker_fee      NUMERIC     NOT NULL DEFAULT '0.001',
		    active         BOOLEAN     NOT NULL DEFAULT true,
		    PRIMARY KEY (symbol, market)
		)`)
	return err
}
