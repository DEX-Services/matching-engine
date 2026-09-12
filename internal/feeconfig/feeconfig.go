// Package feeconfig loads and hot-reloads the platform's base fee rates
// (spot/futures maker+taker, P2P, swap, liquidation) from a single small
// Postgres table. All values are loaded into RAM at startup and refreshed on
// a slow background timer, exactly like internal/config's SymbolConfig
// registry — no per-order or per-fill network call is ever made from the
// settlement hot path. See FEE-TIER-SYSTEM-PLAN.md §1a for why this matters.
package feeconfig

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

// Key names for fee_config rows. Every key here is seeded on boot with the
// platform's launch default (see SeedDefaults) if the row does not already
// exist, so an admin's prior edit is never overwritten by a restart.
const (
	KeySpotMaker    = "spot.maker"
	KeySpotTaker    = "spot.taker"
	KeyFuturesMaker = "futures.maker"
	KeyFuturesTaker = "futures.taker"
	KeyP2PBuyer     = "p2p.buyer"
	KeyP2PSeller    = "p2p.seller"
	KeySwapIn       = "swap.in"  // USDT/USDC -> BIUSDB
	KeySwapOut      = "swap.out" // BIUSDB -> USDT/USDC
	KeyLiquidation  = "liquidation"
)

// defaultRates are the platform's launch fee rates, as decimal fractions
// (0.0015 == 0.15%). Seeded once; admin edits after that are never
// overwritten by a restart (see SeedDefaults's ON CONFLICT DO NOTHING).
var defaultRates = map[string]string{
	KeySpotMaker:    "0.0015",  // 0.15%
	KeySpotTaker:    "0.0045",  // 0.45%
	KeyFuturesMaker: "0.00015", // 0.015%
	KeyFuturesTaker: "0.00045", // 0.045%
	KeyP2PBuyer:     "0.0025",  // 0.25%
	KeyP2PSeller:    "0.0025",  // 0.25%
	KeySwapIn:       "0",       // 0%
	KeySwapOut:      "0.01",    // 1%
	KeyLiquidation:  "0.02",    // 2%
}

// Registry is the in-memory fee-rate store, hot-reloaded from Postgres.
type Registry struct {
	mu    sync.RWMutex
	rates map[string]decimal.Decimal
	pool  *pgxpool.Pool
	log   *slog.Logger
}

// NewInMemoryRegistry creates a Registry seeded with the launch defaults but
// with no Postgres backing (used when Postgres is disabled, e.g. local dev
// without a DB) — Rate() still returns sensible values instead of zero.
func NewInMemoryRegistry() *Registry {
	r := &Registry{rates: make(map[string]decimal.Decimal), log: slog.Default()}
	for k, v := range defaultRates {
		if d, err := decimal.NewFromString(v); err == nil {
			r.rates[k] = d
		}
	}
	return r
}

// NewRegistry creates a Registry and loads initial rates from Postgres.
func NewRegistry(ctx context.Context, pool *pgxpool.Pool) (*Registry, error) {
	r := &Registry{
		rates: make(map[string]decimal.Decimal),
		pool:  pool,
		log:   slog.Default(),
	}
	if err := r.reload(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// Rate returns the current rate for key, or zero if unconfigured. A plain
// in-memory map read — safe to call on the settlement hot path.
func (r *Registry) Rate(key string) decimal.Decimal {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.rates[key]
}

// All returns a snapshot of every configured rate, keyed by fee_config.key.
// Used by the admin API to list current values.
func (r *Registry) All() map[string]decimal.Decimal {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]decimal.Decimal, len(r.rates))
	for k, v := range r.rates {
		out[k] = v
	}
	return out
}

// StartHotReload begins periodic reloading at the given interval. Call in a
// goroutine; stops when ctx is cancelled.
func (r *Registry) StartHotReload(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := r.reload(ctx); err != nil {
				r.log.Error("fee config reload failed", "error", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (r *Registry) reload(ctx context.Context) error {
	rows, err := r.pool.Query(ctx, `SELECT key, rate FROM fee_config`)
	if err != nil {
		return fmt.Errorf("query fee_config: %w", err)
	}
	defer rows.Close()

	fresh := make(map[string]decimal.Decimal)
	for rows.Next() {
		var key, rateStr string
		if err := rows.Scan(&key, &rateStr); err != nil {
			return fmt.Errorf("scan fee_config row: %w", err)
		}
		rate, err := decimal.NewFromString(rateStr)
		if err != nil {
			r.log.Error("fee_config row has unparseable rate; skipping", "key", key, "raw", rateStr)
			continue
		}
		fresh[key] = rate
	}
	if err := rows.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	r.rates = fresh
	r.mu.Unlock()
	r.log.Info("fee config reloaded", "count", len(fresh))
	return nil
}

// EnsureSchema creates the fee_config table if it does not exist.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS fee_config (
		    key        TEXT PRIMARY KEY,
		    rate       NUMERIC(10,6) NOT NULL,
		    updated_by TEXT,
		    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`)
	return err
}

// SeedDefaults idempotently inserts the launch default rate for every key in
// defaultRates. ON CONFLICT DO NOTHING means an admin's prior edit is never
// clobbered by a restart — this only fills in rows that don't exist yet.
func SeedDefaults(ctx context.Context, pool *pgxpool.Pool) {
	for key, rate := range defaultRates {
		if _, err := pool.Exec(ctx,
			`INSERT INTO fee_config (key, rate) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`,
			key, rate,
		); err != nil {
			slog.Error("seed fee_config default", "key", key, "error", err)
		}
	}
}

// ValidKeys returns every recognized fee_config key, for admin API validation.
func ValidKeys() []string {
	return []string{
		KeySpotMaker, KeySpotTaker,
		KeyFuturesMaker, KeyFuturesTaker,
		KeyP2PBuyer, KeyP2PSeller,
		KeySwapIn, KeySwapOut,
		KeyLiquidation,
	}
}
