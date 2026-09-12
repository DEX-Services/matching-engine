// Package discounts loads and hot-reloads each user's active fee-tier
// discount from Postgres. Like internal/config and internal/feeconfig, the
// settlement hot path only ever reads an in-memory map — see
// FEE-TIER-SYSTEM-PLAN.md §1a for why a per-fill Postgres lookup would be
// unacceptable regardless of how much hardware backs the process.
//
// This ships with the "poll all active subscriptions on a timer" approach
// (§1a's simpler phase-2 option): a purchase's discount takes effect within
// one poll interval, not instantly. The lazy-cache-plus-pub/sub upgrade
// described in the plan can replace reload()'s internals later without any
// change to ActiveDiscountFor's signature or any settlement call site.
package discounts

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

// Registry is the in-memory per-user discount store.
type Registry struct {
	mu   sync.RWMutex
	byID map[string]decimal.Decimal // userID -> discount fraction (0.15 == 15%)
	pool *pgxpool.Pool
	log  *slog.Logger
}

// NewInMemoryRegistry creates an empty Registry with no Postgres backing —
// every user reads back a zero discount. Used when Postgres is disabled.
func NewInMemoryRegistry() *Registry {
	return &Registry{byID: make(map[string]decimal.Decimal), log: slog.Default()}
}

// NewRegistry creates a Registry and loads initial discounts from Postgres.
func NewRegistry(ctx context.Context, pool *pgxpool.Pool) (*Registry, error) {
	r := &Registry{byID: make(map[string]decimal.Decimal), pool: pool, log: slog.Default()}
	if err := r.reload(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// ActiveDiscountFor returns userID's current fee discount as a fraction
// (0.15 for 15%off), or zero if they have no active subscription. A plain
// in-memory map read — safe to call on the settlement hot path.
func (r *Registry) ActiveDiscountFor(userID string) decimal.Decimal {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byID[userID]
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
				r.log.Error("discount registry reload failed", "error", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// reload pulls every currently-active, unexpired subscription and takes the
// highest discount_pct per user (defensive: only one row should ever be
// 'active' per user at a time, since a repurchase supersedes the prior row,
// but this guards against any transient double-active state instead of
// picking an arbitrary one).
func (r *Registry) reload(ctx context.Context) error {
	rows, err := r.pool.Query(ctx, `
		SELECT s.user_id, t.discount_pct
		FROM user_fee_subscriptions s
		JOIN fee_tiers t ON t.tier = s.tier
		WHERE s.status = 'active' AND s.expires_at > now()`)
	if err != nil {
		return fmt.Errorf("query user_fee_subscriptions: %w", err)
	}
	defer rows.Close()

	fresh := make(map[string]decimal.Decimal)
	for rows.Next() {
		var userID, pctStr string
		if err := rows.Scan(&userID, &pctStr); err != nil {
			return fmt.Errorf("scan subscription row: %w", err)
		}
		pct, err := decimal.NewFromString(pctStr)
		if err != nil {
			continue
		}
		discount := pct.Div(decimal.NewFromInt(100)) // discount_pct is e.g. 15 -> 0.15
		if existing, ok := fresh[userID]; !ok || discount.GreaterThan(existing) {
			fresh[userID] = discount
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	r.byID = fresh
	r.mu.Unlock()
	r.log.Info("discount registry reloaded", "activeUsers", len(fresh))
	return nil
}

// EnsureSchema creates fee_tiers and user_fee_subscriptions if they do not
// exist.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS fee_tiers (
		    tier         INT PRIMARY KEY,
		    biusdb_value NUMERIC(20,2) NOT NULL,
		    discount_pct NUMERIC(5,2) NOT NULL,
		    active       BOOLEAN NOT NULL DEFAULT true
		)`); err != nil {
		return fmt.Errorf("ensure fee_tiers: %w", err)
	}
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS user_fee_subscriptions (
		    id                  BIGSERIAL PRIMARY KEY,
		    user_id             TEXT NOT NULL,
		    tier                INT NOT NULL REFERENCES fee_tiers(tier),
		    bi2x_price_snapshot NUMERIC(20,8) NOT NULL,
		    bi2x_amount_paid    NUMERIC(38,0) NOT NULL,
		    purchased_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
		    expires_at          TIMESTAMPTZ NOT NULL,
		    status              TEXT NOT NULL DEFAULT 'active',
		    created_by_admin    BOOLEAN NOT NULL DEFAULT false
		);
		CREATE INDEX IF NOT EXISTS idx_user_fee_subscriptions_lookup
		    ON user_fee_subscriptions (user_id, status, expires_at)`)
	if err != nil {
		return fmt.Errorf("ensure user_fee_subscriptions: %w", err)
	}
	return nil
}

// tierDefaults are the platform's 10 launch discount tiers (tier, BIUSDB
// value, discount %). Seeded once via SeedTiers; admin edits (if ever
// enabled) are never clobbered by a restart.
var tierDefaults = []struct {
	tier        int
	biusdbValue string
	discountPct string
}{
	{1, "500", "5"},
	{2, "1000", "10"},
	{3, "5000", "15"},
	{4, "10000", "20"},
	{5, "20000", "25"},
	{6, "40000", "30"},
	{7, "60000", "35"},
	{8, "80000", "40"},
	{9, "100000", "45"},
	{10, "200000", "50"},
}

// SeedTiers idempotently inserts the 10 launch discount tiers.
func SeedTiers(ctx context.Context, pool *pgxpool.Pool) {
	for _, t := range tierDefaults {
		if _, err := pool.Exec(ctx,
			`INSERT INTO fee_tiers (tier, biusdb_value, discount_pct) VALUES ($1, $2, $3) ON CONFLICT (tier) DO NOTHING`,
			t.tier, t.biusdbValue, t.discountPct,
		); err != nil {
			slog.Error("seed fee_tiers default", "tier", t.tier, "error", err)
		}
	}
}
