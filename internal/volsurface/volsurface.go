// Package volsurface persists implied-volatility observations per option
// contract and interpolates a usable IV for contracts that have never traded
// (and so have no live book to blend/imply from — see /option-chain).
//
// Without this, /option-chain fell back to a single flat assumedVol (60%)
// for every untraded strike/expiry regardless of how far it sits from an
// actively-quoted neighbor — the roadmap's "IV surface" gap (Phase 2 item 2).
// This is a real but intentionally simple surface: same-expiry strike
// interpolation from the two nearest observed strikes (or nearest single
// observation), NOT a full multi-expiry/skew-fitted surface. That is a
// reasonable v1 given how thin the seeded chain is; a proper parametric fit
// (SVI or similar) is future work once there is enough real trading data to
// fit one meaningfully.
package volsurface

import (
	"context"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

// EnsureSchema creates the iv_snapshots table if it does not already exist.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS iv_snapshots (
		    underlying_symbol TEXT        NOT NULL,
		    strike_price      NUMERIC     NOT NULL,
		    expiry            TIMESTAMPTZ NOT NULL,
		    option_type       TEXT        NOT NULL,
		    iv                NUMERIC     NOT NULL,
		    observed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		    PRIMARY KEY (underlying_symbol, strike_price, expiry, option_type)
		)`)
	return err
}

// Store reads and writes IV observations.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore builds a Store. pool may be nil (e.g. in tests or when Postgres is
// unconfigured), in which case every method is a no-op / cache-only fallback
// so the engine degrades to the pre-existing flat-vol behavior rather than
// erroring the whole /option-chain response.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Observation is one contract's most recently recorded implied vol.
type Observation struct {
	Strike     decimal.Decimal
	Expiry     time.Time
	OptionType string
	IV         float64 // annualized, e.g. 0.6 = 60%
	ObservedAt time.Time
}

// maxObservationAge bounds how stale a snapshot may be before Interpolate
// ignores it — a real quote from three days ago on a fast-moving underlying
// is no longer a trustworthy stand-in for today's vol.
const maxObservationAge = 24 * time.Hour

// Record upserts the latest observed IV for one contract. Called from
// /option-chain whenever a contract's IV was re-implied from a real blended
// book price (see cmd/engine/main.go) — never for the flat assumedVol
// fallback, which would just be recording the assumption back to itself.
// A nil pool (Postgres unconfigured) makes this a no-op.
func (s *Store) Record(ctx context.Context, underlying string, strike decimal.Decimal, expiry time.Time, optionType string, iv float64) {
	if s == nil || s.pool == nil || iv <= 0 {
		return
	}
	// Best-effort: a failed write here must never fail the /option-chain
	// request itself (the caller doesn't check this error), it only means
	// slightly staler interpolation data for other strikes next time.
	_, _ = s.pool.Exec(ctx, `
		INSERT INTO iv_snapshots (underlying_symbol, strike_price, expiry, option_type, iv, observed_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (underlying_symbol, strike_price, expiry, option_type)
		DO UPDATE SET iv = EXCLUDED.iv, observed_at = EXCLUDED.observed_at`,
		underlying, strike, expiry, optionType, iv)
}

// Interpolate returns an IV estimate for (strike, expiry, optionType) derived
// from recent same-expiry, same-type observations at OTHER strikes, or false
// if there are none recent enough to use. Linear interpolation between the
// two nearest bracketing strikes when both exist; the single nearest
// observation's IV otherwise (flat extrapolation beyond the observed range,
// which is the standard simple fallback for a thin surface — better than
// guessing a slope from one point).
//
// This deliberately does not read back the CURRENT contract's own most
// recent snapshot — the caller (main.go) only calls this when it does NOT
// have a live book price to imply IV from directly, so there is no "own"
// observation to prefer here; every row for this (expiry, optionType) is a
// same-expiry NEIGHBOR's data.
func (s *Store) Interpolate(ctx context.Context, underlying string, strike decimal.Decimal, expiry time.Time, optionType string) (float64, bool) {
	if s == nil || s.pool == nil {
		return 0, false
	}
	cutoff := time.Now().Add(-maxObservationAge)
	rows, err := s.pool.Query(ctx, `
		SELECT strike_price, iv FROM iv_snapshots
		WHERE underlying_symbol = $1 AND expiry = $2 AND option_type = $3 AND observed_at >= $4
		ORDER BY strike_price ASC`,
		underlying, expiry, optionType, cutoff)
	if err != nil {
		return 0, false
	}
	defer rows.Close()

	type point struct {
		strike float64
		iv     float64
	}
	var points []point
	for rows.Next() {
		var strikeDec decimal.Decimal
		var iv float64
		if err := rows.Scan(&strikeDec, &iv); err != nil {
			continue
		}
		f, _ := strikeDec.Float64()
		points = append(points, point{strike: f, iv: iv})
	}
	if len(points) == 0 {
		return 0, false
	}
	target, _ := strike.Float64()
	strikes := make([]float64, len(points))
	ivs := make([]float64, len(points))
	for i, p := range points {
		strikes[i], ivs[i] = p.strike, p.iv
	}
	return interpolate(target, strikes, ivs)
}

// interpolate returns the linearly-interpolated IV at targetStrike given
// parallel (unsorted, possibly unordered) strikes/ivs slices of equal
// length. Sorts a local copy, finds the two bracketing strikes, and linearly
// interpolates between them; flat-extrapolates using the nearest single
// observation when target falls outside the observed range. Returns
// (0, false) only when strikes is empty — callers already guarantee
// len(strikes) == len(ivs) since they only come from Interpolate's own
// parallel construction above.
func interpolate(target float64, strikes, ivs []float64) (float64, bool) {
	if len(strikes) == 0 {
		return 0, false
	}
	type point struct{ strike, iv float64 }
	points := make([]point, len(strikes))
	for i := range strikes {
		points[i] = point{strikes[i], ivs[i]}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].strike < points[j].strike })

	var lo, hi *point
	for i := range points {
		if points[i].strike <= target {
			lo = &points[i]
		}
		if points[i].strike >= target && hi == nil {
			hi = &points[i]
		}
	}
	switch {
	case lo != nil && hi != nil && lo != hi:
		span := hi.strike - lo.strike
		if span == 0 {
			return lo.iv, true
		}
		weight := (target - lo.strike) / span
		return lo.iv + weight*(hi.iv-lo.iv), true
	case lo != nil:
		return lo.iv, true // target is at/beyond the highest observed strike
	case hi != nil:
		return hi.iv, true // target is at/below the lowest observed strike
	default:
		return 0, false
	}
}
