package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/dex/matching-engine/internal/marketdata"
	"github.com/dex/matching-engine/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

// comboMemoryFallback holds combo instruments in-process when Postgres is
// disabled (local/dev), so getOrCreateComboInstrument and loadComboInstrument
// agree on the same set of registered combos instead of the create path
// silently succeeding while the settlement-time lookup always fails. Combos
// created this way do not survive a restart, same limitation option
// instruments already have without Postgres.
var (
	comboMemoryMu    sync.RWMutex
	comboMemoryStore = map[string]*comboInstrument{}
)

// comboInstrument is a registered N-leg options combo, matching how real
// options exchanges list a spread/butterfly/condor as its own tradable
// instrument (Deribit/CME/Binance Options combos) rather than something
// assembled client-side from separate independent orders.
type comboInstrument struct {
	Symbol     string            // deterministic, derived from the legs — see comboSymbolFor
	Legs       []models.ComboLeg // each leg's option instrument symbol + signed ratio
	Underlying string
}

// comboSymbolFor derives a deterministic combo symbol from its legs (each a
// symbol+ratio pair), so requesting the same leg set twice always resolves
// to the same instrument (and the same order book) instead of creating
// duplicates. Legs are sorted by symbol before hashing so the same
// structure requested with its legs listed in a different order still
// resolves to the identical combo. A short hash keeps the symbol a
// reasonable length regardless of leg count, while staying human-traceable
// via the COMBO: prefix and the full leg list recorded in combo_instruments.
func comboSymbolFor(legs []models.ComboLeg) string {
	sorted := make([]models.ComboLeg, len(legs))
	copy(sorted, legs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Symbol < sorted[j].Symbol })
	h := sha256.New()
	for _, leg := range sorted {
		fmt.Fprintf(h, "%s:%d|", leg.Symbol, leg.Ratio)
	}
	return "COMBO:" + hex.EncodeToString(h.Sum(nil)[:8])
}

// ensureComboSchema creates the combo_instruments table if it does not
// already exist. legs is stored as JSONB (a variable-length leg list, not a
// fixed 2-column buy/sell pair) so this schema supports any leg count.
func ensureComboSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS combo_instruments (
		    symbol            TEXT NOT NULL PRIMARY KEY,
		    legs              JSONB NOT NULL,
		    underlying_symbol TEXT NOT NULL,
		    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`)
	return err
}

// getOrCreateComboInstrument returns the combo instrument for the given
// legs, creating and persisting it on first use — the same lazy-
// registration pattern validateAndPrepareOption uses for individual option
// contracts (each new strike/expiry/type also only exists once an order
// actually references it).
func getOrCreateComboInstrument(ctx context.Context, pool *pgxpool.Pool, legs []models.ComboLeg, underlying string) (*comboInstrument, error) {
	symbol := comboSymbolFor(legs)
	inst := &comboInstrument{Symbol: symbol, Legs: legs, Underlying: underlying}
	if pool == nil {
		// Postgres disabled (local/dev): register in the in-memory fallback
		// so loadComboInstrument (used at settlement time) can still find
		// it — mirrors option_instruments' own pool==nil behavior, except
		// that path has no analogous settlement-time re-lookup to keep in
		// sync with.
		comboMemoryMu.Lock()
		comboMemoryStore[symbol] = inst
		comboMemoryMu.Unlock()
		return inst, nil
	}
	legsJSON, err := json.Marshal(legs)
	if err != nil {
		return nil, fmt.Errorf("marshal combo legs: %w", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO combo_instruments (symbol, legs, underlying_symbol)
		VALUES ($1, $2, $3)
		ON CONFLICT (symbol) DO NOTHING`,
		symbol, legsJSON, underlying)
	if err != nil {
		return nil, fmt.Errorf("register combo instrument: %w", err)
	}
	return inst, nil
}

// loadComboInstrument looks up an already-registered combo by its symbol,
// for resolving an order/trade back to its legs at settlement time.
func loadComboInstrument(ctx context.Context, pool *pgxpool.Pool, symbol string) (*comboInstrument, error) {
	if pool == nil {
		comboMemoryMu.RLock()
		inst, ok := comboMemoryStore[symbol]
		comboMemoryMu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("combo instrument %s not found (in-memory store, Postgres disabled)", symbol)
		}
		return inst, nil
	}
	var c comboInstrument
	var legsJSON []byte
	err := pool.QueryRow(ctx, `
		SELECT symbol, legs, underlying_symbol
		FROM combo_instruments WHERE symbol = $1`, symbol).
		Scan(&c.Symbol, &legsJSON, &c.Underlying)
	if err != nil {
		return nil, fmt.Errorf("combo instrument %s not found: %w", symbol, err)
	}
	if err := json.Unmarshal(legsJSON, &c.Legs); err != nil {
		return nil, fmt.Errorf("unmarshal combo legs for %s: %w", symbol, err)
	}
	return &c, nil
}

// comboSettlementAdapter implements settlement.ComboLegResolver and
// settlement.ComboOptionsMarkSource over this package's Postgres-backed
// instrument tables and the shared marketdata.Service — the one-way
// dependency bridge that lets internal/settlement stay ignorant of
// cmd/engine's private optionInstrument/comboInstrument types (see
// settlement.ComboLegResolver's doc comment).
type comboSettlementAdapter struct {
	pool  *pgxpool.Pool
	mdSvc *marketdata.Service
}

func (a *comboSettlementAdapter) ResolveComboLegs(ctx context.Context, comboSymbol string) ([]models.ComboLeg, string, error) {
	inst, err := loadComboInstrument(ctx, a.pool, comboSymbol)
	if err != nil {
		return nil, "", err
	}
	return inst.Legs, inst.Underlying, nil
}

func (a *comboSettlementAdapter) UnderlyingMark(underlying string) (decimal.Decimal, bool) {
	return a.mdSvc.UnderlyingMark(underlying)
}

func (a *comboSettlementAdapter) LegSpec(ctx context.Context, legSymbol string) (strike decimal.Decimal, expiry time.Time, optionType, quoteCurrency string, ok bool) {
	inst, err := loadOptionInstrument(ctx, a.pool, legSymbol)
	if err != nil || inst == nil {
		return decimal.Zero, time.Time{}, "", "", false
	}
	quote := "BIUSD"
	if parts := splitOptionSymbol(legSymbol); len(parts) >= 2 {
		quote = parts[1]
	}
	return inst.Strike, inst.Expiry, inst.OptionType, quote, true
}

// validateComboLegs checks that a set of N legs actually forms a coherent
// options combo: fetches each leg's real instrument spec from Postgres,
// then delegates the shape rules to validateComboLegSpecs. Split into a
// DB-driving wrapper (this function) and a pure logic function
// (validateComboLegSpecs) so the actual validation rules are testable
// without a live Postgres pool — loadOptionInstrument needs one to look
// symbols up in the first place (it returns (nil, nil) rather than an
// error when pool is nil, so this wrapper cannot be meaningfully exercised
// in a pool-less unit test; see cmd/engine's combo_test.go for the split).
func validateComboLegs(ctx context.Context, pool *pgxpool.Pool, legs []models.ComboLeg) ([]*optionInstrument, string, error) {
	if len(legs) < 2 {
		return nil, "", fmt.Errorf("a combo needs at least 2 legs, got %d", len(legs))
	}
	insts := make([]*optionInstrument, 0, len(legs))
	for _, leg := range legs {
		if leg.Ratio == 0 {
			return nil, "", fmt.Errorf("leg %s has a zero ratio (must be non-zero, positive=long/negative=short)", leg.Symbol)
		}
		inst, err := loadOptionInstrument(ctx, pool, leg.Symbol)
		if err != nil || inst == nil {
			return nil, "", fmt.Errorf("leg %s is not a known option instrument", leg.Symbol)
		}
		insts = append(insts, inst)
	}
	underlying, err := validateComboLegSpecs(insts)
	if err != nil {
		return nil, "", err
	}
	return insts, underlying, nil
}

// validateComboLegSpecs checks the actual combo shape rules on already-
// loaded instruments: all legs must share the same underlying and expiry
// (mixing expiries breaks the single-expiry payoff-diagram math
// risk.ComboMaxLossMargin relies on). Strikes/types may repeat or vary
// freely — that's what distinguishes a vertical (2 legs) from a butterfly
// (3, one strike repeated with ratio 2) from an iron condor (4, two option
// types) from a generalized ratio spread (any ratios). Caller (
// validateComboLegs) already enforces >= 2 legs and non-zero ratios before
// this runs.
func validateComboLegSpecs(insts []*optionInstrument) (underlying string, err error) {
	underlying = insts[0].Underlying
	expiry := insts[0].Expiry
	for i, inst := range insts {
		if inst.Underlying != underlying {
			return "", fmt.Errorf("all legs must share the same underlying (leg %d: %s, expected %s)", i, inst.Underlying, underlying)
		}
		if !inst.Expiry.Equal(expiry) {
			return "", fmt.Errorf("all legs must share the same expiry (leg %d has a different expiry) — mixed-expiry combos (calendars/diagonals) are not supported", i)
		}
	}
	return underlying, nil
}
