package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/dex/matching-engine/internal/marketdata"
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

// comboInstrument is a registered 2-leg vertical spread combo, matching how
// real options exchanges list a spread as its own tradable instrument
// (Deribit/CME/Binance Options combos) rather than something assembled
// client-side from two independent orders.
type comboInstrument struct {
	Symbol     string // deterministic, derived from the two legs — see comboSymbolFor
	BuySymbol  string // the leg a BUY on the combo goes long
	SellSymbol string // the leg a BUY on the combo goes short
	Underlying string
}

// comboSymbolFor derives a deterministic combo symbol from its two legs, so
// requesting the same (buySymbol, sellSymbol) pair twice always resolves to
// the same instrument (and the same order book) instead of creating
// duplicates. A short hash keeps it a reasonable length regardless of how
// long the underlying option symbols are, while staying human-traceable via
// the COMBO: prefix and the full leg symbols recorded in combo_instruments.
func comboSymbolFor(buySymbol, sellSymbol string) string {
	sum := sha256.Sum256([]byte(buySymbol + "|" + sellSymbol))
	return "COMBO:" + hex.EncodeToString(sum[:8])
}

// ensureComboSchema creates the combo_instruments table if it does not
// already exist.
func ensureComboSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS combo_instruments (
		    symbol            TEXT NOT NULL PRIMARY KEY,
		    buy_symbol        TEXT NOT NULL,
		    sell_symbol       TEXT NOT NULL,
		    underlying_symbol TEXT NOT NULL,
		    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`)
	return err
}

// getOrCreateComboInstrument returns the combo instrument for (buySymbol,
// sellSymbol), creating and persisting it on first use — the same
// lazy-registration pattern validateAndPrepareOption uses for individual
// option contracts (each new strike/expiry/type also only exists once an
// order actually references it).
func getOrCreateComboInstrument(ctx context.Context, pool *pgxpool.Pool, buyInst, sellInst *optionInstrument) (*comboInstrument, error) {
	symbol := comboSymbolFor(buyInst.Symbol, sellInst.Symbol)
	inst := &comboInstrument{Symbol: symbol, BuySymbol: buyInst.Symbol, SellSymbol: sellInst.Symbol, Underlying: buyInst.Underlying}
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
	_, err := pool.Exec(ctx, `
		INSERT INTO combo_instruments (symbol, buy_symbol, sell_symbol, underlying_symbol)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (symbol) DO NOTHING`,
		symbol, buyInst.Symbol, sellInst.Symbol, buyInst.Underlying)
	if err != nil {
		return nil, fmt.Errorf("register combo instrument: %w", err)
	}
	return inst, nil
}

// loadComboInstrument looks up an already-registered combo by its symbol,
// for resolving an order/trade back to its two legs at settlement time.
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
	err := pool.QueryRow(ctx, `
		SELECT symbol, buy_symbol, sell_symbol, underlying_symbol
		FROM combo_instruments WHERE symbol = $1`, symbol).
		Scan(&c.Symbol, &c.BuySymbol, &c.SellSymbol, &c.Underlying)
	if err != nil {
		return nil, fmt.Errorf("combo instrument %s not found: %w", symbol, err)
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

func (a *comboSettlementAdapter) ResolveComboLegs(ctx context.Context, comboSymbol string) (buySymbol, sellSymbol, underlying string, err error) {
	inst, err := loadComboInstrument(ctx, a.pool, comboSymbol)
	if err != nil {
		return "", "", "", err
	}
	return inst.BuySymbol, inst.SellSymbol, inst.Underlying, nil
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

// validateVerticalPair checks the actual vertical-spread shape rules on two
// already-loaded instruments: same underlying, same expiry, same option
// type, different strikes. Split out as its own function so this logic is
// testable without a live Postgres pool (loadOptionInstrument requires one
// to look symbols up in the first place).
func validateVerticalPair(buyInst, sellInst *optionInstrument) error {
	if buyInst.Underlying != sellInst.Underlying {
		return fmt.Errorf("both legs must share the same underlying (got %s and %s)", buyInst.Underlying, sellInst.Underlying)
	}
	if !buyInst.Expiry.Equal(sellInst.Expiry) {
		return fmt.Errorf("both legs must share the same expiry for a vertical spread")
	}
	if buyInst.OptionType != sellInst.OptionType {
		return fmt.Errorf("both legs must be the same option type (both CALL or both PUT) for a vertical spread")
	}
	if buyInst.Strike.Equal(sellInst.Strike) {
		return fmt.Errorf("legs must have different strikes")
	}
	return nil
}
