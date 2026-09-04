package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

// seedSymbolConfigs inserts default configuration rows for every pair this
// engine registers at startup, if not already present. Idempotent.
func seedSymbolConfigs(ctx context.Context, pool *pgxpool.Pool) {
	rows := []struct {
		symbol, market, base, quote, underlying string
		maxLeverage, fundingIntervalHours       int
		maintenanceMarginRate, contractMult     string
	}{
		// UnderlyingSymbol on a FUTURES row must be a real registered SPOT
		// symbol — the funding scheduler and the /ticker handler both look
		// up Ticker(UnderlyingSymbol, Spot) to get the "index" price funding
		// is computed against. BTC-USDC's row previously pointed at itself
		// ("BTC-USDC"), which isn't a registered spot market at all — every
		// funding tick silently fell back to indexPrice == markPrice (zero
		// drift, zero rate, nothing ever paid) instead of erroring loudly.
		// Fixed to point at the real BTC-USDB spot market.
		//
		// Every market — spot, futures, and options — quotes in USDB, the
		// platform's internal stable currency pegged 1:1 to USDT — see
		// Dex-Backend's chain.Listener. USDT/USDC are no longer tradable
		// quote currencies anywhere on the exchange; futures collateral used
		// to be real USDC, now converts to/settles in USDB like everything
		// else. The futures row's symbol is "BTC-USDB"/"ETH-USDB" too — a
		// distinct (symbol, market) row from the SPOT row of the same name,
		// so the two coexist without collision.
		{"BTC-USDB", "SPOT", "BTC", "USDB", "", 0, 0, "0", "0"},
		{"ETH-USDB", "SPOT", "ETH", "USDB", "", 0, 0, "0", "0"},
		{"SOL-USDB", "SPOT", "SOL", "USDB", "", 0, 0, "0", "0"},
		{"BNB-USDB", "SPOT", "BNB", "USDB", "", 0, 0, "0", "0"},
		{"BTC-USDB", "FUTURES", "BTC", "USDB", "BTC-USDB", 100, 8, "0.005", "0"},
		{"ETH-USDB", "FUTURES", "ETH", "USDB", "ETH-USDB", 75, 8, "0.0075", "0"},
		{"BTC-USDB", "OPTIONS", "BTC", "USDB", "BTC-USDB", 0, 0, "0", "1"},
	}
	for _, r := range rows {
		_, err := pool.Exec(ctx, `
			INSERT INTO symbol_configs
			    (symbol, market, base_currency, quote_currency, max_leverage,
			     maintenance_margin_rate, funding_interval_hours, contract_multiplier, underlying_symbol)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (symbol, market) DO UPDATE SET
			    max_leverage = EXCLUDED.max_leverage,
			    maintenance_margin_rate = EXCLUDED.maintenance_margin_rate,
			    funding_interval_hours = EXCLUDED.funding_interval_hours,
			    contract_multiplier = EXCLUDED.contract_multiplier,
			    underlying_symbol = EXCLUDED.underlying_symbol`,
			r.symbol, r.market, r.base, r.quote, r.maxLeverage,
			r.maintenanceMarginRate, r.fundingIntervalHours, r.contractMult, r.underlying)
		if err != nil {
			slog.Error("seed symbol_configs", "symbol", r.symbol, "market", r.market, "error", err)
		}
	}
}

// seedOptionInstruments inserts a small BTC-USDB option chain (a handful of
// strikes at two expiries) whenever there are no unexpired contracts left,
// so /option-chain always has data instead of going empty forever once the
// first seeded batch expires.
//
// Previously this checked `count(*) > 0` (any row ever, expired or not),
// which is idempotent on the FIRST boot but never fires again — the
// 7-day/30-day contracts seeded once eventually all expire, /option-chain's
// tYears<=0 filter drops every one of them, and the chain silently goes
// empty with no error and no path back to non-empty short of a manual DB
// edit. Checking unexpired count instead makes this genuinely self-healing:
// any boot (or the periodic re-check below) that finds nothing live re-seeds
// a fresh batch of expiries relative to "now".
func seedOptionInstruments(ctx context.Context, pool *pgxpool.Pool) {
	var liveCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM option_instruments
		WHERE underlying_symbol = 'BTC-USDB' AND active = true AND expiry > now()`).Scan(&liveCount); err != nil {
		slog.Error("count live option_instruments", "error", err)
		return
	}
	if liveCount > 0 {
		return
	}

	strikes := []int{55000, 60000, 65000, 70000, 75000}
	expiries := []time.Duration{7 * 24 * time.Hour, 30 * 24 * time.Hour}
	for _, dur := range expiries {
		expiry := time.Now().Add(dur)
		for _, strike := range strikes {
			for _, optType := range []string{"CALL", "PUT"} {
				// Instrument symbol encodes BASE-QUOTE-STRIKE-EXPIRY-TYPE so
				// each contract gets its own order book and the underlying
				// spot pair can be parsed from the symbol.
				symbol := fmt.Sprintf("BTC-USDB-%d-%s-%s", strike, expiry.Format("20060102"), optType)
				_, err := pool.Exec(ctx, `
					INSERT INTO option_instruments (symbol, underlying_symbol, strike_price, expiry, option_type)
					VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT DO NOTHING`,
					symbol, "BTC-USDB", decimal.NewFromInt(int64(strike)), expiry, optType)
				if err != nil {
					slog.Error("seed option_instruments", "symbol", symbol, "error", err)
				}
			}
		}
	}
}

// optionInstrument is a discrete listed option contract.
type optionInstrument struct {
	Symbol     string
	Underlying string
	OptionType string
	Strike     decimal.Decimal
	Expiry     time.Time
}

// loadOptionInstruments returns all active option instruments for an underlying.
// Returns an empty slice (not an error) when Postgres is disabled.
func loadOptionInstruments(ctx context.Context, pool *pgxpool.Pool, underlying string) ([]optionInstrument, error) {
	if pool == nil {
		return nil, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT symbol, underlying_symbol, option_type, strike_price, expiry
		FROM option_instruments
		WHERE underlying_symbol = $1 AND active = true
		ORDER BY expiry, strike_price`, underlying)
	if err != nil {
		return nil, fmt.Errorf("query option_instruments: %w", err)
	}
	defer rows.Close()

	var out []optionInstrument
	for rows.Next() {
		var inst optionInstrument
		var strike string
		if err := rows.Scan(&inst.Symbol, &inst.Underlying, &inst.OptionType, &strike, &inst.Expiry); err != nil {
			return nil, fmt.Errorf("scan option_instrument: %w", err)
		}
		inst.Strike, _ = decimal.NewFromString(strike)
		out = append(out, inst)
	}
	return out, rows.Err()
}

// loadOptionInstrument returns a single active option instrument by its
// symbol, or nil when Postgres is disabled or the instrument is not found.
func loadOptionInstrument(ctx context.Context, pool *pgxpool.Pool, symbol string) (*optionInstrument, error) {
	if pool == nil {
		return nil, nil
	}
	var inst optionInstrument
	var strike string
	err := pool.QueryRow(ctx, `
		SELECT symbol, underlying_symbol, option_type, strike_price, expiry
		FROM option_instruments
		WHERE symbol = $1 AND active = true`, symbol).Scan(
		&inst.Symbol, &inst.Underlying, &inst.OptionType, &strike, &inst.Expiry)
	if err != nil {
		return nil, fmt.Errorf("query option_instrument %s: %w", symbol, err)
	}
	inst.Strike, _ = decimal.NewFromString(strike)
	return &inst, nil
}
