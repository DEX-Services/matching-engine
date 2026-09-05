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
		maintenance                             string
		contractMult                            string
		// tickSize/lotSize are "" for markets that just want the schema
		// defaults (0.01 / 0.00001). "" is passed as SQL NULL, which the
		// statement below reads as "leave whatever is already there" — an
		// explicit "0" would instead overwrite a live market's granularity
		// with zero and disable its tick/lot validation.
		tickSize, lotSize string
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
		{"BTC-USDB", "SPOT", "BTC", "USDB", "", 0, 0, "0", "0", "", ""},
		{"ETH-USDB", "SPOT", "ETH", "USDB", "", 0, 0, "0", "0", "", ""},
		{"SOL-USDB", "SPOT", "SOL", "USDB", "", 0, 0, "0", "0", "", ""},
		{"BNB-USDB", "SPOT", "BNB", "USDB", "", 0, 0, "0", "0", "", ""},
		{"BTC-USDB", "FUTURES", "BTC", "USDB", "BTC-USDB", 100, 8, "0.005", "0", "", ""},
		{"ETH-USDB", "FUTURES", "ETH", "USDB", "ETH-USDB", 75, 8, "0.0075", "0", "", ""},
		{"BTC-USDB", "OPTIONS", "BTC", "USDB", "BTC-USDB", 0, 0, "0", "1", "", ""},
		// SOL/BNB perps mirror BTC/ETH: the spot books above exist, so they
		// double as the funding/index underlying.
		{"SOL-USDB", "FUTURES", "SOL", "USDB", "SOL-USDB", 50, 8, "0.005", "0", "", ""},
		{"BNB-USDB", "FUTURES", "BNB", "USDB", "BNB-USDB", 50, 8, "0.005", "0", "", ""},
		// Non-crypto perps have no engine spot book to serve as a funding
		// underlying, so funding_interval_hours = 0 keeps the funding
		// scheduler off for them (underlying_symbol stays empty for the same
		// reason). Leverage is kept conservative for FX/commodities/stocks.
		// tick reflects each instrument's native quoting increment (forex to
		// the pip, SILVER to a tenth of a cent, the rest to the cent). lot
		// stays FINE deliberately: it is a granularity, not a minimum size,
		// and the market maker floors every quote quantity down to it. A
		// coarse lot (one FX standard lot, say) silently rounds a whole ladder
		// to zero unless the desk is funded past roughly lot x price x levels,
		// and a desk that quotes nothing looks identical to a broken one. The
		// min_notional column is what actually floors order size.
		{"EURUSD-USDB", "FUTURES", "EURUSD", "USDB", "", 20, 0, "0.01", "0", "0.0001", "0.01"},
		{"GBPUSD-USDB", "FUTURES", "GBPUSD", "USDB", "", 20, 0, "0.01", "0", "0.0001", "0.01"},
		{"AUDUSD-USDB", "FUTURES", "AUDUSD", "USDB", "", 20, 0, "0.01", "0", "0.0001", "0.01"},
		{"GOLD-USDB", "FUTURES", "GOLD", "USDB", "", 20, 0, "0.01", "0", "0.01", "0.001"},
		{"SILVER-USDB", "FUTURES", "SILVER", "USDB", "", 20, 0, "0.01", "0", "0.001", "0.01"},
		{"CrudeOIL-USDB", "FUTURES", "CrudeOIL", "USDB", "", 20, 0, "0.01", "0", "0.01", "0.01"},
		{"AAPL.us-USDB", "FUTURES", "AAPL.us", "USDB", "", 20, 0, "0.01", "0", "0.01", "0.001"},
		{"TSLA.us-USDB", "FUTURES", "TSLA.us", "USDB", "", 20, 0, "0.01", "0", "0.01", "0.001"},
		{"NVDA.us-USDB", "FUTURES", "NVDA.us", "USDB", "", 20, 0, "0.01", "0", "0.01", "0.001"},
	}
	for _, r := range rows {
		// "" means "unspecified" for the granularity columns; NULL lets the
		// statement below fall through to the schema default on insert and to
		// the existing value on conflict.
		var tick, lot any
		if r.tickSize != "" {
			tick = r.tickSize
		}
		if r.lotSize != "" {
			lot = r.lotSize
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO symbol_configs
			    (symbol, market, base_currency, quote_currency, max_leverage,
			     maintenance_margin_rate, funding_interval_hours, contract_multiplier, underlying_symbol,
			     tick_size, lot_size)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
			        COALESCE($10::numeric, '0.01'), COALESCE($11::numeric, '0.00001'))
			ON CONFLICT (symbol, market) DO UPDATE SET
			    max_leverage = EXCLUDED.max_leverage,
			    maintenance_margin_rate = EXCLUDED.maintenance_margin_rate,
			    funding_interval_hours = EXCLUDED.funding_interval_hours,
			    contract_multiplier = EXCLUDED.contract_multiplier,
			    underlying_symbol = EXCLUDED.underlying_symbol,
			    tick_size = COALESCE($10::numeric, symbol_configs.tick_size),
			    lot_size = COALESCE($11::numeric, symbol_configs.lot_size)`,
			r.symbol, r.market, r.base, r.quote, r.maxLeverage,
			r.maintenance, r.fundingIntervalHours, r.contractMult, r.underlying,
			tick, lot)
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
