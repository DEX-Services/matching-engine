package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/config"
	"github.com/dex/matching-engine/internal/marketdata"
	"github.com/dex/matching-engine/internal/matching"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/dex/matching-engine/internal/settlement"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

// submitDeps bundles the shared state the order-submission pipeline needs.
// Extracted so both POST /order and POST /attached-order (which submits an
// entry plus its TP/SL legs) run through exactly one validation/reservation/
// matching code path instead of two copies that could silently diverge.
type submitDeps struct {
	reg               *matching.Registry
	ledger            *risk.Ledger
	backend           *backendclient.Client
	checker           *risk.Checker
	symbolRegistry    *config.Registry
	futuresSettlement *settlement.FuturesSettlement
	pgPool            *pgxpool.Pool
	mdSvc             *marketdata.Service
}

// submitOrderPipeline runs the exact validation/reservation/matching/release
// sequence the original POST /order handler used, unchanged in behavior.
// slippageBps is the raw query-string value ("" if not supplied). Returns
// the post-submit snapshot, trades, and (on error) the HTTP status the
// caller should respond with.
func submitOrderPipeline(ctx context.Context, d submitDeps, o *models.Order, slippageBps string) (*models.Order, []*models.Trade, int, error) {
	// Market-order slippage protection: an optional slippageBps value caps
	// how far a market order may walk the book from the best opposite quote
	// at submission time. Without this, a market order against a thin book
	// can execute at an arbitrarily bad price (matchAggressively applies no
	// price limit to Market orders). Implemented here (rather than inside
	// the matching core) by converting the order to an equivalent
	// marketable LIMIT at the slippage-bounded price — this reuses the
	// existing, well-tested price-limited matching path instead of adding a
	// second code path.
	if o.Type == models.Market && slippageBps != "" {
		bps, berr := decimal.NewFromString(slippageBps)
		if berr != nil || bps.IsNegative() {
			return nil, nil, http.StatusBadRequest, fmt.Errorf("invalid slippageBps")
		}
		eng, gerr := d.reg.Get(o.Symbol, o.Market)
		if gerr != nil {
			return nil, nil, http.StatusBadRequest, fmt.Errorf("invalid order: %w", gerr)
		}
		var refPrice decimal.Decimal
		if o.IsBuy() {
			refPrice = eng.BestAsk()
		} else {
			refPrice = eng.BestBid()
		}
		if refPrice.IsPositive() {
			factor := bps.Div(decimal.NewFromInt(10000))
			if o.IsBuy() {
				o.Price = refPrice.Mul(decimal.NewFromInt(1).Add(factor))
			} else {
				o.Price = refPrice.Mul(decimal.NewFromInt(1).Sub(factor))
			}
			o.Type = models.IOC // marketable limit: fill up to the cap, cancel remainder, never rests
		}
	}

	// Reduce-only enforcement (futures only): reject an order that would
	// increase or flip the account's position instead of only shrinking it.
	// Checked against the position as it stands at order-entry time; this is
	// a pre-trade guard, not a per-fill clamp, consistent with how the
	// reservation sizing below is also computed at entry time.
	if o.ReduceOnly && o.Market == models.Futures && !o.InternalLiquidation {
		pos := d.futuresSettlement.GetPosition(o.AccountID, o.Symbol)
		if err := checkReduceOnly(o, pos); err != nil {
			return nil, nil, http.StatusBadRequest, fmt.Errorf("invalid order: %w", err)
		}
	}

	// Options require per-instrument validation and engine creation. Each
	// option contract (unique strike/expiry/type) gets its own order book so
	// different instruments never share a book.
	if o.Market == models.Options {
		if err := validateAndPrepareOption(ctx, d.pgPool, d.symbolRegistry, d.reg, d.mdSvc, o); err != nil {
			return nil, nil, http.StatusBadRequest, fmt.Errorf("invalid option order: %w", err)
		}
	}

	if err := validateOrderConfig(d.symbolRegistry, o); err != nil {
		return nil, nil, http.StatusBadRequest, fmt.Errorf("invalid order: %w", err)
	}
	if err := d.checker.Check(o); err != nil {
		return nil, nil, http.StatusBadRequest, fmt.Errorf("risk: %w", err)
	}

	// Reservation. Compute the worst-case margin/notional to reserve so that
	// settlement's debit (at actual fill prices) never exceeds the
	// reservation — which would either fail the debit (inconsistent ledger)
	// or leak permanently-locked funds.
	//
	//   Market orders:   best opposite quote (no own price).
	//   Stop-market orders (STOP with no limit Price): same worst-case
	//     estimate as Market — the order rests untriggered but will execute
	//     as a market order the instant it fires, so funds must be reserved
	//     now, not at trigger time (the account could spend the balance
	//     elsewhere in between otherwise).
	//   Futures sell limit: max(limit, bestBid) — margin scales with fill
	//     price, and the worst case for a short is filling at the best bid.
	//   Buy limits / spot: the limit price (a buyer never pays more).
	var resAsset string
	var resAmount decimal.Decimal
	if o.Type == models.Market || (o.Type == models.Stop && !o.Price.IsPositive()) {
		eng, gerr := d.reg.Get(o.Symbol, o.Market)
		if gerr != nil {
			return nil, nil, http.StatusBadRequest, fmt.Errorf("risk: %w", gerr)
		}
		var estPrice decimal.Decimal
		if o.IsBuy() {
			estPrice = eng.BestAsk()
		} else {
			estPrice = eng.BestBid()
		}
		resAsset, resAmount = risk.EstimatedRequired(o, estPrice)
	} else if o.Market == models.Futures && o.Side == models.Sell {
		resAsset, resAmount = risk.RequiredFor(o)
		if eng, gerr := d.reg.Get(o.Symbol, o.Market); gerr == nil {
			if bestBid := eng.BestBid(); bestBid.GreaterThan(o.Price) {
				resAsset, resAmount = risk.EstimatedRequired(o, bestBid)
			}
		}
	} else {
		resAsset, resAmount = risk.RequiredFor(o)
	}

	if resAmount.IsPositive() {
		if err := d.ledger.Reserve(o.AccountID, resAsset, resAmount); err != nil {
			return nil, nil, http.StatusBadRequest, fmt.Errorf("risk: %w", err)
		}
		// Mirror the reservation into Postgres synchronously: if the real
		// wallet doesn't have the funds (or Dex-Backend is unreachable), the
		// in-memory ledger and Postgres must not diverge, so roll back the
		// local reservation and reject the order.
		if d.backend.Enabled() {
			if err := d.backend.Lock(ctx, o.AccountID, resAsset, backendclient.ToRawUnits(resAmount)); err != nil {
				d.ledger.Release(o.AccountID, resAsset, resAmount)
				return nil, nil, http.StatusBadRequest, fmt.Errorf("risk: balance lock failed: %w", err)
			}
		}
	}

	// releaseOverReservation releases the difference between what was
	// reserved (at the worst-case price) and what settlement actually
	// debited (at fill prices), minus the reservation still needed for any
	// resting remainder (at the limit price, since a resting maker fills at
	// its own price). This fixes the price-improvement leak for limit
	// orders and generalises the market-order release to all types.
	releaseOverReservation := func(trades []*models.Trade) {
		filledDebit := risk.FilledDebit(o, trades)
		_, restingReserved := risk.ReleaseAmountFor(o)
		overReserved := resAmount.Sub(filledDebit).Sub(restingReserved)
		if overReserved.IsPositive() {
			d.ledger.Release(o.AccountID, resAsset, overReserved)
			if d.backend.Enabled() {
				backendclient.Async("unlock", func(ctx context.Context) error {
					return d.backend.Unlock(ctx, o.AccountID, resAsset, backendclient.ToRawUnits(overReserved))
				})
			}
		}
	}

	trades, snap, err := d.reg.SubmitSnapshot(o)
	if err != nil {
		// Nothing filled in rejection paths (halt, FOK-not-filled,
		// post-only-cross, invalid order) — release the full reservation.
		if resAmount.IsPositive() {
			d.ledger.Release(o.AccountID, resAsset, resAmount)
			if d.backend.Enabled() {
				backendclient.Async("unlock", func(ctx context.Context) error {
					return d.backend.Unlock(ctx, o.AccountID, resAsset, backendclient.ToRawUnits(resAmount))
				})
			}
		}
		return nil, nil, http.StatusBadRequest, err
	}
	// Release any unused reservation after settlement (price improvement on
	// the filled portion + unfilled remainder for market/IOC orders).
	releaseOverReservation(trades)
	return snap, trades, http.StatusOK, nil
}
