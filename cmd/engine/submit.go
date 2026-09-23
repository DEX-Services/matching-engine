package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/config"
	"github.com/dex/matching-engine/internal/events"
	"github.com/dex/matching-engine/internal/fixedpoint"
	"github.com/dex/matching-engine/internal/marketdata"
	"github.com/dex/matching-engine/internal/matching"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/dex/matching-engine/internal/settlement"
	"github.com/jackc/pgx/v5/pgxpool"
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
	// bus publishes an EventOrderRejected for every order that fails before
	// reaching reg.SubmitSnapshot (invalid slippageBps, reduce-only
	// violation, option validation, config/tick/lot validation, risk check,
	// reservation/lock failure). These orders have a real ID (assigned by
	// the caller) but previously never reached the matching engine, so no
	// event was ever published and no row was ever persisted for them - a
	// rejected order at this stage was invisible to order history entirely.
	// bus may be nil (e.g. in tests); rejectPipeline simply skips publishing.
	bus *events.Bus

	// mmAcctLocks serializes marketMakerReplaceHandler per account. See
	// mmAccountLocks' doc comment for why. submitDeps is passed BY VALUE
	// throughout this package (rejectPipeline, submitOrderPipeline, every
	// handler constructor) — go vet flags embedding a sync.Mutex directly
	// here for exactly that reason, since each value copy would silently
	// get its own independent lock, defeating the point. A pointer to a
	// dedicated struct is shared correctly across every copy instead.
	mmAcctLocks *mmAccountLocks
}

// mmAccountLocks is a per-account try-lock, one 1-buffered channel per
// account used the same way Dex-Backend's TradeServer.acctLocks is:
// acquiring means sending into it, releasing means receiving.
//
// marketMakerReplaceHandler's sequence — read the account's current orders
// (AllOrders), swap the live book (ReplaceAccountOrders), then update the
// in-memory reservation and the durable Postgres lock — is four separate
// steps, none atomic with each other across two overlapping HTTP requests
// for the SAME account. Two replace calls close together (e.g. a bot
// requoting faster than one round-trip completes) can otherwise interleave
// arbitrarily: both read the same starting AllOrders snapshot, both compute
// their own targets, and their ReplaceAccountOrders/ReplaceReservations/
// ReplaceLocks calls race each other, so the ledger and the durable lock can
// each end up reflecting a DIFFERENT request's targets. A later fill against
// whichever ladder actually ended up live then checks a lock that belongs to
// the other request's targets and fails "insufficient locked ...", halting
// the whole symbol for every account trading it — reproduced live under
// back-to-back MM replace calls with no client-side wait between them.
// Serializing the whole handler body per account closes this: only one
// replace for a given account runs the sequence at a time.
type mmAccountLocks struct {
	mu    sync.Mutex
	locks map[string]chan struct{}
}

func newMMAccountLocks() *mmAccountLocks {
	return &mmAccountLocks{locks: make(map[string]chan struct{})}
}

// acquire waits for account's turn (creating its slot on first use) and
// returns a release func, or ok=false if the wait times out — the account
// already has a replace in flight that isn't completing.
func (l *mmAccountLocks) acquire(ctx context.Context, account string) (release func(), ok bool) {
	l.mu.Lock()
	ch, exists := l.locks[account]
	if !exists {
		ch = make(chan struct{}, 1)
		l.locks[account] = ch
	}
	l.mu.Unlock()

	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, true
	case <-timer.C:
		return nil, false
	case <-ctx.Done():
		return nil, false
	}
}

// worstCaseFillPrice estimates the worst price a market order of the given
// quantity could fill at, by walking the opposite side of the book
// (deepest-first is unnecessary — Depth already returns levels nearest the
// touch first) until the requested quantity is covered. See its call site's
// comment for why top-of-book alone understates this for a multi-level fill.
// If the visible book doesn't have enough depth to cover the full quantity,
// the deepest available level is used for the shortfall — better than
// under-reserving entirely, though a fill that exhausts the whole visible
// book and continues past it (extremely thin liquidity) can still exceed
// this estimate; that residual gap is the same kind of stale-snapshot race
// this change narrows but cannot fully close from outside the matching
// goroutine.
func worstCaseFillPrice(eng *matching.Engine, isBuy bool, qty fixedpoint.Fixed) fixedpoint.Fixed {
	const maxLevels = 50
	bids, asks := eng.Depth(maxLevels)
	levels := asks
	if !isBuy {
		levels = bids
	}
	if len(levels) == 0 {
		return fixedpoint.Zero
	}
	remaining := qty
	worst := levels[0].Price
	for _, lvl := range levels {
		worst = lvl.Price
		if remaining.LessThanOrEqual(lvl.TotalQuantity) {
			break
		}
		remaining = remaining.Sub(lvl.TotalQuantity)
	}
	return worst
}

// rejectPipeline marks o rejected with reason, publishes an
// EventOrderRejected (so the persistence writer records it exactly like any
// other order event), and returns the (nil, nil, status, err) tuple every
// early-return site in submitOrderPipeline needs. Centralized here so every
// pre-book rejection path is persisted identically instead of some being
// remembered and others not.
func rejectPipeline(d submitDeps, o *models.Order, reason string, status int, err error) (*models.Order, []*models.Trade, int, error) {
	o.Status = models.StatusRejected
	o.RejectReason = reason
	if d.bus != nil {
		d.bus.Publish(&models.Event{
			Type:           models.EventOrderRejected,
			Symbol:         o.Symbol,
			Market:         string(o.Market),
			SequenceNumber: d.bus.NextOutOfBandSequence(),
			Order:          o.Copy(),
		})
	}
	return nil, nil, status, err
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
		bps, berr := fixedpoint.FromString(slippageBps)
		if berr != nil || bps.IsNegative() {
			return rejectPipeline(d, o, "invalid slippageBps", http.StatusBadRequest, fmt.Errorf("invalid slippageBps"))
		}
		eng, gerr := d.reg.Get(o.Symbol, o.Market)
		if gerr != nil {
			return rejectPipeline(d, o, gerr.Error(), http.StatusBadRequest, fmt.Errorf("invalid order: %w", gerr))
		}
		var refPrice fixedpoint.Fixed
		if o.IsBuy() {
			refPrice = eng.BestAsk()
		} else {
			refPrice = eng.BestBid()
		}
		if refPrice.IsPositive() {
			factor := bps.Div(fixedpoint.FromInt64(10000))
			if o.IsBuy() {
				o.Price = refPrice.Mul(fixedpoint.FromInt64(1).Add(factor))
			} else {
				o.Price = refPrice.Mul(fixedpoint.FromInt64(1).Sub(factor))
			}
			// The slippage cap becomes an IOC limit internally, so it must
			// obey the same tick-size rule as a user-entered limit. Round the
			// buy cap up and sell cap down so the protection is never narrowed.
			if cfg, cerr := d.symbolRegistry.Get(o.Symbol, o.Market); cerr == nil && cfg.TickSize.IsPositive() {
				steps := o.Price.Div(cfg.TickSize)
				if o.IsBuy() {
					steps = steps.Ceil()
				} else {
					steps = steps.Floor()
				}
				o.Price = steps.Mul(cfg.TickSize)
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
			return rejectPipeline(d, o, err.Error(), http.StatusBadRequest, fmt.Errorf("invalid order: %w", err))
		}
	}

	// Options AND combos are DISABLED (see markets.go's optionsEnabled for the
	// full context — same crypto-only launch decision as forex/commodities/
	// stocks). Rejected here, before any per-instrument lookup/engine-creation
	// work runs (validateAndPrepareOption/validateAndPrepareCombo would
	// otherwise happily create a live order book on demand for any
	// well-formed order — there's no static registration for options the way
	// currentMarkets provides for spot/futures, so this check is the only
	// thing standing between "seeding is off" and "orders still work
	// anyway"). The rejection is deliberately the same generic shape the mux
	// itself gives an unregistered symbol/market — not a "coming soon"
	// message — since that messaging is frontend-only now; the API doesn't
	// say anything about options being on a roadmap.
	if !optionsEnabled && (o.Market == models.Options || o.Market == models.ComboOptions) {
		notRegistered := fmt.Errorf("no engine registered for %s/%s", o.Symbol, o.Market)
		return rejectPipeline(d, o, notRegistered.Error(), http.StatusNotFound, notRegistered)
	}

	// Options require per-instrument validation and engine creation. Each
	// option contract (unique strike/expiry/type) gets its own order book so
	// different instruments never share a book.
	if o.Market == models.Options {
		if err := validateAndPrepareOption(ctx, d.pgPool, d.symbolRegistry, d.reg, d.mdSvc, o); err != nil {
			return rejectPipeline(d, o, err.Error(), http.StatusBadRequest, fmt.Errorf("invalid option order: %w", err))
		}
	}

	// Combo (multi-leg spread) orders similarly need their own dedicated
	// order book — one per unique (buy leg, sell leg) pair — created lazily
	// on first use exactly like individual option contracts above.
	if o.Market == models.ComboOptions {
		if err := validateAndPrepareCombo(ctx, d.pgPool, d.reg, d.mdSvc, o); err != nil {
			return rejectPipeline(d, o, err.Error(), http.StatusBadRequest, fmt.Errorf("invalid combo order: %w", err))
		}
	}

	// Every non-options market (SPOT/FUTURES) must already have a registered
	// order book — checked here, early and generically, rather than only
	// implicitly via whichever later step happens to call d.reg.Get() first
	// (a market order's slippage/reservation lookup, or a limit order not
	// hitting reg.Get() at all until the matching core itself does). Without
	// this, forex/commodity/stock symbols (currently DISABLED — see
	// markets.go's disabledMarkets, not currently in currentMarkets) fell
	// through to whatever generic error happened to fire first: a market
	// order got "no engine registered" wrapped as a 400 risk error, while a
	// funded account's limit order could reach much further into the
	// pipeline before failing. Checking registration up front, before any of
	// that, gives every disabled market — options above, and these below —
	// the exact same clean 404 "no engine registered for X/Y" regardless of
	// order type or account balance, with nothing in the response distinguishing
	// "genuinely never existed" from "disabled by policy".
	if o.Market != models.Options && o.Market != models.ComboOptions {
		if _, gerr := d.reg.Get(o.Symbol, o.Market); gerr != nil {
			return rejectPipeline(d, o, gerr.Error(), http.StatusNotFound, gerr)
		}
	}

	if err := validateOrderConfig(d.symbolRegistry, o); err != nil {
		return rejectPipeline(d, o, err.Error(), http.StatusBadRequest, fmt.Errorf("invalid order: %w", err))
	}
	if err := d.checker.Check(o); err != nil {
		return rejectPipeline(d, o, err.Error(), http.StatusBadRequest, fmt.Errorf("risk: %w", err))
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
	var resAmount fixedpoint.Fixed
	if o.Type == models.Market || (o.Type == models.Stop && !o.Price.IsPositive()) {
		eng, gerr := d.reg.Get(o.Symbol, o.Market)
		if gerr != nil {
			return rejectPipeline(d, o, gerr.Error(), http.StatusBadRequest, fmt.Errorf("risk: %w", gerr))
		}
		// Walk the book for this order's quantity instead of using a single
		// top-of-book price. Top-of-book alone understates the reservation
		// for any order whose quantity exceeds what's resting at that one
		// price level: settlement debits at the ACTUAL fill price(s), and a
		// multi-level market fill settles at a worse average price than
		// best-bid/best-ask, which can exceed a reservation sized off the
		// top level alone and fail settlement's locked-balance check with
		// "insufficient locked ..." — halting the whole symbol. Reproduced
		// live: a market order filling across several levels of a fast-
		// moving book (e.g. during rapid market-maker requoting) settled for
		// more than its top-of-book reservation covered.
		//
		// This narrows but does not eliminate the race — Depth is still a
		// snapshot taken before the order actually reaches the matching
		// goroutine, so the book can still move in that gap. It is a real,
		// large improvement (the dominant real-world cause was understating
		// a multi-level fill's cost, not sub-millisecond top-of-book drift)
		// without the risk of restructuring reservation to be atomic with
		// matching, which touches every order type and is out of scope here.
		estPrice := worstCaseFillPrice(eng, o.IsBuy(), o.Quantity)
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
	// A spot buy pays its execution fee in the quote asset. Reserve the
	// highest possible fee (the order can enter as either maker or taker), not
	// just price × quantity. Without this, the durable settlement transaction
	// correctly refuses to consume more quote than was locked whenever a fee is
	// configured, leaving a matched order unable to settle.
	if o.Market == models.Spot && o.Side == models.Buy && resAmount.IsPositive() {
		if cfg, cerr := d.symbolRegistry.Get(o.Symbol, o.Market); cerr == nil {
			feeRate := cfg.MakerFee
			if cfg.TakerFee.GreaterThan(feeRate) {
				feeRate = cfg.TakerFee
			}
			resAmount = resAmount.Add(resAmount.Mul(feeRate))
		}
	}

	if resAmount.IsPositive() {
		if err := d.ledger.Reserve(o.AccountID, resAsset, resAmount); err != nil {
			return rejectPipeline(d, o, err.Error(), http.StatusBadRequest, fmt.Errorf("risk: %w", err))
		}
		// Mirror the reservation into Postgres synchronously: if the real
		// wallet doesn't have the funds (or Dex-Backend is unreachable), the
		// in-memory ledger and Postgres must not diverge, so roll back the
		// local reservation and reject the order.
		if d.backend.Enabled() {
			if err := d.backend.LockIdempotent(ctx, o.AccountID, resAsset, backendclient.ToRawUnits(resAmount), o.ID); err != nil {
				d.ledger.Release(o.AccountID, resAsset, resAmount)
				return rejectPipeline(d, o, "balance lock failed: "+err.Error(), http.StatusBadRequest, fmt.Errorf("risk: balance lock failed: %w", err))
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
				// Idempotency key generated once here, outside Async's retry
				// loop, and reused across every retry attempt of this one
				// logical unlock (see UnlockIdempotent's doc comment).
				key := o.ID + ":release"
				amount := backendclient.ToRawUnits(overReserved)
				backendclient.Async(backendclient.PendingSync{Op: "unlock", AccountID: o.AccountID, Asset: resAsset, Amount: amount, IdempotencyKey: key}, func(ctx context.Context) error {
					return d.backend.UnlockIdempotent(ctx, o.AccountID, resAsset, amount, key)
				})
			}
		}
	}

	// Re-check and top up the reservation for a market order immediately
	// before it reaches matching. The reservation above was computed from a
	// Depth() snapshot, then went through a synchronous HTTP round-trip to
	// Dex-Backend (LockIdempotent) that can take hundreds of milliseconds
	// against the real cloud-hosted DB this stack uses — during which the
	// book can genuinely move (e.g. a market maker replacing its ladder),
	// so the price this order actually matches at can end up worse than
	// what was reserved for. Root-caused with diagnostic logging after
	// three prior fix attempts (each addressing a plausible but wrong
	// mechanism) failed live re-testing: a real trade was reserved at
	// est.price=2.00 but settled at fill price=2.10 because the market
	// maker's ladder replaced twice during the ~340ms LockIdempotent call.
	//
	// This re-check happens with NO network call in between it and
	// SubmitSnapshot below — both go through the same engine goroutine's
	// serialized request channel — so the remaining staleness window
	// shrinks from "one HTTP round-trip" (hundreds of ms) to "however long
	// it takes another request already queued ahead of this one on the
	// engine's single channel to run" (microseconds). It does not
	// eliminate the window in principle, but it removes the dominant, real
	// cause confirmed above.
	if (o.Type == models.Market || (o.Type == models.Stop && !o.Price.IsPositive())) && resAmount.IsPositive() {
		if eng, gerr := d.reg.Get(o.Symbol, o.Market); gerr == nil {
			recheckPrice := worstCaseFillPrice(eng, o.IsBuy(), o.RemainingQty())
			_, recheckAmount := risk.EstimatedRequired(o, recheckPrice)
			if o.Market == models.Spot && o.Side == models.Buy && recheckAmount.IsPositive() {
				if cfg, cerr := d.symbolRegistry.Get(o.Symbol, o.Market); cerr == nil {
					feeRate := cfg.MakerFee
					if cfg.TakerFee.GreaterThan(feeRate) {
						feeRate = cfg.TakerFee
					}
					recheckAmount = recheckAmount.Add(recheckAmount.Mul(feeRate))
				}
			}
			if shortfall := recheckAmount.Sub(resAmount); shortfall.IsPositive() {
				if err := d.ledger.Reserve(o.AccountID, resAsset, shortfall); err == nil {
					topUpOK := true
					if d.backend.Enabled() {
						if err := d.backend.LockIdempotent(ctx, o.AccountID, resAsset, backendclient.ToRawUnits(shortfall), o.ID+":topup"); err != nil {
							d.ledger.Release(o.AccountID, resAsset, shortfall)
							topUpOK = false
						}
					}
					if topUpOK {
						resAmount = resAmount.Add(shortfall)
					}
				}
				// A failed top-up (insufficient balance, or the backend
				// call itself failing) is deliberately NOT a hard reject
				// here: the original reservation is still valid and the
				// order proceeds on it. Settlement may still fail if the
				// book moved even further in the brief remaining window,
				// which correctly halts the symbol per the existing
				// settlement-failure safety behavior — this re-check
				// closes the dominant real cause without introducing a new
				// way for a legitimately-affordable order to be rejected.
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
				key := o.ID + ":reject-unlock"
				amount := backendclient.ToRawUnits(resAmount)
				backendclient.Async(backendclient.PendingSync{Op: "unlock", AccountID: o.AccountID, Asset: resAsset, Amount: amount, IdempotencyKey: key}, func(ctx context.Context) error {
					return d.backend.UnlockIdempotent(ctx, o.AccountID, resAsset, amount, key)
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
