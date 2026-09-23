package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/fixedpoint"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/google/uuid"
)

// MMReplaceRequest is an internal-only, all-or-nothing market-maker ladder.
// The reference price is retained in the response/log path so callers can
// prove precisely which external index produced the executable quotes.
type MMReplaceRequest struct {
	Account              string       `json:"account"`
	Symbol               string       `json:"symbol"`
	Market               string       `json:"market"`
	ReferencePrice       string       `json:"referencePrice"`
	ReferenceTimestampMs int64        `json:"referenceTimestampMs"`
	Orders               []MMQuoteDTO `json:"orders"`
}

type MMQuoteDTO struct {
	Side  string `json:"side"`
	Price string `json:"price"`
	Qty   string `json:"qty"`
}

type MMReplaceResponse struct {
	Status               string         `json:"status"`
	ReferencePrice       string         `json:"referencePrice"`
	ReferenceTimestampMs int64          `json:"referenceTimestampMs"`
	Orders               []OpenOrderDTO `json:"orders"`
	// Removed carries the FINAL state (Filled/Status) of every order this
	// replace cancelled from the previous ladder. A resting order can fill —
	// fully or partially — in the instant before a replace cancels it; the
	// bot's local tracking only ever sees this replace's response for that
	// order, never a fresh /orders poll (the order is already gone from the
	// live book), so if this were dropped that fill would be permanently
	// invisible to the strategy — real inventory moves, the strategy's
	// belief never catches up, and every later requote asks the engine to
	// lock more than is actually left, forever. See mm.detectFills, which
	// reconciles OpenOrders against a live /orders poll for everything
	// EXCEPT the ladder a replace just tore down — Removed closes that gap.
	Removed []OpenOrderDTO `json:"removed"`
}

func marketMakerReplaceHandler(d submitDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req MMReplaceRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		market := models.MarketType(req.Market)
		ref, err := fixedpoint.FromString(req.ReferencePrice)
		if req.Account == "" || req.Symbol == "" || (len(req.Orders) != 0 && (err != nil || !ref.IsPositive() || len(req.Orders) != 10)) {
			http.Error(w, "account and symbol are required; replacement needs a positive referencePrice and exactly ten orders", http.StatusBadRequest)
			return
		}
		if len(req.Orders) != 0 && (req.ReferenceTimestampMs <= 0 || time.Now().UnixMilli()-req.ReferenceTimestampMs > 10_000) {
			http.Error(w, "stale reference price", http.StatusBadRequest)
			return
		}
		if _, err := d.reg.Get(req.Symbol, market); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// See mmAccountLocks' doc comment: this handler's read-swap-update
		// sequence is not atomic across two overlapping requests for the
		// same account, so only one may run it at a time.
		release, ok := d.mmAcctLocks.acquire(r.Context(), req.Account)
		if !ok {
			http.Error(w, "account has a replace already in flight", http.StatusConflict)
			return
		}
		defer release()

		orders := make([]*models.Order, 0, len(req.Orders))
		targets := map[string]fixedpoint.Fixed{}
		buys, sells := 0, 0
		for _, q := range req.Orders {
			price, perr := fixedpoint.FromString(q.Price)
			qty, qerr := fixedpoint.FromString(q.Qty)
			side := models.OrderSide(q.Side)
			if perr != nil || qerr != nil || !price.IsPositive() || !qty.IsPositive() || (side != models.Buy && side != models.Sell) {
				http.Error(w, "invalid quote", http.StatusBadRequest)
				return
			}
			if side == models.Buy {
				buys++
				if !price.LessThan(ref) {
					http.Error(w, "buy must be below reference", http.StatusBadRequest)
					return
				}
			} else {
				sells++
				if !price.GreaterThan(ref) {
					http.Error(w, "sell must be above reference", http.StatusBadRequest)
					return
				}
			}
			// This whole handler exists only for the platform's own
			// market-maker desks (see the type doc comment above) — every
			// order it creates is, by definition, market-maker-origin, not
			// just accounts happening to match the "mm:" prefix.
			o := &models.Order{ID: uuid.NewString(), AccountID: req.Account, Symbol: req.Symbol, Market: market, Side: side, Type: models.Limit, Price: price, Quantity: qty, TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(), IsMarketMaker: true}
			if err := validateOrderConfig(d.symbolRegistry, o); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			asset, amount := risk.RequiredFor(o)
			targets[asset] = targets[asset].Add(amount)
			orders = append(orders, o)
		}
		if len(req.Orders) != 0 && (buys != 5 || sells != 5) {
			http.Error(w, "ladder must contain five buys and five sells", http.StatusBadRequest)
			return
		}

		// Fetch existing account reservations before replacing them. Dedicated MM
		// wallets have no unrelated orders, so these are their exact old totals.
		eng, _ := d.reg.Get(req.Symbol, market)
		oldTargets := map[string]fixedpoint.Fixed{}
		for _, o := range eng.AllOrders() {
			if o.AccountID != req.Account {
				continue
			}
			asset, amount := risk.ReleaseAmountFor(o)
			oldTargets[asset] = oldTargets[asset].Add(amount)
		}
		// Keep every previously locked asset in the replacement map with a
		// zero target when clearing a ladder; otherwise the durable lock would
		// survive a stop/restart.
		for asset := range oldTargets {
			if _, ok := targets[asset]; !ok {
				targets[asset] = fixedpoint.Zero
			}
		}
		// After an engine restart the live book is empty, so oldTargets has no
		// assets. A batch clear still must explicitly release the durable
		// wallet's locks instead of sending an empty replacement map.
		//
		// Which assets those are depends on the market. A SPOT ladder locks
		// both legs, so both are released. A FUTURES ladder only ever locks
		// margin in the quote currency (see risk.RequiredFor) — and its base
		// is merely the contract's underlying, which for the non-crypto perps
		// is a ticker like "GOLD" or "AAPL.us" that Dex-Backend has no balance
		// column for. Naming it here made the whole replace-locks call fail
		// "unsupported asset", so a market maker on those markets could never
		// finish initializing.
		if len(targets) == 0 {
			parts := strings.SplitN(strings.ToUpper(req.Symbol), "-", 2)
			if len(parts) == 2 {
				if market != models.Futures {
					targets[parts[0]] = fixedpoint.Zero
				}
				targets[parts[1]] = fixedpoint.Zero
			}
		}
		toRaw := func(m map[string]fixedpoint.Fixed) map[string]string {
			out := make(map[string]string, len(m))
			for asset, amount := range m {
				out[asset] = backendclient.ToRawUnits(amount)
			}
			return out
		}
		// Order matters here to close a real race: the live book swap below
		// (Registry.ReplaceAccountOrders) is atomic relative to matching —
		// it runs on the symbol's single engine goroutine, same as Submit —
		// so once it returns, the OLD orders are guaranteed gone and can
		// never be matched again. Previously this handler updated the
		// durable Postgres lock (ReplaceLocks) and the in-memory reservation
		// FIRST, then swapped the book last. That left a window where an old
		// order was still resting and matchable while Postgres's lock had
		// already been overwritten to the NEW target amounts: an incoming
		// order matching that old resting order made settlement check the
		// lock against the wrong (new) target, fail "insufficient locked
		// <asset> for buyer/seller", and auto-halt the entire symbol for
		// every account trading it (see Engine.postProcess's settlement
		// failure path). Swapping the book first eliminates the window
		// entirely: no old order can still be resting by the time either
		// ledger reflects the new targets, so settlement of an old order
		// against a new-target lock can no longer happen.
		removed, accepted, err := d.reg.ReplaceAccountOrders(req.Symbol, market, req.Account, orders)
		if err != nil {
			http.Error(w, fmt.Sprintf("replacement failed: %v", err), http.StatusBadRequest)
			return
		}
		if err := d.ledger.ReplaceReservations(req.Account, targets); err != nil {
			// The book has already been swapped to the new orders, so there
			// is no old book state left to roll back to — put the OLD
			// reservation totals back rather than leaving the account
			// under-reserved relative to its now-live new orders. This
			// mismatch is intentionally accepted: it can only be reached by
			// a target that exceeds the account's real balance, which
			// RequiredFor/the earlier validation above is expected to have
			// already prevented for a well-formed dedicated MM wallet.
			_ = d.ledger.ReplaceReservations(req.Account, oldTargets)
			http.Error(w, "risk: "+err.Error(), http.StatusBadRequest)
			return
		}
		if d.backend.Enabled() {
			if err := d.backend.ReplaceLocks(r.Context(), req.Account, toRaw(targets)); err != nil {
				_ = d.ledger.ReplaceReservations(req.Account, oldTargets)
				http.Error(w, "balance replacement failed: "+err.Error(), http.StatusConflict)
				return
			}
		}
		out := make([]OpenOrderDTO, 0, len(accepted))
		for _, o := range accepted {
			out = append(out, OpenOrderDTO{ID: o.ID, Symbol: o.Symbol, Market: string(o.Market), Side: string(o.Side), Price: o.Price.String(), Qty: o.Quantity.String(), Filled: o.Filled.String(), Status: string(o.Status)})
		}
		removedOut := make([]OpenOrderDTO, 0, len(removed))
		for _, o := range removed {
			removedOut = append(removedOut, OpenOrderDTO{ID: o.ID, Symbol: o.Symbol, Market: string(o.Market), Side: string(o.Side), Price: o.Price.String(), Qty: o.Quantity.String(), Filled: o.Filled.String(), Status: string(o.Status)})
		}
		writeJSON(w, http.StatusOK, MMReplaceResponse{Status: "REPLACED", ReferencePrice: ref.String(), ReferenceTimestampMs: req.ReferenceTimestampMs, Orders: out, Removed: removedOut})
	}
}
