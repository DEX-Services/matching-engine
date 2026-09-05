package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
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
		ref, err := decimal.NewFromString(req.ReferencePrice)
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

		orders := make([]*models.Order, 0, len(req.Orders))
		targets := map[string]decimal.Decimal{}
		buys, sells := 0, 0
		for _, q := range req.Orders {
			price, perr := decimal.NewFromString(q.Price)
			qty, qerr := decimal.NewFromString(q.Qty)
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
			o := &models.Order{ID: uuid.NewString(), AccountID: req.Account, Symbol: req.Symbol, Market: market, Side: side, Type: models.Limit, Price: price, Quantity: qty, TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now()}
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
		oldTargets := map[string]decimal.Decimal{}
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
				targets[asset] = decimal.Zero
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
					targets[parts[0]] = decimal.Zero
				}
				targets[parts[1]] = decimal.Zero
			}
		}
		toRaw := func(m map[string]decimal.Decimal) map[string]string {
			out := make(map[string]string, len(m))
			for asset, amount := range m {
				out[asset] = backendclient.ToRawUnits(amount)
			}
			return out
		}
		if d.backend.Enabled() {
			if err := d.backend.ReplaceLocks(r.Context(), req.Account, toRaw(targets)); err != nil {
				http.Error(w, "balance replacement failed: "+err.Error(), http.StatusConflict)
				return
			}
		}
		if err := d.ledger.ReplaceReservations(req.Account, targets); err != nil {
			if d.backend.Enabled() {
				_ = d.backend.ReplaceLocks(r.Context(), req.Account, toRaw(oldTargets))
			}
			http.Error(w, "risk: "+err.Error(), http.StatusBadRequest)
			return
		}
		removed, accepted, err := d.reg.ReplaceAccountOrders(req.Symbol, market, req.Account, orders)
		if err != nil {
			_ = d.ledger.ReplaceReservations(req.Account, oldTargets)
			if d.backend.Enabled() {
				_ = d.backend.ReplaceLocks(r.Context(), req.Account, toRaw(oldTargets))
			}
			http.Error(w, fmt.Sprintf("replacement failed: %v", err), http.StatusBadRequest)
			return
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
