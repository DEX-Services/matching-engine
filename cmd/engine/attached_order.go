package main

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dex/matching-engine/internal/attached"
	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// spotPositionSizer implements attached.PositionSizer for SPOT groups
// (added 2026-09-16 for SPOT TP/SL support). Unlike futures
// (*settlement.FuturesSettlement, a real margined position object), spot
// has no "position" — the closest equivalent to "current exposure" is
// simply how much of the base asset the account still holds, which shrinks
// via an ordinary manual sell exactly as validly as a futures position
// shrinks via a partial close. Reports Balance (available+reserved), not
// just Available, so an already-locked TP/SL reservation on that same
// holding doesn't make its own protection look like it's shrinking.
type spotPositionSizer struct {
	ledger *risk.Ledger
}

// CurrentSize returns the account's current total (available + reserved)
// balance of symbol's base asset. symbol is the engine's internal
// "BASE-QUOTE" form (e.g. "BI2X-BI2XUSD"); the base asset is everything
// before the first "-", same split used throughout this package for spot
// symbols (see risk.assetFor's default case).
func (s *spotPositionSizer) CurrentSize(accountID, symbol string) decimal.Decimal {
	base, _, ok := strings.Cut(symbol, "-")
	if !ok || base == "" {
		return decimal.Zero
	}
	return s.ledger.Balance(accountID, base)
}

// AttachedOrderResponse is the payload for POST /attached-order.
type AttachedOrderResponse struct {
	OrderID      string `json:"orderId"`
	Status       string `json:"status"`
	Filled       string `json:"filled"`
	Trades       int    `json:"trades"`
	GroupID      string `json:"groupId,omitempty"`
	TakeProfitID string `json:"takeProfitId,omitempty"`
	StopLossID   string `json:"stopLossId,omitempty"`
}

// legSpec is the query-string shape for one protective leg, prefixed "tp" or
// "sl" (e.g. tpPrice, slStopPrice) to keep the existing query-param style
// used by /order instead of introducing a JSON-body request shape.
type legSpec struct {
	price     decimal.Decimal
	stopPrice decimal.Decimal
	present   bool
}

func parseLegSpec(q map[string][]string, prefix string) legSpec {
	get := func(name string) string {
		if v, ok := q[prefix+name]; ok && len(v) > 0 {
			return v[0]
		}
		return ""
	}
	priceStr, stopStr := get("Price"), get("StopPrice")
	if priceStr == "" && stopStr == "" {
		return legSpec{}
	}
	price, _ := decimal.NewFromString(priceStr)
	stop, _ := decimal.NewFromString(stopStr)
	return legSpec{price: price, stopPrice: stop, present: true}
}

// attachedOrderHandler builds POST /attached-order: submits the entry order
// through the exact same pipeline as POST /order, then - only if and once
// the entry actually filled - activates and places the TP/SL legs as real
// reduce-only resting orders tagged with a shared GroupID, via
// internal/attached.Execute. This is what makes the group atomic and
// fill-aware: an entry that rests unfilled or is rejected leaves no
// protective orders behind, and protection is sized to the real fill, not
// the requested quantity.
func attachedOrderHandler(d submitDeps, attachedReg *attached.Registry) http.HandlerFunc {
	return requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		side := models.Buy
		if q.Get("side") == "SELL" {
			side = models.Sell
		}
		orderType := models.Limit
		switch q.Get("type") {
		case "MARKET":
			orderType = models.Market
		case "IOC":
			orderType = models.IOC
		case "FOK":
			orderType = models.FOK
		case "POST_ONLY":
			orderType = models.PostOnly
		case "STOP":
			orderType = models.Stop
		}
		price, _ := decimal.NewFromString(q.Get("price"))
		qty, _ := decimal.NewFromString(q.Get("qty"))
		leverage, _ := strconv.Atoi(q.Get("leverage"))
		reduceOnly := q.Get("reduceOnly") == "true"

		entry := &models.Order{
			ID: uuid.NewString(), AccountID: q.Get("account"),
			Symbol: q.Get("symbol"), Market: models.MarketType(q.Get("market")),
			Side: side, Type: orderType, Price: price, Quantity: qty,
			TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
			Leverage: leverage, MarginMode: q.Get("marginMode"), ReduceOnly: reduceOnly,
			IsMarketMaker: models.IsMarketMakerAccount(q.Get("account")),
		}

		// SPOT TP/SL added 2026-09-16 (previously FUTURES-only — see git
		// history/session notes for why: it was never that spot couldn't
		// support this, just that OCO reservation needed its own design,
		// solved by the shared reserveGroup step below, and there was no
		// "exposure changed" signal for a spot holding, solved by
		// spotPositionSizer in main.go).
		if entry.Market != models.Futures && entry.Market != models.Spot {
			http.Error(w, "invalid order: attached TP/SL is only supported for futures and spot", http.StatusBadRequest)
			return
		}
		// SPOT is scoped to BUY entries only (protect a purchase by planning
		// to sell it later — the normal, real-world meaning of "TP/SL" on
		// every spot exchange). A SELL entry's legs would be BUYs needing
		// QUOTE-asset reservations priced at each leg's own limit/stop price
		// — the TP and SL legs could legitimately need different amounts,
		// which breaks the single-shared-lock design reserveGroup relies on
		// (see its doc comment). Not supported for now rather than guessed
		// at with a placeholder price that could under- or over-reserve.
		if entry.Market == models.Spot && side != models.Buy {
			http.Error(w, "invalid order: attached TP/SL for spot is only supported on BUY entries", http.StatusBadRequest)
			return
		}

		tp := parseLegSpec(q, "tp")
		sl := parseLegSpec(q, "sl")
		if !tp.present && !sl.present {
			http.Error(w, "invalid order: at least one of tpPrice or slStopPrice is required", http.StatusBadRequest)
			return
		}

		group := attached.Group{
			ID:            uuid.NewString(),
			ParentOrderID: entry.ID,
			Market:        entry.Market,
		}
		if tp.present {
			group.TakeProfit = &attached.Leg{ID: uuid.NewString(), LimitPrice: tp.price}
		}
		if sl.present {
			group.StopLoss = &attached.Leg{ID: uuid.NewString(), StopPrice: sl.stopPrice}
		}

		var (
			entrySnap *models.Order
			trades    []*models.Trade
			status    = http.StatusOK
			submitErr error
		)
		submit := func(o *models.Order) (*models.Order, error) {
			snap, tr, st, err := submitOrderPipeline(r.Context(), d, o, q.Get("slippageBps"))
			trades, status, submitErr = tr, st, err
			if err != nil {
				return nil, err
			}
			entrySnap = snap
			return snap, nil
		}
		submitLeg := func(o *models.Order) error {
			_, _, _, err := submitOrderPipeline(r.Context(), d, o, "")
			return err
		}
		// SPOT only: take the single shared reservation backing both legs —
		// see attached.ReserveGroup's doc comment for why a spot TP/SL pair
		// cannot let each leg reserve independently the way futures does.
		// Scoped to BUY entries only (checked above), so the closing side is
		// always SELL and risk.RequiredFor's spot-SELL case (notionalFor's
		// default branch) reserves exactly ProtectedQty of the base asset,
		// independent of price — no placeholder price needed. Reserves
		// through the exact same ledger.Reserve + backend.Lock sequence
		// submitOrderPipeline itself uses, so this shared lock is durably
		// mirrored in Postgres exactly like every other reservation on this
		// platform, not a special, less-safe path.
		reserveGroup := func(g attached.Group) error {
			synthetic := &models.Order{
				AccountID: g.AccountID, Symbol: g.Symbol, Market: g.Market,
				Side: models.Sell, Type: models.Limit, Quantity: g.ProtectedQty,
			}
			resAsset, resAmount := risk.RequiredFor(synthetic)
			if !resAmount.IsPositive() {
				return nil
			}
			if err := d.ledger.Reserve(g.AccountID, resAsset, resAmount); err != nil {
				return err
			}
			if d.backend.Enabled() {
				if err := d.backend.LockIdempotent(context.Background(), g.AccountID, resAsset, backendclient.ToRawUnits(resAmount), g.ID); err != nil {
					d.ledger.Release(g.AccountID, resAsset, resAmount)
					return err
				}
			}
			return nil
		}
		// releaseGroup undoes reserveGroup's lock — called by attached.Execute
		// ONLY when every leg failed to place after a successful reservation
		// (see Execute's doc comment on the incident this fixes: without this,
		// funds locked by reserveGroup had no leg left anywhere to eventually
		// release them, leaving them stuck with no user-facing recovery).
		// Recomputes the exact same asset/amount reserveGroup used, via the
		// same risk.RequiredFor call on the same synthetic order shape, so
		// this can never accidentally release a different amount than what
		// was actually locked.
		releaseGroup := func(g attached.Group) error {
			synthetic := &models.Order{
				AccountID: g.AccountID, Symbol: g.Symbol, Market: g.Market,
				Side: models.Sell, Type: models.Limit, Quantity: g.ProtectedQty,
			}
			resAsset, resAmount := risk.RequiredFor(synthetic)
			if !resAmount.IsPositive() {
				return nil
			}
			d.ledger.Release(g.AccountID, resAsset, resAmount)
			if d.backend.Enabled() {
				if err := d.backend.UnlockIdempotent(context.Background(), g.AccountID, resAsset, backendclient.ToRawUnits(resAmount), g.ID+":release"); err != nil {
					return err
				}
			}
			return nil
		}

		result, activatedGroup, err := attached.Execute(attachedReg, attached.Command{Group: group, Entry: entry}, submit, submitLeg, reserveGroup, releaseGroup)
		if err != nil {
			if submitErr != nil {
				http.Error(w, submitErr.Error(), status)
				return
			}
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_ = result // entry itself is the authoritative record; entrySnap below carries its post-submit state

		resp := AttachedOrderResponse{OrderID: entry.ID}
		filled := entry.Filled
		respStatus := entry.Status
		if entrySnap != nil {
			filled = entrySnap.Filled
			respStatus = entrySnap.Status
		}
		resp.Status = string(respStatus)
		resp.Filled = filled.String()
		resp.Trades = len(trades)
		if activatedGroup != nil {
			resp.GroupID = activatedGroup.ID
			if activatedGroup.TakeProfit != nil {
				resp.TakeProfitID = activatedGroup.TakeProfit.ID
			}
			if activatedGroup.StopLoss != nil {
				resp.StopLossID = activatedGroup.StopLoss.ID
			}
		}
		writeJSON(w, http.StatusOK, resp)
	})
}
