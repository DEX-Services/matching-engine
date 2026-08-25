package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/dex/matching-engine/internal/attached"
	"github.com/dex/matching-engine/internal/models"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

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
		}

		if entry.Market != models.Futures {
			http.Error(w, "invalid order: attached TP/SL is only supported for futures", http.StatusBadRequest)
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

		result, activatedGroup, err := attached.Execute(attachedReg, attached.Command{Group: group, Entry: entry}, submit, submitLeg)
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
