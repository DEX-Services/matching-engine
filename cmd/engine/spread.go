package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// SpreadRequest is the JSON body for POST /spread. A variable-length leg
// list doesn't fit cleanly into query-string params (unlike /order's fixed
// field set), so this endpoint takes a JSON body instead — the one HTTP
// shape difference from /order in this file; everything downstream
// (submitOrderPipeline, the matching core, settlement) is unchanged.
type SpreadRequest struct {
	Account     string            `json:"account"`
	Legs        []models.ComboLeg `json:"legs"`  // at least 2; symbol = option instrument symbol, ratio = signed contracts-per-combo-unit
	Side        string            `json:"side"`  // "BUY" | "SELL"
	Type        string            `json:"type"`  // "LIMIT" (default) | "IOC" | "FOK" | "MARKET"
	Price       string            `json:"price"` // required unless type=MARKET; the combo's NET price (positive=debit, negative=credit)
	Qty         string            `json:"qty"`
	SlippageBps string            `json:"slippageBps,omitempty"`
}

// SpreadResponse is the payload for POST /spread.
type SpreadResponse struct {
	OrderID string `json:"orderId"`
	Symbol  string `json:"symbol"` // the combo's own deterministic instrument symbol
	Status  string `json:"status"`
	Filled  string `json:"filled"`
	Trades  int    `json:"trades"`
}

// spreadHandler builds POST /spread: submits ONE order against the combo's
// own native order book (models.ComboOptions) — the same way Deribit, CME,
// and Binance Options implement combo/strategy trading. Any N-leg structure
// (vertical, butterfly, iron condor, ratio spread — anything expressible as
// a list of {symbol, signed ratio} legs, all sharing the same underlying
// and expiry) is registered as its own tradable instrument the first time
// this endpoint sees a given leg set (see getOrCreateComboInstrument), with
// its own dedicated order book alongside every other instrument's. From
// then on, combo orders match against OTHER combo orders on that exact
// book through the normal matching core — price-time priority, IOC/limit/
// market semantics, all of it unchanged and reused, not reimplemented for
// combos.
//
// This is what makes execution genuinely atomic, unlike the endpoint's
// earlier design (independently-submitted option orders coordinated by
// client-side unwind logic): a combo trade IS one match on one book. All
// legs settle together inside settlement.ComboSettlement.Settle, called
// synchronously from the SAME matching-goroutine critical section every
// other trade's settlement runs in (see matching.Engine.run) — there is no
// window where some legs exist and others don't, because there is only
// ever one trade object for the matching core to know about in the first
// place, regardless of leg count.
//
// side=BUY goes long the combo (long every positive-ratio leg, short every
// negative-ratio leg) at the given net price (positive = debit, negative =
// credit). side=SELL does the exact reverse on every leg (closes or
// reverses an existing long combo position). price is the LIMIT price for
// the combo order; qty is combo units (each leg's actual traded quantity is
// qty * |leg.Ratio|). type controls resting behavior exactly like any other
// order (LIMIT rests if it doesn't fully cross; IOC/FOK do not rest; MARKET
// has no price limit).
func spreadHandler(d submitDeps) http.HandlerFunc {
	return requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req SpreadRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		if req.Account == "" {
			http.Error(w, "account is required", http.StatusBadRequest)
			return
		}
		if len(req.Legs) < 2 {
			http.Error(w, "legs must contain at least 2 entries", http.StatusBadRequest)
			return
		}
		qty, qerr := decimal.NewFromString(req.Qty)
		if qerr != nil || !qty.IsPositive() {
			http.Error(w, "qty must be a positive number", http.StatusBadRequest)
			return
		}
		side := models.Buy
		if req.Side == "SELL" {
			side = models.Sell
		}
		orderType := models.Limit
		switch req.Type {
		case "IOC":
			orderType = models.IOC
		case "FOK":
			orderType = models.FOK
		case "MARKET":
			orderType = models.Market
		}
		price, perr := decimal.NewFromString(req.Price)
		if orderType != models.Market && (perr != nil || !price.IsPositive()) {
			http.Error(w, "price must be a positive net limit price (a positive number; use side to express long/short, not price sign)", http.StatusBadRequest)
			return
		}

		order := &models.Order{
			ID: uuid.NewString(), AccountID: req.Account, Market: models.ComboOptions,
			Side: side, Type: orderType, Price: price, Quantity: qty,
			TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
			ComboLegs: req.Legs,
		}

		snap, trades, status, err := submitOrderPipeline(r.Context(), d, order, req.SlippageBps)
		if err != nil {
			http.Error(w, err.Error(), status)
			return
		}

		resp := SpreadResponse{OrderID: order.ID, Symbol: order.Symbol, Trades: len(trades)}
		if snap != nil {
			resp.Status = string(snap.Status)
			resp.Filled = snap.Filled.String()
		} else {
			resp.Status = string(order.Status)
			resp.Filled = order.Filled.String()
		}
		writeJSON(w, http.StatusOK, resp)
	})
}
