package main

import (
	"net/http"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

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
// and Binance Options implement combo/strategy trading. A vertical spread
// (buy one option, sell another — same underlying/expiry/type, different
// strikes) is registered as its own tradable instrument the first time this
// endpoint sees a given (buySymbol, sellSymbol) pair (see
// getOrCreateComboInstrument), with its own dedicated order book alongside
// every other instrument's. From then on, combo orders match against OTHER
// combo orders on that exact book through the normal matching core — price-
// time priority, IOC/limit/market semantics, all of it unchanged and
// reused, not reimplemented for combos.
//
// This is what makes execution genuinely atomic, unlike the earlier version
// of this endpoint (two independently-submitted option orders coordinated
// by client-side unwind logic): a combo trade IS one match on one book. Both
// legs settle together inside settlement.ComboSettlement.Settle, called
// synchronously from the SAME matching-goroutine critical section every
// other trade's settlement runs in (see matching.Engine.run) — there is no
// window where one leg exists without the other, because there is only ever
// one trade object for the matching core to know about in the first place.
//
// side=BUY goes long the spread (buy buySymbol, sell sellSymbol) at the
// given net price (positive = debit, negative = credit). side=SELL does the
// reverse (closes or reverses an existing long combo position). price is
// the LIMIT price for the combo order; qty is spreads (both legs move by
// the same amount, since this engine only supports 1:1 verticals). type
// controls resting behavior exactly like any other order (LIMIT rests if it
// doesn't fully cross; IOC/FOK do not rest; MARKET has no price limit).
func spreadHandler(d submitDeps) http.HandlerFunc {
	return requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		account := q.Get("account")
		buySymbol := q.Get("buySymbol")
		sellSymbol := q.Get("sellSymbol")
		qty, qerr := decimal.NewFromString(q.Get("qty"))
		price, perr := decimal.NewFromString(q.Get("price"))
		if account == "" || buySymbol == "" || sellSymbol == "" {
			http.Error(w, "account, buySymbol, and sellSymbol are required", http.StatusBadRequest)
			return
		}
		if qerr != nil || !qty.IsPositive() {
			http.Error(w, "qty must be a positive number", http.StatusBadRequest)
			return
		}
		side := models.Buy
		if q.Get("side") == "SELL" {
			side = models.Sell
		}
		orderType := models.Limit
		switch q.Get("type") {
		case "IOC":
			orderType = models.IOC
		case "FOK":
			orderType = models.FOK
		case "MARKET":
			orderType = models.Market
		}
		if orderType != models.Market && (perr != nil || !price.IsPositive()) {
			http.Error(w, "price must be a positive net limit price (a positive number; use side to express long/short, not price sign)", http.StatusBadRequest)
			return
		}

		order := &models.Order{
			ID: uuid.NewString(), AccountID: account, Market: models.ComboOptions,
			Side: side, Type: orderType, Price: price, Quantity: qty,
			TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
			ComboBuySymbol: buySymbol, ComboSellSymbol: sellSymbol,
		}

		snap, trades, status, err := submitOrderPipeline(r.Context(), d, order, q.Get("slippageBps"))
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
