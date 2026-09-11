package models

import (
	"time"

	"github.com/shopspring/decimal"
)

// OrderSide represents buy or sell.
type OrderSide string

const (
	Buy  OrderSide = "BUY"
	Sell OrderSide = "SELL"
)

// OrderType defines order execution behaviour.
type OrderType string

const (
	Limit    OrderType = "LIMIT"
	Market   OrderType = "MARKET"
	Stop     OrderType = "STOP"
	IOC      OrderType = "IOC" // Immediate-Or-Cancel
	FOK      OrderType = "FOK" // Fill-Or-Kill
	PostOnly OrderType = "POST_ONLY"
)

// TimeInForce controls how long an order stays active.
type TimeInForce string

const (
	GTC TimeInForce = "GTC" // Good-Till-Cancelled
	GTD TimeInForce = "GTD" // Good-Till-Date
	GFD TimeInForce = "GFD" // Good-For-Day
)

// Margin mode constants for futures positions.
const (
	MarginIsolated = "ISOLATED"
	MarginCross    = "CROSS"
)

// OrderStatus tracks the lifecycle of an order.
type OrderStatus string

const (
	StatusPending         OrderStatus = "PENDING"
	StatusOpen            OrderStatus = "OPEN"
	StatusPartiallyFilled OrderStatus = "PARTIALLY_FILLED"
	StatusFilled          OrderStatus = "FILLED"
	StatusCancelled       OrderStatus = "CANCELLED"
	StatusRejected        OrderStatus = "REJECTED"
	StatusExpired         OrderStatus = "EXPIRED"
)

// MarketType distinguishes Spot, Futures, Options, and ComboOptions books.
type MarketType string

const (
	Spot    MarketType = "SPOT"
	Futures MarketType = "FUTURES"
	Options MarketType = "OPTIONS"
	// ComboOptions is a native multi-leg (currently: 2-leg vertical spread)
	// order book, matching how real options exchanges (Deribit, CME, Binance
	// Options combos) implement spread trading: the combo is registered as
	// its own instrument with its own order book, quoted and matched as ONE
	// unit at a single net price — not two independently-submitted orders
	// coordinated by a client. A combo fill is atomic by construction (it is
	// a single trade on a single book) and fans out into two linked leg
	// trades at settlement time; see settlement.ComboSettlement.
	ComboOptions MarketType = "COMBO_OPTIONS"
)

// ComboLeg is one leg of a multi-leg options combo order (see
// Order.ComboLegs). Ratio's sign is relative to the combo's own BUY side:
// positive = long this leg when the combo order is a BUY, negative = short.
// Magnitude is how many contracts of this leg per 1 unit of the combo (1
// for a plain vertical/iron condor leg, 2 for a butterfly's short middle
// strike).
type ComboLeg struct {
	Symbol string `json:"symbol"`
	Ratio  int    `json:"ratio"`
}

// Order is the core struct shared by all asset classes.
// Futures/Options-specific fields live in their settlement handlers, not here.
type Order struct {
	ID            string      `json:"id"`
	ClientOrderID string      `json:"clientOrderId,omitempty"`
	Symbol        string      `json:"symbol"`
	Market        MarketType  `json:"market"`
	Side          OrderSide   `json:"side"`
	Type          OrderType   `json:"type"`
	TimeInForce   TimeInForce `json:"timeInForce"`

	Price    decimal.Decimal `json:"price"` // zero for market orders
	Quantity decimal.Decimal `json:"quantity"`
	Filled   decimal.Decimal `json:"filled"`

	Status    OrderStatus `json:"status"`
	CreatedAt time.Time   `json:"createdAt"`
	UpdatedAt time.Time   `json:"updatedAt"`

	// Stop price for stop orders; zero otherwise.
	StopPrice decimal.Decimal `json:"stopPrice,omitempty"`

	// ReduceOnly applies to futures; ignored by spot settlement.
	ReduceOnly bool `json:"reduceOnly,omitempty"`

	// AccountID is required for risk/balance checks (Phase 3+).
	AccountID string `json:"accountId"`

	// Leverage and MarginMode apply to futures only; ignored by spot/options.
	Leverage   int    `json:"leverage,omitempty"`
	MarginMode string `json:"marginMode,omitempty"` // "ISOLATED" | "CROSS"

	// QuoteCurrency is the settlement currency for the order (e.g. "BIUSDB").
	// For spot/futures it is derived from the symbol (BASE-QUOTE). For options
	// it is set by the order handler from the instrument's underlying config,
	// because option instrument symbols (BASE-STRIKE-EXPIRY-TYPE) do not
	// encode the quote currency. When empty, risk.assetFor falls back to
	// parsing the symbol.
	QuoteCurrency string `json:"quoteCurrency,omitempty"`

	// OptionType, StrikePrice, and Expiry apply to options only; ignored by spot/futures.
	OptionType  string          `json:"optionType,omitempty"` // "CALL" | "PUT"
	StrikePrice decimal.Decimal `json:"strikePrice,omitempty"`
	Expiry      time.Time       `json:"expiry,omitempty"`

	// ComboLegs applies to ComboOptions orders only: the N option
	// instrument symbols making up this combo, each with a signed ratio —
	// positive means a BUY order on the combo book goes LONG that leg,
	// negative means it goes SHORT (a SELL order on the combo does the
	// exact reverse on every leg, i.e. closing/reversing the whole
	// position). 2 legs with ratios {+1,-1} is a vertical spread; 3 legs
	// {+1,-2,+1} is a butterfly; 4 legs {+1,-1,-1,+1} is an iron condor;
	// any other combination of ratios is a generalized ratio spread. Set
	// once at combo-instrument creation (see cmd/engine's combo.go) and
	// copied onto every order against that combo symbol so settlement can
	// resolve every leg without a Postgres round-trip per leg per trade.
	ComboLegs []ComboLeg `json:"comboLegs,omitempty"`

	// InternalLiquidation marks an order forced by the liquidation engine;
	// such orders bypass pre-trade risk checks (the position is already open).
	InternalLiquidation bool `json:"-"`

	// GroupID links a take-profit or stop-loss order back to the attached
	// order group it protects (see internal/attached). Empty for ordinary
	// orders. GroupRole distinguishes which leg this order is, so the OCO
	// listener knows which sibling to cancel when this one fills/triggers.
	GroupID   string `json:"groupId,omitempty"`
	GroupRole string `json:"groupRole,omitempty"` // "TP" | "SL"

	// RejectReason explains why the order ended in StatusRejected or
	// StatusCancelled (self-trade prevention, FOK not fillable, post-only
	// crossing, halted symbol, IOC/market unfilled remainder, explicit user
	// cancel, etc). Empty for orders that are still open/filled normally.
	RejectReason string `json:"rejectReason,omitempty"`
}

// RemainingQty returns the unfilled portion of the order.
func (o *Order) RemainingQty() decimal.Decimal {
	return o.Quantity.Sub(o.Filled)
}

// IsTerminal returns true when the order can no longer be matched.
func (o *Order) IsTerminal() bool {
	switch o.Status {
	case StatusFilled, StatusCancelled, StatusRejected, StatusExpired:
		return true
	}
	return false
}

// IsBuy is a convenience helper.
func (o *Order) IsBuy() bool { return o.Side == Buy }

// Copy returns a shallow copy safe for handing to callers outside the
// engine goroutine (all fields are value types, so shallow == deep here).
func (o *Order) Copy() *Order {
	if o == nil {
		return nil
	}
	c := *o
	return &c
}
