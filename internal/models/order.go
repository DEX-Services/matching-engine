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

// MarketType distinguishes Spot, Futures, and Options books.
type MarketType string

const (
	Spot    MarketType = "SPOT"
	Futures MarketType = "FUTURES"
	Options MarketType = "OPTIONS"
)

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

	// OptionType, StrikePrice, and Expiry apply to options only; ignored by spot/futures.
	OptionType  string          `json:"optionType,omitempty"` // "CALL" | "PUT"
	StrikePrice decimal.Decimal `json:"strikePrice,omitempty"`
	Expiry      time.Time       `json:"expiry,omitempty"`

	// InternalLiquidation marks an order forced by the liquidation engine;
	// such orders bypass pre-trade risk checks (the position is already open).
	InternalLiquidation bool `json:"-"`
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
