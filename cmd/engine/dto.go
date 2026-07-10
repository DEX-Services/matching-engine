package main

// DepthLevel is one aggregated price level in an order book snapshot.
type DepthLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
	Total string `json:"total"`
}

// DepthResponse is the payload for GET /depth.
type DepthResponse struct {
	Symbol string       `json:"symbol"`
	Market string       `json:"market"`
	Bids   []DepthLevel `json:"bids"`
	Asks   []DepthLevel `json:"asks"`
}

// TradeDTO is one trade in a GET /trades response.
type TradeDTO struct {
	ID        string `json:"id"`
	Symbol    string `json:"symbol"`
	Market    string `json:"market"`
	Price     string `json:"price"`
	Quantity  string `json:"quantity"`
	Side      string `json:"side"` // taker side: BUY or SELL
	Timestamp int64  `json:"timestamp"` // unix millis
}

// TradesResponse is the payload for GET /trades.
type TradesResponse struct {
	Symbol string     `json:"symbol"`
	Market string     `json:"market"`
	Trades []TradeDTO `json:"trades"`
}

// OrderResponse is the payload for POST /order and POST /cancel.
type OrderResponse struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
	Filled  string `json:"filled"`
	Trades  int    `json:"trades"`
}

// BalanceResponse is the payload for GET /admin/balance.
type BalanceResponse struct {
	Account   string `json:"account"`
	Asset     string `json:"asset"`
	Balance   string `json:"balance"`
	Reserved  string `json:"reserved"`
	Available string `json:"available"`
}
