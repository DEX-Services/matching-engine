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

// OpenOrderDTO is one resting order in a GET /orders response.
type OpenOrderDTO struct {
	ID     string `json:"id"`
	Symbol string `json:"symbol"`
	Market string `json:"market"`
	Side   string `json:"side"`
	Price  string `json:"price"`
	Qty    string `json:"qty"`
	Filled string `json:"filled"`
	Status string `json:"status"`
}

// OrdersResponse is the payload for GET /orders.
type OrdersResponse struct {
	Orders []OpenOrderDTO `json:"orders"`
}

// BalanceResponse is the payload for GET /admin/balance.
type BalanceResponse struct {
	Account   string `json:"account"`
	Asset     string `json:"asset"`
	Balance   string `json:"balance"`
	Reserved  string `json:"reserved"`
	Available string `json:"available"`
}

// FuturesPositionDTO is one open futures position in a GET /positions response.
type FuturesPositionDTO struct {
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	Size          string `json:"size"`
	EntryPrice    string `json:"entryPrice"`
	MarkPrice     string `json:"markPrice"`
	Margin        string `json:"margin"`
	Leverage      int    `json:"leverage"`
	UnrealizedPnl string `json:"unrealizedPnl"`
}

// OptionsPositionDTO is one open options position in a GET /positions response.
type OptionsPositionDTO struct {
	Symbol      string `json:"symbol"`
	OptionType  string `json:"optionType"`
	StrikePrice string `json:"strikePrice"`
	Expiry      string `json:"expiry"`
	Size        string `json:"size"`
	Premium     string `json:"premium"`
}

// PositionsResponse is the payload for GET /positions.
type PositionsResponse struct {
	Futures []FuturesPositionDTO `json:"futures"`
	Options []OptionsPositionDTO `json:"options"`
}

// OptionChainEntry is one contract's live quote/greeks in a GET /option-chain response.
type OptionChainEntry struct {
	Symbol     string  `json:"symbol"`
	OptionType string  `json:"optionType"`
	Strike     string  `json:"strike"`
	Expiry     string  `json:"expiry"`
	Bid        string  `json:"bid"`
	Ask        string  `json:"ask"`
	Mid        string  `json:"mid"`
	IV         float64 `json:"iv"`
	Delta      float64 `json:"delta"`
	Gamma      float64 `json:"gamma"`
	Theta      float64 `json:"theta"`
	Vega       float64 `json:"vega"`
	Rho        float64 `json:"rho"`
}

// OptionChainResponse is the payload for GET /option-chain.
type OptionChainResponse struct {
	Underlying string             `json:"underlying"`
	Spot       string             `json:"spot"`
	Chain      []OptionChainEntry `json:"chain"`
}
