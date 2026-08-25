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
	Side      string `json:"side"`      // taker side: BUY or SELL
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

// OrderStatusResponse is the payload for GET /order/status. Found reports
// whether the order is known at all (live book or durable record); resting is
// true only while it is still in the live book. Callers use Filled to account
// the real filled quantity instead of assuming a vanished order filled in full.
type OrderStatusResponse struct {
	OrderID string `json:"orderId"`
	Found   bool   `json:"found"`
	Resting bool   `json:"resting"`
	Status  string `json:"status"`
	Filled  string `json:"filled"`
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
	// GroupID/GroupRole are set only for a take-profit or stop-loss leg
	// belonging to an attached order group (see internal/attached), so the
	// frontend can show its TP/SL lifecycle instead of listing it as an
	// ordinary standalone order. Empty for regular orders.
	GroupID   string `json:"groupId,omitempty"`
	GroupRole string `json:"groupRole,omitempty"` // "TP" | "SL"
}

// OrdersResponse is the payload for GET /orders.
type OrdersResponse struct {
	Orders []OpenOrderDTO `json:"orders"`
}

type OrderHistoryResponse struct {
	Orders     []OrderHistoryDTO `json:"orders"`
	NextCursor string            `json:"nextCursor,omitempty"`
}
type OrderHistoryDTO struct {
	ID        string `json:"id"`
	Symbol    string `json:"symbol"`
	Market    string `json:"market"`
	Side      string `json:"side"`
	Type      string `json:"type"`
	Price     string `json:"price"`
	Quantity  string `json:"quantity"`
	Filled    string `json:"filled"`
	Status    string `json:"status"`
	CreatedAt string `json:"createdAt"`
	UpdatedAt string `json:"updatedAt"`
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

// TickerResponse is the payload for GET /ticker. All price fields are
// order-book-derived (best bid/ask/mid) or settlement-derived (mark price,
// funding) — there is no 24h high/low/volume here, since the engine doesn't
// track rolling windows; the frontend gets those from the price-fetcher's
// Redis-backed index feed instead (see bots' /index/{base} endpoint).
type TickerResponse struct {
	Symbol   string `json:"symbol"`
	Market   string `json:"market"`
	BestBid  string `json:"bestBid"`
	BestAsk  string `json:"bestAsk"`
	MidPrice string `json:"midPrice"`
	// MarkPrice is the blended price liquidation and funding are computed
	// against — order-book mid for spot, exchange-standard blend for
	// futures. Equal to MidPrice for markets where no separate blend exists.
	MarkPrice string `json:"markPrice"`
	// IndexPrice is only populated for FUTURES: the underlying spot market's
	// own mark price, i.e. what funding is computed against. Empty for
	// SPOT/OPTIONS, where there is no separate index.
	IndexPrice string `json:"indexPrice,omitempty"`
	Spread     string `json:"spread"`
	// FundingRatePct is only populated for FUTURES: the rate that WOULD be
	// applied at the next funding settlement given the current mark/index
	// spread (mirrors settlement/funding.go's own computation and cap, but
	// does not apply anything — this is a preview, not a payment). Empty for
	// SPOT/OPTIONS.
	FundingRatePct string `json:"fundingRatePct,omitempty"`
	// MakerFeePct / TakerFeePct come straight from the symbol's real fee
	// config (symbol_configs.maker_fee/taker_fee) rather than being
	// approximated client-side.
	MakerFeePct string `json:"makerFeePct,omitempty"`
	TakerFeePct string `json:"takerFeePct,omitempty"`
	// MaintenanceMarginRatePct is only populated for FUTURES — the real,
	// live MMR from symbol_configs, so the frontend's liquidation-price
	// preview can stop hardcoding it.
	MaintenanceMarginRatePct string `json:"maintenanceMarginRatePct,omitempty"`
}
