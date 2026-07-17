// Package marketdata provides read-only views into the order book:
// best bid/ask, mid price, spread, and volume-weighted average price (VWAP).
// It reads from the matching engines via snapshots — never from the hot path.
package marketdata

import (
	"fmt"
	"sync"

	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/orderbook"
	"github.com/shopspring/decimal"
)

// BookReader is implemented by matching.Engine (subset of its public API).
type BookReader interface {
	BestBid() decimal.Decimal
	BestAsk() decimal.Decimal
	Depth(levels int) (bids, asks []orderbook.LevelSnapshot)
}

// Ticker is a snapshot of current market data for one symbol/market.
type Ticker struct {
	Symbol    string
	Market    models.MarketType
	BestBid   decimal.Decimal
	BestAsk   decimal.Decimal
	MidPrice  decimal.Decimal
	MarkPrice decimal.Decimal // blended mark price for liquidation/funding
	Spread    decimal.Decimal
	BidDepth  decimal.Decimal // total qty on bid side (top 5 levels)
	AskDepth  decimal.Decimal // total qty on ask side (top 5 levels)
}

// Service aggregates market data across all registered symbols.
type Service struct {
	mu         sync.RWMutex
	books      map[string]BookReader      // key: symbol+":"+market
	lastPrices map[string]decimal.Decimal // key: symbol+":"+market
}

// NewService creates an empty Service.
func NewService() *Service {
	return &Service{books: make(map[string]BookReader), lastPrices: make(map[string]decimal.Decimal)}
}

// Register adds a book reader for the given symbol/market.
func (s *Service) Register(symbol string, market models.MarketType, reader BookReader) {
	s.mu.Lock()
	s.books[symbol+":"+string(market)] = reader
	s.mu.Unlock()
}

// RecordTrade records the last trade price for a symbol/market, used to
// compute a manipulation-resistant mark price. Called from the trade-event
// subscriber goroutine in main.go.
func (s *Service) RecordTrade(symbol string, market models.MarketType, price decimal.Decimal) {
	if price.IsZero() || price.IsNegative() {
		return
	}
	s.mu.Lock()
	s.lastPrices[symbol+":"+string(market)] = price
	s.mu.Unlock()
}

// Ticker returns a market data snapshot for symbol/market.
func (s *Service) Ticker(symbol string, market models.MarketType) (*Ticker, error) {
	s.mu.RLock()
	reader, ok := s.books[symbol+":"+string(market)]
	lastPrice := s.lastPrices[symbol+":"+string(market)]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no market data for %s/%s", symbol, market)
	}

	bestBid := reader.BestBid()
	bestAsk := reader.BestAsk()

	var mid, spread decimal.Decimal
	if !bestBid.IsZero() && !bestAsk.IsZero() {
		mid = bestBid.Add(bestAsk).Div(decimal.NewFromInt(2))
		spread = bestAsk.Sub(bestBid)
	}

	bids, asks := reader.Depth(5)
	var bidDepth, askDepth decimal.Decimal
	for _, l := range bids {
		bidDepth = bidDepth.Add(l.TotalQuantity)
	}
	for _, l := range asks {
		askDepth = askDepth.Add(l.TotalQuantity)
	}

	mark := computeMarkPrice(mid, lastPrice)

	return &Ticker{
		Symbol:    symbol,
		Market:    market,
		BestBid:   bestBid,
		BestAsk:   bestAsk,
		MidPrice:  mid,
		MarkPrice: mark,
		Spread:    spread,
		BidDepth:  bidDepth,
		AskDepth:  askDepth,
	}, nil
}

// computeMarkPrice blends the mid-price with the last trade price to reduce
// manipulation risk from a thin book. If both are available, use a simple
// average but cap the deviation from mid to ±1% so a single wash trade
// cannot skew the mark beyond the band. If only one source is available,
// use it directly.
func computeMarkPrice(mid, lastPrice decimal.Decimal) decimal.Decimal {
	if mid.IsZero() {
		return lastPrice
	}
	if lastPrice.IsZero() {
		return mid
	}
	blended := mid.Add(lastPrice).Div(decimal.NewFromInt(2))
	cap := mid.Mul(markDeviationCap)
	upper := mid.Add(cap)
	lower := mid.Sub(cap)
	if blended.GreaterThan(upper) {
		return upper
	}
	if blended.LessThan(lower) {
		return lower
	}
	return blended
}

// markDeviationCap bounds how far the blended mark price may deviate from the
// mid-price, preventing a single manipulated/wash trade from moving the mark
// more than this fraction.
var markDeviationCap = decimal.NewFromFloat(0.01) // 1%

// VWAP computes the volume-weighted average price for a hypothetical order of
// `qty` on the given side, sweeping through the top `maxLevels` price levels.
// Returns an error if there is insufficient liquidity.
func (s *Service) VWAP(symbol string, market models.MarketType, side models.OrderSide, qty decimal.Decimal, maxLevels int) (decimal.Decimal, error) {
	s.mu.RLock()
	reader, ok := s.books[symbol+":"+string(market)]
	s.mu.RUnlock()
	if !ok {
		return decimal.Zero, fmt.Errorf("no market data for %s/%s", symbol, market)
	}

	bids, asks := reader.Depth(maxLevels)
	var levels []orderbook.LevelSnapshot
	if side == models.Buy {
		levels = asks
	} else {
		levels = bids
	}

	remaining := qty
	totalCost := decimal.Decimal{}

	for _, lvl := range levels {
		if remaining.IsZero() {
			break
		}
		take := decimal.Min(remaining, lvl.TotalQuantity)
		totalCost = totalCost.Add(lvl.Price.Mul(take))
		remaining = remaining.Sub(take)
	}

	if remaining.IsPositive() {
		return decimal.Zero, fmt.Errorf("insufficient liquidity: %s unfilled out of %s", remaining, qty)
	}

	filled := qty.Sub(remaining)
	if filled.IsZero() {
		return decimal.Zero, nil
	}
	return totalCost.Div(filled), nil
}
