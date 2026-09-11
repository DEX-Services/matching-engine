// Package marketdata provides read-only views into the order book:
// best bid/ask, mid price, spread, and volume-weighted average price (VWAP).
// It reads from the matching engines via snapshots — never from the hot path.
package marketdata

import (
	"fmt"
	"strings"
	"sync"
	"time"

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
	trades     map[string][]recordedTrade // key: symbol+":"+market, oldest first
}

type recordedTrade struct {
	price decimal.Decimal
	qty   decimal.Decimal
	at    time.Time
}

// Summary is the rolling, engine-derived market state used by the trade UI.
// Change and volume cover the trailing 24 hours and are unavailable until the
// engine has seen trades in that window.
type Summary struct {
	Symbol       string
	Market       models.MarketType
	Price        decimal.Decimal
	Change24hPct decimal.Decimal
	Volume24h    decimal.Decimal
	Has24hData   bool
	UpdatedAt    time.Time
}

// SymbolKey identifies one registered book by its symbol and market type.
type SymbolKey struct {
	Symbol string
	Market models.MarketType
}

// Symbols returns every registered (symbol, market) pair — the set of books
// this service can produce market data for. Used by the periodic ticker
// broadcaster to enumerate the frame contents without depending on the
// engine's static market list.
func (s *Service) Symbols() []SymbolKey {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]SymbolKey, 0, len(s.books))
	for k := range s.books {
		parts := strings.SplitN(k, ":", 2)
		if len(parts) == 2 {
			out = append(out, SymbolKey{Symbol: parts[0], Market: models.MarketType(parts[1])})
		}
	}
	return out
}

// NewService creates an empty Service.
func NewService() *Service {
	return &Service{books: make(map[string]BookReader), lastPrices: make(map[string]decimal.Decimal), trades: make(map[string][]recordedTrade)}
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
func (s *Service) RecordTrade(symbol string, market models.MarketType, price, qty decimal.Decimal, at time.Time) {
	if price.IsZero() || price.IsNegative() {
		return
	}
	if at.IsZero() {
		at = time.Now()
	}
	key := symbol + ":" + string(market)
	s.mu.Lock()
	s.lastPrices[key] = price
	cutoff := at.Add(-24 * time.Hour)
	trades := append(s.trades[key], recordedTrade{price: price, qty: qty, at: at})
	firstCurrent := 0
	for firstCurrent < len(trades) && trades[firstCurrent].at.Before(cutoff) {
		firstCurrent++
	}
	s.trades[key] = append([]recordedTrade(nil), trades[firstCurrent:]...)
	s.mu.Unlock()
}

// Summary returns a price and rolling 24h change/volume from real engine
// trades. It never synthesizes a value when there is no liquidity.
//
// Read-only by design: the 1s TICKER broadcaster calls this for every symbol,
// and it previously took the WRITE lock to lazily trim the 24h window on each
// read — contending with matching-side RecordTrade and /depth on the same
// mutex 17x/s. The window is now trimmed exclusively on the write path
// (RecordTrade); here we merely skip trades older than the cutoff when
// accumulating, so a symbol that stops trading simply freezes its last window
// instead of mutating shared state on read. Reading the stored slice without
// holding the lock during accumulation is safe: RecordTrade only ever appends
// at indexes >= the published length or replaces the slice with a fresh copy,
// so elements within an observed snapshot are immutable.
func (s *Service) Summary(symbol string, market models.MarketType) (*Summary, error) {
	ticker, err := s.Ticker(symbol, market)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	s.mu.RLock()
	trades := s.trades[symbol+":"+string(market)]
	s.mu.RUnlock()

	price := ticker.MarkPrice
	summary := &Summary{Symbol: symbol, Market: market, Price: price, UpdatedAt: now}
	// Accumulate over the true trailing-24h window without mutating the
	// stored slice — trimming lives on the RecordTrade write path (see the
	// doc comment above).
	cutoff := now.Add(-24 * time.Hour)
	var opening decimal.Decimal
	for _, trade := range trades {
		if trade.at.Before(cutoff) {
			continue
		}
		if opening.IsZero() {
			opening = trade.price
		}
		summary.Volume24h = summary.Volume24h.Add(trade.price.Mul(trade.qty))
	}
	if opening.IsZero() || price.IsZero() {
		return summary, nil
	}
	summary.Change24hPct = price.Sub(opening).Div(opening).Mul(decimal.NewFromInt(100))
	summary.Has24hData = true
	return summary, nil
}

// SummaryAll returns summaries for every registered book in one pass. Used
// by the batched /market-summary endpoint and delegates to Summary per symbol
// so batch and single-symbol responses are computed identically.
func (s *Service) SummaryAll() []Summary {
	s.mu.RLock()
	keys := make([]SymbolKey, 0, len(s.books))
	for k := range s.books {
		parts := strings.SplitN(k, ":", 2)
		if len(parts) == 2 {
			keys = append(keys, SymbolKey{Symbol: parts[0], Market: models.MarketType(parts[1])})
		}
	}
	s.mu.RUnlock()

	out := make([]Summary, 0, len(keys))
	for _, k := range keys {
		if summ, err := s.Summary(k.Symbol, k.Market); err == nil {
			out = append(out, *summ)
		}
	}
	return out
}

// UnderlyingMark returns the current Spot mark price for symbol (e.g.
// "BTC-BIUSDB"), or false if no Spot market data exists for it yet. Used by
// risk.Checker (via the UnderlyingMarkSource interface) to price options
// writer margin against the real underlying instead of only the strike.
func (s *Service) UnderlyingMark(symbol string) (decimal.Decimal, bool) {
	ticker, err := s.Ticker(symbol, models.Spot)
	if err != nil || !ticker.MarkPrice.IsPositive() {
		return decimal.Decimal{}, false
	}
	return ticker.MarkPrice, true
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
