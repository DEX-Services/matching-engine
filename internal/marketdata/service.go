// Package marketdata provides read-only views into the order book:
// best bid/ask, mid price, spread, and volume-weighted average price (VWAP).
// It reads from the matching engines via snapshots — never from the hot path.
package marketdata

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dex/matching-engine/internal/fixedpoint"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/orderbook"
)

// BookReader is implemented by matching.Engine (subset of its public API).
type BookReader interface {
	BestBid() fixedpoint.Fixed
	BestAsk() fixedpoint.Fixed
	Depth(levels int) (bids, asks []orderbook.LevelSnapshot)
}

// Ticker is a snapshot of current market data for one symbol/market.
type Ticker struct {
	Symbol    string
	Market    models.MarketType
	BestBid   fixedpoint.Fixed
	BestAsk   fixedpoint.Fixed
	MidPrice  fixedpoint.Fixed
	MarkPrice fixedpoint.Fixed // blended mark price for liquidation/funding
	Spread    fixedpoint.Fixed
	BidDepth  fixedpoint.Fixed // total qty on bid side (top 5 levels)
	AskDepth  fixedpoint.Fixed // total qty on ask side (top 5 levels)
	// LastTradeAt is the timestamp of the most recent trade recorded for
	// this symbol/market, or the zero Time if none has ever traded. Used by
	// liquidation/funding to refuse to act on a mark price derived from a
	// market that has gone quiet — see MarkPriceFresh.
	LastTradeAt time.Time
}

// Service aggregates market data across all registered symbols.
type Service struct {
	mu          sync.RWMutex
	books       map[string]BookReader       // key: symbol+":"+market
	lastPrices  map[string]fixedpoint.Fixed // key: symbol+":"+market
	lastTradeAt map[string]time.Time        // key: symbol+":"+market
	trades      map[string]*tradeRing       // key: symbol+":"+market
}

type recordedTrade struct {
	price fixedpoint.Fixed
	qty   fixedpoint.Fixed
	at    time.Time
}

// tradeRing is a fixed-capacity circular buffer of recordedTrade, doubling
// its backing array when full rather than ever reallocating on every
// RecordTrade call. See PERFORMANCE-CODE-REVIEW-FINDINGS.md item #6:
// RecordTrade previously rebuilt the entire live 24h trade slice
// (`append([]recordedTrade(nil), trades[firstCurrent:]...)`) on EVERY
// single trade, an O(window size) copy per trade regardless of how many
// trades were actually being trimmed. A ring buffer instead evicts expired
// entries by advancing head — O(1) amortized per trade, no allocation once
// warmed up to its steady-state size.
type tradeRing struct {
	buf   []recordedTrade
	head  int // index of the oldest live entry
	count int // number of live entries
}

const tradeRingInitialCap = 64

func newTradeRing() *tradeRing {
	return &tradeRing{buf: make([]recordedTrade, tradeRingInitialCap)}
}

// pushEvictingBefore appends t and evicts every entry older than cutoff
// (from the head, since entries are always inserted in non-decreasing `at`
// order by RecordTrade's caller — the matching engine's own event stream).
func (r *tradeRing) pushEvictingBefore(t recordedTrade, cutoff time.Time) {
	for r.count > 0 && r.buf[r.head].at.Before(cutoff) {
		r.head = (r.head + 1) % len(r.buf)
		r.count--
	}
	if r.count == len(r.buf) {
		r.grow()
	}
	tail := (r.head + r.count) % len(r.buf)
	r.buf[tail] = t
	r.count++
}

func (r *tradeRing) grow() {
	newBuf := make([]recordedTrade, len(r.buf)*2)
	for i := 0; i < r.count; i++ {
		newBuf[i] = r.buf[(r.head+i)%len(r.buf)]
	}
	r.buf = newBuf
	r.head = 0
}

// forEach calls fn for every live entry, oldest first. fn must not retain
// the recordedTrade beyond the call (it is a copy, not a pointer into the
// ring, so retaining is actually safe — but callers should treat it as a
// point-in-time read regardless, consistent with the old slice-based API).
func (r *tradeRing) forEach(fn func(recordedTrade)) {
	for i := 0; i < r.count; i++ {
		fn(r.buf[(r.head+i)%len(r.buf)])
	}
}

// Summary is the rolling, engine-derived market state used by the trade UI.
// Change and volume cover the trailing 24 hours and are unavailable until the
// engine has seen trades in that window.
type Summary struct {
	Symbol       string
	Market       models.MarketType
	Price        fixedpoint.Fixed
	Change24hPct fixedpoint.Fixed
	Volume24h    fixedpoint.Fixed
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
	return &Service{
		books:       make(map[string]BookReader),
		lastPrices:  make(map[string]fixedpoint.Fixed),
		lastTradeAt: make(map[string]time.Time),
		trades:      make(map[string]*tradeRing),
	}
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
func (s *Service) RecordTrade(symbol string, market models.MarketType, price, qty fixedpoint.Fixed, at time.Time) {
	if price.IsZero() || price.IsNegative() {
		return
	}
	if at.IsZero() {
		at = time.Now()
	}
	key := symbol + ":" + string(market)
	s.mu.Lock()
	s.lastPrices[key] = price
	s.lastTradeAt[key] = at
	ring, ok := s.trades[key]
	if !ok {
		ring = newTradeRing()
		s.trades[key] = ring
	}
	cutoff := at.Add(-24 * time.Hour)
	ring.pushEvictingBefore(recordedTrade{price: price, qty: qty, at: at}, cutoff)
	s.mu.Unlock()
}

// Summary returns a price and rolling 24h change/volume from real engine
// trades. It never synthesizes a value when there is no liquidity.
//
// The 1s TICKER broadcaster calls this for every symbol. Trimming the 24h
// window lives entirely on the write path (RecordTrade's pushEvictingBefore);
// here we merely skip trades older than the cutoff when accumulating, so a
// symbol that stops trading simply freezes its last window instead of
// mutating shared state on read. Unlike the old slice-based storage (which
// only ever appended or swapped in a fresh copy, safe to read after
// releasing the lock), tradeRing overwrites its backing array in place
// (grow, tail-slot reuse) — so accumulation must happen while still holding
// the read lock, not after copying out a stale slice header.
func (s *Service) Summary(symbol string, market models.MarketType) (*Summary, error) {
	ticker, err := s.Ticker(symbol, market)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	price := ticker.MarkPrice
	summary := &Summary{Symbol: symbol, Market: market, Price: price, UpdatedAt: now}
	cutoff := now.Add(-24 * time.Hour)
	var opening fixedpoint.Fixed

	s.mu.RLock()
	if ring := s.trades[symbol+":"+string(market)]; ring != nil {
		ring.forEach(func(trade recordedTrade) {
			if trade.at.Before(cutoff) {
				return
			}
			if opening.IsZero() {
				opening = trade.price
			}
			summary.Volume24h = summary.Volume24h.Add(trade.price.Mul(trade.qty))
		})
	}
	s.mu.RUnlock()

	if opening.IsZero() || price.IsZero() {
		return summary, nil
	}
	summary.Change24hPct = price.Sub(opening).Div(opening).Mul(fixedpoint.FromInt64(100))
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
// "BTC-BI2XUSD"), or false if no Spot market data exists for it yet. Used by
// risk.Checker (via the UnderlyingMarkSource interface) to price options
// writer margin against the real underlying instead of only the strike.
func (s *Service) UnderlyingMark(symbol string) (fixedpoint.Fixed, bool) {
	ticker, err := s.Ticker(symbol, models.Spot)
	if err != nil || !ticker.MarkPrice.IsPositive() {
		return fixedpoint.Zero, false
	}
	return ticker.MarkPrice, true
}

// Ticker returns a market data snapshot for symbol/market.
func (s *Service) Ticker(symbol string, market models.MarketType) (*Ticker, error) {
	key := symbol + ":" + string(market)
	s.mu.RLock()
	reader, ok := s.books[key]
	lastPrice := s.lastPrices[key]
	lastTradeAt := s.lastTradeAt[key]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no market data for %s/%s", symbol, market)
	}

	bestBid := reader.BestBid()
	bestAsk := reader.BestAsk()

	var mid, spread fixedpoint.Fixed
	if !bestBid.IsZero() && !bestAsk.IsZero() {
		mid = bestBid.Add(bestAsk).Div(fixedpoint.FromInt64(2))
		spread = bestAsk.Sub(bestBid)
	}

	bids, asks := reader.Depth(5)
	var bidDepth, askDepth fixedpoint.Fixed
	for _, l := range bids {
		bidDepth = bidDepth.Add(l.TotalQuantity)
	}
	for _, l := range asks {
		askDepth = askDepth.Add(l.TotalQuantity)
	}

	mark := computeMarkPrice(mid, lastPrice)

	return &Ticker{
		Symbol:      symbol,
		Market:      market,
		BestBid:     bestBid,
		BestAsk:     bestAsk,
		MidPrice:    mid,
		MarkPrice:   mark,
		Spread:      spread,
		BidDepth:    bidDepth,
		AskDepth:    askDepth,
		LastTradeAt: lastTradeAt,
	}, nil
}

// MarkPriceFresh reports whether t's mark price is safe to act on for a
// liquidation or funding decision: it requires that this market has
// actually traded within maxAge. A market with resting orders but no recent
// trades (thin/quiet/possibly stuck) can have a MidPrice that no real trade
// has confirmed in a long time — acting on it anyway is exactly the failure
// mode this exists to prevent (previously nothing checked this at all; the
// engine had zero staleness guard anywhere in liquidation/funding, unlike
// bots and prediction-service, which already refuse to act on a stale
// external index price the same way).
func (t *Ticker) MarkPriceFresh(maxAge time.Duration) bool {
	if t.LastTradeAt.IsZero() {
		return false
	}
	return time.Since(t.LastTradeAt) <= maxAge
}

// computeMarkPrice blends the mid-price with the last trade price to reduce
// manipulation risk from a thin book. If both are available, use a simple
// average but cap the deviation from mid to ±1% so a single wash trade
// cannot skew the mark beyond the band. If only one source is available,
// use it directly.
func computeMarkPrice(mid, lastPrice fixedpoint.Fixed) fixedpoint.Fixed {
	if mid.IsZero() {
		return lastPrice
	}
	if lastPrice.IsZero() {
		return mid
	}
	blended := mid.Add(lastPrice).Div(fixedpoint.FromInt64(2))
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
var markDeviationCap = fixedpoint.MustFromString("0.01") // 1%

// VWAP computes the volume-weighted average price for a hypothetical order of
// `qty` on the given side, sweeping through the top `maxLevels` price levels.
// Returns an error if there is insufficient liquidity.
func (s *Service) VWAP(symbol string, market models.MarketType, side models.OrderSide, qty fixedpoint.Fixed, maxLevels int) (fixedpoint.Fixed, error) {
	s.mu.RLock()
	reader, ok := s.books[symbol+":"+string(market)]
	s.mu.RUnlock()
	if !ok {
		return fixedpoint.Zero, fmt.Errorf("no market data for %s/%s", symbol, market)
	}

	bids, asks := reader.Depth(maxLevels)
	var levels []orderbook.LevelSnapshot
	if side == models.Buy {
		levels = asks
	} else {
		levels = bids
	}

	remaining := qty
	totalCost := fixedpoint.Zero

	for _, lvl := range levels {
		if remaining.IsZero() {
			break
		}
		take := fixedpoint.Min(remaining, lvl.TotalQuantity)
		totalCost = totalCost.Add(lvl.Price.Mul(take))
		remaining = remaining.Sub(take)
	}

	if remaining.IsPositive() {
		return fixedpoint.Zero, fmt.Errorf("insufficient liquidity: %s unfilled out of %s", remaining, qty)
	}

	filled := qty.Sub(remaining)
	if filled.IsZero() {
		return fixedpoint.Zero, nil
	}
	return totalCost.Div(filled), nil
}
