// Package orderbook implements a price-time priority (FIFO) order book
// for a single symbol/market pair.  It is single-goroutine; all
// concurrency is the responsibility of the matching engine layer (Phase 2).
package orderbook

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// Sentinel errors returned by book operations.
var (
	ErrOrderNotFound    = errors.New("order not found")
	ErrInvalidOrder     = errors.New("invalid order")
	ErrBookHalted       = errors.New("order book is halted")
	ErrFOKNotFilled     = errors.New("FOK order could not be fully filled")
	ErrPostOnlyCrossing = errors.New("post-only order would cross the book")
)

// Book is the concrete, single-symbol, single-threaded order book.
// Use New() to construct.
type Book struct {
	symbol string
	market models.MarketType

	// bids: price -> PriceLevel, sorted descending (best bid first)
	bids map[string]*PriceLevel
	// asks: price -> PriceLevel, sorted ascending (best ask first)
	asks map[string]*PriceLevel

	// sorted price keys (maintained on insert/delete)
	bidPrices []decimal.Decimal // descending
	askPrices []decimal.Decimal // ascending

	// orderIndex maps orderID -> *models.Order for O(1) cancel/lookup.
	orderIndex map[string]*models.Order

	// tradeIDFunc generates unique trade IDs.
	tradeIDFunc func() string
}

// New constructs an empty order book for the given symbol and market type.
func New(symbol string, market models.MarketType) *Book {
	return &Book{
		symbol:     symbol,
		market:     market,
		bids:       make(map[string]*PriceLevel),
		asks:       make(map[string]*PriceLevel),
		bidPrices:  nil,
		askPrices:  nil,
		orderIndex: make(map[string]*models.Order),
		tradeIDFunc: func() string { return uuid.NewString() },
	}
}

// ─── Public interface ────────────────────────────────────────────────────────

// Submit processes an incoming order against the book and returns generated trades.
func (b *Book) Submit(order *models.Order) ([]*models.Trade, error) {
	if err := validateOrder(order); err != nil {
		order.Status = models.StatusRejected
		order.UpdatedAt = time.Now()
		return nil, fmt.Errorf("%w: %v", ErrInvalidOrder, err)
	}

	order.UpdatedAt = time.Now()

	switch order.Type {
	case models.Market:
		return b.processMarket(order)
	case models.Limit:
		return b.processLimit(order)
	case models.IOC:
		return b.processIOC(order)
	case models.FOK:
		return b.processFOK(order)
	case models.PostOnly:
		return b.processPostOnly(order)
	case models.Stop:
		// Phase 1: stop orders are accepted and rested; activation logic in Phase 2.
		return b.restOrder(order)
	default:
		order.Status = models.StatusRejected
		return nil, fmt.Errorf("%w: unknown order type %s", ErrInvalidOrder, order.Type)
	}
}

// Cancel removes a resting order by ID.
func (b *Book) Cancel(orderID string) (*models.Order, error) {
	order, ok := b.orderIndex[orderID]
	if !ok {
		return nil, ErrOrderNotFound
	}
	b.removeFromBook(order)
	order.Status = models.StatusCancelled
	order.UpdatedAt = time.Now()
	return order, nil
}

// Modify performs a cancel-and-replace, resetting time priority.
func (b *Book) Modify(orderID string, newPrice, newQty decimal.Decimal) (*models.Order, error) {
	order, err := b.Cancel(orderID)
	if err != nil {
		return nil, err
	}
	order.Price = newPrice
	order.Quantity = newQty
	order.Filled = decimal.Zero
	order.Status = models.StatusPending
	order.UpdatedAt = time.Now()
	_, err = b.Submit(order)
	return order, err
}

// BestBid returns the highest resting bid price, or zero if no bids exist.
func (b *Book) BestBid() decimal.Decimal {
	if len(b.bidPrices) == 0 {
		return decimal.Zero
	}
	return b.bidPrices[0]
}

// BestAsk returns the lowest resting ask price, or zero if no asks exist.
func (b *Book) BestAsk() decimal.Decimal {
	if len(b.askPrices) == 0 {
		return decimal.Zero
	}
	return b.askPrices[0]
}

// Depth returns up to `levels` price levels per side.
func (b *Book) Depth(levels int) (bids, asks []*PriceLevel) {
	for i, p := range b.bidPrices {
		if i >= levels {
			break
		}
		bids = append(bids, b.bids[p.String()])
	}
	for i, p := range b.askPrices {
		if i >= levels {
			break
		}
		asks = append(asks, b.asks[p.String()])
	}
	return
}

// OrderByID returns a resting order without removing it.
func (b *Book) OrderByID(orderID string) (*models.Order, bool) {
	o, ok := b.orderIndex[orderID]
	return o, ok
}

// AllOrders returns every resting order in the book, unordered.
func (b *Book) AllOrders() []*models.Order {
	out := make([]*models.Order, 0, len(b.orderIndex))
	for _, o := range b.orderIndex {
		out = append(out, o)
	}
	return out
}

// ─── Order processing ────────────────────────────────────────────────────────

func (b *Book) processMarket(order *models.Order) ([]*models.Trade, error) {
	trades := b.matchAggressively(order)
	if order.RemainingQty().IsPositive() {
		// Market orders cannot rest; cancel any unfilled remainder.
		order.Status = models.StatusCancelled
	}
	order.UpdatedAt = time.Now()
	return trades, nil
}

func (b *Book) processLimit(order *models.Order) ([]*models.Trade, error) {
	trades := b.matchAggressively(order)
	if order.RemainingQty().IsPositive() {
		// Rest the unfilled remainder.
		if _, err := b.restOrder(order); err != nil {
			return trades, err
		}
	}
	return trades, nil
}

func (b *Book) processIOC(order *models.Order) ([]*models.Trade, error) {
	trades := b.matchAggressively(order)
	if order.RemainingQty().IsPositive() {
		// IOC: cancel remainder immediately; never rests.
		order.Status = models.StatusCancelled
	}
	order.UpdatedAt = time.Now()
	return trades, nil
}

func (b *Book) processFOK(order *models.Order) ([]*models.Trade, error) {
	// Check whether the full quantity can be filled before touching the book.
	if !b.canFillFully(order) {
		order.Status = models.StatusCancelled
		return nil, ErrFOKNotFilled
	}
	trades := b.matchAggressively(order)
	return trades, nil
}

func (b *Book) processPostOnly(order *models.Order) ([]*models.Trade, error) {
	// Post-only orders must not cross the book; reject if they would.
	if b.wouldCross(order) {
		order.Status = models.StatusRejected
		return nil, ErrPostOnlyCrossing
	}
	return b.restOrder(order)
}

// ─── Core matching loop ──────────────────────────────────────────────────────

// matchAggressively walks the opposite side of the book and generates trades
// until the incoming order is fully filled or no more matching levels exist.
func (b *Book) matchAggressively(aggressor *models.Order) []*models.Trade {
	var trades []*models.Trade

	for aggressor.RemainingQty().IsPositive() {
		level := b.bestOppositeLevel(aggressor)
		if level == nil {
			break
		}
		maker := level.Front()
		if maker == nil {
			break
		}

		// Price check: limit aggressors may not execute at a worse price.
		if aggressor.Type == models.Limit || aggressor.Type == models.IOC ||
			aggressor.Type == models.FOK || aggressor.Type == models.PostOnly {
			if !b.priceAcceptable(aggressor, maker.Price) {
				break
			}
		}

		// Determine fill quantity: minimum of remaining on both sides.
		fillQty := decimal.Min(aggressor.RemainingQty(), maker.RemainingQty())
		fillPrice := maker.Price // price-time priority: maker sets the price

		// Apply fill to both orders.
		aggressor.Filled = aggressor.Filled.Add(fillQty)
		maker.Filled = maker.Filled.Add(fillQty)
		now := time.Now()
		aggressor.UpdatedAt = now
		maker.UpdatedAt = now

		// Update statuses.
		b.updateStatus(aggressor)
		b.updateStatus(maker)

		// Build trade record. Attach transient order refs for settlement (Phase 6).
		var buyOrder, sellOrder *models.Order
		if aggressor.IsBuy() {
			buyOrder, sellOrder = aggressor, maker
		} else {
			buyOrder, sellOrder = maker, aggressor
		}
		trade := &models.Trade{
			ID:           b.tradeIDFunc(),
			Symbol:       b.symbol,
			Market:       b.market,
			MakerOrderID: maker.ID,
			TakerOrderID: aggressor.ID,
			MakerSide:    maker.Side,
			Price:        fillPrice,
			Quantity:     fillQty,
			ExecutedAt:   now,
			BuyOrder:     buyOrder,
			SellOrder:    sellOrder,
		}
		trades = append(trades, trade)

		// Remove fully-filled maker from the book.
		if maker.RemainingQty().IsZero() {
			b.removeFromBook(maker)
		}
	}

	if aggressor.RemainingQty().IsZero() {
		aggressor.Status = models.StatusFilled
	}

	return trades
}

// ─── FOK pre-check ───────────────────────────────────────────────────────────

// canFillFully checks whether a FOK order can be entirely matched without
// modifying the book.
func (b *Book) canFillFully(order *models.Order) bool {
	remaining := order.RemainingQty()

	levels := b.oppositeLevels(order)
	for _, level := range levels {
		if order.Type == models.FOK || order.Type == models.Limit {
			if !b.priceAcceptable(order, level.Price) {
				break
			}
		}
		remaining = remaining.Sub(level.TotalQuantity())
		if remaining.IsNegative() || remaining.IsZero() {
			return true
		}
	}
	return false
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func (b *Book) restOrder(order *models.Order) ([]*models.Trade, error) {
	order.Status = models.StatusOpen
	order.UpdatedAt = time.Now()
	b.addToBook(order)
	return nil, nil
}

func (b *Book) addToBook(order *models.Order) {
	key := order.Price.String()
	if order.IsBuy() {
		if _, exists := b.bids[key]; !exists {
			b.bids[key] = NewPriceLevel(order.Price)
			b.insertBidPrice(order.Price)
		}
		b.bids[key].Add(order)
	} else {
		if _, exists := b.asks[key]; !exists {
			b.asks[key] = NewPriceLevel(order.Price)
			b.insertAskPrice(order.Price)
		}
		b.asks[key].Add(order)
	}
	b.orderIndex[order.ID] = order
}

func (b *Book) removeFromBook(order *models.Order) {
	key := order.Price.String()
	if order.IsBuy() {
		if level, ok := b.bids[key]; ok {
			level.Remove(order.ID)
			if level.IsEmpty() {
				delete(b.bids, key)
				b.removeBidPrice(order.Price)
			}
		}
	} else {
		if level, ok := b.asks[key]; ok {
			level.Remove(order.ID)
			if level.IsEmpty() {
				delete(b.asks, key)
				b.removeAskPrice(order.Price)
			}
		}
	}
	delete(b.orderIndex, order.ID)
}

// bestOppositeLevel returns the best price level on the opposite side.
func (b *Book) bestOppositeLevel(order *models.Order) *PriceLevel {
	if order.IsBuy() {
		if len(b.askPrices) == 0 {
			return nil
		}
		return b.asks[b.askPrices[0].String()]
	}
	if len(b.bidPrices) == 0 {
		return nil
	}
	return b.bids[b.bidPrices[0].String()]
}

// oppositeLevels returns all levels on the opposite side in matching order.
func (b *Book) oppositeLevels(order *models.Order) []*PriceLevel {
	var levels []*PriceLevel
	if order.IsBuy() {
		for _, p := range b.askPrices {
			levels = append(levels, b.asks[p.String()])
		}
	} else {
		for _, p := range b.bidPrices {
			levels = append(levels, b.bids[p.String()])
		}
	}
	return levels
}

// priceAcceptable returns true if the maker price is acceptable to the aggressor.
func (b *Book) priceAcceptable(aggressor *models.Order, makerPrice decimal.Decimal) bool {
	if aggressor.IsBuy() {
		return makerPrice.LessThanOrEqual(aggressor.Price)
	}
	return makerPrice.GreaterThanOrEqual(aggressor.Price)
}

// wouldCross returns true if a post-only order would match immediately.
func (b *Book) wouldCross(order *models.Order) bool {
	if order.IsBuy() {
		bestAsk := b.BestAsk()
		return !bestAsk.IsZero() && order.Price.GreaterThanOrEqual(bestAsk)
	}
	bestBid := b.BestBid()
	return !bestBid.IsZero() && order.Price.LessThanOrEqual(bestBid)
}

func (b *Book) updateStatus(order *models.Order) {
	if order.RemainingQty().IsZero() {
		order.Status = models.StatusFilled
	} else if order.Filled.IsPositive() {
		order.Status = models.StatusPartiallyFilled
	}
}

// ─── Price key management ────────────────────────────────────────────────────

func (b *Book) insertBidPrice(price decimal.Decimal) {
	b.bidPrices = append(b.bidPrices, price)
	sort.Slice(b.bidPrices, func(i, j int) bool {
		return b.bidPrices[i].GreaterThan(b.bidPrices[j]) // descending
	})
}

func (b *Book) removeBidPrice(price decimal.Decimal) {
	for i, p := range b.bidPrices {
		if p.Equal(price) {
			b.bidPrices = append(b.bidPrices[:i], b.bidPrices[i+1:]...)
			return
		}
	}
}

func (b *Book) insertAskPrice(price decimal.Decimal) {
	b.askPrices = append(b.askPrices, price)
	sort.Slice(b.askPrices, func(i, j int) bool {
		return b.askPrices[i].LessThan(b.askPrices[j]) // ascending
	})
}

func (b *Book) removeAskPrice(price decimal.Decimal) {
	for i, p := range b.askPrices {
		if p.Equal(price) {
			b.askPrices = append(b.askPrices[:i], b.askPrices[i+1:]...)
			return
		}
	}
}

// ─── Validation ──────────────────────────────────────────────────────────────

func validateOrder(order *models.Order) error {
	if order.ID == "" {
		return errors.New("order ID is required")
	}
	if order.Symbol == "" {
		return errors.New("symbol is required")
	}
	if order.Side != models.Buy && order.Side != models.Sell {
		return fmt.Errorf("invalid side: %s", order.Side)
	}
	if order.Quantity.IsNegative() || order.Quantity.IsZero() {
		return errors.New("quantity must be positive")
	}
	if order.Type == models.Limit || order.Type == models.PostOnly ||
		order.Type == models.IOC || order.Type == models.FOK {
		if order.Price.IsNegative() || order.Price.IsZero() {
			return errors.New("limit price must be positive")
		}
	}
	return nil
}
