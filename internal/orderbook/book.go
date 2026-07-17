// Package orderbook implements a price-time priority (FIFO) order book
// for a single symbol/market pair.  It is single-goroutine; all
// concurrency is the responsibility of the matching engine layer (Phase 2).
package orderbook

import (
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
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

// Submit processes an incoming order against the book and returns generated
// trades plus any resting maker orders cancelled by self-trade prevention.
func (b *Book) Submit(order *models.Order) ([]*models.Trade, []*models.Order, error) {
	if err := validateOrder(order); err != nil {
		order.Status = models.StatusRejected
		order.UpdatedAt = time.Now()
		return nil, nil, fmt.Errorf("%w: %v", ErrInvalidOrder, err)
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
		// PostOnly never crosses the book (rejected if it would), so it never
		// calls matchAggressively and can never produce STP cancellations —
		// nil is correct today. If Phase 2 ever routes PostOnly through
		// matchAggressively, this must return the real cancelled-makers slice.
		trades, err := b.processPostOnly(order)
		return trades, nil, err
	case models.Stop:
		// Phase 1: stop orders are accepted and rested; activation logic in Phase 2.
		// Resting never calls matchAggressively, so nil is correct today. If
		// Phase 2 activation later routes triggered stop orders through
		// matchAggressively, this must return the real cancelled-makers slice.
		trades, err := b.restOrder(order)
		return trades, nil, err
	default:
		order.Status = models.StatusRejected
		return nil, nil, fmt.Errorf("%w: unknown order type %s", ErrInvalidOrder, order.Type)
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
func (b *Book) Modify(orderID string, newPrice, newQty decimal.Decimal) (order *models.Order, trades []*models.Trade, cancelled []*models.Order, err error) {
	order, err = b.Cancel(orderID)
	if err != nil {
		return nil, nil, nil, err
	}
	order.Price = newPrice
	order.Quantity = newQty
	order.Filled = decimal.Zero
	order.Status = models.StatusPending
	order.UpdatedAt = time.Now()
	trades, cancelled, err = b.Submit(order)
	return order, trades, cancelled, err
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

// Depth returns up to `levels` price levels per side as immutable snapshots.
// Snapshots are returned (not live *PriceLevel pointers) so callers reading
// them off the engine goroutine cannot race concurrent book mutation.
func (b *Book) Depth(levels int) (bids, asks []LevelSnapshot) {
	for i, p := range b.bidPrices {
		if i >= levels {
			break
		}
		if lvl := b.bids[priceKey(p)]; lvl != nil {
			bids = append(bids, lvl.Snapshot())
		}
	}
	for i, p := range b.askPrices {
		if i >= levels {
			break
		}
		if lvl := b.asks[priceKey(p)]; lvl != nil {
			asks = append(asks, lvl.Snapshot())
		}
	}
	return
}

// OrderByID returns a copy of a resting order without removing it.
func (b *Book) OrderByID(orderID string) (*models.Order, bool) {
	o, ok := b.orderIndex[orderID]
	return o.Copy(), ok
}

// AllOrders returns a copy of every resting order in the book, unordered.
func (b *Book) AllOrders() []*models.Order {
	out := make([]*models.Order, 0, len(b.orderIndex))
	for _, o := range b.orderIndex {
		out = append(out, o.Copy())
	}
	return out
}

// ─── Order processing ────────────────────────────────────────────────────────

func (b *Book) processMarket(order *models.Order) ([]*models.Trade, []*models.Order, error) {
	trades, cancelled := b.matchAggressively(order)
	if order.RemainingQty().IsPositive() {
		// Market orders cannot rest; cancel any unfilled remainder.
		order.Status = models.StatusCancelled
	}
	order.UpdatedAt = time.Now()
	return trades, cancelled, nil
}

func (b *Book) processLimit(order *models.Order) ([]*models.Trade, []*models.Order, error) {
	trades, cancelled := b.matchAggressively(order)
	if order.RemainingQty().IsPositive() {
		// Rest the unfilled remainder.
		if _, err := b.restOrder(order); err != nil {
			return trades, cancelled, err
		}
	}
	return trades, cancelled, nil
}

func (b *Book) processIOC(order *models.Order) ([]*models.Trade, []*models.Order, error) {
	trades, cancelled := b.matchAggressively(order)
	if order.RemainingQty().IsPositive() {
		// IOC: cancel remainder immediately; never rests.
		order.Status = models.StatusCancelled
	}
	order.UpdatedAt = time.Now()
	return trades, cancelled, nil
}

func (b *Book) processFOK(order *models.Order) ([]*models.Trade, []*models.Order, error) {
	// Check whether the full quantity can be filled before touching the book.
	if !b.canFillFully(order) {
		order.Status = models.StatusCancelled
		return nil, nil, ErrFOKNotFilled
	}
	trades, cancelled := b.matchAggressively(order)
	return trades, cancelled, nil
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
// The second return value lists resting maker orders that were cancelled by
// self-trade prevention (see selfTradeCancelled) rather than matched.
func (b *Book) matchAggressively(aggressor *models.Order) ([]*models.Trade, []*models.Order) {
	var trades []*models.Trade
	var cancelledMakers []*models.Order

	for aggressor.RemainingQty().IsPositive() {
		level := b.bestOppositeLevel(aggressor)
		if level == nil {
			break
		}
		maker := level.Front()
		if maker == nil {
			break
		}

		// Self-trade prevention: an account may not match against its own
		// resting order. Cancel the resting maker and try the next order at
		// this price level (or the next level, once this one empties out).
		if aggressor.AccountID != "" && maker.AccountID == aggressor.AccountID {
			maker.Status = models.StatusCancelled
			maker.UpdatedAt = time.Now()
			b.removeFromBook(maker)
			cancelledMakers = append(cancelledMakers, maker)
			continue
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

	return trades, cancelledMakers
}

// ─── FOK pre-check ───────────────────────────────────────────────────────────

// canFillFully checks whether a FOK order can be entirely matched without
// modifying the book.
func (b *Book) canFillFully(order *models.Order) bool {
	remaining := order.RemainingQty()

	levels := b.oppositeLevels(order)
	for _, level := range levels {
		// FOK is price-limited: stop once the next level crosses the limit.
		if !b.priceAcceptable(order, level.Price) {
			break
		}
		remaining = remaining.Sub(level.TotalQuantityExcludingAccount(order.AccountID))
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
	key := priceKey(order.Price)
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
	key := priceKey(order.Price)
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
		return b.asks[priceKey(b.askPrices[0])]
	}
	if len(b.bidPrices) == 0 {
		return nil
	}
	return b.bids[priceKey(b.bidPrices[0])]
}

// oppositeLevels returns all levels on the opposite side in matching order.
func (b *Book) oppositeLevels(order *models.Order) []*PriceLevel {
	var levels []*PriceLevel
	if order.IsBuy() {
		for _, p := range b.askPrices {
			levels = append(levels, b.asks[priceKey(p)])
		}
	} else {
		for _, p := range b.bidPrices {
			levels = append(levels, b.bids[priceKey(p)])
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

// priceKey returns a canonical map key for a decimal price so that the same
// numeric value is never split across multiple levels regardless of how it
// was constructed. shopspring/decimal preserves the input exponent, so
// "100" (coeff=100, exp=0) and "100.0" (coeff=1000, exp=-1) would otherwise
// produce distinct String() keys. We canonicalize by reducing the
// coefficient/exponent pair to its minimal form.
func priceKey(p decimal.Decimal) string {
	coeff := new(big.Int).Set(p.Coefficient())
	exp := p.Exponent()
	// Absorb positive exponents into the coefficient (e.g. 1e2 -> 100e0).
	for exp > 0 {
		coeff.Mul(coeff, big.NewInt(10))
		exp--
	}
	// Strip trailing zeros while the exponent is negative (e.g. 1000e-1 -> 100e0).
	ten := big.NewInt(10)
	for exp < 0 {
		q, r := new(big.Int).DivMod(coeff, ten, new(big.Int))
		if r.Sign() != 0 {
			break
		}
		coeff = q
		exp++
	}
	return coeff.String() + "e" + strconv.FormatInt(int64(exp), 10)
}

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
