// Package orderbook implements a price-time priority (FIFO) order book
// for a single symbol/market pair.  It is single-goroutine; all
// concurrency is the responsibility of the matching engine layer (Phase 2).
package orderbook

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/dex/matching-engine/internal/fixedpoint"
	"github.com/dex/matching-engine/internal/models"
	"github.com/google/uuid"
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
	bids map[fixedpoint.Fixed]*PriceLevel
	// asks: price -> PriceLevel, sorted ascending (best ask first)
	asks map[fixedpoint.Fixed]*PriceLevel

	// sorted price keys (maintained on insert/delete). fixedpoint.Fixed is a
	// plain int64, so this slice is directly comparable/sortable with no
	// canonicalization step — unlike decimal.Decimal, where "100" and
	// "100.0" could previously hash to different priceKey() strings unless
	// explicitly canonicalized (see this file's git history), an int64
	// value has exactly one representation for a given price, always.
	bidPrices []fixedpoint.Fixed // descending
	askPrices []fixedpoint.Fixed // ascending

	// orderIndex maps orderID -> *models.Order for O(1) cancel/lookup.
	orderIndex map[string]*models.Order

	// stopOrders holds untriggered stop orders keyed by ID, for O(1)
	// cancel-by-ID/OrderByID/AllOrders. They are NOT part of the matchable
	// book: they activate (convert to market/limit) when the last trade
	// price (or, via CheckMarkPriceTriggers, the mark price) crosses their
	// StopPrice.
	stopOrders map[string]*models.Order

	// buyStopLevels/sellStopLevels index the SAME orders as stopOrders, by
	// StopPrice, using the identical PriceLevel FIFO structure the main
	// book already uses — reused here rather than inventing a second data
	// structure. This is what makes processStopTriggersAt's activation
	// sweep O(log n + number fired) instead of the previous O(n) linear
	// scan (repeated for every stop that fires, making a sweep that
	// activates k stops out of n resting O(n*k), i.e. up to O(n²) — see
	// PERFORMANCE-CODE-REVIEW-FINDINGS.md item #5, confirmed against this
	// exact code before the rewrite).
	//
	// Direction matters for which end of the sorted array is "next to
	// trigger": a BUY stop fires once price rises to/above its StopPrice,
	// so the buy stops with the LOWEST StopPrice are closest to triggering
	// as price rises — buyStopPrices is kept ascending, and the sweep
	// always looks at index 0. A SELL stop fires once price falls to/below
	// its StopPrice, so the sell stops with the HIGHEST StopPrice are
	// closest to triggering as price falls — sellStopPrices is kept
	// descending, sweep also looks at index 0. This mirrors bidPrices/
	// askPrices' own descending/ascending convention for the exact same
	// "best/next is always at index 0" reason.
	buyStopLevels  map[fixedpoint.Fixed]*PriceLevel
	sellStopLevels map[fixedpoint.Fixed]*PriceLevel
	buyStopPrices  []fixedpoint.Fixed // ascending
	sellStopPrices []fixedpoint.Fixed // descending

	// lastTradePrice is the price of the most recent fill, used to evaluate
	// stop triggers. Zero until the first trade.
	lastTradePrice fixedpoint.Fixed

	// activated accumulates stop orders triggered during the current Submit;
	// drained by the engine via DrainActivated for event publication.
	activated []*models.Order

	// tradeIDFunc generates unique trade IDs.
	tradeIDFunc func() string

	// version increments on every mutation that could change what Depth()
	// returns for either side: adding/removing a resting order (addToBook/
	// removeFromBook) AND partial fills, which mutate a resting maker's
	// Filled in place without removing it from the book (see
	// matchAggressively). depthCache/depthCacheVersion below let Depth()
	// skip rebuilding the bids/asks snapshot slices when nothing has
	// changed since the last call — see PERFORMANCE-CODE-REVIEW-FINDINGS.md
	// item #4: the 1s TICKER broadcaster calls Depth() across every symbol
	// every second regardless of whether that symbol has traded, so between
	// trades this made every tick a full deep-copy of both sides for
	// nothing.
	version uint64

	// depthCache holds the full (unsliced) bids/asks snapshot as of
	// depthCacheVersion. Depth(levels) truncates from this shared cache
	// instead of rebuilding when version == depthCacheVersion. Invalid
	// (never populated) when depthCacheVersion == 0 and version == 0 is
	// indistinguishable from "just built" — resolved by depthCacheValid.
	depthCacheValid   bool
	depthCacheVersion uint64
	depthCacheBids    []LevelSnapshot
	depthCacheAsks    []LevelSnapshot
}

// New constructs an empty order book for the given symbol and market type.
func New(symbol string, market models.MarketType) *Book {
	return &Book{
		symbol:         symbol,
		market:         market,
		bids:           make(map[fixedpoint.Fixed]*PriceLevel),
		asks:           make(map[fixedpoint.Fixed]*PriceLevel),
		bidPrices:      nil,
		askPrices:      nil,
		orderIndex:     make(map[string]*models.Order),
		stopOrders:     make(map[string]*models.Order),
		buyStopLevels:  make(map[fixedpoint.Fixed]*PriceLevel),
		sellStopLevels: make(map[fixedpoint.Fixed]*PriceLevel),
		tradeIDFunc:    func() string { return uuid.NewString() },
	}
}

// ─── Public interface ────────────────────────────────────────────────────────

// Submit processes an incoming order against the book and returns generated
// trades plus any resting maker orders cancelled by self-trade prevention.
// Trades executed by the incoming order may trigger resting stop orders;
// their executions are appended to the returned slices, and the activated
// stop orders themselves are retrievable via DrainActivated.
func (b *Book) Submit(order *models.Order) ([]*models.Trade, []*models.Order, error) {
	trades, cancelled, err := b.submitCore(order)
	if err != nil {
		return trades, cancelled, err
	}
	stopTrades, stopCancelled := b.processStopTriggers()
	return append(trades, stopTrades...), append(cancelled, stopCancelled...), nil
}

func (b *Book) submitCore(order *models.Order) ([]*models.Trade, []*models.Order, error) {
	if err := validateOrder(order); err != nil {
		order.Status = models.StatusRejected
		order.RejectReason = err.Error()
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
		return nil, nil, b.restStopOrder(order)
	default:
		order.Status = models.StatusRejected
		order.RejectReason = fmt.Sprintf("unknown order type %s", order.Type)
		return nil, nil, fmt.Errorf("%w: unknown order type %s", ErrInvalidOrder, order.Type)
	}
}

// Cancel removes a resting order by ID.
func (b *Book) Cancel(orderID string) (*models.Order, error) {
	if stop, ok := b.stopOrders[orderID]; ok {
		delete(b.stopOrders, orderID)
		b.removeFromStopIndex(stop)
		stop.Status = models.StatusCancelled
		stop.RejectReason = "cancelled by user"
		stop.UpdatedAt = time.Now()
		return stop, nil
	}
	order, ok := b.orderIndex[orderID]
	if !ok {
		return nil, ErrOrderNotFound
	}
	b.removeFromBook(order)
	order.Status = models.StatusCancelled
	order.RejectReason = "cancelled by user"
	order.UpdatedAt = time.Now()
	return order, nil
}

// Modify performs a cancel-and-replace, resetting time priority.
func (b *Book) Modify(orderID string, newPrice, newQty fixedpoint.Fixed) (order *models.Order, trades []*models.Trade, cancelled []*models.Order, err error) {
	order, err = b.Cancel(orderID)
	if err != nil {
		return nil, nil, nil, err
	}
	order.Price = newPrice
	order.Quantity = newQty
	order.Filled = fixedpoint.Zero
	order.Status = models.StatusPending
	order.UpdatedAt = time.Now()
	trades, cancelled, err = b.Submit(order)
	return order, trades, cancelled, err
}

// BestBid returns the highest resting bid price, or zero if no bids exist.
func (b *Book) BestBid() fixedpoint.Fixed {
	if len(b.bidPrices) == 0 {
		return fixedpoint.Zero
	}
	return b.bidPrices[0]
}

// BestAsk returns the lowest resting ask price, or zero if no asks exist.
func (b *Book) BestAsk() fixedpoint.Fixed {
	if len(b.askPrices) == 0 {
		return fixedpoint.Zero
	}
	return b.askPrices[0]
}

// Depth returns up to `levels` price levels per side as immutable snapshots.
// Snapshots are returned (not live *PriceLevel pointers) so callers reading
// them off the engine goroutine cannot race concurrent book mutation.
//
// Rebuilding the full-depth snapshot is skipped whenever nothing has
// mutated the book since the last call (see the version/depthCache fields'
// doc comment) — repeated ticks between trades (the common case for most
// symbols most of the time) become a slice-truncate instead of a full
// PriceLevel walk across every level on both sides.
func (b *Book) Depth(levels int) (bids, asks []LevelSnapshot) {
	if !b.depthCacheValid || b.depthCacheVersion != b.version {
		b.depthCacheBids = b.depthCacheBids[:0]
		for _, p := range b.bidPrices {
			if lvl := b.bids[p]; lvl != nil {
				b.depthCacheBids = append(b.depthCacheBids, lvl.Snapshot())
			}
		}
		b.depthCacheAsks = b.depthCacheAsks[:0]
		for _, p := range b.askPrices {
			if lvl := b.asks[p]; lvl != nil {
				b.depthCacheAsks = append(b.depthCacheAsks, lvl.Snapshot())
			}
		}
		b.depthCacheVersion = b.version
		b.depthCacheValid = true
	}
	if levels <= 0 {
		return nil, nil
	}
	if levels < len(b.depthCacheBids) {
		bids = append(bids, b.depthCacheBids[:levels]...)
	} else {
		bids = append(bids, b.depthCacheBids...)
	}
	if levels < len(b.depthCacheAsks) {
		asks = append(asks, b.depthCacheAsks[:levels]...)
	} else {
		asks = append(asks, b.depthCacheAsks...)
	}
	return
}

// OrderByID returns a copy of a resting order without removing it, checking
// both the matchable book (orderIndex) and untriggered stop orders
// (stopOrders — see Book's own field doc comment: these are deliberately
// NOT part of the matchable book, but they are very much real, live,
// cancellable orders).
//
// Fixed 2026-09-16: this only ever checked orderIndex, so any untriggered
// stop order (a manually-placed STOP, or — the case that surfaced this live —
// a Stop-Loss leg from internal/attached, which BuildLegOrder deliberately
// builds as a STOP order so it activates as a market order once triggered)
// was invisible here. cmd/engine's /cancel handler calls this FIRST as an
// existence/ownership pre-check before ever calling Book.Cancel (which
// itself already correctly handles stopOrders, and always has) — so a
// perfectly real, resting, correctly-reserved stop order could never be
// cancelled through the normal user-facing action at all: the pre-check
// always 404'd "order not found" before Book.Cancel was ever reached, even
// though cancelling it would have worked fine. Checking both maps here
// closes that gap without touching Book.Cancel's already-correct logic.
func (b *Book) OrderByID(orderID string) (*models.Order, bool) {
	if o, ok := b.orderIndex[orderID]; ok {
		return o.Copy(), true
	}
	if o, ok := b.stopOrders[orderID]; ok {
		return o.Copy(), true
	}
	return nil, false
}

// AllOrders returns a copy of every resting order in the book, unordered,
// including untriggered stop orders.
func (b *Book) AllOrders() []*models.Order {
	out := make([]*models.Order, 0, len(b.orderIndex)+len(b.stopOrders))
	for _, o := range b.orderIndex {
		out = append(out, o.Copy())
	}
	for _, o := range b.stopOrders {
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
		order.RejectReason = "insufficient opposing liquidity to fill remainder"
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
		order.RejectReason = "IOC remainder cancelled: insufficient opposing liquidity"
	}
	order.UpdatedAt = time.Now()
	return trades, cancelled, nil
}

func (b *Book) processFOK(order *models.Order) ([]*models.Trade, []*models.Order, error) {
	// Check whether the full quantity can be filled before touching the book.
	if !b.canFillFully(order) {
		order.Status = models.StatusCancelled
		order.RejectReason = ErrFOKNotFilled.Error()
		return nil, nil, ErrFOKNotFilled
	}
	trades, cancelled := b.matchAggressively(order)
	return trades, cancelled, nil
}

func (b *Book) processPostOnly(order *models.Order) ([]*models.Trade, error) {
	// Post-only orders must not cross the book; reject if they would.
	if b.wouldCross(order) {
		order.Status = models.StatusRejected
		order.RejectReason = ErrPostOnlyCrossing.Error()
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

		// Price check: limit aggressors may not execute at a worse price.
		if aggressor.Type == models.Limit || aggressor.Type == models.IOC ||
			aggressor.Type == models.FOK || aggressor.Type == models.PostOnly {
			if !b.priceAcceptable(aggressor, maker.Price) {
				break
			}
		}

		// Self-trade prevention only applies after this incoming order is
		// actually price-crossing. Applying it first incorrectly cancelled
		// non-crossing quotes from the same market maker, leaving one-sided
		// books even when bids and asks were safely separated.
		if aggressor.AccountID != "" && maker.AccountID == aggressor.AccountID {
			maker.Status = models.StatusCancelled
			maker.RejectReason = "cancelled: self-trade prevention"
			maker.UpdatedAt = time.Now()
			b.removeFromBook(maker)
			cancelledMakers = append(cancelledMakers, maker)
			continue
		}

		// Determine fill quantity: minimum of remaining on both sides.
		fillQty := fixedpoint.Min(aggressor.RemainingQty(), maker.RemainingQty())
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
		b.lastTradePrice = fillPrice

		// Remove fully-filled maker from the book. A partial fill leaves the
		// maker resting (not added/removed), but still changes what Depth()
		// would report for this level's TotalQuantity — bump version so the
		// depth cache invalidates either way (removeFromBook also bumps it,
		// so this is only reached in the partial-fill branch in practice,
		// but bumping unconditionally here is simpler and correct either way).
		if maker.RemainingQty().IsZero() {
			b.removeFromBook(maker)
		} else {
			b.version++
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

// restStopOrder parks an untriggered stop order in the stop store.
func (b *Book) restStopOrder(order *models.Order) error {
	if !order.StopPrice.IsPositive() {
		order.Status = models.StatusRejected
		order.RejectReason = "stop order requires a positive stopPrice"
		return fmt.Errorf("%w: stop order requires a positive stopPrice", ErrInvalidOrder)
	}
	order.Status = models.StatusOpen
	order.UpdatedAt = time.Now()
	b.stopOrders[order.ID] = order
	b.addToStopIndex(order)
	return nil
}

// addToStopIndex/removeFromStopIndex maintain the sorted buy/sell stop
// PriceLevel structures alongside stopOrders — see this Book's own struct
// field doc comment for why two separate sorted indexes (one per side)
// exist and which direction each is sorted.
func (b *Book) addToStopIndex(order *models.Order) {
	if order.IsBuy() {
		if _, exists := b.buyStopLevels[order.StopPrice]; !exists {
			b.buyStopLevels[order.StopPrice] = NewPriceLevel(order.StopPrice)
			b.insertStopPrice(&b.buyStopPrices, order.StopPrice, true) // ascending
		}
		b.buyStopLevels[order.StopPrice].Add(order)
	} else {
		if _, exists := b.sellStopLevels[order.StopPrice]; !exists {
			b.sellStopLevels[order.StopPrice] = NewPriceLevel(order.StopPrice)
			b.insertStopPrice(&b.sellStopPrices, order.StopPrice, false) // descending
		}
		b.sellStopLevels[order.StopPrice].Add(order)
	}
}

func (b *Book) removeFromStopIndex(order *models.Order) {
	if order.IsBuy() {
		if level, ok := b.buyStopLevels[order.StopPrice]; ok {
			level.Remove(order.ID)
			if level.IsEmpty() {
				delete(b.buyStopLevels, order.StopPrice)
				b.removeStopPrice(&b.buyStopPrices, order.StopPrice, true)
			}
		}
	} else {
		if level, ok := b.sellStopLevels[order.StopPrice]; ok {
			level.Remove(order.ID)
			if level.IsEmpty() {
				delete(b.sellStopLevels, order.StopPrice)
				b.removeStopPrice(&b.sellStopPrices, order.StopPrice, false)
			}
		}
	}
}

// insertStopPrice/removeStopPrice are the shared bisect-insert/binary-search
// helpers behind buyStopPrices (ascending) and sellStopPrices (descending) —
// same technique as insertBidPrice/insertAskPrice, parameterized on sort
// direction via ascending so the two call sites don't duplicate the search
// predicate logic.
func (b *Book) insertStopPrice(prices *[]fixedpoint.Fixed, price fixedpoint.Fixed, ascending bool) {
	p := *prices
	var i int
	if ascending {
		i = sort.Search(len(p), func(i int) bool { return p[i] >= price })
	} else {
		i = sort.Search(len(p), func(i int) bool { return p[i] <= price })
	}
	p = append(p, 0)
	copy(p[i+1:], p[i:])
	p[i] = price
	*prices = p
}

func (b *Book) removeStopPrice(prices *[]fixedpoint.Fixed, price fixedpoint.Fixed, ascending bool) {
	p := *prices
	var i int
	if ascending {
		i = sort.Search(len(p), func(i int) bool { return p[i] >= price })
	} else {
		i = sort.Search(len(p), func(i int) bool { return p[i] <= price })
	}
	if i < len(p) && p[i] == price {
		*prices = append(p[:i], p[i+1:]...)
	}
}

// triggered reports whether a stop order should activate at the given price.
// Stop-buy triggers when the last trade price rises to/above StopPrice;
// stop-sell when it falls to/below StopPrice.
func triggered(stop *models.Order, lastPrice fixedpoint.Fixed) bool {
	if stop.IsBuy() {
		return lastPrice.GreaterThanOrEqual(stop.StopPrice)
	}
	return lastPrice.LessThanOrEqual(stop.StopPrice)
}

// processStopTriggers activates every stop order whose trigger price has been
// crossed by the current lastTradePrice, converting it to a market order (or
// a limit order when a limit Price is set) and matching it immediately.
// Activations can cascade: a triggered stop's fills move lastTradePrice,
// which may trigger further stops — the loop runs until quiescent.
func (b *Book) processStopTriggers() (trades []*models.Trade, cancelled []*models.Order) {
	if b.lastTradePrice.IsZero() {
		return
	}
	return b.processStopTriggersAt(b.lastTradePrice)
}

// CheckMarkPriceTriggers activates every resting stop order (this is how
// futures Take Profit/Stop Loss legs from internal/attached rest on the
// book — see BuildLegOrder) whose trigger price has been crossed by markPrice,
// exactly like processStopTriggers but driven by an externally-supplied mark
// price instead of the book's own lastTradePrice.
//
// Added 2026-09-16: TP/SL used to trigger ONLY off processStopTriggers above,
// which only re-evaluates when an actual trade prints on THIS symbol's book.
// Liquidation (internal/liquidation) has always watched the continuous mark
// price on a timer instead — a blended mid/last-trade price that keeps
// moving even when nobody happens to trade at a given level. In a quiet or
// thin book, the mark price could drift past a user's SL/TP level with no
// real trade occurring there to fire processStopTriggers, leaving a
// supposedly-protected position unprotected for however long the book stays
// quiet. This method lets a periodic mark-price sweep (mirroring
// liquidation.Engine.Run's ticker loop) drive the exact same trigger/
// activation logic on demand, closing that gap without changing how a real
// trade still triggers stops immediately (both paths share
// processStopTriggersAt and can never disagree about what "triggered" means).
func (b *Book) CheckMarkPriceTriggers(markPrice fixedpoint.Fixed) (trades []*models.Trade, cancelled []*models.Order) {
	if !markPrice.IsPositive() {
		return
	}
	return b.processStopTriggersAt(markPrice)
}

// processStopTriggersAt is the shared activation loop behind both
// processStopTriggers (keyed on the book's own last trade price) and
// CheckMarkPriceTriggers (keyed on an externally-supplied mark price) — one
// implementation, so "what counts as triggered" can never diverge between
// the two call sites.
//
// Previously a linear scan of the ENTIRE stopOrders map per activation
// (repeated once per fired stop, so a sweep that fires k of n resting stops
// cost O(n*k)) — see PERFORMANCE-CODE-REVIEW-FINDINGS.md item #5. Now uses
// buyStopLevels/sellStopLevels (see this Book's struct field doc comment):
// buyStopPrices is kept ascending and a buy-stop triggers as price rises,
// so the NEXT buy-stop to possibly trigger is always at index 0; symmetric
// for sellStopPrices (descending, triggers as price falls). Each check is
// therefore O(1) (peek index 0) instead of O(n), and the loop only ever
// does real work proportional to how many stops actually fire this sweep —
// O(log n + k) overall (the log n is PriceLevel insert/remove's own bisect
// cost when a level empties out), not O(n) or worse.
func (b *Book) processStopTriggersAt(refPrice fixedpoint.Fixed) (trades []*models.Trade, cancelled []*models.Order) {
	for {
		fired := b.nextTriggeredStop(refPrice)
		if fired == nil {
			return
		}
		delete(b.stopOrders, fired.ID)
		b.removeFromStopIndex(fired)
		if fired.Price.IsPositive() {
			fired.Type = models.Limit // stop-limit
		} else {
			fired.Type = models.Market // stop-market
		}
		fired.UpdatedAt = time.Now()
		b.activated = append(b.activated, fired)
		t, c, err := b.submitCore(fired)
		if err != nil {
			fired.Status = models.StatusRejected
			if fired.RejectReason == "" {
				fired.RejectReason = err.Error()
			}
			continue
		}
		trades = append(trades, t...)
		cancelled = append(cancelled, c...)
		// A fill here moves b.lastTradePrice (via submitCore), which is
		// exactly the cascading behavior processStopTriggers already
		// documents — but for the mark-price-driven path, subsequent
		// iterations of this loop should keep evaluating against refPrice
		// (the mark price), not whatever the just-triggered fill's trade
		// price happened to be, since mark price does not change just
		// because one stop fired. Re-reading refPrice as a fixed loop
		// parameter (not re-derived from b.lastTradePrice each iteration)
		// preserves this correctly for both callers.
	}
}

// nextTriggeredStop returns the single stop order (if any) closest to
// triggering on either side that has actually crossed refPrice — checking
// only the head of each sorted index (O(1) each) rather than scanning every
// resting stop. Returns nil if nothing on either side has triggered.
func (b *Book) nextTriggeredStop(refPrice fixedpoint.Fixed) *models.Order {
	if len(b.buyStopPrices) > 0 {
		price := b.buyStopPrices[0]
		if level := b.buyStopLevels[price]; level != nil {
			if o := level.Front(); o != nil && triggered(o, refPrice) {
				return o
			}
		}
	}
	if len(b.sellStopPrices) > 0 {
		price := b.sellStopPrices[0]
		if level := b.sellStopLevels[price]; level != nil {
			if o := level.Front(); o != nil && triggered(o, refPrice) {
				return o
			}
		}
	}
	return nil
}

// DrainActivated returns stop orders activated during the last Submit call
// (in activation order) and clears the list. The engine publishes order
// events for them so clients learn their stop became a live order.
func (b *Book) DrainActivated() []*models.Order {
	out := b.activated
	b.activated = nil
	return out
}

func (b *Book) restOrder(order *models.Order) ([]*models.Trade, error) {
	order.Status = models.StatusOpen
	order.UpdatedAt = time.Now()
	b.addToBook(order)
	return nil, nil
}

func (b *Book) addToBook(order *models.Order) {
	key := order.Price
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
	b.version++
}

func (b *Book) removeFromBook(order *models.Order) {
	key := order.Price
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
	b.version++
}

// bestOppositeLevel returns the best price level on the opposite side.
func (b *Book) bestOppositeLevel(order *models.Order) *PriceLevel {
	if order.IsBuy() {
		if len(b.askPrices) == 0 {
			return nil
		}
		return b.asks[b.askPrices[0]]
	}
	if len(b.bidPrices) == 0 {
		return nil
	}
	return b.bids[b.bidPrices[0]]
}

// oppositeLevels returns all levels on the opposite side in matching order.
func (b *Book) oppositeLevels(order *models.Order) []*PriceLevel {
	var levels []*PriceLevel
	if order.IsBuy() {
		for _, p := range b.askPrices {
			levels = append(levels, b.asks[p])
		}
	} else {
		for _, p := range b.bidPrices {
			levels = append(levels, b.bids[p])
		}
	}
	return levels
}

// priceAcceptable returns true if the maker price is acceptable to the aggressor.
func (b *Book) priceAcceptable(aggressor *models.Order, makerPrice fixedpoint.Fixed) bool {
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
//
// fixedpoint.Fixed is a plain int64, so it needs no canonicalization step
// before use as a map key (unlike decimal.Decimal, where "100" and "100.0"
// could previously produce distinct representations unless explicitly
// normalized via priceKey() — see this file's git history; that whole
// function is gone, not just rewritten, since int64 equality already means
// exactly what it needs to mean).
//
// insertBidPrice/insertAskPrice previously did a full sort.Slice over the
// ENTIRE price-level array on every single insert (O(n log n) per insert,
// repeated for every new price level that ever appears). Both bidPrices and
// askPrices are maintained as always-sorted slices, so a new price only
// ever needs to be spliced into its correct position — sort.Search does a
// binary search (O(log n)) to find that position, and the slice insert
// itself is O(n) (shifting the tail), for an overall O(n) insert instead of
// O(n log n) — and no comparison closure/interface dispatch per swap the
// way sort.Slice needed either.

func (b *Book) insertBidPrice(price fixedpoint.Fixed) {
	// Descending order: find the first index whose price is <= the new
	// price (the new price's insertion point to keep descending order).
	i := sort.Search(len(b.bidPrices), func(i int) bool {
		return b.bidPrices[i] <= price
	})
	b.bidPrices = append(b.bidPrices, 0)
	copy(b.bidPrices[i+1:], b.bidPrices[i:])
	b.bidPrices[i] = price
}

func (b *Book) removeBidPrice(price fixedpoint.Fixed) {
	// bidPrices is sorted descending; binary search for the price, then
	// remove it — same O(log n) find, O(n) shift as before, but the find
	// itself no longer needs a linear scan.
	i := sort.Search(len(b.bidPrices), func(i int) bool {
		return b.bidPrices[i] <= price
	})
	if i < len(b.bidPrices) && b.bidPrices[i] == price {
		b.bidPrices = append(b.bidPrices[:i], b.bidPrices[i+1:]...)
	}
}

func (b *Book) insertAskPrice(price fixedpoint.Fixed) {
	// Ascending order: standard sort.Search precondition (first index whose
	// price is >= the new price).
	i := sort.Search(len(b.askPrices), func(i int) bool {
		return b.askPrices[i] >= price
	})
	b.askPrices = append(b.askPrices, 0)
	copy(b.askPrices[i+1:], b.askPrices[i:])
	b.askPrices[i] = price
}

func (b *Book) removeAskPrice(price fixedpoint.Fixed) {
	i := sort.Search(len(b.askPrices), func(i int) bool {
		return b.askPrices[i] >= price
	})
	if i < len(b.askPrices) && b.askPrices[i] == price {
		b.askPrices = append(b.askPrices[:i], b.askPrices[i+1:]...)
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
