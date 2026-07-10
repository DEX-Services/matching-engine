package orderbook

import (
	"container/list"

	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

// PriceLevel holds all resting orders at a single price, maintaining strict
// FIFO time priority via a doubly-linked list.
type PriceLevel struct {
	Price  decimal.Decimal
	orders *list.List                       // *models.Order, front = oldest (highest priority)
	index  map[string]*list.Element         // orderID -> element for O(1) removal
}

// NewPriceLevel creates an empty price level at the given price.
func NewPriceLevel(price decimal.Decimal) *PriceLevel {
	return &PriceLevel{
		Price:  price,
		orders: list.New(),
		index:  make(map[string]*list.Element),
	}
}

// Add appends an order to the back of the FIFO queue.
// Time priority: earlier orders are at the front.
func (pl *PriceLevel) Add(order *models.Order) {
	elem := pl.orders.PushBack(order)
	pl.index[order.ID] = elem
}

// Remove deletes an order by ID. Returns false if not found.
func (pl *PriceLevel) Remove(orderID string) bool {
	elem, ok := pl.index[orderID]
	if !ok {
		return false
	}
	pl.orders.Remove(elem)
	delete(pl.index, orderID)
	return true
}

// Front returns the oldest (highest time-priority) order, or nil if empty.
func (pl *PriceLevel) Front() *models.Order {
	elem := pl.orders.Front()
	if elem == nil {
		return nil
	}
	return elem.Value.(*models.Order)
}

// IsEmpty returns true when no orders remain at this price level.
func (pl *PriceLevel) IsEmpty() bool {
	return pl.orders.Len() == 0
}

// Len returns the number of resting orders at this level.
func (pl *PriceLevel) Len() int {
	return pl.orders.Len()
}

// TotalQuantity returns the aggregate resting quantity at this level.
func (pl *PriceLevel) TotalQuantity() decimal.Decimal {
	total := decimal.Zero
	for e := pl.orders.Front(); e != nil; e = e.Next() {
		o := e.Value.(*models.Order)
		total = total.Add(o.RemainingQty())
	}
	return total
}

// Orders returns all resting orders in FIFO order (copy-safe iteration).
func (pl *PriceLevel) Orders() []*models.Order {
	result := make([]*models.Order, 0, pl.orders.Len())
	for e := pl.orders.Front(); e != nil; e = e.Next() {
		result = append(result, e.Value.(*models.Order))
	}
	return result
}
