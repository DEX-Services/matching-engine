package orderbook

import (
	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

// OrderBook is the interface that all matching engines use.
// It is asset-class agnostic; settlement logic is handled outside.
type OrderBook interface {
	// Submit accepts a new order and returns any trades generated plus
	// resting maker orders cancelled by self-trade prevention.
	Submit(order *models.Order) ([]*models.Trade, []*models.Order, error)

	// Cancel removes a resting order by ID. Returns the cancelled order or an error.
	Cancel(orderID string) (*models.Order, error)

	// Modify replaces price/qty of a resting order (cancel-and-replace, losing time priority).
	Modify(orderID string, newPrice, newQty decimal.Decimal) (order *models.Order, trades []*models.Trade, cancelled []*models.Order, err error)

	// BestBid returns the highest resting bid price, or zero if no bids.
	BestBid() decimal.Decimal

	// BestAsk returns the lowest resting ask price, or zero if no asks.
	BestAsk() decimal.Decimal

	// Depth returns up to `levels` price levels for each side (bids descending, asks ascending)
	// as immutable snapshots.
	Depth(levels int) (bids, asks []LevelSnapshot)

	// OrderByID looks up a resting order without removing it.
	OrderByID(orderID string) (*models.Order, bool)
}
