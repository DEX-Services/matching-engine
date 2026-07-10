package orderbook

import (
	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

// OrderBook is the interface that all matching engines use.
// It is asset-class agnostic; settlement logic is handled outside.
type OrderBook interface {
	// Submit accepts a new order and returns any trades generated.
	Submit(order *models.Order) ([]*models.Trade, error)

	// Cancel removes a resting order by ID. Returns the cancelled order or an error.
	Cancel(orderID string) (*models.Order, error)

	// Modify replaces price/qty of a resting order (cancel-and-replace, losing time priority).
	Modify(orderID string, newPrice, newQty decimal.Decimal) (*models.Order, error)

	// BestBid returns the highest resting bid price, or zero if no bids.
	BestBid() decimal.Decimal

	// BestAsk returns the lowest resting ask price, or zero if no asks.
	BestAsk() decimal.Decimal

	// Depth returns up to `levels` price levels for each side (bids descending, asks ascending).
	Depth(levels int) (bids, asks []*PriceLevel)

	// OrderByID looks up a resting order without removing it.
	OrderByID(orderID string) (*models.Order, bool)
}
