package settlement

import (
	"fmt"
	"strings"

	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
)

// SpotSettlement transfers base and quote assets between buyer and seller
// immediately upon trade execution.
//
// For a BTC-USDT trade:
//   - Buyer  pays  price × qty USDT, receives qty BTC.
//   - Seller pays  qty BTC,          receives price × qty USDT.
type SpotSettlement struct {
	ledger *risk.Ledger
}

// NewSpotSettlement constructs a SpotSettlement backed by the given ledger.
func NewSpotSettlement(ledger *risk.Ledger) *SpotSettlement {
	return &SpotSettlement{ledger: ledger}
}

// Settle debits and credits both sides of the trade atomically in the ledger.
// It is called inside the matching goroutine — no external locking needed.
func (s *SpotSettlement) Settle(trade *models.Trade) error {
	if trade.BuyOrder == nil || trade.SellOrder == nil {
		return fmt.Errorf("spot settle: missing order references on trade %s", trade.ID)
	}

	base, quote, err := parseSymbol(trade.Symbol)
	if err != nil {
		return err
	}

	notional := trade.Price.Mul(trade.Quantity)
	buyerID := trade.BuyOrder.AccountID
	sellerID := trade.SellOrder.AccountID

	// Buyer: pays quote, receives base.
	if err := s.ledger.Debit(buyerID, quote, notional); err != nil {
		return fmt.Errorf("spot settle debit buyer quote: %w", err)
	}
	s.ledger.Credit(buyerID, base, trade.Quantity)

	// Seller: pays base, receives quote.
	if err := s.ledger.Debit(sellerID, base, trade.Quantity); err != nil {
		return fmt.Errorf("spot settle debit seller base: %w", err)
	}
	s.ledger.Credit(sellerID, quote, notional)

	return nil
}

func parseSymbol(symbol string) (base, quote string, err error) {
	parts := strings.SplitN(symbol, "-", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid symbol format %q: expected BASE-QUOTE", symbol)
	}
	return parts[0], parts[1], nil
}

// Ensure SpotSettlement satisfies the Handler interface.
var _ Handler = (*SpotSettlement)(nil)
