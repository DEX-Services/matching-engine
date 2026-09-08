package settlement

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/shopspring/decimal"
)

// FeeLookup returns the maker/taker fee fractions for a symbol/market.
// Returning zeros means no fees are charged. Satisfied by a closure over
// config.Registry.
type FeeLookup func(symbol string, market models.MarketType) (maker, taker decimal.Decimal)

// SpotSettlement transfers base and quote assets between buyer and seller
// immediately upon trade execution.
//
// For a BTC-USDT trade:
//   - Buyer  pays  price × qty USDT, receives qty BTC.
//   - Seller pays  qty BTC,          receives price × qty USDT.
//
// Fees (when configured) are charged in the quote currency: the maker and
// taker are each debited notional × fee-rate on top of / out of their quote
// flow, recorded on the trade as MakerFeePaid / TakerFeePaid.
type SpotSettlement struct {
	ledger  *risk.Ledger
	backend *backendclient.Client
	fees    FeeLookup
}

// NewSpotSettlement constructs a SpotSettlement backed by the given ledger.
// fees may be nil (no fees charged).
func NewSpotSettlement(ledger *risk.Ledger, backend *backendclient.Client, fees FeeLookup) *SpotSettlement {
	if backend == nil {
		backend = &backendclient.Client{}
	}
	return &SpotSettlement{ledger: ledger, backend: backend, fees: fees}
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

	// Resolve fees. Maker fee applies to the resting side, taker to the aggressor.
	var makerFee, takerFee decimal.Decimal
	if s.fees != nil {
		makerRate, takerRate := s.fees(trade.Symbol, trade.Market)
		makerFee = notional.Mul(makerRate)
		takerFee = notional.Mul(takerRate)
	}
	buyerFee, sellerFee := takerFee, makerFee
	if trade.MakerSide == models.Buy {
		buyerFee, sellerFee = makerFee, takerFee
	}

	// Persist both sides as one backend transaction before changing the
	// in-memory ledger. The durable reservations already exist, so a failure
	// leaves both ledgers untouched rather than producing a partial credit.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := s.backend.SettleSpot(ctx, buyerID, sellerID, base, quote,
		backendclient.ToRawUnits(trade.Quantity),
		backendclient.ToRawUnits(notional.Add(buyerFee)),
		backendclient.ToRawUnits(notional.Sub(sellerFee))); err != nil {
		return fmt.Errorf("spot settle backend: %w", err)
	}

	// Buyer: pays quote (notional + fee), receives base.
	if err := s.ledger.Debit(buyerID, quote, notional.Add(buyerFee)); err != nil {
		return fmt.Errorf("spot settle debit buyer quote: %w", err)
	}
	s.ledger.Credit(buyerID, base, trade.Quantity)

	// Seller: pays base, receives quote net of fee.
	if err := s.ledger.Debit(sellerID, base, trade.Quantity); err != nil {
		// Roll back the buyer leg so a failed settle leaves the ledger unchanged.
		s.ledger.Credit(buyerID, quote, notional.Add(buyerFee))
		if derr := s.ledger.Debit(buyerID, base, trade.Quantity); derr != nil {
			return fmt.Errorf("spot settle debit seller base: %w (rollback of buyer leg also failed: %v)", err, derr)
		}
		return fmt.Errorf("spot settle debit seller base: %w", err)
	}
	s.ledger.Credit(sellerID, quote, notional.Sub(sellerFee))

	if trade.MakerSide == models.Buy {
		trade.MakerFeePaid, trade.TakerFeePaid = buyerFee, sellerFee
	} else {
		trade.MakerFeePaid, trade.TakerFeePaid = sellerFee, buyerFee
	}

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
