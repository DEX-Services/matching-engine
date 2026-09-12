package settlement

import (
	"testing"

	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/shopspring/decimal"
)

// discountedFees builds a FeeLookup with fixed base rates and a per-account
// discount map, mirroring how cmd/engine/main.go composes feeconfig +
// discounts into one closure — without needing a live Postgres-backed
// registry in these unit tests.
func discountedFees(makerRate, takerRate decimal.Decimal, discounts map[string]decimal.Decimal) FeeLookup {
	return func(symbol string, market models.MarketType, accountID string) (decimal.Decimal, decimal.Decimal) {
		discount := discounts[accountID]
		factor := decimal.NewFromInt(1).Sub(discount)
		return makerRate.Mul(factor), takerRate.Mul(factor)
	}
}

func TestSpotSettlement_ChargesDiscountedFeePerAccount(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Credit("buyer", "USDT", decimal.NewFromInt(1_000_000))
	ledger.Credit("seller", "BTC", decimal.NewFromInt(1_000_000))

	maker := decimal.NewFromFloat(0.0015) // 0.15%
	taker := decimal.NewFromFloat(0.0045) // 0.45%
	// Buyer (taker here) has a 20% discount; seller (maker) has none.
	fees := discountedFees(maker, taker, map[string]decimal.Decimal{
		"buyer": decimal.NewFromFloat(0.20),
	})
	s := NewSpotSettlement(ledger, nil, fees)

	qty := decimal.NewFromInt(1)
	price := decimal.NewFromInt(50000)
	trade := &models.Trade{
		ID: "t1", Symbol: "BTC-USDT", Market: models.Spot,
		Price: price, Quantity: qty, MakerSide: models.Sell,
		BuyOrder:  &models.Order{AccountID: "buyer"},
		SellOrder: &models.Order{AccountID: "seller"},
	}
	if err := s.Settle(trade); err != nil {
		t.Fatalf("settle: %v", err)
	}

	notional := price.Mul(qty)
	wantTakerFee := notional.Mul(taker).Mul(decimal.NewFromFloat(0.80)) // buyer's 20% discount applied
	wantMakerFee := notional.Mul(maker)                                 // seller: no discount

	if !trade.TakerFeePaid.Equal(wantTakerFee) {
		t.Errorf("taker (buyer) fee = %s, want %s (discounted)", trade.TakerFeePaid, wantTakerFee)
	}
	if !trade.MakerFeePaid.Equal(wantMakerFee) {
		t.Errorf("maker (seller) fee = %s, want %s (undiscounted)", trade.MakerFeePaid, wantMakerFee)
	}
}

func TestFuturesSettlement_ChargesMakerTakerFee(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Credit("buyer", "USDC", decimal.NewFromInt(1_000_000))
	ledger.Credit("seller", "USDC", decimal.NewFromInt(1_000_000))

	maker := decimal.NewFromFloat(0.00015) // 0.015%
	taker := decimal.NewFromFloat(0.00045) // 0.045%
	fees := discountedFees(maker, taker, nil)
	f := NewFuturesSettlement(ledger, nil, nil, fees)

	qty := decimal.NewFromInt(1)
	price := decimal.NewFromInt(50000)
	trade := &models.Trade{
		ID: "t1", Symbol: "BTC-USDC", Market: models.Futures,
		Price: price, Quantity: qty, MakerSide: models.Buy,
		BuyOrder:  &models.Order{AccountID: "buyer", Leverage: 10, MarginMode: models.MarginIsolated},
		SellOrder: &models.Order{AccountID: "seller", Leverage: 10, MarginMode: models.MarginIsolated},
	}
	if err := f.Settle(trade); err != nil {
		t.Fatalf("settle: %v", err)
	}

	notional := price.Mul(qty)
	wantMakerFee := notional.Mul(maker) // buyer is maker
	wantTakerFee := notional.Mul(taker) // seller is taker

	if !trade.MakerFeePaid.Equal(wantMakerFee) {
		t.Errorf("maker (buyer) fee = %s, want %s", trade.MakerFeePaid, wantMakerFee)
	}
	if !trade.TakerFeePaid.Equal(wantTakerFee) {
		t.Errorf("taker (seller) fee = %s, want %s", trade.TakerFeePaid, wantTakerFee)
	}

	// Confirm the fee was actually debited from balance (on top of margin),
	// not just recorded on the trade struct: buyer started with 1,000,000,
	// paid margin (price*qty/leverage = 5000) plus their maker fee.
	margin := decimal.NewFromInt(50000).Div(decimal.NewFromInt(10))
	wantBuyerBalance := decimal.NewFromInt(1_000_000).Sub(margin).Sub(wantMakerFee)
	gotBuyerBalance := ledger.Available("buyer", "USDC")
	if !gotBuyerBalance.Equal(wantBuyerBalance) {
		t.Errorf("buyer available balance = %s, want %s (margin + fee both debited)", gotBuyerBalance, wantBuyerBalance)
	}
}

func TestFuturesSettlement_NilFeeLookupChargesNothing(t *testing.T) {
	// Regression guard: passing nil (as every pre-existing test in this
	// package does) must behave exactly as before this feature existed —
	// zero fees, not a nil-pointer panic.
	ledger := risk.NewLedger()
	ledger.Credit("buyer", "USDC", decimal.NewFromInt(1_000_000))
	ledger.Credit("seller", "USDC", decimal.NewFromInt(1_000_000))
	f := NewFuturesSettlement(ledger, nil, nil, nil)

	trade := &models.Trade{
		ID: "t1", Symbol: "BTC-USDC", Market: models.Futures,
		Price: decimal.NewFromInt(50000), Quantity: decimal.NewFromInt(1), MakerSide: models.Buy,
		BuyOrder:  &models.Order{AccountID: "buyer", Leverage: 10, MarginMode: models.MarginIsolated},
		SellOrder: &models.Order{AccountID: "seller", Leverage: 10, MarginMode: models.MarginIsolated},
	}
	if err := f.Settle(trade); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if !trade.MakerFeePaid.IsZero() || !trade.TakerFeePaid.IsZero() {
		t.Errorf("expected zero fees with nil FeeLookup, got maker=%s taker=%s", trade.MakerFeePaid, trade.TakerFeePaid)
	}
}

func TestClosePosition_ChargesLiquidationFee(t *testing.T) {
	ledger := risk.NewLedger()
	f := NewFuturesSettlement(ledger, nil, nil, nil)

	symbol, quote := "BTC-USDC", "USDC"
	qty := decimal.NewFromInt(1)
	openLong(t, f, ledger, "acct1", symbol, quote, qty, decimal.NewFromInt(50000))

	balanceBeforeClose := ledger.Available("acct1", quote)

	markPrice := decimal.NewFromInt(50000) // flat PnL, isolates the fee's effect
	notional := markPrice.Mul(qty)
	liquidationFee := notional.Mul(decimal.NewFromFloat(0.02)) // 2%, the platform default

	f.ClosePosition("acct1", symbol, quote, markPrice, liquidationFee)

	margin := decimal.NewFromInt(50000).Div(decimal.NewFromInt(10))   // leverage 10
	wantBalance := balanceBeforeClose.Add(margin).Sub(liquidationFee) // margin released, minus the fee
	gotBalance := ledger.Available("acct1", quote)
	if !gotBalance.Equal(wantBalance) {
		t.Errorf("balance after liquidation close = %s, want %s (margin released minus liquidation fee)", gotBalance, wantBalance)
	}
}

func TestClosePosition_ZeroLiquidationFeeChangesNothing(t *testing.T) {
	// Regression guard: every pre-existing caller passed no fee concept at
	// all; confirm passing decimal.Zero reproduces that exact prior behavior.
	ledger := risk.NewLedger()
	f := NewFuturesSettlement(ledger, nil, nil, nil)

	symbol, quote := "BTC-USDC", "USDC"
	qty := decimal.NewFromInt(1)
	openLong(t, f, ledger, "acct1", symbol, quote, qty, decimal.NewFromInt(50000))
	balanceBeforeClose := ledger.Available("acct1", quote)

	f.ClosePosition("acct1", symbol, quote, decimal.NewFromInt(50000), decimal.Zero)

	margin := decimal.NewFromInt(50000).Div(decimal.NewFromInt(10))
	wantBalance := balanceBeforeClose.Add(margin)
	if got := ledger.Available("acct1", quote); !got.Equal(wantBalance) {
		t.Errorf("balance = %s, want %s (margin released, no fee)", got, wantBalance)
	}
}
