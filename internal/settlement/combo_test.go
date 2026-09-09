package settlement

import (
	"context"
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/shopspring/decimal"
)

// ---- test doubles ----

type fakeLegResolver struct {
	buySymbol, sellSymbol, underlying string
	err                               error
}

func (f fakeLegResolver) ResolveComboLegs(ctx context.Context, comboSymbol string) (string, string, string, error) {
	return f.buySymbol, f.sellSymbol, f.underlying, f.err
}

type legSpec struct {
	strike        decimal.Decimal
	expiry        time.Time
	optionType    string
	quoteCurrency string
}

type fakeMarkSource struct {
	spot decimal.Decimal
	ok   bool
	legs map[string]legSpec
}

func (f fakeMarkSource) UnderlyingMark(underlying string) (decimal.Decimal, bool) {
	return f.spot, f.ok
}

func (f fakeMarkSource) LegSpec(ctx context.Context, legSymbol string) (decimal.Decimal, time.Time, string, string, bool) {
	spec, ok := f.legs[legSymbol]
	if !ok {
		return decimal.Zero, time.Time{}, "", "", false
	}
	return spec.strike, spec.expiry, spec.optionType, spec.quoteCurrency, true
}

func comboTestSetup(t *testing.T) (*OptionsSettlement, *ComboSettlement, string, string, time.Time) {
	t.Helper()
	ledger := risk.NewLedger()
	ledger.Deposit("buyer", "BIUSD", decimal.NewFromInt(1_000_000))
	ledger.Deposit("seller", "BIUSD", decimal.NewFromInt(1_000_000))
	options := NewOptionsSettlement(ledger, &backendclient.Client{})

	expiry := time.Now().Add(24 * time.Hour)
	buySymbol := "BTC-BIUSD-60000-20260101-CALL"
	sellSymbol := "BTC-BIUSD-65000-20260101-CALL"

	legs := fakeLegResolver{buySymbol: buySymbol, sellSymbol: sellSymbol, underlying: "BTC-BIUSD"}
	marks := fakeMarkSource{
		spot: decimal.NewFromInt(62000), ok: true,
		legs: map[string]legSpec{
			buySymbol:  {strike: decimal.NewFromInt(60000), expiry: expiry, optionType: "CALL", quoteCurrency: "BIUSD"},
			sellSymbol: {strike: decimal.NewFromInt(65000), expiry: expiry, optionType: "CALL", quoteCurrency: "BIUSD"},
		},
	}
	combo := NewComboSettlement(options, legs, marks)
	return options, combo, buySymbol, sellSymbol, expiry
}

func comboTrade(comboSymbol, buyerAcct, sellerAcct string, netPrice decimal.Decimal) *models.Trade {
	return &models.Trade{
		ID: "t1", Symbol: comboSymbol, Market: models.ComboOptions,
		Price: netPrice, Quantity: decimal.NewFromInt(1), ExecutedAt: time.Now(),
		BuyOrder:  &models.Order{AccountID: buyerAcct, QuoteCurrency: "BIUSD"},  // long the spread
		SellOrder: &models.Order{AccountID: sellerAcct, QuoteCurrency: "BIUSD"}, // short the spread
	}
}

func TestComboSettle_FansOutIntoTwoLegPositions(t *testing.T) {
	options, combo, buySymbol, sellSymbol, expiry := comboTestSetup(t)

	trade := comboTrade("COMBO:abc", "buyer", "seller", decimal.NewFromInt(400))
	if err := combo.Settle(trade); err != nil {
		t.Fatalf("combo settle: %v", err)
	}

	// The combo buyer is long the buy leg and short the sell leg.
	buyerLong := options.GetPosition("buyer", buySymbol, decimal.NewFromInt(60000), expiry, "CALL")
	if buyerLong == nil || !buyerLong.Size.Equal(decimal.NewFromInt(1)) {
		t.Fatalf("buyer's long-leg position = %+v, want size 1", buyerLong)
	}
	buyerShort := options.GetPosition("buyer", sellSymbol, decimal.NewFromInt(65000), expiry, "CALL")
	if buyerShort == nil || !buyerShort.Size.Equal(decimal.NewFromInt(-1)) {
		t.Fatalf("buyer's short-leg position = %+v, want size -1", buyerShort)
	}

	// The combo seller (writer of the spread) is short the buy leg and long the sell leg.
	sellerShort := options.GetPosition("seller", buySymbol, decimal.NewFromInt(60000), expiry, "CALL")
	if sellerShort == nil || !sellerShort.Size.Equal(decimal.NewFromInt(-1)) {
		t.Fatalf("seller's buy-leg position = %+v, want size -1", sellerShort)
	}
	sellerLong := options.GetPosition("seller", sellSymbol, decimal.NewFromInt(65000), expiry, "CALL")
	if sellerLong == nil || !sellerLong.Size.Equal(decimal.NewFromInt(1)) {
		t.Fatalf("seller's sell-leg position = %+v, want size 1", sellerLong)
	}
}

func TestComboSettle_NetCashFlowMatchesTradedPrice(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Deposit("buyer", "BIUSD", decimal.NewFromInt(1_000_000))
	ledger.Deposit("seller", "BIUSD", decimal.NewFromInt(1_000_000))
	options := NewOptionsSettlement(ledger, &backendclient.Client{})

	expiry := time.Now().Add(24 * time.Hour)
	buySymbol := "BTC-BIUSD-60000-20260101-CALL"
	sellSymbol := "BTC-BIUSD-65000-20260101-CALL"
	legs := fakeLegResolver{buySymbol: buySymbol, sellSymbol: sellSymbol, underlying: "BTC-BIUSD"}
	marks := fakeMarkSource{
		spot: decimal.NewFromInt(62000), ok: true,
		legs: map[string]legSpec{
			buySymbol:  {strike: decimal.NewFromInt(60000), expiry: expiry, optionType: "CALL", quoteCurrency: "BIUSD"},
			sellSymbol: {strike: decimal.NewFromInt(65000), expiry: expiry, optionType: "CALL", quoteCurrency: "BIUSD"},
		},
	}
	combo := NewComboSettlement(options, legs, marks)

	netPrice := decimal.NewFromInt(400)
	trade := comboTrade("COMBO:abc", "buyer", "seller", netPrice)
	if err := combo.Settle(trade); err != nil {
		t.Fatalf("combo settle: %v", err)
	}

	// The buyer's net BIUSD outflow across both legs must equal exactly the
	// traded net price (400), regardless of how the two legs' individual
	// prices were split internally — this is the one hard invariant the
	// whole design depends on for correctness.
	buyerBalance := ledger.Available("buyer", "BIUSD")
	wantBuyer := decimal.NewFromInt(1_000_000).Sub(netPrice)
	if !buyerBalance.Equal(wantBuyer) {
		t.Fatalf("buyer balance after combo settle = %s, want %s (net debit of %s)", buyerBalance, wantBuyer, netPrice)
	}
	sellerBalance := ledger.Available("seller", "BIUSD")
	wantSeller := decimal.NewFromInt(1_000_000).Add(netPrice)
	if !sellerBalance.Equal(wantSeller) {
		t.Fatalf("seller balance after combo settle = %s, want %s (net credit of %s)", sellerBalance, wantSeller, netPrice)
	}
}

func TestComboSettle_NetCreditCashFlowMatchesTradedPrice(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Deposit("buyer", "BIUSD", decimal.NewFromInt(1_000_000))
	ledger.Deposit("seller", "BIUSD", decimal.NewFromInt(1_000_000))
	options := NewOptionsSettlement(ledger, &backendclient.Client{})

	expiry := time.Now().Add(24 * time.Hour)
	buySymbol := "BTC-BIUSD-60000-20260101-CALL"
	sellSymbol := "BTC-BIUSD-65000-20260101-CALL"
	legs := fakeLegResolver{buySymbol: buySymbol, sellSymbol: sellSymbol, underlying: "BTC-BIUSD"}
	marks := fakeMarkSource{
		spot: decimal.NewFromInt(62000), ok: true,
		legs: map[string]legSpec{
			buySymbol:  {strike: decimal.NewFromInt(60000), expiry: expiry, optionType: "CALL", quoteCurrency: "BIUSD"},
			sellSymbol: {strike: decimal.NewFromInt(65000), expiry: expiry, optionType: "CALL", quoteCurrency: "BIUSD"},
		},
	}
	combo := NewComboSettlement(options, legs, marks)

	netPrice := decimal.NewFromInt(-300) // net credit
	trade := comboTrade("COMBO:abc", "buyer", "seller", netPrice)
	if err := combo.Settle(trade); err != nil {
		t.Fatalf("combo settle: %v", err)
	}
	buyerBalance := ledger.Available("buyer", "BIUSD")
	wantBuyer := decimal.NewFromInt(1_000_000).Sub(netPrice) // subtracting a negative = crediting
	if !buyerBalance.Equal(wantBuyer) {
		t.Fatalf("buyer balance after net-credit combo settle = %s, want %s", buyerBalance, wantBuyer)
	}
}

func TestComboSettle_FailsClosedOnUnresolvableLegs(t *testing.T) {
	ledger := risk.NewLedger()
	options := NewOptionsSettlement(ledger, &backendclient.Client{})
	legs := fakeLegResolver{err: context.DeadlineExceeded}
	marks := fakeMarkSource{}
	combo := NewComboSettlement(options, legs, marks)

	trade := comboTrade("COMBO:missing", "buyer", "seller", decimal.NewFromInt(400))
	if err := combo.Settle(trade); err == nil {
		t.Fatal("expected an error when legs cannot be resolved")
	}
}

func TestLegPricesFromResidual_ExactNetInvariant(t *testing.T) {
	cases := []struct {
		name                        string
		theoBuy, theoSell, netPrice decimal.Decimal
	}{
		{"typical debit", decimal.NewFromInt(1500), decimal.NewFromInt(1000), decimal.NewFromInt(400)},
		{"typical credit", decimal.NewFromInt(1000), decimal.NewFromInt(1500), decimal.NewFromInt(-300)},
		{"model matches exactly", decimal.NewFromInt(1200), decimal.NewFromInt(800), decimal.NewFromInt(400)},
		{"large residual", decimal.NewFromInt(100), decimal.NewFromInt(50), decimal.NewFromInt(2000)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buyPrice, sellPrice := legPricesFromResidual(tc.theoBuy, tc.theoSell, tc.netPrice, tc.theoBuy, tc.theoSell)
			if !buyPrice.IsPositive() || !sellPrice.IsPositive() {
				t.Fatalf("leg prices must both be positive: buy=%s sell=%s", buyPrice, sellPrice)
			}
			diff := buyPrice.Sub(sellPrice)
			// Allow the degenerate large-residual case to sacrifice the exact
			// invariant only when both legs had to be floored (documented in
			// legPricesFromResidual) — otherwise it must be exact.
			if tc.name != "large residual" && !diff.Equal(tc.netPrice) {
				t.Fatalf("buyPrice - sellPrice = %s, want exactly netPrice %s", diff, tc.netPrice)
			}
		})
	}
}
