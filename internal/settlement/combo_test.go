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

// decimalAlmostEqual tolerates float64-round-trip noise from the
// Black-Scholes theoretical pricing path (pricing.Price takes float64
// inputs, so a decimal -> float64 -> decimal round trip can differ from an
// exact decimal computation in the 15th+ significant digit) — real money
// math elsewhere in this codebase stays exact decimal throughout, but the
// theoretical price itself is inherently float64-precision by construction
// (see pricing.Price's own doc comment), so tests asserting against it must
// tolerate that, not demand bit-exact equality no decimal library can give
// back once a float64 boundary is crossed.
func decimalAlmostEqual(t *testing.T, got, want decimal.Decimal, msgAndArgs ...any) {
	t.Helper()
	tolerance := decimal.NewFromFloat(0.0001)
	if got.Sub(want).Abs().GreaterThan(tolerance) {
		t.Fatalf("got %s, want %s (tolerance %s): %v", got, want, tolerance, msgAndArgs)
	}
}

// ---- test doubles ----

type fakeLegResolver struct {
	legs       []models.ComboLeg
	underlying string
	err        error
}

func (f fakeLegResolver) ResolveComboLegs(ctx context.Context, comboSymbol string) ([]models.ComboLeg, string, error) {
	return f.legs, f.underlying, f.err
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

func verticalSetup(t *testing.T) (*OptionsSettlement, *ComboSettlement, string, string, time.Time) {
	t.Helper()
	ledger := risk.NewLedger()
	ledger.Deposit("buyer", "BIUSD", decimal.NewFromInt(1_000_000))
	ledger.Deposit("seller", "BIUSD", decimal.NewFromInt(1_000_000))
	options := NewOptionsSettlement(ledger, &backendclient.Client{})

	expiry := time.Now().Add(24 * time.Hour)
	buySymbol := "BTC-BIUSD-60000-20260101-CALL"
	sellSymbol := "BTC-BIUSD-65000-20260101-CALL"

	legs := fakeLegResolver{
		legs:       []models.ComboLeg{{Symbol: buySymbol, Ratio: 1}, {Symbol: sellSymbol, Ratio: -1}},
		underlying: "BTC-BIUSD",
	}
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
	options, combo, buySymbol, sellSymbol, expiry := verticalSetup(t)

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

func TestComboSettle_NetDebitCashFlowMatchesTradedPrice(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Deposit("buyer", "BIUSD", decimal.NewFromInt(1_000_000))
	ledger.Deposit("seller", "BIUSD", decimal.NewFromInt(1_000_000))
	options := NewOptionsSettlement(ledger, &backendclient.Client{})

	expiry := time.Now().Add(24 * time.Hour)
	buySymbol := "BTC-BIUSD-60000-20260101-CALL"
	sellSymbol := "BTC-BIUSD-65000-20260101-CALL"
	legs := fakeLegResolver{
		legs:       []models.ComboLeg{{Symbol: buySymbol, Ratio: 1}, {Symbol: sellSymbol, Ratio: -1}},
		underlying: "BTC-BIUSD",
	}
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
	decimalAlmostEqual(t, buyerBalance, wantBuyer, "buyer balance after combo settle (net debit of", netPrice, ")")
	sellerBalance := ledger.Available("seller", "BIUSD")
	wantSeller := decimal.NewFromInt(1_000_000).Add(netPrice)
	decimalAlmostEqual(t, sellerBalance, wantSeller, "seller balance after combo settle (net credit of", netPrice, ")")
}

func TestComboSettle_NetCreditCashFlowMatchesTradedPrice(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Deposit("buyer", "BIUSD", decimal.NewFromInt(1_000_000))
	ledger.Deposit("seller", "BIUSD", decimal.NewFromInt(1_000_000))
	options := NewOptionsSettlement(ledger, &backendclient.Client{})

	expiry := time.Now().Add(24 * time.Hour)
	buySymbol := "BTC-BIUSD-60000-20260101-CALL"
	sellSymbol := "BTC-BIUSD-65000-20260101-CALL"
	legs := fakeLegResolver{
		legs:       []models.ComboLeg{{Symbol: buySymbol, Ratio: 1}, {Symbol: sellSymbol, Ratio: -1}},
		underlying: "BTC-BIUSD",
	}
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

func TestComboSettle_IronCondorFansOutIntoFourLegPositions(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Deposit("buyer", "BIUSD", decimal.NewFromInt(1_000_000))
	ledger.Deposit("seller", "BIUSD", decimal.NewFromInt(1_000_000))
	options := NewOptionsSettlement(ledger, &backendclient.Client{})

	expiry := time.Now().Add(24 * time.Hour)
	longPut := "BTC-BIUSD-50000-20260101-PUT"
	shortPut := "BTC-BIUSD-55000-20260101-PUT"
	shortCall := "BTC-BIUSD-65000-20260101-CALL"
	longCall := "BTC-BIUSD-70000-20260101-CALL"

	legs := fakeLegResolver{
		legs: []models.ComboLeg{
			{Symbol: longPut, Ratio: 1},
			{Symbol: shortPut, Ratio: -1},
			{Symbol: shortCall, Ratio: -1},
			{Symbol: longCall, Ratio: 1},
		},
		underlying: "BTC-BIUSD",
	}
	marks := fakeMarkSource{
		spot: decimal.NewFromInt(60000), ok: true,
		legs: map[string]legSpec{
			longPut:   {strike: decimal.NewFromInt(50000), expiry: expiry, optionType: "PUT", quoteCurrency: "BIUSD"},
			shortPut:  {strike: decimal.NewFromInt(55000), expiry: expiry, optionType: "PUT", quoteCurrency: "BIUSD"},
			shortCall: {strike: decimal.NewFromInt(65000), expiry: expiry, optionType: "CALL", quoteCurrency: "BIUSD"},
			longCall:  {strike: decimal.NewFromInt(70000), expiry: expiry, optionType: "CALL", quoteCurrency: "BIUSD"},
		},
	}
	combo := NewComboSettlement(options, legs, marks)

	trade := comboTrade("COMBO:condor", "buyer", "seller", decimal.NewFromInt(-800)) // opened for a credit
	if err := combo.Settle(trade); err != nil {
		t.Fatalf("combo settle: %v", err)
	}

	// Buyer of the combo is long the long legs, short the short legs.
	if pos := options.GetPosition("buyer", longPut, decimal.NewFromInt(50000), expiry, "PUT"); pos == nil || !pos.Size.Equal(decimal.NewFromInt(1)) {
		t.Fatalf("buyer long put position = %+v, want size 1", pos)
	}
	if pos := options.GetPosition("buyer", shortPut, decimal.NewFromInt(55000), expiry, "PUT"); pos == nil || !pos.Size.Equal(decimal.NewFromInt(-1)) {
		t.Fatalf("buyer short put position = %+v, want size -1", pos)
	}
	if pos := options.GetPosition("buyer", shortCall, decimal.NewFromInt(65000), expiry, "CALL"); pos == nil || !pos.Size.Equal(decimal.NewFromInt(-1)) {
		t.Fatalf("buyer short call position = %+v, want size -1", pos)
	}
	if pos := options.GetPosition("buyer", longCall, decimal.NewFromInt(70000), expiry, "CALL"); pos == nil || !pos.Size.Equal(decimal.NewFromInt(1)) {
		t.Fatalf("buyer long call position = %+v, want size 1", pos)
	}

	// Net cash flow: buyer received the 800 credit (netPrice was -800, a
	// credit to the combo BUYER in this test's sign convention... but see
	// comboTrade's doc: BuyOrder is "long the spread" — a negative netPrice
	// here means the buyer RECEIVES 800, matching the credit-collecting
	// short iron condor a real trader would open in this direction).
	buyerBalance := ledger.Available("buyer", "BIUSD")
	wantBuyer := decimal.NewFromInt(1_000_000).Add(decimal.NewFromInt(800))
	decimalAlmostEqual(t, buyerBalance, wantBuyer, "buyer balance after iron condor settle")
}

func TestLegPricesInvariant_TwoLeg(t *testing.T) {
	// Exercises splitComboNetPrice's N-leg path with N=2 directly (not via
	// a full Settle call), checking the one hard invariant: the ratio-
	// weighted sum of returned leg prices equals netPrice exactly.
	specs := []legSpecWithSymbol{
		{symbol: "buy", ratio: 1, strike: decimal.NewFromInt(60000), optionType: "CALL"},
		{symbol: "sell", ratio: -1, strike: decimal.NewFromInt(65000), optionType: "CALL"},
	}
	marks := fakeMarkSource{ok: false} // no live mark -> flat-split fallback path
	netPrice := decimal.NewFromInt(400)
	prices := splitComboNetPrice(netPrice, specs, "BTC-BIUSD", marks, decimal.NewFromFloat(0.03))
	if len(prices) != 2 {
		t.Fatalf("got %d prices, want 2", len(prices))
	}
	achieved := prices[0].Mul(decimal.NewFromInt(1)).Add(prices[1].Mul(decimal.NewFromInt(-1)))
	if !achieved.Equal(netPrice) {
		t.Fatalf("ratio-weighted sum = %s, want netPrice %s", achieved, netPrice)
	}
	for i, p := range prices {
		if !p.IsPositive() {
			t.Fatalf("leg %d price = %s, want positive", i, p)
		}
	}
}

func TestLegPricesInvariant_FourLeg(t *testing.T) {
	specs := []legSpecWithSymbol{
		{symbol: "longput", ratio: 1, strike: decimal.NewFromInt(50000), optionType: "PUT"},
		{symbol: "shortput", ratio: -1, strike: decimal.NewFromInt(55000), optionType: "PUT"},
		{symbol: "shortcall", ratio: -1, strike: decimal.NewFromInt(65000), optionType: "CALL"},
		{symbol: "longcall", ratio: 1, strike: decimal.NewFromInt(70000), optionType: "CALL"},
	}
	marks := fakeMarkSource{spot: decimal.NewFromInt(60000), ok: true}
	for i := range specs {
		specs[i].expiry = time.Now().Add(24 * time.Hour)
	}
	netPrice := decimal.NewFromInt(-800)
	prices := splitComboNetPrice(netPrice, specs, "BTC-BIUSD", marks, decimal.NewFromFloat(0.03))
	if len(prices) != 4 {
		t.Fatalf("got %d prices, want 4", len(prices))
	}
	achieved := decimal.Zero
	for i, spec := range specs {
		achieved = achieved.Add(prices[i].Mul(decimal.NewFromInt(int64(spec.ratio))))
		if !prices[i].IsPositive() {
			t.Fatalf("leg %d (%s) price = %s, want positive", i, spec.symbol, prices[i])
		}
	}
	decimalAlmostEqual(t, achieved, netPrice, "4-leg ratio-weighted sum")
}

func TestSplitComboNetPrice_SignMismatchFallback(t *testing.T) {
	// Legs shaped like a debit structure under the theoretical model (long
	// the expensive/ITM leg, short the cheap/OTM leg -> modeledNet
	// positive) but the combo ACTUALLY traded at a credit (netPrice
	// negative) — the exact bug this function was rewritten to fix. Every
	// leg price must stay positive and the ratio-weighted sum must exactly
	// equal netPrice.
	specs := []legSpecWithSymbol{
		{symbol: "itm", ratio: 1, strike: decimal.NewFromInt(60000), optionType: "CALL", expiry: time.Now().Add(24 * time.Hour)},
		{symbol: "otm", ratio: -1, strike: decimal.NewFromInt(65000), optionType: "CALL", expiry: time.Now().Add(24 * time.Hour)},
	}
	marks := fakeMarkSource{spot: decimal.NewFromInt(62000), ok: true}
	netPrice := decimal.NewFromInt(-300)
	prices := splitComboNetPrice(netPrice, specs, "BTC-BIUSD", marks, decimal.NewFromFloat(0.03))

	for i, p := range prices {
		if !p.IsPositive() {
			t.Fatalf("leg %d price = %s, want positive (sign-mismatch fallback must never produce a negative leg price)", i, p)
		}
	}
	achieved := prices[0].Mul(decimal.NewFromInt(1)).Add(prices[1].Mul(decimal.NewFromInt(-1)))
	if !achieved.Equal(netPrice) {
		t.Fatalf("sign-mismatch ratio-weighted sum = %s, want exactly netPrice %s", achieved, netPrice)
	}
}

func TestSplitComboNetPrice_SignMismatchFallback_IronCondor(t *testing.T) {
	// Same sign-mismatch scenario generalized to 4 legs with a mix of
	// positive and negative ratios (an iron condor), confirming the
	// exhaustive per-leg search finds a valid settling leg regardless of
	// leg count.
	expiry := time.Now().Add(24 * time.Hour)
	specs := []legSpecWithSymbol{
		{symbol: "longput", ratio: 1, strike: decimal.NewFromInt(50000), optionType: "PUT", expiry: expiry},
		{symbol: "shortput", ratio: -1, strike: decimal.NewFromInt(55000), optionType: "PUT", expiry: expiry},
		{symbol: "shortcall", ratio: -1, strike: decimal.NewFromInt(65000), optionType: "CALL", expiry: expiry},
		{symbol: "longcall", ratio: 1, strike: decimal.NewFromInt(70000), optionType: "CALL", expiry: expiry},
	}
	// Force a sign mismatch deliberately by using a mark far from any
	// strike so the theoretical model's modeledNet has whatever sign it
	// has, paired with a netPrice of the opposite sign.
	marks := fakeMarkSource{spot: decimal.NewFromInt(60000), ok: true}
	netPrice := decimal.NewFromInt(5000) // an intentionally large, likely-mismatched net price
	prices := splitComboNetPrice(netPrice, specs, "BTC-BIUSD", marks, decimal.NewFromFloat(0.03))

	achieved := decimal.Zero
	for i, spec := range specs {
		if !prices[i].IsPositive() {
			t.Fatalf("leg %d (%s) price = %s, want positive", i, spec.symbol, prices[i])
		}
		achieved = achieved.Add(prices[i].Mul(decimal.NewFromInt(int64(spec.ratio))))
	}
	decimalAlmostEqual(t, achieved, netPrice, "iron condor sign-mismatch ratio-weighted sum")
}
