package risk

import (
	"testing"

	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

func comboOrder(side models.OrderSide, legs []models.ComboLeg, price string) *models.Order {
	return &models.Order{
		ID: "c1", AccountID: "acct", Market: models.ComboOptions, Side: side, Type: models.Limit,
		Price: decimal.RequireFromString(price), Quantity: decimal.NewFromInt(1),
		ComboLegs: legs, QuoteCurrency: "BIUSD",
	}
}

func verticalLegs() []models.ComboLeg {
	return []models.ComboLeg{
		{Symbol: "BTC-BIUSD-60000-20260101-CALL", Ratio: 1},
		{Symbol: "BTC-BIUSD-65000-20260101-CALL", Ratio: -1},
	}
}

func TestComboLegSpecs_ParsesAllLegs(t *testing.T) {
	o := comboOrder(models.Buy, verticalLegs(), "400")
	specs, ok := comboLegSpecs(o)
	if !ok {
		t.Fatal("expected comboLegSpecs to succeed")
	}
	if len(specs) != 2 {
		t.Fatalf("got %d specs, want 2", len(specs))
	}
	if !specs[0].Strike.Equal(decimal.NewFromInt(60000)) || specs[0].OptionType != "CALL" || specs[0].Ratio != 1 {
		t.Fatalf("leg 0 = %+v, want strike 60000 CALL ratio 1", specs[0])
	}
	if !specs[1].Strike.Equal(decimal.NewFromInt(65000)) || specs[1].OptionType != "CALL" || specs[1].Ratio != -1 {
		t.Fatalf("leg 1 = %+v, want strike 65000 CALL ratio -1", specs[1])
	}
}

func TestComboLegSpecs_ParsesIronCondor(t *testing.T) {
	legs := []models.ComboLeg{
		{Symbol: "BTC-BIUSD-50000-20260101-PUT", Ratio: 1},
		{Symbol: "BTC-BIUSD-55000-20260101-PUT", Ratio: -1},
		{Symbol: "BTC-BIUSD-65000-20260101-CALL", Ratio: -1},
		{Symbol: "BTC-BIUSD-70000-20260101-CALL", Ratio: 1},
	}
	o := comboOrder(models.Buy, legs, "800")
	specs, ok := comboLegSpecs(o)
	if !ok || len(specs) != 4 {
		t.Fatalf("expected 4 parsed specs for an iron condor, got %d (ok=%v)", len(specs), ok)
	}
}

func TestComboLegSpecs_RejectsMalformedSymbol(t *testing.T) {
	legs := []models.ComboLeg{
		{Symbol: "not-a-real-symbol", Ratio: 1},
		{Symbol: "BTC-BIUSD-65000-20260101-CALL", Ratio: -1},
	}
	o := comboOrder(models.Buy, legs, "400")
	if _, ok := comboLegSpecs(o); ok {
		t.Fatal("expected comboLegSpecs to fail on a malformed leg symbol")
	}
}

func TestComboLegSpecs_RejectsEmptyLegs(t *testing.T) {
	o := comboOrder(models.Buy, nil, "400")
	if _, ok := comboLegSpecs(o); ok {
		t.Fatal("expected comboLegSpecs to fail with no legs")
	}
}

func TestNotionalFor_ComboBuyNetDebitNeedsNoMargin(t *testing.T) {
	// price=400 (net debit, since a BUY combo order specifies the debit it's
	// willing to pay): netCredit = -400 -> margin = 0.
	o := comboOrder(models.Buy, verticalLegs(), "400")
	got := notionalFor(o, decimal.NewFromInt(1), decimal.NewFromInt(400))
	if !got.IsZero() {
		t.Fatalf("combo net-debit margin = %s, want 0", got)
	}
}

func TestNotionalFor_ComboBuyNetCreditNeedsStrikeDistanceMinusCredit(t *testing.T) {
	// verticalLegs() is long the LOWER strike call, short the HIGHER strike
	// call — a bull call spread, which by construction cannot lose money
	// (its worst-case payoff floors at 0, never negative). A "credit"
	// input on top of a structure that already cannot lose needs no
	// margin at all: max(0, 0 - credit) = 0. This mirrors
	// VerticalSpreadMargin's identical behavior for the same leg shape.
	o := comboOrder(models.Buy, verticalLegs(), "-500")
	got := notionalFor(o, decimal.NewFromInt(1), decimal.NewFromInt(-500))
	if !got.IsZero() {
		t.Fatalf("combo margin for a can't-lose structure with a credit = %s, want 0", got)
	}
}

func TestNotionalFor_ComboCreditCollectingShortCallSpread(t *testing.T) {
	// The credit-collecting mirror of verticalLegs(): short the LOWER
	// strike call, long the HIGHER strike call — this structure's
	// worst-case loss is the full strike distance, so a real credit
	// received against it correctly needs strikeDistance - credit.
	//
	// notionalFor's ComboOptions case derives netCredit = price.Neg(), and a
	// BUY order's price is the net DEBIT the buyer is willing to pay to open
	// the combo — so a NEGATIVE price means the buyer is actually being
	// paid (a credit) to take this position. price=-500 -> netCredit=500.
	legs := []models.ComboLeg{
		{Symbol: "BTC-BIUSD-60000-20260101-CALL", Ratio: -1},
		{Symbol: "BTC-BIUSD-65000-20260101-CALL", Ratio: 1},
	}
	o := comboOrder(models.Buy, legs, "-500")
	got := notionalFor(o, decimal.NewFromInt(1), decimal.NewFromInt(-500))
	want := decimal.NewFromInt(4500) // strikeDistance(5000) - credit(500)
	if !got.Equal(want) {
		t.Fatalf("credit-collecting short call spread margin = %s, want %s", got, want)
	}
}

func TestNotionalFor_ComboSellNeedsNoNewMargin(t *testing.T) {
	// A SELL combo order closes/reverses an existing position — no fresh
	// margin reservation of its own, same as a reduce-only futures close.
	o := comboOrder(models.Sell, verticalLegs(), "400")
	got := notionalFor(o, decimal.NewFromInt(1), decimal.NewFromInt(400))
	if !got.IsZero() {
		t.Fatalf("combo SELL margin = %s, want 0", got)
	}
}

func TestNotionalFor_ComboMalformedLegsFailsClosedToZero(t *testing.T) {
	legs := []models.ComboLeg{{Symbol: "garbage", Ratio: 1}, {Symbol: "also-garbage", Ratio: -1}}
	o := comboOrder(models.Buy, legs, "400")
	got := notionalFor(o, decimal.NewFromInt(1), decimal.NewFromInt(400))
	if !got.IsZero() {
		t.Fatalf("malformed combo margin = %s, want 0 (fail-open is handled by validateAndPrepareCombo rejecting this before Check runs)", got)
	}
}

func TestNotionalFor_ComboIronCondor(t *testing.T) {
	legs := []models.ComboLeg{
		{Symbol: "BTC-BIUSD-50000-20260101-PUT", Ratio: 1},
		{Symbol: "BTC-BIUSD-55000-20260101-PUT", Ratio: -1},
		{Symbol: "BTC-BIUSD-65000-20260101-CALL", Ratio: -1},
		{Symbol: "BTC-BIUSD-70000-20260101-CALL", Ratio: 1},
	}
	// Opened for an 800 credit -> margin = wing width (5000) - 800 = 4200.
	o := comboOrder(models.Buy, legs, "-800")
	got := notionalFor(o, decimal.NewFromInt(1), decimal.NewFromInt(-800))
	want := decimal.NewFromInt(4200)
	if !got.Equal(want) {
		t.Fatalf("iron condor combo margin = %s, want %s", got, want)
	}
}

func TestAssetFor_ComboUsesQuoteCurrencyThenFirstLegSymbol(t *testing.T) {
	withQuote := comboOrder(models.Buy, verticalLegs(), "400")
	if got := assetFor(withQuote); got != "BIUSD" {
		t.Fatalf("assetFor with QuoteCurrency set = %s, want BIUSD", got)
	}

	noQuote := comboOrder(models.Buy, verticalLegs(), "400")
	noQuote.QuoteCurrency = ""
	if got := assetFor(noQuote); got != "BIUSD" {
		t.Fatalf("assetFor falling back to first-leg symbol parse = %s, want BIUSD", got)
	}
}
