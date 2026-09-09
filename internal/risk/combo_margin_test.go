package risk

import (
	"testing"

	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

func comboOrder(side models.OrderSide, buySymbol, sellSymbol, price string) *models.Order {
	return &models.Order{
		ID: "c1", AccountID: "acct", Market: models.ComboOptions, Side: side, Type: models.Limit,
		Price: decimal.RequireFromString(price), Quantity: decimal.NewFromInt(1),
		ComboBuySymbol: buySymbol, ComboSellSymbol: sellSymbol, QuoteCurrency: "BIUSD",
	}
}

func TestComboLegStrikes_ParsesBothLegs(t *testing.T) {
	o := comboOrder(models.Buy, "BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD-65000-20260101-CALL", "400")
	buyStrike, sellStrike, ok := comboLegStrikes(o)
	if !ok {
		t.Fatal("expected comboLegStrikes to succeed")
	}
	if !buyStrike.Equal(decimal.NewFromInt(60000)) || !sellStrike.Equal(decimal.NewFromInt(65000)) {
		t.Fatalf("strikes = (%s, %s), want (60000, 65000)", buyStrike, sellStrike)
	}
}

func TestComboLegStrikes_RejectsMalformedSymbol(t *testing.T) {
	o := comboOrder(models.Buy, "not-a-real-symbol", "BTC-BIUSD-65000-20260101-CALL", "400")
	if _, _, ok := comboLegStrikes(o); ok {
		t.Fatal("expected comboLegStrikes to fail on a malformed leg symbol")
	}
}

func TestNotionalFor_ComboBuyNetDebitNeedsNoMargin(t *testing.T) {
	// price=400 (net debit, since a BUY combo order specifies the debit it's
	// willing to pay): netCredit = -400 -> VerticalSpreadMargin = 0.
	o := comboOrder(models.Buy, "BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD-65000-20260101-CALL", "400")
	got := notionalFor(o, decimal.NewFromInt(1), decimal.NewFromInt(400))
	if !got.IsZero() {
		t.Fatalf("combo net-debit margin = %s, want 0", got)
	}
}

func TestNotionalFor_ComboBuyNetCreditNeedsStrikeDistanceMinusCredit(t *testing.T) {
	// A BUY order priced at a NEGATIVE net price means the trader is
	// actually receiving a credit to open the spread (short call spread
	// bought "negative debit"). netCredit = -(-500) = 500.
	// strikeDistance(5000) - 500 = 4500.
	o := comboOrder(models.Buy, "BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD-65000-20260101-CALL", "-500")
	got := notionalFor(o, decimal.NewFromInt(1), decimal.NewFromInt(-500))
	want := decimal.NewFromInt(4500)
	if !got.Equal(want) {
		t.Fatalf("combo net-credit margin = %s, want %s", got, want)
	}
}

func TestNotionalFor_ComboSellNeedsNoNewMargin(t *testing.T) {
	// A SELL combo order closes/reverses an existing position — no fresh
	// margin reservation of its own, same as a reduce-only futures close.
	o := comboOrder(models.Sell, "BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD-65000-20260101-CALL", "400")
	got := notionalFor(o, decimal.NewFromInt(1), decimal.NewFromInt(400))
	if !got.IsZero() {
		t.Fatalf("combo SELL margin = %s, want 0", got)
	}
}

func TestNotionalFor_ComboMalformedLegsFailsClosedToZero(t *testing.T) {
	o := comboOrder(models.Buy, "garbage", "also-garbage", "400")
	got := notionalFor(o, decimal.NewFromInt(1), decimal.NewFromInt(400))
	if !got.IsZero() {
		t.Fatalf("malformed combo margin = %s, want 0 (fail-open is handled by validateAndPrepareCombo rejecting this before Check runs)", got)
	}
}

func TestAssetFor_ComboUsesQuoteCurrencyThenBuyLegSymbol(t *testing.T) {
	withQuote := comboOrder(models.Buy, "BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD-65000-20260101-CALL", "400")
	if got := assetFor(withQuote); got != "BIUSD" {
		t.Fatalf("assetFor with QuoteCurrency set = %s, want BIUSD", got)
	}

	noQuote := comboOrder(models.Buy, "BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD-65000-20260101-CALL", "400")
	noQuote.QuoteCurrency = ""
	if got := assetFor(noQuote); got != "BIUSD" {
		t.Fatalf("assetFor falling back to buy-leg symbol parse = %s, want BIUSD", got)
	}
}
