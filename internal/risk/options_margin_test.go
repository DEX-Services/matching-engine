package risk

import (
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

// fakeMarkSource is a minimal UnderlyingMarkSource for testing shortOptionMargin
// without spinning up a real marketdata.Service/order book.
type fakeMarkSource map[string]decimal.Decimal

func (f fakeMarkSource) UnderlyingMark(symbol string) (decimal.Decimal, bool) {
	v, ok := f[symbol]
	return v, ok
}

func newOptionOrder(side models.OrderSide, symbol, optionType, strike string, expiry time.Time) *models.Order {
	return &models.Order{
		ID: "o1", AccountID: "writer", Symbol: symbol, Market: models.Options,
		Side: side, Type: models.Limit, OptionType: optionType,
		StrikePrice: decimal.RequireFromString(strike), Expiry: expiry,
		QuoteCurrency: "BIUSDB", Quantity: decimal.NewFromInt(1),
	}
}

func TestOptionsMargin_FallsBackToCashSecuredWithoutMarkSource(t *testing.T) {
	markSource = nil // ensure no leakage from other tests
	order := newOptionOrder(models.Sell, "BTC-BIUSDB-60000-20260101-CALL", "CALL", "60000", time.Now().Add(24*time.Hour))
	got := shortOptionMargin(order, decimal.NewFromInt(1), decimal.NewFromInt(500))
	want := decimal.NewFromInt(60000)
	if !got.Equal(want) {
		t.Fatalf("shortOptionMargin without mark source = %s, want %s (cash-secured)", got, want)
	}
}

func TestOptionsMargin_OTMCallUsesFloorBelowCashSecured(t *testing.T) {
	SetMarkSource(fakeMarkSource{"BTC-BIUSDB": decimal.NewFromInt(50000)})
	t.Cleanup(func() { markSource = nil })

	// OTM call: strike 60000, spot 50000, premium received 500, qty 1.
	// floor = premium(500) + 20% * spot notional (50000) = 500 + 10000 = 10500
	// itm = max(0, 50000-60000) = 0
	// required = max(10500, 0) = 10500, well under cash-secured 60000.
	order := newOptionOrder(models.Sell, "BTC-BIUSDB-60000-20260101-CALL", "CALL", "60000", time.Now().Add(24*time.Hour))
	got := shortOptionMargin(order, decimal.NewFromInt(1), decimal.NewFromInt(500))
	want := decimal.NewFromInt(10500)
	if !got.Equal(want) {
		t.Fatalf("shortOptionMargin OTM call = %s, want %s", got, want)
	}
	if got.GreaterThan(decimal.NewFromInt(60000)) {
		t.Fatalf("shortOptionMargin must never exceed cash-secured strike*qty; got %s", got)
	}
}

func TestOptionsMargin_ITMPutUsesIntrinsicFloor(t *testing.T) {
	SetMarkSource(fakeMarkSource{"BTC-BIUSDB": decimal.NewFromInt(40000)})
	t.Cleanup(func() { markSource = nil })

	// ITM put: strike 60000, spot 40000, premium received 200, qty 1.
	// floor = 200 + 20% * 40000 = 200 + 8000 = 8200
	// itm = max(0, 60000-40000) = 20000
	// required = max(8200, 20000) = 20000
	order := newOptionOrder(models.Sell, "BTC-BIUSDB-60000-20260101-PUT", "PUT", "60000", time.Now().Add(24*time.Hour))
	got := shortOptionMargin(order, decimal.NewFromInt(1), decimal.NewFromInt(200))
	want := decimal.NewFromInt(20000)
	if !got.Equal(want) {
		t.Fatalf("shortOptionMargin ITM put = %s, want %s", got, want)
	}
}

func TestOptionsMargin_NeverExceedsCashSecured(t *testing.T) {
	// Deep ITM with a huge premium: floor/itm could theoretically exceed
	// strike*qty (e.g. an absurd premium input) — must still be capped.
	SetMarkSource(fakeMarkSource{"BTC-BIUSDB": decimal.NewFromInt(200000)})
	t.Cleanup(func() { markSource = nil })

	order := newOptionOrder(models.Sell, "BTC-BIUSDB-60000-20260101-CALL", "CALL", "60000", time.Now().Add(24*time.Hour))
	got := shortOptionMargin(order, decimal.NewFromInt(1), decimal.NewFromInt(999999))
	cashSecured := decimal.NewFromInt(60000)
	if !got.Equal(cashSecured) {
		t.Fatalf("shortOptionMargin must cap at cash-secured %s, got %s", cashSecured, got)
	}
}

func TestOptionsMargin_BuyerAlwaysPaysPremiumRegardlessOfMarkSource(t *testing.T) {
	SetMarkSource(fakeMarkSource{"BTC-BIUSDB": decimal.NewFromInt(50000)})
	t.Cleanup(func() { markSource = nil })

	order := newOptionOrder(models.Buy, "BTC-BIUSDB-60000-20260101-CALL", "CALL", "60000", time.Now().Add(24*time.Hour))
	got := notionalFor(order, decimal.NewFromInt(2), decimal.NewFromInt(500))
	want := decimal.NewFromInt(1000) // premium * qty, untouched by the margin floor logic
	if !got.Equal(want) {
		t.Fatalf("buyer notional = %s, want %s", got, want)
	}
}
