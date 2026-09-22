package settlement

import (
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/fixedpoint"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
)

func TestOptionsPosition_PnL_ShortLosesAsValueRises(t *testing.T) {
	pos := &OptionsPosition{Size: fixedpoint.FromInt64(-1), Premium: fixedpoint.FromInt64(-500)}
	// Received 500 to write; option now worth 700 to buy back -> lost 200.
	got := pos.PnL(fixedpoint.FromInt64(700))
	want := fixedpoint.FromInt64(-200)
	if !got.Equal(want) {
		t.Fatalf("short PnL = %s, want %s", got, want)
	}
}

func TestOptionsPosition_PnL_ShortProfitsAsValueFalls(t *testing.T) {
	pos := &OptionsPosition{Size: fixedpoint.FromInt64(-1), Premium: fixedpoint.FromInt64(-500)}
	// Received 500; option now worth 100 to buy back -> gained 400.
	got := pos.PnL(fixedpoint.FromInt64(100))
	want := fixedpoint.FromInt64(400)
	if !got.Equal(want) {
		t.Fatalf("short PnL = %s, want %s", got, want)
	}
}

func TestOptionsPosition_PnL_LongGainsAsValueRises(t *testing.T) {
	pos := &OptionsPosition{Size: fixedpoint.FromInt64(1), Premium: fixedpoint.FromInt64(500)}
	got := pos.PnL(fixedpoint.FromInt64(700))
	want := fixedpoint.FromInt64(200)
	if !got.Equal(want) {
		t.Fatalf("long PnL = %s, want %s", got, want)
	}
}

func TestOptionsPosition_PnL_ScalesWithSize(t *testing.T) {
	pos := &OptionsPosition{Size: fixedpoint.FromInt64(-3), Premium: fixedpoint.FromInt64(-1500)}
	got := pos.PnL(fixedpoint.FromInt64(700))
	want := fixedpoint.FromInt64(-600) // (500 - 700) * 3
	if !got.Equal(want) {
		t.Fatalf("scaled short PnL = %s, want %s", got, want)
	}
}

func TestOptionsPosition_PnL_ZeroSizeIsZero(t *testing.T) {
	pos := &OptionsPosition{Size: fixedpoint.Zero, Premium: fixedpoint.FromInt64(-500)}
	if got := pos.PnL(fixedpoint.FromInt64(999)); !got.IsZero() {
		t.Fatalf("zero-size PnL = %s, want 0", got)
	}
}

func openWriterPosition(t *testing.T, ledger *risk.Ledger, os *OptionsSettlement, writer, symbol, optionType string, strike, qty, premium fixedpoint.Fixed, expiry time.Time) {
	t.Helper()
	buyOrder := &models.Order{
		AccountID: "buyer", Symbol: symbol, Market: models.Options, Side: models.Buy,
		OptionType: optionType, StrikePrice: strike, Expiry: expiry, QuoteCurrency: "BI2XUSD",
	}
	sellOrder := &models.Order{
		AccountID: writer, Symbol: symbol, Market: models.Options, Side: models.Sell,
		OptionType: optionType, StrikePrice: strike, Expiry: expiry, QuoteCurrency: "BI2XUSD",
	}
	trade := &models.Trade{
		Symbol: symbol, Market: models.Options, Price: premium, Quantity: qty,
		BuyOrder: buyOrder, SellOrder: sellOrder, ExecutedAt: time.Now(),
	}
	if err := os.Settle(trade); err != nil {
		t.Fatalf("settle: %v", err)
	}
}

func TestForceClosePosition_DebitsLossAndRemovesPosition(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Deposit("buyer", "BI2XUSD", fixedpoint.FromInt64(1_000_000))
	ledger.Deposit("writer", "BI2XUSD", fixedpoint.FromInt64(1_000_000))
	os := NewOptionsSettlement(ledger, &backendclient.Client{})

	expiry := time.Now().Add(24 * time.Hour)
	symbol := "BTC-BI2XUSD-60000-20260101-CALL"
	strike := fixedpoint.FromInt64(60000)
	openWriterPosition(t, ledger, os, "writer", symbol, "CALL", strike, fixedpoint.FromInt64(1), fixedpoint.FromInt64(500), expiry)

	// Reserve the collateral this position "would have locked" (mirroring
	// what the order-time risk check actually reserves) so ForceClosePosition
	// has something real to release.
	reserved := fixedpoint.FromInt64(60000)
	if err := ledger.Reserve("writer", "BI2XUSD", reserved); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	mark := fixedpoint.FromInt64(700) // deep loss for the writer
	pnl, err := os.ForceClosePosition("writer", symbol, strike, expiry, "CALL", reserved, mark)
	if err != nil {
		t.Fatalf("force close: %v", err)
	}
	wantPnL := fixedpoint.FromInt64(-200)
	if !pnl.Equal(wantPnL) {
		t.Fatalf("force-close pnl = %s, want %s", pnl, wantPnL)
	}

	if pos := os.GetPosition("writer", symbol, strike, expiry, "CALL"); pos != nil {
		t.Fatalf("expected position to be removed after force-close, still present: %+v", pos)
	}

	// Balance: started 1,000,000, received 500 premium already (via Settle),
	// reserved 60000 (now released), then lost 200 on close ->
	// 1,000,000 + 500 - 200 = 1,000,300.
	got := ledger.Available("writer", "BI2XUSD")
	want := fixedpoint.FromInt64(1_000_000).Add(fixedpoint.FromInt64(500)).Sub(fixedpoint.FromInt64(200))
	if !got.Equal(want) {
		t.Fatalf("writer balance after force-close = %s, want %s", got, want)
	}
}

func TestForceClosePosition_UnknownPositionIsNoop(t *testing.T) {
	ledger := risk.NewLedger()
	os := NewOptionsSettlement(ledger, &backendclient.Client{})
	pnl, err := os.ForceClosePosition("nobody", "BTC-BI2XUSD-60000-20260101-CALL", fixedpoint.FromInt64(60000), time.Now().Add(time.Hour), "CALL", fixedpoint.Zero, fixedpoint.FromInt64(100))
	if err != nil {
		t.Fatalf("unexpected error for unknown position: %v", err)
	}
	if !pnl.IsZero() {
		t.Fatalf("pnl for unknown position = %s, want 0", pnl)
	}
}
