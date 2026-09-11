package settlement

import (
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/shopspring/decimal"
)

func TestOptionsPosition_PnL_ShortLosesAsValueRises(t *testing.T) {
	pos := &OptionsPosition{Size: decimal.NewFromInt(-1), Premium: decimal.NewFromInt(-500)}
	// Received 500 to write; option now worth 700 to buy back -> lost 200.
	got := pos.PnL(decimal.NewFromInt(700))
	want := decimal.NewFromInt(-200)
	if !got.Equal(want) {
		t.Fatalf("short PnL = %s, want %s", got, want)
	}
}

func TestOptionsPosition_PnL_ShortProfitsAsValueFalls(t *testing.T) {
	pos := &OptionsPosition{Size: decimal.NewFromInt(-1), Premium: decimal.NewFromInt(-500)}
	// Received 500; option now worth 100 to buy back -> gained 400.
	got := pos.PnL(decimal.NewFromInt(100))
	want := decimal.NewFromInt(400)
	if !got.Equal(want) {
		t.Fatalf("short PnL = %s, want %s", got, want)
	}
}

func TestOptionsPosition_PnL_LongGainsAsValueRises(t *testing.T) {
	pos := &OptionsPosition{Size: decimal.NewFromInt(1), Premium: decimal.NewFromInt(500)}
	got := pos.PnL(decimal.NewFromInt(700))
	want := decimal.NewFromInt(200)
	if !got.Equal(want) {
		t.Fatalf("long PnL = %s, want %s", got, want)
	}
}

func TestOptionsPosition_PnL_ScalesWithSize(t *testing.T) {
	pos := &OptionsPosition{Size: decimal.NewFromInt(-3), Premium: decimal.NewFromInt(-1500)}
	got := pos.PnL(decimal.NewFromInt(700))
	want := decimal.NewFromInt(-600) // (500 - 700) * 3
	if !got.Equal(want) {
		t.Fatalf("scaled short PnL = %s, want %s", got, want)
	}
}

func TestOptionsPosition_PnL_ZeroSizeIsZero(t *testing.T) {
	pos := &OptionsPosition{Size: decimal.Zero, Premium: decimal.NewFromInt(-500)}
	if got := pos.PnL(decimal.NewFromInt(999)); !got.IsZero() {
		t.Fatalf("zero-size PnL = %s, want 0", got)
	}
}

func openWriterPosition(t *testing.T, ledger *risk.Ledger, os *OptionsSettlement, writer, symbol, optionType string, strike, qty, premium decimal.Decimal, expiry time.Time) {
	t.Helper()
	buyOrder := &models.Order{
		AccountID: "buyer", Symbol: symbol, Market: models.Options, Side: models.Buy,
		OptionType: optionType, StrikePrice: strike, Expiry: expiry, QuoteCurrency: "BIUSDB",
	}
	sellOrder := &models.Order{
		AccountID: writer, Symbol: symbol, Market: models.Options, Side: models.Sell,
		OptionType: optionType, StrikePrice: strike, Expiry: expiry, QuoteCurrency: "BIUSDB",
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
	ledger.Deposit("buyer", "BIUSDB", decimal.NewFromInt(1_000_000))
	ledger.Deposit("writer", "BIUSDB", decimal.NewFromInt(1_000_000))
	os := NewOptionsSettlement(ledger, &backendclient.Client{})

	expiry := time.Now().Add(24 * time.Hour)
	symbol := "BTC-BIUSDB-60000-20260101-CALL"
	strike := decimal.NewFromInt(60000)
	openWriterPosition(t, ledger, os, "writer", symbol, "CALL", strike, decimal.NewFromInt(1), decimal.NewFromInt(500), expiry)

	// Reserve the collateral this position "would have locked" (mirroring
	// what the order-time risk check actually reserves) so ForceClosePosition
	// has something real to release.
	reserved := decimal.NewFromInt(60000)
	if err := ledger.Reserve("writer", "BIUSDB", reserved); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	mark := decimal.NewFromInt(700) // deep loss for the writer
	pnl, err := os.ForceClosePosition("writer", symbol, strike, expiry, "CALL", reserved, mark)
	if err != nil {
		t.Fatalf("force close: %v", err)
	}
	wantPnL := decimal.NewFromInt(-200)
	if !pnl.Equal(wantPnL) {
		t.Fatalf("force-close pnl = %s, want %s", pnl, wantPnL)
	}

	if pos := os.GetPosition("writer", symbol, strike, expiry, "CALL"); pos != nil {
		t.Fatalf("expected position to be removed after force-close, still present: %+v", pos)
	}

	// Balance: started 1,000,000, received 500 premium already (via Settle),
	// reserved 60000 (now released), then lost 200 on close ->
	// 1,000,000 + 500 - 200 = 1,000,300.
	got := ledger.Available("writer", "BIUSDB")
	want := decimal.NewFromInt(1_000_000).Add(decimal.NewFromInt(500)).Sub(decimal.NewFromInt(200))
	if !got.Equal(want) {
		t.Fatalf("writer balance after force-close = %s, want %s", got, want)
	}
}

func TestForceClosePosition_UnknownPositionIsNoop(t *testing.T) {
	ledger := risk.NewLedger()
	os := NewOptionsSettlement(ledger, &backendclient.Client{})
	pnl, err := os.ForceClosePosition("nobody", "BTC-BIUSDB-60000-20260101-CALL", decimal.NewFromInt(60000), time.Now().Add(time.Hour), "CALL", decimal.Zero, decimal.NewFromInt(100))
	if err != nil {
		t.Fatalf("unexpected error for unknown position: %v", err)
	}
	if !pnl.IsZero() {
		t.Fatalf("pnl for unknown position = %s, want 0", pnl)
	}
}
