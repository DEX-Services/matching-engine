package main

import (
	"testing"

	"github.com/dex/matching-engine/internal/risk"
	"github.com/shopspring/decimal"
)

// TestSpotPositionSizer_ReportsTotalBalance covers spotPositionSizer (added
// 2026-09-16 for SPOT TP/SL support): "current exposure" for a spot holding
// is its current total (available + reserved) balance of the base asset,
// so an account's own TP/SL reservation on that holding doesn't make its
// own protection look like it's shrinking to the resize listener.
func TestSpotPositionSizer_ReportsTotalBalance(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Deposit("trader", "BI2X", decimal.NewFromInt(10))
	ledger.Reserve("trader", "BI2X", decimal.NewFromInt(4)) // e.g. the group's own shared TP/SL reservation

	sizer := &spotPositionSizer{ledger: ledger}
	got := sizer.CurrentSize("trader", "BI2X-BI2XUSD")
	if !got.Equal(decimal.NewFromInt(10)) {
		t.Fatalf("CurrentSize = %s, want 10 (total balance, not just available)", got)
	}
}

// TestSpotPositionSizer_ShrinksAfterManualSell confirms the whole point of
// this type: a manual spot sell (via Debit, exactly what real settlement
// does on a fill) reduces the reported exposure, which is what lets
// onExposureChanged shrink/cancel a resting TP/SL after the user sells some
// of the asset elsewhere — the gap that had no equivalent for spot before
// this, since spot has no "position" object to report a resize event for.
func TestSpotPositionSizer_ShrinksAfterManualSell(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Deposit("trader", "BI2X", decimal.NewFromInt(10))
	sizer := &spotPositionSizer{ledger: ledger}
	if got := sizer.CurrentSize("trader", "BI2X-BI2XUSD"); !got.Equal(decimal.NewFromInt(10)) {
		t.Fatalf("CurrentSize before sell = %s, want 10", got)
	}

	if err := ledger.Debit("trader", "BI2X", decimal.NewFromInt(6)); err != nil {
		t.Fatalf("debit: %v", err)
	}
	if got := sizer.CurrentSize("trader", "BI2X-BI2XUSD"); !got.Equal(decimal.NewFromInt(4)) {
		t.Fatalf("CurrentSize after selling 6 of 10 = %s, want 4", got)
	}
}

// TestSpotPositionSizer_UnknownAccountIsZero confirms a never-deposited
// account reports zero exposure rather than panicking or erroring — the
// resize listener treats zero exposure as "cancel the group entirely" (see
// attached.Listener.onExposureChanged), which is exactly right for an
// account with no holding at all.
func TestSpotPositionSizer_UnknownAccountIsZero(t *testing.T) {
	ledger := risk.NewLedger()
	sizer := &spotPositionSizer{ledger: ledger}
	got := sizer.CurrentSize("nobody", "BI2X-BI2XUSD")
	if !got.IsZero() {
		t.Fatalf("CurrentSize for unknown account = %s, want 0", got)
	}
}

// TestSpotPositionSizer_MalformedSymbolIsZero confirms a symbol with no "-"
// separator (should never happen for a real engine symbol, but defensively)
// returns zero rather than panicking.
func TestSpotPositionSizer_MalformedSymbolIsZero(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Deposit("trader", "BI2X", decimal.NewFromInt(10))
	sizer := &spotPositionSizer{ledger: ledger}
	got := sizer.CurrentSize("trader", "BI2X")
	if !got.IsZero() {
		t.Fatalf("CurrentSize for malformed symbol = %s, want 0", got)
	}
}
