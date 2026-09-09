package main

import (
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/events"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/shopspring/decimal"
)

func sampleInstrument(symbol, underlying, optionType, strike string, expiry time.Time) *optionInstrument {
	return &optionInstrument{
		Symbol: symbol, Underlying: underlying, OptionType: optionType,
		Strike: decimal.RequireFromString(strike), Expiry: expiry,
	}
}

func TestValidateVerticalPair_AcceptsValidVertical(t *testing.T) {
	expiry := time.Now().Add(24 * time.Hour)
	buy := sampleInstrument("BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD", "CALL", "60000", expiry)
	sell := sampleInstrument("BTC-BIUSD-65000-20260101-CALL", "BTC-BIUSD", "CALL", "65000", expiry)
	if err := validateVerticalPair(buy, sell); err != nil {
		t.Fatalf("expected a valid vertical, got error: %v", err)
	}
}

func TestValidateVerticalPair_RejectsDifferentUnderlying(t *testing.T) {
	expiry := time.Now().Add(24 * time.Hour)
	buy := sampleInstrument("BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD", "CALL", "60000", expiry)
	sell := sampleInstrument("ETH-BIUSD-65000-20260101-CALL", "ETH-BIUSD", "CALL", "65000", expiry)
	if err := validateVerticalPair(buy, sell); err == nil {
		t.Fatal("expected an error for mismatched underlyings")
	}
}

func TestValidateVerticalPair_RejectsDifferentExpiry(t *testing.T) {
	buy := sampleInstrument("BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD", "CALL", "60000", time.Now().Add(24*time.Hour))
	sell := sampleInstrument("BTC-BIUSD-65000-20260201-CALL", "BTC-BIUSD", "CALL", "65000", time.Now().Add(48*time.Hour))
	if err := validateVerticalPair(buy, sell); err == nil {
		t.Fatal("expected an error for mismatched expiries")
	}
}

func TestValidateVerticalPair_RejectsDifferentOptionType(t *testing.T) {
	expiry := time.Now().Add(24 * time.Hour)
	buy := sampleInstrument("BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD", "CALL", "60000", expiry)
	sell := sampleInstrument("BTC-BIUSD-65000-20260101-PUT", "BTC-BIUSD", "PUT", "65000", expiry)
	if err := validateVerticalPair(buy, sell); err == nil {
		t.Fatal("expected an error for mismatched option types")
	}
}

func TestValidateVerticalPair_RejectsSameStrike(t *testing.T) {
	expiry := time.Now().Add(24 * time.Hour)
	buy := sampleInstrument("BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD", "CALL", "60000", expiry)
	sell := sampleInstrument("BTC-BIUSD-60000-20260101-CALL-2", "BTC-BIUSD", "CALL", "60000", expiry)
	if err := validateVerticalPair(buy, sell); err == nil {
		t.Fatal("expected an error for identical strikes (not a spread)")
	}
}

func TestNetVerticalMargin_ReleasesExcessAboveNetRequirement(t *testing.T) {
	bus := events.NewBus()
	d := newTestSubmitDeps(bus)
	d.ledger.Deposit("writer", "BIUSD", decimal.NewFromInt(1_000_000))
	// No mark source wired -> shortOptionMargin/RequiredOptionsMargin falls
	// back to the full cash-secured strike*qty ceiling, exactly like a
	// standalone short leg would have reserved at order time.
	risk.SetMarkSource(nil)

	expiry := time.Now().Add(24 * time.Hour)
	buy := sampleInstrument("BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD", "CALL", "60000", expiry)
	sell := sampleInstrument("BTC-BIUSD-65000-20260101-CALL", "BTC-BIUSD", "CALL", "65000", expiry)

	qty := decimal.NewFromInt(1)
	// Reserve what the short leg's own order-time check would have locked:
	// cash-secured strike*qty = 65000.
	if err := d.ledger.Reserve("writer", "BIUSD", decimal.NewFromInt(65000)); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	// Net credit of 500 -> VerticalSpreadMargin = strikeDistance(5000) - 500 = 4500.
	netCredit := decimal.NewFromInt(500)
	got := netVerticalMargin(d, "writer", buy, sell, qty, netCredit, decimal.NewFromInt(500), "BIUSD")

	want := decimal.NewFromInt(4500)
	if !got.Equal(want) {
		t.Fatalf("netVerticalMargin returned %s, want %s", got, want)
	}
	// 65000 reserved - 4500 required = 60500 should have been released back
	// to available.
	available := d.ledger.Available("writer", "BIUSD")
	wantAvailable := decimal.NewFromInt(1_000_000).Sub(decimal.NewFromInt(4500))
	if !available.Equal(wantAvailable) {
		t.Fatalf("available after netting = %s, want %s", available, wantAvailable)
	}
}

func TestNetVerticalMargin_NoReleaseWhenAlreadyAtOrBelowRequirement(t *testing.T) {
	bus := events.NewBus()
	d := newTestSubmitDeps(bus)
	d.ledger.Deposit("writer", "BIUSD", decimal.NewFromInt(1_000_000))
	risk.SetMarkSource(nil)

	expiry := time.Now().Add(24 * time.Hour)
	buy := sampleInstrument("BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD", "CALL", "60000", expiry)
	sell := sampleInstrument("BTC-BIUSD-65000-20260101-CALL", "BTC-BIUSD", "CALL", "65000", expiry)
	qty := decimal.NewFromInt(1)

	if err := d.ledger.Reserve("writer", "BIUSD", decimal.NewFromInt(65000)); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// Net DEBIT (negative netCredit) -> VerticalSpreadMargin = 0, which is
	// below the reserved 65000, so this should still release the excess
	// down to 0 (a net-debit vertical needs no margin at all).
	got := netVerticalMargin(d, "writer", buy, sell, qty, decimal.NewFromInt(-1000), decimal.NewFromInt(500), "BIUSD")
	if !got.IsZero() {
		t.Fatalf("netVerticalMargin for a net-debit spread = %s, want 0", got)
	}
	available := d.ledger.Available("writer", "BIUSD")
	if !available.Equal(decimal.NewFromInt(1_000_000)) {
		t.Fatalf("available after netting a net-debit spread = %s, want the full 1,000,000 (nothing should stay locked)", available)
	}
}
