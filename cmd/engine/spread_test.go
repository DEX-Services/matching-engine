package main

import (
	"context"
	"testing"
	"time"

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

func TestComboSymbolFor_DeterministicAndOrderSensitive(t *testing.T) {
	a := comboSymbolFor("BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD-65000-20260101-CALL")
	b := comboSymbolFor("BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD-65000-20260101-CALL")
	if a != b {
		t.Fatalf("comboSymbolFor must be deterministic for the same inputs: %s vs %s", a, b)
	}
	// Swapping which leg is "buy" vs "sell" is a DIFFERENT combo (going long
	// the 60k/65k spread is not the same instrument as going long the
	// 65k/60k spread — the buy/sell assignment IS the spread's identity).
	reversed := comboSymbolFor("BTC-BIUSD-65000-20260101-CALL", "BTC-BIUSD-60000-20260101-CALL")
	if a == reversed {
		t.Fatal("comboSymbolFor must distinguish leg order (buy vs sell assignment defines the spread)")
	}
}

func TestGetOrCreateComboInstrument_MemoryFallbackRoundTrips(t *testing.T) {
	ctx := context.Background()
	expiry := time.Now().Add(24 * time.Hour)
	buy := sampleInstrument("BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD", "CALL", "60000", expiry)
	sell := sampleInstrument("BTC-BIUSD-65000-20260101-CALL", "BTC-BIUSD", "CALL", "65000", expiry)

	created, err := getOrCreateComboInstrument(ctx, nil, buy, sell)
	if err != nil {
		t.Fatalf("getOrCreateComboInstrument: %v", err)
	}

	loaded, err := loadComboInstrument(ctx, nil, created.Symbol)
	if err != nil {
		t.Fatalf("loadComboInstrument: %v", err)
	}
	if loaded.BuySymbol != buy.Symbol || loaded.SellSymbol != sell.Symbol || loaded.Underlying != buy.Underlying {
		t.Fatalf("loaded combo instrument = %+v, want legs %s/%s underlying %s", loaded, buy.Symbol, sell.Symbol, buy.Underlying)
	}
}

func TestLoadComboInstrument_UnknownSymbolErrors(t *testing.T) {
	if _, err := loadComboInstrument(context.Background(), nil, "COMBO:doesnotexist"); err == nil {
		t.Fatal("expected an error looking up an unregistered combo symbol")
	}
}
