package main

import (
	"context"
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

func sampleInstrument(symbol, underlying, optionType, strike string, expiry time.Time) *optionInstrument {
	return &optionInstrument{
		Symbol: symbol, Underlying: underlying, OptionType: optionType,
		Strike: decimal.RequireFromString(strike), Expiry: expiry,
	}
}

func TestValidateComboLegSpecs_AcceptsValidVertical(t *testing.T) {
	expiry := time.Now().Add(24 * time.Hour)
	insts := []*optionInstrument{
		sampleInstrument("BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD", "CALL", "60000", expiry),
		sampleInstrument("BTC-BIUSD-65000-20260101-CALL", "BTC-BIUSD", "CALL", "65000", expiry),
	}
	underlying, err := validateComboLegSpecs(insts)
	if err != nil {
		t.Fatalf("expected a valid vertical, got error: %v", err)
	}
	if underlying != "BTC-BIUSD" {
		t.Fatalf("underlying = %s, want BTC-BIUSD", underlying)
	}
}

func TestValidateComboLegSpecs_AcceptsIronCondor(t *testing.T) {
	expiry := time.Now().Add(24 * time.Hour)
	insts := []*optionInstrument{
		sampleInstrument("BTC-BIUSD-50000-20260101-PUT", "BTC-BIUSD", "PUT", "50000", expiry),
		sampleInstrument("BTC-BIUSD-55000-20260101-PUT", "BTC-BIUSD", "PUT", "55000", expiry),
		sampleInstrument("BTC-BIUSD-65000-20260101-CALL", "BTC-BIUSD", "CALL", "65000", expiry),
		sampleInstrument("BTC-BIUSD-70000-20260101-CALL", "BTC-BIUSD", "CALL", "70000", expiry),
	}
	if _, err := validateComboLegSpecs(insts); err != nil {
		t.Fatalf("expected a valid iron condor (4 legs, mixed types), got error: %v", err)
	}
}

func TestValidateComboLegSpecs_RejectsDifferentUnderlying(t *testing.T) {
	expiry := time.Now().Add(24 * time.Hour)
	insts := []*optionInstrument{
		sampleInstrument("BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD", "CALL", "60000", expiry),
		sampleInstrument("ETH-BIUSD-65000-20260101-CALL", "ETH-BIUSD", "CALL", "65000", expiry),
	}
	if _, err := validateComboLegSpecs(insts); err == nil {
		t.Fatal("expected an error for mismatched underlyings")
	}
}

func TestValidateComboLegSpecs_RejectsDifferentExpiry(t *testing.T) {
	insts := []*optionInstrument{
		sampleInstrument("BTC-BIUSD-60000-20260101-CALL", "BTC-BIUSD", "CALL", "60000", time.Now().Add(24*time.Hour)),
		sampleInstrument("BTC-BIUSD-65000-20260201-CALL", "BTC-BIUSD", "CALL", "65000", time.Now().Add(48*time.Hour)),
	}
	if _, err := validateComboLegSpecs(insts); err == nil {
		t.Fatal("expected an error for mismatched expiries (calendars/diagonals unsupported)")
	}
}

func TestValidateComboLegs_RejectsFewerThanTwoLegs(t *testing.T) {
	_, _, err := validateComboLegs(context.Background(), nil, []models.ComboLeg{{Symbol: "x", Ratio: 1}})
	if err == nil {
		t.Fatal("expected an error for a single-leg combo")
	}
}

func TestValidateComboLegs_RejectsZeroRatio(t *testing.T) {
	legs := []models.ComboLeg{{Symbol: "a", Ratio: 1}, {Symbol: "b", Ratio: 0}}
	_, _, err := validateComboLegs(context.Background(), nil, legs)
	if err == nil {
		t.Fatal("expected an error for a zero-ratio leg")
	}
}

func TestComboSymbolFor_DeterministicAndOrderInsensitive(t *testing.T) {
	legs := []models.ComboLeg{
		{Symbol: "BTC-BIUSD-60000-20260101-CALL", Ratio: 1},
		{Symbol: "BTC-BIUSD-65000-20260101-CALL", Ratio: -1},
	}
	a := comboSymbolFor(legs)
	b := comboSymbolFor(legs)
	if a != b {
		t.Fatalf("comboSymbolFor must be deterministic for the same inputs: %s vs %s", a, b)
	}

	// Legs listed in a different ORDER (but same symbol+ratio pairs) must
	// resolve to the identical instrument — sorted internally before hashing.
	reordered := []models.ComboLeg{legs[1], legs[0]}
	if comboSymbolFor(reordered) != a {
		t.Fatal("comboSymbolFor must be insensitive to leg list order")
	}
}

func TestComboSymbolFor_DistinguishesRatioSign(t *testing.T) {
	// Same two symbols, but which one is long vs short flips the actual
	// spread being traded (a bull call spread vs its mirror bear call
	// spread) — these must be different instruments.
	a := comboSymbolFor([]models.ComboLeg{
		{Symbol: "BTC-BIUSD-60000-20260101-CALL", Ratio: 1},
		{Symbol: "BTC-BIUSD-65000-20260101-CALL", Ratio: -1},
	})
	b := comboSymbolFor([]models.ComboLeg{
		{Symbol: "BTC-BIUSD-60000-20260101-CALL", Ratio: -1},
		{Symbol: "BTC-BIUSD-65000-20260101-CALL", Ratio: 1},
	})
	if a == b {
		t.Fatal("comboSymbolFor must distinguish which leg is long vs short")
	}
}

func TestComboSymbolFor_DistinguishesLegCount(t *testing.T) {
	vertical := comboSymbolFor([]models.ComboLeg{
		{Symbol: "BTC-BIUSD-60000-20260101-CALL", Ratio: 1},
		{Symbol: "BTC-BIUSD-65000-20260101-CALL", Ratio: -1},
	})
	butterfly := comboSymbolFor([]models.ComboLeg{
		{Symbol: "BTC-BIUSD-55000-20260101-CALL", Ratio: 1},
		{Symbol: "BTC-BIUSD-60000-20260101-CALL", Ratio: -2},
		{Symbol: "BTC-BIUSD-65000-20260101-CALL", Ratio: 1},
	})
	if vertical == butterfly {
		t.Fatal("comboSymbolFor must distinguish different leg counts/structures")
	}
}

func TestGetOrCreateComboInstrument_MemoryFallbackRoundTrips(t *testing.T) {
	ctx := context.Background()
	legs := []models.ComboLeg{
		{Symbol: "BTC-BIUSD-60000-20260101-CALL", Ratio: 1},
		{Symbol: "BTC-BIUSD-65000-20260101-CALL", Ratio: -1},
	}
	created, err := getOrCreateComboInstrument(ctx, nil, legs, "BTC-BIUSD")
	if err != nil {
		t.Fatalf("getOrCreateComboInstrument: %v", err)
	}

	loaded, err := loadComboInstrument(ctx, nil, created.Symbol)
	if err != nil {
		t.Fatalf("loadComboInstrument: %v", err)
	}
	if len(loaded.Legs) != 2 || loaded.Underlying != "BTC-BIUSD" {
		t.Fatalf("loaded combo instrument = %+v, want 2 legs, underlying BTC-BIUSD", loaded)
	}
}

func TestGetOrCreateComboInstrument_MemoryFallbackRoundTripsNLegs(t *testing.T) {
	ctx := context.Background()
	legs := []models.ComboLeg{
		{Symbol: "BTC-BIUSD-50000-20260101-PUT", Ratio: 1},
		{Symbol: "BTC-BIUSD-55000-20260101-PUT", Ratio: -1},
		{Symbol: "BTC-BIUSD-65000-20260101-CALL", Ratio: -1},
		{Symbol: "BTC-BIUSD-70000-20260101-CALL", Ratio: 1},
	}
	created, err := getOrCreateComboInstrument(ctx, nil, legs, "BTC-BIUSD")
	if err != nil {
		t.Fatalf("getOrCreateComboInstrument (iron condor): %v", err)
	}
	loaded, err := loadComboInstrument(ctx, nil, created.Symbol)
	if err != nil {
		t.Fatalf("loadComboInstrument: %v", err)
	}
	if len(loaded.Legs) != 4 {
		t.Fatalf("loaded iron condor legs = %d, want 4", len(loaded.Legs))
	}
}

func TestLoadComboInstrument_UnknownSymbolErrors(t *testing.T) {
	if _, err := loadComboInstrument(context.Background(), nil, "COMBO:doesnotexist"); err == nil {
		t.Fatal("expected an error looking up an unregistered combo symbol")
	}
}
