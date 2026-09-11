package volsurface

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestNilPoolIsSafeNoop(t *testing.T) {
	s := NewStore(nil)
	ctx := context.Background()
	expiry := time.Now().Add(30 * 24 * time.Hour)

	// Record must not panic with a nil pool.
	s.Record(ctx, "BTC-BIUSDB", decimal.NewFromInt(60000), expiry, "CALL", 0.55)

	iv, ok := s.Interpolate(ctx, "BTC-BIUSDB", decimal.NewFromInt(60000), expiry, "CALL")
	if ok {
		t.Fatalf("expected no interpolation from a nil-pool store, got iv=%f", iv)
	}
}

func TestNilStoreIsSafe(t *testing.T) {
	var s *Store // nil receiver, e.g. a caller that never constructed one
	ctx := context.Background()
	expiry := time.Now().Add(30 * 24 * time.Hour)

	s.Record(ctx, "BTC-BIUSDB", decimal.NewFromInt(60000), expiry, "CALL", 0.55)
	if _, ok := s.Interpolate(ctx, "BTC-BIUSDB", decimal.NewFromInt(60000), expiry, "CALL"); ok {
		t.Fatal("expected no interpolation from a nil *Store")
	}
}

func TestInterpolate_EmptyReturnsFalse(t *testing.T) {
	if _, ok := interpolate(60000, nil, nil); ok {
		t.Fatal("expected false for no observations")
	}
}

func TestInterpolate_SinglePointFlatExtrapolates(t *testing.T) {
	iv, ok := interpolate(65000, []float64{60000}, []float64{0.5})
	if !ok || iv != 0.5 {
		t.Fatalf("interpolate with one point = (%f, %v), want (0.5, true)", iv, ok)
	}
	// Below the single observed strike too.
	iv, ok = interpolate(50000, []float64{60000}, []float64{0.5})
	if !ok || iv != 0.5 {
		t.Fatalf("interpolate below single point = (%f, %v), want (0.5, true)", iv, ok)
	}
}

func TestInterpolate_LinearBetweenTwoBracketingStrikes(t *testing.T) {
	// 55000 -> 0.4, 65000 -> 0.6; target 60000 is exactly halfway.
	iv, ok := interpolate(60000, []float64{55000, 65000}, []float64{0.4, 0.6})
	if !ok {
		t.Fatal("expected an interpolated value")
	}
	if diff := iv - 0.5; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("interpolated iv = %f, want 0.5", iv)
	}
}

func TestInterpolate_UnsortedInputStillWorks(t *testing.T) {
	iv, ok := interpolate(60000, []float64{65000, 55000}, []float64{0.6, 0.4})
	if !ok || (iv-0.5) > 1e-9 || (iv-0.5) < -1e-9 {
		t.Fatalf("interpolate with unsorted input = (%f, %v), want (0.5, true)", iv, ok)
	}
}

func TestInterpolate_FlatExtrapolationBeyondObservedRange(t *testing.T) {
	strikes := []float64{55000, 60000, 65000}
	ivs := []float64{0.4, 0.5, 0.6}

	// Above the highest observed strike -> nearest (highest) IV.
	iv, ok := interpolate(90000, strikes, ivs)
	if !ok || iv != 0.6 {
		t.Fatalf("extrapolation above range = (%f, %v), want (0.6, true)", iv, ok)
	}
	// Below the lowest observed strike -> nearest (lowest) IV.
	iv, ok = interpolate(30000, strikes, ivs)
	if !ok || iv != 0.4 {
		t.Fatalf("extrapolation below range = (%f, %v), want (0.4, true)", iv, ok)
	}
}

func TestInterpolate_ExactMatchReturnsItsOwnIV(t *testing.T) {
	iv, ok := interpolate(60000, []float64{55000, 60000, 65000}, []float64{0.4, 0.5, 0.6})
	if !ok || iv != 0.5 {
		t.Fatalf("exact-match interpolation = (%f, %v), want (0.5, true)", iv, ok)
	}
}
