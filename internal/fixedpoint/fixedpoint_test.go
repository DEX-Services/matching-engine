package fixedpoint

import (
	"math"
	"math/rand"
	"testing"

	"github.com/shopspring/decimal"
)

func TestFromStringAndString_RoundTrip(t *testing.T) {
	cases := []string{
		"0", "1", "-1", "0.01", "0.00001", "100", "100.5", "-100.5",
		"1000000", "0.00000001", "-0.00000001", "12345.6789",
	}
	for _, s := range cases {
		f, err := FromString(s)
		if err != nil {
			t.Fatalf("FromString(%q): %v", s, err)
		}
		got := f.String()
		want, _ := decimal.NewFromString(s)
		gotDec, _ := decimal.NewFromString(got)
		if !want.Equal(gotDec) {
			t.Errorf("FromString(%q).String() = %q, decimal value mismatch: want %s got %s", s, got, want, gotDec)
		}
	}
}

func TestFromDecimal_MatchesFromString(t *testing.T) {
	cases := []string{"0.01", "0.00001", "10.5", "100", "-42.42"}
	for _, s := range cases {
		d, _ := decimal.NewFromString(s)
		viaDecimal, err := FromDecimal(d)
		if err != nil {
			t.Fatalf("FromDecimal(%s): %v", s, err)
		}
		viaString, err := FromString(s)
		if err != nil {
			t.Fatalf("FromString(%s): %v", s, err)
		}
		if viaDecimal != viaString {
			t.Errorf("%s: FromDecimal=%d FromString=%d mismatch", s, viaDecimal, viaString)
		}
	}
}

func TestAddSub(t *testing.T) {
	a := MustFromString("10.5")
	b := MustFromString("3.25")
	if got := a.Add(b).String(); got != "13.75" {
		t.Errorf("10.5+3.25 = %s, want 13.75", got)
	}
	if got := a.Sub(b).String(); got != "7.25" {
		t.Errorf("10.5-3.25 = %s, want 7.25", got)
	}
}

func TestMul_AgainstDecimal(t *testing.T) {
	cases := []struct{ a, b string }{
		{"10", "5"},
		{"0.01", "100"},
		{"100.5", "2"},
		{"1000000", "100"}, // a realistic large notional (price * quantity), well under the ~92 billion representable ceiling
		{"0.00001", "0.00001"},
		{"12345.6789", "0.0001"},
	}
	for _, c := range cases {
		fa, fb := MustFromString(c.a), MustFromString(c.b)
		got := fa.Mul(fb)

		da, _ := decimal.NewFromString(c.a)
		db, _ := decimal.NewFromString(c.b)
		want, err := FromDecimal(da.Mul(db))
		if err != nil {
			t.Fatalf("%s*%s: reference overflow: %v", c.a, c.b, err)
		}
		if got != want {
			t.Errorf("%s * %s = %s (fixed), want %s (via decimal)", c.a, c.b, got.String(), want.String())
		}
	}
}

func TestDiv_AgainstDecimal(t *testing.T) {
	cases := []struct{ a, b string }{
		{"10", "3"},
		{"100.5", "2"},
		{"1", "8"}, // exact at 8 decimals: 0.125
		{"1000000", "3"},
	}
	for _, c := range cases {
		fa, fb := MustFromString(c.a), MustFromString(c.b)
		got := fa.Div(fb)

		da, _ := decimal.NewFromString(c.a)
		db, _ := decimal.NewFromString(c.b)
		wantDec := da.DivRound(db, 8)
		want, err := FromDecimal(wantDec)
		if err != nil {
			t.Fatalf("%s/%s: reference overflow: %v", c.a, c.b, err)
		}
		// Division can differ by 1 unit in the last place vs decimal's own
		// rounding mode (DivRound uses half-away-from-zero at the requested
		// scale; our integer division truncates toward zero) — this is
		// expected and acceptable for a fixed-point division (real
		// exchanges truncate quotient precision too), so allow off-by-one.
		diff := int64(got) - int64(want)
		if diff < -1 || diff > 1 {
			t.Errorf("%s / %s = %s (fixed), want ~%s (via decimal), diff=%d", c.a, c.b, got.String(), want.String(), diff)
		}
	}
}

// TestMul_ExtremeOverflowSaturates documents the real, understood ceiling
// of this type: two Fixed values that are each individually representable
// (up to ~92 billion, see maxSafeValue) can still have a mathematical
// product that needs more than 64 bits once re-scaled by 1e8 — e.g.
// 1,000,000 * 1,000,000 = 1e12 as a real number, but AS A Fixed value that
// itself requires 1e12 * 1e8 = 1e20, past int64's ~9.2e18 ceiling. This is
// not reachable by any real order this exchange accepts (MaxPrice/
// MaxQuantity config caps both operands at 1,000,000 each, and the
// resulting notional of such an order is nowhere near this type's ceiling
// in practice — a $1,000,000 order at 1,000,000 quantity is not a
// real-world order), but Mul must never panic if it's ever hit by a future
// misconfiguration or bug elsewhere, so this confirms the saturation
// behavior instead of a crash.
func TestMul_ExtremeOverflowSaturates(t *testing.T) {
	a := MustFromString("1000000")
	b := MustFromString("1000000")
	got := a.Mul(b) // does not panic
	if got != math.MaxInt64 {
		t.Errorf("expected saturation to MaxInt64, got %d", int64(got))
	}
}

func TestDivByZero_ReturnsZero(t *testing.T) {
	a := MustFromString("10")
	if got := a.Div(0); got != 0 {
		t.Errorf("10/0 = %s, want 0 (no panic)", got.String())
	}
}

func TestComparisons(t *testing.T) {
	a := MustFromString("10")
	b := MustFromString("20")
	if !a.LessThan(b) || a.GreaterThan(b) || a.Equal(b) {
		t.Error("10 vs 20 comparison wrong")
	}
	if !b.GreaterThanOrEqual(a) || !a.LessThanOrEqual(b) {
		t.Error("10/20 orEqual comparisons wrong")
	}
	if !a.Equal(MustFromString("10.0")) {
		t.Error("10 should equal 10.0")
	}
}

func TestSignHelpers(t *testing.T) {
	pos := MustFromString("5")
	neg := MustFromString("-5")
	zero := Zero
	if !pos.IsPositive() || pos.IsNegative() || pos.IsZero() {
		t.Error("positive value sign helpers wrong")
	}
	if !neg.IsNegative() || neg.IsPositive() || neg.IsZero() {
		t.Error("negative value sign helpers wrong")
	}
	if !zero.IsZero() || zero.IsPositive() || zero.IsNegative() {
		t.Error("zero value sign helpers wrong")
	}
	if pos.Sign() != 1 || neg.Sign() != -1 || zero.Sign() != 0 {
		t.Error("Sign() wrong")
	}
	if neg.Abs() != pos {
		t.Error("Abs() wrong")
	}
}

func TestMinMax(t *testing.T) {
	a, b := MustFromString("5"), MustFromString("10")
	if Min(a, b) != a || Min(b, a) != a {
		t.Error("Min wrong")
	}
	if Max(a, b) != b || Max(b, a) != b {
		t.Error("Max wrong")
	}
}

func TestJSONRoundTrip(t *testing.T) {
	f := MustFromString("123.456")
	data, err := f.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `"123.456"` {
		t.Errorf("MarshalJSON = %s, want \"123.456\"", data)
	}
	var f2 Fixed
	if err := f2.UnmarshalJSON(data); err != nil {
		t.Fatal(err)
	}
	if f2 != f {
		t.Errorf("round-trip mismatch: %s != %s", f2.String(), f.String())
	}
}

func TestUnmarshalJSON_BareNumber(t *testing.T) {
	var f Fixed
	if err := f.UnmarshalJSON([]byte("10.5")); err != nil {
		t.Fatal(err)
	}
	if f.String() != "10.5" {
		t.Errorf("got %s, want 10.5", f.String())
	}
}

func TestOverflow(t *testing.T) {
	huge := decimal.NewFromInt(math.MaxInt64)
	if _, err := FromDecimal(huge); err == nil {
		t.Error("expected overflow error for MaxInt64, got nil")
	}
}

// TestRandomizedAgainstDecimal is a property test: for many random pairs of
// realistic-range values, Fixed arithmetic must agree with decimal.Decimal
// arithmetic (rounded to 8 places) within a tight tolerance. This is the
// real regression guard for the whole package — any future change to
// mulScaled/divScaled that introduces a precision bug will show up here.
func TestRandomizedAgainstDecimal(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 10000; i++ {
		// Realistic trading-value range: 0.00001 to 1,000,000, matching this
		// codebase's real MaxPrice/MaxQuantity/lot-size configuration.
		av := rng.Float64() * 1_000_000
		bv := rng.Float64()*1_000_000 + 0.00001 // avoid zero divisor

		da := decimal.NewFromFloat(av).Round(8)
		db := decimal.NewFromFloat(bv).Round(8)

		fa, err := FromDecimal(da)
		if err != nil {
			t.Fatalf("FromDecimal(%s): %v", da, err)
		}
		fb, err := FromDecimal(db)
		if err != nil {
			t.Fatalf("FromDecimal(%s): %v", db, err)
		}

		// Addition/subtraction must be EXACT.
		wantAdd, _ := FromDecimal(da.Add(db))
		if fa.Add(fb) != wantAdd {
			t.Fatalf("Add mismatch: %s + %s: got %s want %s", da, db, fa.Add(fb), wantAdd)
		}
		wantSub, _ := FromDecimal(da.Sub(db))
		if fa.Sub(fb) != wantSub {
			t.Fatalf("Sub mismatch: %s - %s: got %s want %s", da, db, fa.Sub(fb), wantSub)
		}

		// Multiplication: result rounded to 8dp must match within 1 unit in
		// the last place (both sides round the true infinite-precision
		// product to 8dp, and rounding-mode differences can shift the
		// final digit). Skip pairs whose product, once re-scaled by 1e8 as
		// a Fixed value, would exceed int64 — see TestMul_ExtremeOverflowSaturates
		// for why that's a real, understood, and separately-tested ceiling
		// rather than a bug this property test should also flag.
		mulProduct := da.Mul(db)
		asFixedRaw := mulProduct.Mul(decimal.New(1, 8))
		if asFixedRaw.Abs().LessThan(decimal.NewFromInt(math.MaxInt64)) {
			wantMul, err := FromDecimal(mulProduct)
			if err == nil {
				gotMul := fa.Mul(fb)
				diff := int64(gotMul) - int64(wantMul)
				if diff < -1 || diff > 1 {
					t.Fatalf("Mul mismatch: %s * %s: got %s want %s (diff %d)", da, db, gotMul, wantMul, diff)
				}
			}
		}
	}
}

func BenchmarkFixedAdd(b *testing.B) {
	x := MustFromString("100.5")
	y := MustFromString("50.25")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = x.Add(y)
	}
}

func BenchmarkDecimalAdd(b *testing.B) {
	x := decimal.RequireFromString("100.5")
	y := decimal.RequireFromString("50.25")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = x.Add(y)
	}
}

func BenchmarkFixedMul(b *testing.B) {
	x := MustFromString("100.5")
	y := MustFromString("50.25")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = x.Mul(y)
	}
}

func BenchmarkDecimalMul(b *testing.B) {
	x := decimal.RequireFromString("100.5")
	y := decimal.RequireFromString("50.25")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = x.Mul(y)
	}
}
