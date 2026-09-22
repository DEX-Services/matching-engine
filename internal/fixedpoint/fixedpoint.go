// Package fixedpoint is a scaled-int64 replacement for shopspring/decimal on
// the matching engine's hot path (order book, risk checks, settlement).
//
// Why: decimal.Decimal represents every value as a big.Int coefficient plus
// an exponent, so every arithmetic op (Add, Sub, Mul, comparisons via
// GreaterThan/LessThan, even priceKey()'s own canonicalization) allocates a
// fresh big.Int. At matching-engine's order rates this is the single
// largest source of GC pressure in the hot path — see
// PERFORMANCE-CODE-REVIEW-FINDINGS.md item #1, independently confirmed
// against the real source before this package was written.
//
// Design: ONE global scale (1e8, "satoshi" convention — 8 decimal digits),
// not a per-symbol scale derived from each market's own tick/lot size.
// Real tick sizes seeded in this codebase are 0.01 and lot sizes 0.00001
// (cmd/engine/markets.go) — both exact at 1e8 with wide headroom, so a
// global scale loses no real precision. A per-symbol scale was considered
// and rejected: risk/settlement code routinely combines values that
// originate from different symbols (fees, margin, multi-leg combo pricing,
// PnL against a quote-currency amount) — forcing every one of those call
// sites to track and reconcile two different scales would trade one class
// of bug (GC churn) for a much worse one (silent cross-scale arithmetic
// errors in live-money code). A single fixed scale means every Fixed value
// in the system is directly comparable and combinable with every other,
// exactly like an int64 nanosecond duration is throughout Go's time
// package — the same reasoning applies here.
//
// Fixed wraps a plain int64: Add/Sub are single machine instructions (no
// allocation), Mul/Div use int64/big.Int only where genuinely needed
// (Mul of two Fixed values momentarily needs more than 64 bits of
// intermediate precision — handled internally, still zero heap allocation
// for values in the realistic trading range).
package fixedpoint

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
)

// Scale is the fixed number of decimal digits every Fixed value carries.
// 1e8 (8 digits) matches common "satoshi" convention and covers every real
// tick_size/lot_size/price/quantity/fee value seeded in this codebase with
// margin to spare (see this package's own doc comment for the real values
// checked before choosing this).
const Scale = 100_000_000 // 10^8

// Fixed is a scaled int64: the real value is int64(Fixed) / Scale.
// Zero value is a valid zero, same as decimal.Decimal's zero value.
type Fixed int64

// Zero is the additive identity, spelled out for readability at call sites
// that would otherwise write `fixedpoint.Fixed(0)`.
const Zero Fixed = 0

// maxSafeValue bounds what FromDecimal/FromString/FromFloat64 will accept
// without silently overflowing int64 once multiplied by Scale. int64 tops
// out at ~9.22e18; dividing by Scale (1e8) leaves ~9.22e10 (92 billion) as
// the largest representable whole-number value — far beyond any real
// price, quantity, or notional this exchange handles (its largest configured
// MaxPrice/MaxQuantity is 1,000,000 — see config.EnsureSchema's defaults),
// but checked explicitly so a future misconfiguration fails loudly with
// ErrOverflow instead of silently wrapping into a wrong (possibly negative)
// number, which would be far worse in live-money code.
const maxSafeValue = math.MaxInt64 / Scale

// ErrOverflow is returned by any constructor whose input would exceed the
// representable range at Scale.
var ErrOverflow = fmt.Errorf("fixedpoint: value out of representable range")

// FromDecimal converts a decimal.Decimal to Fixed, rounding to the nearest
// representable value (half-away-from-zero, matching decimal.Decimal's own
// default .Round() behavior) rather than truncating — a silent truncation
// bias would systematically under-count on every conversion, which over
// millions of orders is a real, directional bug, not a rounding footnote.
func FromDecimal(d decimal.Decimal) (Fixed, error) {
	scaled := d.Mul(decimal.New(1, 8)).Round(0)
	if scaled.GreaterThan(decimal.NewFromInt(math.MaxInt64)) || scaled.LessThan(decimal.NewFromInt(math.MinInt64)) {
		return 0, ErrOverflow
	}
	return Fixed(scaled.IntPart()), nil
}

// MustFromDecimal is FromDecimal for call sites that already know the input
// is in range (e.g. converting a freshly-loaded SymbolConfig constant) and
// would rather panic loudly at startup than thread an error through every
// caller. Never call this on user-supplied input.
func MustFromDecimal(d decimal.Decimal) Fixed {
	f, err := FromDecimal(d)
	if err != nil {
		panic(err)
	}
	return f
}

// FromString parses a decimal string directly into Fixed without an
// intermediate decimal.Decimal allocation on the common path — this is the
// one conversion that DOES run on every incoming order (HTTP query params
// are strings), so it is written to avoid decimal.Decimal entirely when the
// input has no more than 8 fractional digits (the overwhelmingly common
// case), falling back to the decimal.Decimal path only for pathological
// input (more than 8 fractional digits supplied, which simply rounds).
func FromString(s string) (Fixed, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("fixedpoint: empty string")
	}
	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	} else if s[0] == '+' {
		s = s[1:]
	}
	intPart := s
	fracPart := ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		intPart = s[:i]
		fracPart = s[i+1:]
	}
	if intPart == "" {
		intPart = "0"
	}
	// More than 8 fractional digits, scientific notation, or anything else
	// non-trivial: fall back to the exact decimal.Decimal path rather than
	// hand-rolling more parsing edge cases here.
	if len(fracPart) > 8 || strings.ContainsAny(s, "eE") {
		d, err := decimal.NewFromString(s)
		if err != nil {
			return 0, err
		}
		if neg {
			d = d.Neg()
		}
		return FromDecimal(d)
	}
	for len(fracPart) < 8 {
		fracPart += "0"
	}
	intVal, err := strconv.ParseInt(intPart, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("fixedpoint: invalid integer part %q: %w", intPart, err)
	}
	fracVal, err := strconv.ParseInt(fracPart, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("fixedpoint: invalid fractional part %q: %w", fracPart, err)
	}
	if intVal > maxSafeValue {
		return 0, ErrOverflow
	}
	result := intVal*Scale + fracVal
	if neg {
		result = -result
	}
	return Fixed(result), nil
}

// MustFromString is FromString for constants/tests. Panics on invalid input.
func MustFromString(s string) Fixed {
	f, err := FromString(s)
	if err != nil {
		panic(err)
	}
	return f
}

// FromInt64 builds a Fixed representing a whole number, e.g.
// FromInt64(5) == FromString("5").
func FromInt64(n int64) Fixed {
	return Fixed(n * Scale)
}

// ToDecimal converts back to decimal.Decimal — used only at the API/DTO
// boundary (JSON responses, Kafka publishing) and in settlement call sites
// that still hand off to Postgres via pgx's decimal binding, never on the
// matching hot path itself.
func (f Fixed) ToDecimal() decimal.Decimal {
	return decimal.New(int64(f), -8)
}

// String renders the value as a plain decimal string with no trailing
// zeros beyond what's needed (e.g. "10", "10.5", "0.00001") — matching the
// exact display convention every existing DTO/HTTP handler already expects
// from decimal.Decimal.String(), so swapping the underlying type at a
// boundary that still calls .String() requires no format changes downstream.
func (f Fixed) String() string {
	neg := f < 0
	v := int64(f)
	if neg {
		v = -v
	}
	intPart := v / Scale
	fracPart := v % Scale
	if fracPart == 0 {
		if neg {
			return "-" + strconv.FormatInt(intPart, 10)
		}
		return strconv.FormatInt(intPart, 10)
	}
	frac := strconv.FormatInt(fracPart, 10)
	for len(frac) < 8 {
		frac = "0" + frac
	}
	frac = strings.TrimRight(frac, "0")
	sign := ""
	if neg {
		sign = "-"
	}
	return sign + strconv.FormatInt(intPart, 10) + "." + frac
}

// MarshalJSON encodes as a JSON string (e.g. "10.5"), matching what every
// existing HTTP DTO already produces by calling decimal.Decimal.String()
// explicitly — this is for the Kafka wire format specifically (see this
// package's own doc comment and PERFORMANCE-CODE-REVIEW-FINDINGS.md's
// Kafka-boundary discussion): models.Order/Trade are published to Kafka
// as-is with no separate DTO, so Fixed's own JSON encoding IS the wire
// format real consumers (matching-engine's own Postgres writer) parse.
// Encoding as a string (not a bare number) avoids floating-point precision
// loss in any consumer that decodes through encoding/json's default
// float64 path, and keeps the wire format visually identical to the old
// decimal.Decimal-based one, even though the decision was made to treat
// this as a clean breaking change (old consumers must be updated
// regardless, per the explicit choice to update Kafka consumers rather
// than preserve exact byte-for-byte compatibility).
func (f Fixed) MarshalJSON() ([]byte, error) {
	return []byte(`"` + f.String() + `"`), nil
}

// UnmarshalJSON accepts either a JSON string ("10.5") or a bare JSON number
// (10.5) for compatibility with any caller that hasn't been updated to the
// string convention yet — but every internal producer always emits the
// string form via MarshalJSON above.
func (f *Fixed) UnmarshalJSON(data []byte) error {
	s := strings.Trim(string(data), `"`)
	if s == "null" {
		*f = 0
		return nil
	}
	v, err := FromString(s)
	if err != nil {
		return err
	}
	*f = v
	return nil
}

// Add, Sub: single int64 op, zero allocation.
func (f Fixed) Add(other Fixed) Fixed { return f + other }
func (f Fixed) Sub(other Fixed) Fixed { return f - other }
func (f Fixed) Neg() Fixed            { return -f }

// Mul multiplies two Fixed values, correctly re-scaling the result.
// f and other are each already ×Scale, so a naive f*other would be
// ×Scale²; this divides back down by Scale. The intermediate product can
// exceed int64 range for large-but-realistic inputs (e.g. price ~1e6 and
// quantity ~1e6, each already ×1e8, multiply to ~1e28), so the intermediate
// is computed in int128-equivalent space via math/bits.Mul64+Div64 —
// zero heap allocation, unlike decimal.Decimal's big.Int path, while still
// never silently overflowing on realistic exchange values.
func (f Fixed) Mul(other Fixed) Fixed {
	return Fixed(mulScaled(int64(f), int64(other)))
}

// Div divides f by other, returning a Fixed result. Division by zero
// returns Zero rather than panicking — every call site in this codebase
// already checks for a zero divisor before calling decimal.Decimal's
// equivalent (which also panics on zero), so this matches existing
// call-site expectations rather than introducing a new panic path into
// live order-matching code.
func (f Fixed) Div(other Fixed) Fixed {
	if other == 0 {
		return 0
	}
	return Fixed(divScaled(int64(f), int64(other)))
}

// Cmp, comparisons — direct int64 comparisons, zero allocation, unlike
// decimal.Decimal's GreaterThan/LessThan which each allocate.
func (f Fixed) Equal(other Fixed) bool              { return f == other }
func (f Fixed) GreaterThan(other Fixed) bool        { return f > other }
func (f Fixed) GreaterThanOrEqual(other Fixed) bool { return f >= other }
func (f Fixed) LessThan(other Fixed) bool           { return f < other }
func (f Fixed) LessThanOrEqual(other Fixed) bool    { return f <= other }
func (f Fixed) IsZero() bool                        { return f == 0 }
func (f Fixed) IsPositive() bool                    { return f > 0 }
func (f Fixed) IsNegative() bool                    { return f < 0 }
func (f Fixed) Sign() int {
	switch {
	case f > 0:
		return 1
	case f < 0:
		return -1
	default:
		return 0
	}
}

// Abs returns the absolute value.
func (f Fixed) Abs() Fixed {
	if f < 0 {
		return -f
	}
	return f
}

// Mod returns f modulo other, matching decimal.Decimal.Mod's semantics
// (result has the sign of f, truncated division) — used by
// validateOrderConfig's tick/lot-size checks (o.Price.Mod(cfg.TickSize)).
// Since both f and other already share this package's single global Scale,
// no rescaling is needed: it's a direct int64 remainder, unlike Mul/Div
// which must correct for the doubled/cancelled scale. Division by zero
// returns Zero rather than panicking, consistent with Div's own zero-divisor
// behavior above.
func (f Fixed) Mod(other Fixed) Fixed {
	if other == 0 {
		return 0
	}
	return Fixed(int64(f) % int64(other))
}

// Ceil rounds f up to the nearest whole number (toward +Inf), matching
// decimal.Decimal.Ceil's semantics. Used by submit.go's lot-size step
// rounding.
func (f Fixed) Ceil() Fixed {
	v := int64(f)
	r := v % Scale
	if r == 0 {
		return f
	}
	if v > 0 {
		return Fixed(v - r + Scale)
	}
	return Fixed(v - r)
}

// Floor rounds f down to the nearest whole number (toward -Inf), matching
// decimal.Decimal.Floor's semantics. Used by submit.go's lot-size step
// rounding.
func (f Fixed) Floor() Fixed {
	v := int64(f)
	r := v % Scale
	if r == 0 {
		return f
	}
	if v < 0 {
		return Fixed(v - r - Scale)
	}
	return Fixed(v - r)
}

// Min/Max: free functions matching decimal.Min/decimal.Max's call
// convention at existing call sites (fillQty := decimal.Min(a, b) style).
func Min(a, b Fixed) Fixed {
	if a < b {
		return a
	}
	return b
}

func Max(a, b Fixed) Fixed {
	if a > b {
		return a
	}
	return b
}

// Sum adds a slice of Fixed values — convenience for call sites that
// previously did a decimal.Zero accumulator loop.
func Sum(values ...Fixed) Fixed {
	var total Fixed
	for _, v := range values {
		total += v
	}
	return total
}
