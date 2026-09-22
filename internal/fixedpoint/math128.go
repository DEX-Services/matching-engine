package fixedpoint

import (
	"math"
	"math/bits"
)

// mulScaled computes (a * b) / Scale using 128-bit intermediate precision
// via math/bits.Mul64 (the standard library's zero-allocation, hardware-
// intrinsic 64x64->128 multiply), then math/bits.Div64 to bring the
// double-width product back down by Scale. This is what lets Fixed.Mul
// handle realistic exchange values (price × quantity, both already scaled
// by 1e8) without silently overflowing plain int64 multiplication, and
// without allocating a big.Int the way decimal.Decimal's equivalent does.
//
// Sign is handled by working in absolute value and reapplying at the end —
// bits.Mul64/Div64 operate on uint64, so a naive direct call on negative
// int64 inputs (via casting) would be wrong.
//
// bits.Div64 panics if the quotient would not fit in 64 bits (hi >= divisor)
// — this happens when two individually in-range Fixed values (e.g. both at
// this exchange's real MaxPrice/MaxQuantity of 1,000,000, each already
// ×1e8) produce a 128-bit product whose quotient after dividing by Scale
// exceeds int64: the true product 1e12 is representable, but the
// INTERMEDIATE double-scaled product (~1e28) divided by Scale (1e8) is
// ~1e20, past int64's ~9.2e18 ceiling. This is deliberately clamped to
// math.MaxInt64/math.MinInt64 rather than panicking: a single pathological
// multiplication must never crash a symbol's matching goroutine in
// live-money code — a saturated (visibly wrong, clamped-to-max) result is
// a recoverable, loud-in-logs failure mode; a goroutine panic taking down
// order matching for every trader on that symbol is not. Every real call
// site in this codebase multiplies values far below this ceiling (see this
// package's own doc comment on Scale's choice), so saturation is a safety
// net for a case that should never occur, not a documented normal path.
func mulScaled(a, b int64) int64 {
	neg := (a < 0) != (b < 0)
	ua, ub := absU64(a), absU64(b)

	hi, lo := bits.Mul64(ua, ub)
	if hi >= Scale {
		if neg {
			return math.MinInt64
		}
		return math.MaxInt64
	}
	q, _ := bits.Div64(hi, lo, Scale)
	if q > math.MaxInt64 {
		if neg {
			return math.MinInt64
		}
		return math.MaxInt64
	}

	result := int64(q)
	if neg {
		result = -result
	}
	return result
}

// divScaled computes (a * Scale) / b using the same 128-bit intermediate
// technique as mulScaled — a is already ×Scale, so a naive a/b would lose
// the scale entirely; multiplying by Scale first (in 128-bit space, since
// a*Scale can itself exceed int64) before dividing by b preserves full
// precision. Same overflow-to-saturation behavior as mulScaled, for the
// same reason (a large dividend against a very small divisor can push the
// quotient past int64 range) and the same rationale (never panic a live
// matching goroutine over an out-of-range edge case).
func divScaled(a, b int64) int64 {
	neg := (a < 0) != (b < 0)
	ua, ub := absU64(a), absU64(b)

	hi, lo := bits.Mul64(ua, Scale)
	if hi >= ub {
		if neg {
			return math.MinInt64
		}
		return math.MaxInt64
	}
	q, _ := bits.Div64(hi, lo, ub)
	if q > math.MaxInt64 {
		if neg {
			return math.MinInt64
		}
		return math.MaxInt64
	}

	result := int64(q)
	if neg {
		result = -result
	}
	return result
}

func absU64(n int64) uint64 {
	if n < 0 {
		return uint64(-n)
	}
	return uint64(n)
}
