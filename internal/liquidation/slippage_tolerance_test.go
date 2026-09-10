package liquidation

import "testing"

// TestOptionsSlippageToleranceIsWiderThanFutures locks in a deliberate
// asymmetry that is easy to "tidy up" back into a single shared constant.
//
// Both force-close paths try a reduce-only IOC on the real book first and fall
// back to a theoretical mark for whatever doesn't fill. That fallback settles a
// real liquidation against a MODEL price rather than one anyone traded at,
// making the exchange the counterparty of last resort — so it should stay a
// last resort, not the normal path.
//
// A 1% cap is fine on a deep futures book. On these option books it frequently
// could not even reach the options market maker's own resting quote: that
// desk's minimum half-spread is 100 bps of premium and it defaults to 300 (see
// strategy.newOptionsMarketMaker), so a 1% cap meant the IOC filled nothing and
// essentially every options liquidation fell through to the theoretical mark.
func TestOptionsSlippageToleranceIsWiderThanFutures(t *testing.T) {
	if optionsLiquidationSlippageTolerance <= liquidationSlippageTolerance {
		t.Fatalf("options tolerance (%v) must be wider than futures (%v): option books are far thinner, and a cap this tight makes the theoretical-mark fallback the default path instead of the last resort",
			optionsLiquidationSlippageTolerance, liquidationSlippageTolerance)
	}
}

// TestOptionsSlippageToleranceClearsMarketMakerSpread is the concrete reason
// the number is what it is: the cap must be able to reach the options MM's own
// quote, or the IOC can never fill against the desk providing the liquidity.
//
// The desk quotes at fair value ± spreadBps of premium, defaulting to 300 bps
// (3%) and floored at 100 bps (1%). A cap at exactly the futures 1% sits right
// on that floor and below the default, so it could not cross the desk's quote
// in the normal configuration.
func TestOptionsSlippageToleranceClearsMarketMakerSpread(t *testing.T) {
	const optionsMMDefaultHalfSpread = 0.03 // 300 bps, strategy.optionsMMParams default
	if optionsLiquidationSlippageTolerance <= optionsMMDefaultHalfSpread {
		t.Fatalf("options tolerance (%v) must exceed the options MM's default half-spread (%v), otherwise a liquidation IOC cannot reach the desk's resting quote and always falls through to the theoretical mark",
			optionsLiquidationSlippageTolerance, optionsMMDefaultHalfSpread)
	}
}
