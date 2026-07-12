package pricing

import (
	"math"
	"testing"
)

func TestPriceATMCallPutParity(t *testing.T) {
	spot, strike, tYears, vol, rate := 100.0, 100.0, 1.0, 0.2, 0.05
	call := Price(spot, strike, tYears, vol, rate, true)
	put := Price(spot, strike, tYears, vol, rate, false)
	// Put-call parity: C - P = S - K*e^(-rT)
	lhs := call - put
	rhs := spot - strike*math.Exp(-rate*tYears)
	if diff := lhs - rhs; diff > 1e-6 || diff < -1e-6 {
		t.Fatalf("put-call parity violated: lhs=%f rhs=%f", lhs, rhs)
	}
	if call <= 0 || put <= 0 {
		t.Fatalf("expected positive premiums, got call=%f put=%f", call, put)
	}
}

func TestIntrinsicAtExpiry(t *testing.T) {
	if got := Price(120, 100, 0, 0.2, 0.05, true); got != 20 {
		t.Fatalf("expected intrinsic 20, got %f", got)
	}
	if got := Price(80, 100, 0, 0.2, 0.05, false); got != 20 {
		t.Fatalf("expected intrinsic 20, got %f", got)
	}
}

func TestGreeksDeltaBounds(t *testing.T) {
	g := CalcGreeks(100, 100, 1, 0.2, 0.05, true)
	if g.Delta < 0 || g.Delta > 1 {
		t.Fatalf("call delta out of bounds: %f", g.Delta)
	}
	gp := CalcGreeks(100, 100, 1, 0.2, 0.05, false)
	if gp.Delta < -1 || gp.Delta > 0 {
		t.Fatalf("put delta out of bounds: %f", gp.Delta)
	}
}

func TestImpliedVolRoundTrip(t *testing.T) {
	spot, strike, tYears, rate := 100.0, 105.0, 0.5, 0.03
	trueVol := 0.35
	price := Price(spot, strike, tYears, trueVol, rate, true)
	iv := ImpliedVol(price, spot, strike, tYears, rate, true)
	if diff := iv - trueVol; diff > 1e-3 || diff < -1e-3 {
		t.Fatalf("implied vol mismatch: got %f want %f", iv, trueVol)
	}
}
