// Package pricing implements Black-Scholes-Merton option pricing and greeks.
// All functions operate on float64 (pricing model math, not ledger accounting)
// since option premiums are quoted/rounded before hitting the decimal ledger.
package pricing

import "math"

// Greeks holds the standard first/second-order option sensitivities.
type Greeks struct {
	Delta float64
	Gamma float64
	Theta float64 // per calendar day
	Vega  float64 // per 1 vol point (0.01 change in sigma)
	Rho   float64 // per 1% change in risk-free rate
}

func normCDF(x float64) float64 {
	return 0.5 * math.Erfc(-x/math.Sqrt2)
}

func normPDF(x float64) float64 {
	return math.Exp(-0.5*x*x) / math.Sqrt(2*math.Pi)
}

func d1d2(spot, strike, tYears, vol, rate float64) (d1, d2 float64) {
	if tYears <= 0 || vol <= 0 {
		return 0, 0
	}
	sqrtT := math.Sqrt(tYears)
	d1 = (math.Log(spot/strike) + (rate+0.5*vol*vol)*tYears) / (vol * sqrtT)
	d2 = d1 - vol*sqrtT
	return d1, d2
}

// Price returns the Black-Scholes premium for a European call or put.
// tYears is time to expiry in years; vol is annualized volatility (e.g. 0.6 = 60%);
// rate is the annualized risk-free rate (e.g. 0.05 = 5%).
func Price(spot, strike, tYears, vol, rate float64, isCall bool) float64 {
	if tYears <= 0 {
		return Intrinsic(spot, strike, isCall)
	}
	d1, d2 := d1d2(spot, strike, tYears, vol, rate)
	discStrike := strike * math.Exp(-rate*tYears)
	if isCall {
		return spot*normCDF(d1) - discStrike*normCDF(d2)
	}
	return discStrike*normCDF(-d2) - spot*normCDF(-d1)
}

// Intrinsic returns the exercise value of an option with no time value remaining.
func Intrinsic(spot, strike float64, isCall bool) float64 {
	if isCall {
		return math.Max(0, spot-strike)
	}
	return math.Max(0, strike-spot)
}

// CalcGreeks returns the standard option sensitivities at the given parameters.
func CalcGreeks(spot, strike, tYears, vol, rate float64, isCall bool) Greeks {
	if tYears <= 0 || vol <= 0 {
		return Greeks{}
	}
	d1, d2 := d1d2(spot, strike, tYears, vol, rate)
	sqrtT := math.Sqrt(tYears)
	pdf := normPDF(d1)
	discStrike := strike * math.Exp(-rate*tYears)

	gamma := pdf / (spot * vol * sqrtT)
	vega := spot * pdf * sqrtT / 100 // per 1 vol point

	var delta, theta, rho float64
	if isCall {
		delta = normCDF(d1)
		theta = (-(spot*pdf*vol)/(2*sqrtT) - rate*discStrike*normCDF(d2)) / 365
		rho = tYears * discStrike * normCDF(d2) / 100
	} else {
		delta = normCDF(d1) - 1
		theta = (-(spot*pdf*vol)/(2*sqrtT) + rate*discStrike*normCDF(-d2)) / 365
		rho = -tYears * discStrike * normCDF(-d2) / 100
	}
	return Greeks{Delta: delta, Gamma: gamma, Theta: theta, Vega: vega, Rho: rho}
}

// ImpliedVol solves for the volatility that reproduces marketPrice, via bisection.
// Returns 0 if it fails to converge within the bracket [1e-4, 5.0].
func ImpliedVol(marketPrice, spot, strike, tYears, rate float64, isCall bool) float64 {
	if tYears <= 0 || marketPrice <= 0 {
		return 0
	}
	lo, hi := 1e-4, 5.0
	for i := 0; i < 100; i++ {
		mid := (lo + hi) / 2
		price := Price(spot, strike, tYears, mid, rate, isCall)
		if math.Abs(price-marketPrice) < 1e-6 {
			return mid
		}
		if price > marketPrice {
			hi = mid
		} else {
			lo = mid
		}
	}
	return (lo + hi) / 2
}
