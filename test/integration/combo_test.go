package integration

import (
	"context"
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/matching"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/dex/matching-engine/internal/settlement"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

// fixedLegResolver/fixedMarkSource are minimal settlement.ComboLegResolver /
// settlement.ComboOptionsMarkSource implementations for driving a real
// matching.Engine end-to-end, without depending on cmd/engine's private
// Postgres-backed instrument types.
type fixedLegResolver struct {
	legs       []models.ComboLeg
	underlying string
}

func (f fixedLegResolver) ResolveComboLegs(ctx context.Context, comboSymbol string) ([]models.ComboLeg, string, error) {
	return f.legs, f.underlying, nil
}

type comboLegSpec struct {
	strike decimal.Decimal
	expiry time.Time
	typ    string
}

type fixedMarkSource struct {
	spot decimal.Decimal
	legs map[string]comboLegSpec
}

func (f fixedMarkSource) UnderlyingMark(underlying string) (decimal.Decimal, bool) {
	return f.spot, true
}

func (f fixedMarkSource) LegSpec(ctx context.Context, legSymbol string) (decimal.Decimal, time.Time, string, string, bool) {
	l, ok := f.legs[legSymbol]
	if !ok {
		return decimal.Zero, time.Time{}, "", "", false
	}
	return l.strike, l.expiry, l.typ, "BIUSDB", true
}

// almostEqual tolerates float64-round-trip noise from the theoretical
// Black-Scholes pricing path used to split a combo's net price across legs
// (see settlement.splitComboNetPrice) — the theoretical price itself is
// inherently float64-precision by construction, so end-to-end cash-flow
// assertions built on it must tolerate that rather than demand bit-exact
// decimal equality.
func almostEqual(t *testing.T, got, want decimal.Decimal, msg string) {
	t.Helper()
	tolerance := decimal.NewFromFloat(0.0001)
	if got.Sub(want).Abs().GreaterThan(tolerance) {
		t.Fatalf("%s: got %s, want %s (tolerance %s)", msg, got, want, tolerance)
	}
}

// TestComboOrderBook_AtomicTwoLegSettlement drives a REAL matching.Engine
// (the same goroutine-per-symbol core every other market uses) for a combo
// instrument: a resting SELL order (writer of the spread) is matched by an
// incoming BUY order (buyer of the spread) on the combo's own order book,
// producing exactly one trade — which settlement.ComboSettlement then fans
// out into two linked option-leg positions, atomically, inside the same
// matching-goroutine critical section every other trade's settlement runs
// in. This is the actual end-to-end proof that combo execution is native
// atomic matching, not client-coordinated separate orders.
func TestComboOrderBook_AtomicTwoLegSettlement(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Deposit("writer", "BIUSDB", decimal.NewFromInt(1_000_000))
	ledger.Deposit("buyer", "BIUSDB", decimal.NewFromInt(1_000_000))
	options := settlement.NewOptionsSettlement(ledger, &backendclient.Client{})

	expiry := time.Now().Add(24 * time.Hour)
	buySymbol := "BTC-BIUSDB-60000-20260101-CALL"
	sellSymbol := "BTC-BIUSDB-65000-20260101-CALL"
	legs := fixedLegResolver{
		legs:       []models.ComboLeg{{Symbol: buySymbol, Ratio: 1}, {Symbol: sellSymbol, Ratio: -1}},
		underlying: "BTC-BIUSDB",
	}
	marks := fixedMarkSource{
		spot: decimal.NewFromInt(62000),
		legs: map[string]comboLegSpec{
			buySymbol:  {strike: decimal.NewFromInt(60000), expiry: expiry, typ: "CALL"},
			sellSymbol: {strike: decimal.NewFromInt(65000), expiry: expiry, typ: "CALL"},
		},
	}
	comboSettlement := settlement.NewComboSettlement(options, legs, marks)

	comboSymbol := "COMBO:test"
	eng := matching.NewEngine(comboSymbol, models.ComboOptions, noopBus{}, comboSettlement, nil)
	defer eng.Stop()

	// Writer rests a SELL order on the combo book: willing to receive a net
	// credit of 200 to write (go short) the spread.
	sellOrder := &models.Order{
		ID: uuid.NewString(), AccountID: "writer", Symbol: comboSymbol, Market: models.ComboOptions,
		Side: models.Sell, Type: models.Limit, Price: decimal.NewFromInt(200), Quantity: decimal.NewFromInt(1),
		TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
	}
	trades, err := eng.Submit(sellOrder)
	require.NoError(t, err)
	require.Empty(t, trades, "resting order alone should not trade")

	// Buyer crosses it: willing to pay UP TO a net debit of 200 to go long
	// the spread — this should match the resting sell in ONE trade.
	buyOrder := &models.Order{
		ID: uuid.NewString(), AccountID: "buyer", Symbol: comboSymbol, Market: models.ComboOptions,
		Side: models.Buy, Type: models.Limit, Price: decimal.NewFromInt(200), Quantity: decimal.NewFromInt(1),
		TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
	}
	trades, err = eng.Submit(buyOrder)
	require.NoError(t, err)
	require.Len(t, trades, 1, "one combo order crossing one resting combo order must produce exactly one trade")
	require.True(t, trades[0].Price.Equal(decimal.NewFromInt(200)), "combo trade price should be the resting order's price (price-time priority)")

	// The single combo trade must have fanned out into positions on BOTH
	// legs for BOTH accounts — this is the atomicity being tested: there is
	// no possible intermediate state where only one leg exists, because
	// ComboSettlement.Settle ran once, synchronously, inside the same
	// matching-goroutine call that produced the trade.
	buyerLong := options.GetPosition("buyer", buySymbol, decimal.NewFromInt(60000), expiry, "CALL")
	require.NotNil(t, buyerLong, "buyer should hold a long position in the buy leg")
	require.True(t, buyerLong.Size.Equal(decimal.NewFromInt(1)))

	buyerShort := options.GetPosition("buyer", sellSymbol, decimal.NewFromInt(65000), expiry, "CALL")
	require.NotNil(t, buyerShort, "buyer should hold a short position in the sell leg")
	require.True(t, buyerShort.Size.Equal(decimal.NewFromInt(-1)))

	writerShort := options.GetPosition("writer", buySymbol, decimal.NewFromInt(60000), expiry, "CALL")
	require.NotNil(t, writerShort, "writer should hold a short position in the buy leg")
	require.True(t, writerShort.Size.Equal(decimal.NewFromInt(-1)))

	writerLong := options.GetPosition("writer", sellSymbol, decimal.NewFromInt(65000), expiry, "CALL")
	require.NotNil(t, writerLong, "writer should hold a long position in the sell leg")
	require.True(t, writerLong.Size.Equal(decimal.NewFromInt(1)))

	// Net cash flow must equal exactly the traded net price (200), on both
	// sides — the buyer paid 200 net, the writer received 200 net, no matter
	// how the two legs' individual synthetic prices were split.
	buyerBalance := ledger.Available("buyer", "BIUSDB")
	almostEqual(t, buyerBalance, decimal.NewFromInt(1_000_000-200), "buyer balance")
	writerBalance := ledger.Available("writer", "BIUSDB")
	almostEqual(t, writerBalance, decimal.NewFromInt(1_000_000+200), "writer balance")
}

// TestComboOrderBook_IronCondorAtomicSettlement extends the atomicity proof
// to a 4-leg structure, confirming the native combo book handles more than
// a plain 2-leg vertical — an iron condor's mixed CALL/PUT, mixed long/
// short legs all settle together from one real matched trade.
func TestComboOrderBook_IronCondorAtomicSettlement(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Deposit("writer", "BIUSDB", decimal.NewFromInt(1_000_000))
	ledger.Deposit("buyer", "BIUSDB", decimal.NewFromInt(1_000_000))
	options := settlement.NewOptionsSettlement(ledger, &backendclient.Client{})

	expiry := time.Now().Add(24 * time.Hour)
	longPut := "BTC-BIUSDB-50000-20260101-PUT"
	shortPut := "BTC-BIUSDB-55000-20260101-PUT"
	shortCall := "BTC-BIUSDB-65000-20260101-CALL"
	longCall := "BTC-BIUSDB-70000-20260101-CALL"

	legs := fixedLegResolver{
		legs: []models.ComboLeg{
			{Symbol: longPut, Ratio: 1},
			{Symbol: shortPut, Ratio: -1},
			{Symbol: shortCall, Ratio: -1},
			{Symbol: longCall, Ratio: 1},
		},
		underlying: "BTC-BIUSDB",
	}
	marks := fixedMarkSource{
		spot: decimal.NewFromInt(60000),
		legs: map[string]comboLegSpec{
			longPut:   {strike: decimal.NewFromInt(50000), expiry: expiry, typ: "PUT"},
			shortPut:  {strike: decimal.NewFromInt(55000), expiry: expiry, typ: "PUT"},
			shortCall: {strike: decimal.NewFromInt(65000), expiry: expiry, typ: "CALL"},
			longCall:  {strike: decimal.NewFromInt(70000), expiry: expiry, typ: "CALL"},
		},
	}
	comboSettlement := settlement.NewComboSettlement(options, legs, marks)

	comboSymbol := "COMBO:condor-test"
	eng := matching.NewEngine(comboSymbol, models.ComboOptions, noopBus{}, comboSettlement, nil)
	defer eng.Stop()

	// Writer sells the condor (collects a credit); price -300 expresses a
	// net credit in this combo's LIMIT-price convention (a SELL resting at
	// a price the incoming BUY must meet or improve on).
	sellOrder := &models.Order{
		ID: uuid.NewString(), AccountID: "writer", Symbol: comboSymbol, Market: models.ComboOptions,
		Side: models.Sell, Type: models.Limit, Price: decimal.NewFromInt(300), Quantity: decimal.NewFromInt(1),
		TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
	}
	_, err := eng.Submit(sellOrder)
	require.NoError(t, err)

	buyOrder := &models.Order{
		ID: uuid.NewString(), AccountID: "buyer", Symbol: comboSymbol, Market: models.ComboOptions,
		Side: models.Buy, Type: models.Limit, Price: decimal.NewFromInt(300), Quantity: decimal.NewFromInt(1),
		TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
	}
	trades, err := eng.Submit(buyOrder)
	require.NoError(t, err)
	require.Len(t, trades, 1, "one combo order crossing one resting combo order must produce exactly one trade")

	// All 4 legs must have settled for both accounts.
	for _, symbol := range []string{longPut, shortPut, shortCall, longCall} {
		spec := marks.legs[symbol]
		if pos := options.GetPosition("buyer", symbol, spec.strike, spec.expiry, spec.typ); pos == nil || pos.Size.IsZero() {
			t.Fatalf("buyer has no position in leg %s", symbol)
		}
		if pos := options.GetPosition("writer", symbol, spec.strike, spec.expiry, spec.typ); pos == nil || pos.Size.IsZero() {
			t.Fatalf("writer has no position in leg %s", symbol)
		}
	}
}
