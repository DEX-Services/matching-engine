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
type fixedLegResolver struct{ buySymbol, sellSymbol, underlying string }

func (f fixedLegResolver) ResolveComboLegs(ctx context.Context, comboSymbol string) (string, string, string, error) {
	return f.buySymbol, f.sellSymbol, f.underlying, nil
}

type comboLeg struct {
	strike decimal.Decimal
	expiry time.Time
	typ    string
}

type fixedMarkSource struct {
	spot decimal.Decimal
	legs map[string]comboLeg
}

func (f fixedMarkSource) UnderlyingMark(underlying string) (decimal.Decimal, bool) {
	return f.spot, true
}

func (f fixedMarkSource) LegSpec(ctx context.Context, legSymbol string) (decimal.Decimal, time.Time, string, string, bool) {
	l, ok := f.legs[legSymbol]
	if !ok {
		return decimal.Zero, time.Time{}, "", "", false
	}
	return l.strike, l.expiry, l.typ, "BIUSD", true
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
	ledger.Deposit("writer", "BIUSD", decimal.NewFromInt(1_000_000))
	ledger.Deposit("buyer", "BIUSD", decimal.NewFromInt(1_000_000))
	options := settlement.NewOptionsSettlement(ledger, &backendclient.Client{})

	expiry := time.Now().Add(24 * time.Hour)
	buySymbol := "BTC-BIUSD-60000-20260101-CALL"
	sellSymbol := "BTC-BIUSD-65000-20260101-CALL"
	legs := fixedLegResolver{buySymbol: buySymbol, sellSymbol: sellSymbol, underlying: "BTC-BIUSD"}
	marks := fixedMarkSource{
		spot: decimal.NewFromInt(62000),
		legs: map[string]comboLeg{
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
	buyerBalance := ledger.Available("buyer", "BIUSD")
	require.True(t, buyerBalance.Equal(decimal.NewFromInt(1_000_000-200)), "buyer balance = %s", buyerBalance)
	writerBalance := ledger.Available("writer", "BIUSD")
	require.True(t, writerBalance.Equal(decimal.NewFromInt(1_000_000+200)), "writer balance = %s", writerBalance)
}
