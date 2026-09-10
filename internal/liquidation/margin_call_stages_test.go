package liquidation

import (
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/events"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/dex/matching-engine/internal/settlement"
	"github.com/shopspring/decimal"
)

// TestLossBufferConsumed covers the band arithmetic behind the options
// margin-call warning.
//
// Background: the warning stage never actually existed. The constant that was
// supposed to drive it (optionsMarginCallThresholdPct = 120, "warn at 120% of
// maintenance") was declared and never referenced anywhere, so the sweep
// skipped every position with equity >= maintenance and then warned AND
// force-closed in the same instant once it fell below. A writer's first
// notification was their liquidation receipt.
//
// Wiring that 120 in as-written would have been worse than leaving it dead.
// equity = reserved + PnL and maintenance == reserved here, so
// equity/maintenance = 1 + PnL/reserved; a writer's PnL is capped at the
// premium received, which is tiny beside the 20%-of-underlying margin floor.
// Measured on a real position (60k call, 500 premium) the best possible case —
// option expires worthless, writer keeps the entire premium — reaches only
// 1.11, and breakeven sits at exactly 1.00. Every healthy writer would have
// been held in permanent warning.
//
// lossBufferConsumed is bounded by construction instead: 0 at full collateral,
// 1 at the liquidation point, whatever the strike or premium. It measures
// against the BUFFER (full requirement minus the maintenance bar), not against
// the full requirement — see the function's own doc for why that distinction
// is what keeps the warning strictly ahead of the liquidation.
func TestLossBufferConsumed(t *testing.T) {
	d := decimal.NewFromInt

	// With reserved=10000 and a 50% maintenance bar, the buffer is the 5000
	// between full collateral and the 5000 liquidation bar.
	cases := []struct {
		name     string
		reserved decimal.Decimal
		equity   decimal.Decimal
		want     string
	}{
		{"untouched collateral consumes nothing", d(10000), d(10000), "0"},
		{"a profitable writer consumes nothing", d(10000), d(11000), "0"},
		{"half the buffer gone", d(10000), d(7500), "0.5"},
		{"three quarters gone — the warning point", d(10000), d(6250), "0.75"},
		{"at the liquidation bar", d(10000), d(5000), "1"},
		{"past the liquidation bar", d(10000), decimal.Zero, "2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := lossBufferConsumed(tc.reserved, tc.equity)
			if !got.Equal(decimal.RequireFromString(tc.want)) {
				t.Fatalf("lossBufferConsumed(%s, %s) = %s, want %s", tc.reserved, tc.equity, got, tc.want)
			}
		})
	}
}

// TestLossBufferConsumed_NonPositiveReservedFailsClosed: a position with no
// buffer left to measure must never be reported as healthy.
func TestLossBufferConsumed_NonPositiveReservedFailsClosed(t *testing.T) {
	for _, reserved := range []decimal.Decimal{decimal.Zero, decimal.NewFromInt(-100)} {
		got := lossBufferConsumed(reserved, decimal.NewFromInt(1000))
		if !got.Equal(decimal.NewFromInt(1)) {
			t.Fatalf("reserved=%s must report a fully-consumed buffer, got %s", reserved, got)
		}
	}
}

// TestWarningFiresBeforeLiquidation is the property that was missing entirely:
// the warning threshold must be strictly inside the healthy range, so a
// position crosses it while still solvent and closable, rather than at the
// same moment it is force-closed.
func TestWarningFiresBeforeLiquidation(t *testing.T) {
	if !warningLossFraction.LessThan(decimal.NewFromInt(1)) {
		t.Fatalf("warning fraction %s must be < 1 (the liquidation point), otherwise the warning arrives at or after the force-close and gives the writer no window to react",
			warningLossFraction)
	}
	if !warningLossFraction.IsPositive() {
		t.Fatalf("warning fraction %s must be > 0, otherwise every position warns from the moment it opens", warningLossFraction)
	}

	// The ordering invariant, stated in equity terms: the equity level that
	// triggers a warning must sit strictly ABOVE the equity level that
	// triggers a liquidation, or the warning is unreachable.
	//
	// An earlier version of lossBufferConsumed divided by `reserved` instead
	// of by the buffer, which put the 75% warning at equity = 25% of reserved
	// — below the 50% liquidation bar — so every position was force-closed
	// before its warning could fire. This test would have caught that.
	reserved := decimal.NewFromInt(10000)
	liquidationAt := maintenanceBar(reserved)
	buffer := reserved.Sub(liquidationAt)
	warningAt := reserved.Sub(buffer.Mul(warningLossFraction))

	if !warningAt.GreaterThan(liquidationAt) {
		t.Fatalf("warning triggers at equity %s but liquidation triggers at %s: the position is closed before it can ever be warned",
			warningAt, liquidationAt)
	}

	// A position sitting exactly at the warning threshold must be in the
	// warning band and NOT yet liquidatable.
	if !lossBufferConsumed(reserved, warningAt).GreaterThanOrEqual(warningLossFraction) {
		t.Fatal("a position at the warning threshold must trigger the warning band")
	}
	if warningAt.LessThan(liquidationAt) {
		t.Fatal("a position at the warning threshold must not already be past liquidation")
	}
}

// TestMaintenanceBarLeavesRoomToMove is the fix for the bug that made the
// warning stage unreachable in practice.
//
// The maintenance bar used to be the FULL current requirement, so with
// equity = reserved + PnL the liquidation test reduced to `PnL < 0` — a
// writer was force-closed on essentially any unrealized loss. Measured on a
// real position (60k call, 500 premium) at spot 61,000, the writer held
// equity 11,836 against a required 12,700 and was liquidated having consumed
// only ~7% of their collateral, with ~12,000 still posted against a ~1,000
// loss.
func TestMaintenanceBarLeavesRoomToMove(t *testing.T) {
	reserved := decimal.NewFromInt(10000)
	bar := maintenanceBar(reserved)

	if !bar.LessThan(reserved) {
		t.Fatalf("maintenance bar %s must be below the full requirement %s, otherwise liquidation reduces to 'any unrealized loss'", bar, reserved)
	}
	if !bar.IsPositive() {
		t.Fatalf("maintenance bar %s must be positive, otherwise a writer is never liquidated however deep the loss", bar)
	}

	// A writer barely underwater must survive: this is the case that used to
	// force-close.
	barelyUnderwater := reserved.Sub(decimal.NewFromInt(1000)) // 10% of collateral gone
	if barelyUnderwater.LessThan(bar) {
		t.Fatalf("a writer with 90%% of collateral intact (equity %s) must not be liquidatable (bar %s)", barelyUnderwater, bar)
	}
}

// TestMarginCallStages_WarnsWhileSolventThenLiquidates is the end-to-end
// proof of the behavior that was missing: a writer whose position has moved
// against them gets a WARNING while still solvent and NOT closed, and is only
// force-closed once equity actually goes through the maintenance requirement.
//
// Before this fix both of these spot levels produced the same outcome —
// immediate force-close with the "warning" event emitted in the same instant.
func TestMarginCallStages_WarnsWhileSolventThenLiquidates(t *testing.T) {
	const symbol = "BTC-BIUSD-60000-20260101-CALL"

	// Written when BTC was 50k (well OTM, cheap premium), then the underlying
	// runs up through the strike. At 66.5k the writer has burned ~87% of their
	// loss buffer but is still above the maintenance bar; by 75k they are well
	// through it. Under the old full-requirement bar BOTH of these liquidated
	// immediately, which is what made the warning stage unreachable.
	t.Run("warns without closing while still solvent", func(t *testing.T) {
		os, eng, ch := openWriterThenMoveSpot(t, symbol, 50000, 66500)

		stage, liquidated := drainMarginCall(ch)
		if stage != models.MarginCallWarning {
			t.Fatalf("stage = %q, want %q — a solvent writer must be warned, not liquidated", stage, models.MarginCallWarning)
		}
		if liquidated {
			t.Fatal("position was force-closed during the warning stage; the writer gets no window to react")
		}
		pos := os.GetPosition("writer", symbol, decimal.NewFromInt(60000), time.Now().Add(24*time.Hour), "CALL")
		if pos == nil || pos.Size.IsZero() {
			t.Fatal("expected the position to still be open after a warning")
		}
		_ = eng
	})

	t.Run("liquidates once equity goes through maintenance", func(t *testing.T) {
		os, eng, ch := openWriterThenMoveSpot(t, symbol, 50000, 75000)

		stage, liquidated := drainMarginCall(ch)
		if stage != models.MarginCallLiquidated {
			t.Fatalf("stage = %q, want %q", stage, models.MarginCallLiquidated)
		}
		if !liquidated {
			t.Fatal("expected an EventLiquidation for an insolvent writer")
		}
		pos := os.GetPosition("writer", symbol, decimal.NewFromInt(60000), time.Now().Add(24*time.Hour), "CALL")
		if pos != nil && !pos.Size.IsZero() {
			t.Fatalf("expected the position to be force-closed, still open: %+v", pos)
		}
		_ = eng
	})
}

// openWriterThenMoveSpot writes a short call at openSpot, then re-marks the
// world at nowSpot and runs one margin-call sweep.
func openWriterThenMoveSpot(t *testing.T, symbol string, openSpot, nowSpot int64) (*settlement.OptionsSettlement, *Engine, <-chan *models.Event) {
	t.Helper()
	ledger := risk.NewLedger()
	ledger.Deposit("buyer", "BIUSD", decimal.NewFromInt(50_000_000))
	ledger.Deposit("writer", "BIUSD", decimal.NewFromInt(50_000_000))
	os := settlement.NewOptionsSettlement(ledger, &backendclient.Client{})

	risk.SetMarkSource(fakeMarkSource{"BTC-BIUSD": decimal.NewFromInt(openSpot)})
	openShortOption(t, os, "writer", symbol, "CALL", "60000", "1", "500")

	risk.SetMarkSource(fakeMarkSource{"BTC-BIUSD": decimal.NewFromInt(nowSpot)})
	t.Cleanup(func() { risk.SetMarkSource(nil) })

	md := mdWithUnderlyingSpot("BTC-BIUSD", decimal.NewFromInt(nowSpot))
	bus := events.NewBus()
	ch := bus.Subscribe(20)
	eng := newTestOptionsEngine(os, md, bus)
	eng.checkOptionsMarginCalls()
	return os, eng, ch
}

// drainMarginCall reads every buffered event, returning the margin-call stage
// seen and whether a liquidation was published.
func drainMarginCall(ch <-chan *models.Event) (models.MarginCallStage, bool) {
	var stage models.MarginCallStage
	var liquidated bool
	for {
		select {
		case evt := <-ch:
			if evt.MarginCallInfo != nil {
				stage = evt.MarginCallInfo.Stage
			}
			if evt.Type == models.EventLiquidation {
				liquidated = true
			}
		default:
			return stage, liquidated
		}
	}
}
