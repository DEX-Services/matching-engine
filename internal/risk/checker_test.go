package risk

import (
	"testing"

	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

func newBuyOrder(qty, price string) *models.Order {
	return &models.Order{
		ID: "o1", AccountID: "buyer", Symbol: "BTC-USDT",
		Side: models.Buy, Type: models.Limit,
		Price: decimal.RequireFromString(price), Quantity: decimal.RequireFromString(qty),
	}
}

func newSellOrder(qty, price string) *models.Order {
	return &models.Order{
		ID: "o1", AccountID: "seller", Symbol: "BTC-USDT",
		Side: models.Sell, Type: models.Limit,
		Price: decimal.RequireFromString(price), Quantity: decimal.RequireFromString(qty),
	}
}

func TestReserveRelease_FullCancel(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("buyer", "USDT", decimal.NewFromInt(1000))

	order := newBuyOrder("1", "100")
	if err := checker.Reserve(order); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if got := ledger.Available("buyer", "USDT"); !got.Equal(decimal.NewFromInt(900)) {
		t.Fatalf("available after reserve = %s, want 900", got)
	}

	// Simulate cancel with nothing filled.
	checker.Release(order)
	if got := ledger.Available("buyer", "USDT"); !got.Equal(decimal.NewFromInt(1000)) {
		t.Fatalf("available after release = %s, want 1000", got)
	}
	if got := ledger.Reserved("buyer", "USDT"); !got.IsZero() {
		t.Fatalf("reserved after release = %s, want 0", got)
	}
}

func TestReserveRelease_PartialFillThenCancel(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("buyer", "USDT", decimal.NewFromInt(1000))

	order := newBuyOrder("10", "100") // reserves 1000
	if err := checker.Reserve(order); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	// Simulate a partial fill: 4 of 10 filled, settlement debits 400.
	order.Filled = decimal.NewFromInt(4)
	if err := ledger.Debit("buyer", "USDT", decimal.NewFromInt(400)); err != nil {
		t.Fatalf("debit: %v", err)
	}
	if got := ledger.Reserved("buyer", "USDT"); !got.Equal(decimal.NewFromInt(600)) {
		t.Fatalf("reserved after partial fill = %s, want 600", got)
	}

	// Cancel the remainder (6 unfilled @ 100 = 600).
	checker.Release(order)
	if got := ledger.Reserved("buyer", "USDT"); !got.IsZero() {
		t.Fatalf("reserved after cancel = %s, want 0 (no double release / residual)", got)
	}
	if got := ledger.Available("buyer", "USDT"); !got.Equal(decimal.NewFromInt(600)) {
		t.Fatalf("available after cancel = %s, want 600 (1000 - 400 debited)", got)
	}
	if got := ledger.Balance("buyer", "USDT"); !got.Equal(decimal.NewFromInt(600)) {
		t.Fatalf("balance after cancel = %s, want 600", got)
	}
}

func TestReserveRelease_RejectPath(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("buyer", "USDT", decimal.NewFromInt(1000))

	order := newBuyOrder("2", "100") // reserves 200
	if err := checker.Reserve(order); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// FOK rejection: nothing filled, release full reservation.
	checker.Release(order)
	if got := ledger.Available("buyer", "USDT"); !got.Equal(decimal.NewFromInt(1000)) {
		t.Fatalf("available after reject-release = %s, want 1000", got)
	}
}

func TestReserveRelease_SellerSide(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("seller", "BTC", decimal.NewFromInt(10))

	order := newSellOrder("10", "100") // reserves 10 BTC
	if err := checker.Reserve(order); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if got := ledger.Available("seller", "BTC"); !got.IsZero() {
		t.Fatalf("available after reserve = %s, want 0", got)
	}

	// Partial fill: 3 of 10 filled, settlement debits 3 BTC.
	order.Filled = decimal.NewFromInt(3)
	if err := ledger.Debit("seller", "BTC", decimal.NewFromInt(3)); err != nil {
		t.Fatalf("debit: %v", err)
	}
	if got := ledger.Reserved("seller", "BTC"); !got.Equal(decimal.NewFromInt(7)) {
		t.Fatalf("reserved after partial fill = %s, want 7", got)
	}

	checker.Release(order) // releases remaining 7 BTC
	if got := ledger.Reserved("seller", "BTC"); !got.IsZero() {
		t.Fatalf("reserved after cancel = %s, want 0", got)
	}
	if got := ledger.Available("seller", "BTC"); !got.Equal(decimal.NewFromInt(7)) {
		t.Fatalf("available after cancel = %s, want 7 (10 - 3 debited)", got)
	}
}

func TestRelease_NeverGoesNegative(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("buyer", "USDT", decimal.NewFromInt(1000))

	order := newBuyOrder("1", "100")
	if err := checker.Reserve(order); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	checker.Release(order)
	checker.Release(order) // double release should be a safe no-op past zero
	if got := ledger.Reserved("buyer", "USDT"); !got.IsZero() {
		t.Fatalf("reserved after double release = %s, want 0", got)
	}
	if got := ledger.Available("buyer", "USDT"); !got.Equal(decimal.NewFromInt(1000)) {
		t.Fatalf("available after double release = %s, want 1000", got)
	}
}

func TestReserve_InsufficientBalance(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("buyer", "USDT", decimal.NewFromInt(50))

	order := newBuyOrder("1", "100") // needs 100, only has 50
	if err := checker.Reserve(order); err == nil {
		t.Fatal("expected reserve to fail with insufficient balance")
	}
	if got := ledger.Reserved("buyer", "USDT"); !got.IsZero() {
		t.Fatalf("reserved after failed reserve = %s, want 0", got)
	}
}

// newFuturesCloseOrder builds a ReduceOnly futures order shaped like the
// "Close Position" action for a SHORT: a BUY that reduces/closes it. leverage
// intentionally defaults to 0 to reproduce the exact live incident this fix
// addresses -- a close request that omits leverage (Atoi("") == 0).
func newFuturesCloseOrder(qty, price string, leverage int) *models.Order {
	return &models.Order{
		ID: "close1", AccountID: "trader", Symbol: "BI2X-BI2XUSD",
		Side: models.Buy, Type: models.Limit, Market: models.Futures,
		Price: decimal.RequireFromString(price), Quantity: decimal.RequireFromString(qty),
		ReduceOnly: true, Leverage: leverage,
	}
}

// TestCheck_ReduceOnlyFutures_RequiresNoMargin is a regression test for the
// 2026-09-15 incident: a user could not close a profitable 50x SHORT
// (position margin $10.61) because the close request's missing leverage made
// the risk check demand near-full notional ($526.45) instead of nothing.
// Mirrors the exact numbers from that incident: qty 214.00377 @ price 2.46
// with leverage 0 (as if the field were omitted) previously computed a
// required amount of ~526.45 via MarginRequired's default-to-1 clamp; Check
// must now pass regardless, on an account with far less than that available.
func TestCheck_ReduceOnlyFutures_RequiresNoMargin(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	// Only $10.81 available -- less than the old ~$526.45 requirement, more
	// than enough for the correct ~$10.61 one, and (this is the point of the
	// fix) irrelevant either way: a reduce-only futures close needs no fresh
	// margin check at all.
	ledger.Deposit("trader", "BI2XUSD", decimal.RequireFromString("10.8071913811"))

	order := newFuturesCloseOrder("214.00377", "2.46", 0)
	if err := checker.Check(order); err != nil {
		t.Fatalf("Check on reduce-only futures close = %v, want nil (no margin required)", err)
	}
}

// TestCheck_NonReduceOnlyFutures_StillRequiresMargin confirms the fix is
// scoped to ReduceOnly only -- a normal position-opening futures order on the
// same account/symbol must still be margin-checked exactly as before.
func TestCheck_NonReduceOnlyFutures_StillRequiresMargin(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("trader", "BI2XUSD", decimal.RequireFromString("10.8071913811"))

	order := newFuturesCloseOrder("214.00377", "2.46", 0)
	order.ReduceOnly = false // opening, not closing
	if err := checker.Check(order); err == nil {
		t.Fatal("expected Check to still reject an under-margined non-reduce-only futures order")
	}
}

// TestReserve_ReduceOnlyFutures_LocksNothing confirms Reserve (which is what
// actually locks funds via the ledger) agrees with Check -- fixing Check
// alone while Reserve still locked the old wrong amount would have left the
// bug's real-world symptom (funds unavailable for other use) in place even
// after Check stopped rejecting the order.
func TestReserve_ReduceOnlyFutures_LocksNothing(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("trader", "BI2XUSD", decimal.NewFromInt(1000))

	order := newFuturesCloseOrder("214.00377", "2.46", 0)
	if err := checker.Reserve(order); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if got := ledger.Reserved("trader", "BI2XUSD"); !got.IsZero() {
		t.Fatalf("reserved after reduce-only futures close = %s, want 0", got)
	}
	if got := ledger.Available("trader", "BI2XUSD"); !got.Equal(decimal.NewFromInt(1000)) {
		t.Fatalf("available after reduce-only futures close reserve = %s, want unchanged 1000", got)
	}
}

// TestReserveMarket_ReduceOnlyFutures_LocksNothing covers the MARKET-order
// close path (submit.go routes Market-type orders through ReserveMarket
// regardless of ReduceOnly), which required its own fix mirroring Reserve.
func TestReserveMarket_ReduceOnlyFutures_LocksNothing(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("trader", "BI2XUSD", decimal.NewFromInt(1000))

	order := newFuturesCloseOrder("214.00377", "0", 0) // Market orders carry no own price
	order.Type = models.Market
	asset, amount, err := checker.ReserveMarket(order, decimal.RequireFromString("2.46"))
	if err != nil {
		t.Fatalf("reserveMarket: %v", err)
	}
	if !amount.IsZero() {
		t.Fatalf("ReserveMarket amount for reduce-only futures close = %s, want 0", amount)
	}
	if asset != "BI2XUSD" {
		t.Fatalf("ReserveMarket asset = %s, want BI2XUSD", asset)
	}
	if got := ledger.Reserved("trader", "BI2XUSD"); !got.IsZero() {
		t.Fatalf("reserved after ReserveMarket reduce-only futures close = %s, want 0", got)
	}
}

// TestRequiredFor_ReduceOnlyFutures_ReturnsZero covers the external-caller
// mirror (the Postgres balance-lock bridge) -- it must agree with Reserve or
// Dex-Backend would durably lock funds for a reservation the engine's own
// in-memory ledger never took.
func TestRequiredFor_ReduceOnlyFutures_ReturnsZero(t *testing.T) {
	order := newFuturesCloseOrder("214.00377", "2.46", 0)
	asset, amount := RequiredFor(order)
	if !amount.IsZero() {
		t.Fatalf("RequiredFor amount for reduce-only futures close = %s, want 0", amount)
	}
	if asset != "BI2XUSD" {
		t.Fatalf("RequiredFor asset = %s, want BI2XUSD", asset)
	}
}

// TestEstimatedRequired_ReduceOnlyFutures_ReturnsZero covers the MARKET-order
// external-caller mirror, same reasoning as TestRequiredFor above.
func TestEstimatedRequired_ReduceOnlyFutures_ReturnsZero(t *testing.T) {
	order := newFuturesCloseOrder("214.00377", "0", 0)
	order.Type = models.Market
	asset, amount := EstimatedRequired(order, decimal.RequireFromString("2.46"))
	if !amount.IsZero() {
		t.Fatalf("EstimatedRequired amount for reduce-only futures close = %s, want 0", amount)
	}
	if asset != "BI2XUSD" {
		t.Fatalf("EstimatedRequired asset = %s, want BI2XUSD", asset)
	}
}

// newSpotOCOLegOrder builds a ReduceOnly SPOT SELL order shaped like a TP or
// SL leg placed by internal/attached.BuildLegOrder for a SPOT BUY entry —
// GroupID set (as every real leg carries), ReduceOnly true.
func newSpotOCOLegOrder(qty, price string) *models.Order {
	return &models.Order{
		ID: "leg1", AccountID: "trader", Symbol: "BI2X-BI2XUSD",
		Side: models.Sell, Type: models.Limit, Market: models.Spot,
		Price: decimal.RequireFromString(price), Quantity: decimal.RequireFromString(qty),
		ReduceOnly: true, GroupID: "group-1", GroupRole: "TP",
	}
}

// TestReserve_SpotOCOLeg_LocksNothing is a regression test for SPOT TP/SL
// support (2026-09-16): a SPOT OCO leg (ReduceOnly + GroupID set) must not
// take its own reservation — attached.Execute's reserveGroup already took
// ONE shared reservation for the whole pair before either leg was
// submitted (see that function's doc comment). Reserving again per-leg
// here would double-lock the same base-asset holding for a pair where
// only one leg can ever actually fire.
func TestReserve_SpotOCOLeg_LocksNothing(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("trader", "BI2X", decimal.NewFromInt(10))
	ledger.Reserve("trader", "BI2X", decimal.NewFromInt(10)) // simulates reserveGroup's up-front shared lock

	order := newSpotOCOLegOrder("10", "6.00")
	if err := checker.Reserve(order); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if got := ledger.Reserved("trader", "BI2X"); !got.Equal(decimal.NewFromInt(10)) {
		t.Fatalf("reserved after spot OCO leg reserve = %s, want unchanged 10 (no second lock)", got)
	}
}

// TestReserve_SpotNonOCOOrder_StillRequiresFullReservation confirms the
// exemption is scoped precisely to GroupID-tagged reduce-only spot orders —
// an ordinary spot sell (no group, or ReduceOnly false) must still reserve
// normally. Getting this scoping wrong in either direction is a real bug:
// too broad would let ordinary spot sells skip reservation entirely
// (unbacked orders); too narrow would reintroduce the OCO double-lock.
func TestReserve_SpotNonOCOOrder_StillRequiresFullReservation(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("trader", "BI2X", decimal.NewFromInt(10))

	order := newSpotOCOLegOrder("10", "6.00")
	order.GroupID = "" // not an attached-order leg
	if err := checker.Reserve(order); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if got := ledger.Reserved("trader", "BI2X"); !got.Equal(decimal.NewFromInt(10)) {
		t.Fatalf("reserved after ordinary spot sell reserve = %s, want 10 (normal reservation, not exempted)", got)
	}
}

// TestRelease_SpotOCOLeg_ReleasesNothing is the Release-side counterpart to
// TestReserve_SpotOCOLeg_LocksNothing: when one OCO leg fills and its
// sibling is cancelled, cancelling the sibling must NOT release any amount
// (it never separately reserved one) — doing so would release phantom
// availability for this account+asset that the shared reservation's actual
// consumer (whichever leg filled) already accounted for.
func TestRelease_SpotOCOLeg_ReleasesNothing(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("trader", "BI2X", decimal.NewFromInt(10))
	ledger.Reserve("trader", "BI2X", decimal.NewFromInt(10)) // the shared reservation, still held (sibling filled and consumed nothing more of it here since this test only exercises the cancelled leg's Release call)

	order := newSpotOCOLegOrder("10", "6.00")
	checker.Release(order)
	if got := ledger.Reserved("trader", "BI2X"); !got.Equal(decimal.NewFromInt(10)) {
		t.Fatalf("reserved after cancelling un-filled OCO sibling = %s, want unchanged 10 (nothing to release for this leg)", got)
	}
}

// TestRelease_SpotNonOCOOrder_StillReleasesNormally confirms Release's new
// exemption doesn't affect an ordinary (non-attached-order) spot cancel.
func TestRelease_SpotNonOCOOrder_StillReleasesNormally(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("trader", "BI2X", decimal.NewFromInt(10))
	ledger.Reserve("trader", "BI2X", decimal.NewFromInt(10))

	order := newSpotOCOLegOrder("10", "6.00")
	order.GroupID = ""
	checker.Release(order)
	if got := ledger.Reserved("trader", "BI2X"); !got.IsZero() {
		t.Fatalf("reserved after cancelling ordinary spot order = %s, want 0 (normal release)", got)
	}
}
