package attached

import (
	"log/slog"

	"github.com/dex/matching-engine/internal/fixedpoint"
	"github.com/dex/matching-engine/internal/models"
)

// Canceller is the subset of the matching registry the listener needs to
// cancel a resting protective order. Implemented by *matching.Registry.
type Canceller interface {
	Cancel(symbol string, market models.MarketType, orderID string) (*models.Order, error)
}

// Submitter resubmits a resized protective leg. Implemented by
// *matching.Registry (via SubmitSnapshot, ignoring the trade/snapshot
// results here since a resized reduce-only STOP/LIMIT never crosses).
type Submitter interface {
	SubmitSnapshot(order *models.Order) ([]*models.Trade, *models.Order, error)
}

// PositionSizer reports the current absolute exposure for an account/symbol
// so Resize can cap protection to what is actually still open. Implemented
// by *settlement.FuturesSettlement for FUTURES, and (added 2026-09-16 for
// SPOT TP/SL support) by a SPOT-specific adapter reporting the account's
// current base-asset wallet balance instead of a margined position size —
// see NewListener's spotPos parameter.
type PositionSizer interface {
	CurrentSize(accountID, symbol string) fixedpoint.Fixed
}

// Listener reacts to order/liquidation events for orders that belong to an
// attached group, enforcing OCO (cancel the sibling leg when one triggers)
// and fill/exposure-aware resizing (shrink or cancel protection when the
// underlying position shrinks from a partial close, liquidation, or
// reversal). It is wired as an events.Bus subscriber, external to the
// matching engine goroutine, consistent with the ws hub / Postgres writer.
//
// Two PositionSizers, not one: FUTURES exposure is a margined position
// (*settlement.FuturesSettlement.CurrentSize); SPOT exposure (added
// 2026-09-16) is just "how much of the base asset does this account still
// hold" — a spot holding can shrink via an ordinary manual sell with no
// "position" object involved at all, which futures's position-based signal
// has no way to report. Each group carries its own Market (see groups.go),
// so sizeFor picks the right sizer per group rather than assuming one
// market for the whole listener.
type Listener struct {
	reg     *Registry
	cancel  Canceller
	submit  Submitter
	pos     PositionSizer // FUTURES
	spotPos PositionSizer // SPOT; nil-safe, same as pos
	log     *slog.Logger
}

func NewListener(reg *Registry, cancel Canceller, submit Submitter, pos, spotPos PositionSizer) *Listener {
	return &Listener{reg: reg, cancel: cancel, submit: submit, pos: pos, spotPos: spotPos, log: slog.Default()}
}

// sizeFor picks the PositionSizer matching g's market — see Listener's doc
// comment for why FUTURES and SPOT need genuinely different sizers, not
// just different arguments to the same one.
func (l *Listener) sizeFor(g *Group) PositionSizer {
	if g.Market == models.Spot {
		return l.spotPos
	}
	return l.pos
}

// Run consumes events from ch until it is closed. Intended to be started in
// its own goroutine, mirroring the ws hub / trade-history writer pattern.
func (l *Listener) Run(ch <-chan *models.Event) {
	for evt := range ch {
		l.handle(evt)
	}
}

func (l *Listener) handle(evt *models.Event) {
	switch evt.Type {
	case models.EventOrderFilled, models.EventOrderPartial:
		if evt.Order == nil {
			return
		}
		if evt.Order.GroupID != "" {
			// One of this group's own protective legs filled/partially
			// filled: enforce OCO by cancelling its sibling immediately.
			l.onLegFilled(evt.Order)
			return
		}
		// Any other fill (entry orders, manual reduce-only closes, resized
		// legs, etc.) can shrink or grow the account's position, so resize
		// every active group on this account/symbol to match. Groups with
		// no change are cheap no-ops (see onExposureChanged).
		l.onExposureChanged(evt.Order.AccountID, evt.Order.Symbol)
	case models.EventLiquidation:
		if evt.Liquidation != nil {
			l.onExposureChanged(evt.Liquidation.AccountID, evt.Liquidation.Symbol)
		}
	}
}

// onLegFilled runs OCO: when one protective leg fills (fully or partially),
// its sibling is triggered-cancelled immediately so a filled TP can never
// coexist with a live SL for the same protected exposure (and vice versa).
func (l *Listener) onLegFilled(o *models.Order) {
	group, peerLegID, err := l.reg.Trigger(o.GroupID, o.ID)
	if err != nil {
		// Already triggered (peer filled first and cancel is already in
		// flight) or unknown group (e.g. group expired) - not an error
		// worth surfacing, just skip.
		return
	}
	if peerLegID == "" {
		return
	}
	if _, cerr := l.cancel.Cancel(group.Symbol, group.Market, peerLegID); cerr != nil {
		l.log.Warn("attached: failed to cancel OCO sibling leg", "group", group.ID, "peer", peerLegID, "err", cerr)
	}
}

// onExposureChanged resizes (or removes) protection after the account's
// position/holding in symbol changes for any reason other than a group leg
// itself filling (partial close, liquidation, reversal, a manual close
// order, or — added 2026-09-16 for SPOT — a manual sell of the base asset
// elsewhere).
//
// Exposure is computed PER GROUP, not once per account+symbol: the engine's
// internal symbol string is the SAME for a base asset's SPOT and FUTURES
// markets (e.g. "BI2X-BI2XUSD" for both — see backendMarkets' REGISTERED
// table), so a single shared exposure number computed with the wrong
// market's sizer would silently misreport a spot group's holding as a
// futures position's size or vice versa. Each Group carries its own Market
// (added alongside this), so sizeFor picks the correct sizer every time.
func (l *Listener) onExposureChanged(accountID, symbol string) {
	if accountID == "" || symbol == "" {
		return
	}
	for _, g := range l.reg.GroupsFor(accountID, symbol) {
		sizer := l.sizeFor(g)
		if sizer == nil {
			continue
		}
		exposure := sizer.CurrentSize(accountID, symbol).Abs()
		resized, ok := l.reg.Resize(g.ID, exposure)
		if !ok {
			// Exposure hit zero: registry already dropped the group.
			// Cancel any still-resting legs so no orphaned protection stays live.
			// resized still carries the (now-stale) leg IDs for this purpose.
			if resized != nil {
				l.cancelGroupLegs(resized)
			}
			continue
		}
		if resized.ProtectedQty.Equal(g.ProtectedQty) {
			continue // no change, nothing to resubmit
		}
		l.resizeGroupLegs(resized)
	}
}

func (l *Listener) cancelGroupLegs(g *Group) {
	if g.TakeProfit != nil && g.TakeProfit.Active {
		_, _ = l.cancel.Cancel(g.Symbol, g.Market, g.TakeProfit.ID)
	}
	if g.StopLoss != nil && g.StopLoss.Active {
		_, _ = l.cancel.Cancel(g.Symbol, g.Market, g.StopLoss.ID)
	}
}

// resizeGroupLegs cancels each active leg and resubmits it at the reduced
// protected quantity. The registry has no in-place quantity mutation for
// resting orders, so cancel-and-replace reuses the already-tested
// Cancel/SubmitSnapshot paths rather than adding a new mutation surface to
// the matching core.
func (l *Listener) resizeGroupLegs(g *Group) {
	if g.ProtectedQty.LessThanOrEqual(fixedpoint.Zero) {
		l.cancelGroupLegs(g)
		return
	}
	if g.TakeProfit != nil && g.TakeProfit.Active {
		l.replaceLeg(g, g.TakeProfit, "TP")
	}
	if g.StopLoss != nil && g.StopLoss.Active {
		l.replaceLeg(g, g.StopLoss, "SL")
	}
}

func (l *Listener) replaceLeg(g *Group, leg *Leg, role string) {
	if _, err := l.cancel.Cancel(g.Symbol, g.Market, leg.ID); err != nil {
		l.log.Warn("attached: resize cancel failed", "group", g.ID, "leg", leg.ID, "err", err)
		return
	}
	newLeg := BuildLegOrder(*g, leg, role, g.ProtectedQty)
	_, _, err := l.submit.SubmitSnapshot(newLeg)
	if err != nil {
		l.log.Warn("attached: resize resubmit failed", "group", g.ID, "role", role, "err", err)
		return
	}
	l.reg.RelinkLeg(g.ID, role, newLeg.ID)
}
