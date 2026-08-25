package attached

import (
	"log/slog"

	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
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
// by *settlement.FuturesSettlement.
type PositionSizer interface {
	CurrentSize(accountID, symbol string) decimal.Decimal
}

// Listener reacts to order/liquidation events for orders that belong to an
// attached group, enforcing OCO (cancel the sibling leg when one triggers)
// and fill/exposure-aware resizing (shrink or cancel protection when the
// underlying position shrinks from a partial close, liquidation, or
// reversal). It is wired as an events.Bus subscriber, external to the
// matching engine goroutine, consistent with the ws hub / Postgres writer.
type Listener struct {
	reg    *Registry
	cancel Canceller
	submit Submitter
	pos    PositionSizer
	log    *slog.Logger
}

func NewListener(reg *Registry, cancel Canceller, submit Submitter, pos PositionSizer) *Listener {
	return &Listener{reg: reg, cancel: cancel, submit: submit, pos: pos, log: slog.Default()}
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
	if _, cerr := l.cancel.Cancel(group.Symbol, models.Futures, peerLegID); cerr != nil {
		l.log.Warn("attached: failed to cancel OCO sibling leg", "group", group.ID, "peer", peerLegID, "err", cerr)
	}
}

// onExposureChanged resizes (or removes) protection after the account's
// position in symbol changes for any reason other than a group leg itself
// filling (partial close, liquidation, reversal, or a manual close order).
func (l *Listener) onExposureChanged(accountID, symbol string) {
	if accountID == "" || symbol == "" || l.pos == nil {
		return
	}
	exposure := l.pos.CurrentSize(accountID, symbol).Abs()
	for _, g := range l.reg.GroupsFor(accountID, symbol) {
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
		_, _ = l.cancel.Cancel(g.Symbol, models.Futures, g.TakeProfit.ID)
	}
	if g.StopLoss != nil && g.StopLoss.Active {
		_, _ = l.cancel.Cancel(g.Symbol, models.Futures, g.StopLoss.ID)
	}
}

// resizeGroupLegs cancels each active leg and resubmits it at the reduced
// protected quantity. The registry has no in-place quantity mutation for
// resting orders, so cancel-and-replace reuses the already-tested
// Cancel/SubmitSnapshot paths rather than adding a new mutation surface to
// the matching core.
func (l *Listener) resizeGroupLegs(g *Group) {
	if g.ProtectedQty.LessThanOrEqual(decimal.Zero) {
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
	if _, err := l.cancel.Cancel(g.Symbol, models.Futures, leg.ID); err != nil {
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
