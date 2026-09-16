package attached

import (
	"fmt"
	"log/slog"

	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

// Command is the engine-facing atomic unit: entry plus declarative exits.
// Exits are registered - and only then actually placed on the book - once
// Submit reports an actual fill, so an unfilled/rejected entry never leaves
// a resting TP or SL behind.
type Command struct {
	Group Group
	Entry *models.Order
}
type Submit func(*models.Order) (*models.Order, error)

// SubmitLeg places a built protective leg order on the book. Errors placing
// one leg are logged, not fatal to the whole command: the entry already
// filled and must not be unwound, so a best-effort second leg is better
// than none (the remaining leg still protects the position; the missing
// one is visible to the caller via the returned Group's Active flags).
type SubmitLeg func(*models.Order) error

// ReserveGroup takes the ONE shared reservation backing a SPOT TP/SL pair,
// for the group's ProtectedQty of its base asset, before either leg is
// submitted. Nil for FUTURES commands (that market has no equivalent — the
// position's already-posted margin backs both legs, see
// risk.exemptFromFreshReservation).
//
// Why a single shared reservation instead of letting each leg reserve its
// own: added 2026-09-16 alongside SPOT TP/SL support. A SPOT TP leg and SL
// leg are both SELL orders for the SAME underlying holding, and by OCO
// construction only one of them can ever actually execute — but each is an
// ordinary order as far as risk.Checker's reservation logic is concerned,
// with no futures-style "already margined" exemption to fall back on
// (there is no margin in spot). Letting BuildLegOrder's two legs each
// independently call Reserve for the full ProtectedQty would require the
// account to hold 2×qty of the base asset just to place a TP+SL pair on a
// qty it only has once — either double-locking funds the user doesn't have
// spare, or failing the second leg's Reserve outright with "insufficient
// balance" and silently leaving only one leg of the pair actually placed.
// Reserving once here, up front, and then having both legs submit with
// ReduceOnly+GroupID set (which risk.Checker's exemption trusts as "already
// covered, don't reserve again") means exactly one lock of qty exists for
// the pair, regardless of which leg eventually fires — the leg that fills
// releases it via its own normal settlement debit; the sibling that gets
// OCO-cancelled never separately reserved anything to release.
type ReserveGroup func(Group) error

// ReleaseGroup undoes a ReserveGroup lock. Only ever called when NO leg
// ended up actually resting on the book after a successful ReserveGroup —
// see Execute's cleanup step below for why this matters and the incident
// that motivated it.
type ReleaseGroup func(Group) error

func Execute(reg *Registry, cmd Command, submit Submit, submitLeg SubmitLeg, reserveGroup ReserveGroup, releaseGroup ReleaseGroup) (*models.Order, *Group, error) {
	if cmd.Entry == nil || cmd.Group.ParentOrderID != cmd.Entry.ID {
		return nil, nil, fmt.Errorf("attached entry/group mismatch")
	}
	result, err := submit(cmd.Entry)
	if err != nil {
		return nil, nil, err
	}
	if result.Filled.LessThanOrEqual(decimal.Zero) {
		return result, nil, nil
	}
	cmd.Group.AccountID, cmd.Group.Symbol = result.AccountID, result.Symbol
	cmd.Group.EntrySide = result.Side
	if err := reg.Activate(cmd.Group, result.Filled); err != nil {
		return result, nil, err
	}
	group, _ := reg.Get(cmd.Group.ID)

	// SPOT only: take the single shared reservation backing both legs before
	// placing either — see ReserveGroup's doc comment. If this fails (e.g. the
	// entry fill somehow left less available than it should have), no legs
	// are placed at all rather than placing an under-collateralized pair;
	// the entry fill itself is NOT unwound (same reasoning as the per-leg
	// best-effort placement below — an already-filled entry must stand).
	if group.Market == models.Spot && reserveGroup != nil {
		if err := reserveGroup(*group); err != nil {
			slog.Default().Warn("attached: shared SPOT reservation failed, no protective legs placed", "group", group.ID, "err", err)
			// Nothing was locked (reserveGroup itself failed), but the group
			// is still registered as "active" describing legs that will
			// never be placed — same phantom-group cleanup as the
			// zero-legs-placed case below, for the same reason: leave no
			// registry entry around with nothing real backing it.
			reg.Resize(group.ID, decimal.Zero)
			return result, nil, nil
		}
	}

	// Place the actual resting orders on the book now that protection is
	// activated for the filled quantity. Each leg order reuses the ID
	// already recorded on the Leg so the registry and the live order agree.
	legsPlaced := 0
	if group.TakeProfit != nil && group.TakeProfit.Active {
		leg := BuildLegOrder(*group, group.TakeProfit, "TP", group.ProtectedQty)
		leg.ID = group.TakeProfit.ID
		if err := submitLeg(leg); err != nil {
			slog.Default().Warn("attached: failed to place TP leg", "group", group.ID, "err", err)
		} else {
			legsPlaced++
		}
	}
	if group.StopLoss != nil && group.StopLoss.Active {
		leg := BuildLegOrder(*group, group.StopLoss, "SL", group.ProtectedQty)
		leg.ID = group.StopLoss.ID
		if err := submitLeg(leg); err != nil {
			slog.Default().Warn("attached: failed to place SL leg", "group", group.ID, "err", err)
		} else {
			legsPlaced++
		}
	}

	// Fixed 2026-09-16: a live incident showed that if a SPOT group's shared
	// reservation succeeded but EVERY leg then failed to place (e.g. the
	// symbol was halted at that exact moment), the reservation stayed
	// locked forever — no leg ever existed for the user to cancel, and the
	// registry still held the group as "active" describing legs that were
	// never actually placed. Funds were not lost, but became unreachable
	// through any normal user-facing action: nothing to cancel, no visible
	// order, no automatic recovery. If NO leg made it onto the book at all,
	// undo both the reservation and the registry entry here so this failure
	// mode leaves nothing stranded — the caller sees an empty/no-op result
	// (Active flags all false) exactly as if the legs had never been
	// requested, and the shared reservation this Execute call took is
	// released back to the account immediately rather than silently.
	//
	// Deliberately scoped to "zero legs placed", not "any leg failed": a
	// partial placement (one leg up, one down) is the existing, correct
	// best-effort behavior — the resting leg still protects the position,
	// and its own eventual cancel/fill will release the shared reservation
	// normally. Only total failure leaves nothing to ever release it.
	if legsPlaced == 0 && group.Market == models.Spot && releaseGroup != nil {
		if err := releaseGroup(*group); err != nil {
			slog.Default().Error("attached: failed to release shared SPOT reservation after all legs failed to place — funds may be stuck, needs manual investigation", "group", group.ID, "account", group.AccountID, "err", err)
		}
		reg.Resize(group.ID, decimal.Zero) // remove the group entirely: no legs exist for it to protect
		return result, nil, nil
	}
	return result, group, nil
}
