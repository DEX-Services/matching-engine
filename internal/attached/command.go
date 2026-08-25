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

func Execute(reg *Registry, cmd Command, submit Submit, submitLeg SubmitLeg) (*models.Order, *Group, error) {
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

	// Place the actual resting orders on the book now that protection is
	// activated for the filled quantity. Each leg order reuses the ID
	// already recorded on the Leg so the registry and the live order agree.
	if group.TakeProfit != nil && group.TakeProfit.Active {
		leg := BuildLegOrder(*group, group.TakeProfit, "TP", group.ProtectedQty)
		leg.ID = group.TakeProfit.ID
		if err := submitLeg(leg); err != nil {
			slog.Default().Warn("attached: failed to place TP leg", "group", group.ID, "err", err)
		}
	}
	if group.StopLoss != nil && group.StopLoss.Active {
		leg := BuildLegOrder(*group, group.StopLoss, "SL", group.ProtectedQty)
		leg.ID = group.StopLoss.ID
		if err := submitLeg(leg); err != nil {
			slog.Default().Warn("attached: failed to place SL leg", "group", group.ID, "err", err)
		}
	}
	return result, group, nil
}
