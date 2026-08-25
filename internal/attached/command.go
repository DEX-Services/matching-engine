package attached

import (
	"fmt"
	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

// Command is the engine-facing atomic unit: entry plus declarative exits.
// Exits are registered only after Submit reports an actual fill.
type Command struct {
	Group Group
	Entry *models.Order
}
type Submit func(*models.Order) (*models.Order, error)

func Execute(reg *Registry, cmd Command, submit Submit) (*models.Order, *Group, error) {
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
	if err := reg.Activate(cmd.Group, result.Filled); err != nil {
		return result, nil, err
	}
	group, _ := reg.Get(cmd.Group.ID)
	return result, group, nil
}
