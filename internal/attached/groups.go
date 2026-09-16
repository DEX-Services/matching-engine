// Package attached owns the lifecycle rules for fill-aware protective orders.
package attached

import (
	"fmt"
	"sync"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type Leg struct {
	ID         string
	StopPrice  decimal.Decimal
	LimitPrice decimal.Decimal
	Active     bool
}

type Group struct {
	ID, AccountID, Symbol, ParentOrderID string
	// Market is the entry order's market (FUTURES or SPOT — see
	// BuildLegOrder's doc comment on the 2026-09-16 SPOT support added
	// alongside this field; previously always implicitly FUTURES, hardcoded
	// in BuildLegOrder rather than carried on the group).
	Market models.MarketType
	// EntrySide is the entry order's side. Both protective legs close the
	// position, so they submit on the opposite side (entry BUY -> legs SELL).
	EntrySide            models.OrderSide
	ProtectedQty         decimal.Decimal
	TakeProfit, StopLoss *Leg
	TriggeredLeg         string
}

type Registry struct {
	mu     sync.RWMutex
	groups map[string]*Group
}

func NewRegistry() *Registry { return &Registry{groups: make(map[string]*Group)} }

// Activate stores only the actual filled quantity. A zero-fill entry creates
// no protection, so an unfilled/resting entry cannot leave stray exits live.
func (r *Registry) Activate(g Group, filled decimal.Decimal) error {
	if filled.LessThanOrEqual(decimal.Zero) {
		return nil
	}
	if g.ID == "" || g.ParentOrderID == "" || (g.TakeProfit == nil && g.StopLoss == nil) {
		return fmt.Errorf("invalid attached order group")
	}
	g.ProtectedQty = filled
	r.mu.Lock()
	defer r.mu.Unlock()
	copy := g
	if copy.TakeProfit != nil {
		copy.TakeProfit.Active = true
	}
	if copy.StopLoss != nil {
		copy.StopLoss.Active = true
	}
	r.groups[g.ID] = &copy
	return nil
}

// Trigger marks one protective leg as consumed and returns the peer that must
// be cancelled atomically by the order executor (OCO semantics).
func (r *Registry) Trigger(id, legID string) (*Group, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[id]
	if !ok {
		return nil, "", fmt.Errorf("attached group not found")
	}
	if g.TriggeredLeg != "" {
		return nil, "", fmt.Errorf("attached group already triggered")
	}
	if (g.TakeProfit == nil || g.TakeProfit.ID != legID) && (g.StopLoss == nil || g.StopLoss.ID != legID) {
		return nil, "", fmt.Errorf("leg does not belong to group")
	}
	g.TriggeredLeg = legID
	peer := ""
	if g.TakeProfit != nil && g.TakeProfit.ID == legID {
		g.TakeProfit.Active = false
		if g.StopLoss != nil {
			g.StopLoss.Active = false
			peer = g.StopLoss.ID
		}
	}
	if g.StopLoss != nil && g.StopLoss.ID == legID {
		g.StopLoss.Active = false
		if g.TakeProfit != nil {
			g.TakeProfit.Active = false
			peer = g.TakeProfit.ID
		}
	}
	copy := *g
	return &copy, peer, nil
}

// Resize caps protection to remaining exposure; zero exposure removes it.
// On removal, the returned Group still carries the (now-stale) leg IDs so
// the caller can cancel them — only the ok flag signals the group is gone.
func (r *Registry) Resize(id string, exposure decimal.Decimal) (*Group, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[id]
	if !ok {
		return nil, false
	}
	if exposure.LessThanOrEqual(decimal.Zero) {
		delete(r.groups, id)
		copy := *g
		return &copy, false
	}
	if exposure.LessThan(g.ProtectedQty) {
		g.ProtectedQty = exposure
	}
	copy := *g
	return &copy, true
}

func (r *Registry) Get(id string) (*Group, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.groups[id]
	if !ok {
		return nil, false
	}
	copy := *g
	return &copy, true
}

// GroupsFor returns every active group for an account/symbol, for the
// resize listener to re-evaluate whenever that position's exposure changes.
func (r *Registry) GroupsFor(accountID, symbol string) []*Group {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*Group
	for _, g := range r.groups {
		if g.AccountID == accountID && g.Symbol == symbol && g.TriggeredLeg == "" {
			copy := *g
			out = append(out, &copy)
		}
	}
	return out
}

// RelinkLeg updates a group's leg order ID after a resize cancel-and-replace
// (the resubmitted leg gets a new order ID from the matching engine).
func (r *Registry) RelinkLeg(groupID, role, newOrderID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[groupID]
	if !ok {
		return
	}
	switch role {
	case "TP":
		if g.TakeProfit != nil {
			g.TakeProfit.ID = newOrderID
		}
	case "SL":
		if g.StopLoss != nil {
			g.StopLoss.ID = newOrderID
		}
	}
}

// ClosingSide returns the order side that reduces/closes a position opened
// with entrySide (BUY entry closes with a SELL, and vice versa). Exported
// (added 2026-09-16) so cmd/engine's SPOT reserveGroup callback can build a
// same-shaped order for the shared OCO reservation without duplicating this
// logic — see command.go's ReserveGroup doc comment.
func ClosingSide(entrySide models.OrderSide) models.OrderSide {
	if entrySide == models.Buy {
		return models.Sell
	}
	return models.Buy
}

// closingSide is BuildLegOrder's own unexported call site, kept as a
// same-package alias so this file's callers don't need the exported name.
func closingSide(entrySide models.OrderSide) models.OrderSide {
	return ClosingSide(entrySide)
}

// BuildLegOrder constructs the resting protective order for one leg of a
// group, sized to qty. TP submits as a reduce-only LIMIT at the leg's
// LimitPrice; SL submits as a reduce-only STOP (StopPrice trigger, no limit
// price - executes as a market order once triggered, consistent with how
// the engine already treats stop-market orders for reservation/estimation).
// Both legs carry GroupID/GroupRole so fills route back through the OCO
// listener, and InternalLiquidation is left false so ordinary risk/config
// validation still applies to the leg itself.
//
// Market is taken from the group (added 2026-09-16 for SPOT TP/SL support;
// previously hardcoded to models.Futures here, since TP/SL was futures-only
// until then). ReduceOnly is set for both markets, but means something
// different for each: for FUTURES it exempts the leg from a fresh margin
// reservation entirely (risk.Checker's ReduceOnly&&Futures short-circuit —
// the position's already-posted margin backs it). SPOT has no margin to
// trust that way, so a SPOT leg's reservation is instead handled by
// Execute's shared-reservation step below, and ReduceOnly&&GroupID!=""
// (Checker's SPOT OCO exemption) tells the risk checker to trust THAT
// shared lock rather than reserving this leg's own qty a second time —
// see Execute's doc comment for why a second, independent per-leg
// reservation would double-lock the same underlying holding.
func BuildLegOrder(g Group, leg *Leg, role string, qty decimal.Decimal) *models.Order {
	o := &models.Order{
		ID:          uuid.NewString(),
		AccountID:   g.AccountID,
		Symbol:      g.Symbol,
		Market:      g.Market,
		Side:        closingSide(g.EntrySide),
		Quantity:    qty,
		TimeInForce: models.GTC,
		Status:      models.StatusPending,
		CreatedAt:   time.Now(),
		ReduceOnly:  true,
		GroupID:     g.ID,
		GroupRole:   role,
	}
	switch role {
	case "TP":
		o.Type = models.Limit
		o.Price = leg.LimitPrice
	case "SL":
		o.Type = models.Stop
		o.StopPrice = leg.StopPrice
		// Price left zero: a stop order with no limit price activates as a
		// market order when triggered (see orderbook.Book stop handling).
	}
	return o
}
