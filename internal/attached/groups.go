// Package attached owns the lifecycle rules for fill-aware protective orders.
package attached

import (
	"fmt"
	"sync"

	"github.com/shopspring/decimal"
)

type Leg struct {
	ID         string
	StopPrice  decimal.Decimal
	LimitPrice decimal.Decimal
}

type Group struct {
	ID, AccountID, Symbol, ParentOrderID string
	ProtectedQty                         decimal.Decimal
	TakeProfit, StopLoss                 *Leg
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
	r.groups[g.ID] = &copy
	return nil
}

// Resize caps protection to remaining exposure; zero exposure removes it.
func (r *Registry) Resize(id string, exposure decimal.Decimal) (*Group, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[id]
	if !ok {
		return nil, false
	}
	if exposure.LessThanOrEqual(decimal.Zero) {
		delete(r.groups, id)
		return nil, false
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
