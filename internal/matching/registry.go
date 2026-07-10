package matching

import (
	"fmt"
	"sync"

	"github.com/dex/matching-engine/internal/models"
)

// SymbolKey uniquely identifies a symbol + market combination.
type SymbolKey struct {
	Symbol string
	Market models.MarketType
}

// SettlementFactory produces a SettlementHandler for a given symbol/market.
// Pass nil to use NoopSettlement for all symbols.
type SettlementFactory func(symbol string, market models.MarketType) SettlementHandler

// Registry manages a collection of matching engines, one per SymbolKey.
// Onboarding a new trading pair is a runtime operation — no code change required.
type Registry struct {
	mu      sync.RWMutex
	engines map[SymbolKey]*Engine

	pub     EventPublisher
	factory SettlementFactory
}

// NewRegistry creates a Registry.
func NewRegistry(pub EventPublisher, factory SettlementFactory) *Registry {
	return &Registry{
		engines: make(map[SymbolKey]*Engine),
		pub:     pub,
		factory: factory,
	}
}

// Register creates an engine for the given symbol/market.
// Returns an error if the symbol is already registered.
func (r *Registry) Register(symbol string, market models.MarketType) (*Engine, error) {
	key := SymbolKey{symbol, market}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.engines[key]; exists {
		return nil, fmt.Errorf("engine already registered for %s/%s", symbol, market)
	}
	eng := r.newEngine(symbol, market)
	r.engines[key] = eng
	return eng, nil
}

// MustRegister is like Register but panics on error. Useful in startup code.
func (r *Registry) MustRegister(symbol string, market models.MarketType) *Engine {
	eng, err := r.Register(symbol, market)
	if err != nil {
		panic(err)
	}
	return eng
}

// Get returns the engine for the given symbol/market, or an error if not found.
func (r *Registry) Get(symbol string, market models.MarketType) (*Engine, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	eng, ok := r.engines[SymbolKey{symbol, market}]
	if !ok {
		return nil, fmt.Errorf("no engine registered for %s/%s", symbol, market)
	}
	return eng, nil
}

// GetOrCreate returns the existing engine or creates a new one.
func (r *Registry) GetOrCreate(symbol string, market models.MarketType) *Engine {
	key := SymbolKey{symbol, market}
	r.mu.Lock()
	defer r.mu.Unlock()
	if eng, ok := r.engines[key]; ok {
		return eng
	}
	eng := r.newEngine(symbol, market)
	r.engines[key] = eng
	return eng
}

// Submit routes an order to the correct engine. Returns ErrNoEngine if the
// symbol is not registered.
func (r *Registry) Submit(order *models.Order) ([]*models.Trade, error) {
	eng, err := r.Get(order.Symbol, order.Market)
	if err != nil {
		return nil, err
	}
	return eng.Submit(order)
}

// Cancel routes a cancel request to the correct engine.
func (r *Registry) Cancel(symbol string, market models.MarketType, orderID string) (*models.Order, error) {
	eng, err := r.Get(symbol, market)
	if err != nil {
		return nil, err
	}
	return eng.Cancel(orderID)
}

// Symbols returns all registered SymbolKeys.
func (r *Registry) Symbols() []SymbolKey {
	r.mu.RLock()
	defer r.mu.RUnlock()
	keys := make([]SymbolKey, 0, len(r.engines))
	for k := range r.engines {
		keys = append(keys, k)
	}
	return keys
}

// StopAll shuts down all engines and waits for them to drain.
func (r *Registry) StopAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, eng := range r.engines {
		eng.Stop()
	}
}

func (r *Registry) newEngine(symbol string, market models.MarketType) *Engine {
	var sh SettlementHandler
	if r.factory != nil {
		sh = r.factory(symbol, market)
	}
	return NewEngine(symbol, market, r.pub, sh)
}
