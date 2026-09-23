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

// SeqLookup returns the starting event-sequence number a newly constructed
// engine for symbol should resume from — see Engine.seq's doc comment for
// why this exists (without it, every engine restarts its sequence counter
// at 0, which collides with pre-restart (symbol, sequence_number) rows
// already in Postgres and silently drops that event's persisted history).
// Implementations should return the highest sequence_number already
// persisted for that symbol (0 if none), e.g. via
// `SELECT COALESCE(MAX(sequence_number), 0) FROM events WHERE symbol = $1`.
// May be nil, in which case every engine starts at 0 as before (this is the
// expected/correct behavior with Postgres disabled, e.g. local dev without
// POSTGRES_HOST set — there is no persisted history to resume from).
//
// Called from newEngine while Registry.mu's write lock is held (both
// Register at startup and GetOrCreate's lazy per-instrument creation for
// options/combos), so it should be a bounded, single fast query — not
// something that can block indefinitely — since it stalls every other
// registry operation on ANY symbol for its duration, not just this one.
type SeqLookup func(symbol string) uint64

// Registry manages a collection of matching engines, one per SymbolKey.
// Onboarding a new trading pair is a runtime operation — no code change required.
type Registry struct {
	mu      sync.RWMutex
	engines map[SymbolKey]*Engine

	pub       EventPublisher
	factory   SettlementFactory
	release   ReleaseFunc
	seqLookup SeqLookup

	// OnAutoHalt, if set, is attached to every engine this registry creates
	// (including lazily-created option/combo engines) so a self-halt (e.g.
	// settlement failure) gets recorded in the admin halt registry instead
	// of being invisible to it. Set directly after NewRegistry, before any
	// Register/GetOrCreate call.
	OnAutoHalt func(symbol, market, reason, note string)
}

// NewRegistry creates a Registry. release may be nil (defaults to a no-op),
// and is invoked for any resting maker order cancelled by self-trade
// prevention so its reserved funds are returned to the ledger. seqLookup may
// be nil (every engine then starts its sequence counter at 0, the prior
// behavior) — see SeqLookup's doc comment.
func NewRegistry(pub EventPublisher, factory SettlementFactory, release ReleaseFunc, seqLookup SeqLookup) *Registry {
	return &Registry{
		engines:   make(map[SymbolKey]*Engine),
		pub:       pub,
		factory:   factory,
		release:   release,
		seqLookup: seqLookup,
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

// SubmitSnapshot routes an order to the correct engine and also returns a
// race-safe copy of its post-submit state (see Engine.SubmitSnapshot).
func (r *Registry) SubmitSnapshot(order *models.Order) ([]*models.Trade, *models.Order, error) {
	eng, err := r.Get(order.Symbol, order.Market)
	if err != nil {
		return nil, nil, err
	}
	return eng.SubmitSnapshot(order)
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

// ReplaceAccountOrders atomically replaces all resting orders for one account
// in a symbol/market engine. See Engine.ReplaceAccountOrders for visibility
// guarantees.
func (r *Registry) ReplaceAccountOrders(symbol string, market models.MarketType, account string, orders []*models.Order) (removed, accepted []*models.Order, err error) {
	eng, err := r.Get(symbol, market)
	if err != nil {
		return nil, nil, err
	}
	return eng.ReplaceAccountOrders(account, orders)
}

// OrderByID looks up a resting order in a symbol's live book, returning whether
// it is currently resting. Terminal orders are no longer in the book.
func (r *Registry) OrderByID(symbol string, market models.MarketType, orderID string) (*models.Order, bool, error) {
	eng, err := r.Get(symbol, market)
	if err != nil {
		return nil, false, err
	}
	o, ok := eng.OrderByID(orderID)
	return o, ok, nil
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
	var startSeq uint64
	if r.seqLookup != nil {
		startSeq = r.seqLookup(symbol)
	}
	eng := NewEngine(symbol, market, r.pub, sh, r.release, startSeq)
	if r.OnAutoHalt != nil {
		eng.SetOnAutoHalt(func(reason, note string) {
			r.OnAutoHalt(symbol, string(market), reason, note)
		})
	}
	return eng
}
