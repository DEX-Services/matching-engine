// Package risk implements the in-memory balance ledger that is the authoritative
// source of truth for account balances during the lifetime of the engine process.
// Postgres is the asynchronous durable log — not the source of truth.
package risk

import (
	"fmt"
	"sync"

	"github.com/shopspring/decimal"
)

// Ledger is the authoritative, in-memory balance store.
//
// Concurrency model:
//   - Settlement handlers (called from the matching goroutine) are the sole writers.
//   - The RiskChecker reads with an RLock for pre-trade checks.
//   - This matches the spec: ledger writes happen on the matching goroutine; reads
//     are fast and use a shared read lock.
type Ledger struct {
	mu       sync.RWMutex
	balances map[string]map[string]decimal.Decimal // accountID → asset → total balance
	reserved map[string]map[string]decimal.Decimal // accountID → asset → soft-reserved amount
}

// NewLedger creates an empty ledger.
func NewLedger() *Ledger {
	return &Ledger{
		balances: make(map[string]map[string]decimal.Decimal),
		reserved: make(map[string]map[string]decimal.Decimal),
	}
}

// Deposit credits amount of asset to accountID. Used for funding and tests.
func (l *Ledger) Deposit(accountID, asset string, amount decimal.Decimal) error {
	if !amount.IsPositive() {
		return fmt.Errorf("deposit amount must be positive, got %s", amount)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ensure(accountID)
	l.balances[accountID][asset] = l.balances[accountID][asset].Add(amount)
	return nil
}

// Available returns the free (unreserved) balance.
func (l *Ledger) Available(accountID, asset string) decimal.Decimal {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.available(accountID, asset)
}

// Balance returns the total balance (including reserved).
func (l *Ledger) Balance(accountID, asset string) decimal.Decimal {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.balances[accountID][asset]
}

// Reserved returns the amount currently soft-locked for open orders.
func (l *Ledger) Reserved(accountID, asset string) decimal.Decimal {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.reserved[accountID][asset]
}

// Reserve soft-locks amount for an open order (pre-trade hold).
// Returns an error when the available balance is insufficient.
func (l *Ledger) Reserve(accountID, asset string, amount decimal.Decimal) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ensure(accountID)
	if l.available(accountID, asset).LessThan(amount) {
		return fmt.Errorf("insufficient %s for %s: available=%s required=%s",
			asset, accountID, l.available(accountID, asset), amount)
	}
	l.reserved[accountID][asset] = l.reserved[accountID][asset].Add(amount)
	return nil
}

// Release frees a previously reserved amount (on cancel or rejection).
func (l *Ledger) Release(accountID, asset string, amount decimal.Decimal) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cur := l.reserved[accountID][asset]
	l.reserved[accountID][asset] = decimal.Max(decimal.Zero, cur.Sub(amount))
}

// Debit removes amount from accountID's balance and releases the same reservation.
// Called synchronously by settlement handlers after a fill.
func (l *Ledger) Debit(accountID, asset string, amount decimal.Decimal) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	bal := l.balances[accountID][asset]
	if bal.LessThan(amount) {
		return fmt.Errorf("debit exceeds balance: account=%s asset=%s balance=%s amount=%s",
			accountID, asset, bal, amount)
	}
	l.balances[accountID][asset] = bal.Sub(amount)
	// Release reservation up to the debited amount.
	res := l.reserved[accountID][asset]
	l.reserved[accountID][asset] = decimal.Max(decimal.Zero, res.Sub(amount))
	return nil
}

// Credit adds amount to accountID's balance.
// Called synchronously by settlement handlers after a fill.
func (l *Ledger) Credit(accountID, asset string, amount decimal.Decimal) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ensure(accountID)
	l.balances[accountID][asset] = l.balances[accountID][asset].Add(amount)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func (l *Ledger) available(accountID, asset string) decimal.Decimal {
	bal := l.balances[accountID][asset]
	res := l.reserved[accountID][asset]
	v := bal.Sub(res)
	if v.IsNegative() {
		return decimal.Zero
	}
	return v
}

func (l *Ledger) ensure(accountID string) {
	if l.balances[accountID] == nil {
		l.balances[accountID] = make(map[string]decimal.Decimal)
		l.reserved[accountID] = make(map[string]decimal.Decimal)
	}
}
