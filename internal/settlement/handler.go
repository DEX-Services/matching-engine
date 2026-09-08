// Package settlement contains asset-class-specific post-trade logic.
//
// Design rule from the spec (Section 4.3):
//   - The matching loop is identical for Spot, Futures, and Options.
//   - Settlement is invoked AFTER a trade is generated, outside the matching loop.
//   - Futures-only concerns (margin, funding, liquidation) live in FuturesSettlement.
//   - Options-only concerns (strike, expiry, exercise) live in OptionsSettlement.
//   - Core Order/Trade structs carry NONE of these fields.
package settlement

import "github.com/dex/matching-engine/internal/models"

// Handler is invoked by the matching engine goroutine synchronously after each
// trade, before the trade event is published. This guarantees the ledger is
// consistent before any downstream consumer sees the trade.
//
// The trade carries transient BuyOrder and SellOrder references (json:"-") that
// provide the account IDs and quantities needed for settlement without requiring
// a database lookup on the hot path.
type Handler interface {
	Settle(trade *models.Trade) error
}

// Noop is a no-op settlement handler. Used in tests and as the Phase 2 default.
type Noop struct{}

func (Noop) Settle(_ *models.Trade) error { return nil }
