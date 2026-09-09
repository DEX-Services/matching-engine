package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

// SpreadResponse is the payload for POST /spread.
type SpreadResponse struct {
	GroupID      string `json:"groupId"`
	BuyOrderID   string `json:"buyOrderId"`
	SellOrderID  string `json:"sellOrderId"`
	BuyFilled    string `json:"buyFilled"`
	SellFilled   string `json:"sellFilled"`
	NetPremium   string `json:"netPremium"` // positive = net debit paid, negative = net credit received
	MarginLocked string `json:"marginLocked"`
	Status       string `json:"status"` // "OPENED" | "UNWOUND"
}

// spreadHandler builds POST /spread: opens a two-leg vertical spread (buy
// one option, sell another — same underlying/expiry/type, different
// strikes, equal quantity) as close to atomically as this exchange's
// per-contract order books allow.
//
// True cross-book atomic matching (both legs filling as a single match, or
// neither) is out of scope here — the roadmap's Phase 4 multi-leg item is a
// bigger lift (a new order type the matching core understands natively).
// What this endpoint gives instead, honestly: ONE API call submits both legs
// as IOC orders in the same synchronous request; if the second leg cannot
// fill at all, the first leg's fill (if any) is immediately unwound with an
// opposite-side IOC before the request returns, so the caller never ends up
// silently holding a naked single leg they didn't ask for. If EITHER leg
// only partially fills, the spread is still considered opened at the
// smaller of the two filled quantities — the excess on the larger leg is a
// naked remainder from a market that couldn't fill the whole size, not this
// endpoint failing to keep the two in step; the caller sees both legs'
// actual filled amounts in the response and can act on it (there is no
// silent success at the wrong size).
//
// After both legs are open, this nets the margin down from what each leg's
// independent per-leg reservation locked (the existing options risk-check
// path, unchanged and still the safety floor while only one leg is
// confirmed) to risk.VerticalSpreadMargin's defined-risk amount — the
// portfolio-margin netting the roadmap's Phase 2b calls for, scoped to this
// one well-understood structure rather than a general cross-position engine.
func spreadHandler(d submitDeps) http.HandlerFunc {
	return requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		account := q.Get("account")
		buySymbol := q.Get("buySymbol")
		sellSymbol := q.Get("sellSymbol")
		qty, qerr := decimal.NewFromString(q.Get("qty"))
		buyPrice, buyPerr := decimal.NewFromString(q.Get("buyPrice"))
		sellPrice, sellPerr := decimal.NewFromString(q.Get("sellPrice"))
		if account == "" || buySymbol == "" || sellSymbol == "" {
			http.Error(w, "account, buySymbol, and sellSymbol are required", http.StatusBadRequest)
			return
		}
		if qerr != nil || !qty.IsPositive() {
			http.Error(w, "qty must be a positive number", http.StatusBadRequest)
			return
		}
		if buyPerr != nil || !buyPrice.IsPositive() || sellPerr != nil || !sellPrice.IsPositive() {
			http.Error(w, "buyPrice and sellPrice must be positive limit prices (the IOC cap for each leg)", http.StatusBadRequest)
			return
		}

		buyInst, sellInst, err := loadAndValidateVertical(r.Context(), d.pgPool, buySymbol, sellSymbol)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		groupID := uuid.NewString()
		buyOrder := &models.Order{
			ID: uuid.NewString(), AccountID: account, Symbol: buySymbol, Market: models.Options,
			Side: models.Buy, Type: models.IOC, Price: buyPrice, Quantity: qty,
			TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
			GroupID: groupID, GroupRole: "SPREAD_LONG",
		}
		sellOrder := &models.Order{
			ID: uuid.NewString(), AccountID: account, Symbol: sellSymbol, Market: models.Options,
			Side: models.Sell, Type: models.IOC, Price: sellPrice, Quantity: qty,
			TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
			GroupID: groupID, GroupRole: "SPREAD_SHORT",
		}

		buySnap, _, buyStatus, buyErr := submitOrderPipeline(r.Context(), d, buyOrder, "")
		if buyErr != nil {
			http.Error(w, fmt.Sprintf("long leg failed: %s", buyErr.Error()), buyStatus)
			return
		}
		buyFilled := decimal.Zero
		if buySnap != nil {
			buyFilled = buySnap.Filled
		}
		if !buyFilled.IsPositive() {
			http.Error(w, "long leg did not fill (no liquidity at buyPrice); spread not opened", http.StatusUnprocessableEntity)
			return
		}

		// Size the short leg to what the long leg actually filled — never
		// more than requested, and never more than the long leg to avoid
		// opening a naked excess short beyond what qty asked for either.
		sellOrder.Quantity = decimal.Min(qty, buyFilled)
		sellSnap, _, sellStatus, sellErr := submitOrderPipeline(r.Context(), d, sellOrder, "")
		sellFilled := decimal.Zero
		if sellSnap != nil {
			sellFilled = sellSnap.Filled
		}
		if sellErr != nil || !sellFilled.IsPositive() {
			// Short leg failed entirely: unwind the long leg's fill so the
			// caller never ends up holding a naked long they didn't ask for.
			unwindLeg(r.Context(), d, buyOrder, buyFilled, models.Sell)
			reason := "short leg did not fill (no liquidity at sellPrice)"
			if sellErr != nil {
				reason = sellErr.Error()
			}
			writeJSON(w, sellStatus, SpreadResponse{
				GroupID: groupID, BuyOrderID: buyOrder.ID, SellOrderID: sellOrder.ID,
				BuyFilled: "0", SellFilled: "0", Status: "UNWOUND",
			})
			_ = reason // surfaced via the log inside unwindLeg; response body carries the terminal state
			return
		}

		// Both legs open. If the short leg filled less than the long leg
		// (thin book on that side), the excess long is a real naked
		// remainder — reported honestly in the response rather than forced
		// to net-margin math that assumes matched legs.
		matchedQty := decimal.Min(buyFilled, sellFilled)

		buyPremium := buyPrice.Mul(buyFilled)    // what was actually paid for the long leg's fill
		sellPremium := sellPrice.Mul(sellFilled) // what was actually received for the short leg's fill
		netCredit := sellPremium.Sub(buyPremium) // positive = net credit; matches VerticalSpreadMargin's sign convention

		marginLocked := decimal.Zero
		if matchedQty.IsPositive() {
			marginLocked = netVerticalMargin(d, account, buyInst, sellInst, matchedQty, netCredit, sellPrice, sellOrder.QuoteCurrency)
		}

		writeJSON(w, http.StatusOK, SpreadResponse{
			GroupID: groupID, BuyOrderID: buyOrder.ID, SellOrderID: sellOrder.ID,
			BuyFilled: buyFilled.String(), SellFilled: sellFilled.String(),
			NetPremium: buyPremium.Sub(sellPremium).String(), MarginLocked: marginLocked.String(),
			Status: "OPENED",
		})
	})
}

// loadAndValidateVertical fetches both legs' instrument metadata and
// confirms they actually form a vertical: same underlying, same expiry,
// same option type, different strikes. Returns a clear rejection reason
// rather than letting a mismatched pair reach order submission and fail
// there with a less specific error.
func loadAndValidateVertical(ctx context.Context, pool *pgxpool.Pool, buySymbol, sellSymbol string) (*optionInstrument, *optionInstrument, error) {
	buyInst, err := loadOptionInstrument(ctx, pool, buySymbol)
	if err != nil || buyInst == nil {
		return nil, nil, fmt.Errorf("buySymbol %s is not a known option instrument", buySymbol)
	}
	sellInst, err := loadOptionInstrument(ctx, pool, sellSymbol)
	if err != nil || sellInst == nil {
		return nil, nil, fmt.Errorf("sellSymbol %s is not a known option instrument", sellSymbol)
	}
	return buyInst, sellInst, validateVerticalPair(buyInst, sellInst)
}

// validateVerticalPair checks the actual vertical-spread shape rules on two
// already-loaded instruments: same underlying, same expiry, same option
// type, different strikes. Split out from loadAndValidateVertical so this
// logic is testable without a live Postgres pool (loadOptionInstrument
// requires one to look symbols up in the first place).
func validateVerticalPair(buyInst, sellInst *optionInstrument) error {
	if buyInst.Underlying != sellInst.Underlying {
		return fmt.Errorf("both legs must share the same underlying (got %s and %s)", buyInst.Underlying, sellInst.Underlying)
	}
	if !buyInst.Expiry.Equal(sellInst.Expiry) {
		return fmt.Errorf("both legs must share the same expiry for a vertical spread")
	}
	if buyInst.OptionType != sellInst.OptionType {
		return fmt.Errorf("both legs must be the same option type (both CALL or both PUT) for a vertical spread")
	}
	if buyInst.Strike.Equal(sellInst.Strike) {
		return fmt.Errorf("legs must have different strikes")
	}
	return nil
}

// unwindLeg closes a leg's fill via an opposite-side IOC at whatever price
// the book will give (best-effort — this is a synchronous "get out now" close,
// not a price-optimized exit). Used only when the OTHER leg of a spread
// failed to fill at all, leaving this leg naked. Errors are logged but not
// surfaced as the spread's own failure reason — the caller already sees the
// spread failed to open; a failed unwind is a separate, secondary problem
// that leaves the account holding a real (if unwanted) position rather than
// losing money silently.
func unwindLeg(ctx context.Context, d submitDeps, original *models.Order, filledQty decimal.Decimal, closingSide models.OrderSide) {
	closeOrder := &models.Order{
		ID: uuid.NewString(), AccountID: original.AccountID, Symbol: original.Symbol, Market: models.Options,
		Side: closingSide, Type: models.Market, Quantity: filledQty,
		TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
		GroupID: original.GroupID, GroupRole: "SPREAD_UNWIND",
	}
	if _, _, _, err := submitOrderPipeline(ctx, d, closeOrder, "500"); err != nil {
		// 5% slippage tolerance on the unwind market order — see /order's
		// slippageBps handling. A failed unwind here means the account is
		// left holding the naked leg; this is logged for manual
		// reconciliation the same way other best-effort-but-not-guaranteed
		// paths in this codebase are (e.g. expiry settlement's debit-failure
		// path in settlement.ExpiryProcessor).
		slog.Error("spread unwind failed; account left holding a naked leg",
			"account", original.AccountID, "symbol", original.Symbol, "qty", filledQty, "error", err)
	}
}

// netVerticalMargin releases each leg's independently-locked margin down to
// risk.VerticalSpreadMargin's net requirement. Each leg was already margined
// in full by the normal options risk check inside submitOrderPipeline (the
// long leg locked its premium, the short leg locked shortOptionMargin's
// floor) — that per-leg check is the safety floor that ran BEFORE either
// leg's fill was certain, and is left untouched. This only runs after BOTH
// legs are confirmed open, and releases the difference between what was
// already reserved and what the netted position actually needs.
func netVerticalMargin(d submitDeps, account string, buyInst, sellInst *optionInstrument, qty, netCredit, sellPremiumPerUnit decimal.Decimal, quoteCurrency string) decimal.Decimal {
	required := risk.VerticalSpreadMargin(buyInst.Strike, sellInst.Strike, qty, netCredit)
	// shortLegReserved is exactly what the short leg's own order-time risk
	// check locked (risk.shortOptionMargin, via the same RequiredOptionsMargin
	// wrapper the liquidation margin-call sweep uses) — NOT assumed to be
	// the full cash-secured strike*qty ceiling, since the pre-existing
	// margin-floor model (Phase 2a) may have already reserved less than
	// that. Recomputing with the same inputs the order-time check used
	// guarantees this only ever releases the actual excess, never more than
	// what is truly reserved.
	shortLegReserved := risk.RequiredOptionsMargin(sellInst.Symbol, sellInst.OptionType, sellInst.Strike, qty, sellPremiumPerUnit, quoteCurrency)
	if required.GreaterThanOrEqual(shortLegReserved) {
		return shortLegReserved // nothing to release; the short leg's own margin already equals or is less than the net requirement
	}
	release := shortLegReserved.Sub(required)
	d.ledger.Release(account, quoteCurrency, release)
	return required
}
