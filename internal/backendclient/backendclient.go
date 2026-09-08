// Package backendclient calls Dex-Backend's /internal/balance/* endpoints so
// that the engine's in-memory risk.Ledger holds/settlements are mirrored into
// the real Postgres user_balances table. The in-memory ledger stays
// authoritative for trading; this client is a best-effort mirror for the
// account-freeze display, consistent with risk.Ledger's doc comment
// ("Postgres is the asynchronous durable log — not the source of truth").
package backendclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// RawUnitScale is the number of decimals Dex-Backend's Postgres user_balances
// columns use for raw fixed-point integer storage (matches USDC's 6 on-chain
// decimals, e.g. 40000000 = $40). Callers must convert decimal dollar
// notionals to this raw integer scale before calling Lock/Unlock/Settle —
// Dex-Backend rejects non-integer amount strings.
const RawUnitScale = 6

// ToRawUnits converts a decimal dollar amount (e.g. engine risk notionals) to
// the raw integer string Dex-Backend expects.
func ToRawUnits(amount decimal.Decimal) string {
	return amount.Shift(RawUnitScale).Truncate(0).String()
}

// Client calls Dex-Backend's internal balance-lock endpoints. A nil/zero-value
// Client (created when DEX_BACKEND_URL or DEX_BACKEND_ENGINE_SECRET is unset)
// no-ops every call so the engine runs unaffected when the bridge is disabled.
type Client struct {
	baseURL string
	secret  string
	http    *http.Client
}

// New builds a Client from DEX_BACKEND_URL / DEX_BACKEND_ENGINE_SECRET env
// vars. If either is unset, the returned Client is disabled: every method
// becomes a no-op that returns nil, and a warning is logged once.
func New() *Client {
	base := os.Getenv("DEX_BACKEND_URL")
	secret := os.Getenv("DEX_BACKEND_ENGINE_SECRET")
	if base == "" || secret == "" {
		slog.Warn("DEX_BACKEND_URL or DEX_BACKEND_ENGINE_SECRET not set, Postgres balance-lock bridge disabled")
		return &Client{}
	}
	return &Client{
		baseURL: base,
		secret:  secret,
		// A lock/unlock touches a remote Postgres instance. Five seconds was too
		// short during a full market-maker ladder cancellation and left durable
		// locks behind, starving the next quote cycle.
		http: &http.Client{Timeout: 20 * time.Second},
	}
}

// Enabled reports whether this client will actually call Dex-Backend.
func (c *Client) Enabled() bool {
	return c != nil && c.baseURL != ""
}

type balanceReq struct {
	UserID string `json:"userId"`
	Asset  string `json:"asset"`
	Amount string `json:"amount"`
}

type replaceLocksReq struct {
	UserID string            `json:"userId"`
	Locks  map[string]string `json:"locks"`
}

type spotSettleReq struct {
	BuyerID           string `json:"buyerId"`
	SellerID          string `json:"sellerId"`
	Base              string `json:"base"`
	Quote             string `json:"quote"`
	BaseQuantity      string `json:"baseQuantity"`
	BuyerQuoteDebit   string `json:"buyerQuoteDebit"`
	SellerQuoteCredit string `json:"sellerQuoteCredit"`
}

// Lock calls POST /internal/balance/lock. Returns an error if the backend
// rejects the lock (e.g. insufficient real funds) or is unreachable.
func (c *Client) Lock(ctx context.Context, userID, asset, amount string) error {
	return c.call(ctx, "/internal/balance/lock", userID, asset, amount)
}

// Unlock calls POST /internal/balance/unlock. Best-effort: callers should log
// failures but generally should not fail the in-memory release over it.
func (c *Client) Unlock(ctx context.Context, userID, asset, amount string) error {
	return c.call(ctx, "/internal/balance/unlock", userID, asset, amount)
}

// ReplaceLocks atomically sets durable reservations for a dedicated
// market-maker account.
func (c *Client) ReplaceLocks(ctx context.Context, userID string, locks map[string]string) error {
	if !c.Enabled() {
		return nil
	}
	body, err := json.Marshal(replaceLocksReq{UserID: userID, Locks: locks})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/balance/replace-locks", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Engine-Secret", c.secret)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("backendclient /internal/balance/replace-locks: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusMultipleChoices {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("backendclient /internal/balance/replace-locks: status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

// Settle calls POST /internal/balance/settle, converting a Postgres lock into
// a real debit when a fill settles.
func (c *Client) Settle(ctx context.Context, userID, asset, amount string) error {
	return c.call(ctx, "/internal/balance/settle", userID, asset, amount)
}

// Credit calls POST /internal/balance/credit, realizing released margin plus
// PnL into a user's real Postgres balance when a futures position closes.
// amount may be negative (net loss); Dex-Backend applies it as a debit.
func (c *Client) Credit(ctx context.Context, userID, asset, amount string) error {
	return c.call(ctx, "/internal/balance/credit", userID, asset, amount)
}

// SettleSpot atomically persists both legs of a completed spot trade.
func (c *Client) SettleSpot(ctx context.Context, buyerID, sellerID, base, quote, baseQuantity, buyerQuoteDebit, sellerQuoteCredit string) error {
	if !c.Enabled() {
		return nil
	}
	body, err := json.Marshal(spotSettleReq{buyerID, sellerID, base, quote, baseQuantity, buyerQuoteDebit, sellerQuoteCredit})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/balance/spot-settle", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Engine-Secret", c.secret)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("backendclient /internal/balance/spot-settle: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("backendclient /internal/balance/spot-settle: status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

// Backfill calls POST /internal/engine-backfill, asking Dex-Backend to push
// every nonzero Postgres balance into the engine's in-memory ledger. Used on
// engine startup to self-heal after a restart wipes the in-memory ledger.
// No-ops (returns zero values, nil error) when the client is disabled.
func (c *Client) Backfill(ctx context.Context) (synced, failed, total int, err error) {
	if !c.Enabled() {
		return 0, 0, 0, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/engine-backfill", nil)
	if err != nil {
		return 0, 0, 0, err
	}
	req.Header.Set("X-Engine-Secret", c.secret)

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("backendclient /internal/engine-backfill: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, 0, fmt.Errorf("backendclient /internal/engine-backfill: status %d", resp.StatusCode)
	}
	var result struct {
		Synced int `json:"synced"`
		Failed int `json:"failed"`
		Total  int `json:"total"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, 0, 0, fmt.Errorf("backendclient /internal/engine-backfill: decode response: %w", err)
	}
	return result.Synced, result.Failed, result.Total, nil
}

func (c *Client) call(ctx context.Context, path, userID, asset, amount string) error {
	if !c.Enabled() {
		return nil
	}
	body, err := json.Marshal(balanceReq{UserID: userID, Asset: asset, Amount: amount})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Engine-Secret", c.secret)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("backendclient %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("backendclient %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// asyncRetryAttempts and asyncRetryDelay bound how hard Async fights a
// transient failure (a network blip, the backend restarting) before giving
// up and only logging. Kept short: this runs off the matching goroutine, but
// an unbounded or slow retry loop still risks piling up if the backend stays
// down for a while.
const (
	asyncRetryAttempts = 3
	asyncRetryDelay    = 2 * time.Second
)

// Async runs fn in a goroutine with a fresh timeout context per attempt,
// retrying a handful of times before giving up and only logging. Use for
// Unlock/Settle calls that must never block the matching goroutine and whose
// failure shouldn't undo work already committed to the in-memory ledger.
//
// Retrying matters specifically for Unlock: the in-memory ledger release
// this always follows already ran synchronously and succeeded, so from the
// engine's own point of view the reservation is gone. If the durable
// Postgres unlock then fails just once (a transient network error, the
// backend mid-restart) with no retry, the account's real `locked` balance is
// stranded above what's actually reserved, permanently — every future
// available-balance check subtracts a hold that no longer exists anywhere
// but Postgres, degrading or zeroing that account's tradeable balance with
// no way to self-heal (the release already "happened" from the engine's
// perspective, so nothing re-attempts it). A few retries absorb exactly the
// kind of one-off blip that caused that in practice, without turning this
// into a full durable outbox.
func Async(op string, fn func(ctx context.Context) error) {
	go func() {
		var err error
		for attempt := 0; attempt < asyncRetryAttempts; attempt++ {
			if attempt > 0 {
				time.Sleep(asyncRetryDelay)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = fn(ctx)
			cancel()
			if err == nil {
				return
			}
			slog.Warn("backendclient async call failed, retrying", "op", op, "attempt", attempt+1, "error", err)
		}
		slog.Error("backendclient async call failed after retries", "op", op, "attempts", asyncRetryAttempts, "error", err)
	}()
}
