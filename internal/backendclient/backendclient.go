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
	"log/slog"
	"net/http"
	"os"
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
		http:    &http.Client{Timeout: 5 * time.Second},
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

// Settle calls POST /internal/balance/settle, converting a Postgres lock into
// a real debit when a fill settles.
func (c *Client) Settle(ctx context.Context, userID, asset, amount string) error {
	return c.call(ctx, "/internal/balance/settle", userID, asset, amount)
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
		return fmt.Errorf("backendclient %s: status %d", path, resp.StatusCode)
	}
	return nil
}

// Async runs fn in a goroutine with a fresh timeout context, logging failures
// instead of propagating them. Use for Unlock/Settle calls that must never
// block the matching goroutine and whose failure shouldn't undo work already
// committed to the in-memory ledger.
func Async(op string, fn func(ctx context.Context) error) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := fn(ctx); err != nil {
			slog.Error("backendclient async call failed", "op", op, "error", err)
		}
	}()
}
