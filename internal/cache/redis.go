// Package cache manages the Redis connection and order book snapshots.
// Redis is NOT the source of truth for any data. It is used for:
//   - Fast-restart book snapshots (avoids full Kafka replay)
//   - Session tokens and rate-limit counters (Phase 7)
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

const snapshotTTL = 24 * time.Hour

// Client wraps a redis.Client with helpers for book snapshots.
type Client struct {
	rdb *redis.Client
	log *slog.Logger
}

// NewClient connects to Aiven Redis using REDIS_SERVICE_URI (rediss:// = TLS).
func NewClient(ctx context.Context) (*Client, error) {
	uri := os.Getenv("REDIS_SERVICE_URI")
	if uri == "" {
		return nil, fmt.Errorf("REDIS_SERVICE_URI is not set")
	}
	opts, err := redis.ParseURL(uri)
	if err != nil {
		return nil, fmt.Errorf("parse redis URI: %w", err)
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	slog.Info("redis connected", "host", os.Getenv("REDIS_HOST"))
	return &Client{rdb: rdb, log: slog.Default()}, nil
}

// BookSnapshot holds the resting orders of one side of the book.
type BookSnapshot struct {
	Symbol    string           `json:"symbol"`
	Market    models.MarketType `json:"market"`
	Bids      []SnapOrder      `json:"bids"`
	Asks      []SnapOrder      `json:"asks"`
	CreatedAt time.Time        `json:"created_at"`
}

// SnapOrder is a compact representation of a resting order.
type SnapOrder struct {
	ID        string          `json:"id"`
	AccountID string          `json:"account_id"`
	Price     decimal.Decimal `json:"price"`
	Quantity  decimal.Decimal `json:"quantity"`
	Filled    decimal.Decimal `json:"filled"`
	CreatedAt time.Time       `json:"created_at"`
}

// SaveSnapshot serialises and stores a book snapshot in Redis.
func (c *Client) SaveSnapshot(ctx context.Context, snap *BookSnapshot) error {
	key := snapshotKey(snap.Symbol, string(snap.Market))
	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	return c.rdb.Set(ctx, key, data, snapshotTTL).Err()
}

// LoadSnapshot retrieves the latest snapshot for a symbol/market, or returns
// (nil, nil) if no snapshot exists.
func (c *Client) LoadSnapshot(ctx context.Context, symbol string, market models.MarketType) (*BookSnapshot, error) {
	key := snapshotKey(symbol, string(market))
	data, err := c.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil // no snapshot — fall back to Kafka replay
	}
	if err != nil {
		return nil, fmt.Errorf("redis get snapshot: %w", err)
	}
	var snap BookSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	return &snap, nil
}

// DeleteSnapshot removes a stale snapshot (e.g., after a clean shutdown).
func (c *Client) DeleteSnapshot(ctx context.Context, symbol string, market models.MarketType) error {
	return c.rdb.Del(ctx, snapshotKey(symbol, string(market))).Err()
}

// Close closes the Redis connection.
func (c *Client) Close() error {
	return c.rdb.Close()
}

func snapshotKey(symbol, market string) string {
	return fmt.Sprintf("book:snapshot:%s:%s", symbol, market)
}
