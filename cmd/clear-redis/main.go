// Command clear-redis is a DEV-ONLY tool that flushes every key from the
// platform's Redis instance (FLUSHALL).
//
// Safe by design, not just by convention: per internal/cache/redis.go's own
// package doc comment, "Redis is NOT the source of truth for any data" —
// it only holds order-book fast-restart snapshots and (per that same
// comment) session tokens / rate-limit counters. Nothing here is
// irreplaceable: order books rebuild from Kafka replay on next use,
// sessions simply require signing in again, and rate-limit counters reset
// to zero (briefly more permissive, never less safe).
//
// Usage:
//
//	go run ./cmd/clear-redis          # dry run: reports the current key count
//	go run ./cmd/clear-redis --apply  # actually flushes everything
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

func main() {
	_ = godotenv.Load()
	apply := len(os.Args) > 1 && os.Args[1] == "--apply"

	uri := os.Getenv("REDIS_SERVICE_URI")
	if uri == "" {
		fmt.Println("REDIS_SERVICE_URI is not set")
		os.Exit(1)
	}
	opts, err := redis.ParseURL(uri)
	if err != nil {
		fmt.Println("parse redis URI error:", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(opts)
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Println("redis ping error:", err)
		os.Exit(1)
	}

	count, err := rdb.DBSize(ctx).Result()
	if err != nil {
		fmt.Println("dbsize error:", err)
		os.Exit(1)
	}
	fmt.Printf("Current key count: %d\n", count)

	if !apply {
		fmt.Println("\nDry run only — nothing was touched.")
		fmt.Println("Re-run with --apply to FLUSHALL (delete every key).")
		return
	}

	if err := rdb.FlushAll(ctx).Err(); err != nil {
		fmt.Println("flushall error:", err)
		os.Exit(1)
	}
	fmt.Println("\nFlushed. Every key is gone — order-book snapshots rebuild from Kafka replay on next use.")
}
