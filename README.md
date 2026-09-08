# DEX Matching Engine

A production-grade, modular, concurrent, in-memory matching engine and order book system written in Go. Built for a multi-asset exchange supporting **Crypto, Stocks, Forex, and Commodities**, each independently running **Spot, Futures, and Options** markets with price-time priority (FIFO) matching.

All 8 build phases are complete, with 21 tests passing and zero conservation violations under 100,000-order concurrent load.

---

## Table of Contents

- [Architecture at a Glance](#architecture-at-a-glance)
- [Core Design Rule](#core-design-rule)
- [Data Placement](#data-placement)
- [Project Structure](#project-structure)
- [Packages](#packages)
- [Order Flow](#order-flow)
- [Supported Order Types](#supported-order-types)
- [Build Phases](#build-phases)
- [Getting Started](#getting-started)
- [Running Tests](#running-tests)
- [Performance](#performance)
- [HTTP Endpoints](#http-endpoints)
- [Infrastructure](#infrastructure)
- [Design Decisions](#design-decisions)
- [Invariants](#invariants)
- [Dependencies](#dependencies)
- [Non-Goals](#non-goals)

---

## Architecture at a Glance

```
Order ──► RiskChecker ──► Registry ──► Engine goroutine (per symbol)
                                              │
                                    book.Submit() → []Trade
                                    settlement.Settle(trade)
                                    seq.Add(1)
                                    bus.Publish(event)
                                              │
                            ┌─────────────────┼─────────────────┐
                            ▼                 ▼                  ▼
                      KafkaPublisher       WSHub             (drop if full)
                            │               │ broadcast
                            ▼               ▼
                          Kafka         WebSocket clients
                            │
                    Postgres Writer (async, at-least-once)
```

---

## Core Design Rule

> **Anything read or written on the path from order-arrives to trade-executes must live in RAM and be owned by a single goroutine with no lock contention. Everything else — persistence, analytics, notifications, WebSocket fan-out — is asynchronous, cached, or durable-but-slow.**

---

## Data Placement

| Data | Location | Notes |
|---|---|---|
| Live order book | **RAM only** | Never touches disk/network on hot path |
| Order index (orderID → order) | **RAM**, per-engine | O(1) cancel; owned by matching goroutine |
| Account balances | **RAM ledger** (authoritative) | Synchronously updated on fill; Postgres is async log |
| Book snapshots | **Redis** | Fast-restart shortcut; not authoritative |
| Full event history | **Kafka → Postgres** | Kafka is source of truth; Postgres is queryable store |
| Session tokens, rate limits | **Redis** | Eventually consistent; fine to lose |
| Symbol configs | **RAM** at startup | Hot-reloadable; never a per-order network call |

---

## Project Structure

```
matching-engine/
├── .env                               # Aiven credentials (never commit publicly)
├── go.mod / go.sum
├── cmd/engine/main.go                 # Entry point: wires all phases, HTTP server, demo
├── internal/
│   ├── models/
│   │   ├── order.go                   # Order struct, enums (Side/Type/Status/MarketType)
│   │   ├── trade.go                   # Trade struct + transient BuyOrder/SellOrder refs
│   │   └── event.go                   # Event envelope with per-symbol sequence number
│   ├── orderbook/
│   │   ├── interface.go               # OrderBook interface (asset-class agnostic)
│   │   ├── price_level.go             # FIFO doubly-linked list per price point
│   │   └── book.go                    # Full single-symbol matching engine (Phase 1)
│   ├── matching/
│   │   ├── engine.go                  # Engine: goroutine-per-symbol, request channel, halt
│   │   └── registry.go                # Registry: symbol → engine map, dynamic onboarding
│   ├── risk/
│   │   ├── ledger.go                  # In-memory authoritative balance ledger
│   │   └── checker.go                 # Pre-trade checks + reserve/release
│   ├── events/
│   │   ├── bus.go                     # Non-blocking fan-out event bus
│   │   ├── publisher.go               # Kafka publisher (SASL/TLS for Aiven)
│   │   └── topics.go                  # Kafka topic constants
│   ├── ws/
│   │   ├── hub.go                     # WebSocket broadcast hub
│   │   └── client.go                  # Per-connection write pump with ping/pong
│   ├── persistence/
│   │   ├── postgres.go                # pgx pool, Migrate() DDL
│   │   └── writer.go                  # Kafka consumer → Postgres async writer
│   ├── cache/
│   │   └── redis.go                   # Redis client (TLS), book snapshot save/load
│   ├── settlement/
│   │   ├── handler.go                 # Handler interface + Noop
│   │   ├── spot.go                    # SpotSettlement: debit/credit base & quote
│   │   ├── futures.go                 # FuturesSettlement: margin, position tracking
│   │   └── options.go                 # OptionsSettlement: premium transfer, positions
│   ├── risk_admin/
│   │   └── halt.go                    # Symbol-wide halt/resume registry
│   ├── config/
│   │   └── symbol.go                  # SymbolConfig + hot-reloading Registry
│   └── marketdata/
│       └── service.go                 # Best bid/ask, spread, VWAP
└── test/
    ├── invariants/                    # 15 property-based invariant tests (Phase 1)
    ├── integration/                   # 4 concurrency + halt + sequence tests (Phase 2)
    └── stress/                        # 100k-order multi-symbol stress test (Phase 8)
```

---

## Packages

### `internal/models`
Shared structs used by all packages. No business logic.

- **Order** — `ID`, `AccountID`, `Symbol`, `Market`, `Side`, `Type`, `TimeInForce`, `Price`, `Quantity`, `Filled`, `Status`, `StopPrice`, `ReduceOnly`. Helpers: `RemainingQty()`, `IsTerminal()`, `IsBuy()`.
- **Trade** — `ID`, `Symbol`, `Market`, `MakerOrderID`, `TakerOrderID`, `MakerSide`, `Price`, `Quantity`, `ExecutedAt`, `SequenceNumber`. Transient: `BuyOrder`, `SellOrder` (`json:"-"`).
- **Event** — `Type`, `Symbol`, `Market`, `SequenceNumber`, `*Order`, `*Trade`.

### `internal/orderbook`
Single-symbol, single-threaded order book.

- **PriceLevel** — `container/list` (doubly-linked list) for O(1) FIFO insertion/removal. Index map for O(1) cancel by order ID.
- **Book** — separate `bids`/`asks` price maps with sorted price slices. `orderIndex` for O(1) lookup. `matchAggressively()` implements the core loop: peek best opposite → price check → fill min qty → emit trade.

### `internal/matching`
Goroutine-per-symbol concurrency wrapper.

- **Engine** — owns one `Book`, one `inputCh chan request`, and one `atomic.Uint64` sequence counter. The `run()` goroutine is the sole writer to the book and sequence counter. API: `Submit()`, `Cancel()`, `Modify()`, `Halt()`, `Resume()`, `BestBid()`, `BestAsk()`, `Depth()`.
- **Registry** — `map[SymbolKey]*Engine` protected by `sync.RWMutex`. `Register()`, `GetOrCreate()`, `Submit()` (routes to correct engine), `StopAll()`. New trading pairs are a runtime operation — no code change required.

### `internal/risk`
In-memory authoritative balance store.

- **Ledger** — `map[accountID]map[asset]decimal`. `Deposit()`, `Available()`, `Reserve()`, `Release()`, `Debit()`, `Credit()`. `sync.RWMutex`: settlement handlers write; risk checker reads.
- **Checker** — `Check()` reads available balance. `Reserve()` places soft hold. `Release()` frees on cancel/rejection. Symbol format `BASE-QUOTE` determines which asset to check.

### `internal/events`
Event fan-out and Kafka publishing.

- **Bus** — subscriber channels with configurable buffer depth. `Publish()` is non-blocking; slow subscribers drop events.
- **KafkaPublisher** — reads from Bus, serialises events as JSON, writes to `matching-engine.events` using `segmentio/kafka-go` with SASL/PLAIN + TLS for Aiven. Async mode.
- **Topics**: `matching-engine.events`, `matching-engine.trades`, `matching-engine.outbox`.

### `internal/ws`
WebSocket broadcast hub.

- **Hub** — reads events from `<-chan *models.Event` and broadcasts JSON to all connected clients non-blocking.
- **client** — per-connection write pump with 10s write deadline and 54s ping interval.

### `internal/persistence`
Asynchronous Postgres writer. Never on the hot path.

- **NewPool()** — pgx pool from `POSTGRES_SERVICE_URI`, max 20 connections.
- **Migrate()** — idempotent DDL for `orders`, `trades`, `events` tables with unique indexes on `(symbol, sequence_number)`.
- **Writer** — Kafka consumer (group `postgres-writer`). Offset committed only on success — at-least-once with idempotency keys.

### `internal/cache`
Redis client and book snapshots.

- **NewClient()** — `redis.ParseURL(REDIS_SERVICE_URI)` handles `rediss://` TLS automatically.
- **SaveSnapshot() / LoadSnapshot()** — JSON-serialised `BookSnapshot` at `book:snapshot:{symbol}:{market}` with 24h TTL.

### `internal/settlement`
Asset-class-specific post-trade logic. Called synchronously inside the matching goroutine after each trade, before the event is published.

- **SpotSettlement** — debit buyer's quote + credit buyer's base; debit seller's base + credit seller's quote.
- **FuturesSettlement** — debit initial margin from both sides (default 10× leverage); update `Position` records (VWAP entry price, size, margin).
- **OptionsSettlement** — transfer premium (price × qty) from buyer to seller; record `OptionsPosition`.

### `internal/risk_admin`
Symbol-wide circuit breakers. Separate code path from per-order rejection.

- **Registry** — `Halt(symbol, market, reason, note)` flips the engine's `atomic.Bool` via injected callback. `Resume()` re-enables it. Halt reasons: `MANUAL`, `CIRCUIT_BREAK`, `MAINTENANCE`.

### `internal/config`
Symbol configuration loaded into RAM at startup, hot-reloaded periodically.

- **SymbolConfig** — `TickSize`, `LotSize`, `MinNotional`, `MaxPrice`, `MakerFee`, `TakerFee`, `Active`.
- **Registry** — loads from `symbol_configs` Postgres table. `StartHotReload()` refreshes in background.

### `internal/marketdata`
Read-only views into the order books.

- **Service** — `Ticker()` returns best bid/ask, mid price, spread, depth. `VWAP()` computes volume-weighted average price for a hypothetical order of given size.

---

## Order Flow

```
1. POST /order
2. RiskChecker.Check()      — reads Available() from Ledger (RLock)
3. RiskChecker.Reserve()    — soft-locks required funds
4. Registry.Submit(order)   — routes to Engine.Submit()
5. Engine goroutine:
   a. book.Submit()         → []Trade
   b. settlement.Settle()   → Ledger.Debit() / Credit() (Lock)
   c. seq.Add(1)            → monotonic sequence number
   d. bus.Publish(event)    → non-blocking fan-out
6. Result returned to caller
7. Async:
   ├─ KafkaPublisher → Kafka (TopicEvents)
   └─ WSHub → WebSocket clients
8. Further async (Kafka consumers):
   └─ Postgres Writer → upserts orders/trades/events
```

---

## Supported Order Types

| Type | Behaviour |
|---|---|
| `LIMIT` | Match aggressively at or better than limit; rest unfilled remainder |
| `MARKET` | Match at any price; cancel unfilled remainder (never rests) |
| `IOC` | Match aggressively; cancel remainder immediately |
| `FOK` | Pre-check full fillability; cancel entirely if not fillable |
| `POST_ONLY` | Reject if would immediately cross; rest otherwise |
| `STOP` | Rested immediately; price-trigger activation at engine level |

**Time-in-Force**: `GTC`, `GTD`, `GFD`.

---

## Build Phases

| # | Phase | Key Deliverables | Status |
|---|---|---|---|
| 1 | Core Models & Order Book | `models`, `orderbook` — full matching logic, FIFO, all order types | ✅ Complete |
| 2 | Concurrency | `matching` — goroutine-per-symbol, symbol registry, halt flag | ✅ Complete |
| 3 | Risk Engine & Ledger | `risk` — authoritative ledger, pre-trade checks, reserve/release | ✅ Complete |
| 4 | Event Bus & Fan-out | `events`, `ws` — non-blocking bus, Kafka publisher, WebSocket hub | ✅ Complete |
| 5 | Persistence | `persistence`, `cache` — async Postgres writer, Redis snapshots, DDL | ✅ Complete |
| 6 | Settlement Handlers | `settlement` — Spot, Futures (margin/positions), Options (premium/positions) | ✅ Complete |
| 7 | Circuit Breakers & Observability | `risk_admin`, `config`, `marketdata` — halt/resume, hot-reload, VWAP | ✅ Complete |
| 8 | Stress Testing | `test/stress` — 100k orders, 20 symbols, 500 goroutines, 0 violations | ✅ Complete |

---

## Getting Started

### Prerequisites

| Tool | Version |
|---|---|
| Go | 1.22+ |
| Aiven Redis | See `.env` |
| Aiven PostgreSQL | See `.env` |
| Aiven Kafka | See `.env` |

### Install and Run

```bash
# Clone and install
cd matching-engine
GOTOOLCHAIN=local go mod download

# Run (loads .env automatically, starts HTTP server on :8080)
GOTOOLCHAIN=local go run ./cmd/engine/
```

### Environment Variables

The `.env` file is pre-configured with all Aiven credentials. Key variables:

```bash
# Redis (book snapshots, session tokens)
REDIS_SERVICE_URI=rediss://default:<password>@<host>:19334

# PostgreSQL (async event log)
POSTGRES_SERVICE_URI=postgres://avnadmin:<password>@<host>:19333/dexdb
POSTGRES_HOST=<host>
POSTGRES_PORT=19333
POSTGRES_DB=dexdb
POSTGRES_USER=avnadmin
POSTGRES_PASSWORD=<password>

# Kafka (primary event bus)
KAFKA_HOST=<host>
KAFKA_PORT=19346
KAFKA_USER=avnadmin
KAFKA_PASSWORD=<password>
```

> **Security:** Never commit `.env` to a public repository. Rotate credentials if exposed.

### Startup Output

```json
{"level":"INFO","msg":"matching engine starting","postgres_host":"pg-...","redis_host":"dex-redis-...","kafka_host":"dex-kafka-..."}
{"level":"INFO","msg":"engine registered","symbol":"BTC-USDT","market":"SPOT"}
{"level":"INFO","msg":"engine registered","symbol":"ETH-USDT","market":"SPOT"}
{"level":"INFO","msg":"engine registered","symbol":"BTC-USDT","market":"FUTURES"}
{"level":"INFO","msg":"postgres connected","host":"pg-..."}
{"level":"INFO","msg":"redis connected","host":"dex-redis-..."}
{"level":"INFO","msg":"kafka publisher started"}
{"level":"INFO","msg":"postgres writer started"}
{"level":"INFO","msg":"HTTP server listening","addr":":8080"}
```

---

## Running Tests

### All Tests

```bash
GOTOOLCHAIN=local go test ./...
# ok  github.com/dex/matching-engine/test/invariants   (15 tests)
# ok  github.com/dex/matching-engine/test/integration  (4 tests)
# ok  github.com/dex/matching-engine/test/stress       (2 tests)
```

### Invariant Tests (Phase 1) — 15 tests

```bash
GOTOOLCHAIN=local go test ./test/invariants/... -v
```

```
PASS: TestPriceTimePriority_SamePrice
PASS: TestPriceTimePriority_BetterPriceFirst
PASS: TestConservation_SingleTrade
PASS: TestConservation_MultiplePartialFills
PASS: TestLimitPriceNeverViolated_Buy
PASS: TestLimitPriceNeverViolated_Sell
PASS: TestBookNeverCrossed
PASS: TestOrderTerminalState_MarketOrderFullyFilled
PASS: TestOrderTerminalState_MarketOrderNoLiquidity
PASS: TestOrderTerminalState_IOCPartiallyFilled
PASS: TestOrderTerminalState_FOKCancelledWhenCannotFill
PASS: TestPartialFill_StatusAndQuantities
PASS: TestCancel_RemovesOrderFromBook
PASS: TestPostOnly_RejectsWhenCrossing
PASS: TestPostOnly_RestsWhenNotCrossing
```

### Integration Tests (Phase 2) — 4 tests

```bash
GOTOOLCHAIN=local go test ./test/integration/... -v
```

```
PASS: TestMultipleSymbolsRunIndependently
PASS: TestConcurrentOrdersOnSameSymbol
PASS: TestHaltAndResume
PASS: TestSequenceNumbersAreMonotonic
```

### Stress Test (Phase 8) — 2 tests

```bash
GOTOOLCHAIN=local go test ./test/stress/... -v
```

```
=== Stress Test Results ===
  Total orders submitted : 100000 / 100000
  Total trades generated : ~48000
  Conservation violations: 0
  Throughput             : ~350,000–450,000 orders/sec
PASS: TestStress_MultiSymbolConcurrentLoad
PASS: TestStress_RaceDetector
```

### Race Detector

```bash
GOTOOLCHAIN=local go test -race ./...
```

---

## Performance

Measured on a single machine, Go 1.22.2, Linux/amd64:

| Metric | Value |
|---|---|
| Order throughput (20 symbols, 500 goroutines) | ~350,000–450,000 orders/sec |
| Orders in stress test | 100,000 |
| Trades generated | ~48,000 |
| Conservation violations | **0** |
| Race conditions | **0** |
| Latency target | Low-millisecond (retail exchange, not HFT) |

**Hot path operations per order:**

1. Channel send into buffered `chan request` — O(1)
2. `book.Submit()` — O(log n) price level lookup, O(1) FIFO dequeue
3. `settlement.Settle()` — 2 debits + 2 credits (RWMutex)
4. `seq.Add(1)` — atomic fetch-add
5. `bus.Publish()` — one non-blocking channel send per subscriber (RLock)

---

## HTTP Endpoints

The engine exposes a minimal HTTP interface for testing and integration:

| Method | Path | Description |
|---|---|---|
| `GET` | `/ws` | WebSocket event feed — all events as JSON |
| `GET` | `/ticker?symbol=BTC-USDT&market=SPOT` | Best bid/ask, mid price, spread |
| `POST` | `/order?symbol=BTC-USDT&market=SPOT&side=BUY&price=50000&qty=0.1&account=acc1` | Submit limit order |
| `GET` | `/admin/halt?symbol=BTC-USDT&market=SPOT` | Halt trading on symbol |
| `GET` | `/admin/resume?symbol=BTC-USDT&market=SPOT` | Resume trading on symbol |

---

## Infrastructure

All services are hosted on [Aiven Cloud](https://aiven.io).

### Redis (Aiven)

**Role:** Book snapshots (fast-restart), session tokens, rate-limit counters. **Not** source of truth.

| Property | Value |
|---|---|
| Host | `dex-redis-agentmail-604e.a.aivencloud.com` |
| Port | `19334` |
| User | `default` |
| TLS | Required (`rediss://` scheme) |
| Replica | `replica-dex-redis-agentmail-604e.a.aivencloud.com:19334` |

### PostgreSQL (Aiven)

**Role:** Async durable log for events, orders, trades. Static symbol config. Never on the hot path.

| Property | Value |
|---|---|
| Host | `pg-3603208d-agentmail-604e.b.aivencloud.com` |
| Port | `19333` |
| Database | `dexdb` |
| User | `avnadmin` |
| SSL | Required |
| Replica | `replica-pg-3603208d-agentmail-604e.b.aivencloud.com:19333` |

**Auto-migrated schema:**

```sql
orders  (id PK, account_id, symbol, market, side, type, price, quantity, filled, status, ...)
trades  (id PK, symbol, sequence_number INDEXED, price, quantity, executed_at, ...)
events  (id BIGSERIAL, symbol, sequence_number UNIQUE, type, payload JSONB, ...)
```

### Kafka (Aiven)

**Role:** Primary durable event bus. Real source of truth for event history. Durable outbox for Postgres retry.

| Property | Value |
|---|---|
| Host | `dex-kafka-agentmail-604e.a.aivencloud.com` |
| Port | `19346` |
| Auth | SASL/PLAIN over TLS |
| Schema Registry | Port `19338` |
| Kafka REST | Port `19337` |

| Topic | Purpose |
|---|---|
| `matching-engine.events` | All order state changes and trades |
| `matching-engine.trades` | Trade-only stream for market data |
| `matching-engine.outbox` | Durable outbox for Postgres retry |

---

## Design Decisions

**Why one goroutine per symbol?**
A dedicated goroutine consuming from a buffered channel eliminates all lock contention on the order book. The book struct itself requires no mutex. Throughput scales by adding goroutines for new symbols; symbols never contend with each other.

**Why `shopspring/decimal` instead of `float64`?**
`float64` introduces rounding errors that violate the conservation invariant (`buy filled != sell filled` in edge cases). `decimal` gives exact results at the cost of slightly higher CPU — always correct for financial arithmetic.

**Why is the in-memory ledger authoritative and not Postgres?**
A pre-trade risk check that queries Postgres adds a network round-trip to every order. The in-memory ledger is updated synchronously on every fill. On crash it is rebuilt from the Kafka event log.

**Why does FOK pre-check instead of tentative match with rollback?**
The matching loop has no rollback by design — it keeps the hot path branch-free and mutation-only. The pre-check scans resting levels read-only before touching any state.

**Why is settlement outside the matching loop?**
Futures margin and options exercise logic must not contaminate the core `Order`/`Trade` structs. The `SettlementHandler` interface receives a completed `Trade` with transient order refs (`json:"-"`) and applies asset-class-specific logic, keeping the matching loop identical for Spot, Futures, and Options.

**Why is the circuit breaker a separate code path from risk rejection?**
Per-order rejection does not stop other orders from being processed. A symbol-wide halt stops the engine goroutine from consuming its input channel entirely — needed for regulatory halts, erroneous price discovery, or maintenance windows.

---

## Invariants

All invariants hold under concurrent, high-load conditions (verified by automated tests):

| # | Invariant | Tests |
|---|---|---|
| 1 | **Price-time priority** — never filled ahead of a better-priced or same-price earlier order | `TestPriceTimePriority_*` |
| 2 | **Conservation** — buy filled qty equals sell filled qty for every trade | `TestConservation_*`, `TestStress_MultiSymbolConcurrentLoad` |
| 3 | **Limit price** — no order executes at a price worse than its own limit | `TestLimitPriceNeverViolated_*` |
| 4 | **No crossed book** — best bid is never >= best ask after any operation | `TestBookNeverCrossed` |
| 5 | **Terminal state** — every order reaches FILLED, CANCELLED, REJECTED, or EXPIRED | `TestOrderTerminalState_*` |
| 6 | **Sequence monotonicity** — per-symbol sequence numbers are strictly monotonic | `TestSequenceNumbersAreMonotonic` |
| 7 | **Partial fill correctness** — partially filled orders stay in book with correct remaining qty | `TestPartialFill_StatusAndQuantities` |
| 8 | **Cancel removes from book** — cancelled orders are no longer matchable | `TestCancel_RemovesOrderFromBook` |

---

## Dependencies

| Package | Version | Purpose |
|---|---|---|
| `shopspring/decimal` | v1.4.0 | Exact decimal arithmetic |
| `google/uuid` | v1.6.0 | Order and trade ID generation |
| `segmentio/kafka-go` | v0.4.47 | Kafka producer + consumer (SASL/TLS) |
| `jackc/pgx/v5` | v5.6.0 | PostgreSQL connection pool |
| `redis/go-redis/v9` | v9.5.1 | Redis client with TLS support |
| `gorilla/websocket` | v1.5.3 | WebSocket server |
| `joho/godotenv` | v1.5.1 | `.env` file loading |
| `stretchr/testify` | v1.9.0 | Test assertions |

---

## Non-Goals

- **No production UI or public REST API** — HTTP endpoints exist solely for testing and demonstration.
- **No real payment, custody, or withdrawal logic** — balances are simulated in the in-memory ledger.
- **No sub-microsecond HFT latency** — the target is consistent low-millisecond latency at high throughput for a retail-facing exchange, not a colocated HFT venue.
