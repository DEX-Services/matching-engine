// cmd/engine/main.go wires together all phases and runs the matching engine.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/dex/matching-engine/internal/cache"
	"github.com/dex/matching-engine/internal/events"
	"github.com/dex/matching-engine/internal/marketdata"
	"github.com/dex/matching-engine/internal/matching"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/orderbook"
	"github.com/dex/matching-engine/internal/persistence"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/dex/matching-engine/internal/risk_admin"
	"github.com/dex/matching-engine/internal/settlement"
	"github.com/dex/matching-engine/internal/ws"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/shopspring/decimal"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	if err := godotenv.Load(); err != nil {
		slog.Info("no .env file, using env vars")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	slog.Info("matching engine starting",
		"postgres_host", os.Getenv("POSTGRES_HOST"),
		"redis_host", os.Getenv("REDIS_HOST"),
		"kafka_host", os.Getenv("KAFKA_HOST"),
	)

	// Phase 4: Event Bus
	bus := events.NewBus()
	wsCh := bus.Subscribe(10_000)

	// Trade history ring buffer for GET /trades
	tradeHistory := marketdata.NewTradeHistory(500)
	tradeCh := bus.Subscribe(10_000)
	go tradeHistory.Run(tradeCh)

	// Phase 3: Risk Ledger
	ledger := risk.NewLedger()
	checker := risk.NewChecker(ledger)

	// Phase 6: Settlement factory
	settlementFactory := func(symbol string, market models.MarketType) matching.SettlementHandler {
		switch market {
		case models.Futures:
			return settlement.NewFuturesSettlement(ledger)
		case models.Options:
			return settlement.NewOptionsSettlement(ledger)
		default:
			return settlement.NewSpotSettlement(ledger)
		}
	}

	// Phase 2: Registry
	reg := matching.NewRegistry(bus, settlementFactory)
	defer reg.StopAll()

	// Phase 7: Halt Registry
	haltReg := risk_admin.NewRegistry()
	haltReg.HaltFunc = func(sym, mkt string) {
		if eng, err := reg.Get(sym, models.MarketType(mkt)); err == nil {
			eng.Halt()
		}
	}
	haltReg.ResumeFunc = func(sym, mkt string) {
		if eng, err := reg.Get(sym, models.MarketType(mkt)); err == nil {
			eng.Resume()
		}
	}

	// Register trading pairs
	pairs := []struct {
		symbol string
		market models.MarketType
	}{
		{"BTC-USDT", models.Spot},
		{"ETH-USDT", models.Spot},
		{"BTC-USDT", models.Futures},
	}
	for _, p := range pairs {
		if _, err := reg.Register(p.symbol, p.market); err != nil {
			slog.Error("register engine", "error", err)
			os.Exit(1)
		}
		slog.Info("engine registered", "symbol", p.symbol, "market", string(p.market))
	}

	// Phase 7: Market Data
	mdSvc := marketdata.NewService()
	for _, p := range pairs {
		eng, _ := reg.Get(p.symbol, p.market)
		mdSvc.Register(p.symbol, p.market, eng)
	}

	// Phase 4: WebSocket
	hub := ws.NewHub(wsCh)
	go hub.Run()

	// Phase 4: Kafka Publisher
	var kafkaPub *events.KafkaPublisher
	if os.Getenv("KAFKA_HOST") != "" {
		var err error
		kafkaPub, err = events.NewKafkaPublisher(bus)
		if err != nil {
			slog.Warn("kafka publisher disabled", "reason", err)
		} else {
			go kafkaPub.Run(ctx)
			slog.Info("kafka publisher started")
		}
	}

	// Phase 5: Postgres
	if os.Getenv("POSTGRES_HOST") != "" {
		pool, err := persistence.NewPool(ctx)
		if err != nil {
			slog.Warn("postgres disabled", "reason", err)
		} else {
			persistence.Migrate(ctx, pool)
			if writer, err := persistence.NewWriter(pool); err == nil {
				go writer.Run(ctx)
				slog.Info("postgres writer started")
			}
		}
	}

	// Phase 5: Redis
	if os.Getenv("REDIS_SERVICE_URI") != "" {
		if rc, err := cache.NewClient(ctx); err == nil {
			defer rc.Close()
		} else {
			slog.Warn("redis disabled", "reason", err)
		}
	}

	// HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", hub.ServeWS)
	mux.HandleFunc("/ticker", func(w http.ResponseWriter, r *http.Request) {
		sym := r.URL.Query().Get("symbol")
		mkt := models.MarketType(r.URL.Query().Get("market"))
		ticker, err := mdSvc.Ticker(sym, mkt)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		fmt.Fprintf(w, "symbol=%s market=%s bid=%s ask=%s mid=%s spread=%s\n",
			ticker.Symbol, ticker.Market, ticker.BestBid, ticker.BestAsk, ticker.MidPrice, ticker.Spread)
	})
	mux.HandleFunc("/admin/halt", func(w http.ResponseWriter, r *http.Request) {
		sym, mkt := r.URL.Query().Get("symbol"), r.URL.Query().Get("market")
		haltReg.Halt(sym, mkt, risk_admin.HaltManual, "admin")
		fmt.Fprintf(w, "halted %s/%s\n", sym, mkt)
	})
	mux.HandleFunc("/admin/resume", func(w http.ResponseWriter, r *http.Request) {
		sym, mkt := r.URL.Query().Get("symbol"), r.URL.Query().Get("market")
		haltReg.Resume(sym, mkt)
		fmt.Fprintf(w, "resumed %s/%s\n", sym, mkt)
	})
	mux.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		side := models.Buy
		if q.Get("side") == "SELL" {
			side = models.Sell
		}
		orderType := models.Limit
		switch q.Get("type") {
		case "MARKET":
			orderType = models.Market
		case "IOC":
			orderType = models.IOC
		case "FOK":
			orderType = models.FOK
		}
		price, _ := decimal.NewFromString(q.Get("price"))
		qty, _ := decimal.NewFromString(q.Get("qty"))
		o := &models.Order{
			ID: uuid.NewString(), AccountID: q.Get("account"),
			Symbol: q.Get("symbol"), Market: models.MarketType(q.Get("market")),
			Side: side, Type: orderType, Price: price, Quantity: qty,
			TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
		}
		if err := checker.Check(o); err != nil {
			http.Error(w, "risk: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := checker.Reserve(o); err != nil {
			http.Error(w, "risk: "+err.Error(), http.StatusBadRequest)
			return
		}
		trades, err := reg.Submit(o)
		if err != nil {
			// Nothing filled in any of these rejection paths (halt, FOK-not-
			// filled, post-only-cross, invalid order) — release the full
			// reservation we just took.
			checker.Release(o)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, OrderResponse{
			OrderID: o.ID, Status: string(o.Status), Filled: o.Filled.String(), Trades: len(trades),
		})
	})

	mux.HandleFunc("/cancel", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		symbol := q.Get("symbol")
		market := models.MarketType(q.Get("market"))
		orderID := q.Get("order_id")
		if symbol == "" || orderID == "" {
			http.Error(w, "symbol and order_id are required", http.StatusBadRequest)
			return
		}
		order, err := reg.Cancel(symbol, market, orderID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		// Release whatever remains reserved for the unfilled portion. Safe
		// for partial fills: order.Filled reflects everything settled before
		// cancel, so RemainingQty() is exactly what's still held.
		checker.Release(order)
		writeJSON(w, http.StatusOK, OrderResponse{
			OrderID: order.ID, Status: string(order.Status), Filled: order.Filled.String(),
		})
	})

	mux.HandleFunc("/depth", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		sym := q.Get("symbol")
		mkt := models.MarketType(q.Get("market"))
		levels := 20
		if lv := q.Get("levels"); lv != "" {
			if n, err := strconv.Atoi(lv); err == nil && n > 0 {
				levels = n
			}
		}
		eng, err := reg.Get(sym, mkt)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		bidLevels, askLevels := eng.Depth(levels)
		toDTO := func(levels []*orderbook.PriceLevel) []DepthLevel {
			out := make([]DepthLevel, 0, len(levels))
			var total decimal.Decimal
			for _, l := range levels {
				total = total.Add(l.TotalQuantity())
				out = append(out, DepthLevel{
					Price: l.Price.String(),
					Size:  l.TotalQuantity().String(),
					Total: total.String(),
				})
			}
			return out
		}
		writeJSON(w, http.StatusOK, DepthResponse{
			Symbol: sym, Market: string(mkt),
			Bids: toDTO(bidLevels), Asks: toDTO(askLevels),
		})
	})

	mux.HandleFunc("/trades", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		sym := q.Get("symbol")
		mkt := q.Get("market")
		limit := 50
		if lv := q.Get("limit"); lv != "" {
			if n, err := strconv.Atoi(lv); err == nil && n > 0 {
				limit = n
			}
		}
		trades := tradeHistory.Recent(sym, mkt, limit)
		dtos := make([]TradeDTO, 0, len(trades))
		for _, t := range trades {
			side := "BUY"
			if t.MakerSide == models.Buy {
				// taker is opposite of the resting (maker) side
				side = "SELL"
			}
			dtos = append(dtos, TradeDTO{
				ID: t.ID, Symbol: t.Symbol, Market: string(t.Market),
				Price: t.Price.String(), Quantity: t.Quantity.String(),
				Side: side, Timestamp: t.ExecutedAt.UnixMilli(),
			})
		}
		writeJSON(w, http.StatusOK, TradesResponse{Symbol: sym, Market: mkt, Trades: dtos})
	})

	mux.HandleFunc("/admin/balance", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		account := q.Get("account")
		asset := q.Get("asset")
		if account == "" || asset == "" {
			http.Error(w, "account and asset are required", http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, BalanceResponse{
			Account:   account,
			Asset:     asset,
			Balance:   ledger.Balance(account, asset).String(),
			Reserved:  ledger.Reserved(account, asset).String(),
			Available: ledger.Available(account, asset).String(),
		})
	})

	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		slog.Info("HTTP server listening", "addr", ":8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server", "error", err)
		}
	}()

	runDemo(reg, ledger)

	<-ctx.Done()
	slog.Info("shutting down")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	srv.Shutdown(shutCtx)
	if kafkaPub != nil {
		kafkaPub.Close()
	}
	slog.Info("shutdown complete")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func runDemo(reg *matching.Registry, ledger *risk.Ledger) {
	const sym, mkt = "BTC-USDT", models.Spot
	_ = ledger.Deposit("buyer", "USDT", decimal.NewFromInt(100_000))
	_ = ledger.Deposit("seller", "BTC", decimal.NewFromInt(100))

	sub := func(acct string, side models.OrderSide, price, qty string) *models.Order {
		o := &models.Order{
			ID: uuid.NewString(), AccountID: acct, Symbol: sym, Market: mkt,
			Side: side, Type: models.Limit, Price: decimal.RequireFromString(price),
			Quantity: decimal.RequireFromString(qty), TimeInForce: models.GTC,
			Status: models.StatusPending, CreatedAt: time.Now(),
		}
		trades, err := reg.Submit(o)
		if err != nil {
			fmt.Printf("  REJECTED [%s %s@%s]: %v\n", side, qty, price, err)
			return o
		}
		for _, t := range trades {
			fmt.Printf("  TRADE  price=%-10s qty=%-6s maker=%s taker=%s\n",
				t.Price, t.Quantity, t.MakerOrderID[:8], t.TakerOrderID[:8])
		}
		fmt.Printf("  ORDER  [%s %s@%s] status=%-18s filled=%s\n", side, qty, price, o.Status, o.Filled)
		return o
	}

	fmt.Println("\n=== Integrated Demo (BTC-USDT Spot, Phases 1–7) ===")
	sub("buyer", models.Buy, "99", "2")
	sub("buyer", models.Buy, "100", "5")
	sub("seller", models.Sell, "102", "3")
	sub("seller", models.Sell, "103", "4")

	eng, _ := reg.Get(sym, mkt)
	fmt.Printf("\nBest Bid: %s  Best Ask: %s\n\n", eng.BestBid(), eng.BestAsk())

	sub("buyer", models.Buy, "102", "3")
	sub("buyer", models.Buy, "100", "5")

	mktO := &models.Order{
		ID: uuid.NewString(), AccountID: "seller", Symbol: sym, Market: mkt,
		Side: models.Sell, Type: models.Market, Quantity: decimal.NewFromInt(6),
		Status: models.StatusPending, CreatedAt: time.Now(),
	}
	trades, _ := reg.Submit(mktO)
	for _, t := range trades {
		fmt.Printf("  TRADE  price=%-10s qty=%s\n", t.Price, t.Quantity)
	}
	fmt.Printf("  Market sell status=%s filled=%s\n", mktO.Status, mktO.Filled)

	fmt.Printf("\nBuyer  USDT: %s  BTC: %s\n", ledger.Balance("buyer", "USDT"), ledger.Balance("buyer", "BTC"))
	fmt.Printf("Seller BTC:  %s  USDT: %s\n\n", ledger.Balance("seller", "BTC"), ledger.Balance("seller", "USDT"))
	fmt.Println("HTTP server :8080 — Ctrl+C to exit")
}
