// cmd/engine/main.go wires together all phases and runs the matching engine.
package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dex/matching-engine/internal/attached"
	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/cache"
	"github.com/dex/matching-engine/internal/config"
	"github.com/dex/matching-engine/internal/events"
	"github.com/dex/matching-engine/internal/liquidation"
	"github.com/dex/matching-engine/internal/marketdata"
	"github.com/dex/matching-engine/internal/matching"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/orderbook"
	"github.com/dex/matching-engine/internal/persistence"
	"github.com/dex/matching-engine/internal/pricing"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/dex/matching-engine/internal/risk_admin"
	"github.com/dex/matching-engine/internal/settlement"
	"github.com/dex/matching-engine/internal/volsurface"
	"github.com/dex/matching-engine/internal/ws"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
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

	// Postgres balance-lock bridge: mirrors Reserve/Release/Debit into
	// Dex-Backend's real user_balances table so the wallet UI's "Available"
	// figure reflects held funds. No-ops if unconfigured.
	backend := backendclient.New()

	// Futures/Options settlement handlers are shared singletons (not one per
	// symbol) so the liquidation engine, funding scheduler, and expiry
	// processor can see every open position across all registered markets.
	futuresSettlement := settlement.NewFuturesSettlement(ledger, backend, bus)
	optionsSettlement := settlement.NewOptionsSettlement(ledger, backend)

	// Phase 6: Settlement factory. Fee lookup is late-bound: symbolRegistry is
	// loaded further down (after Postgres init), so the closure reads it at
	// settle time rather than capturing a nil value now.
	var symbolRegistryRef atomic.Pointer[config.Registry]
	feeLookup := func(symbol string, market models.MarketType) (maker, taker decimal.Decimal) {
		reg := symbolRegistryRef.Load()
		if reg == nil {
			return decimal.Zero, decimal.Zero
		}
		cfg, err := reg.Get(symbol, market)
		if err != nil {
			return decimal.Zero, decimal.Zero
		}
		return cfg.MakerFee, cfg.TakerFee
	}
	// comboSettlementRef is filled in once mdSvc/pgPool exist further down
	// (settlementFactory is only INVOKED lazily, when an engine is actually
	// created — by then this closure's captured comboSettlementRef will
	// already be set, same late-binding shape symbolRegistryRef uses for
	// feeLookup above).
	var comboSettlementRef atomic.Pointer[settlement.ComboSettlement]
	settlementFactory := func(symbol string, market models.MarketType) matching.SettlementHandler {
		switch market {
		case models.Futures:
			return futuresSettlement
		case models.Options:
			return optionsSettlement
		case models.ComboOptions:
			if cs := comboSettlementRef.Load(); cs != nil {
				return cs
			}
			// Should never happen in practice (a combo engine is only ever
			// created via /spread, which runs long after main() finishes
			// wiring) but fail closed rather than nil-panic if it somehow does.
			return matching.NoopSettlement{}
		default:
			return settlement.NewSpotSettlement(ledger, backend, feeLookup)
		}
	}

	// Phase 2: Registry
	reg := matching.NewRegistry(bus, settlementFactory, checker.Release)
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

	// Register trading pairs. Options engines are created lazily per
	// instrument via GetOrCreate (see validateAndPrepareOption), not
	// pre-registered here, because each strike/expiry/type contract needs
	// its own order book.
	for _, p := range currentMarkets {
		if _, err := reg.Register(p.symbol, p.market); err != nil {
			slog.Error("register engine", "error", err)
			os.Exit(1)
		}
		slog.Info("engine registered", "symbol", p.symbol, "market", string(p.market))
	}

	// Phase 7: Market Data
	mdSvc := marketdata.NewService()
	for _, p := range currentMarkets {
		eng, _ := reg.Get(p.symbol, p.market)
		mdSvc.Register(p.symbol, p.market, eng)
	}

	// Record last trade prices for mark-price computation. The mark price
	// blends the book mid-price with the most recent trade (capped to ±1%
	// deviation) so a thin book cannot be manipulated to trigger spurious
	// liquidations or skew funding.
	priceCh := bus.Subscribe(10_000)
	go func() {
		for evt := range priceCh {
			if evt.Type == models.EventTrade && evt.Trade != nil {
				mdSvc.RecordTrade(evt.Symbol, models.MarketType(evt.Market), evt.Trade.Price, evt.Trade.Quantity, evt.Trade.ExecutedAt)
			}
		}
	}()

	// Options writer margin needs a live underlying mark price (see
	// risk.shortOptionMargin) — wire it up now that mdSvc exists. The
	// Checker was constructed earlier (before mdSvc) so this is a late
	// setter rather than a constructor argument; nil-safe until this runs,
	// so order checks before this line still see cash-secured behavior.
	risk.SetMarkSource(mdSvc)

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
	var symbolRegistry *config.Registry
	var pgPool *pgxpool.Pool
	if os.Getenv("POSTGRES_HOST") != "" {
		pool, err := persistence.NewPool(ctx)
		if err != nil {
			slog.Warn("postgres disabled", "reason", err)
		} else {
			pgPool = pool
			persistence.Migrate(ctx, pool)
			if writer, err := persistence.NewWriter(pool); err == nil {
				go writer.Run(ctx)
				slog.Info("postgres writer started")
			}

			// Durable outbox: give the (already-started) Kafka publisher a
			// fallback for events a broker outage kept out of Kafka
			// entirely (see KafkaPublisher.publish's doc comment), and start
			// the sweeper that drains event_outbox back into the normal
			// tables. Postgres is set up after Kafka in this boot sequence
			// (see Phase 4 above), so this wiring has to happen here rather
			// than at KafkaPublisher construction.
			if kafkaPub != nil {
				kafkaPub.SetOutbox(persistence.NewOutboxWriter(pool))
			}
			go persistence.NewOutboxSweeper(pool, 10*time.Second).Run(ctx)

			if err := config.EnsureSchema(ctx, pool); err != nil {
				slog.Error("ensure symbol_configs schema", "error", err)
			} else if err := config.EnsureOptionInstruments(ctx, pool); err != nil {
				slog.Error("ensure option_instruments schema", "error", err)
			} else if err := volsurface.EnsureSchema(ctx, pool); err != nil {
				slog.Error("ensure iv_snapshots schema", "error", err)
			} else if err := ensureComboSchema(ctx, pool); err != nil {
				slog.Error("ensure combo_instruments schema", "error", err)
			} else {
				seedSymbolConfigs(ctx, pool)
				// See markets.go's optionsEnabled: seeding option_instruments
				// rows and running the 6h re-seed ticker are pure overhead
				// for a market nothing can trade on while it's flagged off.
				if optionsEnabled {
					seedOptionInstruments(ctx, pool)
					// Re-check periodically, not just at boot: a long-lived
					// process can outlive its seeded contracts' expiries (the
					// shortest is 7 days) without ever restarting to trigger the
					// boot-time reseed above.
					go func() {
						ticker := time.NewTicker(6 * time.Hour)
						defer ticker.Stop()
						for {
							select {
							case <-ctx.Done():
								return
							case <-ticker.C:
								seedOptionInstruments(ctx, pool)
							}
						}
					}()
				}
				if cfgReg, err := config.NewRegistry(ctx, pool); err != nil {
					slog.Error("load symbol config registry", "error", err)
				} else {
					symbolRegistry = cfgReg
					go cfgReg.StartHotReload(ctx, time.Minute)
				}
			}
		}
	}
	if symbolRegistry == nil {
		// Postgres disabled (local/dev without a DB): fall back to an empty
		// in-memory registry so futures maintenance-margin/funding config
		// simply reads as "not configured" instead of nil-panicking.
		symbolRegistry = config.NewInMemoryRegistry()
	}
	symbolRegistryRef.Store(symbolRegistry)

	// IV surface store for /option-chain: nil-pool-safe (NewStore tolerates
	// pgPool == nil, e.g. Postgres disabled locally), so this is always safe
	// to construct even when the schema-ensure above never ran.
	ivStore := volsurface.NewStore(pgPool)

	// Combo (multi-leg spread) settlement: fans a native combo book's trades
	// out into two linked option-leg trades against the SAME optionsSettlement
	// standalone option orders use. See settlement.ComboSettlement's doc
	// comment for why this is genuinely atomic, unlike the old /spread
	// endpoint's two-independent-orders-plus-unwind approach.
	comboAdapter := &comboSettlementAdapter{pool: pgPool, mdSvc: mdSvc}
	comboSettlementRef.Store(settlement.NewComboSettlement(optionsSettlement, comboAdapter, comboAdapter))

	// Attached (TP/SL) order groups: OCO cancel-sibling-on-fill and
	// fill/exposure-aware resize live outside the matching goroutine, as an
	// event-bus subscriber - the same pattern as the ws hub and trade
	// history writer above - so the matching core stays untouched.
	attachedReg := attached.NewRegistry()
	attachedCh := bus.Subscribe(10_000)
	attachedListener := attached.NewListener(attachedReg, reg, reg, futuresSettlement)
	go attachedListener.Run(attachedCh)

	// Futures liquidation, funding, and options expiry background loops.
	liqEngine := liquidation.New(reg, futuresSettlement, mdSvc, symbolRegistry, checker, bus, ledger)
	liqEngine.SetOptionsSettlement(optionsSettlement)
	go liqEngine.Run(ctx, time.Second)

	fundingScheduler := settlement.NewFundingScheduler(futuresSettlement, mdSvc, symbolRegistry, bus, pgPool)
	go fundingScheduler.Run(ctx, time.Minute)

	expiryProcessor := settlement.NewExpiryProcessor(optionsSettlement, ledger, mdSvc, bus, backend)
	go expiryProcessor.Run(ctx, time.Minute)

	// Phase 5: Redis
	if os.Getenv("REDIS_SERVICE_URI") != "" {
		// Redis is optional cache infrastructure. Never let a transient remote
		// connection stall the trading HTTP server indefinitely at startup.
		redisCtx, redisCancel := context.WithTimeout(ctx, 8*time.Second)
		defer redisCancel()
		if rc, err := cache.NewClient(redisCtx); err == nil {
			defer rc.Close()
		} else {
			slog.Warn("redis disabled", "reason", err)
		}
	}

	// HTTP server
	mux := http.NewServeMux()
	// Periodic TICKER frames: one snapshot per second for ALL symbols,
	// fanned out to every connected WebSocket client (see ticker.go). This
	// is what lets the trade UI drop its per-second /ticker and 5s
	// per-market /market-summary polling without losing its 1s cadence.
	go runTickerBroadcaster(mdSvc, symbolRegistry, hub, time.Second)
	mux.HandleFunc("/ws", hub.ServeWS)
	// /markets is the authoritative executable-market catalogue. It contains
	// only the five engines registered above; the frontend may continue to
	// display other assets until their separate implementation is ready.
	mux.HandleFunc("/markets", marketsHandler(symbolRegistry))
	mux.HandleFunc("/market-summary", marketSummaryHandler(mdSvc))
	// /ticker returns real JSON (was plain-text bid/ask/mid/spread with no
	// mark price, funding, fee, or MMR — insufficient for the trade page's
	// header, which was previously falling back to fabricated ±0.01%
	// mark/index numbers and a hardcoded MMR because there was nothing real
	// to fetch). Futures-only fields (index price, funding rate, MMR) are
	// omitted for spot/options.
	mux.HandleFunc("/ticker", func(w http.ResponseWriter, r *http.Request) {
		sym := r.URL.Query().Get("symbol")
		mkt := models.MarketType(r.URL.Query().Get("market"))
		// Shares its body with the periodic TICKER WS frame (see ticker.go)
		// so the HTTP fallback and the push stream can never drift apart.
		resp, err := buildTickerResponse(mdSvc, symbolRegistry, sym, mkt)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})
	// Halt/resume can take an entire market offline for every trader, so they
	// need the same shared-secret gate as every other privileged endpoint —
	// previously anyone with network access to the engine's port could halt
	// any market at will.
	mux.HandleFunc("/admin/halt", requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
		sym, mkt := r.URL.Query().Get("symbol"), r.URL.Query().Get("market")
		if err := haltReg.Halt(sym, mkt, risk_admin.HaltManual, "admin"); err != nil {
			http.Error(w, "halt failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "halted %s/%s\n", sym, mkt)
	}))
	mux.HandleFunc("/admin/resume", requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
		sym, mkt := r.URL.Query().Get("symbol"), r.URL.Query().Get("market")
		if err := haltReg.Resume(sym, mkt); err != nil {
			http.Error(w, "resume failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "resumed %s/%s\n", sym, mkt)
	}))
	// Halt STATUS. Without this, a halted symbol was invisible: a settlement
	// failure halts a market for every account trading it, and the only way
	// to discover that had happened was to read the engine's own logs or to
	// notice every order suddenly rejecting. Nothing could list what was
	// halted, so nothing could offer to un-halt it — recovery meant a manual
	// curl carrying the engine shared secret. Dex-Backend proxies this behind
	// the admin session so the admin UI can show and clear halts.
	mux.HandleFunc("/admin/halted", requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
		records := haltReg.HaltedSymbols()
		out := make([]map[string]string, 0, len(records))
		for _, rec := range records {
			out = append(out, map[string]string{
				"symbol": rec.Symbol,
				"market": rec.Market,
				"reason": string(rec.Reason),
				"note":   rec.Note,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"halted": out})
	}))
	mux.HandleFunc("/order", requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
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
		case "POST_ONLY":
			orderType = models.PostOnly
		case "STOP":
			orderType = models.Stop
		}
		price, _ := decimal.NewFromString(q.Get("price"))
		qty, _ := decimal.NewFromString(q.Get("qty"))
		leverage, _ := strconv.Atoi(q.Get("leverage"))
		strike, _ := decimal.NewFromString(q.Get("strike"))
		stopPrice, _ := decimal.NewFromString(q.Get("stopPrice"))
		reduceOnly := q.Get("reduceOnly") == "true"
		var expiry time.Time
		if exp := q.Get("expiry"); exp != "" {
			expiry, _ = time.Parse(time.RFC3339, exp)
		}
		o := &models.Order{
			ID: uuid.NewString(), AccountID: q.Get("account"),
			Symbol: q.Get("symbol"), Market: models.MarketType(q.Get("market")),
			Side: side, Type: orderType, Price: price, Quantity: qty,
			TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
			Leverage: leverage, MarginMode: q.Get("marginMode"), ReduceOnly: reduceOnly,
			StopPrice:  stopPrice,
			OptionType: q.Get("optionType"), StrikePrice: strike, Expiry: expiry,
		}

		snap, trades, status, err := submitOrderPipeline(r.Context(), submitDeps{
			reg: reg, ledger: ledger, backend: backend, checker: checker,
			symbolRegistry: symbolRegistry, futuresSettlement: futuresSettlement,
			pgPool: pgPool, mdSvc: mdSvc, bus: bus,
		}, o, q.Get("slippageBps"))
		if err != nil {
			http.Error(w, err.Error(), status)
			return
		}
		filled := o.Filled
		respStatus := o.Status
		if snap != nil {
			filled = snap.Filled
			respStatus = snap.Status
		}
		writeJSON(w, http.StatusOK, OrderResponse{
			OrderID: o.ID, Status: string(respStatus), Filled: filled.String(), Trades: len(trades),
		})
	}))

	mux.HandleFunc("/attached-order", attachedOrderHandler(submitDeps{
		reg: reg, ledger: ledger, backend: backend, checker: checker,
		symbolRegistry: symbolRegistry, futuresSettlement: futuresSettlement,
		pgPool: pgPool, mdSvc: mdSvc, bus: bus,
	}, attachedReg))

	mux.HandleFunc("/spread", spreadHandler(submitDeps{
		reg: reg, ledger: ledger, backend: backend, checker: checker,
		symbolRegistry: symbolRegistry, futuresSettlement: futuresSettlement,
		pgPool: pgPool, mdSvc: mdSvc, bus: bus,
	}))

	mux.HandleFunc("/cancel", requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		symbol := q.Get("symbol")
		market := models.MarketType(q.Get("market"))
		orderID := q.Get("order_id")
		accountID := q.Get("account")
		if symbol == "" || orderID == "" || accountID == "" {
			http.Error(w, "symbol, order_id, and account are required", http.StatusBadRequest)
			return
		}
		order, resting, lookupErr := reg.OrderByID(symbol, market, orderID)
		if lookupErr != nil || !resting {
			http.Error(w, "order not found", http.StatusNotFound)
			return
		}
		if err := requireOrderOwner(order, accountID); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
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
		if order.Type == models.Stop && !order.Price.IsPositive() {
			// Stop-market orders were reserved at submission time against an
			// estimated worst-case price (best opposite quote then), which
			// the order itself doesn't retain. Re-estimate at cancel time —
			// this is at least as conservative as the original reservation
			// in a typical market and avoids leaving funds permanently
			// locked for a cancelled, never-triggered stop.
			asset := ""
			if eng, gerr := reg.Get(order.Symbol, order.Market); gerr == nil {
				var estPrice decimal.Decimal
				if order.IsBuy() {
					estPrice = eng.BestAsk()
				} else {
					estPrice = eng.BestBid()
				}
				if estPrice.IsPositive() {
					var amount decimal.Decimal
					asset, amount = risk.EstimatedRequired(order, estPrice)
					if amount.IsPositive() {
						ledger.Release(order.AccountID, asset, amount)
						if backend.Enabled() {
							// A cancellation must durably release its hold before the
							// caller can place replacement quotes. Async unlocks raced
							// the next MM ladder and caused false insufficient-balance
							// rejections.
							if err := backend.Unlock(r.Context(), order.AccountID, asset, backendclient.ToRawUnits(amount)); err != nil {
								slog.Error("backend unlock after cancel failed", "order", order.ID, "error", err)
							}
						}
					}
				}
			}
		} else {
			checker.Release(order)
			if unlockAsset, unlockAmount := risk.ReleaseAmountFor(order); unlockAmount.IsPositive() {
				// Wait for the durable release before acknowledging cancellation;
				// market makers immediately replace cancelled orders.
				if err := backend.Unlock(r.Context(), order.AccountID, unlockAsset, backendclient.ToRawUnits(unlockAmount)); err != nil {
					slog.Error("backend unlock after cancel failed", "order", order.ID, "error", err)
				}
			}
		}
		writeJSON(w, http.StatusOK, OrderResponse{
			OrderID: order.ID, Status: string(order.Status), Filled: order.Filled.String(),
		})
	}))

	// Internal market-maker batch replacement. Unlike /order and /cancel this
	// route installs a full passive ladder in one engine command.
	mux.HandleFunc("/market-maker/replace", requireEngineServiceAuth(marketMakerReplaceHandler(submitDeps{
		reg: reg, ledger: ledger, backend: backend, checker: checker,
		symbolRegistry: symbolRegistry, futuresSettlement: futuresSettlement,
		pgPool: pgPool, mdSvc: mdSvc, bus: bus,
	})))

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
		toDTO := func(levels []orderbook.LevelSnapshot) []DepthLevel {
			out := make([]DepthLevel, 0, len(levels))
			var total decimal.Decimal
			for _, l := range levels {
				total = total.Add(l.TotalQuantity)
				out = append(out, DepthLevel{
					Price: l.Price.String(),
					Size:  l.TotalQuantity.String(),
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

	mux.HandleFunc("/orders", requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
		account := r.URL.Query().Get("account")
		if account == "" {
			http.Error(w, "account is required", http.StatusBadRequest)
			return
		}
		out := make([]OpenOrderDTO, 0)
		for _, key := range reg.Symbols() {
			eng, err := reg.Get(key.Symbol, key.Market)
			if err != nil {
				continue
			}
			for _, o := range eng.AllOrders() {
				if o.AccountID != account {
					continue
				}
				out = append(out, OpenOrderDTO{
					ID: o.ID, Symbol: o.Symbol, Market: string(o.Market),
					Side: string(o.Side), Price: o.Price.String(),
					Qty: o.Quantity.String(), Filled: o.Filled.String(),
					Status:  string(o.Status),
					GroupID: o.GroupID, GroupRole: o.GroupRole,
				})
			}
		}
		writeJSON(w, http.StatusOK, OrdersResponse{Orders: out})
	}))
	mux.HandleFunc("/order-history", requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		account := q.Get("account")
		if account == "" {
			http.Error(w, "account is required", http.StatusBadRequest)
			return
		}
		limit, _ := strconv.Atoi(q.Get("limit"))
		before, _ := time.Parse(time.RFC3339Nano, q.Get("before"))
		after, _ := time.Parse(time.RFC3339Nano, q.Get("after"))
		items, err := persistence.OrderHistory(r.Context(), pgPool, persistence.OrderHistoryFilter{
			Account: account, Symbol: q.Get("symbol"), Market: q.Get("market"),
			After: after, Before: before, Limit: limit,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out := make([]OrderHistoryDTO, 0, len(items))
		for _, o := range items {
			out = append(out, OrderHistoryDTO{
				ID: o.ID, Symbol: o.Symbol, Market: o.Market, Side: o.Side, Type: o.Type,
				Price: o.Price.String(), Quantity: o.Quantity.String(), Filled: o.Filled.String(),
				Status: o.Status, RejectReason: o.RejectReason,
				AvgFillPrice: o.AvgFillPrice.String(), FeePaid: o.FeePaid.String(),
				CreatedAt: o.CreatedAt.UTC().Format(time.RFC3339Nano), UpdatedAt: o.UpdatedAt.UTC().Format(time.RFC3339Nano),
			})
		}
		resp := OrderHistoryResponse{Orders: out}
		if len(out) > 0 {
			resp.NextCursor = out[len(out)-1].CreatedAt
		}
		writeJSON(w, http.StatusOK, resp)
	}))

	// /fills is the fill-level complement to /order-history: individual trade
	// executions rather than per-order aggregates, for a "recent fills" or
	// "trade history" view that shows each execution as its own row.
	mux.HandleFunc("/fills", requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		account := q.Get("account")
		if account == "" {
			http.Error(w, "account is required", http.StatusBadRequest)
			return
		}
		limit, _ := strconv.Atoi(q.Get("limit"))
		before, _ := time.Parse(time.RFC3339Nano, q.Get("before"))
		after, _ := time.Parse(time.RFC3339Nano, q.Get("after"))
		items, err := persistence.Fills(r.Context(), pgPool, persistence.FillsFilter{
			Account: account, Symbol: q.Get("symbol"), Market: q.Get("market"),
			After: after, Before: before, Limit: limit,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out := make([]FillDTO, 0, len(items))
		for _, f := range items {
			out = append(out, FillDTO{
				TradeID: f.TradeID, OrderID: f.OrderID, Symbol: f.Symbol, Market: f.Market, Side: f.Side,
				Price: f.Price.String(), Quantity: f.Quantity.String(), FeePaid: f.FeePaid.String(),
				ExecutedAt: f.ExecutedAt.UTC().Format(time.RFC3339Nano),
			})
		}
		resp := FillsResponse{Fills: out}
		if len(out) > 0 {
			resp.NextCursor = out[len(out)-1].ExecutedAt
		}
		writeJSON(w, http.StatusOK, resp)
	}))

	// /funding-history is the persisted, queryable complement to the live
	// WebSocket FUNDING event: previously the frontend only had the
	// ephemeral WS stream (capped client-side, wiped on refresh), even
	// though funding payments were already being written to Postgres.
	mux.HandleFunc("/funding-history", requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		account := q.Get("account")
		if account == "" {
			http.Error(w, "account is required", http.StatusBadRequest)
			return
		}
		limit, _ := strconv.Atoi(q.Get("limit"))
		before, _ := time.Parse(time.RFC3339Nano, q.Get("before"))
		after, _ := time.Parse(time.RFC3339Nano, q.Get("after"))
		items, err := persistence.FundingHistory(r.Context(), pgPool, persistence.HistoryFilter{
			Account: account, Symbol: q.Get("symbol"), After: after, Before: before, Limit: limit,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out := make([]FundingPaymentDTO, 0, len(items))
		for _, it := range items {
			out = append(out, FundingPaymentDTO{
				Symbol: it.Symbol, Rate: it.Rate.String(), Amount: it.Amount.String(),
				CreatedAt: it.CreatedAt.UTC().Format(time.RFC3339Nano),
			})
		}
		resp := FundingHistoryResponse{Payments: out}
		if len(out) > 0 {
			resp.NextCursor = out[len(out)-1].CreatedAt
		}
		writeJSON(w, http.StatusOK, resp)
	}))

	// /pnl-history returns authoritative realized PnL per position close
	// (full or partial), including whether it was a forced liquidation.
	// This settlement math already existed in FuturesSettlement.closePortion
	// but was previously applied to the ledger only, never recorded.
	mux.HandleFunc("/pnl-history", requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		account := q.Get("account")
		if account == "" {
			http.Error(w, "account is required", http.StatusBadRequest)
			return
		}
		limit, _ := strconv.Atoi(q.Get("limit"))
		before, _ := time.Parse(time.RFC3339Nano, q.Get("before"))
		after, _ := time.Parse(time.RFC3339Nano, q.Get("after"))
		items, err := persistence.RealizedPnlHistory(r.Context(), pgPool, persistence.HistoryFilter{
			Account: account, Symbol: q.Get("symbol"), After: after, Before: before, Limit: limit,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out := make([]RealizedPnlDTO, 0, len(items))
		for _, it := range items {
			out = append(out, RealizedPnlDTO{
				Symbol: it.Symbol, ClosedQty: it.ClosedQty.String(), Pnl: it.Pnl.String(),
				MarginReturned: it.MarginReturned.String(), IsLiquidation: it.IsLiquidation,
				CreatedAt: it.CreatedAt.UTC().Format(time.RFC3339Nano),
			})
		}
		resp := PnlHistoryResponse{Entries: out}
		if len(out) > 0 {
			resp.NextCursor = out[len(out)-1].CreatedAt
		}
		writeJSON(w, http.StatusOK, resp)
	}))

	// /order/status reports the real state of a single order so a maker bot can
	// tell an actual (possibly partial) fill apart from a self-trade-prevention
	// cancel or an order lost to an engine restart. The live book is the
	// synchronous source of truth for resting orders; once an order leaves the
	// book we fall back to the durable Postgres record. When neither knows the
	// order (e.g. the async writer hasn't flushed it yet), found=false and the
	// caller accounts nothing rather than assuming a full fill.
	mux.HandleFunc("/order/status", requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		orderID := q.Get("id")
		symbol := q.Get("symbol")
		market := models.MarketType(q.Get("market"))
		if orderID == "" {
			http.Error(w, "id is required", http.StatusBadRequest)
			return
		}
		if symbol != "" {
			if o, resting, err := reg.OrderByID(symbol, market, orderID); err == nil && resting {
				writeJSON(w, http.StatusOK, OrderStatusResponse{
					OrderID: orderID, Found: true, Resting: true,
					Status: string(o.Status), Filled: o.Filled.String(),
				})
				return
			}
		}
		// Not resting: consult the durable record for its terminal state.
		st, err := persistence.OrderStatusByID(r.Context(), pgPool, orderID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !st.Found {
			writeJSON(w, http.StatusOK, OrderStatusResponse{OrderID: orderID, Found: false})
			return
		}
		writeJSON(w, http.StatusOK, OrderStatusResponse{
			OrderID: orderID, Found: true, Resting: false,
			Status: st.Status, Filled: st.Filled.String(),
		})
	}))

	mux.HandleFunc("/admin/balance", requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
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
	}))

	// /internal/ledger/sync lets Dex-Backend keep the engine's in-memory risk
	// ledger in step with real Postgres balance changes (deposits, approved
	// withdrawals). Authenticated with the same shared secret used for the
	// reverse-direction backendclient calls.
	mux.HandleFunc("/internal/ledger/sync", func(w http.ResponseWriter, r *http.Request) {
		engineSecret := os.Getenv("DEX_BACKEND_ENGINE_SECRET")
		if engineSecret == "" || r.Header.Get("X-Engine-Secret") != engineSecret {
			http.Error(w, "not authorized", http.StatusForbidden)
			return
		}
		var req struct {
			AccountID string `json:"accountId"`
			Asset     string `json:"asset"`
			Amount    string `json:"amount"`
			Direction string `json:"direction"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.AccountID == "" {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		amount, err := decimal.NewFromString(req.Amount)
		if err != nil || !amount.IsPositive() {
			http.Error(w, "amount must be a positive decimal", http.StatusBadRequest)
			return
		}
		switch req.Direction {
		case "credit":
			ledger.Credit(req.AccountID, req.Asset, amount)
		case "debit":
			if err := ledger.Debit(req.AccountID, req.Asset, amount); err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
		default:
			http.Error(w, "direction must be credit or debit", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/positions", requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		account := q.Get("account")
		if account == "" {
			http.Error(w, "account is required", http.StatusBadRequest)
			return
		}
		out := PositionsResponse{
			Futures: make([]FuturesPositionDTO, 0),
			Options: make([]OptionsPositionDTO, 0),
		}
		for _, p := range futuresSettlement.AllPositions() {
			if p.AccountID != account || p.Size.IsZero() {
				continue
			}
			mark := decimal.Zero
			if ticker, err := mdSvc.Ticker(p.Symbol, models.Futures); err == nil {
				mark = ticker.MarkPrice
			}
			out.Futures = append(out.Futures, FuturesPositionDTO{
				Symbol: p.Symbol, Side: string(p.Side), Size: p.Size.String(),
				EntryPrice: p.EntryPrice.String(), MarkPrice: mark.String(),
				Margin: p.Margin.String(), Leverage: p.Leverage,
				UnrealizedPnl: p.PnL(mark).String(),
			})
		}
		for _, p := range optionsSettlement.AllPositions() {
			if p.AccountID != account || p.Size.IsZero() {
				continue
			}
			out.Options = append(out.Options, OptionsPositionDTO{
				Symbol: p.Symbol, OptionType: p.OptionType, StrikePrice: p.StrikePrice.String(),
				Expiry: p.Expiry.Format(time.RFC3339), Size: p.Size.String(), Premium: p.Premium.String(),
			})
			accumulatePortfolioGreeks(&out.OptionsGreeks, p, mdSvc, ivStore, r.Context())
		}
		writeJSON(w, http.StatusOK, out)
	}))

	mux.HandleFunc("/option-chain", func(w http.ResponseWriter, r *http.Request) {
		// Options are DISABLED (2026-09-11 product decision: crypto
		// spot/futures only for the current launch) — see submit.go's order-
		// rejection gate for the full context. Rejected here too, or the
		// chain would still serve stale contracts left over from BEFORE
		// seeding was disabled (option_instruments rows persist until their
		// own expiry, up to 30 days) — a live-looking, seemingly-tradable
		// chain that then rejects every order is worse than an honest
		// "not available", and misleads the frontend's Coming Soon gate,
		// which shows this data if this endpoint ever returns any. Not
		// deleted: everything below still works exactly as before.
		if !optionsEnabled {
			http.Error(w, "options trading is coming soon", http.StatusServiceUnavailable)
			return
		}

		q := r.URL.Query()
		underlying := q.Get("underlying")
		if underlying == "" {
			http.Error(w, "underlying is required", http.StatusBadRequest)
			return
		}
		spotTicker, err := mdSvc.Ticker(underlying, models.Spot)
		if err != nil || spotTicker.MidPrice.IsZero() {
			http.Error(w, "no mark price for underlying", http.StatusNotFound)
			return
		}
		spot, _ := spotTicker.MidPrice.Float64()

		const assumedVol = 0.6 // annualized IV assumption until a real vol surface exists
		const riskFreeRate = 0.03

		instruments, err := loadOptionInstruments(ctx, pgPool, underlying)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out := make([]OptionChainEntry, 0, len(instruments))
		for _, inst := range instruments {
			strike, _ := inst.Strike.Float64()
			tYears := time.Until(inst.Expiry).Hours() / 24 / 365
			if tYears <= 0 {
				continue
			}
			isCall := inst.OptionType == "CALL"

			// vol starts from the IV surface's interpolation across this
			// expiry's other observed strikes (nearby contracts that have
			// recently had a live book) rather than a single flat guess for
			// the whole chain — a strike near actively-quoted neighbors gets
			// a much better estimate than one 60% assumption applied
			// everywhere. Falls back to assumedVol only when there is
			// nothing nearby to interpolate from yet (e.g. right after a
			// fresh contract is seeded, before any strike on this expiry has
			// ever traded).
			vol := assumedVol
			if iv, ok := ivStore.Interpolate(r.Context(), underlying, inst.Strike, inst.Expiry, inst.OptionType); ok {
				vol = iv
			}
			theo := pricing.Price(spot, strike, tYears, vol, riskFreeRate, isCall)

			// Blend with the instrument's own order book when it has live two-
			// sided quotes: a thin/never-traded strike falls back to pure
			// theoretical (mdSvc has no ticker for it at all until an order
			// touches it — see validateAndPrepareOption's mdSvc.Register), but
			// once quoted, the book mid is real price discovery and should not
			// be ignored in favor of the (interpolated or flat) vol guess.
			price := theo
			if bookTicker, err := mdSvc.Ticker(inst.Symbol, models.Options); err == nil && bookTicker.MidPrice.IsPositive() {
				bookMid, _ := bookTicker.MidPrice.Float64()
				price = (theo + bookMid) / 2
				// Re-imply vol from the blended price so the displayed
				// IV/greeks match what's actually being quoted, not the
				// interpolated/flat starting guess — bisection is cheap and
				// this endpoint is polled, not hot-path. This freshly-implied
				// IV is also what gets persisted below for OTHER strikes on
				// this expiry to interpolate from next time.
				if iv := pricing.ImpliedVol(price, spot, strike, tYears, riskFreeRate, isCall); iv > 0 {
					vol = iv
					ivStore.Record(r.Context(), underlying, inst.Strike, inst.Expiry, inst.OptionType, iv)
				}
			}
			if price < 0 {
				price = 0
			}
			greeks := pricing.CalcGreeks(spot, strike, tYears, vol, riskFreeRate, isCall)
			spread := math.Max(price*0.02, 0.01)
			out = append(out, OptionChainEntry{
				Symbol: inst.Symbol, OptionType: inst.OptionType, Strike: inst.Strike.String(),
				Expiry: inst.Expiry.Format(time.RFC3339),
				Bid:    fmt.Sprintf("%.4f", math.Max(price-spread/2, 0)),
				Ask:    fmt.Sprintf("%.4f", price+spread/2),
				Mid:    fmt.Sprintf("%.4f", price),
				IV:     vol * 100,
				Delta:  greeks.Delta, Gamma: greeks.Gamma, Theta: greeks.Theta, Vega: greeks.Vega, Rho: greeks.Rho,
			})
		}
		// Real per-instrument fee from symbol_configs (market=OPTIONS, keyed
		// by the underlying — see seed.go's BTC-BIUSD OPTIONS row), not a
		// frontend-hardcoded literal. Falls back to the schema default
		// (0.001 = 0.1%, matching the previous hardcoded value) if the
		// registry has no row yet, so this never regresses to a worse
		// default than what was already assumed.
		makerFeePct, takerFeePct := "0.1", "0.1"
		if cfg, err := symbolRegistryRef.Load().Get(underlying, models.Options); err == nil {
			makerFeePct = cfg.MakerFee.Mul(decimal.NewFromInt(100)).String()
			takerFeePct = cfg.TakerFee.Mul(decimal.NewFromInt(100)).String()
		}
		writeJSON(w, http.StatusOK, OptionChainResponse{
			Underlying: underlying, Spot: spotTicker.MidPrice.String(), Chain: out,
			MakerFeePct: makerFeePct, TakerFeePct: takerFeePct,
		})
	})

	srv := &http.Server{Addr: ":8080", Handler: withCORS(mux)}
	listener, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		slog.Error("failed to bind HTTP listener", "addr", srv.Addr, "error", err)
		os.Exit(1)
	}
	go func() {
		slog.Info("HTTP server listening", "addr", ":8080")
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			slog.Error("http server", "error", err)
		}
	}()

	// Startup ledger backfill: Dex-Backend calls back into this engine's own
	// /internal/ledger/sync for each nonzero balance, so it must run only
	// after the listener above is bound (guaranteed by net.Listen returning
	// above, not by the goroutine having reached Serve yet). Fail-open: an
	// unreachable Dex-Backend at boot shouldn't block the engine from serving
	// traffic.
	if backend.Enabled() {
		synced, failed, total, err := backend.Backfill(ctx)
		if err != nil {
			slog.Error("startup ledger backfill failed", "error", err)
		} else {
			slog.Info("startup ledger backfill complete", "synced", synced, "failed", failed, "total", total)
		}
	}

	// The integration-test demo seeder injects fake orders (buy@99-100,
	// sell@102-103) straight into the real BTC-USDT SPOT book on every boot.
	// That's fine for a local smoke test but was previously unconditional —
	// running it against a real deployment silently corrupts the one spot
	// market with orders nowhere near the real price, and skews the options
	// chain's underlying spot ticker along with it. Opt-in only now.
	if os.Getenv("ENGINE_DEMO_SEED") == "true" {
		runDemo(reg, ledger)
	}

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

// requireEngineServiceAuth protects account-scoped endpoints from direct
// browser access. Dex-Backend and the bots service call these endpoints with
// the shared service credential; end users reach them only through the
// authenticated Dex-Backend gateway, which derives the account from the
// wallet session rather than trusting a query parameter.
func requireEngineServiceAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		secret := os.Getenv("DEX_BACKEND_ENGINE_SECRET")
		if secret == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Engine-Secret")), []byte(secret)) != 1 {
			http.Error(w, "not authorized", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

func requireOrderOwner(order *models.Order, accountID string) error {
	if order == nil || order.AccountID != accountID {
		return fmt.Errorf("not authorized to cancel this order")
	}
	return nil
}

// checkReduceOnly rejects a reduce-only order that would open a new
// position, flip an existing one, or close more than is currently open.
// pos may be nil (no open position). Extracted as a pure function so the
// decision logic is unit-testable without standing up the HTTP handler.
func checkReduceOnly(o *models.Order, pos *settlement.Position) error {
	if pos == nil || pos.Size.IsZero() {
		return fmt.Errorf("reduceOnly order rejected, no open position")
	}
	sameDirection := (pos.Side == models.Buy && o.IsBuy()) || (pos.Side == models.Sell && !o.IsBuy())
	if sameDirection {
		return fmt.Errorf("reduceOnly order would increase the position")
	}
	if o.Quantity.GreaterThan(pos.Size) {
		return fmt.Errorf("reduceOnly order quantity exceeds open position size")
	}
	return nil
}

// validateOrderConfig enforces the per-symbol TickSize, LotSize, MinNotional
// and MaxPrice rules from the config registry against an incoming order. When
// no config is registered for the symbol/market (e.g. dev without Postgres),
// validation is skipped so the engine still accepts orders.
func validateOrderConfig(reg *config.Registry, o *models.Order) error {
	cfg, err := reg.Get(o.Symbol, o.Market)
	if err != nil {
		return nil // unconfigured symbol: no tick/lot rules to enforce
	}
	if cfg.TickSize.IsPositive() && o.Type != models.Market {
		if remainder := o.Price.Mod(cfg.TickSize); !remainder.IsZero() {
			return fmt.Errorf("price %s not a multiple of tick size %s", o.Price, cfg.TickSize)
		}
	}
	// Spot sellers may liquidate their complete remaining balance, including
	// dust below the standard market lot size. All other orders keep the
	// configured lot rule, including market-maker quotes.
	allowsDustSpotSell := o.Market == models.Spot && o.Side == models.Sell
	if cfg.LotSize.IsPositive() && !allowsDustSpotSell {
		if remainder := o.Quantity.Mod(cfg.LotSize); !remainder.IsZero() {
			return fmt.Errorf("quantity %s not a multiple of lot size %s", o.Quantity, cfg.LotSize)
		}
	}
	if cfg.MaxQuantity.IsPositive() && o.Quantity.GreaterThan(cfg.MaxQuantity) {
		return fmt.Errorf("quantity %s exceeds maximum quantity %s", o.Quantity, cfg.MaxQuantity)
	}
	if cfg.MaxPrice.IsPositive() && o.Type != models.Market && o.Price.GreaterThan(cfg.MaxPrice) {
		return fmt.Errorf("price %s exceeds max price %s", o.Price, cfg.MaxPrice)
	}
	if cfg.MinNotional.IsPositive() && o.Type != models.Market && !allowsDustSpotSell {
		// A stop order with no limit price (Price == 0) intentionally carries
		// no execution price yet — it activates as a market order once
		// StopPrice triggers (see orderbook.Book stop handling and
		// attached.BuildLegOrder's SL leg, which is built exactly this way).
		// Pricing notional off o.Price for such an order always computes 0
		// and rejects every stop-loss leg regardless of position size — use
		// StopPrice instead, which is the real reference price for this
		// order's eventual execution.
		notionalPrice := o.Price
		if o.Type == models.Stop && !o.Price.IsPositive() {
			notionalPrice = o.StopPrice
		}
		notional := notionalPrice.Mul(o.Quantity)
		if notional.LessThan(cfg.MinNotional) {
			return fmt.Errorf("notional %s below min notional %s", notional, cfg.MinNotional)
		}
	}
	if err := validateConfiguredLeverage(cfg, o); err != nil {
		return err
	}
	return nil
}

func validateConfiguredLeverage(cfg *config.SymbolConfig, o *models.Order) error {
	if o.Market != models.Futures || cfg.MaxLeverage <= 0 {
		return nil
	}
	leverage := o.Leverage
	if leverage < 1 {
		leverage = 10 // matches settlement's legacy default for omitted leverage.
	}
	if leverage > cfg.MaxLeverage {
		return fmt.Errorf("leverage %dx exceeds maximum %dx", leverage, cfg.MaxLeverage)
	}
	return nil
}

// validateAndPrepareOption validates option-specific order fields and ensures
// a dedicated matching engine exists for the instrument. Option contracts
// (each unique strike/expiry/type combination) must have their own order book
// so different instruments never match against each other.
//
// When Postgres is available, the instrument is looked up by symbol and the
// order's StrikePrice, Expiry, OptionType, and QuoteCurrency are populated
// from the database (not trusted from the client). When Postgres is
// disabled (dev mode), client-provided fields are used after validation, and
// QuoteCurrency is parsed from the instrument symbol (BASE-QUOTE-...).
func validateAndPrepareOption(ctx context.Context, pool *pgxpool.Pool, symbols *config.Registry,
	reg *matching.Registry, mdSvc *marketdata.Service, o *models.Order) error {

	if o.Symbol == "" {
		return fmt.Errorf("symbol is required")
	}

	// Try to look up the instrument from Postgres (authoritative source).
	if pool != nil {
		inst, err := loadOptionInstrument(ctx, pool, o.Symbol)
		if err != nil {
			return fmt.Errorf("instrument lookup: %w", err)
		}
		if inst != nil {
			o.StrikePrice = inst.Strike
			o.Expiry = inst.Expiry
			o.OptionType = inst.OptionType
			if cfg, err := symbols.Get(inst.Underlying, models.Options); err == nil {
				o.QuoteCurrency = cfg.QuoteCurrency
			}
		}
	}

	// Validate required fields regardless of source.
	if o.OptionType != "CALL" && o.OptionType != "PUT" {
		return fmt.Errorf("optionType must be CALL or PUT, got %q", o.OptionType)
	}
	if !o.StrikePrice.IsPositive() {
		return fmt.Errorf("strikePrice must be positive")
	}
	if o.Expiry.IsZero() {
		return fmt.Errorf("expiry is required")
	}
	if o.Expiry.Before(time.Now()) {
		return fmt.Errorf("expiry must be in the future")
	}

	// Determine quote currency if not already set from config.
	if o.QuoteCurrency == "" {
		// Parse from instrument symbol: BASE-QUOTE-STRIKE-EXPIRY-TYPE.
		parts := splitOptionSymbol(o.Symbol)
		if len(parts) >= 2 {
			o.QuoteCurrency = parts[1]
		}
		if o.QuoteCurrency == "" {
			return fmt.Errorf("cannot determine quote currency for option %s; set it via instrument config", o.Symbol)
		}
	}

	// Create a dedicated matching engine for this instrument if one does
	// not already exist. Each option contract gets its own order book.
	eng := reg.GetOrCreate(o.Symbol, o.Market)

	// Register the engine with market data so /ticker and /depth endpoints
	// work for option instruments too.
	mdSvc.Register(o.Symbol, o.Market, eng)

	return nil
}

// validateAndPrepareCombo resolves a combo order's N legs (o.ComboLegs,
// already set by the caller — see cmd/engine's combo submission path),
// validates they form a coherent combo (same underlying/expiry, at least 2
// legs, every leg a real instrument), registers the combo instrument
// (creating it on first use, same lazy pattern as individual option
// contracts), rewrites o.Symbol to the combo's own deterministic symbol so
// it gets its own dedicated order book, and sets QuoteCurrency.
//
// This is what makes combo orders match natively: after this runs, o.Symbol
// is a real registered instrument with a real order book — matching against
// OTHER resting combo orders on the exact same book, atomically, the same
// way any other instrument matches, regardless of how many legs it has.
// There is no client-side coordination of separate orders left anywhere in
// this path.
func validateAndPrepareCombo(ctx context.Context, pool *pgxpool.Pool, reg *matching.Registry, mdSvc *marketdata.Service, o *models.Order) error {
	if len(o.ComboLegs) == 0 {
		return fmt.Errorf("comboLegs is required")
	}
	insts, underlying, err := validateComboLegs(ctx, pool, o.ComboLegs)
	if err != nil {
		return err
	}

	combo, err := getOrCreateComboInstrument(ctx, pool, o.ComboLegs, underlying)
	if err != nil {
		return err
	}
	o.Symbol = combo.Symbol
	o.QuoteCurrency = "BIUSD"
	if len(insts) > 0 {
		if parts := splitOptionSymbol(insts[0].Symbol); len(parts) >= 2 {
			o.QuoteCurrency = parts[1]
		}
	}

	eng := reg.GetOrCreate(o.Symbol, o.Market)
	mdSvc.Register(o.Symbol, o.Market, eng)
	return nil
}

// accumulatePortfolioGreeks prices one open options position with Black-
// Scholes (spot from the underlying's live mark, vol from the IV surface
// with the same interpolate-or-flat-assumedVol fallback /option-chain uses)
// and adds its per-contract Greeks, weighted by signed position size, into
// the running portfolio total. Silently skips a position it cannot price
// (no live mark for its underlying yet, or it has already expired) rather
// than erroring the whole /positions response over one bad position — the
// account still sees its raw position list either way.
func accumulatePortfolioGreeks(totals *PortfolioGreeksDTO, p *settlement.OptionsPosition, mdSvc *marketdata.Service, ivStore *volsurface.Store, ctx context.Context) {
	const assumedVol = 0.6
	const riskFreeRate = 0.03

	underlying := underlyingFromOptionSymbol(p.Symbol, p.QuoteCurrency)
	spotTicker, err := mdSvc.Ticker(underlying, models.Spot)
	if err != nil || !spotTicker.MarkPrice.IsPositive() {
		return
	}
	spot, _ := spotTicker.MarkPrice.Float64()
	strike, _ := p.StrikePrice.Float64()
	tYears := time.Until(p.Expiry).Hours() / 24 / 365
	if tYears <= 0 {
		return
	}
	vol := assumedVol
	if iv, ok := ivStore.Interpolate(ctx, underlying, p.StrikePrice, p.Expiry, p.OptionType); ok {
		vol = iv
	}
	isCall := p.OptionType == "CALL"
	greeks := pricing.CalcGreeks(spot, strike, tYears, vol, riskFreeRate, isCall)
	size, _ := p.Size.Float64() // signed: positive long, negative short

	totals.Delta += greeks.Delta * size
	totals.Gamma += greeks.Gamma * size
	totals.Theta += greeks.Theta * size
	totals.Vega += greeks.Vega * size
}

// underlyingFromOptionSymbol extracts the underlying spot symbol (e.g.
// "BTC-BIUSD") from an option instrument symbol, using the same 5-part
// BASE-QUOTE-STRIKE-EXPIRY-TYPE format splitOptionSymbol parses elsewhere in
// this file. Mirrors settlement.underlyingFromSymbol and
// risk.underlyingFromOrderSymbol — each package has its own tiny copy of
// this parse rather than a shared export, since cmd/engine (main) already
// depends on both and duplicating four lines here is simpler than
// restructuring either package's public surface just for this.
func underlyingFromOptionSymbol(symbol, quoteCurrency string) string {
	parts := splitOptionSymbol(symbol)
	if len(parts) >= 5 {
		return parts[0] + "-" + parts[1]
	}
	if len(parts) >= 1 && quoteCurrency != "" {
		return parts[0] + "-" + quoteCurrency
	}
	return symbol
}

// splitOptionSymbol splits an option instrument symbol on "-" into its
// components (BASE, QUOTE, STRIKE, EXPIRY, TYPE).
func splitOptionSymbol(symbol string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(symbol); i++ {
		if symbol[i] == '-' {
			parts = append(parts, symbol[start:i])
			start = i + 1
		}
	}
	parts = append(parts, symbol[start:])
	return parts
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
