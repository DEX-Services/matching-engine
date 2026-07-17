// cmd/engine/main.go wires together all phases and runs the matching engine.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

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
	futuresSettlement := settlement.NewFuturesSettlement(ledger, backend)
	optionsSettlement := settlement.NewOptionsSettlement(ledger, backend)

	// Phase 6: Settlement factory
	settlementFactory := func(symbol string, market models.MarketType) matching.SettlementHandler {
		switch market {
		case models.Futures:
			return futuresSettlement
		case models.Options:
			return optionsSettlement
		default:
			return settlement.NewSpotSettlement(ledger)
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
	pairs := []struct {
		symbol string
		market models.MarketType
	}{
		{"BTC-USDT", models.Spot},
		{"ETH-USDT", models.Spot},
		{"BTC-USDC", models.Futures},
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

	// Record last trade prices for mark-price computation. The mark price
	// blends the book mid-price with the most recent trade (capped to ±1%
	// deviation) so a thin book cannot be manipulated to trigger spurious
	// liquidations or skew funding.
	priceCh := bus.Subscribe(10_000)
	go func() {
		for evt := range priceCh {
			if evt.Type == models.EventTrade && evt.Trade != nil {
				mdSvc.RecordTrade(evt.Symbol, models.MarketType(evt.Market), evt.Trade.Price)
			}
		}
	}()

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

			if err := config.EnsureSchema(ctx, pool); err != nil {
				slog.Error("ensure symbol_configs schema", "error", err)
			} else if err := config.EnsureOptionInstruments(ctx, pool); err != nil {
				slog.Error("ensure option_instruments schema", "error", err)
			} else {
				seedSymbolConfigs(ctx, pool)
				seedOptionInstruments(ctx, pool)
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

	// Futures liquidation, funding, and options expiry background loops.
	liqEngine := liquidation.New(reg, futuresSettlement, mdSvc, symbolRegistry, checker, bus, ledger)
	go liqEngine.Run(ctx, time.Second)

	fundingScheduler := settlement.NewFundingScheduler(futuresSettlement, mdSvc, symbolRegistry, bus, pgPool)
	go fundingScheduler.Run(ctx, time.Minute)

	expiryProcessor := settlement.NewExpiryProcessor(optionsSettlement, ledger, mdSvc, bus, backend)
	go expiryProcessor.Run(ctx, time.Minute)

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
		if err := haltReg.Halt(sym, mkt, risk_admin.HaltManual, "admin"); err != nil {
			http.Error(w, "halt failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "halted %s/%s\n", sym, mkt)
	})
	mux.HandleFunc("/admin/resume", func(w http.ResponseWriter, r *http.Request) {
		sym, mkt := r.URL.Query().Get("symbol"), r.URL.Query().Get("market")
		if err := haltReg.Resume(sym, mkt); err != nil {
			http.Error(w, "resume failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
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
		leverage, _ := strconv.Atoi(q.Get("leverage"))
		strike, _ := decimal.NewFromString(q.Get("strike"))
		var expiry time.Time
		if exp := q.Get("expiry"); exp != "" {
			expiry, _ = time.Parse(time.RFC3339, exp)
		}
		o := &models.Order{
			ID: uuid.NewString(), AccountID: q.Get("account"),
			Symbol: q.Get("symbol"), Market: models.MarketType(q.Get("market")),
			Side: side, Type: orderType, Price: price, Quantity: qty,
			TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
			Leverage: leverage, MarginMode: q.Get("marginMode"),
			OptionType: q.Get("optionType"), StrikePrice: strike, Expiry: expiry,
		}

		// Options require per-instrument validation and engine creation.
		// Each option contract (unique strike/expiry/type) gets its own
		// order book so different instruments never share a book.
		if o.Market == models.Options {
			if err := validateAndPrepareOption(ctx, pgPool, symbolRegistry, reg, mdSvc, o); err != nil {
				http.Error(w, "invalid option order: "+err.Error(), http.StatusBadRequest)
				return
			}
		}

		if err := validateOrderConfig(symbolRegistry, o); err != nil {
			http.Error(w, "invalid order: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := checker.Check(o); err != nil {
			http.Error(w, "risk: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Reservation. Compute the worst-case margin/notional to reserve so
		// that settlement's debit (at actual fill prices) never exceeds the
		// reservation — which would either fail the debit (inconsistent
		// ledger) or leak permanently-locked funds.
		//
		//   Market orders:   best opposite quote (no own price).
		//   Futures sell limit: max(limit, bestBid) — margin scales with fill
		//     price, and the worst case for a short is filling at the best bid.
		//   Buy limits / spot: the limit price (a buyer never pays more).
		var resAsset string
		var resAmount decimal.Decimal
		if o.Type == models.Market {
			eng, gerr := reg.Get(o.Symbol, o.Market)
			if gerr != nil {
				http.Error(w, "risk: "+gerr.Error(), http.StatusBadRequest)
				return
			}
			var estPrice decimal.Decimal
			if o.IsBuy() {
				estPrice = eng.BestAsk()
			} else {
				estPrice = eng.BestBid()
			}
			resAsset, resAmount = risk.EstimatedRequired(o, estPrice)
		} else if o.Market == models.Futures && o.Side == models.Sell {
			resAsset, resAmount = risk.RequiredFor(o)
			if eng, gerr := reg.Get(o.Symbol, o.Market); gerr == nil {
				if bestBid := eng.BestBid(); bestBid.GreaterThan(o.Price) {
					resAsset, resAmount = risk.EstimatedRequired(o, bestBid)
				}
			}
		} else {
			resAsset, resAmount = risk.RequiredFor(o)
		}

		if resAmount.IsPositive() {
			if err := ledger.Reserve(o.AccountID, resAsset, resAmount); err != nil {
				http.Error(w, "risk: "+err.Error(), http.StatusBadRequest)
				return
			}
			// Mirror the reservation into Postgres synchronously: if the real
			// wallet doesn't have the funds (or Dex-Backend is unreachable),
			// the in-memory ledger and Postgres must not diverge, so roll
			// back the local reservation and reject the order.
			if backend.Enabled() {
				if err := backend.Lock(r.Context(), o.AccountID, resAsset, backendclient.ToRawUnits(resAmount)); err != nil {
					ledger.Release(o.AccountID, resAsset, resAmount)
					http.Error(w, "risk: balance lock failed: "+err.Error(), http.StatusBadRequest)
					return
				}
			}
		}

		// releaseOverReservation releases the difference between what was
		// reserved (at the worst-case price) and what settlement actually
		// debited (at fill prices), minus the reservation still needed for
		// any resting remainder (at the limit price, since a resting maker
		// fills at its own price). This fixes the price-improvement leak for
		// limit orders and generalises the market-order release to all types.
		releaseOverReservation := func(trades []*models.Trade) {
			filledDebit := risk.FilledDebit(o, trades)
			_, restingReserved := risk.ReleaseAmountFor(o)
			overReserved := resAmount.Sub(filledDebit).Sub(restingReserved)
			if overReserved.IsPositive() {
				ledger.Release(o.AccountID, resAsset, overReserved)
				if backend.Enabled() {
					backendclient.Async("unlock", func(ctx context.Context) error {
						return backend.Unlock(ctx, o.AccountID, resAsset, backendclient.ToRawUnits(overReserved))
					})
				}
			}
		}

		trades, snap, err := reg.SubmitSnapshot(o)
		if err != nil {
			// Nothing filled in rejection paths (halt, FOK-not-filled,
			// post-only-cross, invalid order) — release the full reservation.
			if resAmount.IsPositive() {
				ledger.Release(o.AccountID, resAsset, resAmount)
				if backend.Enabled() {
					backendclient.Async("unlock", func(ctx context.Context) error {
						return backend.Unlock(ctx, o.AccountID, resAsset, backendclient.ToRawUnits(resAmount))
					})
				}
			}
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Release any unused reservation after settlement (price improvement
		// on the filled portion + unfilled remainder for market/IOC orders).
		releaseOverReservation(trades)
		status := o.Status
		filled := o.Filled
		if snap != nil {
			status = snap.Status
			filled = snap.Filled
		}
		writeJSON(w, http.StatusOK, OrderResponse{
			OrderID: o.ID, Status: string(status), Filled: filled.String(), Trades: len(trades),
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
		if unlockAsset, unlockAmount := risk.ReleaseAmountFor(order); unlockAmount.IsPositive() {
			backendclient.Async("unlock", func(ctx context.Context) error {
				return backend.Unlock(ctx, order.AccountID, unlockAsset, backendclient.ToRawUnits(unlockAmount))
			})
		}
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

	mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
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
					Status: string(o.Status),
				})
			}
		}
		writeJSON(w, http.StatusOK, OrdersResponse{Orders: out})
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

	mux.HandleFunc("/positions", func(w http.ResponseWriter, r *http.Request) {
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
		}
		writeJSON(w, http.StatusOK, out)
	})

	mux.HandleFunc("/option-chain", func(w http.ResponseWriter, r *http.Request) {
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
			price := pricing.Price(spot, strike, tYears, assumedVol, riskFreeRate, isCall)
			greeks := pricing.CalcGreeks(spot, strike, tYears, assumedVol, riskFreeRate, isCall)
			spread := price * 0.02
			out = append(out, OptionChainEntry{
				Symbol: inst.Symbol, OptionType: inst.OptionType, Strike: inst.Strike.String(),
				Expiry: inst.Expiry.Format(time.RFC3339),
				Bid:    fmt.Sprintf("%.4f", price-spread/2),
				Ask:    fmt.Sprintf("%.4f", price+spread/2),
				Mid:    fmt.Sprintf("%.4f", price),
				IV:     assumedVol * 100,
				Delta:  greeks.Delta, Gamma: greeks.Gamma, Theta: greeks.Theta, Vega: greeks.Vega, Rho: greeks.Rho,
			})
		}
		writeJSON(w, http.StatusOK, OptionChainResponse{Underlying: underlying, Spot: spotTicker.MidPrice.String(), Chain: out})
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
	if cfg.LotSize.IsPositive() {
		if remainder := o.Quantity.Mod(cfg.LotSize); !remainder.IsZero() {
			return fmt.Errorf("quantity %s not a multiple of lot size %s", o.Quantity, cfg.LotSize)
		}
	}
	if cfg.MaxPrice.IsPositive() && o.Type != models.Market && o.Price.GreaterThan(cfg.MaxPrice) {
		return fmt.Errorf("price %s exceeds max price %s", o.Price, cfg.MaxPrice)
	}
	if cfg.MinNotional.IsPositive() && o.Type != models.Market {
		notional := o.Price.Mul(o.Quantity)
		if notional.LessThan(cfg.MinNotional) {
			return fmt.Errorf("notional %s below min notional %s", notional, cfg.MinNotional)
		}
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
