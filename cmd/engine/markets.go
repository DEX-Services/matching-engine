package main

import (
	"net/http"
	"time"

	"github.com/dex/matching-engine/internal/config"
	"github.com/dex/matching-engine/internal/marketdata"
	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

// optionsEnabled is the single switch for options/combos, per the product
// decision to launch crypto spot/futures only (same treatment as
// forex/commodities/stocks — see disabledMarkets below).
//
// The /spread and /option-chain ROUTES are commented out of the mux
// entirely in main.go/spread.go — not registered at all, so a request there
// gets a plain 404 like any endpoint that doesn't exist, with no "coming
// soon" message from the API. This flag is what remains: submit.go's /order
// pipeline is shared with spot/futures and can't be commented out wholesale,
// so it checks this flag to reject an OPTIONS/COMBO_OPTIONS order the same
// generic way an unregistered symbol/market is rejected elsewhere — main.go
// also checks it to skip seeding option_instruments (pure overhead for a
// market nothing can submit an order to). Re-enabling options is flipping
// this to true AND uncommenting the two routes.
const optionsEnabled = false

// currentMarkets is the deliberately small execution set for this delivery.
// It is separate from the wider list of assets the frontend displays: those
// assets remain visible while their engines/configuration are implemented.
// Every market — spot AND futures — quotes in BI2XUSD, the platform's internal
// stable currency (pegged 1:1 to USDT, no on-chain contract of its own) —
// see Dex-Backend's chain.Listener and repo/ledger.go for the credit side.
// USDT/USDC are no longer tradable quote currencies anywhere on the
// exchange; a real USDC deposit still lands as USDC in the deposit-intake
// ledger, but every tradable balance and every market (spot or futures)
// converts to/settles in BI2XUSD at 1:1. Futures collateral used to be real
// USDC (see the (symbol, market) key below — BTC-BI2XUSD/FUTURES is a distinct
// row from BTC-BI2XUSD/SPOT, so the two coexist without collision).
// currentMarkets was narrowed 2026-09-12 (product decision) to an explicit,
// short list: SPOT is BI2X and BTC only; FUTURES is BI2X, BTC, ETH, AVAX,
// LINK, SOL, DOGE, TAO, ADA, and XRP. ETH/SOL/BNB spot and BNB futures were
// REMOVED entirely per that decision (not just disabled — see the removal
// note below); AVAX/LINK/DOGE/TAO/ADA/XRP are new futures-only listings.
var currentMarkets = []marketDefinition{
	// --- SPOT: BI2X only (BTC-BI2XUSD SPOT removed 2026-09-13 — see
	// removedMarkets below; BTC-PERP FUTURES below is unaffected) ---
	// BI2X: not a Binance-tracked asset like BTC below — its index price
	// comes from the dedicated BI2X data feed (internal Price-Fetcher
	// bitdxfeed client, added 2026-09-12), not Binance.
	{displaySymbol: "BI2X-BI2XUSD", symbol: "BI2X-BI2XUSD", market: models.Spot, base: "BI2X", quote: "BI2XUSD"},

	// --- FUTURES: BI2X, BTC, ETH, AVAX, LINK, SOL, DOGE, TAO, ADA, XRP ---
	{displaySymbol: "BTC-PERP", symbol: "BTC-BI2XUSD", market: models.Futures, base: "BTC", quote: "BI2XUSD"},
	{displaySymbol: "BI2X-PERP", symbol: "BI2X-BI2XUSD", market: models.Futures, base: "BI2X", quote: "BI2XUSD"},
	// ETH, AVAX, LINK, SOL, DOGE, TAO, ADA, and XRP are FUTURES-ONLY — none of
	// them has a spot row above (unlike BTC/BI2X, or the old SOL/BNB rows
	// this replaced, which self-funded off their own spot book). Each needs
	// no engine spot book to serve as its funding/index underlying: all are
	// real Binance <ASSET>USDT tickers (verified live 2026-09-12), so
	// Price-Fetcher's Binance client is the index source directly —
	// underlying_symbol stays empty in seed.go the same way the forex/
	// commodity rows already did before this change, since there is no
	// registered SPOT row for any of these to point at.
	{displaySymbol: "ETH-PERP", symbol: "ETH-BI2XUSD", market: models.Futures, base: "ETH", quote: "BI2XUSD"},
	{displaySymbol: "AVAX-PERP", symbol: "AVAX-BI2XUSD", market: models.Futures, base: "AVAX", quote: "BI2XUSD"},
	{displaySymbol: "LINK-PERP", symbol: "LINK-BI2XUSD", market: models.Futures, base: "LINK", quote: "BI2XUSD"},
	{displaySymbol: "SOL-PERP", symbol: "SOL-BI2XUSD", market: models.Futures, base: "SOL", quote: "BI2XUSD"},
	{displaySymbol: "DOGE-PERP", symbol: "DOGE-BI2XUSD", market: models.Futures, base: "DOGE", quote: "BI2XUSD"},
	{displaySymbol: "TAO-PERP", symbol: "TAO-BI2XUSD", market: models.Futures, base: "TAO", quote: "BI2XUSD"},
	{displaySymbol: "ADA-PERP", symbol: "ADA-BI2XUSD", market: models.Futures, base: "ADA", quote: "BI2XUSD"},
	{displaySymbol: "XRP-PERP", symbol: "XRP-BI2XUSD", market: models.Futures, base: "XRP", quote: "BI2XUSD"},

	// Forex majors, commodities, and US stocks are deliberately DISABLED for
	// now (product decision 2026-09-11: crypto-only for the current launch).
	// See disabledMarkets below — the implementation is untouched, just not
	// registered, so no engine, no book, and no MM/liquidation/funding
	// goroutine spins up for any of them. Re-enable by moving entries back
	// into this slice.
}

// removedMarkets: ETH-BI2XUSD/SOL-BI2XUSD/BNB-BI2XUSD (SPOT) and BNB-PERP
// (FUTURES) were REMOVED, not disabled, per the 2026-09-12 market-list
// restructure — unlike disabledMarkets below (a deliberate, reversible
// product decision to relaunch crypto-only), these four are simply not part
// of the platform's target list and were taken out along with their seed.go
// rows, Price-Fetcher/frontend registrations, and (for ETH/SOL/BNB) their
// user_balances ledger columns are left in place (a balance a user already
// holds must remain readable/withdrawable) but no longer tradable.
//
// BTC-BI2XUSD (SPOT) was additionally REMOVED on 2026-09-13, same treatment:
// taken out of currentMarkets and seed.go's SPOT seed row, and deactivated
// via deactivateRemovedMarkets below. Its (symbol, market) key is shared
// with BTC-PERP FUTURES above ("BTC-BI2XUSD"/models.Futures) — that row is
// untouched; only the SPOT row for this symbol string is gone.

// disabledMarkets lists every non-crypto instrument the engine is CAPABLE of
// running (spot/futures registration, margin, liquidation, and funding all
// already work for these the same as any FUTURES row above) but does not
// currently register, per the 2026-09-11 product decision to launch
// crypto-only. Nothing here is deleted: seed.go's symbol_configs rows,
// Price-Fetcher's instrument list, and the frontend's backendMarkets.ts
// mapping are all still intact and simply unused while this stays commented
// out of currentMarkets.
//
// To bring one of these back: move its line into currentMarkets above,
// uncomment the matching row in seed.go, uncomment the matching entry in
// Price-Fetcher's instrument list, and uncomment the matching row in the
// frontend's backendMarkets.ts REGISTERED map (see that file's own comment).
//
// var disabledMarkets = []marketDefinition{
// 	// Non-crypto perps (forex majors, commodities, US stocks). There is no
// 	// engine spot book for any of these, so no funding underlying exists —
// 	// their symbol_configs rows leave underlying_symbol/funding unset (see
// 	// seed.go). The base ticker is case-sensitive for Live-Rates.com
// 	// instruments ("CrudeOIL", "AAPL.us") and doubles as the Price-Fetcher
// 	// Redis key the MM quotes against.
// 	{displaySymbol: "EURUSD", symbol: "EURUSD-BI2XUSD", market: models.Futures, base: "EURUSD", quote: "BI2XUSD"},
// 	{displaySymbol: "GBPUSD", symbol: "GBPUSD-BI2XUSD", market: models.Futures, base: "GBPUSD", quote: "BI2XUSD"},
// 	{displaySymbol: "AUDUSD", symbol: "AUDUSD-BI2XUSD", market: models.Futures, base: "AUDUSD", quote: "BI2XUSD"},
// 	{displaySymbol: "XAU-USD", symbol: "GOLD-BI2XUSD", market: models.Futures, base: "GOLD", quote: "BI2XUSD"},
// 	{displaySymbol: "XAG-USD", symbol: "SILVER-BI2XUSD", market: models.Futures, base: "SILVER", quote: "BI2XUSD"},
// 	{displaySymbol: "WTI-USD", symbol: "CrudeOIL-BI2XUSD", market: models.Futures, base: "CrudeOIL", quote: "BI2XUSD"},
// 	{displaySymbol: "AAPL-PERP", symbol: "AAPL.us-BI2XUSD", market: models.Futures, base: "AAPL.us", quote: "BI2XUSD"},
// 	{displaySymbol: "TSLA-PERP", symbol: "TSLA.us-BI2XUSD", market: models.Futures, base: "TSLA.us", quote: "BI2XUSD"},
// 	{displaySymbol: "NVDA-PERP", symbol: "NVDA.us-BI2XUSD", market: models.Futures, base: "NVDA.us", quote: "BI2XUSD"},
// }

type marketDefinition struct {
	displaySymbol string
	symbol        string
	market        models.MarketType
	base          string
	quote         string
}

type MarketMetadata struct {
	DisplaySymbol     string   `json:"displaySymbol"`
	Symbol            string   `json:"symbol"`
	Market            string   `json:"market"`
	BaseCurrency      string   `json:"baseCurrency"`
	QuoteCurrency     string   `json:"quoteCurrency"`
	TickSize          string   `json:"tickSize"`
	LotSize           string   `json:"lotSize"`
	MinNotional       string   `json:"minNotional"`
	MaxPrice          string   `json:"maxPrice"`
	MaxQuantity       string   `json:"maxQuantity"`
	MakerFeePct       string   `json:"makerFeePct"`
	TakerFeePct       string   `json:"takerFeePct"`
	MaintenanceMargin string   `json:"maintenanceMarginRatePct,omitempty"`
	MaxLeverage       int      `json:"maxLeverage,omitempty"`
	EnabledOrderTypes []string `json:"enabledOrderTypes"`
}

func marketMetadata(def marketDefinition, cfg *config.SymbolConfig) MarketMetadata {
	// The defaults are the schema defaults used when a local engine runs without
	// Postgres. Production values come from symbol_configs and replace them.
	tickSize, lotSize := decimal.RequireFromString("0.01"), decimal.RequireFromString("0.00001")
	minNotional, maxPrice, maxQuantity := decimal.NewFromInt(1), decimal.NewFromInt(1_000_000), decimal.NewFromInt(1_000_000)
	makerFee, takerFee := decimal.RequireFromString("0.001"), decimal.RequireFromString("0.001")
	base, quote := def.base, def.quote
	maxLeverage := 0
	mmr := decimal.Zero
	if cfg != nil {
		tickSize, lotSize = cfg.TickSize, cfg.LotSize
		minNotional, maxPrice, maxQuantity = cfg.MinNotional, cfg.MaxPrice, cfg.MaxQuantity
		makerFee, takerFee = cfg.MakerFee, cfg.TakerFee
		base, quote = cfg.BaseCurrency, cfg.QuoteCurrency
		maxLeverage, mmr = cfg.MaxLeverage, cfg.MaintenanceMarginRate
	}
	return MarketMetadata{
		DisplaySymbol: def.displaySymbol, Symbol: def.symbol, Market: string(def.market),
		BaseCurrency: base, QuoteCurrency: quote,
		TickSize: tickSize.String(), LotSize: lotSize.String(),
		MinNotional: minNotional.String(), MaxPrice: maxPrice.String(), MaxQuantity: maxQuantity.String(),
		MakerFeePct:       makerFee.Mul(decimal.NewFromInt(100)).String(),
		TakerFeePct:       takerFee.Mul(decimal.NewFromInt(100)).String(),
		MaintenanceMargin: mmr.Mul(decimal.NewFromInt(100)).String(),
		MaxLeverage:       maxLeverage,
		EnabledOrderTypes: []string{string(models.Limit), string(models.Market), string(models.Stop), string(models.IOC), string(models.FOK), string(models.PostOnly)},
	}
}

func currentMarketMetadata(symbols *config.Registry) []MarketMetadata {
	out := make([]MarketMetadata, 0, len(currentMarkets))
	for _, def := range currentMarkets {
		cfg, err := symbols.Get(def.symbol, def.market)
		if err != nil {
			cfg = nil
		}
		out = append(out, marketMetadata(def, cfg))
	}
	return out
}

func marketsHandler(symbols *config.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, currentMarketMetadata(symbols))
	}
}

type MarketSummaryResponse struct {
	Symbol       string `json:"symbol"`
	Market       string `json:"market"`
	Price        string `json:"price"`
	Change24hPct string `json:"change24hPct,omitempty"`
	Volume24h    string `json:"volume24h,omitempty"`
	Has24hData   bool   `json:"has24hData"`
	UpdatedAt    string `json:"updatedAt"`
}

// summarySource is the marketdata surface the summary handlers need.
// SummaryAll enumerates every registered book so the batched response covers
// the full executable catalogue in one request.
type summarySource interface {
	Summary(string, models.MarketType) (*marketdata.Summary, error)
	SummaryAll() []marketdata.Summary
}

// toSummaryResponse converts one engine summary into its wire form.
func toSummaryResponse(s *marketdata.Summary) MarketSummaryResponse {
	return MarketSummaryResponse{
		Symbol: s.Symbol, Market: string(s.Market), Price: s.Price.String(),
		Change24hPct: s.Change24hPct.String(), Volume24h: s.Volume24h.String(),
		Has24hData: s.Has24hData, UpdatedAt: s.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

// marketSummaryHandler serves one summary per symbol/market pair, or — when
// no symbol query param is given — ALL registered symbols in a single batched
// response. The batch form is what the frontend's 5s market-list refresh
// calls: one request instead of one per market keeps the request count flat
// no matter how many symbols are listed (the old per-market fan-out was the
// largest single source of trade-page HTTP chatter).
func marketSummaryHandler(data summarySource) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		symbol, market := r.URL.Query().Get("symbol"), models.MarketType(r.URL.Query().Get("market"))
		if symbol == "" && market == "" {
			all := data.SummaryAll()
			out := make([]MarketSummaryResponse, 0, len(all))
			for i := range all {
				out = append(out, toSummaryResponse(&all[i]))
			}
			writeJSON(w, http.StatusOK, out)
			return
		}
		summary, err := data.Summary(symbol, market)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, toSummaryResponse(summary))
	}
}
