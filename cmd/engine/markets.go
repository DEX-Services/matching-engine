package main

import (
	"net/http"
	"time"

	"github.com/dex/matching-engine/internal/config"
	"github.com/dex/matching-engine/internal/marketdata"
	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

// currentMarkets is the deliberately small execution set for this delivery.
// It is separate from the wider list of assets the frontend displays: those
// assets remain visible while their engines/configuration are implemented.
// Every market — spot AND futures — quotes in USDB, the platform's internal
// stable currency (pegged 1:1 to USDT, no on-chain contract of its own) —
// see Dex-Backend's chain.Listener and repo/ledger.go for the credit side.
// USDT/USDC are no longer tradable quote currencies anywhere on the
// exchange; a real USDC deposit still lands as USDC in the deposit-intake
// ledger, but every tradable balance and every market (spot or futures)
// converts to/settles in USDB at 1:1. Futures collateral used to be real
// USDC (see the (symbol, market) key below — BTC-USDB/FUTURES is a distinct
// row from BTC-USDB/SPOT, so the two coexist without collision).
var currentMarkets = []marketDefinition{
	{displaySymbol: "BTC-USDB", symbol: "BTC-USDB", market: models.Spot, base: "BTC", quote: "USDB"},
	{displaySymbol: "ETH-USDB", symbol: "ETH-USDB", market: models.Spot, base: "ETH", quote: "USDB"},
	{displaySymbol: "SOL-USDB", symbol: "SOL-USDB", market: models.Spot, base: "SOL", quote: "USDB"},
	{displaySymbol: "BNB-USDB", symbol: "BNB-USDB", market: models.Spot, base: "BNB", quote: "USDB"},
	{displaySymbol: "BTC-PERP", symbol: "BTC-USDB", market: models.Futures, base: "BTC", quote: "USDB"},
	{displaySymbol: "ETH-PERP", symbol: "ETH-USDB", market: models.Futures, base: "ETH", quote: "USDB"},
	// Crypto perps beyond BTC/ETH: the SOL/BNB spot books above double as the
	// index/funding underlying (see seedSymbolConfigs's underlying_symbol).
	{displaySymbol: "SOL-PERP", symbol: "SOL-USDB", market: models.Futures, base: "SOL", quote: "USDB"},
	{displaySymbol: "BNB-PERP", symbol: "BNB-USDB", market: models.Futures, base: "BNB", quote: "USDB"},
	// Non-crypto perps (forex majors, commodities, US stocks). There is no
	// engine spot book for any of these, so no funding underlying exists —
	// their symbol_configs rows leave underlying_symbol/funding unset (see
	// seed.go). The base ticker is case-sensitive for Live-Rates.com
	// instruments ("CrudeOIL", "AAPL.us") and doubles as the Price-Fetcher
	// Redis key the MM quotes against.
	{displaySymbol: "EURUSD", symbol: "EURUSD-USDB", market: models.Futures, base: "EURUSD", quote: "USDB"},
	{displaySymbol: "GBPUSD", symbol: "GBPUSD-USDB", market: models.Futures, base: "GBPUSD", quote: "USDB"},
	{displaySymbol: "AUDUSD", symbol: "AUDUSD-USDB", market: models.Futures, base: "AUDUSD", quote: "USDB"},
	{displaySymbol: "XAU-USD", symbol: "GOLD-USDB", market: models.Futures, base: "GOLD", quote: "USDB"},
	{displaySymbol: "XAG-USD", symbol: "SILVER-USDB", market: models.Futures, base: "SILVER", quote: "USDB"},
	{displaySymbol: "WTI-USD", symbol: "CrudeOIL-USDB", market: models.Futures, base: "CrudeOIL", quote: "USDB"},
	{displaySymbol: "AAPL-PERP", symbol: "AAPL.us-USDB", market: models.Futures, base: "AAPL.us", quote: "USDB"},
	{displaySymbol: "TSLA-PERP", symbol: "TSLA.us-USDB", market: models.Futures, base: "TSLA.us", quote: "USDB"},
	{displaySymbol: "NVDA-PERP", symbol: "NVDA.us-USDB", market: models.Futures, base: "NVDA.us", quote: "USDB"},
}

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

func marketSummaryHandler(data interface {
	Summary(string, models.MarketType) (*marketdata.Summary, error)
}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		symbol, market := r.URL.Query().Get("symbol"), models.MarketType(r.URL.Query().Get("market"))
		summary, err := data.Summary(symbol, market)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, MarketSummaryResponse{
			Symbol: summary.Symbol, Market: string(summary.Market), Price: summary.Price.String(),
			Change24hPct: summary.Change24hPct.String(), Volume24h: summary.Volume24h.String(),
			Has24hData: summary.Has24hData, UpdatedAt: summary.UpdatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
}
