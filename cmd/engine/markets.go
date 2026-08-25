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
var currentMarkets = []marketDefinition{
	{displaySymbol: "BTC-USDT", symbol: "BTC-USDT", market: models.Spot, base: "BTC", quote: "USDT"},
	{displaySymbol: "ETH-USDT", symbol: "ETH-USDT", market: models.Spot, base: "ETH", quote: "USDT"},
	{displaySymbol: "SOL-USDT", symbol: "SOL-USDT", market: models.Spot, base: "SOL", quote: "USDT"},
	{displaySymbol: "BTC-PERP", symbol: "BTC-USDC", market: models.Futures, base: "BTC", quote: "USDC"},
	{displaySymbol: "ETH-PERP", symbol: "ETH-USDC", market: models.Futures, base: "ETH", quote: "USDC"},
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
