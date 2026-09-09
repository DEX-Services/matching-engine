package main

import (
	"encoding/json"
	"time"

	"github.com/dex/matching-engine/internal/config"
	"github.com/dex/matching-engine/internal/marketdata"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/settlement"
	"github.com/dex/matching-engine/internal/ws"
	"github.com/shopspring/decimal"
)

// buildTickerResponse assembles the per-symbol ticker payload shared by the
// GET /ticker HTTP handler and the periodic TICKER WebSocket frame. It was
// previously inlined in the handler only; both surfaces must return identical
// fields (mark price, futures index/funding preview, real fee/MMR config) so
// the frontend can consume either interchangeably.
func buildTickerResponse(mdSvc *marketdata.Service, symbols *config.Registry, sym string, mkt models.MarketType) (*TickerResponse, error) {
	ticker, err := mdSvc.Ticker(sym, mkt)
	if err != nil {
		return nil, err
	}
	resp := TickerResponse{
		Symbol: ticker.Symbol, Market: string(ticker.Market),
		BestBid: ticker.BestBid.String(), BestAsk: ticker.BestAsk.String(),
		MidPrice: ticker.MidPrice.String(), MarkPrice: ticker.MarkPrice.String(),
		Spread: ticker.Spread.String(),
	}
	if cfg, cerr := symbols.Get(sym, mkt); cerr == nil {
		resp.MakerFeePct = cfg.MakerFee.Mul(decimal.NewFromInt(100)).String()
		resp.TakerFeePct = cfg.TakerFee.Mul(decimal.NewFromInt(100)).String()
		if mkt == models.Futures {
			resp.MaintenanceMarginRatePct = cfg.MaintenanceMarginRate.Mul(decimal.NewFromInt(100)).String()
			if cfg.UnderlyingSymbol != "" {
				if indexTicker, ierr := mdSvc.Ticker(cfg.UnderlyingSymbol, models.Spot); ierr == nil && indexTicker.MarkPrice.IsPositive() {
					resp.IndexPrice = indexTicker.MarkPrice.String()
					resp.FundingRatePct = settlement.CurrentFundingRate(ticker.MarkPrice, indexTicker.MarkPrice).
						Mul(decimal.NewFromInt(100)).String()
				}
			}
		}
	}
	// Rolling 24h stats ride along so the periodic TICKER frame can replace
	// the frontend's /market-summary polling entirely (see dto.go).
	if summ, serr := mdSvc.Summary(sym, mkt); serr == nil {
		resp.Change24hPct = summ.Change24hPct.String()
		resp.Volume24h = summ.Volume24h.String()
		resp.Has24hData = summ.Has24hData
	}
	return &resp, nil
}

// TickerUpdate is the envelope for the periodic TICKER WebSocket frame. It is
// NOT a models.Event and never touches the event bus: bus events are persisted
// downstream (Postgres, Kafka, attached-order listener) and carry gapless
// per-symbol sequence numbers — neither applies to a 1-second UI snapshot.
// The frontend's wsClient skips sequence checking for this type.
type TickerUpdate struct {
	Type      string           `json:"type"` // "TICKER"
	Timestamp int64            `json:"timestamp"`
	Tickers   []TickerResponse `json:"tickers"`
}

// runTickerBroadcaster publishes one TICKER frame per tick (1s cadence,
// matching the price-fetcher's publication rate) containing every registered
// symbol's ticker. This replaces per-client HTTP polling of /ticker and
// /market-summary: the server computes the snapshot ONCE and fans the same
// bytes out to every connected client, so per-client cost is a single socket
// write regardless of how many markets exist. Frames are skipped entirely
// when no client is connected.
func runTickerBroadcaster(mdSvc *marketdata.Service, symbols *config.Registry, hub *ws.Hub, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for range ticker.C {
		if hub.ClientCount() == 0 {
			continue
		}
		keys := mdSvc.Symbols()
		if len(keys) == 0 {
			continue
		}
		tickers := make([]TickerResponse, 0, len(keys))
		for _, k := range keys {
			resp, err := buildTickerResponse(mdSvc, symbols, k.Symbol, k.Market)
			if err != nil {
				// A book vanishing between Symbols() and Ticker() is
				// benign (unregister race); skip it for this frame.
				continue
			}
			tickers = append(tickers, *resp)
		}
		if len(tickers) == 0 {
			continue
		}
		frame, err := json.Marshal(TickerUpdate{
			Type:      "TICKER",
			Timestamp: time.Now().UnixMilli(),
			Tickers:   tickers,
		})
		if err != nil {
			continue
		}
		hub.BroadcastJSON(frame)
	}
}
