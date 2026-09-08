package marketdata

import (
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/orderbook"
	"github.com/shopspring/decimal"
)

type summaryBook struct{}

func (summaryBook) BestBid() decimal.Decimal                                         { return decimal.NewFromInt(109) }
func (summaryBook) BestAsk() decimal.Decimal                                         { return decimal.NewFromInt(111) }
func (summaryBook) Depth(int) ([]orderbook.LevelSnapshot, []orderbook.LevelSnapshot) { return nil, nil }

func TestSummaryUsesRealTradesForRollingMetrics(t *testing.T) {
	s := NewService()
	s.Register("BTC-USDT", models.Spot, summaryBook{})
	now := time.Now()
	s.RecordTrade("BTC-USDT", models.Spot, decimal.NewFromInt(100), decimal.NewFromInt(2), now.Add(-time.Hour))
	s.RecordTrade("BTC-USDT", models.Spot, decimal.NewFromInt(110), decimal.NewFromInt(3), now)

	got, err := s.Summary("BTC-USDT", models.Spot)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Has24hData || !got.Change24hPct.Equal(decimal.NewFromInt(10)) || !got.Volume24h.Equal(decimal.NewFromInt(530)) {
		t.Fatalf("summary = %#v, want 10%% change and 530 volume", got)
	}
}
