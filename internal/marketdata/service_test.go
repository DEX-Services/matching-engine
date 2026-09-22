package marketdata

import (
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/fixedpoint"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/orderbook"
)

type summaryBook struct{}

func (summaryBook) BestBid() fixedpoint.Fixed                                        { return fixedpoint.FromInt64(109) }
func (summaryBook) BestAsk() fixedpoint.Fixed                                        { return fixedpoint.FromInt64(111) }
func (summaryBook) Depth(int) ([]orderbook.LevelSnapshot, []orderbook.LevelSnapshot) { return nil, nil }

func TestSummaryUsesRealTradesForRollingMetrics(t *testing.T) {
	s := NewService()
	s.Register("BTC-USDT", models.Spot, summaryBook{})
	now := time.Now()
	s.RecordTrade("BTC-USDT", models.Spot, fixedpoint.FromInt64(100), fixedpoint.FromInt64(2), now.Add(-time.Hour))
	s.RecordTrade("BTC-USDT", models.Spot, fixedpoint.FromInt64(110), fixedpoint.FromInt64(3), now)

	got, err := s.Summary("BTC-USDT", models.Spot)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Has24hData || !got.Change24hPct.Equal(fixedpoint.FromInt64(10)) || !got.Volume24h.Equal(fixedpoint.FromInt64(530)) {
		t.Fatalf("summary = %#v, want 10%% change and 530 volume", got)
	}
}
