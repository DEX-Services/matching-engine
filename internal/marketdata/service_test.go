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

// TestTradeRing_EvictsExpiredAndPreservesOrder is a direct test of the ring
// buffer backing RecordTrade's 24h window (PERFORMANCE-CODE-REVIEW-FINDINGS.md
// item #6), independent of Service/Summary — it exercises wraparound (push
// past the initial capacity so the tail index wraps around the backing
// array), growth (push past capacity entirely, forcing grow() to re-linearize
// the wrapped contents), and eviction (entries older than cutoff dropped from
// the head) directly, since a subtly wrong modulo or copy-order bug in any of
// those would silently corrupt or reorder a real symbol's 24h trade history.
func TestTradeRing_EvictsExpiredAndPreservesOrder(t *testing.T) {
	r := newTradeRing()
	base := time.Now()

	// Fill and wrap past the initial capacity entirely (no eviction: a
	// cutoff far in the past keeps every entry live), forcing at least one
	// grow() and exercising the wraparound copy in forEach along the way.
	n := tradeRingInitialCap*2 + 5
	farCutoff := base.Add(-1000 * time.Hour)
	for i := 0; i < n; i++ {
		r.pushEvictingBefore(recordedTrade{
			price: fixedpoint.FromInt64(int64(i)),
			qty:   fixedpoint.FromInt64(1),
			at:    base.Add(time.Duration(i) * time.Second),
		}, farCutoff)
	}
	if r.count != n {
		t.Fatalf("count = %d, want %d", r.count, n)
	}
	var seen []int64
	r.forEach(func(tr recordedTrade) {
		seen = append(seen, int64(tr.price.Sign())*0+tr.price.ToDecimal().IntPart())
	})
	if len(seen) != n {
		t.Fatalf("forEach visited %d entries, want %d", len(seen), n)
	}
	for i, v := range seen {
		if v != int64(i) {
			t.Fatalf("forEach order broken at index %d: got price %d, want %d (full sequence: %v)", i, v, i, seen)
		}
	}

	// Now push one more entry with a cutoff that should evict everything
	// except the newest few — proves eviction advances head correctly even
	// after a grow() reset it to 0.
	cutoff := base.Add(time.Duration(n-3) * time.Second)
	r.pushEvictingBefore(recordedTrade{
		price: fixedpoint.FromInt64(int64(n)),
		qty:   fixedpoint.FromInt64(1),
		at:    base.Add(time.Duration(n) * time.Second),
	}, cutoff)

	var after []int64
	r.forEach(func(tr recordedTrade) {
		after = append(after, tr.price.ToDecimal().IntPart())
	})
	wantAfter := []int64{int64(n - 3), int64(n - 2), int64(n - 1), int64(n)}
	if len(after) != len(wantAfter) {
		t.Fatalf("after eviction, forEach = %v, want %v", after, wantAfter)
	}
	for i := range wantAfter {
		if after[i] != wantAfter[i] {
			t.Fatalf("after eviction, forEach = %v, want %v", after, wantAfter)
		}
	}
}

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
