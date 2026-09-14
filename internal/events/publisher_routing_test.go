package events

import (
	"testing"

	"github.com/dex/matching-engine/internal/models"
)

// TestIsMarketMakerEvent is a regression test for part 2 of the MM/user
// event isolation plan (see TopicMMEvents' doc comment): KafkaPublisher
// must route an order-lifecycle event to TopicMMEvents when and only when
// it carries a market-maker-origin order, and every TRADE event (which
// never carries evt.Order) must always go to TopicEvents regardless of
// which side of the trade was a desk.
func TestIsMarketMakerEvent(t *testing.T) {
	mmOrder := &models.Order{AccountID: "mm:BI2X:spot", IsMarketMaker: true}
	userOrder := &models.Order{AccountID: "DEXUSER_9", IsMarketMaker: false}

	cases := []struct {
		name string
		evt  *models.Event
		want bool
	}{
		{"MM order opened -> MM topic", &models.Event{Type: models.EventOrderOpen, Order: mmOrder}, true},
		{"MM order cancelled -> MM topic", &models.Event{Type: models.EventOrderCancelled, Order: mmOrder}, true},
		{"MM order filled -> MM topic", &models.Event{Type: models.EventOrderFilled, Order: mmOrder}, true},
		{"user order opened -> user topic", &models.Event{Type: models.EventOrderOpen, Order: userOrder}, false},
		{"user order filled -> user topic", &models.Event{Type: models.EventOrderFilled, Order: userOrder}, false},
		{"trade event (no Order at all) -> user topic, always", &models.Event{Type: models.EventTrade, Trade: &models.Trade{}}, false},
		{"funding event (no Order) -> user topic", &models.Event{Type: models.EventFunding, Funding: &models.Funding{}}, false},
		{"liquidation event (no Order) -> user topic", &models.Event{Type: models.EventLiquidation, Liquidation: &models.Liquidation{}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isMarketMakerEvent(tc.evt); got != tc.want {
				t.Errorf("isMarketMakerEvent() = %v, want %v", got, tc.want)
			}
		})
	}
}
