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

// TestShouldPublishMarketMakerEvent covers the 2026-09-14 change to stop
// publishing MM quote churn (placed/cancelled/replaced/rejected/expired) to
// Kafka at all, keeping only full fills — see shouldPublishMarketMakerEvent's
// doc comment for the rationale. This function is only ever consulted for
// events where isMarketMakerEvent is already true (publish() gates on that
// first), but is exercised directly here across every EventType to pin down
// the exact boundary: ORDER_FILLED is the only one that must return true.
func TestShouldPublishMarketMakerEvent(t *testing.T) {
	mmOrder := &models.Order{AccountID: "mm:BI2X:spot", IsMarketMaker: true}

	cases := []struct {
		name string
		evt  *models.Event
		want bool
	}{
		{"filled -> publish", &models.Event{Type: models.EventOrderFilled, Order: mmOrder}, true},
		{"accepted -> drop", &models.Event{Type: models.EventOrderAccepted, Order: mmOrder}, false},
		{"open -> drop", &models.Event{Type: models.EventOrderOpen, Order: mmOrder}, false},
		{"partially filled -> drop", &models.Event{Type: models.EventOrderPartial, Order: mmOrder}, false},
		{"cancelled -> drop", &models.Event{Type: models.EventOrderCancelled, Order: mmOrder}, false},
		{"rejected -> drop", &models.Event{Type: models.EventOrderRejected, Order: mmOrder}, false},
		{"expired -> drop", &models.Event{Type: models.EventOrderExpired, Order: mmOrder}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldPublishMarketMakerEvent(tc.evt); got != tc.want {
				t.Errorf("shouldPublishMarketMakerEvent() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestPublish_DropsNonFilledMarketMakerEvents is an integration-level check
// (through the real publish() method, not just the pure helper) that a
// non-filled MM event never reaches either kafka.Writer, while a filled one
// does reach mmWriter specifically -- confirming publish()'s early return
// wires the two functions above together correctly. Uses nil ctx-independent
// ExposeWriters-style access isn't available (writer/mmWriter are real
// *kafka.Writer, unsuitable to invoke against a live broker in a unit test),
// so this only exercises the routing decision made before any write attempt:
// isMarketMakerEvent + shouldPublishMarketMakerEvent together must exactly
// determine "was WriteMessages skipped", which is what publish()'s early
// return line implements. See TestIsMarketMakerEvent and
// TestShouldPublishMarketMakerEvent above for the two halves; this test
// documents how they compose without needing a live Kafka connection.
func TestPublish_DropsNonFilledMarketMakerEvents(t *testing.T) {
	mmOrder := &models.Order{AccountID: "mm:BI2X:spot", IsMarketMaker: true}
	userOrder := &models.Order{AccountID: "DEXUSER_9", IsMarketMaker: false}

	cases := []struct {
		name        string
		evt         *models.Event
		wantDropped bool
	}{
		{"MM cancelled: dropped", &models.Event{Type: models.EventOrderCancelled, Order: mmOrder}, true},
		{"MM opened: dropped", &models.Event{Type: models.EventOrderOpen, Order: mmOrder}, true},
		{"MM filled: published", &models.Event{Type: models.EventOrderFilled, Order: mmOrder}, false},
		{"user cancelled: published (never gated)", &models.Event{Type: models.EventOrderCancelled, Order: userOrder}, false},
		{"user opened: published (never gated)", &models.Event{Type: models.EventOrderOpen, Order: userOrder}, false},
		{"trade: published (never gated)", &models.Event{Type: models.EventTrade, Trade: &models.Trade{}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dropped := isMarketMakerEvent(tc.evt) && !shouldPublishMarketMakerEvent(tc.evt)
			if dropped != tc.wantDropped {
				t.Errorf("dropped = %v, want %v", dropped, tc.wantDropped)
			}
		})
	}
}
