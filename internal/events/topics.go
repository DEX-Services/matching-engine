package events

// Kafka topic names used across the matching engine and its consumers.
const (
	// TopicEvents is the primary event stream: every REAL USER order-state
	// change and trade. As of 2026-09-14 (part 2 of the MM/user event
	// isolation plan — see TopicMMEvents' doc comment), market-maker desk
	// events are routed to TopicMMEvents instead, so this topic's volume
	// now scales with real user activity only, not with how many MM desks
	// are running or how often they requote.
	TopicEvents = "matching-engine.events"

	// TopicMMEvents carries market-maker desk order-state-change events
	// (models.Order.IsMarketMaker == true) — split off from TopicEvents
	// 2026-09-14 because a single MM desk's routine quote-refresh churn
	// (cancel + replace its whole resting ladder roughly once per second,
	// per price level — bots/internal/strategy/marketmaker.go's OnTick) was
	// confirmed live to delay real users' own trades from reaching
	// order_history/fills by 30 seconds to several minutes: both traffic
	// classes shared one Kafka topic/partition/writer, so a busy desk's
	// churn queued ahead of (and behind) real user events indiscriminately.
	//
	// Trade events (models.EventTrade) and TRADE-carrying fills still go to
	// TopicEvents regardless of which side is a desk — a real fill/trade
	// always matters for history and settlement auditing; it's specifically
	// the high-frequency, mostly-never-filled OPEN/CANCELLED churn of a
	// desk's own resting quotes that has no such urgency and was flooding
	// the shared queue. See internal/matching/engine.go's publishEvent for
	// the exact routing rule.
	//
	// This topic's own writer (persistence.Writer, a second instance) can
	// run with more relaxed timing than the user-events writer without any
	// user-facing consequence — nobody is waiting on a desk's cancelled
	// quote to show up in their own Order History.
	TopicMMEvents = "matching-engine.mm-events"

	// TopicTrades is a compacted topic of trade records only (for marketdata).
	TopicTrades = "matching-engine.trades"

	// TopicOutbox is the durable outbox topic: Postgres writer retries consume
	// from here instead of relying on an in-memory queue (Section 7 of spec).
	TopicOutbox = "matching-engine.outbox"
)
