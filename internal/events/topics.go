package events

// Kafka topic names used across the matching engine and its consumers.
const (
	// TopicEvents is the primary event stream (all order state changes + trades).
	TopicEvents = "matching-engine.events"

	// TopicTrades is a compacted topic of trade records only (for marketdata).
	TopicTrades = "matching-engine.trades"

	// TopicOutbox is the durable outbox topic: Postgres writer retries consume
	// from here instead of relying on an in-memory queue (Section 7 of spec).
	TopicOutbox = "matching-engine.outbox"
)
