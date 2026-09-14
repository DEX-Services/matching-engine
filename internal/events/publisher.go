package events

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/dex/matching-engine/internal/models"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// KafkaTLSConfig builds the TLS config for Kafka connections, trusting the
// Aiven CA certificate if KAFKA_CA_CERT_PATH (or the default kafka-ca.pem)
// is present. Falls back to the system root pool otherwise.
func KafkaTLSConfig() (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

	path := os.Getenv("KAFKA_CA_CERT_PATH")
	if path == "" {
		path = "kafka-ca.pem"
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, fmt.Errorf("reading kafka CA cert: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no valid certificates found in %s", path)
	}
	cfg.RootCAs = pool
	return cfg, nil
}

// Outbox is the durable fallback a KafkaPublisher writes to when a bounded
// publish attempt fails. Declared here (rather than importing package
// persistence, which already imports events — that would cycle) so
// persistence.OutboxWriter can satisfy it without either package depending
// on the other beyond this narrow interface.
type Outbox interface {
	Write(ctx context.Context, evt *models.Event) error
}

// KafkaPublisher consumes events from the Bus and writes them to Kafka.
// It runs in its own goroutine and never blocks the matching goroutines.
//
// Aiven Kafka requires SASL/PLAIN over TLS. Credentials come from the
// environment variables set in .env.
//
// Two underlying kafka.Writer instances, not one writer with a per-message
// topic override: routing a market-maker desk's churn to a physically
// separate writer means a backlog/backpressure event on the MM topic's
// internal batching queue can never delay or block the user-events
// writer's own queue — see mmWriter's doc comment for the full routing
// rationale.
type KafkaPublisher struct {
	writer   *kafka.Writer // TopicEvents: real user order events + all trades
	mmWriter *kafka.Writer // TopicMMEvents: market-maker desk order events only
	sub      <-chan *models.Event
	log      *slog.Logger

	// outbox is set via SetOutbox once Postgres is available (Kafka is
	// constructed before Postgres in main.go's boot order — see that call
	// site). nil until then, and nil entirely when Postgres isn't
	// configured; publish() falls back to log-only in either case, same as
	// before this existed.
	outbox Outbox
}

// SetOutbox wires a durable fallback for publish failures. Safe to call
// once, after construction, from the same goroutine that starts Run — there
// is no concurrent access to outbox before Run's loop begins reading it.
func (p *KafkaPublisher) SetOutbox(o Outbox) {
	p.outbox = o
}

// NewKafkaPublisher constructs a publisher connected to the Aiven Kafka cluster.
func NewKafkaPublisher(bus *Bus) (*KafkaPublisher, error) {
	host := os.Getenv("KAFKA_HOST")
	port := os.Getenv("KAFKA_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("KAFKA_HOST and KAFKA_PORT must be set")
	}

	tlsCfg, err := KafkaTLSConfig()
	if err != nil {
		return nil, err
	}

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS:       tlsCfg,
		SASLMechanism: plain.Mechanism{
			Username: os.Getenv("KAFKA_USER"),
			Password: os.Getenv("KAFKA_PASSWORD"),
		},
	}

	newWriter := func(topic string) *kafka.Writer {
		return kafka.NewWriter(kafka.WriterConfig{
			Brokers:      []string{fmt.Sprintf("%s:%s", host, port)},
			Topic:        topic,
			Dialer:       dialer,
			Balancer:     &kafka.Hash{}, // route by symbol key for ordering
			BatchSize:    100,
			BatchTimeout: 10 * time.Millisecond,
			Async:        true, // fire-and-forget; durability is Kafka's job
			RequiredAcks: int(kafka.RequireOne),
			ErrorLogger: kafka.LoggerFunc(func(msg string, a ...interface{}) {
				slog.Error("kafka writer error", "topic", topic, "msg", fmt.Sprintf(msg, a...))
			}),
		})
	}

	return &KafkaPublisher{
		writer:   newWriter(TopicEvents),
		mmWriter: newWriter(TopicMMEvents),
		sub:      bus.Subscribe(50_000),
		log:      slog.Default(),
	}, nil
}

// Run starts the publish loop. Call in a dedicated goroutine.
// It exits when ctx is cancelled or the subscription channel is closed.
func (p *KafkaPublisher) Run(ctx context.Context) {
	for {
		select {
		case evt, ok := <-p.sub:
			if !ok {
				return
			}
			p.publish(ctx, evt)
		case <-ctx.Done():
			return
		}
	}
}

// isMarketMakerEvent reports whether evt should route to TopicMMEvents
// instead of TopicEvents — see TopicMMEvents' doc comment for the full
// rationale. Only order-lifecycle events (open/partial/filled/cancelled/
// rejected) carry evt.Order; a TRADE event carries evt.Trade instead (with
// only maker/taker order IDs, not the full Order), so this is naturally
// false for every trade regardless of which side is a desk — a real fill
// always goes to the user topic for settlement/audit purposes.
func isMarketMakerEvent(evt *models.Event) bool {
	return evt.Order != nil && evt.Order.IsMarketMaker
}

// shouldPublishMarketMakerEvent reports whether a market-maker-origin event
// is worth persisting at all. Requested 2026-09-14: a desk's placed/
// cancelled/replaced quotes (ORDER_ACCEPTED/OPEN/PARTIALLY_FILLED/CANCELLED/
// REJECTED/EXPIRED) are pure requoting churn — a desk cancels and replaces
// its resting quotes continuously just to track the price, so this dwarfs
// real user traffic in volume while carrying no information anyone needs:
// nobody debugs a live incident from a bot's routine quote churn, and the
// live order book state itself lives in the engine's in-memory book, not in
// Kafka/Postgres, so dropping these here has no effect on trading, matching,
// balances, or what's rendered to users. Only ORDER_FILLED (the desk's order
// fully filled, i.e. a real trade happened) is published: that IS real
// trading history and must be durable for settlement/audit/PnL, same as any
// user's fill.
//
// Deliberately not batching filled MM events either (each is published
// immediately, same as before) — batching was considered and rejected: fill
// volume is low relative to the churn being dropped here, so there is little
// per-message overhead left to amortize, while batching would add both
// staleness (fills invisible in history until the next flush) and a crash
// window (an in-memory batch lost before it flushes). If this needs
// revisiting, see the conversation from 2026-09-14 for the reasoning.
//
// User-origin events are entirely unaffected by this function — it is only
// ever consulted for events where isMarketMakerEvent(evt) is already true.
func shouldPublishMarketMakerEvent(evt *models.Event) bool {
	return evt.Type == models.EventOrderFilled
}

func (p *KafkaPublisher) publish(ctx context.Context, evt *models.Event) {
	if isMarketMakerEvent(evt) && !shouldPublishMarketMakerEvent(evt) {
		return
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		p.log.Error("failed to marshal event", "error", err)
		return
	}
	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("%s-%s-%d", evt.Symbol, evt.Market, evt.SequenceNumber)),
		Value: payload,
		Time:  time.Now(),
	}
	writer := p.writer
	if isMarketMakerEvent(evt) {
		writer = p.mmWriter
	}
	// Even with Async:true, kafka-go's WriteMessages blocks once its internal
	// outstanding-message queue is full — it only fires-and-forgets the
	// broker ACK, not the enqueue itself. Passing Run's own long-lived ctx
	// here (cancelled only on shutdown) meant a slow/unreachable broker (a
	// real, observed failure mode against the hosted Aiven cluster: repeated
	// "context deadline exceeded" / "i/o timeout" writing to
	// matching-engine.events) could block this call indefinitely. That stalls
	// this loop, which stops draining Run's subscriber channel, which fills
	// it (the channel is a generous but finite 50,000), and once it's full
	// Bus.Publish — called synchronously from the MATCHING goroutine for
	// every order/trade — blocks too: a slow Kafka broker froze trading
	// platform-wide, directly contradicting this type's own doc comment
	// ("never blocks the matching goroutines"). Bounding this call's context
	// restores that guarantee: a stuck broker now degrades to a dropped,
	// logged event instead of freezing every symbol — and outbox (below)
	// keeps "dropped" from meaning "lost".
	publishCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := writer.WriteMessages(publishCtx, msg); err != nil {
		// Async writer queues internally; an error here means the queue is
		// full or the context was cancelled — Kafka never actually received
		// this event. Fall back to the durable outbox (persistence.OutboxWriter,
		// wired in via SetOutbox once Postgres is up) so it still reaches
		// order history / fills / PnL once OutboxSweeper drains it, instead
		// of being lost the moment this log line is written.
		p.log.Error("kafka publish failed", "symbol", evt.Symbol, "seq", evt.SequenceNumber, "error", err)
		if p.outbox != nil {
			if obErr := p.outbox.Write(ctx, evt); obErr != nil {
				p.log.Error("outbox fallback also failed; event lost", "symbol", evt.Symbol, "seq", evt.SequenceNumber, "error", obErr)
			}
		}
	}
}

// Close shuts down the Kafka writer gracefully.
func (p *KafkaPublisher) Close() error {
	err := p.writer.Close()
	if mmErr := p.mmWriter.Close(); mmErr != nil && err == nil {
		err = mmErr
	}
	return err
}
