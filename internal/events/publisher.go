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
type KafkaPublisher struct {
	writer *kafka.Writer
	sub    <-chan *models.Event
	log    *slog.Logger

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

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{fmt.Sprintf("%s:%s", host, port)},
		Topic:        TopicEvents,
		Dialer:       dialer,
		Balancer:     &kafka.Hash{}, // route by symbol key for ordering
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true, // fire-and-forget; durability is Kafka's job
		RequiredAcks: int(kafka.RequireOne),
		ErrorLogger:  kafka.LoggerFunc(func(msg string, a ...interface{}) {
			slog.Error("kafka writer error", "msg", fmt.Sprintf(msg, a...))
		}),
	})

	return &KafkaPublisher{
		writer: writer,
		sub:    bus.Subscribe(50_000),
		log:    slog.Default(),
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

func (p *KafkaPublisher) publish(ctx context.Context, evt *models.Event) {
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
	if err := p.writer.WriteMessages(publishCtx, msg); err != nil {
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
	return p.writer.Close()
}
