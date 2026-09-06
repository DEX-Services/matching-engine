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

// KafkaPublisher consumes events from the Bus and writes them to Kafka.
// It runs in its own goroutine and never blocks the matching goroutines.
//
// Aiven Kafka requires SASL/PLAIN over TLS. Credentials come from the
// environment variables set in .env.
type KafkaPublisher struct {
	writer *kafka.Writer
	sub    <-chan *models.Event
	log    *slog.Logger
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
	// logged event instead of freezing every symbol.
	//
	// NOTE: unlike the comment below claims, TopicOutbox is not actually
	// wired up anywhere in this codebase (defined in topics.go, referenced
	// only in comments here and in persistence/writer.go) — a message
	// dropped by this timeout is NOT currently retried or recovered by
	// anything. This trades a platform-wide freeze for a real (if rare,
	// bounded to 3s of sustained broker unavailability) persistence gap;
	// the postgres-writer's own resilience (idempotent upserts, its own
	// consumer group) is unaffected since it never receives a message this
	// never reaches Kafka in the first place. Building the outbox consumer
	// or otherwise closing this gap is a separate, real follow-up.
	publishCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := p.writer.WriteMessages(publishCtx, msg); err != nil {
		// Async writer queues internally; an error here means the queue is full
		// or the context was cancelled — see this function's doc comment above
		// on why a dropped message here is not currently recovered.
		p.log.Error("kafka publish failed", "symbol", evt.Symbol, "seq", evt.SequenceNumber, "error", err)
	}
}

// Close shuts down the Kafka writer gracefully.
func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}
