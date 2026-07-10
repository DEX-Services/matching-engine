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
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		// Async writer queues internally; an error here means the queue is full
		// or the context was cancelled. Phase 5's durable outbox handles retries.
		p.log.Error("kafka publish failed", "symbol", evt.Symbol, "seq", evt.SequenceNumber, "error", err)
	}
}

// Close shuts down the Kafka writer gracefully.
func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}
