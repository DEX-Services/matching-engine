// Command clear-kafka is a DEV-ONLY tool that empties every retained
// message from events.TopicEvents ("matching-engine.events") — the single
// Kafka topic actually in live use by this platform (events.TopicTrades
// and events.TopicOutbox are declared but never published to or consumed
// from anywhere in this codebase; see internal/persistence/postgres.go's
// own comment on TopicOutbox — "previously promised but never actually"
// used).
//
// How: DeleteTopics is unsupported on this managed Kafka cluster (Aiven
// rejects it outright, confirmed empirically — the request fails with EOF
// even against a nonexistent topic, i.e. it's refused before Kafka would
// even look up whether the topic exists). The standard alternative for a
// managed cluster without topic-delete rights: set retention.ms to a very
// small value so the broker expires (deletes) every existing message on
// its own retention-cleanup cycle, then restore the topic's original
// retention.ms afterward so future messages aren't also expired
// immediately.
//
// This is IRREVERSIBLE and drops every retained message in the topic —
// appropriate only alongside a full Postgres truncate (cmd/clear-all-data)
// and Redis flush (cmd/clear-redis), since a partial clear (Kafka wiped but
// Postgres/order history kept, or vice versa) would just recreate the exact
// backlog-discarding class of incident this session already found and
// fixed once tonight.
//
// The broker's retention cleanup runs on its own schedule (log.retention.
// check.interval.ms, typically every few minutes) — this tool does not
// force an immediate purge, only shortens retention so the NEXT cleanup
// pass drops everything. Give it a few minutes after --apply before
// assuming the topic is actually empty; the writer's own "lag" figure in
// its 30s status log is the most reliable way to confirm (0 lag once the
// broker has caught up to a genuinely empty log).
//
// Usage:
//
//	go run ./cmd/clear-kafka          # dry run: reports the topic's current retention.ms
//	go run ./cmd/clear-kafka --apply  # sets retention.ms to 1000, waits, restores the original value
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/dex/matching-engine/internal/events"
	"github.com/joho/godotenv"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// shortRetentionMs is applied temporarily to force the broker to expire
// every existing message on its next retention-cleanup pass. 1 second: far
// shorter than any realistic message age in the topic, so everything
// currently retained qualifies for deletion on the next pass regardless of
// exactly when that pass runs.
const shortRetentionMs = "1000"

// defaultRetentionMs is restored after the purge. Aiven's own default for
// a topic created without an explicit retention.ms is 7 days; used here
// only as a fallback if the topic's current value can't be read for
// whatever reason (should not happen in practice — DescribeConfigs is read
// before any change is made).
const defaultRetentionMs = "604800000"

func newTransport() (*kafka.Transport, error) {
	tlsCfg, err := events.KafkaTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("tls config: %w", err)
	}
	return &kafka.Transport{
		TLS:  tlsCfg,
		SASL: plain.Mechanism{Username: os.Getenv("KAFKA_USER"), Password: os.Getenv("KAFKA_PASSWORD")},
		Dial: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
	}, nil
}

func brokerAddr() (string, error) {
	host := os.Getenv("KAFKA_HOST")
	port := os.Getenv("KAFKA_PORT")
	if host == "" || port == "" {
		return "", fmt.Errorf("KAFKA_HOST and KAFKA_PORT must be set")
	}
	return fmt.Sprintf("%s:%s", host, port), nil
}

func currentRetentionMs(ctx context.Context, client *kafka.Client, addr string) (string, error) {
	resp, err := client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{
		Addr: kafka.TCP(addr),
		Resources: []kafka.DescribeConfigRequestResource{
			{ResourceType: kafka.ResourceTypeTopic, ResourceName: events.TopicEvents, ConfigNames: []string{"retention.ms"}},
		},
	})
	if err != nil {
		return "", err
	}
	for _, res := range resp.Resources {
		for _, entry := range res.ConfigEntries {
			if entry.ConfigName == "retention.ms" {
				return entry.ConfigValue, nil
			}
		}
	}
	return "", fmt.Errorf("retention.ms not found in describe-configs response")
}

func setRetentionMs(ctx context.Context, client *kafka.Client, addr, value string) error {
	resp, err := client.AlterConfigs(ctx, &kafka.AlterConfigsRequest{
		Addr: kafka.TCP(addr),
		Resources: []kafka.AlterConfigRequestResource{
			{
				ResourceType: kafka.ResourceTypeTopic,
				ResourceName: events.TopicEvents,
				Configs:      []kafka.AlterConfigRequestConfig{{Name: "retention.ms", Value: value}},
			},
		},
	})
	if err != nil {
		return err
	}
	for _, rerr := range resp.Errors {
		if rerr != nil {
			return rerr
		}
	}
	return nil
}

func main() {
	_ = godotenv.Load()
	apply := len(os.Args) > 1 && os.Args[1] == "--apply"

	addr, err := brokerAddr()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	transport, err := newTransport()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	client := &kafka.Client{Addr: kafka.TCP(addr), Timeout: 15 * time.Second, Transport: transport}
	ctx := context.Background()

	original, err := currentRetentionMs(ctx, client, addr)
	if err != nil {
		fmt.Println("describe configs error:", err)
		os.Exit(1)
	}
	fmt.Printf("Topic %q current retention.ms: %s\n", events.TopicEvents, original)

	if !apply {
		fmt.Println("\nDry run only — nothing was touched.")
		fmt.Println("Re-run with --apply to temporarily drop retention.ms so the broker expires")
		fmt.Println("every retained message, then restore the original retention.ms.")
		fmt.Println("This drops every retained message — only appropriate alongside a full")
		fmt.Println("Postgres truncate (cmd/clear-all-data) and Redis flush (cmd/clear-redis).")
		return
	}

	restoreTo := original
	if restoreTo == "" {
		restoreTo = defaultRetentionMs
	}

	if err := setRetentionMs(ctx, client, addr, shortRetentionMs); err != nil {
		fmt.Println("set short retention error:", err)
		os.Exit(1)
	}
	fmt.Printf("Set retention.ms to %s. Waiting 10s for the broker to register the change...\n", shortRetentionMs)
	time.Sleep(10 * time.Second)

	if err := setRetentionMs(ctx, client, addr, restoreTo); err != nil {
		fmt.Printf("WARNING: restore retention error: %v\n", err)
		fmt.Printf("retention.ms may still be %s — set it back to %s manually if needed.\n", shortRetentionMs, restoreTo)
		os.Exit(1)
	}
	fmt.Printf("Restored retention.ms to %s.\n\n", restoreTo)
	fmt.Println("The broker's own retention-cleanup cycle (every few minutes) will finish")
	fmt.Println("expiring the old messages that were retained when this ran. Check the")
	fmt.Println("matching-engine's writer status log (\"lag\") after a few minutes to confirm")
	fmt.Println("the topic is actually empty before assuming this is done.")
}
