// Live streaming worker for the mongo-cdc-orderstats example.
//
// Consumes Mongo CDC events delivered via Kafka (the standard Debezium →
// Kafka bridge in production) and applies them through the per-customer Sum
// monoid into the same DynamoDB table the bootstrap binary populated. With
// WithDedup wired up, replays-after-crash are idempotent: a record arriving
// twice from Kafka after a worker restart is folded into the monoid exactly
// once.
//
// Locally:
//
//	# After bootstrap has populated the initial state:
//	docker compose up -d kafka dynamodb-local
//	go run ./examples/mongo-cdc-orderstats/cmd/worker
package main

import (
	"context"
	"log/slog"
	"os"

	orderstats "github.com/gallowaysoftware/murmur/examples/mongo-cdc-orderstats"
	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/murmur"
)

func main() {
	cfg := orderstats.Config{
		KafkaBrokers:  envOr("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:    envOr("KAFKA_TOPIC", "orders.cdc"),
		ConsumerGroup: envOr("CONSUMER_GROUP", "order_totals_worker"),
		DDBEndpoint:   os.Getenv("DDB_LOCAL_ENDPOINT"),
		DDBTable:      envOr("DDB_TABLE", "order_totals"),
		DDBRegion:     envOr("AWS_REGION", "us-east-1"),
		DDBDedupTable: os.Getenv("DDB_DEDUP_TABLE"), // optional
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	ctx := context.Background()
	pipe, store, deduper, err := orderstats.BuildLive(ctx, cfg)
	if err != nil {
		logger.Error("build", "err", err)
		os.Exit(2)
	}
	defer store.Close()
	if deduper != nil {
		defer deduper.Close()
	}

	rec := metrics.NewInMemory()
	opts := []streaming.RunOption{streaming.WithMetrics(rec)}
	if deduper != nil {
		opts = append(opts, streaming.WithDedup(deduper))
	}

	logger.Info("starting order_totals worker",
		"kafka", cfg.KafkaBrokers, "topic", cfg.KafkaTopic,
		"ddb_table", cfg.DDBTable, "dedup_table", cfg.DDBDedupTable)

	os.Exit(murmur.RunStreamingWorker(ctx, pipe, opts...))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
