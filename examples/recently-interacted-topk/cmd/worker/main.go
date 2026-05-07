// Streaming worker binary for the recently-interacted-topk example.
//
// Reads interaction events from a Kafka topic (typically internal application
// events) and folds them into the shared TopK sketch in DynamoDB. Pair with
// the cmd/lambda binary that consumes a Kinesis stream — both binaries write
// to the same DDB row, so the merged TopK reflects interactions from BOTH
// channels.
//
// In production this binary runs as an ECS Fargate service. Locally:
//
//	docker compose up -d kafka dynamodb-local
//	export KAFKA_BROKERS=localhost:9092
//	export KAFKA_TOPIC=interactions
//	export DDB_ENDPOINT=http://localhost:8000
//	go run ./examples/recently-interacted-topk/cmd/worker
package main

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"time"

	example "github.com/gallowaysoftware/murmur/examples/recently-interacted-topk"
	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/murmur"
)

func main() {
	cfg := example.Config{
		KafkaBrokers:    envOr("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:      envOr("KAFKA_TOPIC", "interactions"),
		ConsumerGroup:   envOr("CONSUMER_GROUP", "recently_interacted_worker"),
		DDBEndpoint:     os.Getenv("DDB_ENDPOINT"),
		DDBTable:        envOr("DDB_TABLE", "recently_interacted"),
		DDBRegion:       envOr("AWS_REGION", "us-east-1"),
		DDBDedupTable:   os.Getenv("DDB_DEDUP_TABLE"),
		DedupTTL:        24 * time.Hour,
		K:               envU32("TOPK_K", 32),
		WindowRetention: 30 * 24 * time.Hour,
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)
	logger.Info("starting recently-interacted Kafka worker",
		"kafka", cfg.KafkaBrokers,
		"topic", cfg.KafkaTopic,
		"ddb_table", cfg.DDBTable,
		"dedup", cfg.DDBDedupTable != "",
	)

	ctx := context.Background()
	pipe, store, deduper, err := example.Build(ctx, cfg)
	if err != nil {
		logger.Error("build pipeline", "err", err)
		os.Exit(2)
	}
	defer func() { _ = store.Close() }()
	if deduper != nil {
		defer func() { _ = deduper.Close() }()
	}

	pipe, err = example.AttachKafkaSource(pipe, cfg)
	if err != nil {
		logger.Error("attach kafka source", "err", err)
		os.Exit(2)
	}

	rec := metrics.NewInMemory()
	opts := []streaming.RunOption{
		streaming.WithMetrics(rec),
		streaming.WithMaxAttempts(5),
	}
	if deduper != nil {
		opts = append(opts, streaming.WithDedup(deduper))
	}

	os.Exit(murmur.RunStreamingWorker(ctx, pipe, opts...))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envU32(key string, fallback uint32) uint32 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil || n == 0 {
		return fallback
	}
	return uint32(n)
}
