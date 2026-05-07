// Streaming worker for the page-view-counters example.
//
// Reads page-view events from Kafka, aggregates daily-bucketed counts per page-ID into
// DynamoDB, and (optionally) write-throughs to a Valkey cache.
//
// In production this binary runs as an ECS Fargate service. Locally:
//
//	docker compose up -d kafka dynamodb-local valkey
//	aws --endpoint-url=http://localhost:8000 dynamodb create-table \
//	    --table-name page_views \
//	    --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=N \
//	    --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
//	    --billing-mode PAY_PER_REQUEST
//	export DDB_LOCAL_ENDPOINT=http://localhost:8000
//	export VALKEY_ADDRESS=localhost:6379
//	export KAFKA_BROKERS=localhost:9092
//	go run ./examples/page-view-counters/cmd/worker
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	pageviews "github.com/gallowaysoftware/murmur/examples/page-view-counters"
	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
)

func main() {
	cfg := pageviews.Config{
		KafkaBrokers:    envOr("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:      envOr("KAFKA_TOPIC", "page_views"),
		ConsumerGroup:   envOr("CONSUMER_GROUP", "page_views_worker"),
		DDBEndpoint:     os.Getenv("DDB_LOCAL_ENDPOINT"),
		DDBTable:        envOr("DDB_TABLE", "page_views"),
		DDBRegion:       envOr("AWS_REGION", "us-east-1"),
		ValkeyAddress:   os.Getenv("VALKEY_ADDRESS"),
		ValkeyKeyPrefix: envOr("VALKEY_KEY_PREFIX", "page_views"),
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)
	logger.Info("starting page-view-counters worker",
		"kafka", cfg.KafkaBrokers,
		"topic", cfg.KafkaTopic,
		"ddb_table", cfg.DDBTable,
		"valkey", cfg.ValkeyAddress != "",
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pipe, store, cache, err := pageviews.Build(ctx, cfg, true)
	if err != nil {
		logger.Error("build pipeline", "err", err)
		os.Exit(2)
	}
	defer func() {
		if cache != nil {
			_ = cache.Close()
		}
		_ = store.Close()
	}()

	logger.Info("running streaming runtime")
	if err := streaming.Run(ctx, pipe); err != nil {
		logger.Error("streaming.Run returned", "err", err)
		os.Exit(1)
	}
	logger.Info("streaming runtime exited cleanly")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
