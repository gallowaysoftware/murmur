// Bootstrap binary for the mongo-cdc-orderstats example.
//
// Scans a Mongo `orders` collection, captures a Change Stream resume token at
// the start, applies every document through the per-customer Sum monoid into
// DynamoDB, and prints the captured token to stdout when done. The deployment
// system hands that token to the live worker's StartAfter so the first CDC
// change observed by the live consumer is the first change since bootstrap
// began — no gaps, no duplicates beyond the at-least-once dedup window.
//
// In production this runs as a one-shot ECS Fargate task. Locally:
//
//	docker compose up -d mongo dynamodb-local
//	./scripts/init-mongo-replset.sh   # one-time, idempotent
//	make seed-ddb DDB_TABLE=order_totals
//	go run ./examples/mongo-cdc-orderstats/cmd/bootstrap
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"

	"github.com/gallowaysoftware/murmur/pkg/exec/bootstrap"
	"github.com/gallowaysoftware/murmur/pkg/metrics"

	orderstats "github.com/gallowaysoftware/murmur/examples/mongo-cdc-orderstats"
)

func main() {
	os.Exit(run())
}

func run() int {
	cfg := orderstats.Config{
		MongoURI:        envOr("MONGO_URI", "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"),
		MongoDB:         envOr("MONGO_DB", "shop"),
		MongoCollection: envOr("MONGO_COLLECTION", "orders"),
		DDBEndpoint:     os.Getenv("DDB_LOCAL_ENDPOINT"),
		DDBTable:        envOr("DDB_TABLE", "order_totals"),
		DDBRegion:       envOr("AWS_REGION", "us-east-1"),
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	ctx := context.Background()
	pipe, src, store, err := orderstats.BuildBootstrap(ctx, cfg)
	if err != nil {
		logger.Error("build", "err", err)
		return 2
	}
	defer func() { _ = src.Close() }()
	defer func() { _ = store.Close() }()

	rec := metrics.NewInMemory()

	logger.Info("bootstrap starting",
		"mongo", cfg.MongoURI, "db", cfg.MongoDB, "coll", cfg.MongoCollection,
		"ddb_table", cfg.DDBTable)

	token, err := bootstrap.Run(ctx, pipe, src, bootstrap.WithMetrics(rec))
	if err != nil {
		logger.Error("bootstrap.Run", "err", err)
		return 1
	}

	snap := rec.SnapshotOne("order_totals")
	logger.Info("bootstrap complete",
		"events_processed", snap.EventsProcessed, "errors", snap.Errors,
		"handoff_token_len", len(token))

	// Emit the resume token on stdout in two forms: raw bytes (for piping into
	// a deployment system) and hex (for humans / CloudWatch logs).
	fmt.Printf("%s\n", hex.EncodeToString(token))
	return 0
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
