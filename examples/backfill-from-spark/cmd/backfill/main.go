// Command backfill drives a Murmur counter pipeline from Spark-
// aggregated S3 JSON-Lines into a DynamoDB Sum-monoid store. It's the
// canonical "snapshot then stream" bootstrap step: a Spark job
// pre-aggregates raw events to hourly summaries, lands them in S3,
// and this binary scans the prefix and folds every row into the same
// pipeline a live Kafka/Kinesis worker would feed.
//
// Run it once before flipping the live worker to a fresh DDB table,
// or repeatedly during a 40-day backfill window; the StableEventID
// extractor in package backfill keeps re-runs idempotent.
//
// Required flags:
//
//	-bucket    S3 bucket holding the Spark output
//	-prefix    S3 key prefix to scan (e.g. counters/bot_interaction/)
//	-table     DynamoDB table for the Sum store (the pipeline's primary state)
//	-name      Pipeline name (used for metrics + log lines)
//
// Optional:
//
//	-concurrency  Parallel S3 fetches (default 8)
//	-region       AWS region (default from environment)
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/gallowaysoftware/murmur/examples/backfill-from-spark"
	"github.com/gallowaysoftware/murmur/pkg/exec/bootstrap"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot/s3"
	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

func main() {
	os.Exit(run())
}

// run wraps the body so deferred cleanups (signal-NotifyContext cancel)
// fire before os.Exit. Returns a process exit code.
func run() int {
	var (
		bucket      = flag.String("bucket", "", "S3 bucket holding the Spark output (required)")
		prefix      = flag.String("prefix", "", "S3 key prefix to scan (required)")
		table       = flag.String("table", "", "DynamoDB table backing the Sum store (required)")
		name        = flag.String("name", "", "Pipeline name, used for metrics + logs (required)")
		concurrency = flag.Int("concurrency", 8, "Parallel S3 fetches")
		region      = flag.String("region", "", "AWS region (default from environment)")
		retention   = flag.Duration("retention", 40*24*time.Hour, "Daily-bucket retention; pick max(trailing-window) + slack")
	)
	flag.Parse()
	if *bucket == "" || *prefix == "" || *table == "" || *name == "" {
		flag.Usage()
		return 2
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	awsOpts := []func(*awsconfig.LoadOptions) error{}
	if *region != "" {
		awsOpts = append(awsOpts, awsconfig.WithRegion(*region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsOpts...)
	if err != nil {
		slog.Error("load aws config", "err", err)
		return 1
	}

	s3Client := awss3.NewFromConfig(awsCfg)
	ddbClient := awsddb.NewFromConfig(awsCfg)
	store := mddb.NewInt64SumStore(ddbClient, *table)

	// One pipeline definition; the same shape a live worker would use,
	// minus the live source.
	window := windowed.Daily(*retention)
	pipe := pipeline.NewPipeline[backfill.CountEvent, int64](*name).
		Key(func(e backfill.CountEvent) string { return e.EntityID }).
		Value(func(e backfill.CountEvent) int64 { return e.Count }).
		Aggregate(core.Sum[int64](), window).
		StoreIn(store)

	src, err := s3.NewSource(s3.Config[backfill.CountEvent]{
		Client:      s3Client,
		Bucket:      *bucket,
		Prefix:      *prefix,
		Concurrency: *concurrency,
		Decode:      backfill.DecodeJSONL,
		EventID:     backfill.StableEventID,
		EventTime:   func(e backfill.CountEvent) time.Time { return e.OccurredAt },
		OnDecodeError: func(key string, lineNum int, _ []byte, err error) {
			slog.Warn("decode error", "key", key, "line", lineNum, "err", err)
		},
	})
	if err != nil {
		slog.Error("new s3 source", "err", err)
		return 1
	}

	slog.Info("bootstrap starting",
		"pipeline", *name,
		"bucket", *bucket,
		"prefix", *prefix,
		"concurrency", *concurrency,
		"retention", *retention,
	)

	token, err := bootstrap.Run(ctx, pipe, src)
	if err != nil {
		slog.Error("bootstrap.Run", "err", err)
		return 1
	}

	slog.Info("bootstrap complete",
		"pipeline", *name,
		"handoff_token_len", len(token),
	)
	if _, err := fmt.Fprintln(os.Stdout, "ok"); err != nil {
		// Stdout write failure isn't worth a non-zero exit (the
		// bootstrap itself succeeded), but errcheck wants explicit
		// acknowledgement that the result was considered.
		slog.Warn("stdout write", "err", err)
	}
	return 0
}
