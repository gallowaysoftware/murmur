// Package orderstats defines a Murmur pipeline that aggregates per-customer
// order totals from a Mongo collection (Bootstrap mode) and a Kafka CDC stream
// (Live mode). The same pipeline definition runs in both binaries — one
// imports it and runs bootstrap.Run, the other runs streaming.Run.
//
// In production the Kafka source carries Mongo Change Stream output piped
// through Debezium (or any equivalent CDC tool). The bootstrap captures a
// Change Stream resume token; the deployment system hands that token to the
// live consumer's StartAfter so the first record after handoff is the first
// CDC change since the bootstrap began. No gaps, no duplicates beyond the
// at-least-once dedup window.
//
// The pipeline DSL is execution-mode-agnostic; the runtimes (bootstrap.Run,
// streaming.Run) are what differ.
package orderstats

import (
	"context"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	mkafka "github.com/gallowaysoftware/murmur/pkg/source/kafka"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot/mongo"
	"github.com/gallowaysoftware/murmur/pkg/state"
	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

// Order is the shape stored in Mongo and emitted on the Kafka CDC stream. The
// example uses identical Go and BSON tags for both producers; in real CDC the
// Kafka payload is whatever Debezium serializes (typically JSON envelopes
// with `before` / `after` keys — your Decode would unwrap that).
type Order struct {
	ID         string `bson:"_id" json:"_id"`
	CustomerID string `bson:"customer_id" json:"customer_id"`
	Amount     int64  `bson:"amount" json:"amount"`
}

// Config bundles deployment-time settings shared across both binaries.
type Config struct {
	// Mongo
	MongoURI        string
	MongoDB         string
	MongoCollection string

	// Kafka (live CDC source)
	KafkaBrokers  string // comma-separated
	KafkaTopic    string
	ConsumerGroup string

	// DynamoDB (state)
	DDBEndpoint string
	DDBTable    string
	DDBRegion   string

	// Optional dedup table for at-least-once idempotency
	DDBDedupTable string
}

// BuildLive constructs the pipeline for the Kafka-driven streaming worker.
//
// Note: the murmur.Counter preset hard-codes Value(func(T) int64 { return 1 }),
// which is correct for "count events per key." For monetary totals we want
// Value(func(o Order) int64 { return o.Amount }) instead, so we drop down to
// the lower-level pipeline.NewPipeline builder. Same Sum monoid; different
// value extractor.
func BuildLive(ctx context.Context, cfg Config) (*pipeline.Pipeline[Order, int64], state.Store[int64], state.Deduper, error) {
	store, deduper, err := newDDB(ctx, cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	src, err := mkafka.NewSource(mkafka.Config[Order]{
		Brokers:       splitAndTrim(cfg.KafkaBrokers, ","),
		Topic:         cfg.KafkaTopic,
		ConsumerGroup: cfg.ConsumerGroup,
		Decode:        mkafka.JSONDecoder[Order](),
		// Use the upstream Mongo _id as the dedup key so re-deliveries of the
		// same logical change (Debezium retransmits, dual-write fixers, etc.)
		// fold exactly once even though they land on different Kafka offsets.
		EventID: func(o Order) string { return o.ID },
	})
	if err != nil {
		return nil, nil, nil, err
	}
	pipe := pipeline.NewPipeline[Order, int64]("order_totals").
		From(src).
		Key(func(o Order) string { return o.CustomerID }).
		Value(func(o Order) int64 { return o.Amount }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)
	return pipe, store, deduper, nil
}

// BuildBootstrap constructs the pipeline for the Mongo-collection-scan
// bootstrap binary. Returns the pipeline plus the SnapshotSource the
// bootstrap.Run driver consumes.
func BuildBootstrap(ctx context.Context, cfg Config) (*pipeline.Pipeline[Order, int64], *mongo.Source[Order], state.Store[int64], error) {
	store, _, err := newDDB(ctx, cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	src, err := mongo.NewSource(ctx, mongo.Config[Order]{
		URI:        cfg.MongoURI,
		Database:   cfg.MongoDB,
		Collection: cfg.MongoCollection,
		Decode:     mongo.BSONDecoder[Order](),
	})
	if err != nil {
		return nil, nil, nil, err
	}
	pipe := pipeline.NewPipeline[Order, int64]("order_totals").
		Key(func(o Order) string { return o.CustomerID }).
		Value(func(o Order) int64 { return o.Amount }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)
	return pipe, src, store, nil
}

func newDDB(ctx context.Context, cfg Config) (state.Store[int64], state.Deduper, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.DDBRegion))
	if err != nil {
		return nil, nil, err
	}
	if cfg.DDBEndpoint != "" {
		awsCfg.Credentials = credentials.NewStaticCredentialsProvider("test", "test", "")
	}
	client := awsddb.NewFromConfig(awsCfg, func(o *awsddb.Options) {
		if cfg.DDBEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.DDBEndpoint)
		}
	})
	store := mddb.NewInt64SumStore(client, cfg.DDBTable)
	var deduper state.Deduper
	if cfg.DDBDedupTable != "" {
		deduper = mddb.NewDeduper(client, cfg.DDBDedupTable, 24*time.Hour)
	}
	return store, deduper, nil
}

func splitAndTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
