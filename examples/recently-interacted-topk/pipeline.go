// Package recentlyinteracted defines a TopK "recently interacted" pipeline fed
// by TWO sources at once: a Segment-style Kinesis stream (consumed via an AWS
// Lambda trigger) and an internal Kafka topic (consumed by a long-lived ECS
// streaming worker). Both drivers share a single pipeline definition and a
// single DynamoDB-backed TopK state — the result is a unified Top-N of the
// most-interacted entities across the two channels, queryable through the
// auto-generated query service.
//
// Why two sources?
//
//   - Kinesis is the canonical destination for managed analytics ingest
//     (Segment, AWS-native event buses); Lambda's Kinesis trigger is the
//     operationally cheap path for it (per-shard fan-out, BatchItemFailures
//     for partial-batch retry).
//   - Kafka is the canonical destination for internal service events
//     (CDC streams, application events). A long-running ECS worker with
//     franz-go is the right fit — it's cheap to keep open and Kafka offset
//     commits behave nicely with at-least-once dedup.
//
// Murmur's pipeline DSL is execution-mode-agnostic, so the same TopK
// definition (Misra-Gries summary, daily windowing, DDB BytesStore) can run
// behind both at once with no separate query-time merge logic — both writers
// MergeUpdate against the same DDB row, and the Misra-Gries Combine collapses
// duplicates correctly across sources.
package recentlyinteracted

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/topk"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	mkafka "github.com/gallowaysoftware/murmur/pkg/source/kafka"
	"github.com/gallowaysoftware/murmur/pkg/state"
	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

// Interaction is the cross-source event shape. Both Kinesis (Segment-emitted)
// and Kafka (internal) producers normalize their payloads to this shape — in
// production you'd usually keep a separate per-source struct and project both
// into a common Interaction in your decoder. For the example we keep it
// simple.
type Interaction struct {
	// EntityID is the thing being interacted with (a product ID, a content
	// ID, etc.). The TopK sketch ranks entities by interaction frequency.
	EntityID string `json:"entity_id"`

	// UserID is the actor; carried through for diagnostics but not used by
	// the aggregation key.
	UserID string `json:"user_id"`

	// Source is "kinesis" or "kafka" — useful for breaking down "where the
	// interaction came from" in downstream analytics. Not part of the
	// aggregation key.
	Source string `json:"source,omitempty"`
}

// Config bundles deployment-time settings. The Lambda binary uses a subset
// (DDB + dedup + TopK params); the ECS worker also needs Kafka settings.
type Config struct {
	// DynamoDB
	DDBEndpoint string // empty for real AWS; "http://localhost:8000" for dynamodb-local
	DDBTable    string // TopK byte-store table
	DDBRegion   string

	// Optional dedup table, shared across both drivers. Strongly recommended
	// in production: Lambda BatchItemFailures may redeliver records adjacent
	// to a failure, and Kafka rebalances may re-emit unacked records.
	DDBDedupTable string
	DedupTTL      time.Duration

	// Kafka (live source — used only by the ECS worker binary)
	KafkaBrokers  string // comma-separated
	KafkaTopic    string
	ConsumerGroup string

	// TopK parameters
	K               uint32        // sketch size (memory ~K; default 32 if zero)
	WindowRetention time.Duration // daily-bucket retention (default 30d)
}

// PipelineName is the canonical pipeline identifier — surfaces in metrics,
// the admin UI, and the auto-generated query service.
const PipelineName = "recently_interacted"

// Build constructs the shared pipeline. The driver-specific source is wired
// in by the caller: the Lambda binary leaves Source nil (Lambda owns
// polling), the Kafka worker binary calls AttachKafkaSource on the returned
// pipeline.
//
// Returns the pipeline, the underlying state store (so the caller can Close
// it on shutdown), and an optional Deduper (nil when DDBDedupTable is empty).
func Build(ctx context.Context, cfg Config) (*pipeline.Pipeline[Interaction, []byte], state.Store[[]byte], state.Deduper, error) {
	if cfg.DDBTable == "" {
		return nil, nil, nil, errors.New("recently-interacted: DDBTable is required")
	}
	k := cfg.K
	if k == 0 {
		k = topk.DefaultK
	}
	retention := cfg.WindowRetention
	if retention == 0 {
		retention = 30 * 24 * time.Hour
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.DDBRegion))
	if err != nil {
		return nil, nil, nil, err
	}
	if cfg.DDBEndpoint != "" {
		awsCfg.Credentials = credentials.NewStaticCredentialsProvider("test", "test", "")
	}
	client := awsddb.NewFromConfig(awsCfg, func(o *awsddb.Options) {
		if cfg.DDBEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.DDBEndpoint)
		}
	})

	mon := topk.New(k)
	store := mddb.NewBytesStore(client, cfg.DDBTable, mon)

	var deduper state.Deduper
	if cfg.DDBDedupTable != "" {
		ttl := cfg.DedupTTL
		if ttl == 0 {
			ttl = 24 * time.Hour
		}
		deduper = mddb.NewDeduper(client, cfg.DDBDedupTable, ttl)
	}

	window := windowed.Daily(retention)

	// Use the lower-level pipeline builder rather than the murmur.TopN preset:
	// our TopN preset hard-codes a count of 1 per event, which IS what we
	// want here, but going through the lower-level API keeps the example
	// transparent about the wiring (the Misra-Gries SingleN(k, entity, 1)
	// lift is exactly what murmur.TopN.Build does internally).
	pipe := pipeline.NewPipeline[Interaction, []byte](PipelineName).
		Key(func(Interaction) string { return "global" }). // single Top-N over all entities
		Value(func(e Interaction) []byte { return topk.SingleN(k, e.EntityID, 1) }).
		Aggregate(mon, window).
		StoreIn(store)

	return pipe, store, deduper, nil
}

// AttachKafkaSource wires a franz-go Kafka source into pipe. Used by the ECS
// worker binary; the Lambda binary leaves Source nil because Lambda owns
// shard polling.
//
// Returns the pipeline (chained for fluent style) so the caller can pass it
// directly to streaming.Run.
func AttachKafkaSource(pipe *pipeline.Pipeline[Interaction, []byte], cfg Config) (*pipeline.Pipeline[Interaction, []byte], error) {
	if cfg.KafkaBrokers == "" || cfg.KafkaTopic == "" || cfg.ConsumerGroup == "" {
		return nil, errors.New("recently-interacted: KafkaBrokers, KafkaTopic, ConsumerGroup are required")
	}
	src, err := mkafka.NewSource(mkafka.Config[Interaction]{
		Brokers:       splitAndTrim(cfg.KafkaBrokers, ","),
		Topic:         cfg.KafkaTopic,
		ConsumerGroup: cfg.ConsumerGroup,
		Decode:        mkafka.JSONDecoder[Interaction](),
	})
	if err != nil {
		return nil, err
	}
	return pipe.From(src), nil
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
