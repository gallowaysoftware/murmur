// Package projector is the runnable Pattern B reference implementation
// from doc/search-integration.md. It tails a Murmur counter table via
// DynamoDB Streams and projects bucket transitions into an OpenSearch
// index.
//
// Architecture (see doc/search-integration.md "Pattern B" for the full
// treatment):
//
//	Murmur counter pipeline → DynamoDB → DDB Streams → THIS Lambda → OpenSearch
//
// The projector decides per-record:
//
//   - decode old + new image → log10 buckets via pkg/projection
//   - if bucket changed: emit OpenSearch partial-update for the popularity_bucket field
//   - otherwise: drop (this is the whole point — index update rate is
//     logarithmic in counter magnitude, not linear in event rate)
//
// Hysteresis-band wrapping (also from pkg/projection) is configurable:
// for documents oscillating around a boundary, set a 10–20% band to
// suppress flap reindexing.
//
// The Lambda is wired against Murmur's pkg/exec/lambda/dynamodbstreams
// runtime — same retry/dedup/BatchItemFailures shape, just a different
// downstream "store" (the OpenSearch projector instead of an aggregating
// MergeUpdate).
package projector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/aws/aws-lambda-go/events"

	"github.com/gallowaysoftware/murmur/pkg/projection"
)

// Config is the projector's deployment-time configuration. Pin these via
// environment variables in main.
type Config struct {
	// Index is the OpenSearch index name (the projector writes
	// partial-updates with the popularity_bucket field).
	Index string

	// Field is the document field name to update with the bucket. Default
	// "popularity_bucket". Override when projecting multiple counters into
	// the same index ("post_likes_bucket", "post_views_bucket", ...).
	Field string

	// Hysteresis, when > 0, wraps the bucket function with the named
	// hysteresis band to suppress oscillation around bucket boundaries.
	// Typical values: 0.05–0.20. See pkg/projection.HysteresisBucket.
	//
	// Hysteresis requires the projector to remember the previous bucket
	// per entity. We don't currently persist this state across Lambda
	// invocations — for the prototype, the prev-bucket is derived from
	// the OldImage's value (so the band suppresses flap WITHIN a single
	// invocation but not across them). For full hysteresis spanning
	// invocations, persist (entity, prev-bucket) in a small DDB table
	// and look it up in the projector. Marked as future work.
	Hysteresis float64
}

// Stats reports projector activity. The Lambda runtime can publish these
// to CloudWatch via metrics.Recorder; tests assert on them directly.
type Stats struct {
	Decoded     atomic.Int64 // records successfully decoded
	Skipped     atomic.Int64 // bucket unchanged → no reindex
	Indexed     atomic.Int64 // bucket changed → reindex emitted
	IndexErrors atomic.Int64 // OpenSearch update returned a non-2xx
	DecodeErrs  atomic.Int64 // missing pk / bad attribute / etc.
}

// IndexClient abstracts the OpenSearch UpdateDoc surface so the projector
// is testable without the live cluster. Production: opensearchapi.Client.
// Tests: a fake that records every Update call.
type IndexClient interface {
	UpdateDoc(ctx context.Context, index, docID string, fields map[string]any) error
}

// Projector is the per-record decision engine. Held as a struct so the
// Lambda main can wire its dependencies once and reuse them across
// invocations.
type Projector struct {
	cfg    Config
	client IndexClient
	bf     projection.BucketFn
	hb     projection.HysteresisBucket
	stats  *Stats
}

// New constructs a Projector. The bucket function is fixed to
// projection.LogBucket — the right choice for popularity-counter
// projection, and the canonical pattern in doc/search-integration.md.
// For non-popularity counters with a different shape (linear bands,
// manual breakpoints), wire the projector against pkg/projection.LinearBucket
// or ManualBucket directly via NewWithBucket.
func New(cfg Config, client IndexClient) *Projector {
	return NewWithBucket(cfg, client, projection.LogBucket)
}

// NewWithBucket constructs a Projector with a caller-supplied bucket
// function. Use when LogBucket isn't the right shape for your counter.
func NewWithBucket(cfg Config, client IndexClient, bf projection.BucketFn) *Projector {
	if cfg.Field == "" {
		cfg.Field = "popularity_bucket"
	}
	return &Projector{
		cfg:    cfg,
		client: client,
		bf:     bf,
		hb: projection.HysteresisBucket{
			Inner: bf,
			Band:  cfg.Hysteresis,
		},
		stats: &Stats{},
	}
}

// Stats returns a pointer to the live counters. Safe for concurrent reads.
func (p *Projector) Stats() *Stats { return p.stats }

// Handle processes one DDB Streams record. Returns nil if the record was
// handled (whether or not it triggered an index update); returns an error
// only when the OpenSearch update itself failed and Lambda should add
// the record to BatchItemFailures so it gets redelivered.
func (p *Projector) Handle(ctx context.Context, rec *events.DynamoDBEventRecord) error {
	pk, ok := rec.Change.Keys["pk"]
	if !ok {
		p.stats.DecodeErrs.Add(1)
		return nil
	}
	entity := pk.String()

	oldV, oldOK := decodeInt64(rec.Change.OldImage, "v")
	newV, newOK := decodeInt64(rec.Change.NewImage, "v")
	if !newOK {
		// Tombstone (REMOVE) — the row is gone. Project as bucket-0 so
		// the search index reflects the deletion. Could also issue a
		// delete-doc, but partial-update keeps the search doc alive
		// for slow-moving fields owned by another projector.
		newV = 0
	}
	p.stats.Decoded.Add(1)

	// Apply hysteresis if configured. Without prior state across
	// invocations, prev = bucket(oldV) — the band still suppresses
	// flap WITHIN a contiguous run of writes that all bounce around
	// the boundary, which is the common case in practice.
	prev := p.bf(oldV)
	if !oldOK {
		prev = -1 // first observation, hysteresis disabled for this record
	}
	newBucket := p.hb.Apply(prev, newV)

	if prev == newBucket {
		p.stats.Skipped.Add(1)
		return nil
	}

	if err := p.client.UpdateDoc(ctx, p.cfg.Index, entity, map[string]any{
		p.cfg.Field: newBucket,
	}); err != nil {
		p.stats.IndexErrors.Add(1)
		return fmt.Errorf("opensearch update %s/%s: %w", p.cfg.Index, entity, err)
	}
	p.stats.Indexed.Add(1)
	return nil
}

// HandleEvent is the convenience handler for a full SQS-style batch.
// Returns the BatchItemFailures slice for Lambda's response shape.
func (p *Projector) HandleEvent(ctx context.Context, evt events.DynamoDBEvent) []events.DynamoDBBatchItemFailure {
	var failures []events.DynamoDBBatchItemFailure
	for i := range evt.Records {
		if err := p.Handle(ctx, &evt.Records[i]); err != nil {
			failures = append(failures, events.DynamoDBBatchItemFailure{
				ItemIdentifier: evt.Records[i].EventID,
			})
		}
	}
	return failures
}

// decodeInt64 reads a numeric attribute from an AttributeValue map. The
// Murmur Int64SumStore writes the value as a NUMBER under attribute "v".
func decodeInt64(image map[string]events.DynamoDBAttributeValue, attr string) (int64, bool) {
	v, ok := image[attr]
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(v.Number(), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// MarshalJSON returns a JSON snapshot of the projector counters. Useful
// for the Lambda warmup path or as a debug endpoint when running locally.
func (s *Stats) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Decoded     int64 `json:"decoded"`
		Skipped     int64 `json:"skipped"`
		Indexed     int64 `json:"indexed"`
		IndexErrors int64 `json:"index_errors"`
		DecodeErrs  int64 `json:"decode_errors"`
	}{
		Decoded:     s.Decoded.Load(),
		Skipped:     s.Skipped.Load(),
		Indexed:     s.Indexed.Load(),
		IndexErrors: s.IndexErrors.Load(),
		DecodeErrs:  s.DecodeErrs.Load(),
	})
}

// ErrIndexClientRequired is returned when constructing without a client.
var ErrIndexClientRequired = errors.New("projector: IndexClient is required")
