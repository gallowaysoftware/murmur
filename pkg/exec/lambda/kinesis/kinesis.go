// Package kinesis hosts a Murmur pipeline behind an AWS Lambda Kinesis trigger.
//
// Most Murmur pipelines run as a long-lived ECS Fargate service via
// pkg/exec/streaming.Run, polling the source themselves. Kinesis is a special
// case: AWS Lambda's Kinesis event-source mapping is the operationally cheaper
// path — Lambda owns shard polling, fan-out across instances, automatic
// scaling on shard count, and partial-batch retry semantics. For Kinesis-fed
// pipelines (Segment-style ingest, AWS-native event buses), this package gives
// you the same Murmur pipeline definition deployed as a Lambda handler instead
// of an ECS service.
//
// Wire it up:
//
//	func main() {
//	    pipe := buildPipeline()  // shared with the Kafka worker
//	    handler, err := kinesis.NewHandler(pipe, kinesis.JSONDecoder[Event](),
//	        kinesis.WithMetrics(rec),
//	        kinesis.WithDedup(deduper),
//	    )
//	    if err != nil { log.Fatal(err) }
//	    lambda.Start(handler)
//	}
//
// The same `pipe` can be passed to streaming.Run on a Kafka worker — both
// drivers write through the same DDB store, so a Murmur pipeline can ingest
// from BOTH a Kinesis Lambda and an ECS Kafka worker simultaneously, sharing
// state.
//
// # Partial-batch failure handling
//
// Records that fail every retry are reported via BatchItemFailures with the
// Kinesis sequence number as ItemIdentifier. Configure your event-source
// mapping with `FunctionResponseTypes=["ReportBatchItemFailures"]` so Lambda
// only redelivers the failures (or, in Kinesis shard-order mode, all records
// from the earliest failure forward). Pair WithDedup with a state.Deduper so
// successful records that get redelivered alongside a failure are deduplicated
// at the monoid layer rather than double-counted.
//
// # Limits
//
// Lambda's per-batch payload cap (6 MB synchronous), per-batch record cap
// (10,000 records, configurable), and 15-minute timeout apply. Pipelines
// whose per-record processing time × batch size approaches 15 minutes should
// either lower the event-source mapping's BatchSize or move to streaming.Run
// on ECS.
package kinesis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"github.com/gallowaysoftware/murmur/pkg/exec/processor"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// Handler is the Lambda handler signature for Kinesis triggers — pass the
// returned value directly to lambda.Start.
type Handler = func(context.Context, events.KinesisEvent) (events.KinesisEventResponse, error)

// Decoder converts a raw Kinesis record's Data to a typed pipeline value.
type Decoder[T any] func([]byte) (T, error)

// JSONDecoder returns a Decoder that unmarshals JSON into T. For Avro / Proto /
// SDK-specific shapes, supply a custom Decoder.
func JSONDecoder[T any]() Decoder[T] {
	return func(b []byte) (T, error) {
		var v T
		if err := json.Unmarshal(b, &v); err != nil {
			return v, fmt.Errorf("kinesis lambda json decode: %w", err)
		}
		return v, nil
	}
}

// HandlerOption configures NewHandler.
type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	processor.Config
	onDecodeError func(raw []byte, sequenceNumber, partitionKey string, err error)
	now           func() time.Time
}

// WithMetrics installs a metrics.Recorder. Defaults to metrics.Noop{}.
// The handler records events under the pipeline's Name; retries under
// "<name>:retry"; dedup skips under "<name>:dedup_skip"; and dead letters
// under "<name>:dead_letter" — same conventions as the streaming runtime.
func WithMetrics(r metrics.Recorder) HandlerOption {
	return func(c *handlerConfig) {
		if r != nil {
			c.Recorder = r
		}
	}
}

// WithMaxAttempts sets the per-record retry budget. Defaults to 3. Records
// that exhaust their budget are reported via BatchItemFailures.
//
// Set to 1 to disable retries (any error → reported on the first failure).
// Lambda will redeliver per the event-source-mapping retry config; the
// dedup option keeps redeliveries idempotent.
func WithMaxAttempts(n int) HandlerOption {
	return func(c *handlerConfig) {
		if n >= 1 {
			c.MaxAttempts = n
		}
	}
}

// WithRetryBackoff configures the per-attempt sleep schedule. Doubles after
// each failure starting from base, capped at max, with full jitter. Default
// 50 ms / 5 s — same as the streaming runtime.
//
// Note that the entire retry loop runs inside the Lambda invocation; very
// long backoffs eat into the 15-minute Lambda timeout. For batches with many
// failed records, lower the per-record budget rather than raising backoff.
func WithRetryBackoff(base, max time.Duration) HandlerOption {
	return func(c *handlerConfig) {
		if base > 0 {
			c.BackoffBase = base
		}
		if max > 0 {
			c.BackoffMax = max
		}
	}
}

// WithDedup installs a state.Deduper. Each Kinesis record's EventID
// ("<event-source-ARN>/<sequenceNumber>") is claimed via MarkSeen before
// the merge runs; on a duplicate, the merge is skipped and the record is
// counted as processed.
//
// Strongly recommended in production: Lambda's BatchItemFailures pattern
// can redeliver records adjacent to a failure even if those records had
// already been merged successfully on the prior invocation.
func WithDedup(d state.Deduper) HandlerOption {
	return func(c *handlerConfig) {
		if d != nil {
			c.Dedup = d
		}
	}
}

// WithDecodeErrorCallback installs a callback for records whose Decode
// returned an error. Default behavior is to drop silently and continue with
// the next record (same as streaming.Run with no callback set).
//
// Decode failures are NOT redelivered — the same record will fail to decode
// on the next pass. Wire this to a DLQ producer to move poison pills off
// the hot path.
func WithDecodeErrorCallback(fn func(raw []byte, sequenceNumber, partitionKey string, err error)) HandlerOption {
	return func(c *handlerConfig) {
		if fn != nil {
			c.onDecodeError = fn
		}
	}
}

// WithClock overrides time.Now for windowed-bucket assignment. Useful for
// tests that drive the handler with deterministic timestamps; production
// code should leave this unset.
func WithClock(now func() time.Time) HandlerOption {
	return func(c *handlerConfig) {
		if now != nil {
			c.now = now
		}
	}
}

// NewHandler builds a Lambda handler that drives a Murmur pipeline from a
// Kinesis trigger. The pipeline must be Build-validated; the supplied
// Decoder converts each Kinesis record's Data to the pipeline's input
// type T. The pipeline's Source field is unused — Lambda owns polling.
//
// Returns an error if the pipeline's required fields (Key, Value, Aggregate,
// StoreIn) are not set, or if Decode is nil.
func NewHandler[T any, V any](
	p *pipeline.Pipeline[T, V],
	decode Decoder[T],
	opts ...HandlerOption,
) (Handler, error) {
	if p == nil {
		return nil, errors.New("kinesis lambda handler: pipeline is nil")
	}
	if err := p.Build(); err != nil {
		return nil, fmt.Errorf("kinesis lambda handler: %w", err)
	}
	if decode == nil {
		return nil, errors.New("kinesis lambda handler: decode is required")
	}

	cfg := handlerConfig{Config: processor.Defaults(), now: time.Now}
	for _, o := range opts {
		o(&cfg)
	}

	name := p.Name()
	keyFn := p.KeyFn()
	valueFn := p.ValueFn()
	store := p.Store()
	cacheStore := p.CacheStore()
	window := p.Window()

	return func(ctx context.Context, evt events.KinesisEvent) (events.KinesisEventResponse, error) {
		var resp events.KinesisEventResponse
		for i := range evt.Records {
			rec := &evt.Records[i]
			value, err := decode(rec.Kinesis.Data)
			if err != nil {
				if cfg.onDecodeError != nil {
					cfg.onDecodeError(rec.Kinesis.Data, rec.Kinesis.SequenceNumber, rec.Kinesis.PartitionKey, err)
				}
				cfg.Recorder.RecordError(name, fmt.Errorf("decode %q: %w", rec.Kinesis.SequenceNumber, err))
				// Don't add to BatchItemFailures: a redelivery would just fail
				// again with the same decode error. Poison pills are routed
				// off the pipeline via the callback, not via redelivery.
				continue
			}

			eventID := buildEventID(rec)
			eventTime := rec.Kinesis.ApproximateArrivalTimestamp.Time
			if eventTime.IsZero() {
				eventTime = cfg.now()
			}

			if err := processor.MergeOne(ctx, &cfg.Config, name, eventID, eventTime,
				value, keyFn, valueFn, store, cacheStore, window); err != nil {
				resp.BatchItemFailures = append(resp.BatchItemFailures, events.KinesisBatchItemFailure{
					ItemIdentifier: rec.Kinesis.SequenceNumber,
				})
			}
		}
		return resp, nil
	}, nil
}

// buildEventID derives a stream-globally-unique ID for dedup. Falls back to
// just the sequence number when the EventSourceArn is unset (which only
// happens in synthetic test events).
func buildEventID(rec *events.KinesisEventRecord) string {
	if rec.EventSourceArn == "" {
		return rec.Kinesis.SequenceNumber
	}
	return rec.EventSourceArn + "/" + rec.Kinesis.SequenceNumber
}
