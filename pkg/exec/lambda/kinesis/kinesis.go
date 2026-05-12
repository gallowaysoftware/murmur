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
	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
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
	keysFn := p.KeysFn()
	valueFn := p.ValueFn()
	mon := p.Monoid()
	store := p.Store()
	cacheStore := p.CacheStore()
	window := p.Window()
	coalesceCfg := p.Coalesce()

	if coalesceCfg.Enabled {
		return newCoalescingHandler(&cfg, coalesceCfg, name, mon, keysFn, valueFn,
			store, cacheStore, window, decode), nil
	}

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

			if err := processor.MergeMany(ctx, &cfg.Config, name, eventID, eventTime,
				keysFn(value), valueFn(value), store, cacheStore, window); err != nil {
				resp.BatchItemFailures = append(resp.BatchItemFailures, events.KinesisBatchItemFailure{
					ItemIdentifier: rec.Kinesis.SequenceNumber,
				})
			}
		}
		return resp, nil
	}, nil
}

// newCoalescingHandler returns the Kinesis Lambda handler path that uses
// processor.Coalescer to collapse N events touching the same key into 1
// MergeUpdate per batch.
//
// Failure granularity: when a flushed key fails after retries, the handler
// reports BatchItemFailures for every Kinesis sequence number that contributed
// to that key (the Coalescer surfaces contributing EventIDs; this code maps
// each EventID back to its sequence number via the local arrivedIDs map).
// Records whose sequence numbers don't appear among the contributors are not
// reported — Lambda treats them as successful, the dedup guard prevents
// double-counting on the inevitable replay of the failed-sequence subset.
//
// Records that fail to decode are not added to BatchItemFailures (same poison-
// pill policy as the per-event path).
func newCoalescingHandler[T any, V any](
	cfg *handlerConfig,
	coalesceCfg processor.CoalesceConfig,
	name string,
	mon monoid.Monoid[V],
	keysFn func(T) []string,
	valueFn func(T) V,
	store state.Store[V],
	cacheStore state.Cache[V],
	window *windowed.Config,
	decode Decoder[T],
) Handler {
	return func(ctx context.Context, evt events.KinesisEvent) (events.KinesisEventResponse, error) {
		var resp events.KinesisEventResponse

		// eventIDToSeq maps the dedup EventID (event-source-ARN/sequence-number)
		// back to the raw Kinesis sequence number for BatchItemFailures reporting.
		eventIDToSeq := make(map[string]string, len(evt.Records))

		c := processor.NewCoalescer(&cfg.Config, coalesceCfg, name, mon, store, cacheStore, window)
		// failedMidBatch is set when an auto-flush returned an error inside the
		// add-loop. Subsequent records are not added — we abort the batch and
		// report every record from that point forward as a failure so Lambda
		// redelivers them. Dedup keeps successful prefix-keys from double-
		// counting on redelivery.
		var (
			midBatchErr     error
			abortedAtRecord = -1
		)
		for i := range evt.Records {
			rec := &evt.Records[i]
			value, err := decode(rec.Kinesis.Data)
			if err != nil {
				if cfg.onDecodeError != nil {
					cfg.onDecodeError(rec.Kinesis.Data, rec.Kinesis.SequenceNumber, rec.Kinesis.PartitionKey, err)
				}
				cfg.Recorder.RecordError(name, fmt.Errorf("decode %q: %w", rec.Kinesis.SequenceNumber, err))
				continue
			}

			eventID := buildEventID(rec)
			eventIDToSeq[eventID] = rec.Kinesis.SequenceNumber
			eventTime := rec.Kinesis.ApproximateArrivalTimestamp.Time
			if eventTime.IsZero() {
				eventTime = cfg.now()
			}

			if err := c.AddMany(ctx, eventID, eventTime, keysFn(value), valueFn(value)); err != nil {
				midBatchErr = err
				abortedAtRecord = i
				break
			}
		}

		if midBatchErr != nil {
			// Report the keys that actually failed via the FlushError surface
			// (precise sequence-number mapping where possible).
			appendBatchFailures(&resp, midBatchErr, eventIDToSeq)
			// Plus every record we never got to: they remain unprocessed. Lambda
			// must redeliver them; dedup makes that safe.
			for i := abortedAtRecord + 1; i < len(evt.Records); i++ {
				resp.BatchItemFailures = append(resp.BatchItemFailures, events.KinesisBatchItemFailure{
					ItemIdentifier: evt.Records[i].Kinesis.SequenceNumber,
				})
			}
			return resp, nil
		}

		if err := c.Flush(ctx); err != nil {
			appendBatchFailures(&resp, err, eventIDToSeq)
		}
		return resp, nil
	}
}

// appendBatchFailures maps Coalescer flush failures back to the corresponding
// Kinesis sequence numbers via the contributing-EventID metadata the Coalescer
// preserved. On any other error type, all known sequence numbers are reported
// — Lambda will redeliver the whole batch (safe with the dedup guard).
func appendBatchFailures(
	resp *events.KinesisEventResponse,
	err error,
	eventIDToSeq map[string]string,
) {
	var fe *processor.FlushError
	if !errors.As(err, &fe) {
		// Unknown error (likely context cancellation). Report every known
		// sequence number; Lambda will redeliver the batch and dedup will
		// prevent double-counting of any keys that did complete.
		for _, seq := range eventIDToSeq {
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.KinesisBatchItemFailure{
				ItemIdentifier: seq,
			})
		}
		return
	}
	seen := make(map[string]struct{}, len(fe.FailedKeys))
	for _, kf := range fe.FailedKeys {
		for _, id := range kf.ContributingIDs {
			seq, ok := eventIDToSeq[id]
			if !ok {
				continue
			}
			if _, dup := seen[seq]; dup {
				continue
			}
			seen[seq] = struct{}{}
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.KinesisBatchItemFailure{
				ItemIdentifier: seq,
			})
		}
	}
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
