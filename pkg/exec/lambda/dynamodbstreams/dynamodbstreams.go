// Package dynamodbstreams hosts a Murmur pipeline behind an AWS Lambda
// DynamoDB Streams trigger. This is the natural CDC pattern for shops whose
// source of truth is a DynamoDB table — a common Murmur use case is
// aggregating activity over a DDB-table-backed application's change history.
//
// The package is the symmetric peer of pkg/exec/lambda/kinesis: same
// retry / dedup / metrics / partial-batch-failure semantics; the only
// difference is the input event shape.
//
// Wire it up:
//
//	func main() {
//	    pipe := buildPipeline()
//	    handler, err := dynamodbstreams.NewHandler(pipe,
//	        // The Decoder reads the change record and projects to the
//	        // pipeline's T. Inspect rec.EventName ("INSERT" / "MODIFY" /
//	        // "REMOVE") to decide how to handle each operation.
//	        func(rec *events.DynamoDBEventRecord) (Order, error) {
//	            if rec.EventName == "REMOVE" {
//	                return Order{}, dynamodbstreams.ErrSkipRecord  // Don't aggregate deletes.
//	            }
//	            return decodeOrder(rec.Change.NewImage)
//	        },
//	        dynamodbstreams.WithDedup(deduper),
//	    )
//	    if err != nil { log.Fatal(err) }
//	    lambda.Start(handler)
//	}
//
// # Decoder pattern
//
// DDB Streams records do NOT carry a raw byte payload — they carry a
// `Change` with `NewImage` / `OldImage` / `Keys` as
// `map[string]events.DynamoDBAttributeValue`. The Decoder receives the whole
// record (not just bytes) so callers can:
//
//   - branch on EventName to ignore deletes, or treat MODIFY as a delta
//   - read OldImage to detect which fields changed
//   - dig into Keys when the partition key alone is enough
//
// Return ErrSkipRecord from the decoder to skip a record cleanly (counts as
// processed, no BatchItemFailure entry). Any other error is treated as a
// poison pill: counted via metrics.RecordError, surfaced via
// WithDecodeErrorCallback, and skipped — same poison-pill semantics as the
// Kinesis handler.
//
// # Partial-batch failure handling
//
// Records that exhaust their retry budget are reported via BatchItemFailures
// with the DDB Streams `eventID` as ItemIdentifier. Configure your
// event-source mapping with `FunctionResponseTypes=["ReportBatchItemFailures"]`
// so Lambda only redelivers the failures (or, in shard-order replay mode,
// all records from the earliest failure forward).
package dynamodbstreams

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// ErrSkipRecord is the sentinel a Decoder returns to indicate the record
// should be skipped without being treated as a poison pill. The handler
// counts it as processed and does not add it to BatchItemFailures.
//
// Use this when a DDB change shouldn't drive an aggregation — typically a
// REMOVE event, or a MODIFY whose changed fields don't matter for this
// pipeline.
var ErrSkipRecord = errors.New("dynamodbstreams: skip record")

// Handler is the Lambda handler signature for DynamoDB Streams triggers —
// pass the returned value directly to lambda.Start.
type Handler = func(context.Context, events.DynamoDBEvent) (events.DynamoDBEventResponse, error)

// Decoder converts a DynamoDB Streams change record to the pipeline's input
// type T. Return ErrSkipRecord to skip cleanly; any other error is recorded
// as a decode failure and the record is dropped.
type Decoder[T any] func(*events.DynamoDBEventRecord) (T, error)

// HandlerOption configures NewHandler.
type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	recorder      metrics.Recorder
	maxAttempts   int
	backoffBase   time.Duration
	backoffMax    time.Duration
	dedup         state.Deduper
	onDecodeError func(rec *events.DynamoDBEventRecord, err error)
	now           func() time.Time
}

// WithMetrics installs a metrics.Recorder. Defaults to metrics.Noop{}.
// The handler records events under the pipeline's Name; retries under
// "<name>:retry"; dedup skips under "<name>:dedup_skip"; dead letters under
// "<name>:dead_letter"; and skipped records (ErrSkipRecord) under
// "<name>:skip" — same conventions as the streaming runtime.
func WithMetrics(r metrics.Recorder) HandlerOption {
	return func(c *handlerConfig) {
		if r != nil {
			c.recorder = r
		}
	}
}

// WithMaxAttempts sets the per-record retry budget. Defaults to 3.
func WithMaxAttempts(n int) HandlerOption {
	return func(c *handlerConfig) {
		if n >= 1 {
			c.maxAttempts = n
		}
	}
}

// WithRetryBackoff configures the per-attempt sleep schedule. Doubles after
// each failure starting from base, capped at max, with full jitter. Defaults
// to 50 ms / 5 s.
func WithRetryBackoff(base, max time.Duration) HandlerOption {
	return func(c *handlerConfig) {
		if base > 0 {
			c.backoffBase = base
		}
		if max > 0 {
			c.backoffMax = max
		}
	}
}

// WithDedup installs a state.Deduper. Each DynamoDB Streams record's
// `eventID` (already globally unique within the stream's history) is
// claimed via MarkSeen before the merge runs; on a duplicate, the merge is
// skipped and the record is counted as processed.
//
// Strongly recommended in production: Lambda's BatchItemFailures pattern
// can redeliver records adjacent to a failure even if those records had
// already been merged successfully on the prior invocation.
func WithDedup(d state.Deduper) HandlerOption {
	return func(c *handlerConfig) {
		if d != nil {
			c.dedup = d
		}
	}
}

// WithDecodeErrorCallback installs a callback for records whose Decoder
// returned a non-ErrSkipRecord error. Default behavior is to drop silently
// and continue with the next record.
//
// Decode failures are NOT redelivered — the same record will fail to decode
// on the next pass. Wire this to a DLQ producer to move poison pills off
// the hot path.
func WithDecodeErrorCallback(fn func(rec *events.DynamoDBEventRecord, err error)) HandlerOption {
	return func(c *handlerConfig) {
		if fn != nil {
			c.onDecodeError = fn
		}
	}
}

// WithClock overrides time.Now for windowed-bucket assignment. Useful for
// tests with deterministic clocks; production code should leave this unset.
func WithClock(now func() time.Time) HandlerOption {
	return func(c *handlerConfig) {
		if now != nil {
			c.now = now
		}
	}
}

// NewHandler builds a Lambda handler that drives a Murmur pipeline from a
// DynamoDB Streams trigger. The pipeline must be Build-validated; the
// supplied Decoder converts each DDB Streams change record to the pipeline's
// input type T. The pipeline's Source field is unused — Lambda owns
// polling.
//
// Returns an error if the pipeline's required fields (Key, Value, Aggregate,
// StoreIn) are not set, or if Decode is nil.
func NewHandler[T any, V any](
	p *pipeline.Pipeline[T, V],
	decode Decoder[T],
	opts ...HandlerOption,
) (Handler, error) {
	if p == nil {
		return nil, errors.New("dynamodbstreams lambda handler: pipeline is nil")
	}
	if err := p.Build(); err != nil {
		return nil, fmt.Errorf("dynamodbstreams lambda handler: %w", err)
	}
	if decode == nil {
		return nil, errors.New("dynamodbstreams lambda handler: decode is required")
	}

	cfg := handlerConfig{
		recorder:    metrics.Noop{},
		maxAttempts: 3,
		backoffBase: 50 * time.Millisecond,
		backoffMax:  5 * time.Second,
		now:         time.Now,
	}
	for _, o := range opts {
		o(&cfg)
	}

	name := p.Name()
	keyFn := p.KeyFn()
	valueFn := p.ValueFn()
	store := p.Store()
	cacheStore := p.CacheStore()
	window := p.Window()

	return func(ctx context.Context, evt events.DynamoDBEvent) (events.DynamoDBEventResponse, error) {
		var resp events.DynamoDBEventResponse
		for i := range evt.Records {
			rec := &evt.Records[i]
			value, err := decode(rec)
			if err != nil {
				if errors.Is(err, ErrSkipRecord) {
					cfg.recorder.RecordEvent(name + ":skip")
					continue
				}
				if cfg.onDecodeError != nil {
					cfg.onDecodeError(rec, err)
				}
				cfg.recorder.RecordError(name, fmt.Errorf("decode %q: %w", rec.EventID, err))
				continue
			}

			eventTime := rec.Change.ApproximateCreationDateTime.Time
			if eventTime.IsZero() {
				eventTime = cfg.now()
			}

			ok := processWithRetry(ctx, name, rec.EventID, eventTime, value, keyFn, valueFn, store, cacheStore, window, &cfg)
			if !ok {
				resp.BatchItemFailures = append(resp.BatchItemFailures, events.DynamoDBBatchItemFailure{
					ItemIdentifier: rec.EventID,
				})
			}
		}
		return resp, nil
	}, nil
}

// processWithRetry returns true on success, false on exhausted retries or a
// canceled context. Retries are inline with backoff so the Lambda invocation
// stays linear; the caller adds the record to BatchItemFailures on false.
func processWithRetry[T any, V any](
	ctx context.Context,
	name string,
	eventID string,
	eventTime time.Time,
	value T,
	keyFn func(T) string,
	valueFn func(T) V,
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
	cfg *handlerConfig,
) bool {
	if cfg.dedup != nil && eventID != "" {
		first, err := cfg.dedup.MarkSeen(ctx, eventID)
		if err != nil {
			cfg.recorder.RecordError(name, fmt.Errorf("dedup MarkSeen %q: %w", eventID, err))
		} else if !first {
			cfg.recorder.RecordEvent(name + ":dedup_skip")
			return true
		}
	}

	var lastErr error
	for attempt := 0; attempt < cfg.maxAttempts; attempt++ {
		if attempt > 0 {
			if err := backoffWait(ctx, cfg, attempt); err != nil {
				return false
			}
			cfg.recorder.RecordEvent(name + ":retry")
		}
		if err := processOne(ctx, name, eventTime, value, keyFn, valueFn, store, cache, window, cfg.recorder); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return false
			}
			lastErr = err
			continue
		}
		return true
	}

	wrapped := fmt.Errorf("pipeline %q event %q failed after %d attempts: %w",
		name, eventID, cfg.maxAttempts, lastErr)
	cfg.recorder.RecordError(name, wrapped)
	cfg.recorder.RecordEvent(name + ":dead_letter")
	return false
}

func backoffWait(ctx context.Context, cfg *handlerConfig, attempt int) error {
	d := cfg.backoffBase << (attempt - 1)
	if d > cfg.backoffMax {
		d = cfg.backoffMax
	}
	if d > 0 {
		d += time.Duration(rand.Int64N(int64(d / 2)))
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func processOne[T any, V any](
	ctx context.Context,
	name string,
	eventTime time.Time,
	value T,
	keyFn func(T) string,
	valueFn func(T) V,
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
	rec metrics.Recorder,
) error {
	entity := keyFn(value)
	delta := valueFn(value)

	sk := state.Key{Entity: entity}
	var ttl time.Duration
	if window != nil {
		sk.Bucket = window.BucketID(eventTime)
		ttl = window.Retention
	}

	storeStart := time.Now()
	if err := store.MergeUpdate(ctx, sk, delta, ttl); err != nil {
		return fmt.Errorf("store MergeUpdate: %w", err)
	}
	rec.RecordLatency(name, "store_merge", time.Since(storeStart))

	if cache != nil {
		cacheStart := time.Now()
		if err := cache.MergeUpdate(ctx, sk, delta, ttl); err != nil {
			rec.RecordError(name, fmt.Errorf("cache MergeUpdate: %w", err))
		}
		rec.RecordLatency(name, "cache_merge", time.Since(cacheStart))
	}

	rec.RecordEvent(name)
	return nil
}
