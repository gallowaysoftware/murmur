// Package sqs hosts a Murmur pipeline behind an AWS Lambda SQS trigger.
//
// SQS is the third common Lambda event source after Kinesis and DynamoDB
// Streams. It's the right shape for:
//
//   - Job-style ingest where producers POST aggregation events to a queue
//     and the consumer is a Murmur pipeline (e.g., a webhook receiver
//     buffers incoming events into SQS, the Lambda fans them into Murmur).
//   - Cross-account ingest where multiple producer accounts deliver into
//     a shared queue.
//   - Burst absorption — SQS holds up to 14 days; the consumer Lambda
//     scales on visible-message count rather than shard count.
//
// Symmetric peer of pkg/exec/lambda/kinesis and pkg/exec/lambda/dynamodbstreams:
// same retry / dedup / metrics / BatchItemFailures semantics; the only
// difference is the input event shape and the dedup key derivation.
//
// Wire it up:
//
//	func main() {
//	    pipe := buildPipeline()
//	    handler, err := sqs.NewHandler(pipe, sqs.JSONDecoder[Event](),
//	        sqs.WithMetrics(rec),
//	        sqs.WithDedup(deduper),
//	    )
//	    if err != nil { log.Fatal(err) }
//	    lambda.Start(handler)
//	}
//
// # Partial-batch failure handling
//
// Records that exhaust their retry budget are reported via BatchItemFailures
// with the SQS message ID as ItemIdentifier. Configure your event-source
// mapping with `FunctionResponseTypes=["ReportBatchItemFailures"]` so SQS
// only redelivers the failures rather than retrying the whole batch.
// Without this flag, a single poison message would force the entire batch
// to redeliver until the message hit its maxReceiveCount and went to a DLQ.
//
// # Dedup key
//
// Default EventID = "<event-source-ARN>/<MessageId>". SQS messages have
// stable IDs across redeliveries (the message ID is stable; the receipt
// handle is not). For FIFO queues with content-based dedup, you may prefer
// to derive the EventID from the message body's natural key — pass a
// custom EventIDFn via WithEventID.
//
// # Visibility timeout interactions
//
// SQS event-source mappings handle visibility-timeout extension
// automatically; the Lambda runtime extends the timeout for in-flight
// messages so a long-running handler doesn't trigger a redelivery.
// Keep the per-record retry budget reasonable so a stuck record doesn't
// burn the entire visibility window.
package sqs

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

// Handler is the Lambda handler signature for SQS triggers — pass the
// returned value directly to lambda.Start.
type Handler = func(context.Context, events.SQSEvent) (events.SQSEventResponse, error)

// Decoder converts a raw SQS message body to the pipeline's input type T.
type Decoder[T any] func(body string) (T, error)

// JSONDecoder returns a Decoder that unmarshals the message body as JSON
// into T.
func JSONDecoder[T any]() Decoder[T] {
	return func(body string) (T, error) {
		var v T
		if err := json.Unmarshal([]byte(body), &v); err != nil {
			return v, fmt.Errorf("sqs lambda json decode: %w", err)
		}
		return v, nil
	}
}

// HandlerOption configures NewHandler.
type HandlerOption[T any] func(*handlerConfig[T])

type handlerConfig[T any] struct {
	processor.Config
	onDecodeError func(body string, messageID string, err error)
	eventIDFn     func(msg *events.SQSMessage, decoded T) string
	now           func() time.Time
}

// WithMetrics installs a metrics.Recorder. Defaults to metrics.Noop{}.
// The handler records events under the pipeline's Name; retries under
// "<name>:retry"; dedup skips under "<name>:dedup_skip"; dead letters
// under "<name>:dead_letter".
func WithMetrics[T any](r metrics.Recorder) HandlerOption[T] {
	return func(c *handlerConfig[T]) {
		if r != nil {
			c.Recorder = r
		}
	}
}

// WithMaxAttempts sets the per-record retry budget. Defaults to 3.
func WithMaxAttempts[T any](n int) HandlerOption[T] {
	return func(c *handlerConfig[T]) {
		if n >= 1 {
			c.MaxAttempts = n
		}
	}
}

// WithRetryBackoff configures the per-attempt sleep schedule. Doubles
// after each failure starting from base, capped at max, with full jitter.
// Defaults to 50 ms / 5 s.
func WithRetryBackoff[T any](base, max time.Duration) HandlerOption[T] {
	return func(c *handlerConfig[T]) {
		if base > 0 {
			c.BackoffBase = base
		}
		if max > 0 {
			c.BackoffMax = max
		}
	}
}

// WithDedup installs a state.Deduper. Each SQS message's EventID
// (default: "<event-source-ARN>/<MessageId>") is claimed via MarkSeen
// before the merge runs; on a duplicate, the merge is skipped and the
// message is counted as processed.
//
// Strongly recommended in production: SQS at-least-once delivery means
// duplicate redeliveries are normal during visibility-timeout edge cases
// and during partial-batch redelivery.
func WithDedup[T any](d state.Deduper) HandlerOption[T] {
	return func(c *handlerConfig[T]) {
		if d != nil {
			c.Dedup = d
		}
	}
}

// WithDecodeErrorCallback installs a callback for messages whose Decode
// returned an error. Default behavior is to drop silently. Decode failures
// are NOT redelivered (poison pill loop) — wire to a DLQ producer to move
// poison messages off the hot path.
func WithDecodeErrorCallback[T any](fn func(body string, messageID string, err error)) HandlerOption[T] {
	return func(c *handlerConfig[T]) {
		if fn != nil {
			c.onDecodeError = fn
		}
	}
}

// WithEventID overrides the default EventID derivation. Default uses
// "<event-source-ARN>/<MessageId>" — globally unique across the queue's
// history and stable across redeliveries.
//
// For FIFO queues with content-based dedup, or for cases where the message
// payload carries an upstream identifier (a Mongo `_id`, a Kafka offset
// reference, etc.), supply an extractor that returns that identifier so
// re-deliveries of the same logical change fold idempotently.
//
// Return "" to disable dedup for that message (it will be processed as if
// no Deduper were configured).
func WithEventID[T any](fn func(msg *events.SQSMessage, decoded T) string) HandlerOption[T] {
	return func(c *handlerConfig[T]) {
		if fn != nil {
			c.eventIDFn = fn
		}
	}
}

// WithClock overrides time.Now for windowed-bucket assignment. Useful for
// tests with deterministic clocks.
func WithClock[T any](now func() time.Time) HandlerOption[T] {
	return func(c *handlerConfig[T]) {
		if now != nil {
			c.now = now
		}
	}
}

// NewHandler builds a Lambda handler that drives a Murmur pipeline from
// an SQS trigger. The pipeline must be Build-validated; the supplied
// Decoder converts each message body to the pipeline's input type T. The
// pipeline's Source field is unused — Lambda owns polling.
//
// Returns an error if the pipeline's required fields (Key, Value,
// Aggregate, StoreIn) are not set, or if Decode is nil.
func NewHandler[T any, V any](
	p *pipeline.Pipeline[T, V],
	decode Decoder[T],
	opts ...HandlerOption[T],
) (Handler, error) {
	if p == nil {
		return nil, errors.New("sqs lambda handler: pipeline is nil")
	}
	if err := p.Build(); err != nil {
		return nil, fmt.Errorf("sqs lambda handler: %w", err)
	}
	if decode == nil {
		return nil, errors.New("sqs lambda handler: decode is required")
	}

	cfg := handlerConfig[T]{Config: processor.Defaults(), now: time.Now}
	for _, o := range opts {
		o(&cfg)
	}

	name := p.Name()
	keysFn := p.KeysFn()
	valueFn := p.ValueFn()
	store := p.Store()
	cacheStore := p.CacheStore()
	window := p.Window()

	return func(ctx context.Context, evt events.SQSEvent) (events.SQSEventResponse, error) {
		var resp events.SQSEventResponse
		for i := range evt.Records {
			msg := &evt.Records[i]
			value, err := decode(msg.Body)
			if err != nil {
				if cfg.onDecodeError != nil {
					cfg.onDecodeError(msg.Body, msg.MessageId, err)
				}
				cfg.Recorder.RecordError(name, fmt.Errorf("decode %q: %w", msg.MessageId, err))
				// Don't add to BatchItemFailures: a redelivery would just
				// fail again with the same decode error. Poison pills are
				// routed off the pipeline via the callback.
				continue
			}

			eventID := buildEventID(msg, value, cfg.eventIDFn)
			eventTime := messageTime(msg, cfg.now)

			if err := processor.MergeMany(ctx, &cfg.Config, name, eventID, eventTime,
				keysFn(value), valueFn(value), store, cacheStore, window); err != nil {
				resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{
					ItemIdentifier: msg.MessageId,
				})
			}
		}
		return resp, nil
	}, nil
}

// buildEventID derives the dedup key for a message. Custom extractor wins;
// otherwise default to "<arn>/<id>".
func buildEventID[T any](msg *events.SQSMessage, decoded T, fn func(*events.SQSMessage, T) string) string {
	if fn != nil {
		return fn(msg, decoded)
	}
	if msg.EventSourceARN == "" {
		return msg.MessageId
	}
	return msg.EventSourceARN + "/" + msg.MessageId
}

// messageTime returns the SQS message's enqueue timestamp if present,
// falling back to the configured clock. Used for windowed-bucket
// assignment when the pipeline is windowed.
func messageTime(msg *events.SQSMessage, nowFn func() time.Time) time.Time {
	// SQS exposes the SentTimestamp in Attributes as Unix milliseconds.
	if v, ok := msg.Attributes["SentTimestamp"]; ok {
		var ms int64
		if _, err := fmt.Sscanf(v, "%d", &ms); err == nil && ms > 0 {
			return time.UnixMilli(ms).UTC()
		}
	}
	return nowFn()
}
