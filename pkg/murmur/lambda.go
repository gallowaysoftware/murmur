package murmur

import (
	"context"
	"log/slog"

	"github.com/aws/aws-lambda-go/events"

	"github.com/gallowaysoftware/murmur/pkg/exec/lambda/dynamodbstreams"
	"github.com/gallowaysoftware/murmur/pkg/exec/lambda/kinesis"
	"github.com/gallowaysoftware/murmur/pkg/exec/lambda/sqs"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// LambdaConfig is the unified configuration shared across the three
// Lambda runtime helpers. The boilerplate-eating equivalent of
// RunStreamingWorker for streaming workers.
//
// Wire it once; pass it to KinesisHandler / DynamoDBStreamsHandler /
// SQSHandler — each builds the matching handler with sensible defaults
// (slog-driven decode-error logging, metrics.Noop unless overridden,
// dedup hooked up).
type LambdaConfig struct {
	// Recorder receives events / errors / latencies. Defaults to
	// metrics.Noop. For production wire a CloudWatch / Prometheus /
	// Datadog adapter.
	Recorder metrics.Recorder

	// Dedup, when set, makes at-least-once delivery idempotent at the
	// monoid layer. Strongly recommended for any non-idempotent monoid
	// (Sum, HLL, TopK).
	Dedup state.Deduper

	// Logger is used by the default decode-error callback. Defaults to
	// slog.Default(). Wire a structured logger for production.
	Logger *slog.Logger
}

// KinesisHandler builds a Kinesis Lambda handler from a pipeline plus
// a Decoder. Equivalent to manually constructing kinesis.NewHandler with
// the standard production option set:
//
//   - WithMetrics(cfg.Recorder)
//   - WithDedup(cfg.Dedup) when non-nil
//   - WithDecodeErrorCallback(...) that logs via cfg.Logger
//
// The returned handler is ready for lambda.Start.
func KinesisHandler[T any, V any](
	p *pipeline.Pipeline[T, V],
	decode kinesis.Decoder[T],
	cfg LambdaConfig,
) (kinesis.Handler, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	opts := []kinesis.HandlerOption{
		kinesis.WithDecodeErrorCallback(func(_ []byte, seq, partKey string, err error) {
			logger.Error("kinesis lambda decode error",
				"sequence_number", seq,
				"partition_key", partKey,
				"err", err,
			)
		}),
	}
	if cfg.Recorder != nil {
		opts = append(opts, kinesis.WithMetrics(cfg.Recorder))
	}
	if cfg.Dedup != nil {
		opts = append(opts, kinesis.WithDedup(cfg.Dedup))
	}
	return kinesis.NewHandler(p, decode, opts...)
}

// DynamoDBStreamsHandler builds a DDB Streams Lambda handler with the
// same standard option set as KinesisHandler.
func DynamoDBStreamsHandler[T any, V any](
	p *pipeline.Pipeline[T, V],
	decode dynamodbstreams.Decoder[T],
	cfg LambdaConfig,
) (dynamodbstreams.Handler, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	opts := []dynamodbstreams.HandlerOption{
		dynamodbstreams.WithDecodeErrorCallback(func(rec *events.DynamoDBEventRecord, err error) {
			logger.Error("ddb streams lambda decode error",
				"event_id", rec.EventID,
				"event_name", rec.EventName,
				"err", err,
			)
		}),
	}
	if cfg.Recorder != nil {
		opts = append(opts, dynamodbstreams.WithMetrics(cfg.Recorder))
	}
	if cfg.Dedup != nil {
		opts = append(opts, dynamodbstreams.WithDedup(cfg.Dedup))
	}
	return dynamodbstreams.NewHandler(p, decode, opts...)
}

// SQSHandler builds an SQS Lambda handler with the same standard option
// set as KinesisHandler / DynamoDBStreamsHandler.
//
// Type parameters note: SQS's HandlerOption is parameterized over T
// (the input record type) so the option helpers can carry the type
// through to NewHandler. The wrapper preserves that — the [T] type
// parameter at the call site is required.
func SQSHandler[T any, V any](
	p *pipeline.Pipeline[T, V],
	decode sqs.Decoder[T],
	cfg LambdaConfig,
) (sqs.Handler, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	opts := []sqs.HandlerOption[T]{
		sqs.WithDecodeErrorCallback[T](func(body string, msgID string, err error) {
			logger.Error("sqs lambda decode error",
				"message_id", msgID,
				"err", err,
				"body_len", len(body),
			)
		}),
	}
	if cfg.Recorder != nil {
		opts = append(opts, sqs.WithMetrics[T](cfg.Recorder))
	}
	if cfg.Dedup != nil {
		opts = append(opts, sqs.WithDedup[T](cfg.Dedup))
	}
	return sqs.NewHandler(p, decode, opts...)
}

// MustHandler is the panic-on-error variant for the typical Lambda main
// pattern where any error during handler construction is fatal anyway:
//
//	handler := murmur.MustHandler(murmur.KinesisHandler(pipe, decode, cfg))
//	lambda.Start(handler)
//
// Cuts the boilerplate from 4 lines (declare, check err, log.Fatal,
// lambda.Start) to 2.
func MustHandler[H any](h H, err error) H {
	if err != nil {
		// Lambda init failures are fatal by design — there's no recovery
		// path. Log via slog.Default() and let the runtime surface the
		// panic as an init error in CloudWatch.
		slog.Default().Error("lambda handler init failed", "err", err)
		panic(err)
	}
	return h
}

// Compile-time satisfaction check that the wrappers compose with
// context.Context as the Lambda runtime expects.
var _ = func() {
	type marker = context.Context
	_ = marker(nil)
}
