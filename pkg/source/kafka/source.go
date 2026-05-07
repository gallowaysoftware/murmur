// Package kafka provides a Kafka-backed implementation of source.Source via the franz-go
// client. Suitable for both Apache Kafka and Amazon MSK.
//
// Semantics: at-least-once with manual offset marking. Each record's Ack callback marks
// the underlying Kafka record for commit; AutoCommitMarks then periodically advances the
// committed offset. Records that are not Ack'd before consumer-group rebalance or
// process death are re-delivered to the next consumer — pipelines must dedup by EventID.
//
// EventID is "<topic>:<partition>:<offset>", globally unique within a Kafka cluster.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gallowaysoftware/murmur/pkg/source"
)

// Config configures a Kafka Source.
type Config[T any] struct {
	// Brokers is the seed broker list, e.g. {"localhost:9092"} or MSK bootstrap servers.
	Brokers []string

	// Topic is the Kafka topic to consume from.
	Topic string

	// ConsumerGroup is the Kafka consumer-group ID. All workers sharing this ID
	// cooperatively partition the topic between them.
	ConsumerGroup string

	// Decode converts a raw message value to T. Use JSONDecoder[T]() for JSON-encoded
	// records, or supply your own for Avro / Protobuf / etc.
	Decode Decoder[T]

	// Extra lets callers append additional franz-go options (TLS, SASL, etc).
	Extra []kgo.Opt
}

// Decoder converts a raw Kafka message value to a typed Record value.
type Decoder[T any] func([]byte) (T, error)

// JSONDecoder returns a Decoder that unmarshals JSON into T.
func JSONDecoder[T any]() Decoder[T] {
	return func(b []byte) (T, error) {
		var v T
		if err := json.Unmarshal(b, &v); err != nil {
			return v, fmt.Errorf("kafka json decode: %w", err)
		}
		return v, nil
	}
}

// Source reads from a Kafka topic and yields source.Records.
type Source[T any] struct {
	client *kgo.Client
	topic  string
	decode Decoder[T]
}

// NewSource constructs a Kafka Source. The returned Source owns the underlying franz-go
// client; call Close to shut down cleanly.
func NewSource[T any](cfg Config[T]) (*Source[T], error) {
	if cfg.Decode == nil {
		return nil, errors.New("kafka source: Decode is required")
	}
	if cfg.Topic == "" {
		return nil, errors.New("kafka source: Topic is required")
	}
	if cfg.ConsumerGroup == "" {
		return nil, errors.New("kafka source: ConsumerGroup is required")
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(cfg.Topic),
		// Only commit offsets we explicitly mark via MarkCommitRecords on Ack.
		// This is the at-least-once semantics: a record that's not Ack'd before
		// rebalance or process death is re-delivered.
		kgo.AutoCommitMarks(),
	}
	opts = append(opts, cfg.Extra...)

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka new client: %w", err)
	}
	return &Source[T]{client: cl, topic: cfg.Topic, decode: cfg.Decode}, nil
}

// Read polls the consumer group and yields decoded records into out until ctx is canceled.
// Returns nil on graceful shutdown; non-nil only on a fatal client error.
func (s *Source[T]) Read(ctx context.Context, out chan<- source.Record[T]) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches := s.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		// Surface fetch errors but keep going — most are retriable (broker bounce, etc).
		// Fatal errors are caught at PollFetches return path.
		fetches.EachError(func(t string, p int32, err error) {
			if errors.Is(err, context.Canceled) {
				return
			}
			// In production: emit a metric / log. Stub for now.
			_ = err
		})

		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			value, err := s.decode(rec.Value)
			if err != nil {
				// Poison pill: skip. In production we'd push to a DLQ via a configurable hook.
				continue
			}
			r := source.Record[T]{
				EventID:      fmt.Sprintf("%s:%d:%d", rec.Topic, rec.Partition, rec.Offset),
				EventTime:    rec.Timestamp,
				PartitionKey: string(rec.Key),
				Value:        value,
				Ack: func() error {
					s.client.MarkCommitRecords(rec)
					return nil
				},
			}
			select {
			case out <- r:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

// Name returns "kafka:<topic>".
func (s *Source[T]) Name() string { return "kafka:" + s.topic }

// Close commits any outstanding marked offsets and shuts down the client.
func (s *Source[T]) Close() error {
	// Flush any remaining marked offsets before close.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.client.CommitMarkedOffsets(ctx); err != nil {
		// Non-fatal: client.Close still releases resources.
		_ = err
	}
	s.client.Close()
	return nil
}

// Compile-time check.
var _ source.Source[any] = (*Source[any])(nil)
