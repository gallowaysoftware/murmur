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
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gallowaysoftware/murmur/pkg/source"
)

// slogKgoLogger bridges kgo.Logger to the standard library's log/slog.
// kgo defaults to a no-op logger which silently swallows broker
// unreachability, metadata refresh failures, and group-coordinator
// errors — all of which manifest as "consumer is stuck and produces
// no error" in production. Wiring it to slog at WARN by default
// surfaces these the same way the rest of murmur's logging works.
type slogKgoLogger struct {
	level kgo.LogLevel
}

func (l slogKgoLogger) Level() kgo.LogLevel { return l.level }

func (l slogKgoLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	var slogLevel slog.Level
	switch level {
	case kgo.LogLevelError:
		slogLevel = slog.LevelError
	case kgo.LogLevelWarn:
		slogLevel = slog.LevelWarn
	case kgo.LogLevelInfo:
		slogLevel = slog.LevelInfo
	default:
		slogLevel = slog.LevelDebug
	}
	slog.Default().Log(context.Background(), slogLevel, "kgo: "+msg, keyvals...)
}

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

	// OnDecodeError, if non-nil, is called for every message whose Decode returned
	// an error. The default behavior is to drop the record silently and advance —
	// fine for development but dangerous in production. Wire this to a DLQ producer
	// or a metrics.Recorder.RecordError to surface poison pills.
	OnDecodeError func(raw []byte, partition int32, offset int64, err error)

	// EventID, if non-nil, is called on every decoded record to compute the
	// at-least-once dedup key. The default is "<topic>:<partition>:<offset>"
	// — globally unique within the cluster's history but only deduplicates
	// Kafka-side redeliveries (rebalance after a crash before commit). For
	// CDC pipelines where the same logical change can be produced twice
	// (Debezium retransmits, dual-write fixers), set this to extract the
	// upstream identifier (e.g. Mongo `_id`, Postgres LSN) from the record.
	EventID func(T) string

	// OnFetchError, if non-nil, is called for partition-level fetch errors that the
	// client is going to retry internally (broker bounce, leader change, etc).
	// Default: drop. Wire to logging / metrics to surface persistent failures.
	OnFetchError func(topic string, partition int32, err error)

	// Concurrency controls per-partition decode parallelism. When N > 1, the
	// Source spawns N decoder goroutines plus one fetcher; each partition is
	// pinned to worker (partition mod N), so per-partition order is preserved
	// while decode-heavy formats (Protobuf with schema lookups, encrypted
	// payloads) can saturate multiple cores. Default 1 — the single-goroutine
	// path is identical to the historical behavior. There is no benefit to
	// setting Concurrency above the partition count assigned to this consumer.
	Concurrency int

	// PartitionQueueSize is the per-worker channel depth used when Concurrency
	// > 1. Each fetch batch's per-partition slice is enqueued onto the
	// matching worker's channel; if a slow downstream backs up, the fetcher
	// blocks. Defaults to 256.
	PartitionQueueSize int

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
	client             *kgo.Client
	topic              string
	decode             Decoder[T]
	eventID            func(T) string
	onDecodeError      func(raw []byte, partition int32, offset int64, err error)
	onFetchError       func(topic string, partition int32, err error)
	concurrency        int
	partitionQueueSize int
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
		// Bridge kgo's logger to the standard slog so warnings ("can't
		// reach broker X", "metadata refresh failed", group-coordinator
		// errors) actually surface in production logs instead of being
		// silently swallowed. Default level is Warn; bump via Extra
		// (kgo.WithLogger) for kgo-level debugging.
		kgo.WithLogger(slogKgoLogger{level: kgo.LogLevelWarn}),
	}
	opts = append(opts, cfg.Extra...)

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka new client: %w", err)
	}
	concurrency := cfg.Concurrency
	if concurrency < 1 {
		concurrency = 1
	}
	queueSize := cfg.PartitionQueueSize
	if queueSize <= 0 {
		queueSize = 256
	}
	return &Source[T]{
		client:             cl,
		topic:              cfg.Topic,
		decode:             cfg.Decode,
		eventID:            cfg.EventID,
		onDecodeError:      cfg.OnDecodeError,
		onFetchError:       cfg.OnFetchError,
		concurrency:        concurrency,
		partitionQueueSize: queueSize,
	}, nil
}

// Read polls the consumer group and yields decoded records into out until ctx is canceled.
// Returns nil on graceful shutdown; non-nil only on a fatal client error.
//
// When Config.Concurrency > 1, decode work fans out across N goroutines, with
// each partition pinned to worker (partition mod N) so per-partition order is
// preserved. The single-goroutine path (the default) is unchanged.
func (s *Source[T]) Read(ctx context.Context, out chan<- source.Record[T]) error {
	if s.concurrency <= 1 {
		return s.readSerial(ctx, out)
	}
	return s.readConcurrent(ctx, out)
}

// readSerial is the historical single-goroutine path.
func (s *Source[T]) readSerial(ctx context.Context, out chan<- source.Record[T]) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches := s.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			if errors.Is(err, context.Canceled) {
				return
			}
			if s.onFetchError != nil {
				s.onFetchError(t, p, err)
			}
		})

		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			if err := s.dispatchRecord(ctx, rec, out); err != nil {
				return nil
			}
		}
	}
}

// readConcurrent is the per-partition-parallel path. One fetcher goroutine
// polls the client; per-partition record slices are routed to N decoder
// workers via per-worker channels; each worker decodes and emits to out.
//
// Per-partition order is preserved because every record for partition P
// always lands in worker (P mod N), processed FIFO from its inbound channel.
// Cross-partition order is not preserved (and was never preserved by the
// single-goroutine path either — franz-go's RecordIter interleaves
// partitions in arbitrary order across calls).
func (s *Source[T]) readConcurrent(ctx context.Context, out chan<- source.Record[T]) error {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type job struct {
		recs []*kgo.Record
	}
	queues := make([]chan job, s.concurrency)
	for i := range queues {
		queues[i] = make(chan job, s.partitionQueueSize)
	}

	var wg sync.WaitGroup
	for i := 0; i < s.concurrency; i++ {
		wg.Add(1)
		go func(in <-chan job) {
			defer wg.Done()
			for {
				select {
				case <-workerCtx.Done():
					return
				case j, ok := <-in:
					if !ok {
						return
					}
					for _, rec := range j.recs {
						if err := s.dispatchRecord(workerCtx, rec, out); err != nil {
							return
						}
					}
				}
			}
		}(queues[i])
	}

	// Fetcher loop: serial PollFetches, dispatch by partition mod N.
	defer func() {
		for _, q := range queues {
			close(q)
		}
		wg.Wait()
	}()
	for {
		if err := workerCtx.Err(); err != nil {
			return nil
		}
		fetches := s.client.PollFetches(workerCtx)
		if fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			if errors.Is(err, context.Canceled) {
				return
			}
			if s.onFetchError != nil {
				s.onFetchError(t, p, err)
			}
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			idx := int(uint32(p.Partition)) % s.concurrency
			select {
			case queues[idx] <- job{recs: p.Records}:
			case <-workerCtx.Done():
			}
		})
	}
}

// dispatchRecord decodes one Kafka record and emits a source.Record to out.
// Returns an error when ctx is canceled (the caller should exit) or nil to
// continue. The "error" is just a signal — the caller doesn't propagate it.
func (s *Source[T]) dispatchRecord(ctx context.Context, rec *kgo.Record, out chan<- source.Record[T]) error {
	value, err := s.decode(rec.Value)
	if err != nil {
		if s.onDecodeError != nil {
			s.onDecodeError(rec.Value, rec.Partition, rec.Offset, err)
		}
		// Mark for commit so we don't loop forever on the same poison pill.
		s.client.MarkCommitRecords(rec)
		return nil
	}
	eventID := fmt.Sprintf("%s:%d:%d", rec.Topic, rec.Partition, rec.Offset)
	if s.eventID != nil {
		if id := s.eventID(value); id != "" {
			eventID = id
		}
	}
	r := source.Record[T]{
		EventID:      eventID,
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
		return nil
	case <-ctx.Done():
		return ctx.Err()
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
