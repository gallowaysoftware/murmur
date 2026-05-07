// Package kinesis provides a Kinesis Data Streams source via aws-sdk-go-v2.
//
// Phase 1 ships a simple shard-fanout consumer: ListShards once at startup, spawn one
// goroutine per shard that loops GetRecords + decode + emit. EventID is
// "<stream>/<shard>/<sequenceNumber>", globally unique across the stream's history,
// suitable for at-least-once dedup at the state-store boundary.
//
// What's NOT in Phase 1:
//   - Lease management / shard rebalancing across multiple worker instances. Single-
//     instance only; for horizontally-scaled deployments use KCL v3 (Phase 2).
//   - Checkpointing. Each Read call starts from the configured StartingPosition
//     (LATEST / TRIM_HORIZON / AT_TIMESTAMP). At-least-once dedup at the state level
//     handles re-emission on restart, but lag accumulates if processing is paused
//     longer than the stream retention.
//   - Reshard handling. Splits and merges aren't observed mid-run; restart the
//     consumer to pick up the new shard topology.
//
// For production at scale, integrate awslabs/amazon-kinesis-client-go (KCL v3) — a
// Phase 2 upgrade that is API-compatible with this Source's contract.
package kinesis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/gallowaysoftware/murmur/pkg/source"
)

// Decoder converts a raw Kinesis record's Data to a typed Record value.
type Decoder[T any] func([]byte) (T, error)

// JSONDecoder returns a Decoder that unmarshals JSON into T.
func JSONDecoder[T any]() Decoder[T] {
	return func(b []byte) (T, error) {
		var v T
		if err := json.Unmarshal(b, &v); err != nil {
			return v, fmt.Errorf("kinesis json decode: %w", err)
		}
		return v, nil
	}
}

// Config configures a Kinesis Source.
type Config[T any] struct {
	Client     *kinesis.Client
	StreamName string
	Decode     Decoder[T]

	// StartingPosition controls where each shard begins. Defaults to LATEST.
	StartingPosition types.ShardIteratorType

	// StartTimestamp is required when StartingPosition is AT_TIMESTAMP.
	StartTimestamp *time.Time

	// PollInterval is the sleep between empty GetRecords calls per shard. Defaults
	// to 1s. Lower values reduce latency at the cost of higher API call volume.
	PollInterval time.Duration

	// RecordsPerCall caps GetRecords output. Default 1000 (the Kinesis maximum).
	RecordsPerCall int32
}

// Source implements source.Source for a Kinesis Data Stream.
type Source[T any] struct {
	client         *kinesis.Client
	streamName     string
	decode         Decoder[T]
	startingPos    types.ShardIteratorType
	startTimestamp *time.Time
	pollInterval   time.Duration
	recordsPerCall int32
}

// NewSource constructs a Kinesis Source. The returned Source does not own the client;
// callers manage its lifecycle.
func NewSource[T any](cfg Config[T]) (*Source[T], error) {
	if cfg.Client == nil {
		return nil, errors.New("kinesis source: Client is required")
	}
	if cfg.StreamName == "" {
		return nil, errors.New("kinesis source: StreamName is required")
	}
	if cfg.Decode == nil {
		return nil, errors.New("kinesis source: Decode is required")
	}
	pos := cfg.StartingPosition
	if pos == "" {
		pos = types.ShardIteratorTypeLatest
	}
	if pos == types.ShardIteratorTypeAtTimestamp && cfg.StartTimestamp == nil {
		return nil, errors.New("kinesis source: StartTimestamp required when StartingPosition is AT_TIMESTAMP")
	}
	poll := cfg.PollInterval
	if poll <= 0 {
		poll = time.Second
	}
	limit := cfg.RecordsPerCall
	if limit <= 0 {
		limit = 1000
	}
	return &Source[T]{
		client:         cfg.Client,
		streamName:     cfg.StreamName,
		decode:         cfg.Decode,
		startingPos:    pos,
		startTimestamp: cfg.StartTimestamp,
		pollInterval:   poll,
		recordsPerCall: limit,
	}, nil
}

// Read consumes all shards in parallel until ctx is canceled or every shard returns
// nil NextShardIterator (closed shards). Returns nil on graceful shutdown.
func (s *Source[T]) Read(ctx context.Context, out chan<- source.Record[T]) error {
	shards, err := s.listShards(ctx)
	if err != nil {
		return fmt.Errorf("list shards: %w", err)
	}
	var wg sync.WaitGroup
	for _, sh := range shards {
		wg.Add(1)
		go func(shard types.Shard) {
			defer wg.Done()
			s.consumeShard(ctx, shard, out)
		}(sh)
	}
	wg.Wait()
	return nil
}

// Name returns "kinesis:<stream>".
func (s *Source[T]) Name() string { return "kinesis:" + s.streamName }

// Close is a no-op; the underlying client is owned by the caller.
func (s *Source[T]) Close() error { return nil }

func (s *Source[T]) listShards(ctx context.Context) ([]types.Shard, error) {
	var all []types.Shard
	var token *string
	for {
		input := &kinesis.ListShardsInput{NextToken: token}
		if token == nil {
			input.StreamName = &s.streamName
		}
		out, err := s.client.ListShards(ctx, input)
		if err != nil {
			return nil, err
		}
		all = append(all, out.Shards...)
		if out.NextToken == nil {
			return all, nil
		}
		token = out.NextToken
	}
}

func (s *Source[T]) consumeShard(ctx context.Context, shard types.Shard, out chan<- source.Record[T]) {
	iter, err := s.getInitialIterator(ctx, *shard.ShardId)
	if err != nil {
		return
	}
	for {
		if ctx.Err() != nil {
			return
		}
		resp, err := s.client.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: &iter,
			Limit:         aws.Int32(s.recordsPerCall),
		})
		if err != nil {
			// Backoff on throughput / transient errors. Don't propagate; the source-
			// per-shard goroutine just sleeps and retries until ctx cancels.
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.pollInterval):
			}
			continue
		}
		for _, rec := range resp.Records {
			v, err := s.decode(rec.Data)
			if err != nil {
				continue
			}
			r := source.Record[T]{
				EventID:      fmt.Sprintf("%s/%s/%s", s.streamName, aws.ToString(shard.ShardId), aws.ToString(rec.SequenceNumber)),
				EventTime:    aws.ToTime(rec.ApproximateArrivalTimestamp),
				PartitionKey: aws.ToString(rec.PartitionKey),
				Value:        v,
				Ack:          func() error { return nil },
			}
			select {
			case out <- r:
			case <-ctx.Done():
				return
			}
		}
		if resp.NextShardIterator == nil {
			// Shard closed; we're done with it.
			return
		}
		iter = *resp.NextShardIterator
		if len(resp.Records) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.pollInterval):
			}
		}
	}
}

func (s *Source[T]) getInitialIterator(ctx context.Context, shardID string) (string, error) {
	input := &kinesis.GetShardIteratorInput{
		StreamName:        &s.streamName,
		ShardId:           &shardID,
		ShardIteratorType: s.startingPos,
	}
	if s.startingPos == types.ShardIteratorTypeAtTimestamp {
		input.Timestamp = s.startTimestamp
	}
	out, err := s.client.GetShardIterator(ctx, input)
	if err != nil {
		return "", err
	}
	if out.ShardIterator == nil {
		return "", fmt.Errorf("nil shard iterator for %s/%s", s.streamName, shardID)
	}
	return *out.ShardIterator, nil
}

// Compile-time check.
var _ source.Source[any] = (*Source[any])(nil)
