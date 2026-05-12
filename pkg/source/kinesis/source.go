// Package kinesis provides a Kinesis Data Streams source via aws-sdk-go-v2.
//
// PRODUCTION USERS: prefer pkg/exec/lambda/kinesis. AWS Lambda's Kinesis
// event-source mapping owns shard discovery, lease coordination, automatic
// scaling on shard count (via ParallelizationFactor), checkpointing, and
// partial-batch retry semantics — everything this package punts on. The
// Lambda handler shares the same retry / dedup / metrics core
// (pkg/exec/processor) as streaming.Run, so the pipeline definition is
// identical.
//
// This package is the polling ECS path, kept for dev/demo and for
// single-instance use cases where running a Lambda is unnecessary
// (integration tests, local one-shot consumers). It is NOT a production
// path: it is single-instance, has no checkpointing, and does not handle
// resharding mid-run.
//
// Implementation: ListShards once at startup, spawn one goroutine per
// shard that loops GetRecords + decode + emit. EventID is
// "<stream>/<shard>/<sequenceNumber>", globally unique across the stream's
// history, suitable for at-least-once dedup at the state-store boundary.
//
// What's missing for production scale (intentionally — Lambda owns it):
//   - Lease management / shard rebalancing across multiple workers.
//   - Checkpointing across restarts.
//   - Resharding mid-run.
//
// We do not plan to bring KCL v3 Go in-tree — the Lambda event-source
// mapping is the supported production path. If you need KCL semantics
// inside an ECS worker (e.g. for a long-running per-record cost that
// exceeds Lambda's 15-minute cap), wire awslabs/amazon-kinesis-client-go
// yourself against the source.Source contract.
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

	// OnDecodeError, if non-nil, is called for every record whose Decode returned
	// an error. Default behavior is to drop silently. Wire to a DLQ / metrics
	// recorder to surface poison pills.
	OnDecodeError func(raw []byte, shardID, sequenceNumber string, err error)

	// OnGetRecordsError, if non-nil, is called for every GetRecords error before
	// the per-shard loop backs off and retries. Wire to logging / metrics.
	OnGetRecordsError func(streamName, shardID string, err error)
}

// Source implements source.Source for a Kinesis Data Stream.
type Source[T any] struct {
	client            *kinesis.Client
	streamName        string
	decode            Decoder[T]
	startingPos       types.ShardIteratorType
	startTimestamp    *time.Time
	pollInterval      time.Duration
	recordsPerCall    int32
	onDecodeError     func(raw []byte, shardID, seq string, err error)
	onGetRecordsError func(streamName, shardID string, err error)
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
		client:            cfg.Client,
		streamName:        cfg.StreamName,
		decode:            cfg.Decode,
		startingPos:       pos,
		startTimestamp:    cfg.StartTimestamp,
		pollInterval:      poll,
		recordsPerCall:    limit,
		onDecodeError:     cfg.OnDecodeError,
		onGetRecordsError: cfg.OnGetRecordsError,
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
			if s.onGetRecordsError != nil {
				s.onGetRecordsError(s.streamName, aws.ToString(shard.ShardId), err)
			}
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
				if s.onDecodeError != nil {
					s.onDecodeError(rec.Data, aws.ToString(shard.ShardId), aws.ToString(rec.SequenceNumber), err)
				}
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
