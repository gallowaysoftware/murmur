// Package dynamodb provides a snapshot.Source backed by DynamoDB
// ParallelScan, suitable for bootstrapping a Murmur pipeline from a
// DDB-resident OLTP table.
//
// The companion live source for the same shop is the DDB Streams Lambda
// runtime (pkg/exec/lambda/dynamodbstreams). Together they give you the
// canonical AWS-native CDC story:
//
//	this Source     → bootstrap.Run    → populate initial state
//	DDB Streams →     dynamodbstreams.Lambda → keep state live
//
// # ParallelScan
//
// DDB ParallelScan partitions the table into N segments and reads them
// concurrently. For a 100M-row table, parallelism cuts the scan from
// "an afternoon" to "a few hours" depending on RCU provisioning. The
// implementation here owns N goroutines, each scanning one segment and
// emitting records into the shared output channel.
//
// # Handoff token
//
// CaptureHandoff returns a DDB Streams shard-iterator position
// captured before the scan starts. The deployment system stores it and
// hands it to the live Lambda's event-source mapping as
// `StartingPositionTimestamp` (or equivalent).
//
// Streams shard iterators expire after ~5 minutes if unused, so the
// handoff token here is the SHARD ID + a timestamp, not the iterator
// itself; the live runtime calls GetShardIterator with the captured
// timestamp at startup.
//
// # Resumption
//
// Each segment scan returns a `LastEvaluatedKey` per page. Saving these
// per-segment is straightforward but adds a per-batch checkpoint write
// — for moderate-size tables (<= 100M rows) the simpler "restart from
// the beginning, dedup catches it" approach is what production teams
// pick. Resume() is provided for symmetry with the snapshot.Source
// contract but currently restarts from segment-0 — at-least-once dedup
// at the bootstrap.Run level absorbs the duplicate emissions.
package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot"
)

// Decoder converts a DDB item (the AttributeValue map from a scan
// response) into the pipeline's input type T. The most common shape:
// use aws/aws-sdk-go-v2/feature/dynamodb/attributevalue.UnmarshalMap
// to decode into a typed struct.
type Decoder[T any] func(map[string]types.AttributeValue) (T, error)

// EventIDFn extracts a stable per-record dedup key from the decoded
// value. For tables with a single partition key, returning the
// partition key's string form is the canonical choice — that way a
// re-run of the same scan folds idempotently under
// bootstrap.WithDedup. Default: a synthetic "<segment>:<index>" pair
// per record, which is unique within a single scan but NOT across
// re-runs (because segment work distribution can shift).
type EventIDFn[T any] func(decoded T, item map[string]types.AttributeValue) string

// Config configures a DDB-table snapshot.Source.
type Config[T any] struct {
	// Client is an AWS SDK v2 DynamoDB client. The Source does not own
	// the client's lifecycle.
	Client *dynamodb.Client

	// StreamsClient is the SDK v2 DynamoDB Streams client used to
	// capture the live-source handoff token at scan start. Required when
	// the table has Streams enabled and you want a gap-and-duplicate-
	// free bootstrap → live transition. May be nil to skip handoff
	// capture; in that case CaptureHandoff returns nil token.
	StreamsClient *dynamodbstreams.Client

	// TableName is the DDB table to scan.
	TableName string

	// StreamARN, when non-empty, identifies the Streams ARN whose shard
	// iterator the handoff token should reference. Only meaningful when
	// StreamsClient is also set.
	StreamARN string

	// Segments is the number of parallel scan segments. DDB caps at
	// 1,000,000; in practice 4–32 is the right range for most tables.
	// For a 100M-row table at 1k RCU/s/partition with eventually-
	// consistent reads, segments=8 typically saturates the scan budget.
	// Defaults to 1 (sequential scan).
	Segments int

	// Decode converts a DDB item to T. Required.
	Decode Decoder[T]

	// EventID derives the dedup key per record. Default: synthetic
	// segment:index — unique within one scan but NOT across re-runs.
	// For idempotent re-runs, return the natural primary key.
	EventID EventIDFn[T]

	// PageSize caps Items per Scan call. Defaults to 1000 (DDB max).
	// Lower for memory-constrained workers consuming wide rows.
	PageSize int32

	// FilterExpression, when non-empty, is applied server-side. Saves
	// network bytes and processing for tables where bootstrap should
	// only see a subset of rows.
	FilterExpression string

	// FilterValues / FilterNames support FilterExpression substitutions.
	FilterValues map[string]types.AttributeValue
	FilterNames  map[string]string

	// OnDecodeError, when non-nil, is invoked for items that fail to
	// decode. Default behavior: drop silently. Wire to a DLQ for
	// poison-row visibility.
	OnDecodeError func(item map[string]types.AttributeValue, err error)
}

// Source implements snapshot.Source against a DDB ParallelScan.
type Source[T any] struct {
	cfg Config[T]
}

// NewSource constructs the Source. Validates required fields.
func NewSource[T any](cfg Config[T]) (*Source[T], error) {
	if cfg.Client == nil {
		return nil, errors.New("dynamodb snapshot: Client is required")
	}
	if cfg.TableName == "" {
		return nil, errors.New("dynamodb snapshot: TableName is required")
	}
	if cfg.Decode == nil {
		return nil, errors.New("dynamodb snapshot: Decode is required")
	}
	if cfg.Segments <= 0 {
		cfg.Segments = 1
	}
	if cfg.PageSize <= 0 {
		cfg.PageSize = 1000
	}
	return &Source[T]{cfg: cfg}, nil
}

// Name returns "ddb-snapshot:<table>".
func (s *Source[T]) Name() string { return "ddb-snapshot:" + s.cfg.TableName }

// Close is a no-op; the SDK clients are owned by the caller.
func (s *Source[T]) Close() error { return nil }

// CaptureHandoff captures the DDB Streams shard iterator position the
// live consumer should resume from. Called once at bootstrap start.
//
// If StreamsClient or StreamARN is unset, returns a nil token — caller
// should ensure the live source has its own way to skip past the
// already-bootstrapped data (e.g., starting from LATEST and accepting
// the duplicate-event window covered by dedup).
func (s *Source[T]) CaptureHandoff(ctx context.Context) (snapshot.HandoffToken, error) {
	if s.cfg.StreamsClient == nil || s.cfg.StreamARN == "" {
		return nil, nil
	}
	desc, err := s.cfg.StreamsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(s.cfg.StreamARN),
	})
	if err != nil {
		return nil, fmt.Errorf("describe stream %s: %w", s.cfg.StreamARN, err)
	}
	// The token captures (now, every-active-shard). Live consumer iterates
	// each shard from this timestamp. Encoding here is intentionally
	// minimal — a JSON blob of (timestamp, shard IDs) — which the live
	// runtime decodes when starting up.
	now := time.Now().UTC()
	token := encodeHandoff(now, desc.StreamDescription.Shards)
	_ = streamtypes.Shard{} // pull in the streamtypes import dependency for clarity
	return token, nil
}

// Scan runs the parallel scan, emitting records via out. Returns nil
// when all segments complete; non-nil on the first segment error.
func (s *Source[T]) Scan(ctx context.Context, out chan<- source.Record[T]) error {
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	wg.Add(s.cfg.Segments)
	for seg := 0; seg < s.cfg.Segments; seg++ {
		go func(segment int) {
			defer wg.Done()
			if err := s.scanSegment(ctx, segment, out); err != nil {
				errOnce.Do(func() { firstErr = err })
			}
		}(seg)
	}
	wg.Wait()
	return firstErr
}

// Resume restarts the scan from the beginning. See package doc — at-
// least-once dedup at bootstrap.Run absorbs the duplicate emissions.
// A future variant could decode `marker` to per-segment LastEvaluatedKey
// state for true mid-scan resumption.
func (s *Source[T]) Resume(ctx context.Context, _ []byte, out chan<- source.Record[T]) error {
	return s.Scan(ctx, out)
}

func (s *Source[T]) scanSegment(ctx context.Context, segment int, out chan<- source.Record[T]) error {
	var startKey map[string]types.AttributeValue
	idx := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		input := &dynamodb.ScanInput{
			TableName:         aws.String(s.cfg.TableName),
			Segment:           aws.Int32(int32(segment)),
			TotalSegments:     aws.Int32(int32(s.cfg.Segments)),
			Limit:             aws.Int32(s.cfg.PageSize),
			ExclusiveStartKey: startKey,
		}
		if s.cfg.FilterExpression != "" {
			input.FilterExpression = aws.String(s.cfg.FilterExpression)
			input.ExpressionAttributeValues = s.cfg.FilterValues
			input.ExpressionAttributeNames = s.cfg.FilterNames
		}
		resp, err := s.cfg.Client.Scan(ctx, input)
		if err != nil {
			return fmt.Errorf("ddb scan %s segment %d: %w", s.cfg.TableName, segment, err)
		}
		for _, item := range resp.Items {
			value, err := s.cfg.Decode(item)
			if err != nil {
				if s.cfg.OnDecodeError != nil {
					s.cfg.OnDecodeError(item, err)
				}
				continue
			}
			eventID := s.eventID(value, item, segment, idx)
			rec := source.Record[T]{
				EventID:   eventID,
				EventTime: time.Now(), // bootstrap timestamps are processing-time
				Value:     value,
				Ack:       func() error { return nil },
			}
			select {
			case out <- rec:
			case <-ctx.Done():
				return ctx.Err()
			}
			idx++
		}
		if len(resp.LastEvaluatedKey) == 0 {
			return nil // segment exhausted
		}
		startKey = resp.LastEvaluatedKey
	}
}

func (s *Source[T]) eventID(value T, item map[string]types.AttributeValue, segment, idx int) string {
	if s.cfg.EventID != nil {
		if id := s.cfg.EventID(value, item); id != "" {
			return id
		}
	}
	return fmt.Sprintf("ddb-snapshot:%s:%d:%d", s.cfg.TableName, segment, idx)
}

// encodeHandoff renders the token as a small JSON-ish blob. The live
// runtime decodes it via DecodeHandoff. Format is internal; not a
// stable wire contract.
func encodeHandoff(at time.Time, shards []streamtypes.Shard) snapshot.HandoffToken {
	// Compact stringification: <unix-nanos>|<shard1>|<shard2>...
	// Consumer parses by splitting on '|'.
	var b []byte
	b = append(b, []byte(time.RFC3339Nano)...)
	b = append(b, '|')
	b = append(b, []byte(at.Format(time.RFC3339Nano))...)
	for _, sh := range shards {
		b = append(b, '|')
		if sh.ShardId != nil {
			b = append(b, []byte(*sh.ShardId)...)
		}
	}
	return snapshot.HandoffToken(b)
}

// Compile-time check.
var _ snapshot.Source[any] = (*Source[any])(nil)
