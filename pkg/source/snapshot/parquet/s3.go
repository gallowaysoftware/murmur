// Package parquet — S3 prefix scanner with Hive-style partition
// discovery and predicate-pushdown.
//
// Spark writes partitioned datasets as
//
//	s3://bucket/prefix/year=2026/month=05/day=08/hour=14/part-00000.parquet
//
// where the partition columns are embedded in the key path (Hive
// convention). This source lists all `.parquet` objects under a
// prefix, parses the partition columns out of each key, optionally
// drops whole partitions via a caller-supplied predicate, and decodes
// the remaining objects through pkg/source/snapshot/parquet#Source.
//
// # Concurrency
//
// Object fetches run in parallel up to MaxConcurrency (default 4).
// Decoded records still arrive on the single out channel in
// roughly-arrival order from the bounded worker pool — not
// strictly partition-sorted. Bootstrap callers should not depend on
// arrival order; downstream monoid combine is commutative.
package parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot"
)

// Partition is the set of Hive-style partition columns parsed from a
// single Parquet object key. Only columns present in the key path are
// populated; absent columns hold the zero string. Values are kept as
// strings — the caller's PartitionFilter does its own type conversion.
//
// Multi-segment keys (e.g.
// `events/year=2026/month=05/day=08/hour=14/part.parquet`) yield
// {Key: "events/.../part.parquet", Values: {"year": "2026",
// "month": "05", "day": "08", "hour": "14"}}.
type Partition struct {
	// Key is the full S3 object key.
	Key string

	// Values maps each `name=value` segment found in the key path
	// (relative to Config.Prefix) to its value.
	Values map[string]string
}

// PartitionFilter, when non-nil on Config, is invoked once per
// discovered object. Return false to skip the object entirely (no
// GetObject, no decode). Use this to prune whole partitions by
// time-range without listing them out of S3 (S3 ListObjectsV2 is
// already filtered by prefix, but Hive partitioning typically wants
// finer-grained pruning).
type PartitionFilter func(p Partition) bool

// S3Config configures an S3-prefix Parquet snapshot Source.
type S3Config[T any] struct {
	// Client is an SDK v2 S3 client. The Source does not own its
	// lifecycle.
	Client *awss3.Client

	// Bucket is the S3 bucket to scan.
	Bucket string

	// Prefix narrows the scan, e.g. "events/". Empty means the entire
	// bucket — usually undesirable. Hive-style partition segments
	// (`year=...`) under this prefix are auto-discovered.
	Prefix string

	// Decode is the per-row Arrow decoder. Required; see
	// parquet.Decoder.
	Decode Decoder[T]

	// EventID derives the dedup key per record. Receives the object
	// key + row ordinal within that object. Default:
	// "<key>:<row-ordinal>".
	EventID func(decoded T, key string, rowOrdinal int) string

	// EventTime extracts the upstream event time from a decoded row.
	// Optional; default is processing time at decode.
	EventTime EventTimeFn[T]

	// PartitionFilter, when non-nil, is invoked once per discovered
	// object. Return false to skip — no GetObject, no decode. Use
	// for time-range pruning under a static prefix.
	PartitionFilter PartitionFilter

	// KeyFilter, when non-nil, is a low-level pre-PartitionFilter
	// hook. Receives the raw key and must return true to keep it.
	// Use this to drop control objects like `_SUCCESS` markers or
	// `_committed_*` files that Spark writes alongside the data.
	// Defaults to skipping anything that doesn't end in `.parquet`.
	KeyFilter func(key string) bool

	// OnDecodeError is forwarded to the underlying Parquet source for
	// each object. Receives the object key + row ordinal + error.
	OnDecodeError func(key string, rowOrdinal int, err error)

	// MaxConcurrency caps the number of concurrent object reads.
	// Default 4. Capped at len(matched keys).
	MaxConcurrency int

	// BatchSize is forwarded to the per-file source as the Arrow
	// record-batch size. Defaults to 4096.
	BatchSize int64

	// HandoffToken is what CaptureHandoff returns. Same pattern as
	// the other snapshot sources: caller is responsible for capturing
	// it before the Parquet dataset was generated.
	HandoffToken snapshot.HandoffToken

	// OpenObject, when non-nil, overrides the default GetObject path.
	// Use for tests that want to inject deterministic file bodies
	// without a live S3 client. The returned bytes must be the
	// complete Parquet file (the Arrow library needs ReaderAt + seek,
	// so partial / streaming bodies aren't supported).
	OpenObject func(ctx context.Context, key string) ([]byte, error)
}

// S3Source implements snapshot.Source[T] over an S3 prefix of
// Hive-partitioned Parquet objects.
type S3Source[T any] struct {
	cfg S3Config[T]
}

// NewS3Source constructs the S3Source. Validates required fields.
func NewS3Source[T any](cfg S3Config[T]) (*S3Source[T], error) {
	if cfg.Client == nil && cfg.OpenObject == nil {
		return nil, errors.New("parquet s3 snapshot: Client or OpenObject is required")
	}
	if cfg.Bucket == "" && cfg.OpenObject == nil {
		return nil, errors.New("parquet s3 snapshot: Bucket is required")
	}
	if cfg.Decode == nil {
		return nil, errors.New("parquet s3 snapshot: Decode is required")
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 4
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 4096
	}
	if cfg.KeyFilter == nil {
		cfg.KeyFilter = defaultParquetKeyFilter
	}
	return &S3Source[T]{cfg: cfg}, nil
}

// Name returns "parquet-s3:<bucket>/<prefix>".
func (s *S3Source[T]) Name() string {
	return "parquet-s3:" + s.cfg.Bucket + "/" + s.cfg.Prefix
}

// Close is a no-op; the underlying S3 client is owned by the caller.
func (s *S3Source[T]) Close() error { return nil }

// CaptureHandoff returns the configured handoff token, or nil.
func (s *S3Source[T]) CaptureHandoff(_ context.Context) (snapshot.HandoffToken, error) {
	return s.cfg.HandoffToken, nil
}

// Scan lists the prefix, applies KeyFilter and PartitionFilter, and
// decodes each surviving Parquet object in parallel (bounded by
// MaxConcurrency). Per-object errors abort the scan; per-row decode
// errors fire OnDecodeError without aborting.
func (s *S3Source[T]) Scan(ctx context.Context, out chan<- source.Record[T]) error {
	keys, err := s.listKeys(ctx)
	if err != nil {
		return fmt.Errorf("parquet s3 list %s/%s: %w", s.cfg.Bucket, s.cfg.Prefix, err)
	}
	if len(keys) == 0 {
		return nil
	}

	partitions := make([]Partition, 0, len(keys))
	for _, k := range keys {
		p := parseHivePartition(k)
		if s.cfg.PartitionFilter != nil && !s.cfg.PartitionFilter(p) {
			continue
		}
		partitions = append(partitions, p)
	}
	if len(partitions) == 0 {
		return nil
	}

	concurrency := s.cfg.MaxConcurrency
	if concurrency > len(partitions) {
		concurrency = len(partitions)
	}

	work := make(chan Partition)
	errCh := make(chan error, concurrency)
	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range work {
				if ctx.Err() != nil {
					return
				}
				if err := s.scanOne(ctx, p, out); err != nil {
					select {
					case errCh <- fmt.Errorf("parquet s3 scan %s: %w", p.Key, err):
					default:
					}
					cancel()
					return
				}
			}
		}()
	}

	go func() {
		defer close(work)
		for _, p := range partitions {
			select {
			case work <- p:
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for e := range errCh {
		if e != nil {
			return e
		}
	}
	if ctx.Err() != nil && !errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}
	return nil
}

// Resume restarts the scan from the beginning. At-least-once dedup at
// bootstrap.Run absorbs duplicate emissions.
func (s *S3Source[T]) Resume(ctx context.Context, _ []byte, out chan<- source.Record[T]) error {
	return s.Scan(ctx, out)
}

func (s *S3Source[T]) listKeys(ctx context.Context) ([]string, error) {
	if s.cfg.Client == nil {
		// Pure OpenObject mode (tests): keys are passed in via Prefix
		// as a comma-separated list when the caller has no live S3.
		// Real production use always supplies a Client.
		if s.cfg.Prefix == "" {
			return nil, nil
		}
		raw := strings.Split(s.cfg.Prefix, ",")
		keys := make([]string, 0, len(raw))
		for _, k := range raw {
			k = strings.TrimSpace(k)
			if k != "" && s.cfg.KeyFilter(k) {
				keys = append(keys, k)
			}
		}
		return keys, nil
	}

	var keys []string
	var continuationToken *string
	for {
		out, err := s.cfg.Client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket:            aws.String(s.cfg.Bucket),
			Prefix:            aws.String(s.cfg.Prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, err
		}
		for _, obj := range out.Contents {
			if obj.Key == nil {
				continue
			}
			key := *obj.Key
			if !s.cfg.KeyFilter(key) {
				continue
			}
			keys = append(keys, key)
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		continuationToken = out.NextContinuationToken
	}
	return keys, nil
}

func (s *S3Source[T]) scanOne(ctx context.Context, p Partition, out chan<- source.Record[T]) error {
	body, err := s.openObject(ctx, p.Key)
	if err != nil {
		return err
	}
	reader := bytes.NewReader(body)

	subSrc, err := NewSource(Config[T]{
		Reader:    reader,
		Name:      p.Key,
		Decode:    s.cfg.Decode,
		BatchSize: s.cfg.BatchSize,
		EventTime: s.cfg.EventTime,
		EventID: func(decoded T, rowOrdinal int) string {
			if s.cfg.EventID != nil {
				return s.cfg.EventID(decoded, p.Key, rowOrdinal)
			}
			return p.Key + ":" + strconv.Itoa(rowOrdinal)
		},
		OnDecodeError: func(rowOrdinal int, err error) {
			if s.cfg.OnDecodeError != nil {
				s.cfg.OnDecodeError(p.Key, rowOrdinal, err)
			}
		},
	})
	if err != nil {
		return fmt.Errorf("parquet new %s: %w", p.Key, err)
	}
	return subSrc.Scan(ctx, out)
}

func (s *S3Source[T]) openObject(ctx context.Context, key string) ([]byte, error) {
	if s.cfg.OpenObject != nil {
		return s.cfg.OpenObject(ctx, key)
	}
	resp, err := s.cfg.Client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 GetObject %s: %w", key, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("s3 ReadAll %s: %w", key, err)
	}
	return body, nil
}

// parseHivePartition extracts Hive-style `name=value` pairs from every
// segment of `key`. Segments that don't match `name=value` are ignored.
// The final segment (the filename) is also ignored as it never carries
// partition info.
//
// Example:
//
//	key="events/year=2026/month=05/day=08/part-0.parquet"
//	→ Partition{Key: key, Values: {"year": "2026", "month": "05", "day": "08"}}
//
// The Config.Prefix is intentionally not used for stripping: Hive's
// `name=value` convention is unambiguous regardless of where the
// partition columns appear in the path. This keeps the parser correct
// when callers use a deeper Prefix to limit the listing (e.g.
// `events/year=2026/` to scan a single year).
func parseHivePartition(key string) Partition {
	values := make(map[string]string)
	parts := strings.Split(key, "/")
	// Skip the last segment (filename).
	for i := 0; i < len(parts)-1; i++ {
		seg := parts[i]
		eq := strings.IndexByte(seg, '=')
		if eq <= 0 || eq == len(seg)-1 {
			continue
		}
		name := seg[:eq]
		value := seg[eq+1:]
		values[name] = value
	}
	return Partition{Key: key, Values: values}
}

// defaultParquetKeyFilter keeps only objects ending in `.parquet`
// (case-insensitive). Spark writes `_SUCCESS`, `_committed_*`,
// `_started_*` control files alongside data; those would error on
// decode if we tried to open them.
func defaultParquetKeyFilter(key string) bool {
	return strings.HasSuffix(strings.ToLower(key), ".parquet")
}

// Compile-time check.
var _ snapshot.Source[any] = (*S3Source[any])(nil)
