// Package s3 provides a snapshot.Source that scans every JSON-Lines
// object under an S3 prefix and emits records for bootstrap. The
// canonical use case: bootstrap a Murmur pipeline from a partitioned
// archive — a Firehose-archived stream, a daily DDB export, or a
// Hive-style partitioned dump.
//
// Composes pkg/source/snapshot/jsonl (which decodes one io.Reader of
// JSON Lines) with the S3 ListObjectsV2 + GetObject + gzip pattern.
// Most users want this rather than the bare jsonl source — the prefix-
// scan plus per-object gzip handling is what makes the S3 case
// operationally usable.
//
// # Key ordering
//
// S3 ListObjectsV2 returns keys in lexicographic order. For Firehose
// archives partitioned as `prefix/year=2026/month=05/day=08/`, the
// resulting scan order is chronological, which is the natural order
// for replay. For other partitioning schemes, callers can pre-filter
// the key list via Config.KeyFilter.
//
// # Gzipped objects
//
// Keys ending in `.gz` are auto-decompressed via gzip.NewReader before
// being passed through to the JSON Lines decoder. Other compressions
// (snappy, zstd) require a custom Config.OpenObject hook.
package s3

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot/jsonl"
)

// Config configures an S3 prefix-scanning snapshot Source.
type Config[T any] struct {
	// Client is an SDK v2 S3 client. The Source does not own its
	// lifecycle.
	Client *awss3.Client

	// Bucket is the S3 bucket to scan.
	Bucket string

	// Prefix narrows the scan (e.g. "events/year=2026/month=05/").
	// Empty means the entire bucket — usually undesirable.
	Prefix string

	// Decode is the per-line JSON-Lines decoder. Defaults to
	// jsonl.DefaultDecoder[T].
	Decode jsonl.Decoder[T]

	// EventID derives the dedup key per record. Receives the source
	// object key + line number for context. Default: "<key>:<line>".
	EventID func(decoded T, key string, lineNum int) string

	// KeyFilter, when non-nil, is called for every listed object key
	// and must return true for keys that should be scanned. Use this
	// to skip non-data objects (manifests, _SUCCESS markers) or to
	// restrict the scan to a date range without changing the prefix.
	KeyFilter func(key string) bool

	// OnDecodeError is forwarded to the underlying jsonl source.
	OnDecodeError func(key string, lineNum int, line []byte, err error)

	// HandoffToken is what CaptureHandoff returns. Same caller-supplied
	// pattern as jsonl: the live-source resume position is captured
	// externally (e.g., when generating the S3 archive).
	HandoffToken snapshot.HandoffToken

	// MaxLineSize is forwarded to the jsonl source. Defaults to 1 MB.
	MaxLineSize int

	// OpenObject, when non-nil, overrides the default GetObject + gzip
	// handling. Use for custom compression (snappy, zstd) or for tests
	// that want to inject a static body. The returned Closer is closed
	// after the per-object scan completes.
	OpenObject func(ctx context.Context, key string) (io.ReadCloser, error)
}

// Source implements snapshot.Source[T] over an S3 prefix.
type Source[T any] struct {
	cfg Config[T]
}

// NewSource constructs the Source. Validates required fields.
func NewSource[T any](cfg Config[T]) (*Source[T], error) {
	if cfg.Client == nil && cfg.OpenObject == nil {
		return nil, errors.New("s3 snapshot: Client or OpenObject is required")
	}
	if cfg.Bucket == "" && cfg.OpenObject == nil {
		return nil, errors.New("s3 snapshot: Bucket is required")
	}
	if cfg.Decode == nil {
		cfg.Decode = jsonl.DefaultDecoder[T]()
	}
	return &Source[T]{cfg: cfg}, nil
}

// Name returns "s3:<bucket>/<prefix>".
func (s *Source[T]) Name() string {
	return "s3:" + s.cfg.Bucket + "/" + s.cfg.Prefix
}

// Close is a no-op; the underlying S3 client is owned by the caller.
func (s *Source[T]) Close() error { return nil }

// CaptureHandoff returns the configured handoff token, or nil.
func (s *Source[T]) CaptureHandoff(_ context.Context) (snapshot.HandoffToken, error) {
	return s.cfg.HandoffToken, nil
}

// Scan lists the prefix and scans each object's lines through the
// configured decoder, emitting records into out. Returns nil when
// every object has been consumed; non-nil on the first fatal error.
//
// Per-object decode errors fire OnDecodeError but do NOT abort — a
// single poison line in a 100-object archive shouldn't fail the
// bootstrap.
func (s *Source[T]) Scan(ctx context.Context, out chan<- source.Record[T]) error {
	keys, err := s.listKeys(ctx)
	if err != nil {
		return fmt.Errorf("s3 list %s/%s: %w", s.cfg.Bucket, s.cfg.Prefix, err)
	}
	for _, key := range keys {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := s.scanOne(ctx, key, out); err != nil {
			return fmt.Errorf("s3 scan %s: %w", key, err)
		}
	}
	return nil
}

// Resume restarts the scan from the beginning. At-least-once dedup at
// bootstrap.Run absorbs duplicate emissions; mid-prefix resumption
// would require persisting a per-key checkpoint and is left as future
// work.
func (s *Source[T]) Resume(ctx context.Context, _ []byte, out chan<- source.Record[T]) error {
	return s.Scan(ctx, out)
}

func (s *Source[T]) listKeys(ctx context.Context) ([]string, error) {
	if s.cfg.OpenObject != nil && s.cfg.Client == nil {
		// OpenObject-only mode: caller supplies a fixed key list via
		// the prefix (single key) or composes their own listing.
		// Treat the prefix itself as the single object key.
		return []string{s.cfg.Prefix}, nil
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
			if s.cfg.KeyFilter != nil && !s.cfg.KeyFilter(key) {
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

func (s *Source[T]) scanOne(ctx context.Context, key string, out chan<- source.Record[T]) error {
	body, err := s.openObject(ctx, key)
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()

	reader := io.Reader(body)
	if strings.HasSuffix(strings.ToLower(key), ".gz") {
		gz, err := gzip.NewReader(body)
		if err != nil {
			return fmt.Errorf("gzip open %s: %w", key, err)
		}
		defer func() { _ = gz.Close() }()
		reader = gz
	}

	subSrc, err := jsonl.NewSource(jsonl.Config[T]{
		Reader:      reader,
		Name:        key,
		Decode:      s.cfg.Decode,
		MaxLineSize: s.cfg.MaxLineSize,
		EventID: func(decoded T, lineNum int) string {
			if s.cfg.EventID != nil {
				return s.cfg.EventID(decoded, key, lineNum)
			}
			return key + ":" + jsonlLineKey(lineNum)
		},
		OnDecodeError: func(line []byte, lineNum int, err error) {
			if s.cfg.OnDecodeError != nil {
				s.cfg.OnDecodeError(key, lineNum, line, err)
			}
		},
	})
	if err != nil {
		return fmt.Errorf("jsonl new %s: %w", key, err)
	}
	if err := subSrc.Scan(ctx, out); err != nil {
		return fmt.Errorf("jsonl scan %s: %w", key, err)
	}
	return nil
}

func (s *Source[T]) openObject(ctx context.Context, key string) (io.ReadCloser, error) {
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
	return resp.Body, nil
}

// jsonlLineKey is a tiny helper that avoids pulling strconv in just for
// formatting a positive int. Mirrors the format jsonl uses internally.
func jsonlLineKey(lineNum int) string {
	if lineNum == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	n := lineNum
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// Compile-time check.
var _ snapshot.Source[any] = (*Source[any])(nil)
