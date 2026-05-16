package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/gallowaysoftware/murmur/pkg/source"
)

// ParquetDecoder converts a single Parquet row (column-array + row index)
// into T. Implementations should pull only the columns they need from
// the arrow.Record by name; cross-record allocations are the caller's
// responsibility because the Record is reused after the call returns.
type ParquetDecoder[T any] func(rec arrow.Record, row int) (T, error)

// ParquetConfig configures a Parquet S3 replay driver.
type ParquetConfig[T any] struct {
	// Client is the S3 client. Required.
	Client *awss3.Client

	// Bucket is the S3 bucket. Required.
	Bucket string

	// Prefix narrows the object scan (e.g. "events/year=2026/month=05/").
	// Empty means the entire bucket.
	Prefix string

	// Decode converts a single Arrow row into T. Required.
	Decode ParquetDecoder[T]

	// KeyFilter, when non-nil, decides whether to include a listed key.
	// Default behavior includes every key ending in ".parquet"; pass a
	// custom KeyFilter to broaden ("*.parquet.snappy") or narrow
	// ("only files newer than X").
	KeyFilter func(key string) bool

	// BatchSize is the number of rows materialized per arrow.Record
	// pulled out of Parquet. Defaults to 4096.
	BatchSize int64

	// Allocator is the Arrow memory allocator. Defaults to
	// memory.DefaultAllocator.
	Allocator memory.Allocator

	// OnDecodeError, when non-nil, is invoked for rows that fail to
	// decode. Default: drop silently and continue. Wire to a DLQ or
	// logger to surface poison rows.
	OnDecodeError func(key string, rowOrdinal int, err error)
}

// ParquetDriver implements replay.Driver for S3-archived Parquet event
// streams. The standard "kappa replay" shape for pipelines whose
// upstream archive is Spark-produced or Firehose-with-Parquet-format —
// 5-20x smaller than gzipped JSON Lines on the same data, predicate-
// pushdown-friendly, native to every modern columnar engine.
//
// For ad-hoc / hand-built JSON dumps, the line-based Driver in this
// same package remains the right choice. Use this Parquet driver when
// the archive was written by a columnar engine.
type ParquetDriver[T any] struct {
	client        *awss3.Client
	bucket        string
	prefix        string
	decode        ParquetDecoder[T]
	keyFilter     func(string) bool
	batchSize     int64
	allocator     memory.Allocator
	onDecodeError func(key string, rowOrdinal int, err error)
}

// NewParquetDriver returns a ParquetDriver. The S3 client is owned by
// the caller.
func NewParquetDriver[T any](cfg ParquetConfig[T]) (*ParquetDriver[T], error) {
	if cfg.Client == nil {
		return nil, errors.New("s3 parquet replay: Client is required")
	}
	if cfg.Bucket == "" {
		return nil, errors.New("s3 parquet replay: Bucket is required")
	}
	if cfg.Decode == nil {
		return nil, errors.New("s3 parquet replay: Decode is required")
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 4096
	}
	if cfg.Allocator == nil {
		cfg.Allocator = memory.DefaultAllocator
	}
	if cfg.KeyFilter == nil {
		cfg.KeyFilter = defaultParquetKeyFilter
	}
	return &ParquetDriver[T]{
		client:        cfg.Client,
		bucket:        cfg.Bucket,
		prefix:        cfg.Prefix,
		decode:        cfg.Decode,
		keyFilter:     cfg.KeyFilter,
		batchSize:     cfg.BatchSize,
		allocator:     cfg.Allocator,
		onDecodeError: cfg.OnDecodeError,
	}, nil
}

// Replay enumerates every object under (Bucket, Prefix), opens each one
// that passes KeyFilter as Parquet, and emits one source.Record per row
// via the configured Decoder.
func (d *ParquetDriver[T]) Replay(ctx context.Context, out chan<- source.Record[T]) error {
	keys, err := d.listAll(ctx)
	if err != nil {
		return fmt.Errorf("s3 parquet list: %w", err)
	}
	for _, key := range keys {
		if !d.keyFilter(key) {
			continue
		}
		if err := d.replayOne(ctx, key, out); err != nil {
			return err
		}
	}
	return nil
}

func (d *ParquetDriver[T]) listAll(ctx context.Context) ([]string, error) {
	var keys []string
	var token *string
	for {
		out, err := d.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket:            &d.bucket,
			Prefix:            stringPtrOrNil(d.prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, obj := range out.Contents {
			if obj.Key == nil {
				continue
			}
			if strings.HasSuffix(aws.ToString(obj.Key), "/") {
				continue
			}
			keys = append(keys, aws.ToString(obj.Key))
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		token = out.NextContinuationToken
	}
	return keys, nil
}

func (d *ParquetDriver[T]) replayOne(ctx context.Context, key string, out chan<- source.Record[T]) error {
	// Parquet readers need io.ReaderAt over the full object — Arrow's
	// reader does multiple seeks across the file footer + row groups,
	// not a sequential stream. Buffer the body in memory; a typical
	// Spark-produced parquet part-file is well under 256 MiB, which is
	// what most replay workers can sustain.
	resp, err := d.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: &d.bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("s3 GetObject %s/%s: %w", d.bucket, key, err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return fmt.Errorf("s3 read body %s/%s: %w", d.bucket, key, err)
	}
	reader := bytes.NewReader(body)

	pr, err := file.NewParquetReader(reader)
	if err != nil {
		return fmt.Errorf("parquet open %s: %w", key, err)
	}
	defer func() { _ = pr.Close() }()

	arr, err := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{
		BatchSize: d.batchSize,
	}, d.allocator)
	if err != nil {
		return fmt.Errorf("parquet arrow-reader %s: %w", key, err)
	}
	rr, err := arr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return fmt.Errorf("parquet record-reader %s: %w", key, err)
	}
	defer rr.Release()

	var rowOrdinal int
	for rr.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rec := rr.Record()
		if rec == nil {
			continue
		}
		nrows := int(rec.NumRows())
		for r := 0; r < nrows; r++ {
			rowOrdinal++
			v, derr := d.decode(rec, r)
			if derr != nil {
				if d.onDecodeError != nil {
					d.onDecodeError(key, rowOrdinal, derr)
				}
				continue
			}
			outRec := source.Record[T]{
				EventID:      fmt.Sprintf("s3-parquet:%s/%s:%d", d.bucket, key, rowOrdinal),
				PartitionKey: key,
				Value:        v,
				Ack:          func() error { return nil },
			}
			select {
			case out <- outRec:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	if err := rr.Err(); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("parquet read %s: %w", key, err)
	}
	return nil
}

// Name returns "s3-parquet:<bucket>/<prefix>".
func (d *ParquetDriver[T]) Name() string {
	return fmt.Sprintf("s3-parquet:%s/%s", d.bucket, d.prefix)
}

// Close is a no-op; the underlying client is owned by the caller.
func (d *ParquetDriver[T]) Close() error { return nil }

// defaultParquetKeyFilter includes any key ending in ".parquet". This
// rejects Spark's _SUCCESS / _committed_*.json sentinels as well as the
// JSON-Lines driver's siblings, so the same bucket can hold both
// formats without collision.
func defaultParquetKeyFilter(key string) bool {
	return strings.HasSuffix(key, ".parquet")
}
