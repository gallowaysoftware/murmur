// Package parquet provides a snapshot.Source that reads Apache Parquet
// from any io.ReaderAt — local files, S3 objects fetched into memory,
// in-memory bytes from tests, etc. The canonical use case: bootstrap a
// Murmur pipeline from a Spark-produced backfill written to S3.
//
// Parquet is the recommended interchange format when the upstream is
// Spark (or any columnar engine): 5–20x smaller than gzipped JSON
// Lines, predicate-pushdown-friendly, and native to every modern
// distributed query engine. JSON Lines (pkg/source/snapshot/jsonl) is
// still the right choice for ad-hoc / hand-built dumps and database
// exports.
//
// Pairs with the S3 prefix scanner (pkg/source/snapshot/parquet#S3Source)
// — that's the multi-file partition-aware Hive-style discoverer that
// most production callers want. This file is the single-file reader
// that decodes one Parquet object.
//
// # Compression
//
// Snappy / gzip / zstd / lz4 are handled transparently by the Arrow
// Parquet library based on the per-column-chunk metadata. No
// caller-side compression handling is required.
//
// # Decoding
//
// Parquet is columnar, but bootstrap.Run consumes one Record[T] at a
// time. This source materializes one row group of arrow.RecordBatchs at a
// time and walks each row into T via a user-supplied Decoder[T]. The
// Decoder receives an arrow.RecordBatch + row index so it can pull only the
// columns it needs by name.
package parquet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot"
)

// Decoder converts a single Parquet row (column-array + row index) into
// T. Implementations should pull only the columns they need from the
// arrow.RecordBatch by name; cross-record allocations are the caller's
// responsibility (the Record is reused after the call returns when
// streaming via record batches).
type Decoder[T any] func(rec arrow.RecordBatch, row int) (T, error)

// EventIDFn extracts a stable per-record dedup key from the decoded
// value. When unset, the source emits a synthetic
// "<reader-name>:<row-ordinal>" pair, unique within a single run but
// not stable across re-runs. For idempotent re-runs, return the
// natural primary key (entity_id + bucket).
type EventIDFn[T any] func(decoded T, rowOrdinal int) string

// EventTimeFn extracts the upstream event time. When unset, the source
// uses processing time at decode (time.Now). For the canonical
// CountEvent schema, a typical impl returns
// time.UnixMilli(occurredAtUnixMs).
type EventTimeFn[T any] func(decoded T) time.Time

// Config configures a single-Parquet-file snapshot Source.
type Config[T any] struct {
	// Reader is the Parquet object. parquet.ReaderAtSeeker is
	// satisfied by *os.File and by aws.ReadSeekCloser bodies.
	// The Source does not own its lifecycle; close it externally
	// (or use NewSourceClosing to delegate).
	Reader parquet.ReaderAtSeeker

	// Name is a stable identifier for logging / metrics. Typically the
	// S3 key or the file path.
	Name string

	// Decode converts an Arrow row into T. Required — no
	// default schema-introspection exists; callers know their schema.
	Decode Decoder[T]

	// EventID derives the dedup key per record. Optional; see EventIDFn.
	EventID EventIDFn[T]

	// EventTime extracts the upstream event time. Optional; default
	// is time.Now at decode.
	EventTime EventTimeFn[T]

	// OnDecodeError, when non-nil, is invoked for rows that fail to
	// decode. Default behavior: drop silently and continue. Wire to a
	// DLQ producer or logger to surface poison rows.
	OnDecodeError func(rowOrdinal int, err error)

	// BatchSize is the number of rows materialized per arrow.RecordBatch
	// pulled out of Parquet. Defaults to 4096. Lower for large rows
	// (less peak memory), higher for tiny rows (less per-batch
	// overhead).
	BatchSize int64

	// Allocator is the Arrow memory allocator. Defaults to
	// memory.DefaultAllocator. Override for tracked allocators in
	// tests or for arena-style reuse.
	Allocator memory.Allocator

	// HandoffToken, when non-nil, is what CaptureHandoff returns.
	// Parquet snapshots have no inherent live-source resume position;
	// the caller's deployment system is responsible for capturing one
	// before the Parquet file was generated (typically a Kinesis shard
	// timestamp or Kafka offset).
	HandoffToken snapshot.HandoffToken
}

// Source implements snapshot.Source[T] over a single Parquet file.
type Source[T any] struct {
	cfg     Config[T]
	rows    atomic.Int64
	decoded atomic.Int64
	closed  atomic.Bool
	closeFn func() error
	closeMu sync.Mutex
}

// NewSource constructs the Source. Validates required fields and
// applies defaults.
func NewSource[T any](cfg Config[T]) (*Source[T], error) {
	if cfg.Reader == nil {
		return nil, errors.New("parquet snapshot: Reader is required")
	}
	if cfg.Name == "" {
		return nil, errors.New("parquet snapshot: Name is required")
	}
	if cfg.Decode == nil {
		return nil, errors.New("parquet snapshot: Decode is required")
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 4096
	}
	if cfg.Allocator == nil {
		cfg.Allocator = memory.DefaultAllocator
	}
	return &Source[T]{cfg: cfg}, nil
}

// NewSourceClosing is the convenience variant for callers that want
// snapshot.Source.Close to also close the underlying Reader. The
// Reader must implement io.Closer; otherwise NewSource is the right
// choice.
func NewSourceClosing[T any](cfg Config[T]) (*Source[T], error) {
	src, err := NewSource(cfg)
	if err != nil {
		return nil, err
	}
	if closer, ok := cfg.Reader.(io.Closer); ok {
		src.closeFn = closer.Close
	}
	return src, nil
}

// Name returns "parquet:<configured-name>".
func (s *Source[T]) Name() string { return "parquet:" + s.cfg.Name }

// Close releases resources. If NewSourceClosing was used, also closes
// the underlying Reader. Idempotent.
func (s *Source[T]) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed.Load() {
		return nil
	}
	s.closed.Store(true)
	if s.closeFn != nil {
		return s.closeFn()
	}
	return nil
}

// CaptureHandoff returns the configured handoff token, or nil if none.
// See Config.HandoffToken.
func (s *Source[T]) CaptureHandoff(_ context.Context) (snapshot.HandoffToken, error) {
	return s.cfg.HandoffToken, nil
}

// Scan reads the Parquet file row-group-by-row-group, decoding each
// row into T and emitting a source.Record. Returns nil on EOF; non-nil
// on a fatal reader error.
//
// Per-row decode errors fire OnDecodeError but do NOT abort.
func (s *Source[T]) Scan(ctx context.Context, out chan<- source.Record[T]) error {
	pr, err := file.NewParquetReader(s.cfg.Reader)
	if err != nil {
		return fmt.Errorf("parquet open %s: %w", s.cfg.Name, err)
	}
	defer func() { _ = pr.Close() }()

	arr, err := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{
		BatchSize: s.cfg.BatchSize,
	}, s.cfg.Allocator)
	if err != nil {
		return fmt.Errorf("parquet arrow-reader %s: %w", s.cfg.Name, err)
	}

	rr, err := arr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return fmt.Errorf("parquet record-reader %s: %w", s.cfg.Name, err)
	}
	defer rr.Release()

	var rowOrdinal int
	for rr.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rec := rr.RecordBatch()
		if rec == nil {
			continue
		}
		nrows := int(rec.NumRows())
		for r := 0; r < nrows; r++ {
			rowOrdinal++
			s.rows.Add(1)
			value, derr := s.cfg.Decode(rec, r)
			if derr != nil {
				if s.cfg.OnDecodeError != nil {
					s.cfg.OnDecodeError(rowOrdinal, derr)
				}
				continue
			}
			s.decoded.Add(1)
			outRec := source.Record[T]{
				EventID:   s.eventID(value, rowOrdinal),
				EventTime: s.eventTime(value),
				Value:     value,
				Ack:       noopAck,
			}
			select {
			case out <- outRec:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	if err := rr.Err(); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("parquet read %s: %w", s.cfg.Name, err)
	}
	return nil
}

// noopAck is the shared no-op acknowledger used by every emitted
// Record. Snapshot sources have no per-record ack semantics — the
// commit point is the bootstrap's progress checkpoint, not the
// individual record.
func noopAck() error { return nil }

// Resume — Parquet sources don't inherently support mid-stream
// resumption; the file is read top-to-bottom. Re-Scan from the start;
// at-least-once dedup absorbs the duplicate emissions. Mid-stream
// resumption would require persisting (row-group-index, row-offset)
// and is left as future work.
func (s *Source[T]) Resume(ctx context.Context, _ []byte, out chan<- source.Record[T]) error {
	return s.Scan(ctx, out)
}

// Stats reports rows scanned vs successfully decoded.
type Stats struct {
	RowsScanned int64
	RowsDecoded int64
}

// Stats returns the live counters. Safe for concurrent reads.
func (s *Source[T]) Stats() Stats {
	return Stats{
		RowsScanned: s.rows.Load(),
		RowsDecoded: s.decoded.Load(),
	}
}

func (s *Source[T]) eventID(value T, rowOrdinal int) string {
	if s.cfg.EventID != nil {
		if id := s.cfg.EventID(value, rowOrdinal); id != "" {
			return id
		}
	}
	return s.cfg.Name + ":" + strconv.Itoa(rowOrdinal)
}

func (s *Source[T]) eventTime(value T) time.Time {
	if s.cfg.EventTime != nil {
		return s.cfg.EventTime(value)
	}
	return time.Now()
}

// Compile-time check.
var _ snapshot.Source[any] = (*Source[any])(nil)
