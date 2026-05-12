// Package jsonl provides a snapshot.Source that reads JSON Lines from
// any io.Reader — local files, S3 objects, gzip streams, etc. The
// canonical use case: bootstrap a Murmur pipeline from an S3-archived
// DDB export, a daily database dump, a Firehose archive, or any other
// "one JSON object per line" data set.
//
// Pairs cleanly with pkg/replay/s3 — that's the LIVE-mode replay
// counterpart that drives the same JSON Lines through replay.Run. This
// package is the BOOTSTRAP-mode counterpart that drives them through
// bootstrap.Run with handoff-token capture for the bootstrap → live
// transition.
//
// Common shapes the caller wires the Reader for:
//
//   - Local file (development):
//     f, _ := os.Open("orders.jsonl")
//     defer f.Close()
//     reader := f
//
//   - S3 object:
//     resp, _ := s3client.GetObject(ctx, &s3.GetObjectInput{...})
//     defer resp.Body.Close()
//     reader := resp.Body
//
//   - Gzipped S3 object:
//     resp, _ := s3client.GetObject(...)
//     gz, _ := gzip.NewReader(resp.Body)
//     defer gz.Close()
//     reader := gz
//
// For multi-object S3 prefixes (a partitioned export — many .jsonl
// files in one prefix), wrap multiple readers with io.MultiReader, or
// run bootstrap.Run multiple times with different sources and a shared
// Deduper.
package jsonl

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot"
)

// Decoder converts a single JSON-Lines line to T. The default is
// json.Unmarshal into a typed struct via DefaultDecoder; override when
// the source format isn't standard JSON (e.g., DDB export's wrapped-AV
// shape needs custom decoding to flatten attribute values).
type Decoder[T any] func(line []byte) (T, error)

// DefaultDecoder returns a Decoder that does json.Unmarshal into T.
func DefaultDecoder[T any]() Decoder[T] {
	return func(line []byte) (T, error) {
		var v T
		if err := json.Unmarshal(line, &v); err != nil {
			return v, fmt.Errorf("jsonl decode: %w", err)
		}
		return v, nil
	}
}

// EventIDFn extracts a stable per-record dedup key from the decoded
// value. Default — when unset, the source emits a synthetic
// "<reader-name>:<line-number>" pair, which is unique within a single
// run but NOT across re-runs (line numbers can shift if upstream
// re-exports). For idempotent re-runs, return the natural primary key
// (a Mongo `_id`, a DDB partition key).
type EventIDFn[T any] func(decoded T, lineNum int) string

// EventTimeFn extracts the upstream EventTime from a decoded record.
// When set, the returned time is used as Record.EventTime; the
// bootstrap runtime uses it for window bucket assignment. Default —
// when unset — the source emits time.Now(), which is correct for
// non-windowed counters but wrong for backfill of windowed counters
// (every row would land in the same bucket). Pre-aggregated Spark
// rows must wire this so each row lands in its source-of-truth hour.
type EventTimeFn[T any] func(decoded T) time.Time

// Config configures a JSON-Lines snapshot Source.
type Config[T any] struct {
	// Reader is the line source. The Source consumes it sequentially;
	// the caller manages its lifecycle (typically defer Close after
	// bootstrap.Run returns).
	Reader io.Reader

	// Name is a stable identifier for logging / metrics. Typically the
	// S3 key, the file path, or "stdin".
	Name string

	// Decode converts a JSON line to T. Defaults to DefaultDecoder[T].
	Decode Decoder[T]

	// EventID derives the dedup key per record. Optional; see EventIDFn.
	EventID EventIDFn[T]

	// EventTime, when non-nil, derives the per-record EventTime used
	// for window bucket assignment. Defaults to time.Now() when unset;
	// backfill of windowed counters must wire this to the record's
	// source-of-truth timestamp (e.g., the bucket-mid `occurred_at`
	// field) or every row will land in the same bucket.
	EventTime EventTimeFn[T]

	// OnDecodeError, when non-nil, is invoked for lines that fail to
	// decode. Default behavior: drop silently and continue. Wire to a
	// DLQ producer or logger to surface poison lines.
	OnDecodeError func(line []byte, lineNum int, err error)

	// MaxLineSize caps the bufio.Scanner buffer per line. Defaults to
	// 1 MB. Lines longer than this are reported via OnDecodeError and
	// skipped.
	MaxLineSize int

	// HandoffToken, when non-nil, is what CaptureHandoff returns. The
	// JSON-Lines source has no inherent live-source resume position —
	// the caller is responsible for capturing one externally
	// (typically a Kinesis shard timestamp or Kafka offset taken
	// before the snapshot was generated) and passing it through here.
	HandoffToken snapshot.HandoffToken
}

// Source implements snapshot.Source[T] over a single io.Reader of
// JSON Lines.
type Source[T any] struct {
	cfg     Config[T]
	scanned atomic.Int64
	decoded atomic.Int64
	closed  atomic.Bool
	closeFn func() error // optional, for sources that want to be Closeable via this Source
	closeMu sync.Mutex
}

// NewSource constructs the Source. Validates required fields and
// applies defaults.
func NewSource[T any](cfg Config[T]) (*Source[T], error) {
	if cfg.Reader == nil {
		return nil, errors.New("jsonl snapshot: Reader is required")
	}
	if cfg.Name == "" {
		return nil, errors.New("jsonl snapshot: Name is required")
	}
	if cfg.Decode == nil {
		cfg.Decode = DefaultDecoder[T]()
	}
	if cfg.MaxLineSize <= 0 {
		cfg.MaxLineSize = 1 << 20 // 1 MB
	}
	return &Source[T]{cfg: cfg}, nil
}

// NewSourceClosing is the convenience variant for sources that want the
// snapshot.Source.Close to also close the underlying reader. The Reader
// must implement io.Closer; otherwise NewSource is the right choice.
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

// Name returns "jsonl:<configured-name>".
func (s *Source[T]) Name() string { return "jsonl:" + s.cfg.Name }

// Close releases resources. If NewSourceClosing was used, also closes
// the underlying reader. Idempotent.
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
// The JSON-Lines source has no inherent live-source resume position —
// the caller's deployment system is responsible for capturing one
// before the snapshot was generated and passing it through Config.
func (s *Source[T]) CaptureHandoff(_ context.Context) (snapshot.HandoffToken, error) {
	return s.cfg.HandoffToken, nil
}

// Scan consumes the configured Reader line-by-line, decoding each into
// T and emitting a source.Record. Returns nil when the reader returns
// io.EOF; non-nil only on a fatal scanner error (e.g., line larger
// than MaxLineSize that cannot be skipped).
func (s *Source[T]) Scan(ctx context.Context, out chan<- source.Record[T]) error {
	scanner := bufio.NewScanner(s.cfg.Reader)
	scanner.Buffer(make([]byte, 0, 64*1024), s.cfg.MaxLineSize)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		s.scanned.Add(1)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		raw := scanner.Bytes()
		if len(raw) == 0 {
			continue // skip blank lines silently
		}
		// scanner.Bytes() points at an internal buffer that mutates on
		// the next Scan; copy once so the decoded T can outlive this
		// iteration.
		lineCopy := make([]byte, len(raw))
		copy(lineCopy, raw)

		value, err := s.cfg.Decode(lineCopy)
		if err != nil {
			if s.cfg.OnDecodeError != nil {
				s.cfg.OnDecodeError(lineCopy, lineNum, err)
			}
			continue
		}
		s.decoded.Add(1)
		eventID := s.eventID(value, lineNum)
		eventTime := time.Now()
		if s.cfg.EventTime != nil {
			if t := s.cfg.EventTime(value); !t.IsZero() {
				eventTime = t
			}
		}
		rec := source.Record[T]{
			EventID:   eventID,
			EventTime: eventTime,
			Value:     value,
			Ack:       func() error { return nil },
		}
		select {
		case out <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("jsonl scanner %s: %w", s.cfg.Name, err)
	}
	return nil
}

// Resume — the JSON-Lines source doesn't inherently support
// mid-stream resumption (Reader is one-shot). Currently a no-op that
// re-Scans from the start; at-least-once dedup handles the duplicate
// emissions. Production callers wanting mid-stream resume should
// either: persist a per-line offset and seek the underlying io.Reader
// (file source), or restart from the next archive object (S3 source).
func (s *Source[T]) Resume(ctx context.Context, _ []byte, out chan<- source.Record[T]) error {
	return s.Scan(ctx, out)
}

// Stats reports the lines scanned vs successfully decoded — useful for
// surfacing "what fraction of the input was actually usable" after a
// large bootstrap.
type Stats struct {
	LinesScanned int64
	LinesDecoded int64
}

// Stats returns the live counters. Safe for concurrent reads.
func (s *Source[T]) Stats() Stats {
	return Stats{
		LinesScanned: s.scanned.Load(),
		LinesDecoded: s.decoded.Load(),
	}
}

func (s *Source[T]) eventID(value T, lineNum int) string {
	if s.cfg.EventID != nil {
		if id := s.cfg.EventID(value, lineNum); id != "" {
			return id
		}
	}
	return s.cfg.Name + ":" + strconv.Itoa(lineNum)
}

// Compile-time check.
var _ snapshot.Source[any] = (*Source[any])(nil)
