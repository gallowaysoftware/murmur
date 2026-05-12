package parquet_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	pqlib "github.com/apache/arrow-go/v18/parquet"
	pqcompress "github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot/parquet"
)

// countEvent mirrors the canonical Spark→murmur schema (see
// examples/backfill-from-spark/).
type countEvent struct {
	EntityID         string
	Count            int64
	OccurredAtUnixMs int64
	Year, Month      int32
	Day, Hour        int32
}

func decodeCountEvent(rec arrow.Record, row int) (countEvent, error) {
	sc := rec.Schema()
	var e countEvent

	idx := sc.FieldIndices("entity_id")
	if len(idx) == 0 {
		return e, errors.New("missing entity_id")
	}
	entityArr, ok := rec.Column(idx[0]).(*array.String)
	if !ok {
		return e, errors.New("entity_id is not String")
	}
	e.EntityID = entityArr.Value(row)

	idx = sc.FieldIndices("count")
	if len(idx) == 0 {
		return e, errors.New("missing count")
	}
	cntArr, ok := rec.Column(idx[0]).(*array.Int64)
	if !ok {
		return e, errors.New("count is not Int64")
	}
	e.Count = cntArr.Value(row)

	idx = sc.FieldIndices("occurred_at_unix_ms")
	if len(idx) > 0 {
		if arr, ok := rec.Column(idx[0]).(*array.Int64); ok {
			e.OccurredAtUnixMs = arr.Value(row)
		}
	}
	if idx = sc.FieldIndices("year"); len(idx) > 0 {
		if arr, ok := rec.Column(idx[0]).(*array.Int32); ok {
			e.Year = arr.Value(row)
		}
	}
	if idx = sc.FieldIndices("month"); len(idx) > 0 {
		if arr, ok := rec.Column(idx[0]).(*array.Int32); ok {
			e.Month = arr.Value(row)
		}
	}
	if idx = sc.FieldIndices("day"); len(idx) > 0 {
		if arr, ok := rec.Column(idx[0]).(*array.Int32); ok {
			e.Day = arr.Value(row)
		}
	}
	if idx = sc.FieldIndices("hour"); len(idx) > 0 {
		if arr, ok := rec.Column(idx[0]).(*array.Int32); ok {
			e.Hour = arr.Value(row)
		}
	}
	return e, nil
}

// countEventSchema returns the canonical CountEvent schema for tests.
func countEventSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "entity_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "occurred_at_unix_ms", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "year", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "month", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "day", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "hour", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
}

// buildCountEventParquet writes the supplied countEvents into a Parquet
// byte buffer using the canonical CountEvent schema. compression is
// applied to all columns.
func buildCountEventParquet(t *testing.T, events []countEvent, compression pqcompress.Compression) []byte {
	t.Helper()
	mem := memory.DefaultAllocator
	sc := countEventSchema()

	entityB := array.NewStringBuilder(mem)
	defer entityB.Release()
	countB := array.NewInt64Builder(mem)
	defer countB.Release()
	tsB := array.NewInt64Builder(mem)
	defer tsB.Release()
	yearB := array.NewInt32Builder(mem)
	defer yearB.Release()
	monthB := array.NewInt32Builder(mem)
	defer monthB.Release()
	dayB := array.NewInt32Builder(mem)
	defer dayB.Release()
	hourB := array.NewInt32Builder(mem)
	defer hourB.Release()

	for _, e := range events {
		entityB.Append(e.EntityID)
		countB.Append(e.Count)
		tsB.Append(e.OccurredAtUnixMs)
		yearB.Append(e.Year)
		monthB.Append(e.Month)
		dayB.Append(e.Day)
		hourB.Append(e.Hour)
	}

	entityArr := entityB.NewArray()
	defer entityArr.Release()
	countArr := countB.NewArray()
	defer countArr.Release()
	tsArr := tsB.NewArray()
	defer tsArr.Release()
	yearArr := yearB.NewArray()
	defer yearArr.Release()
	monthArr := monthB.NewArray()
	defer monthArr.Release()
	dayArr := dayB.NewArray()
	defer dayArr.Release()
	hourArr := hourB.NewArray()
	defer hourArr.Release()

	rec := array.NewRecord(sc, []arrow.Array{entityArr, countArr, tsArr, yearArr, monthArr, dayArr, hourArr}, int64(len(events)))
	defer rec.Release()

	props := pqlib.NewWriterProperties(pqlib.WithCompression(compression))
	var buf bytes.Buffer
	if err := pqarrow.WriteTable(arrowTableFromRecord(rec), &buf, int64(len(events)), props, pqarrow.DefaultWriterProps()); err != nil {
		t.Fatalf("WriteTable: %v", err)
	}
	return buf.Bytes()
}

func arrowTableFromRecord(rec arrow.Record) arrow.Table {
	return array.NewTableFromRecords(rec.Schema(), []arrow.Record{rec})
}

func collect[T any](t *testing.T, ctx context.Context, src *parquet.Source[T]) []source.Record[T] {
	t.Helper()
	ch := make(chan source.Record[T], 100)
	done := make(chan error, 1)
	go func() {
		done <- src.Scan(ctx, ch)
		close(ch)
	}()
	var got []source.Record[T]
	for r := range ch {
		got = append(got, r)
	}
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Scan: %v", err)
	}
	return got
}

func TestScan_DecodesEachRow(t *testing.T) {
	events := []countEvent{
		{EntityID: "user-1", Count: 5, OccurredAtUnixMs: 1_700_000_000_000, Year: 2026, Month: 5, Day: 1, Hour: 0},
		{EntityID: "user-2", Count: 7, OccurredAtUnixMs: 1_700_000_010_000, Year: 2026, Month: 5, Day: 1, Hour: 0},
		{EntityID: "user-3", Count: 3, OccurredAtUnixMs: 1_700_000_020_000, Year: 2026, Month: 5, Day: 1, Hour: 1},
	}
	body := buildCountEventParquet(t, events, pqcompress.Codecs.Snappy)

	src, err := parquet.NewSource(parquet.Config[countEvent]{
		Reader: bytes.NewReader(body),
		Name:   "test.parquet",
		Decode: decodeCountEvent,
	})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	got := collect(t, t.Context(), src)
	if len(got) != 3 {
		t.Fatalf("len: got %d, want 3", len(got))
	}
	if got[0].Value.EntityID != "user-1" || got[2].Value.Count != 3 {
		t.Errorf("decode order: got %+v", got)
	}
}

func TestScan_SnappyCompression(t *testing.T) {
	events := []countEvent{
		{EntityID: "a", Count: 1, Year: 2026, Month: 1, Day: 1, Hour: 0},
		{EntityID: "b", Count: 2, Year: 2026, Month: 1, Day: 1, Hour: 0},
	}
	body := buildCountEventParquet(t, events, pqcompress.Codecs.Snappy)

	src, err := parquet.NewSource(parquet.Config[countEvent]{
		Reader: bytes.NewReader(body),
		Name:   "snappy.parquet",
		Decode: decodeCountEvent,
	})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	got := collect(t, t.Context(), src)
	if len(got) != 2 || got[1].Value.EntityID != "b" {
		t.Errorf("snappy decode wrong: %+v", got)
	}
}

func TestScan_GzipCompression(t *testing.T) {
	events := []countEvent{
		{EntityID: "x", Count: 9, Year: 2026, Month: 1, Day: 1, Hour: 0},
	}
	body := buildCountEventParquet(t, events, pqcompress.Codecs.Gzip)

	src, _ := parquet.NewSource(parquet.Config[countEvent]{
		Reader: bytes.NewReader(body),
		Name:   "gz.parquet",
		Decode: decodeCountEvent,
	})
	got := collect(t, t.Context(), src)
	if len(got) != 1 || got[0].Value.EntityID != "x" || got[0].Value.Count != 9 {
		t.Errorf("gzip decode wrong: %+v", got)
	}
}

func TestScan_ZstdCompression(t *testing.T) {
	events := []countEvent{
		{EntityID: "z", Count: 42, Year: 2026, Month: 1, Day: 1, Hour: 0},
	}
	body := buildCountEventParquet(t, events, pqcompress.Codecs.Zstd)

	src, _ := parquet.NewSource(parquet.Config[countEvent]{
		Reader: bytes.NewReader(body),
		Name:   "zstd.parquet",
		Decode: decodeCountEvent,
	})
	got := collect(t, t.Context(), src)
	if len(got) != 1 || got[0].Value.Count != 42 {
		t.Errorf("zstd decode wrong: %+v", got)
	}
}

func TestScan_EventIDDefault(t *testing.T) {
	events := []countEvent{
		{EntityID: "a", Count: 1},
		{EntityID: "b", Count: 2},
	}
	body := buildCountEventParquet(t, events, pqcompress.Codecs.Snappy)

	src, _ := parquet.NewSource(parquet.Config[countEvent]{
		Reader: bytes.NewReader(body),
		Name:   "f",
		Decode: decodeCountEvent,
	})
	got := collect(t, t.Context(), src)
	if got[0].EventID != "f:1" || got[1].EventID != "f:2" {
		t.Errorf("default EventID: got %q, %q", got[0].EventID, got[1].EventID)
	}
}

func TestScan_EventIDFromExtractor(t *testing.T) {
	events := []countEvent{
		{EntityID: "a", Count: 1, Hour: 7},
		{EntityID: "b", Count: 2, Hour: 8},
	}
	body := buildCountEventParquet(t, events, pqcompress.Codecs.Snappy)

	src, _ := parquet.NewSource(parquet.Config[countEvent]{
		Reader: bytes.NewReader(body),
		Name:   "f",
		Decode: decodeCountEvent,
		EventID: func(e countEvent, _ int) string {
			return "natural:" + e.EntityID
		},
	})
	got := collect(t, t.Context(), src)
	if got[0].EventID != "natural:a" || got[1].EventID != "natural:b" {
		t.Errorf("custom EventID: %+v", got)
	}
}

func TestScan_EventTimeFromOccurredAt(t *testing.T) {
	ms := int64(1_750_000_000_000)
	events := []countEvent{{EntityID: "a", Count: 1, OccurredAtUnixMs: ms}}
	body := buildCountEventParquet(t, events, pqcompress.Codecs.Snappy)

	src, _ := parquet.NewSource(parquet.Config[countEvent]{
		Reader: bytes.NewReader(body),
		Name:   "f",
		Decode: decodeCountEvent,
		EventTime: func(e countEvent) time.Time {
			return time.UnixMilli(e.OccurredAtUnixMs)
		},
	})
	got := collect(t, t.Context(), src)
	want := time.UnixMilli(ms)
	if !got[0].EventTime.Equal(want) {
		t.Errorf("EventTime: got %s, want %s", got[0].EventTime, want)
	}
}

func TestScan_DecodeErrorCallback(t *testing.T) {
	events := []countEvent{
		{EntityID: "a", Count: 1},
		{EntityID: "b", Count: 2},
	}
	body := buildCountEventParquet(t, events, pqcompress.Codecs.Snappy)

	var seenRows []int
	src, _ := parquet.NewSource(parquet.Config[countEvent]{
		Reader: bytes.NewReader(body),
		Name:   "f",
		Decode: func(rec arrow.Record, row int) (countEvent, error) {
			// Fail on the second row.
			if row == 1 {
				return countEvent{}, errors.New("synthetic poison row")
			}
			return decodeCountEvent(rec, row)
		},
		OnDecodeError: func(rowOrdinal int, _ error) {
			seenRows = append(seenRows, rowOrdinal)
		},
	})
	got := collect(t, t.Context(), src)
	if len(got) != 1 {
		t.Errorf("len: got %d, want 1 (poison row skipped)", len(got))
	}
	if len(seenRows) != 1 || seenRows[0] != 2 {
		t.Errorf("decode-error callback: got %v, want [2]", seenRows)
	}
}

func TestScan_MissingRequiredColumnError(t *testing.T) {
	// Build a Parquet file whose schema lacks `count` — the decoder
	// should report a clear error per row via OnDecodeError.
	mem := memory.DefaultAllocator
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "entity_id", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	b := array.NewStringBuilder(mem)
	defer b.Release()
	b.Append("a")
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecord(sc, []arrow.Array{arr}, 1)
	defer rec.Release()

	var buf bytes.Buffer
	if err := pqarrow.WriteTable(array.NewTableFromRecords(sc, []arrow.Record{rec}), &buf, 1, nil, pqarrow.DefaultWriterProps()); err != nil {
		t.Fatalf("WriteTable: %v", err)
	}

	var seen int
	var lastErr error
	src, _ := parquet.NewSource(parquet.Config[countEvent]{
		Reader: bytes.NewReader(buf.Bytes()),
		Name:   "missing-cols.parquet",
		Decode: decodeCountEvent,
		OnDecodeError: func(_ int, err error) {
			seen++
			lastErr = err
		},
	})
	got := collect(t, t.Context(), src)
	if len(got) != 0 {
		t.Errorf("expected 0 decoded rows, got %d", len(got))
	}
	if seen != 1 {
		t.Errorf("expected 1 decode-error callback, got %d", seen)
	}
	if lastErr == nil {
		t.Error("expected non-nil err in callback")
	}
}

func TestScan_StatsReportRows(t *testing.T) {
	events := []countEvent{
		{EntityID: "a"},
		{EntityID: "b"},
		{EntityID: "c"},
	}
	body := buildCountEventParquet(t, events, pqcompress.Codecs.Snappy)
	src, _ := parquet.NewSource(parquet.Config[countEvent]{
		Reader: bytes.NewReader(body),
		Name:   "f",
		Decode: func(rec arrow.Record, row int) (countEvent, error) {
			if row == 1 {
				return countEvent{}, errors.New("poison")
			}
			return decodeCountEvent(rec, row)
		},
	})
	_ = collect(t, t.Context(), src)
	stats := src.Stats()
	if stats.RowsScanned != 3 {
		t.Errorf("RowsScanned: got %d, want 3", stats.RowsScanned)
	}
	if stats.RowsDecoded != 2 {
		t.Errorf("RowsDecoded: got %d, want 2", stats.RowsDecoded)
	}
}

func TestCaptureHandoff_PassThrough(t *testing.T) {
	src, _ := parquet.NewSource(parquet.Config[countEvent]{
		Reader:       bytes.NewReader(buildCountEventParquet(t, []countEvent{{EntityID: "a"}}, pqcompress.Codecs.Snappy)),
		Name:         "f",
		Decode:       decodeCountEvent,
		HandoffToken: []byte("opaque-token"),
	})
	tok, err := src.CaptureHandoff(t.Context())
	if err != nil {
		t.Fatalf("CaptureHandoff: %v", err)
	}
	if string(tok) != "opaque-token" {
		t.Errorf("token: got %q", tok)
	}
}

type fakeCloser struct {
	*bytes.Reader
	closed bool
}

func (c *fakeCloser) Close() error { c.closed = true; return nil }

func TestNewSourceClosing_ClosesUnderlying(t *testing.T) {
	body := buildCountEventParquet(t, []countEvent{{EntityID: "a"}}, pqcompress.Codecs.Snappy)
	fc := &fakeCloser{Reader: bytes.NewReader(body)}
	src, err := parquet.NewSourceClosing(parquet.Config[countEvent]{
		Reader: fc,
		Name:   "f",
		Decode: decodeCountEvent,
	})
	if err != nil {
		t.Fatalf("NewSourceClosing: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if !fc.closed {
		t.Error("underlying reader was not closed")
	}
	// Idempotent.
	if err := src.Close(); err != nil {
		t.Errorf("Close idempotency: %v", err)
	}
}

func TestNewSource_RequiresReader(t *testing.T) {
	_, err := parquet.NewSource(parquet.Config[countEvent]{
		Name:   "f",
		Decode: decodeCountEvent,
	})
	if err == nil {
		t.Error("expected error when Reader is nil")
	}
}

func TestNewSource_RequiresName(t *testing.T) {
	_, err := parquet.NewSource(parquet.Config[countEvent]{
		Reader: bytes.NewReader(nil),
		Decode: decodeCountEvent,
	})
	if err == nil {
		t.Error("expected error when Name is empty")
	}
}

func TestNewSource_RequiresDecode(t *testing.T) {
	_, err := parquet.NewSource[countEvent](parquet.Config[countEvent]{
		Reader: bytes.NewReader(nil),
		Name:   "f",
	})
	if err == nil {
		t.Error("expected error when Decode is nil")
	}
}

func TestScan_CtxCancelStops(t *testing.T) {
	events := make([]countEvent, 50)
	for i := range events {
		events[i] = countEvent{EntityID: "a", Count: int64(i)}
	}
	body := buildCountEventParquet(t, events, pqcompress.Codecs.Snappy)
	src, _ := parquet.NewSource(parquet.Config[countEvent]{
		Reader: bytes.NewReader(body),
		Name:   "f",
		Decode: decodeCountEvent,
	})
	ctx, cancel := context.WithCancel(t.Context())
	out := make(chan source.Record[countEvent])
	go func() {
		<-out
		cancel()
		for range out {
		}
	}()
	err := src.Scan(ctx, out)
	close(out)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Scan: got %v, want nil or context.Canceled", err)
	}
}

func TestName(t *testing.T) {
	src, _ := parquet.NewSource(parquet.Config[countEvent]{
		Reader: bytes.NewReader(buildCountEventParquet(t, []countEvent{{EntityID: "a"}}, pqcompress.Codecs.Snappy)),
		Name:   "events/2026/05/01.parquet",
		Decode: decodeCountEvent,
	})
	if got, want := src.Name(), "parquet:events/2026/05/01.parquet"; got != want {
		t.Errorf("Name: got %q, want %q", got, want)
	}
}

// Sanity-check that we read the entire file (multiple row groups) when
// the writer emits >1 row group.
func TestScan_MultipleRowGroups(t *testing.T) {
	events := make([]countEvent, 20)
	for i := range events {
		events[i] = countEvent{EntityID: "u", Count: int64(i + 1)}
	}
	// Write with chunk size 5 → 4 row groups.
	mem := memory.DefaultAllocator
	sc := countEventSchema()
	entityB := array.NewStringBuilder(mem)
	defer entityB.Release()
	countB := array.NewInt64Builder(mem)
	defer countB.Release()
	tsB := array.NewInt64Builder(mem)
	defer tsB.Release()
	yearB := array.NewInt32Builder(mem)
	defer yearB.Release()
	monthB := array.NewInt32Builder(mem)
	defer monthB.Release()
	dayB := array.NewInt32Builder(mem)
	defer dayB.Release()
	hourB := array.NewInt32Builder(mem)
	defer hourB.Release()
	for _, e := range events {
		entityB.Append(e.EntityID)
		countB.Append(e.Count)
		tsB.Append(e.OccurredAtUnixMs)
		yearB.Append(e.Year)
		monthB.Append(e.Month)
		dayB.Append(e.Day)
		hourB.Append(e.Hour)
	}
	entityArr := entityB.NewArray()
	defer entityArr.Release()
	countArr := countB.NewArray()
	defer countArr.Release()
	tsArr := tsB.NewArray()
	defer tsArr.Release()
	yearArr := yearB.NewArray()
	defer yearArr.Release()
	monthArr := monthB.NewArray()
	defer monthArr.Release()
	dayArr := dayB.NewArray()
	defer dayArr.Release()
	hourArr := hourB.NewArray()
	defer hourArr.Release()
	rec := array.NewRecord(sc, []arrow.Array{entityArr, countArr, tsArr, yearArr, monthArr, dayArr, hourArr}, int64(len(events)))
	defer rec.Release()

	var buf bytes.Buffer
	if err := pqarrow.WriteTable(array.NewTableFromRecords(sc, []arrow.Record{rec}), &buf, 5, nil, pqarrow.DefaultWriterProps()); err != nil {
		t.Fatalf("WriteTable: %v", err)
	}

	src, _ := parquet.NewSource(parquet.Config[countEvent]{
		Reader:    bytes.NewReader(buf.Bytes()),
		Name:      "multi.parquet",
		Decode:    decodeCountEvent,
		BatchSize: 3,
	})
	got := collect(t, t.Context(), src)
	if len(got) != 20 {
		t.Errorf("expected 20 rows across row groups, got %d", len(got))
	}
}
