package jsonl_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot/jsonl"
)

type order struct {
	ID     string `json:"id"`
	Region string `json:"region"`
	Amount int64  `json:"amount"`
}

func collect(t *testing.T, src *jsonl.Source[order]) []source.Record[order] {
	t.Helper()
	ch := make(chan source.Record[order], 100)
	done := make(chan error, 1)
	go func() {
		done <- src.Scan(context.Background(), ch)
		close(ch)
	}()
	var got []source.Record[order]
	for r := range ch {
		got = append(got, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Scan: %v", err)
	}
	return got
}

func TestScan_DecodesEachLine(t *testing.T) {
	body := `{"id":"o-1","region":"us-east","amount":100}
{"id":"o-2","region":"us-west","amount":200}
{"id":"o-3","region":"us-east","amount":50}
`
	src, err := jsonl.NewSource(jsonl.Config[order]{
		Reader: strings.NewReader(body),
		Name:   "test",
	})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	got := collect(t, src)
	if len(got) != 3 {
		t.Fatalf("len: got %d, want 3", len(got))
	}
	if got[0].Value.ID != "o-1" || got[2].Value.Amount != 50 {
		t.Errorf("decode order: got %+v", got)
	}
}

func TestScan_BlankLinesSkipped(t *testing.T) {
	body := "\n" +
		`{"id":"o-1"}` + "\n" +
		"\n" +
		`{"id":"o-2"}` + "\n" +
		"\n"
	src, _ := jsonl.NewSource(jsonl.Config[order]{
		Reader: strings.NewReader(body),
		Name:   "test",
	})
	got := collect(t, src)
	if len(got) != 2 {
		t.Errorf("len: got %d, want 2 (blanks should not produce records)", len(got))
	}
}

func TestScan_EventIDFromExtractor(t *testing.T) {
	body := `{"id":"o-1","region":"us"}
{"id":"o-2","region":"us"}
`
	src, _ := jsonl.NewSource(jsonl.Config[order]{
		Reader:  strings.NewReader(body),
		Name:    "test",
		EventID: func(o order, _ int) string { return "natural:" + o.ID },
	})
	got := collect(t, src)
	if got[0].EventID != "natural:o-1" {
		t.Errorf("EventID: got %q, want natural:o-1", got[0].EventID)
	}
}

func TestScan_EventIDDefaultIsLineBased(t *testing.T) {
	body := `{"id":"o-1"}
{"id":"o-2"}
`
	src, _ := jsonl.NewSource(jsonl.Config[order]{
		Reader: strings.NewReader(body),
		Name:   "src",
	})
	got := collect(t, src)
	if got[0].EventID != "src:1" || got[1].EventID != "src:2" {
		t.Errorf("default EventID: got %q,%q", got[0].EventID, got[1].EventID)
	}
}

func TestScan_EventTimeFromExtractor(t *testing.T) {
	// The decoded record carries an OccurredAt that the extractor
	// returns; the source must propagate it into Record.EventTime.
	type ev struct {
		ID         string    `json:"id"`
		OccurredAt time.Time `json:"occurred_at"`
	}
	body := `{"id":"a","occurred_at":"2026-05-08T14:00:00Z"}
{"id":"b","occurred_at":"2026-05-08T15:00:00Z"}
`
	src, err := jsonl.NewSource(jsonl.Config[ev]{
		Reader:    strings.NewReader(body),
		Name:      "test",
		EventTime: func(e ev) time.Time { return e.OccurredAt },
	})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	ch := make(chan source.Record[ev], 10)
	done := make(chan error, 1)
	go func() {
		done <- src.Scan(context.Background(), ch)
		close(ch)
	}()
	var got []source.Record[ev]
	for r := range ch {
		got = append(got, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2", len(got))
	}
	wantA, _ := time.Parse(time.RFC3339, "2026-05-08T14:00:00Z")
	wantB, _ := time.Parse(time.RFC3339, "2026-05-08T15:00:00Z")
	if !got[0].EventTime.Equal(wantA) {
		t.Errorf("EventTime[0]: got %v, want %v", got[0].EventTime, wantA)
	}
	if !got[1].EventTime.Equal(wantB) {
		t.Errorf("EventTime[1]: got %v, want %v", got[1].EventTime, wantB)
	}
}

func TestScan_EventTimeZeroFallsBackToNow(t *testing.T) {
	// EventTime extractor returning the zero value — the source must
	// fall back to time.Now() rather than emitting a zero EventTime.
	body := `{"id":"a"}` + "\n"
	src, _ := jsonl.NewSource(jsonl.Config[order]{
		Reader:    strings.NewReader(body),
		Name:      "test",
		EventTime: func(_ order) time.Time { return time.Time{} },
	})
	before := time.Now()
	ch := make(chan source.Record[order], 1)
	done := make(chan error, 1)
	go func() { done <- src.Scan(context.Background(), ch); close(ch) }()
	var got []source.Record[order]
	for r := range ch {
		got = append(got, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if got[0].EventTime.Before(before) {
		t.Errorf("EventTime: got %v, want >= %v (fallback to time.Now)", got[0].EventTime, before)
	}
}

func TestScan_DecodeErrorCallback(t *testing.T) {
	body := `{"id":"o-1"}
not-json
{"id":"o-3"}
`
	var seen []int
	src, _ := jsonl.NewSource(jsonl.Config[order]{
		Reader: strings.NewReader(body),
		Name:   "src",
		OnDecodeError: func(_ []byte, lineNum int, _ error) {
			seen = append(seen, lineNum)
		},
	})
	got := collect(t, src)
	if len(got) != 2 {
		t.Errorf("len: got %d, want 2 (poison line should be skipped)", len(got))
	}
	if len(seen) != 1 || seen[0] != 2 {
		t.Errorf("decode-error callback: got %v, want [2]", seen)
	}
}

func TestScan_StatsReportLines(t *testing.T) {
	body := `{"id":"o-1"}
not-json
{"id":"o-3"}
`
	src, _ := jsonl.NewSource(jsonl.Config[order]{
		Reader: strings.NewReader(body),
		Name:   "src",
	})
	_ = collect(t, src)
	stats := src.Stats()
	if stats.LinesScanned != 3 {
		t.Errorf("LinesScanned: got %d, want 3", stats.LinesScanned)
	}
	if stats.LinesDecoded != 2 {
		t.Errorf("LinesDecoded: got %d, want 2", stats.LinesDecoded)
	}
}

func TestCaptureHandoff_PassThrough(t *testing.T) {
	src, _ := jsonl.NewSource(jsonl.Config[order]{
		Reader:       strings.NewReader(""),
		Name:         "src",
		HandoffToken: []byte("opaque-token-from-deployment"),
	})
	tok, err := src.CaptureHandoff(context.Background())
	if err != nil {
		t.Fatalf("CaptureHandoff: %v", err)
	}
	if string(tok) != "opaque-token-from-deployment" {
		t.Errorf("token: got %q", tok)
	}
}

type fakeCloser struct {
	io.Reader
	closed bool
}

func (c *fakeCloser) Close() error { c.closed = true; return nil }

func TestNewSourceClosing_ClosesUnderlyingReader(t *testing.T) {
	fc := &fakeCloser{Reader: strings.NewReader("")}
	src, err := jsonl.NewSourceClosing(jsonl.Config[order]{
		Reader: fc,
		Name:   "src",
	})
	if err != nil {
		t.Fatalf("NewSourceClosing: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if !fc.closed {
		t.Error("Close didn't propagate to the underlying reader")
	}
	// Idempotent — second close is a no-op.
	if err := src.Close(); err != nil {
		t.Errorf("Close idempotency: %v", err)
	}
}

func TestNewSource_RequiresReader(t *testing.T) {
	_, err := jsonl.NewSource[order](jsonl.Config[order]{Name: "src"})
	if err == nil {
		t.Error("expected error when Reader is nil")
	}
}

func TestNewSource_RequiresName(t *testing.T) {
	_, err := jsonl.NewSource[order](jsonl.Config[order]{
		Reader: strings.NewReader(""),
	})
	if err == nil {
		t.Error("expected error when Name is empty")
	}
}

func TestScan_CtxCancelStops(t *testing.T) {
	// Slow consumer; ctx cancel mid-emit must return promptly.
	body := strings.Repeat(`{"id":"o-x"}`+"\n", 100)
	src, _ := jsonl.NewSource(jsonl.Config[order]{
		Reader: strings.NewReader(body),
		Name:   "src",
	})
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan source.Record[order]) // unbuffered → first send blocks
	go func() {
		// Read one record then cancel.
		<-out
		cancel()
		// drain the rest so the goroutine isn't deadlocked
		for range out {
		}
	}()
	err := src.Scan(ctx, out)
	close(out)
	// ctx.Err() is acceptable; nil is acceptable if the scan finished
	// before cancel landed.
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Scan: got %v, want nil or context.Canceled", err)
	}
}
