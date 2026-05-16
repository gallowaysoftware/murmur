package s3_test

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot/s3"
)

type order struct {
	ID     string `json:"id"`
	Region string `json:"region"`
}

// Tests use the OpenObject hook to inject deterministic content, so
// they don't need a live S3 client. Live-S3 path is exercised in the
// integration suite (test/e2e/) and via the search-projector example.

func collectAll[T any](t *testing.T, src *s3.Source[T]) []source.Record[T] {
	t.Helper()
	ch := make(chan source.Record[T], 100)
	done := make(chan error, 1)
	go func() {
		done <- src.Scan(context.Background(), ch)
		close(ch)
	}()
	var got []source.Record[T]
	for r := range ch {
		got = append(got, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Scan: %v", err)
	}
	return got
}

func TestScan_OpenObjectMode_SingleKey(t *testing.T) {
	src, err := s3.NewSource(s3.Config[order]{
		Prefix: "fixed-key.jsonl",
		OpenObject: func(_ context.Context, key string) (io.ReadCloser, error) {
			if key != "fixed-key.jsonl" {
				return nil, errors.New("unexpected key: " + key)
			}
			body := `{"id":"o-1","region":"us"}
{"id":"o-2","region":"eu"}
`
			return io.NopCloser(strings.NewReader(body)), nil
		},
	})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	got := collectAll(t, src)
	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2", len(got))
	}
	if got[0].Value.ID != "o-1" || got[1].Value.Region != "eu" {
		t.Errorf("decoded values wrong: %+v", got)
	}
}

func TestScan_GzipDecompression(t *testing.T) {
	// Pre-compressed payload — emit with .gz suffix → s3 source must
	// auto-decompress.
	body := `{"id":"a"}
{"id":"b"}
`
	var compressed strings.Builder
	gw := gzip.NewWriter(&compressed)
	if _, err := gw.Write([]byte(body)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	src, err := s3.NewSource(s3.Config[order]{
		Prefix: "data.jsonl.gz",
		OpenObject: func(_ context.Context, _ string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(compressed.String())), nil
		},
	})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	got := collectAll(t, src)
	if len(got) != 2 || got[1].Value.ID != "b" {
		t.Errorf("gzip decompression: got %+v", got)
	}
}

func TestScan_EventIDIncludesKey(t *testing.T) {
	src, _ := s3.NewSource(s3.Config[order]{
		Prefix: "shard-1.jsonl",
		OpenObject: func(_ context.Context, _ string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(`{"id":"a"}` + "\n")), nil
		},
	})
	got := collectAll(t, src)
	if len(got) != 1 {
		t.Fatalf("len: got %d", len(got))
	}
	// Default EventID format: "<key>:<line>"
	if !strings.Contains(got[0].EventID, "shard-1.jsonl:") {
		t.Errorf("default EventID should include key: got %q", got[0].EventID)
	}
}

func TestScan_CustomEventID(t *testing.T) {
	src, _ := s3.NewSource(s3.Config[order]{
		Prefix: "k",
		OpenObject: func(_ context.Context, _ string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(`{"id":"o-1"}` + "\n")), nil
		},
		EventID: func(o order, _ string, _ int) string {
			return "natural:" + o.ID
		},
	})
	got := collectAll(t, src)
	if got[0].EventID != "natural:o-1" {
		t.Errorf("custom EventID: got %q", got[0].EventID)
	}
}

func TestScan_DecodeErrorCallback(t *testing.T) {
	var seen int64
	src, _ := s3.NewSource(s3.Config[order]{
		Prefix: "k.jsonl",
		OpenObject: func(_ context.Context, _ string) (io.ReadCloser, error) {
			body := `{"id":"a"}
not-json
{"id":"c"}
`
			return io.NopCloser(strings.NewReader(body)), nil
		},
		OnDecodeError: func(key string, lineNum int, _ []byte, _ error) {
			if key != "k.jsonl" {
				t.Errorf("decode-error callback got unexpected key: %q", key)
			}
			if lineNum != 2 {
				t.Errorf("decode-error line: got %d, want 2", lineNum)
			}
			atomic.AddInt64(&seen, 1)
		},
	})
	got := collectAll(t, src)
	if len(got) != 2 {
		t.Errorf("len: got %d, want 2 (poison line skipped)", len(got))
	}
	if atomic.LoadInt64(&seen) != 1 {
		t.Errorf("decode-error callback fires: got %d, want 1", seen)
	}
}

func TestNewSource_RequiresClientOrOpenObject(t *testing.T) {
	_, err := s3.NewSource[order](s3.Config[order]{Bucket: "b"})
	if err == nil {
		t.Error("expected error when both Client and OpenObject are nil")
	}
}

func TestCaptureHandoff_PassThrough(t *testing.T) {
	src, _ := s3.NewSource(s3.Config[order]{
		Prefix:       "k",
		HandoffToken: []byte("opaque-token"),
		OpenObject: func(_ context.Context, _ string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("")), nil
		},
	})
	tok, err := src.CaptureHandoff(context.Background())
	if err != nil {
		t.Fatalf("CaptureHandoff: %v", err)
	}
	if string(tok) != "opaque-token" {
		t.Errorf("token: got %q", tok)
	}
}

func TestScan_EventTimePropagatesFromExtractor(t *testing.T) {
	// EventTime extractor on s3.Config flows through to Record.EventTime
	// (proves the plumb-through into the underlying jsonl source).
	type ev struct {
		ID         string    `json:"id"`
		OccurredAt time.Time `json:"occurred_at"`
	}
	want, _ := time.Parse(time.RFC3339, "2026-05-08T14:00:00Z")
	src, _ := s3.NewSource(s3.Config[ev]{
		Prefix: "k.jsonl",
		OpenObject: func(_ context.Context, _ string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(
				`{"id":"a","occurred_at":"2026-05-08T14:00:00Z"}` + "\n",
			)), nil
		},
		EventTime: func(e ev) time.Time { return e.OccurredAt },
	})
	ch := make(chan source.Record[ev], 1)
	done := make(chan error, 1)
	go func() { done <- src.Scan(context.Background(), ch); close(ch) }()
	var got []source.Record[ev]
	for r := range ch {
		got = append(got, r)
	}
	if err := <-done; err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !got[0].EventTime.Equal(want) {
		t.Errorf("EventTime: got %v, want %v", got[0].EventTime, want)
	}
}

func TestScan_PrefixScanMultipleObjects(t *testing.T) {
	// Three keys under the same prefix; sequential mode preserves
	// lexicographic order in the emitted record stream.
	bodies := map[string]string{
		"events/year=2026/month=05/day=01/data-001.jsonl": `{"id":"a"}` + "\n" + `{"id":"b"}` + "\n",
		"events/year=2026/month=05/day=01/data-002.jsonl": `{"id":"c"}` + "\n",
		"events/year=2026/month=05/day=02/data-001.jsonl": `{"id":"d"}` + "\n" + `{"id":"e"}` + "\n",
	}
	keys := []string{
		"events/year=2026/month=05/day=01/data-001.jsonl",
		"events/year=2026/month=05/day=01/data-002.jsonl",
		"events/year=2026/month=05/day=02/data-001.jsonl",
	}

	src, err := s3.NewSource(s3.Config[order]{
		Prefix: "events/",
		ListKeys: func(_ context.Context) ([]string, error) {
			return keys, nil
		},
		OpenObject: func(_ context.Context, key string) (io.ReadCloser, error) {
			body, ok := bodies[key]
			if !ok {
				return nil, errors.New("unknown key: " + key)
			}
			return io.NopCloser(strings.NewReader(body)), nil
		},
	})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	got := collectAll(t, src)
	if len(got) != 5 {
		t.Fatalf("len: got %d, want 5", len(got))
	}
	gotIDs := []string{got[0].Value.ID, got[1].Value.ID, got[2].Value.ID, got[3].Value.ID, got[4].Value.ID}
	wantIDs := []string{"a", "b", "c", "d", "e"}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] {
			t.Errorf("sequential record order: got %v, want %v", gotIDs, wantIDs)
			break
		}
	}
}

func TestScan_ListKeysWithKeyFilter(t *testing.T) {
	// Listing returns a mix of data and manifest keys; KeyFilter
	// excludes manifests so they're never opened.
	allKeys := []string{
		"events/_manifest.json",
		"events/data-001.jsonl",
		"events/_SUCCESS",
		"events/data-002.jsonl",
	}
	var opened []string
	var openedMu sync.Mutex

	src, _ := s3.NewSource(s3.Config[order]{
		Prefix: "events/",
		ListKeys: func(_ context.Context) ([]string, error) {
			return allKeys, nil
		},
		KeyFilter: func(k string) bool {
			return strings.HasSuffix(k, ".jsonl")
		},
		OpenObject: func(_ context.Context, key string) (io.ReadCloser, error) {
			openedMu.Lock()
			opened = append(opened, key)
			openedMu.Unlock()
			return io.NopCloser(strings.NewReader(`{"id":"x"}` + "\n")), nil
		},
	})
	got := collectAll(t, src)
	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2", len(got))
	}
	openedMu.Lock()
	defer openedMu.Unlock()
	if len(opened) != 2 || opened[0] != "events/data-001.jsonl" || opened[1] != "events/data-002.jsonl" {
		t.Errorf("KeyFilter should skip non-data keys: opened=%v", opened)
	}
}

func TestScan_BoundedConcurrencyAllRecords(t *testing.T) {
	// 32 keys, 4 lines each → 128 records. Concurrency=8 must emit
	// every record exactly once (order is non-deterministic).
	const nKeys = 32
	const linesPerKey = 4
	keys := make([]string, nKeys)
	for i := 0; i < nKeys; i++ {
		keys[i] = "shard-" + strconv.Itoa(i) + ".jsonl"
	}

	var inFlight int64
	var maxInFlight int64
	var opens int64

	src, _ := s3.NewSource(s3.Config[order]{
		Prefix:      "events/",
		Concurrency: 8,
		ListKeys: func(_ context.Context) ([]string, error) {
			return keys, nil
		},
		OpenObject: func(_ context.Context, key string) (io.ReadCloser, error) {
			atomic.AddInt64(&opens, 1)
			cur := atomic.AddInt64(&inFlight, 1)
			defer atomic.AddInt64(&inFlight, -1)
			// Track the high-water mark for the parallelism assertion.
			for {
				m := atomic.LoadInt64(&maxInFlight)
				if cur <= m || atomic.CompareAndSwapInt64(&maxInFlight, m, cur) {
					break
				}
			}
			// Yield so peer workers have a chance to enter OpenObject
			// before we release the in-flight slot. Without this hold,
			// the synthetic workload (NopCloser over a tiny string) can
			// finish faster than the dispatcher pushes the next key,
			// leaving maxInFlight at 1 on slow runners under -race —
			// the spurious-failure mode that bit CI on 2026-05-16. The
			// sibling TestScan_BoundedConcurrencyCapsAtConfig uses the
			// same trick (2 ms).
			time.Sleep(2 * time.Millisecond)
			// Build a tiny body unique to this key.
			var b strings.Builder
			for i := 0; i < linesPerKey; i++ {
				b.WriteString(`{"id":"`)
				b.WriteString(key)
				b.WriteString(":")
				b.WriteString(strconv.Itoa(i))
				b.WriteString(`"}` + "\n")
			}
			return io.NopCloser(strings.NewReader(b.String())), nil
		},
		EventID: func(o order, _ string, _ int) string {
			// Natural-id EventID so the exactly-once assertion is
			// invariant to worker scheduling.
			return o.ID
		},
	})

	got := collectAll(t, src)
	if len(got) != nKeys*linesPerKey {
		t.Fatalf("len: got %d, want %d", len(got), nKeys*linesPerKey)
	}
	if atomic.LoadInt64(&opens) != int64(nKeys) {
		t.Errorf("opens: got %d, want %d (each key opened once)", opens, nKeys)
	}
	if got := atomic.LoadInt64(&maxInFlight); got < 2 {
		// 8 workers across 32 keys must overlap; a >=2 floor is a
		// safe lower bound for this fixture even on a single-CPU
		// runner because the test workload is synthetic and the
		// scheduler will multiplex.
		t.Errorf("maxInFlight: got %d, want >= 2 (concurrent fetch was requested)", got)
	}
	// Exactly-once emission.
	seen := make(map[string]int, len(got))
	for _, r := range got {
		seen[r.EventID]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("duplicate emission for %q: %d", id, n)
		}
	}
	if len(seen) != nKeys*linesPerKey {
		t.Errorf("distinct EventIDs: got %d, want %d", len(seen), nKeys*linesPerKey)
	}
}

func TestScan_BoundedConcurrencyCapsAtConfig(t *testing.T) {
	// 16 keys, Concurrency=4 → the high-water in-flight count must
	// never exceed 4. The sleep is deliberate: it guarantees each
	// worker holds the slot long enough for peers to overlap, so
	// the upper bound is tight rather than coincidental.
	const nKeys = 16
	const conc = 4
	keys := make([]string, nKeys)
	for i := range keys {
		keys[i] = "k-" + strconv.Itoa(i)
	}

	var inFlight int64
	var maxInFlight int64

	src, _ := s3.NewSource(s3.Config[order]{
		Prefix:      "p/",
		Concurrency: conc,
		ListKeys: func(_ context.Context) ([]string, error) {
			return keys, nil
		},
		OpenObject: func(_ context.Context, _ string) (io.ReadCloser, error) {
			cur := atomic.AddInt64(&inFlight, 1)
			for {
				m := atomic.LoadInt64(&maxInFlight)
				if cur <= m || atomic.CompareAndSwapInt64(&maxInFlight, m, cur) {
					break
				}
			}
			// Yield so peers have a chance to overlap before we
			// release the slot.
			time.Sleep(2 * time.Millisecond)
			atomic.AddInt64(&inFlight, -1)
			return io.NopCloser(strings.NewReader(`{"id":"x"}` + "\n")), nil
		},
	})
	_ = collectAll(t, src)
	if got := atomic.LoadInt64(&maxInFlight); got > conc {
		t.Errorf("maxInFlight: got %d, want <= %d (bounded by Concurrency)", got, conc)
	}
}

func TestScan_ConcurrencyOneIsSequential(t *testing.T) {
	// Concurrency=1 (default) must scan in lexicographic key order.
	keys := []string{"a.jsonl", "b.jsonl", "c.jsonl"}
	bodies := map[string]string{
		"a.jsonl": `{"id":"a-1"}` + "\n",
		"b.jsonl": `{"id":"b-1"}` + "\n",
		"c.jsonl": `{"id":"c-1"}` + "\n",
	}
	src, _ := s3.NewSource(s3.Config[order]{
		Prefix: "p/",
		ListKeys: func(_ context.Context) ([]string, error) {
			return keys, nil
		},
		OpenObject: func(_ context.Context, key string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(bodies[key])), nil
		},
	})
	got := collectAll(t, src)
	if got[0].Value.ID != "a-1" || got[1].Value.ID != "b-1" || got[2].Value.ID != "c-1" {
		t.Errorf("sequential order: got %+v", got)
	}
}

func TestScan_ConcurrencyContextCancel(t *testing.T) {
	// 50 keys, Concurrency=4; consumer cancels after a few records.
	// Scan must return promptly without leaking goroutines.
	keys := make([]string, 50)
	for i := range keys {
		keys[i] = "k-" + strconv.Itoa(i)
	}
	src, _ := s3.NewSource(s3.Config[order]{
		Prefix:      "p/",
		Concurrency: 4,
		ListKeys: func(_ context.Context) ([]string, error) {
			return keys, nil
		},
		OpenObject: func(_ context.Context, _ string) (io.ReadCloser, error) {
			// Each object emits 1 line and sleeps briefly, so the
			// consumer can cancel mid-stream.
			time.Sleep(1 * time.Millisecond)
			return io.NopCloser(strings.NewReader(`{"id":"x"}` + "\n")), nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan source.Record[order], 1)
	done := make(chan error, 1)
	go func() {
		done <- src.Scan(ctx, ch)
		close(ch)
	}()
	// Drain until we've seen a handful of records, then cancel.
	seen := 0
	for r := range ch {
		_ = r
		seen++
		if seen == 3 {
			cancel()
		}
	}
	err := <-done
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Scan: got %v, want nil or context.Canceled", err)
	}
}

func TestName(t *testing.T) {
	src, _ := s3.NewSource(s3.Config[order]{
		Bucket: "my-bucket",
		Prefix: "events/",
		OpenObject: func(_ context.Context, _ string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("")), nil
		},
	})
	if got := src.Name(); got != "s3:my-bucket/events/" {
		t.Errorf("Name: got %q", got)
	}
}
