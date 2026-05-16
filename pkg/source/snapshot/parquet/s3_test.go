package parquet_test

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	pqcompress "github.com/apache/arrow-go/v18/parquet/compress"

	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot/parquet"
)

// collectS3 mirrors collect() but for S3Source.
func collectS3[T any](t *testing.T, ctx context.Context, src *parquet.S3Source[T]) []source.Record[T] {
	t.Helper()
	ch := make(chan source.Record[T], 1024)
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
		t.Fatalf("S3 Scan: %v", err)
	}
	return got
}

func TestS3_PrefixModeWithMultipleKeys(t *testing.T) {
	// Two partition objects under one prefix. OpenObject mode uses the
	// comma-separated Prefix shortcut to feed a synthetic key list.
	keyA := "events/year=2026/month=05/day=01/hour=00/part-0.parquet"
	keyB := "events/year=2026/month=05/day=01/hour=01/part-0.parquet"

	bodyA := buildCountEventParquet(t, []countEvent{
		{EntityID: "a", Count: 1, Year: 2026, Month: 5, Day: 1, Hour: 0},
	}, pqcompress.Codecs.Snappy)
	bodyB := buildCountEventParquet(t, []countEvent{
		{EntityID: "b", Count: 2, Year: 2026, Month: 5, Day: 1, Hour: 1},
	}, pqcompress.Codecs.Snappy)

	objects := map[string][]byte{keyA: bodyA, keyB: bodyB}

	src, err := parquet.NewS3Source(parquet.S3Config[countEvent]{
		Prefix: keyA + "," + keyB,
		Decode: decodeCountEvent,
		OpenObject: func(_ context.Context, key string) ([]byte, error) {
			b, ok := objects[key]
			if !ok {
				return nil, errors.New("unexpected key: " + key)
			}
			return b, nil
		},
	})
	if err != nil {
		t.Fatalf("NewS3Source: %v", err)
	}
	got := collectS3(t, t.Context(), src)
	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2", len(got))
	}
	ids := []string{got[0].Value.EntityID, got[1].Value.EntityID}
	sort.Strings(ids)
	if ids[0] != "a" || ids[1] != "b" {
		t.Errorf("ids: got %v", ids)
	}
}

func TestS3_KeyFilterSkipsNonParquet(t *testing.T) {
	// Default KeyFilter strips _SUCCESS and other non-.parquet keys.
	keyData := "events/year=2026/part-0.parquet"
	keySuccess := "events/year=2026/_SUCCESS"

	body := buildCountEventParquet(t, []countEvent{
		{EntityID: "a", Count: 1},
	}, pqcompress.Codecs.Snappy)

	src, _ := parquet.NewS3Source(parquet.S3Config[countEvent]{
		Prefix: keyData + "," + keySuccess,
		Decode: decodeCountEvent,
		OpenObject: func(_ context.Context, key string) ([]byte, error) {
			if key == keySuccess {
				t.Errorf("default KeyFilter should skip %q", key)
			}
			return body, nil
		},
	})
	got := collectS3(t, t.Context(), src)
	if len(got) != 1 || got[0].Value.EntityID != "a" {
		t.Errorf("got %+v", got)
	}
}

func TestS3_PartitionFilterPrunesUnreadObjects(t *testing.T) {
	// 4 partition objects across 2 days; PartitionFilter keeps only day=02.
	keys := []string{
		"events/year=2026/month=05/day=01/hour=00/part-0.parquet",
		"events/year=2026/month=05/day=01/hour=01/part-0.parquet",
		"events/year=2026/month=05/day=02/hour=00/part-0.parquet",
		"events/year=2026/month=05/day=02/hour=01/part-0.parquet",
	}
	body := buildCountEventParquet(t, []countEvent{
		{EntityID: "x", Count: 1},
	}, pqcompress.Codecs.Snappy)

	var openCalls int64
	src, _ := parquet.NewS3Source(parquet.S3Config[countEvent]{
		Prefix: strings.Join(keys, ","),
		Decode: decodeCountEvent,
		PartitionFilter: func(p parquet.Partition) bool {
			return p.Values["day"] == "02"
		},
		OpenObject: func(_ context.Context, key string) ([]byte, error) {
			atomic.AddInt64(&openCalls, 1)
			if strings.Contains(key, "day=01") {
				t.Errorf("PartitionFilter should have pruned %q", key)
			}
			return body, nil
		},
	})
	got := collectS3(t, t.Context(), src)
	if len(got) != 2 {
		t.Errorf("expected 2 surviving records (1 per day=02 partition), got %d", len(got))
	}
	if openCalls != 2 {
		t.Errorf("OpenObject called %d times, want 2 (PartitionFilter prunes day=01)", openCalls)
	}
}

func TestS3_HivePartitionParse(t *testing.T) {
	// One key per partition combination — verify Partition.Values
	// surfaces year/month/day/hour as the caller sees them in the
	// PartitionFilter callback.
	key := "events/year=2026/month=05/day=08/hour=14/part-0.parquet"
	body := buildCountEventParquet(t, []countEvent{
		{EntityID: "a", Count: 1, Year: 2026, Month: 5, Day: 8, Hour: 14},
	}, pqcompress.Codecs.Snappy)

	var seen parquet.Partition
	src, _ := parquet.NewS3Source(parquet.S3Config[countEvent]{
		Prefix: key,
		Decode: decodeCountEvent,
		PartitionFilter: func(p parquet.Partition) bool {
			seen = p
			return true
		},
		OpenObject: func(_ context.Context, _ string) ([]byte, error) {
			return body, nil
		},
	})
	_ = collectS3(t, t.Context(), src)
	want := map[string]string{
		"year":  "2026",
		"month": "05",
		"day":   "08",
		"hour":  "14",
	}
	for k, v := range want {
		if seen.Values[k] != v {
			t.Errorf("partition %s: got %q, want %q", k, seen.Values[k], v)
		}
	}
	if seen.Key != key {
		t.Errorf("Key: got %q, want %q", seen.Key, key)
	}
}

func TestS3_BoundedConcurrency(t *testing.T) {
	// Confirm MaxConcurrency is honored: build N keys, throttle OpenObject
	// so we can observe simultaneous calls, assert peak ≤ MaxConcurrency.
	const numKeys = 8
	const maxConc = 3
	keys := make([]string, 0, numKeys)
	for i := 0; i < numKeys; i++ {
		keys = append(keys, "events/p="+itoaPad(i)+"/part-0.parquet")
	}
	body := buildCountEventParquet(t, []countEvent{
		{EntityID: "x", Count: 1},
	}, pqcompress.Codecs.Snappy)

	var (
		mu          sync.Mutex
		inFlight    int
		maxInFlight int
		gate        = make(chan struct{}, maxConc)
	)

	src, _ := parquet.NewS3Source(parquet.S3Config[countEvent]{
		Prefix:         strings.Join(keys, ","),
		Decode:         decodeCountEvent,
		MaxConcurrency: maxConc,
		OpenObject: func(_ context.Context, _ string) ([]byte, error) {
			gate <- struct{}{}
			mu.Lock()
			inFlight++
			if inFlight > maxInFlight {
				maxInFlight = inFlight
			}
			mu.Unlock()
			// Hold the slot briefly so other workers can stack up.
			defer func() {
				mu.Lock()
				inFlight--
				mu.Unlock()
				<-gate
			}()
			return body, nil
		},
	})
	got := collectS3(t, t.Context(), src)
	if len(got) != numKeys {
		t.Errorf("len: got %d, want %d", len(got), numKeys)
	}
	if maxInFlight > maxConc {
		t.Errorf("maxInFlight %d > MaxConcurrency %d", maxInFlight, maxConc)
	}
}

func TestS3_EmptyPrefixIsNoOpNoError(t *testing.T) {
	src, _ := parquet.NewS3Source(parquet.S3Config[countEvent]{
		Prefix: "",
		Decode: decodeCountEvent,
		OpenObject: func(_ context.Context, _ string) ([]byte, error) {
			t.Error("OpenObject should not be invoked for an empty prefix")
			return nil, errors.New("unreachable")
		},
	})
	got := collectS3(t, t.Context(), src)
	if len(got) != 0 {
		t.Errorf("expected 0 records, got %d", len(got))
	}
}

func TestS3_EventIDCustom(t *testing.T) {
	key := "events/year=2026/part-0.parquet"
	body := buildCountEventParquet(t, []countEvent{
		{EntityID: "alpha", Count: 1},
	}, pqcompress.Codecs.Snappy)
	src, _ := parquet.NewS3Source(parquet.S3Config[countEvent]{
		Prefix: key,
		Decode: decodeCountEvent,
		EventID: func(e countEvent, k string, _ int) string {
			return "key=" + k + "|id=" + e.EntityID
		},
		OpenObject: func(_ context.Context, _ string) ([]byte, error) {
			return body, nil
		},
	})
	got := collectS3(t, t.Context(), src)
	if len(got) != 1 {
		t.Fatalf("len: got %d", len(got))
	}
	want := "key=" + key + "|id=alpha"
	if got[0].EventID != want {
		t.Errorf("EventID: got %q, want %q", got[0].EventID, want)
	}
}

func TestS3_EventIDDefaultIncludesKey(t *testing.T) {
	key := "events/year=2026/part-0.parquet"
	body := buildCountEventParquet(t, []countEvent{
		{EntityID: "a", Count: 1},
	}, pqcompress.Codecs.Snappy)
	src, _ := parquet.NewS3Source(parquet.S3Config[countEvent]{
		Prefix: key,
		Decode: decodeCountEvent,
		OpenObject: func(_ context.Context, _ string) ([]byte, error) {
			return body, nil
		},
	})
	got := collectS3(t, t.Context(), src)
	if len(got) != 1 || !strings.Contains(got[0].EventID, key) {
		t.Errorf("default EventID should include key: got %q", got[0].EventID)
	}
}

func TestS3_DecodeErrorCallback(t *testing.T) {
	key := "events/year=2026/part-0.parquet"
	body := buildCountEventParquet(t, []countEvent{
		{EntityID: "a", Count: 1},
		{EntityID: "b", Count: 2},
	}, pqcompress.Codecs.Snappy)

	var seenKey string
	var seenRows []int
	src, _ := parquet.NewS3Source(parquet.S3Config[countEvent]{
		Prefix: key,
		Decode: func(rec arrow.RecordBatch, row int) (countEvent, error) {
			if row == 0 {
				return countEvent{}, errors.New("synthetic poison")
			}
			return decodeCountEvent(rec, row)
		},
		OnDecodeError: func(k string, rowOrdinal int, _ error) {
			seenKey = k
			seenRows = append(seenRows, rowOrdinal)
		},
		OpenObject: func(_ context.Context, _ string) ([]byte, error) {
			return body, nil
		},
	})
	got := collectS3(t, t.Context(), src)
	if len(got) != 1 {
		t.Errorf("len: got %d, want 1", len(got))
	}
	if seenKey != key {
		t.Errorf("decode-error key: got %q, want %q", seenKey, key)
	}
	if len(seenRows) != 1 || seenRows[0] != 1 {
		t.Errorf("decode-error rows: got %v, want [1]", seenRows)
	}
}

func TestS3_CaptureHandoffPassThrough(t *testing.T) {
	src, _ := parquet.NewS3Source(parquet.S3Config[countEvent]{
		Prefix:       "k.parquet",
		Decode:       decodeCountEvent,
		HandoffToken: []byte("opaque"),
		OpenObject: func(_ context.Context, _ string) ([]byte, error) {
			return buildCountEventParquet(t, []countEvent{{EntityID: "a"}}, pqcompress.Codecs.Snappy), nil
		},
	})
	tok, err := src.CaptureHandoff(t.Context())
	if err != nil {
		t.Fatalf("CaptureHandoff: %v", err)
	}
	if !bytes.Equal(tok, []byte("opaque")) {
		t.Errorf("token: got %q", tok)
	}
}

func TestS3_Name(t *testing.T) {
	src, _ := parquet.NewS3Source(parquet.S3Config[countEvent]{
		Bucket: "my-bucket",
		Prefix: "events/",
		Decode: decodeCountEvent,
		OpenObject: func(_ context.Context, _ string) ([]byte, error) {
			return nil, errors.New("unused")
		},
	})
	if got := src.Name(); got != "parquet-s3:my-bucket/events/" {
		t.Errorf("Name: got %q", got)
	}
}

func TestNewS3Source_RequiresClientOrOpenObject(t *testing.T) {
	_, err := parquet.NewS3Source(parquet.S3Config[countEvent]{
		Bucket: "b",
		Decode: decodeCountEvent,
	})
	if err == nil {
		t.Error("expected error when both Client and OpenObject are nil")
	}
}

func TestNewS3Source_RequiresDecode(t *testing.T) {
	_, err := parquet.NewS3Source[countEvent](parquet.S3Config[countEvent]{
		OpenObject: func(_ context.Context, _ string) ([]byte, error) {
			return nil, nil
		},
	})
	if err == nil {
		t.Error("expected error when Decode is nil")
	}
}

// itoaPad returns a zero-padded 2-char decimal — used in test key
// construction so the keys sort lexicographically.
func itoaPad(n int) string {
	if n < 10 {
		return "0" + string(rune('0'+n))
	}
	return string(rune('0'+n/10)) + string(rune('0'+n%10))
}
