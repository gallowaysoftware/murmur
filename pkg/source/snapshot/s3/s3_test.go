package s3_test

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"

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
