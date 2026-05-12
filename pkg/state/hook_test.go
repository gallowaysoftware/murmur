package state_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/state"
)

// hookFakeStore is a minimal Store[int64] used to exercise the OnMerge
// wrapper without pulling in any backend. It captures the last MergeUpdate
// args and can be primed to fail.
type hookFakeStore struct {
	m         map[state.Key]int64
	mergeErr  error
	getErr    error
	closeErr  error
	mergeArgs struct {
		key   state.Key
		delta int64
		ttl   time.Duration
		count int
	}
}

func (s *hookFakeStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	if s.getErr != nil {
		return 0, false, s.getErr
	}
	v, ok := s.m[k]
	return v, ok, nil
}

func (s *hookFakeStore) GetMany(_ context.Context, ks []state.Key) ([]int64, []bool, error) {
	if s.getErr != nil {
		return nil, nil, s.getErr
	}
	vs := make([]int64, len(ks))
	oks := make([]bool, len(ks))
	for i, k := range ks {
		vs[i], oks[i] = s.m[k]
	}
	return vs, oks, nil
}

func (s *hookFakeStore) MergeUpdate(_ context.Context, k state.Key, delta int64, ttl time.Duration) error {
	if s.mergeErr != nil {
		return s.mergeErr
	}
	if s.m == nil {
		s.m = map[state.Key]int64{}
	}
	s.m[k] += delta
	s.mergeArgs.key = k
	s.mergeArgs.delta = delta
	s.mergeArgs.ttl = ttl
	s.mergeArgs.count++
	return nil
}

func (s *hookFakeStore) Close() error { return s.closeErr }

func TestWithOnMerge_NilFn_ReturnsInner(t *testing.T) {
	inner := &hookFakeStore{}
	wrapped := state.WithOnMerge[int64](inner, nil)
	// nil fn → wrapper returns inner directly. Behavioral check rather than
	// pointer identity: MergeUpdate must still succeed.
	if err := wrapped.MergeUpdate(context.Background(), state.Key{Entity: "x"}, 7, 0); err != nil {
		t.Fatalf("MergeUpdate: %v", err)
	}
	if inner.mergeArgs.count != 1 {
		t.Errorf("inner MergeUpdate not called: count=%d", inner.mergeArgs.count)
	}
}

func TestWithOnMerge_HookFiresOnSuccess(t *testing.T) {
	inner := &hookFakeStore{}
	var (
		gotKey   state.Key
		gotDelta int64
		hookHits int32
	)
	hook := func(_ context.Context, k state.Key, d int64) error {
		gotKey = k
		gotDelta = d
		atomic.AddInt32(&hookHits, 1)
		return nil
	}
	wrapped := state.WithOnMerge[int64](inner, hook)

	wantKey := state.Key{Entity: "page-42", Bucket: 7}
	if err := wrapped.MergeUpdate(context.Background(), wantKey, 9, time.Hour); err != nil {
		t.Fatalf("MergeUpdate: %v", err)
	}
	if atomic.LoadInt32(&hookHits) != 1 {
		t.Errorf("hook hits: got %d, want 1", hookHits)
	}
	if gotKey != wantKey {
		t.Errorf("hook key: got %+v, want %+v", gotKey, wantKey)
	}
	if gotDelta != 9 {
		t.Errorf("hook delta: got %d, want 9", gotDelta)
	}
	if inner.m[wantKey] != 9 {
		t.Errorf("inner value: got %d, want 9", inner.m[wantKey])
	}
}

func TestWithOnMerge_HookSkippedOnMergeFailure(t *testing.T) {
	mergeErr := errors.New("ddb down")
	inner := &hookFakeStore{mergeErr: mergeErr}
	var hookHits int32
	hook := func(_ context.Context, _ state.Key, _ int64) error {
		atomic.AddInt32(&hookHits, 1)
		return nil
	}
	wrapped := state.WithOnMerge[int64](inner, hook)

	err := wrapped.MergeUpdate(context.Background(), state.Key{Entity: "x"}, 1, 0)
	if !errors.Is(err, mergeErr) {
		t.Errorf("MergeUpdate: got %v, want %v", err, mergeErr)
	}
	if atomic.LoadInt32(&hookHits) != 0 {
		t.Errorf("hook fired on failed merge: hits=%d, want 0", hookHits)
	}
}

func TestWithOnMerge_HookErrorLoggedByDefault(t *testing.T) {
	inner := &hookFakeStore{}
	hookErr := errors.New("kafka publish failed")
	hook := func(_ context.Context, _ state.Key, _ int64) error { return hookErr }

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	wrapped := state.WithOnMerge[int64](inner, hook, state.WithHookLogger(logger))

	err := wrapped.MergeUpdate(context.Background(), state.Key{Entity: "x", Bucket: 3}, 1, 0)
	if err != nil {
		t.Errorf("MergeUpdate: got %v, want nil (hook errors logged-only by default)", err)
	}
	// Inner merge should have run.
	if inner.mergeArgs.count != 1 {
		t.Errorf("inner MergeUpdate count: got %d, want 1", inner.mergeArgs.count)
	}
	logOut := buf.String()
	if !strings.Contains(logOut, "post-write hook failed") {
		t.Errorf("log output missing hook-failed message: %q", logOut)
	}
	if !strings.Contains(logOut, "kafka publish failed") {
		t.Errorf("log output missing inner error: %q", logOut)
	}
	if !strings.Contains(logOut, "entity=x") {
		t.Errorf("log output missing entity attribute: %q", logOut)
	}
}

func TestWithOnMerge_HookErrorPropagationOptIn(t *testing.T) {
	inner := &hookFakeStore{}
	hookErr := errors.New("kafka publish failed")
	hook := func(_ context.Context, _ state.Key, _ int64) error { return hookErr }

	wrapped := state.WithOnMerge[int64](inner, hook, state.WithHookErrorPropagation(true))

	err := wrapped.MergeUpdate(context.Background(), state.Key{Entity: "x"}, 1, 0)
	if !errors.Is(err, hookErr) {
		t.Errorf("MergeUpdate: got %v, want %v (opted in to propagation)", err, hookErr)
	}
	// The inner merge must still have run — the wrapper does NOT roll back.
	if inner.mergeArgs.count != 1 {
		t.Errorf("inner MergeUpdate count: got %d, want 1 (no rollback)", inner.mergeArgs.count)
	}
	if inner.m[state.Key{Entity: "x"}] != 1 {
		t.Errorf("inner value: got %d, want 1 (no rollback)", inner.m[state.Key{Entity: "x"}])
	}
}

func TestWithOnMerge_PropagationDisabledExplicit(t *testing.T) {
	inner := &hookFakeStore{}
	hookErr := errors.New("hook failed")
	hook := func(_ context.Context, _ state.Key, _ int64) error { return hookErr }

	// Passing false explicitly should match the default behavior.
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	wrapped := state.WithOnMerge[int64](inner, hook,
		state.WithHookErrorPropagation(false),
		state.WithHookLogger(logger),
	)

	if err := wrapped.MergeUpdate(context.Background(), state.Key{Entity: "x"}, 1, 0); err != nil {
		t.Errorf("MergeUpdate: got %v, want nil", err)
	}
}

func TestWithOnMerge_GetPassthrough(t *testing.T) {
	inner := &hookFakeStore{m: map[state.Key]int64{{Entity: "x"}: 42}}
	wrapped := state.WithOnMerge[int64](inner, func(_ context.Context, _ state.Key, _ int64) error {
		t.Errorf("hook should not fire on Get")
		return nil
	})

	v, ok, err := wrapped.Get(context.Background(), state.Key{Entity: "x"})
	if err != nil || !ok || v != 42 {
		t.Errorf("Get: got (%d, %v, %v), want (42, true, nil)", v, ok, err)
	}

	v2, ok2, err2 := wrapped.Get(context.Background(), state.Key{Entity: "missing"})
	if err2 != nil || ok2 || v2 != 0 {
		t.Errorf("Get(missing): got (%d, %v, %v), want (0, false, nil)", v2, ok2, err2)
	}
}

func TestWithOnMerge_GetManyPassthrough(t *testing.T) {
	inner := &hookFakeStore{m: map[state.Key]int64{
		{Entity: "a"}: 1,
		{Entity: "b"}: 2,
	}}
	wrapped := state.WithOnMerge[int64](inner, func(_ context.Context, _ state.Key, _ int64) error {
		t.Errorf("hook should not fire on GetMany")
		return nil
	})

	keys := []state.Key{{Entity: "a"}, {Entity: "missing"}, {Entity: "b"}}
	vs, oks, err := wrapped.GetMany(context.Background(), keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	wantVs := []int64{1, 0, 2}
	wantOks := []bool{true, false, true}
	for i := range keys {
		if vs[i] != wantVs[i] || oks[i] != wantOks[i] {
			t.Errorf("GetMany[%d]: got (%d, %v), want (%d, %v)",
				i, vs[i], oks[i], wantVs[i], wantOks[i])
		}
	}
}

func TestWithOnMerge_GetErrorPassthrough(t *testing.T) {
	innerErr := errors.New("ddb read failed")
	inner := &hookFakeStore{getErr: innerErr}
	wrapped := state.WithOnMerge[int64](inner, func(_ context.Context, _ state.Key, _ int64) error {
		return nil
	})

	if _, _, err := wrapped.Get(context.Background(), state.Key{Entity: "x"}); !errors.Is(err, innerErr) {
		t.Errorf("Get error: got %v, want %v", err, innerErr)
	}
	if _, _, err := wrapped.GetMany(context.Background(), []state.Key{{Entity: "x"}}); !errors.Is(err, innerErr) {
		t.Errorf("GetMany error: got %v, want %v", err, innerErr)
	}
}

func TestWithOnMerge_ClosePassthrough(t *testing.T) {
	closeErr := errors.New("close failed")
	inner := &hookFakeStore{closeErr: closeErr}
	wrapped := state.WithOnMerge[int64](inner, func(_ context.Context, _ state.Key, _ int64) error {
		return nil
	})

	if err := wrapped.Close(); !errors.Is(err, closeErr) {
		t.Errorf("Close error: got %v, want %v", err, closeErr)
	}

	inner2 := &hookFakeStore{}
	wrapped2 := state.WithOnMerge[int64](inner2, func(_ context.Context, _ state.Key, _ int64) error {
		return nil
	})
	if err := wrapped2.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestWithOnMerge_DeltaAndKeyArePassedThrough(t *testing.T) {
	// Verify the hook sees exactly what was applied — same Key, same delta,
	// across multiple merges with different values.
	inner := &hookFakeStore{}
	type observation struct {
		key   state.Key
		delta int64
	}
	var observed []observation
	hook := func(_ context.Context, k state.Key, d int64) error {
		observed = append(observed, observation{key: k, delta: d})
		return nil
	}
	wrapped := state.WithOnMerge[int64](inner, hook)

	merges := []observation{
		{key: state.Key{Entity: "a", Bucket: 0}, delta: 1},
		{key: state.Key{Entity: "a", Bucket: 0}, delta: 2},
		{key: state.Key{Entity: "b", Bucket: 5}, delta: 100},
	}
	for _, m := range merges {
		if err := wrapped.MergeUpdate(context.Background(), m.key, m.delta, 0); err != nil {
			t.Fatalf("MergeUpdate: %v", err)
		}
	}
	if len(observed) != len(merges) {
		t.Fatalf("observed: got %d, want %d", len(observed), len(merges))
	}
	for i, m := range merges {
		if observed[i] != m {
			t.Errorf("observed[%d]: got %+v, want %+v", i, observed[i], m)
		}
	}
}
