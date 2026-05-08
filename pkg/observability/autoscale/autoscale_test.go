package autoscale_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/observability/autoscale"
)

// fakeEmitter records every Emit call for test inspection.
type fakeEmitter struct {
	mu     sync.Mutex
	emits  []emitCall
	errOnN int   // when > 0, the Nth Emit (1-indexed) returns an error
	emitN  int64 // counter of Emit calls so we can compare
}

type emitCall struct {
	name  string
	value float64
	dims  map[string]string
}

func (e *fakeEmitter) Emit(_ context.Context, name string, value float64, dims map[string]string) error {
	n := atomic.AddInt64(&e.emitN, 1)
	if e.errOnN > 0 && int(n) == e.errOnN {
		return errors.New("simulated emit failure")
	}
	cp := make(map[string]string, len(dims))
	for k, v := range dims {
		cp[k] = v
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.emits = append(e.emits, emitCall{name: name, value: value, dims: cp})
	return nil
}

func (e *fakeEmitter) Close() error { return nil }

func (e *fakeEmitter) snapshot() []emitCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp := make([]emitCall, len(e.emits))
	copy(cp, e.emits)
	return cp
}

func TestRun_PeriodicEmits(t *testing.T) {
	em := &fakeEmitter{}
	signal := func(context.Context) (float64, map[string]string, error) {
		return 42, map[string]string{"env": "test"}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 220*time.Millisecond)
	defer cancel()

	if err := autoscale.Run(ctx, "test_metric", 50*time.Millisecond, signal, em); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := em.snapshot()
	// 220ms / 50ms = 4 ticks. Allow ±1 for scheduler jitter.
	if len(got) < 3 || len(got) > 5 {
		t.Errorf("emits: got %d, want 3-5", len(got))
	}
	for i, e := range got {
		if e.name != "test_metric" {
			t.Errorf("[%d] name: got %q, want test_metric", i, e.name)
		}
		if e.value != 42 {
			t.Errorf("[%d] value: got %v, want 42", i, e.value)
		}
		if e.dims["env"] != "test" {
			t.Errorf("[%d] dims: got %v", i, e.dims)
		}
	}
}

func TestRun_SignalErrorTriggersCallback_ContinuesLoop(t *testing.T) {
	em := &fakeEmitter{}

	var (
		mu             sync.Mutex
		signalErrCount int
	)
	var sigN atomic.Int64
	signal := func(context.Context) (float64, map[string]string, error) {
		n := sigN.Add(1)
		if n%2 == 1 {
			return 0, nil, errors.New("transient signal failure")
		}
		return float64(n), nil, nil
	}
	onSig := func(_ string, _ error) {
		mu.Lock()
		signalErrCount++
		mu.Unlock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 220*time.Millisecond)
	defer cancel()

	if err := autoscale.Run(ctx, "m", 50*time.Millisecond, signal, em,
		autoscale.WithOnSignalError(onSig),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	emits := em.snapshot()
	mu.Lock()
	errs := signalErrCount
	mu.Unlock()
	// Half of the ticks errored on the signal, the other half emitted.
	if errs == 0 {
		t.Error("expected signal-error callback to fire on alternating ticks")
	}
	if len(emits) == 0 {
		t.Error("expected at least one successful emit despite signal errors")
	}
	t.Logf("signal errors=%d emits=%d", errs, len(emits))
}

func TestRun_EmitErrorTriggersCallback_ContinuesLoop(t *testing.T) {
	em := &fakeEmitter{errOnN: 1} // first emit fails
	signal := func(context.Context) (float64, map[string]string, error) {
		return 1, nil, nil
	}
	var emitErrs atomic.Int64
	onEmit := func(_ string, _ error) { emitErrs.Add(1) }

	ctx, cancel := context.WithTimeout(context.Background(), 220*time.Millisecond)
	defer cancel()

	if err := autoscale.Run(ctx, "m", 50*time.Millisecond, signal, em,
		autoscale.WithOnEmitError(onEmit),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if emitErrs.Load() < 1 {
		t.Errorf("emit-error callback didn't fire")
	}
	emits := em.snapshot()
	if len(emits) == 0 {
		t.Error("expected loop to continue after first emit's error")
	}
}

func TestRun_RejectsNilSignal(t *testing.T) {
	err := autoscale.Run(context.Background(), "m", time.Second, nil, &fakeEmitter{})
	if err == nil {
		t.Error("expected error on nil signal")
	}
}

func TestRun_RejectsNilEmitter(t *testing.T) {
	err := autoscale.Run(context.Background(), "m", time.Second,
		func(context.Context) (float64, map[string]string, error) { return 0, nil, nil },
		nil,
	)
	if err == nil {
		t.Error("expected error on nil emitter")
	}
}

func TestRun_RejectsZeroPeriod(t *testing.T) {
	err := autoscale.Run(context.Background(), "m", 0,
		func(context.Context) (float64, map[string]string, error) { return 0, nil, nil },
		&fakeEmitter{},
	)
	if err == nil {
		t.Error("expected error on zero period")
	}
}

func TestRun_RejectsEmptyName(t *testing.T) {
	err := autoscale.Run(context.Background(), "", time.Second,
		func(context.Context) (float64, map[string]string, error) { return 0, nil, nil },
		&fakeEmitter{},
	)
	if err == nil {
		t.Error("expected error on empty name")
	}
}

func TestRun_InitialDelayHonored(t *testing.T) {
	em := &fakeEmitter{}
	signal := func(context.Context) (float64, map[string]string, error) { return 1, nil, nil }
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_ = autoscale.Run(ctx, "m", 30*time.Millisecond, signal, em,
		autoscale.WithInitialDelay(60*time.Millisecond),
	)
	elapsed := time.Since(start)
	if elapsed < 60*time.Millisecond {
		t.Errorf("initial delay didn't apply: elapsed %v", elapsed)
	}
	emits := em.snapshot()
	if len(emits) > 2 {
		t.Errorf("with 60ms initial + 30ms period in 100ms total, expected ≤2 emits, got %d", len(emits))
	}
}

func TestRun_ContextCancelStops(t *testing.T) {
	em := &fakeEmitter{}
	signal := func(context.Context) (float64, map[string]string, error) { return 1, nil, nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- autoscale.Run(ctx, "m", 30*time.Millisecond, signal, em)
	}()

	time.Sleep(70 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v on ctx cancel; want nil", err)
		}
	case <-time.After(time.Second):
		t.Error("Run didn't return after ctx cancel")
	}
}

func TestEventsPerSecond_DerivesRate(t *testing.T) {
	// Synthetic getter: events advance by 100 per call. 50ms period →
	// expected rate ≈ 2000 eps after the first sample (which is a
	// no-data-yet zero).
	var count uint64
	getter := func(context.Context) (uint64, error) {
		count += 100
		return count, nil
	}
	signal := autoscale.EventsPerSecond(getter)

	ctx := context.Background()
	v0, _, _ := signal(ctx) // first sample: no diff
	if v0 != 0 {
		t.Errorf("first sample: got %v, want 0", v0)
	}

	time.Sleep(50 * time.Millisecond)
	v1, _, _ := signal(ctx)
	// Expect ~100 events / ~50ms ≈ 2000 eps, ±generous slack.
	if v1 < 1000 || v1 > 3500 {
		t.Errorf("rate: got %v, want ~2000 eps", v1)
	}
}

func TestEventsPerSecond_HandlesGetterError(t *testing.T) {
	signal := autoscale.EventsPerSecond(func(context.Context) (uint64, error) {
		return 0, errors.New("recorder unavailable")
	})
	_, _, err := signal(context.Background())
	if err == nil {
		t.Error("expected error from failing getter")
	}
}

func TestEventsPerSecond_ResetsOnRollback(t *testing.T) {
	// If the cumulative count goes BACKWARDS (recorder restart, etc.),
	// the rate should be 0, not negative.
	getter := func() func(context.Context) (uint64, error) {
		seq := []uint64{100, 50, 200}
		i := 0
		return func(context.Context) (uint64, error) {
			if i >= len(seq) {
				return seq[len(seq)-1], nil
			}
			v := seq[i]
			i++
			return v, nil
		}
	}()
	signal := autoscale.EventsPerSecond(getter)
	_, _, _ = signal(context.Background()) // 100, no rate yet
	time.Sleep(20 * time.Millisecond)
	v2, _, _ := signal(context.Background()) // 50, rollback
	if v2 < 0 {
		t.Errorf("rate after rollback: got %v, want >= 0 (no negative rates)", v2)
	}
}
