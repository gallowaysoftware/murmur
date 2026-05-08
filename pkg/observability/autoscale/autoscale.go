// Package autoscale emits scaling signals from a Murmur worker so an
// upstream autoscaler (ECS Fargate target tracking, Kubernetes HPA via
// the CloudWatch adapter, etc.) can adjust replica count to load.
//
// Why this exists: ECS Fargate doesn't natively autoscale on Kafka
// consumer lag or Kinesis shard count — only on CPU and memory. Per
// `doc/architecture.md` open question #2, the canonical fix is to
// publish a custom CloudWatch metric and configure target tracking on
// it. This package is the in-process emitter side of that pattern.
//
// # Scope
//
//   - Signal — a periodically-sampled measurement (consumer lag,
//     shard-iterator age, events-per-second, queue depth). User-supplied
//     because the right signal depends on the source.
//   - Emitter — publishes the measurement somewhere. CloudWatch is the
//     reference implementation in pkg/observability/autoscale/cwemitter;
//     Prometheus / Datadog / a Noop are the user's to wire.
//   - Run — periodic loop that ties them together. Worker spawns it once
//     at startup; runs until ctx cancellation.
//
// # What this isn't
//
// Not a metrics framework. The streaming runtime's `metrics.Recorder`
// already records per-pipeline events / errors / latencies — wire that
// to your observability stack via its own Recorder implementation. This
// package is specifically for the SCALING-SIGNAL surface, where the
// autoscaler reads ONE clearly-labeled metric at a target value to
// decide replica count. Conflating the two leads to autoscaler thrash.
package autoscale

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Signal is the periodically-sampled measurement an autoscaler reads.
// Examples:
//
//   - Kafka consumer-group lag (sum of per-partition lag for the group).
//   - Kinesis IteratorAgeMilliseconds (max across shards).
//   - SQS visible-message count (size of the queue waiting to be
//     processed).
//   - Events-per-second from the metrics recorder.
//
// The Signal returns the current value plus optional dimensions
// (CloudWatch labels) that should accompany the metric. Returning an
// error skips the emit but doesn't stop the Run loop — transient
// signal-source errors are normal and shouldn't take the worker down.
type Signal func(ctx context.Context) (value float64, dimensions map[string]string, err error)

// Emitter publishes a metric value. The Emitter contract is intentionally
// minimal so adding a Datadog / Prometheus / generic-HTTP backend is a
// few lines.
type Emitter interface {
	// Emit publishes a single (name, value, dimensions) point. Returns
	// an error only on backend failure; successfully published is
	// silent (the caller will record metrics about emitter success
	// from the outside).
	Emit(ctx context.Context, name string, value float64, dimensions map[string]string) error

	// Close releases any underlying resources (HTTP clients, batchers).
	Close() error
}

// RunOption configures Run.
type RunOption func(*runConfig)

type runConfig struct {
	onEmitError  func(name string, err error)
	onSignalErr  func(name string, err error)
	initialDelay time.Duration
}

// WithOnEmitError installs a callback for metric-publish failures. The
// loop continues regardless; this is the seam for visibility.
func WithOnEmitError(fn func(name string, err error)) RunOption {
	return func(c *runConfig) {
		if fn != nil {
			c.onEmitError = fn
		}
	}
}

// WithOnSignalError installs a callback for Signal sample failures
// (typically a transient connectivity issue with the source the signal
// reads from). Same continue-the-loop semantics.
func WithOnSignalError(fn func(name string, err error)) RunOption {
	return func(c *runConfig) {
		if fn != nil {
			c.onSignalErr = fn
		}
	}
}

// WithInitialDelay sets a stagger before the first emit. Useful when
// many workers come up simultaneously — without a delay, the first
// emit storm hits the metrics backend at the same moment.
func WithInitialDelay(d time.Duration) RunOption {
	return func(c *runConfig) {
		if d > 0 {
			c.initialDelay = d
		}
	}
}

// Run reads `signal` every `period` and publishes each measurement via
// `emitter`. Returns when ctx cancels; non-nil error only if the input
// is invalid (nil signal / nil emitter / non-positive period).
//
// Sample failures and emit failures both fire callbacks (see
// WithOnSignalError / WithOnEmitError) but do not stop the loop —
// autoscaling signals are inherently best-effort, and a stuck emitter
// shouldn't take the worker down.
func Run(
	ctx context.Context,
	name string,
	period time.Duration,
	signal Signal,
	emitter Emitter,
	opts ...RunOption,
) error {
	if signal == nil {
		return errors.New("autoscale.Run: signal is required")
	}
	if emitter == nil {
		return errors.New("autoscale.Run: emitter is required")
	}
	if period <= 0 {
		return errors.New("autoscale.Run: period must be positive")
	}
	if name == "" {
		return errors.New("autoscale.Run: name is required")
	}

	cfg := runConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	if cfg.initialDelay > 0 {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(cfg.initialDelay):
		}
	}

	// Tick at `period`. The Signal is sampled per tick; each successful
	// sample emits one point. Transient errors fall through to the
	// callbacks.
	t := time.NewTicker(period)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			value, dims, err := signal(ctx)
			if err != nil {
				if cfg.onSignalErr != nil {
					cfg.onSignalErr(name, fmt.Errorf("signal sample: %w", err))
				}
				continue
			}
			if err := emitter.Emit(ctx, name, value, dims); err != nil {
				if cfg.onEmitError != nil {
					cfg.onEmitError(name, fmt.Errorf("emit: %w", err))
				}
				continue
			}
		}
	}
}

// EventsPerSecond returns a Signal that derives the events-per-second
// rate for a named pipeline from the metrics.Recorder's events counter.
// Useful when the natural autoscaling signal isn't source-specific lag
// but worker throughput — when EPS rises above target, scale out.
//
// `getter` returns the cumulative events-processed count. Typically
// `func(ctx context.Context) (uint64, error) { return rec.SnapshotOne(name).EventsProcessed, nil }`.
//
// The signal computes the delta between consecutive samples and divides
// by the elapsed wall time, so the first sample always returns 0 (no
// previous to diff against). Subsequent samples are stable.
func EventsPerSecond(getter func(ctx context.Context) (uint64, error)) Signal {
	var (
		lastCount uint64
		lastAt    time.Time
	)
	return func(ctx context.Context) (float64, map[string]string, error) {
		count, err := getter(ctx)
		if err != nil {
			return 0, nil, err
		}
		now := time.Now()
		if lastAt.IsZero() {
			lastCount = count
			lastAt = now
			return 0, nil, nil
		}
		dt := now.Sub(lastAt).Seconds()
		var delta uint64
		if count >= lastCount {
			delta = count - lastCount
		}
		lastCount = count
		lastAt = now
		if dt <= 0 {
			return 0, nil, nil
		}
		return float64(delta) / dt, nil, nil
	}
}
