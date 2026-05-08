// Package bootstrap drives a Murmur pipeline in Bootstrap mode: it reads from a
// SnapshotSource (Mongo collection scan, DynamoDB ParallelScan, etc.), applies the
// pipeline's key/value extractors and monoid Combine, and writes to the primary state
// store — populating initial state when the live event stream lacks full history.
//
// The pattern matches Debezium's "snapshot then stream":
//
//  1. Capture a HandoffToken from the snapshot source. This is the resume position the
//     live source should pick up from once bootstrap completes.
//  2. Scan the snapshot to completion, applying each record through the pipeline.
//  3. Return the captured token. The deployment system stores it and starts the live
//     runtime configured to begin from that point.
//
// At-least-once dedup at the state store handles re-emissions if bootstrap is retried.
// HandoffToken is opaque to the bootstrap runtime — the live source's NewSourceFromToken
// (or equivalent) interprets it.
package bootstrap

import (
	"context"
	"fmt"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/processor"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// RunOption configures Run.
type RunOption func(*runConfig)

type runConfig struct {
	processor.Config
	// failOnError, when true, surfaces the first dead-lettered record's
	// error to the caller and aborts the bootstrap. Default false: dead-
	// lettered records are recorded via the metrics.Recorder and the
	// bootstrap continues — a single poison row in a Mongo collection
	// shouldn't fail the whole snapshot, especially for retry-driven
	// re-runs.
	failOnError bool
}

// WithMetrics installs a metrics.Recorder. Defaults to metrics.Noop{}.
func WithMetrics(r metrics.Recorder) RunOption {
	return func(c *runConfig) {
		if r != nil {
			c.Recorder = r
		}
	}
}

// WithMaxAttempts sets the per-record retry budget for transient store
// failures. Defaults to 3. A bootstrap that fails-fast on the first
// throttled DDB write is unfit for snapshotting a large collection — a
// 30-minute scan needs retries to absorb intermittent backpressure.
func WithMaxAttempts(n int) RunOption {
	return func(c *runConfig) {
		if n >= 1 {
			c.MaxAttempts = n
		}
	}
}

// WithRetryBackoff configures the per-attempt sleep schedule. Doubles
// after each failure starting from base, capped at max, with full jitter.
// Defaults to 50 ms / 5 s — same as the streaming runtime.
func WithRetryBackoff(base, max time.Duration) RunOption {
	return func(c *runConfig) {
		if base > 0 {
			c.BackoffBase = base
		}
		if max > 0 {
			c.BackoffMax = max
		}
	}
}

// WithDedup installs a state.Deduper for snapshot-side dedup. Useful when a
// bootstrap is re-run (operator retry, partial-failure recovery): without it
// every document is folded again on re-run; with it only documents not yet
// claimed are processed. The Deduper sees the same EventID derivation the
// SnapshotSource emits — for Mongo that's the document _id, so re-running
// against the same collection is idempotent.
func WithDedup(d state.Deduper) RunOption {
	return func(c *runConfig) {
		if d != nil {
			c.Dedup = d
		}
	}
}

// WithFailOnError configures whether the bootstrap aborts on the first
// dead-lettered record (after retries exhausted). Default false — the
// runtime records the error via metrics.Recorder and continues. Set true
// when the snapshot must complete fully or not at all (e.g., a one-shot
// migration where a missing entity is a correctness bug, not a tolerable
// blip). The streaming runtime never aborts on poison records; bootstrap
// is permissive by default for the same reason.
func WithFailOnError(v bool) RunOption {
	return func(c *runConfig) {
		c.failOnError = v
	}
}

// Run drives the bootstrap. Returns the captured HandoffToken on success; the deployment
// system persists it and hands it to the live runtime on transition.
func Run[T any, V any](
	ctx context.Context,
	p *pipeline.Pipeline[T, V],
	src snapshot.Source[T],
	opts ...RunOption,
) (snapshot.HandoffToken, error) {
	if err := p.Build(); err != nil {
		return nil, fmt.Errorf("bootstrap.Run: %w", err)
	}
	if src == nil {
		return nil, fmt.Errorf("bootstrap.Run: snapshot source is nil")
	}
	cfg := runConfig{Config: processor.Defaults()}
	for _, o := range opts {
		o(&cfg)
	}

	token, err := src.CaptureHandoff(ctx)
	if err != nil {
		cfg.Recorder.RecordError(p.Name(), fmt.Errorf("capture handoff: %w", err))
		return nil, fmt.Errorf("capture handoff: %w", err)
	}

	name := p.Name()
	keysFn := p.KeysFn() // multi-key aware; falls back to wrapping KeyFn in a 1-element slice
	valueFn := p.ValueFn()
	store := p.Store()
	cache := p.CacheStore()
	window := p.Window()

	records := make(chan source.Record[T], 1024)
	scanErr := make(chan error, 1)
	go func() {
		scanErr <- src.Scan(ctx, records)
		close(records)
	}()

	for rec := range records {
		eventTime := rec.EventTime
		if eventTime.IsZero() {
			eventTime = time.Now()
		}
		err := processor.MergeMany(ctx, &cfg.Config, name, rec.EventID, eventTime,
			keysFn(rec.Value), valueFn(rec.Value), store, cache, window)
		if err != nil {
			// Either context cancellation or retry exhaustion. Either way,
			// the record is dead-lettered (processor.MergeMany already
			// recorded the metric). Decide between abort and continue
			// per the failOnError flag.
			if cfg.failOnError {
				return nil, err
			}
			// Continue: the metrics surface carries the dead-letter event,
			// the operator sees the count, and the bootstrap finishes.
		}
		if rec.Ack != nil {
			if err := rec.Ack(); err != nil {
				cfg.Recorder.RecordError(name, fmt.Errorf("snapshot ack: %w", err))
			}
		}
	}
	if err := <-scanErr; err != nil {
		cfg.Recorder.RecordError(name, fmt.Errorf("snapshot scan: %w", err))
		return nil, fmt.Errorf("snapshot scan: %w", err)
	}

	return token, nil
}
