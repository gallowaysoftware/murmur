// Package processor is the shared per-record processing core that every
// Murmur runtime delegates to. It owns the at-least-once-with-dedup contract,
// the retry-with-backoff loop, and the metrics surface — three concerns that
// would otherwise be duplicated across pkg/exec/streaming, pkg/exec/lambda/*,
// and any future driver (SQS, SNS-fronted EventBridge, etc.).
//
// The package is exported so out-of-tree drivers can sit on the same retry /
// dedup contract without forking the logic. The API surface is intentionally
// small: one Config struct, one MergeOne function. Drivers wire their
// record-decoding to MergeOne and decide what to do with non-nil returns
// (the Kinesis Lambda adds the record to BatchItemFailures; the streaming
// runtime Acks past the poison record after dead-lettering it).
//
// Stability: the API tracks pkg/exec/streaming and pkg/exec/lambda/*; expect
// the same experimental-pre-1.0 churn as those packages.
package processor

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// Config bundles the shared retry / dedup / observability knobs every
// runtime uses. Construct with sensible defaults via Defaults() and
// override fields as needed.
type Config struct {
	// Recorder is the metrics.Recorder; defaults to metrics.Noop{}.
	// Events fire under PipelineName, retries under "<name>:retry", dedup
	// skips under "<name>:dedup_skip", dead letters under "<name>:dead_letter".
	Recorder metrics.Recorder

	// MaxAttempts is the per-record retry budget. MergeOne returns the last
	// non-cancellation error after this many failed tries.
	MaxAttempts int

	// BackoffBase is the first sleep between retries. Doubles per attempt
	// up to BackoffMax with full jitter.
	BackoffBase time.Duration

	// BackoffMax caps the per-retry sleep.
	BackoffMax time.Duration

	// Dedup, if non-nil, is consulted before MergeOne does any work. A
	// duplicate is a no-op (no merge, no retry, no error) — MergeOne
	// returns nil and records a "<name>:dedup_skip" event.
	Dedup state.Deduper
}

// Defaults returns a Config with the conventional defaults: Noop recorder,
// MaxAttempts=3, 50 ms / 5 s backoff, no Dedup. Override fields after
// construction; later WithX functions also exist for fluent builders.
func Defaults() Config {
	return Config{
		Recorder:    metrics.Noop{},
		MaxAttempts: 3,
		BackoffBase: 50 * time.Millisecond,
		BackoffMax:  5 * time.Second,
	}
}

// MergeOne is the canonical per-record processing entry point. It applies
// the keyFn / valueFn extractors, claims the EventID via Dedup (if
// configured), runs the store and cache MergeUpdates, and retries on
// transient failure with exponential backoff.
//
// Return semantics:
//
//   - nil: the record was processed successfully OR was a duplicate skip.
//     The caller should Ack the record (for sources that have an Ack) or
//     do nothing (for Lambda handlers that report only failures).
//   - non-nil: every retry was exhausted. The error wraps the last
//     underlying failure. The caller decides what to do — typically:
//     add to BatchItemFailures (Lambda) or record + Ack-and-skip
//     (streaming).
//
// MergeOne also short-circuits to a non-nil error on context cancellation
// during a retry backoff so the caller can return promptly rather than
// burning the remaining budget.
//
// MergeOne does NOT call Ack. Sources with an Ack callback should invoke
// it themselves on a nil return.
func MergeOne[T any, V any](
	ctx context.Context,
	cfg *Config,
	pipelineName string,
	eventID string,
	eventTime time.Time,
	value T,
	keyFn func(T) string,
	valueFn func(T) V,
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
) error {
	if cfg.Dedup != nil && eventID != "" {
		first, err := cfg.Dedup.MarkSeen(ctx, eventID)
		if err != nil {
			// Dedup backend transient failure: surface as an error but fall
			// through to normal processing. Silently dropping would
			// double-count when the dedup table comes back; failing closed
			// would stall the pipeline on dedup-table outage. Same policy
			// as the streaming runtime.
			cfg.Recorder.RecordError(pipelineName,
				fmt.Errorf("dedup MarkSeen %q: %w", eventID, err))
		} else if !first {
			// Already processed on a prior delivery; nothing to do.
			cfg.Recorder.RecordEvent(pipelineName + ":dedup_skip")
			return nil
		}
	}

	var lastErr error
	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			if err := backoffWait(ctx, cfg, attempt); err != nil {
				return err
			}
			cfg.Recorder.RecordEvent(pipelineName + ":retry")
		}
		err := mergeOneAttempt(ctx, cfg.Recorder, pipelineName, eventTime, value, keyFn, valueFn, store, cache, window)
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		lastErr = err
	}

	wrapped := fmt.Errorf("pipeline %q event %q failed after %d attempts: %w",
		pipelineName, eventID, cfg.MaxAttempts, lastErr)
	cfg.Recorder.RecordError(pipelineName, wrapped)
	cfg.Recorder.RecordEvent(pipelineName + ":dead_letter")
	return wrapped
}

// mergeOneAttempt runs one merge attempt without retries. Internal — used
// by MergeOne. Cache failures are NOT propagated as errors (the cache is a
// repopulatable accelerator); they're surfaced via RecordError and the
// store-side outcome is what the caller sees.
func mergeOneAttempt[T any, V any](
	ctx context.Context,
	rec metrics.Recorder,
	name string,
	eventTime time.Time,
	value T,
	keyFn func(T) string,
	valueFn func(T) V,
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
) error {
	entity := keyFn(value)
	delta := valueFn(value)

	sk := state.Key{Entity: entity}
	var ttl time.Duration
	if window != nil {
		sk.Bucket = window.BucketID(eventTime)
		ttl = window.Retention
	}

	storeStart := time.Now()
	if err := store.MergeUpdate(ctx, sk, delta, ttl); err != nil {
		return fmt.Errorf("store MergeUpdate: %w", err)
	}
	rec.RecordLatency(name, "store_merge", time.Since(storeStart))

	if cache != nil {
		cacheStart := time.Now()
		if err := cache.MergeUpdate(ctx, sk, delta, ttl); err != nil {
			rec.RecordError(name, fmt.Errorf("cache MergeUpdate: %w", err))
		}
		rec.RecordLatency(name, "cache_merge", time.Since(cacheStart))
	}

	rec.RecordEvent(name)
	return nil
}

func backoffWait(ctx context.Context, cfg *Config, attempt int) error {
	d := cfg.BackoffBase << (attempt - 1)
	if d > cfg.BackoffMax {
		d = cfg.BackoffMax
	}
	if d > 0 {
		d += time.Duration(rand.Int64N(int64(d / 2))) // full jitter
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
