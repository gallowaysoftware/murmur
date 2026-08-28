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
	"sync"
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

// MergeOne is the single-key per-record processing entry point. It applies
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
//
// Implementation note: MergeOne is now a thin wrapper around MergeMany
// — it extracts the single key + value and delegates. Use MergeMany
// directly for hierarchical-rollup pipelines where one record contributes
// to many keys.
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
	return MergeMany(ctx, cfg, pipelineName, eventID, eventTime,
		[]string{keyFn(value)}, valueFn(value), store, cache, window)
}

// Detached-release budget. Two pressures pull against each other: the caller is
// usually shutting down and something is waiting on it, but a release that never
// happens costs an event permanently, so the budget cannot be so tight that a
// large batch strands most of it.
const (
	// releaseConcurrency is the width of the release worker pool. Each release is
	// one small delete against a distinct dedup key, so they parallelize cleanly
	// and 16 in flight will not itself throttle the dedup table.
	releaseConcurrency = 16

	// releaseBudgetBase is the floor: enough for a handful of claims plus a
	// round-trip's worth of backend latency.
	releaseBudgetBase = 5 * time.Second

	// releaseBudgetPerRound buys one round of the worker pool. The budget has to
	// scale with the claim count because claimedIDs holds one entry per EVENT, not
	// per key: a Coalescer at DefaultMaxKeys, or a 1s FlushTick at a few thousand
	// events/sec, hands ReleaseClaims thousands of IDs at once, and a fixed budget
	// covers the first few hundred and silently strands the rest.
	releaseBudgetPerRound = 20 * time.Millisecond

	// releaseBudgetMax is the hard ceiling, so the scaling cannot itself outrun a
	// SIGTERM grace period or a Lambda's remaining time. Claims the budget does not
	// cover surface as dedup_release_failed rather than going out silently.
	releaseBudgetMax = 30 * time.Second
)

// releaseBudget scales the detached release budget with the number of distinct
// claims: the base floor plus one worker-pool round per releaseConcurrency
// claims, capped at releaseBudgetMax.
func releaseBudget(n int) time.Duration {
	if n <= 0 {
		return releaseBudgetBase
	}
	rounds := (n + releaseConcurrency - 1) / releaseConcurrency
	d := releaseBudgetBase + time.Duration(rounds)*releaseBudgetPerRound
	if d > releaseBudgetMax {
		return releaseBudgetMax
	}
	return d
}

// ReleaseClaims hands dedup claims back for events whose delta never reached the
// store, so the source's redelivery (or a DLQ replay) re-applies them instead of
// hitting dedup_skip and losing the event permanently — silent count loss for
// Sum / HLL / TopK, with no error surface and no metric.
//
// Pass ONLY EventIDs this caller actually won via Deduper.MarkSeen. MarkSeen is
// fail-open — an error means "proceed unclaimed" — so an in-flight event may be
// riding somebody else's claim, and releasing that claim would drop the winner's
// row and let a third delivery re-apply the event. Batching callers therefore
// have to track ownership per EventID, not per batch.
//
// Repeated IDs are released once. Calling with no Dedup configured, or with an
// empty slice, is a no-op, so batching drivers can call it unconditionally on
// their failure path.
//
// Cost. One call is one budget, sized by releaseBudget and spent across a pool of
// releaseConcurrency workers — so batching drivers should gather the claims of a
// whole failed drain and make ONE call, not one call per failed batch. Deduper and
// Recorder are invoked from several goroutines concurrently; both interfaces
// already require that.
func ReleaseClaims(ctx context.Context, cfg *Config, pipelineName string, eventIDs []string) {
	if cfg.Dedup == nil || len(eventIDs) == 0 {
		return
	}

	// Dedupe first: an event fanned out over several failing keys arrives once per
	// key, and the budget is sized off the count.
	ids := make([]string, 0, len(eventIDs))
	seen := make(map[string]struct{}, len(eventIDs))
	for _, id := range eventIDs {
		if id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return
	}

	// Release on a context DETACHED from ctx. The single most likely reason a
	// merge fails is that ctx was just cancelled by SIGTERM — and releasing with
	// the cancelled ctx fails immediately, so the claim survives exactly the
	// shutdown it most needs to not survive.
	//
	// One budget spans the whole slice rather than one per ID, but it SCALES with
	// the slice: claimedIDs carries one entry per event, so a flush of a hot key
	// hands us thousands. Whatever the budget doesn't cover surfaces as
	// dedup_release_failed rather than going out silently.
	relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseBudget(len(ids)))
	defer cancel()

	workers := releaseConcurrency
	if workers > len(ids) {
		workers = len(ids)
	}
	if workers <= 1 {
		for _, id := range ids {
			releaseOne(relCtx, cfg, pipelineName, id)
		}
		return
	}

	work := make(chan string)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for id := range work {
				releaseOne(relCtx, cfg, pipelineName, id)
			}
		}()
	}
	for _, id := range ids {
		work <- id
	}
	close(work)
	wg.Wait()
}

// releaseOne hands back a single claim and records the outcome. A failure is
// loud on purpose: the claim is now outliving the delta it was taken for, and
// dedup_release_failed is the only signal an operator gets before the redelivery
// arrives to a dedup_skip.
func releaseOne(ctx context.Context, cfg *Config, pipelineName, id string) {
	if err := cfg.Dedup.Release(ctx, id); err != nil {
		cfg.Recorder.RecordError(pipelineName,
			fmt.Errorf("dedup Release %q after failed merge: %w", id, err))
		cfg.Recorder.RecordEvent(pipelineName + ":dedup_release_failed")
		return
	}
	cfg.Recorder.RecordEvent(pipelineName + ":dedup_release")
}

// MergeMany is the multi-key entry point. It claims the EventID via Dedup
// (once, regardless of how many keys), then for each key runs the store
// and cache MergeUpdate with the supplied delta. Used by hierarchical-
// rollup pipelines where one event contributes to many aggregation keys
// (e.g. "likes for this post", "likes for this post per country", "global
// likes" — three keys, one delta).
//
// Failure semantics:
//
//   - All-or-nothing per record (with respect to dead-lettering): if ANY
//     key's merge fails after retries, MergeMany returns an error. Earlier
//     keys that succeeded keep their writes — there is no rollback. The
//     idempotent-merge contract (with Dedup) ensures a redelivery folds
//     correctly: the dedup row prevents the keys-that-succeeded from
//     double-counting on retry, while the keys-that-failed get another
//     attempt.
//
// Pass a one-element keys slice for the single-key case; that's exactly
// what MergeOne does internally. The retry/backoff loop is per-key —
// each key gets its own MaxAttempts budget.
func MergeMany[V any](
	ctx context.Context,
	cfg *Config,
	pipelineName string,
	eventID string,
	eventTime time.Time,
	keys []string,
	delta V,
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
) error {
	if len(keys) == 0 {
		// No-op: caller produced no keys, nothing to merge. Don't claim
		// dedup either — there's nothing to dedupe against.
		return nil
	}

	// Whether *this* call won the dedup claim. Only a winner may release it:
	// releasing on a lost claim would drop the winner's row and let a third
	// delivery re-apply the event.
	claimed := false
	if cfg.Dedup != nil && eventID != "" {
		first, err := cfg.Dedup.MarkSeen(ctx, eventID)
		switch {
		case err != nil:
			// Fail open: a dedup-table outage must not silently drop events.
			cfg.Recorder.RecordError(pipelineName,
				fmt.Errorf("dedup MarkSeen %q: %w", eventID, err))
		case !first:
			cfg.Recorder.RecordEvent(pipelineName + ":dedup_skip")
			return nil
		default:
			claimed = true
		}
	}

	for _, entity := range keys {
		if err := mergeKeyWithRetry(ctx, cfg, pipelineName, eventID, eventTime, entity, delta, store, cache, window); err != nil {
			// The claim must not outlive the failed merge. If it does, the
			// source's redelivery hits dedup_skip and the event is dropped
			// permanently — silent count loss for Sum / HLL / TopK, with no
			// error surface and no metric. Releasing restores at-least-once.
			//
			// KeyByMany caveat: if an earlier key in this batch already
			// merged, releasing lets the redelivery re-apply it, so a
			// hierarchical rollup can over-count that key. That is the
			// correct trade — at-least-once permits re-application but never
			// permits loss, and dedup is a best-effort mitigation layered on
			// top, not a stronger guarantee.
			if claimed {
				ReleaseClaims(ctx, cfg, pipelineName, []string{eventID})
			}
			return err
		}
	}
	return nil
}

// mergeKeyWithRetry runs one (key, delta) merge with retries + backoff.
// Internal — called by MergeMany once per emitted key.
func mergeKeyWithRetry[V any](
	ctx context.Context,
	cfg *Config,
	pipelineName string,
	eventID string,
	eventTime time.Time,
	entity string,
	delta V,
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
) error {
	var lastErr error
	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			if err := backoffWait(ctx, cfg, attempt); err != nil {
				return err
			}
			cfg.Recorder.RecordEvent(pipelineName + ":retry")
		}
		err := mergeOneAttempt(ctx, cfg.Recorder, pipelineName, eventTime, entity, delta, store, cache, window)
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		lastErr = err
	}

	wrapped := fmt.Errorf("pipeline %q event %q key %q failed after %d attempts: %w",
		pipelineName, eventID, entity, cfg.MaxAttempts, lastErr)
	cfg.Recorder.RecordError(pipelineName, wrapped)
	cfg.Recorder.RecordEvent(pipelineName + ":dead_letter")
	return wrapped
}

// mergeOneAttempt runs one merge attempt for a single (entity, delta) pair
// without retries. Cache failures are NOT propagated as errors (the cache
// is a repopulatable accelerator); they're surfaced via RecordError and
// the store-side outcome is what the caller sees.
func mergeOneAttempt[V any](
	ctx context.Context,
	rec metrics.Recorder,
	name string,
	eventTime time.Time,
	entity string,
	delta V,
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
) error {
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
