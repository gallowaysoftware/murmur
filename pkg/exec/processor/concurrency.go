package processor

import (
	"context"
	"sync"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// FlushItem is one pending (key, delta) merge ready to apply to the store.
// Aggregators / batch flushers build a slice of these — typically AFTER
// per-key delta coalescing has already collapsed many input events down to
// one Combine'd delta per Key — and then hand the slice to FlushBatch for
// parallel execution.
//
// EventTime is the timestamp used to bucket the merge under a windowed
// configuration; for non-windowed pipelines it is ignored but the field
// is still required so callers don't accidentally drop windowing context.
type FlushItem[V any] struct {
	Key       state.Key
	Delta     V
	EventTime time.Time
}

// FlushBatch applies every FlushItem in `items` to (store, cache) using the
// processor's retry/backoff/metrics surface, spreading the work across at
// most `concurrency` goroutines.
//
// Semantics:
//
//   - concurrency <= 1: items are applied sequentially in the order given,
//     same as a plain `for _, it := range items { mergeKeyWithRetry(...) }`.
//     This is the default path and preserves the pre-concurrency behavior
//     of every caller.
//
//   - concurrency > 1: a worker pool of size `min(concurrency, len(items))`
//     drains a channel of FlushItems and runs MergeUpdate in parallel.
//     **Each FlushItem must address a distinct (entity, bucket) Key** —
//     callers are expected to coalesce deltas per-key BEFORE calling
//     FlushBatch (additive monoids have already-collapsed all contributions
//     to one delta per key, so parallelizing the per-key flushes is safe
//     regardless of arrival order). FlushBatch does not enforce
//     distinctness — passing duplicate keys with concurrency>1 races the
//     store's MergeUpdate semantics and is undefined.
//
//   - At-least-once preservation: if any worker returns a non-nil error
//     from mergeKeyWithRetry, FlushBatch records the first such error and
//     returns it after all in-flight items finish. Subsequent items that
//     haven't been dispatched yet are skipped (cancellation propagates
//     through the worker context). The caller must treat a non-nil return
//     as "this batch failed" — earlier-completed items have already
//     written to the store, but the batch as a whole has not been Ack'd
//     so the source will redeliver everything. Idempotent-merge + dedup
//     keeps the redelivered batch correct.
//
//   - Ordering: because the worker pool is unordered, callers must NOT
//     rely on items being applied in slice order. This is the explicit
//     tradeoff that unlocks parallelism for additive monoids; for non-
//     additive monoids (First/Last/Min/Max), per-key ordering still
//     matters at the routing layer (see streaming.WithConcurrency), but
//     within a single coalesced flush the deltas are already collapsed
//     so there is at most one delta per key in `items`.
//
// FlushBatch always invokes mergeKeyWithRetry for each item — the same
// retry budget, backoff schedule, dedup-skip handling, and metrics that
// MergeMany uses. Pass the same *Config you'd pass to MergeMany.
func FlushBatch[V any](
	ctx context.Context,
	cfg *Config,
	pipelineName string,
	items []FlushItem[V],
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
	concurrency int,
) error {
	if len(items) == 0 {
		return nil
	}

	// Default & clamp: never spawn more workers than items, never less
	// than 1. concurrency=1 takes the sequential fast path so we don't
	// pay for channel + goroutine overhead in the common single-worker
	// case.
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > len(items) {
		concurrency = len(items)
	}

	if concurrency == 1 {
		for _, it := range items {
			if err := ctx.Err(); err != nil {
				return err
			}
			// EventID="" — coalesced flushes have already deduped at
			// accept-time; mergeKeyWithRetry skips the dedup check when
			// eventID is empty (same contract as aggregator.flushOne).
			if err := mergeKeyWithRetry(ctx, cfg, pipelineName, "", it.EventTime,
				it.Key.Entity, it.Delta, store, cache, window); err != nil {
				return err
			}
		}
		return nil
	}

	// Concurrent path. Workers drain `work` until it closes; the first
	// error short-circuits by canceling workerCtx so still-running
	// MergeUpdates can bail out of their retry-backoff sleep. Items
	// that haven't been dispatched yet are skipped — the producer
	// notices ctx.Err() and stops.
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	work := make(chan FlushItem[V])

	var (
		firstErrOnce sync.Once
		firstErr     error
		wg           sync.WaitGroup
	)
	recordErr := func(err error) {
		firstErrOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range work {
				if err := workerCtx.Err(); err != nil {
					// Drain the rest of `work` to unblock the producer
					// without doing further store calls.
					continue
				}
				err := mergeKeyWithRetry(workerCtx, cfg, pipelineName, "", it.EventTime,
					it.Key.Entity, it.Delta, store, cache, window)
				if err == nil {
					continue
				}
				// Context cancellation is treated the same as any other
				// error for the purpose of returning to the caller —
				// the batch did not fully apply.
				recordErr(err)
			}
		}()
	}

	// Producer: feed items into `work`, bailing out the moment a
	// worker signals an error or the caller cancels.
producer:
	for _, it := range items {
		select {
		case work <- it:
		case <-workerCtx.Done():
			break producer
		}
	}
	close(work)
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	// If the caller's context was canceled but no worker hit an error,
	// surface that cancellation rather than reporting success.
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}
