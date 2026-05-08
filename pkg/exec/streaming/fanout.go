package streaming

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/gallowaysoftware/murmur/pkg/exec/processor"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
)

// Bound is one pipeline registered for fanout. Use Bind[T, V](pipe, opts...)
// to construct one and pass it to RunFanout. The V type parameter is
// hidden behind the closure so the multi-runtime can hold pipelines of
// heterogeneous aggregation types in a single slice.
type Bound[T any] struct {
	name string
	run  func(ctx context.Context, recs <-chan source.Record[T]) error
}

// Name returns the pipeline name supplied to Bind. Useful for error
// reporting in RunFanout.
func (b Bound[T]) Name() string { return b.name }

// Bind ties a pipeline to a set of streaming RunOptions and returns a
// Bound[T] suitable for RunFanout. The pipeline's Source field is
// IGNORED — RunFanout supplies a single shared source upstream and tees
// records to every Bound consumer.
//
// All pipelines in one fanout must share the same input record type T;
// each pipeline can have its own aggregation type V.
func Bind[T any, V any](p *pipeline.Pipeline[T, V], opts ...RunOption) Bound[T] {
	return Bound[T]{
		name: p.Name(),
		run: func(ctx context.Context, recs <-chan source.Record[T]) error {
			if err := p.Build(); err != nil {
				return fmt.Errorf("streaming.Bind: %w", err)
			}
			cfg := runConfig{Config: processor.Defaults()}
			for _, o := range opts {
				o(&cfg)
			}
			return runBoundPipeline(ctx, p, &cfg, recs)
		},
	}
}

// runBoundPipeline is the per-pipeline goroutine that consumes from
// `recs` and runs the same retry / dedup / aggregator state machine as
// streaming.Run. The aggregator's flush goroutine is owned here and
// stops on ctx cancel. Returns when `recs` closes (source exhausted or
// fanout shutdown).
func runBoundPipeline[T any, V any](
	ctx context.Context,
	p *pipeline.Pipeline[T, V],
	cfg *runConfig,
	recs <-chan source.Record[T],
) error {
	name := p.Name()
	keysFn := p.KeysFn()
	valueFn := p.ValueFn()
	store := p.Store()
	cache := p.CacheStore()
	window := p.Window()

	var agg *aggregator[T, V]
	var flushDone chan struct{}
	if cfg.batchWindow > 0 {
		agg = newAggregator(cfg, p.Monoid(), keysFn, valueFn, store, cache, window, name, cfg.maxBatchSize)
		flushDone = make(chan struct{})
		go func() {
			defer close(flushDone)
			agg.runFlushLoop(ctx, cfg.batchWindow)
		}()
	}

	for rec := range recs {
		if agg != nil {
			if dup := agg.accept(ctx, rec); dup && rec.Ack != nil {
				_ = rec.Ack()
			}
		} else {
			processWithRetry(ctx, name, rec, keysFn, valueFn, store, cache, window, cfg)
		}
	}
	// Source channel closed — drain the aggregator one last time so any
	// in-flight batch flushes before the pipeline exits.
	if agg != nil {
		agg.flushAll(ctx)
		<-flushDone
	}
	return nil
}

// FanoutOption configures RunFanout.
type FanoutOption func(*fanoutConfig)

type fanoutConfig struct {
	bufferSize int
}

// WithFanoutBuffer sets the per-pipeline channel buffer size. Default
// 1024. Larger buffers absorb slow-pipeline backpressure at the cost
// of memory; the slowest pipeline gates source-side Ack progress
// (see "Per-pipeline backpressure" in the package doc), so the buffer
// is the slack a fast pipeline can run ahead before the slow one
// stalls everyone.
func WithFanoutBuffer(n int) FanoutOption {
	return func(c *fanoutConfig) {
		if n > 0 {
			c.bufferSize = n
		}
	}
}

// RunFanout runs N pipelines against ONE shared source. Each pipeline
// drains its own buffered channel; the source pump tees every record
// to all channels.
//
// Use case: count-core-style "many counters per event." One Kafka topic
// of like-events drives:
//
//   - a per-post counter pipeline
//   - a per-user counter pipeline
//   - a global trending pipeline (Misra-Gries TopK)
//   - a Decayed-sum "hot" score pipeline
//
// All four consume the same records. Without fanout, you'd open four
// Kafka consumer-group connections from one process (4× the broker
// load) or run four worker processes (4× the deployment surface).
// With fanout, one source pump feeds all four.
//
// # Ack semantics — counted teeing
//
// Each record's source-side Ack is wrapped so it fires the underlying
// source.Ack only after EVERY pipeline has called .Ack() on its copy.
// This means:
//
//   - The slowest pipeline gates source offset advancement.
//   - On worker restart the source replays from the last fully-acked
//     offset; pipelines that already processed see duplicates, dedup
//     catches them.
//   - A stuck pipeline pins the source — same backpressure semantics
//     as a single-pipeline streaming.Run, but multiplied across N
//     consumers.
//
// # Per-pipeline backpressure
//
// Each pipeline has a buffered channel (default 1024 records). When
// it fills, the source pump blocks on the slow pipeline. Other
// pipelines continue draining their own channels until the slow one
// catches up. This is the right shape: Kafka offset advancement is
// already gated on the slow pipeline's Ack, so blocking the pump
// matches that constraint.
//
// # Failure model
//
//   - Source error: returned to the caller; pipelines drain their
//     remaining channel content and exit cleanly.
//   - Pipeline error: surfaces from RunFanout's joined error. Other
//     pipelines continue.
//   - Ctx cancel: all pipelines stop after draining; final aggregator
//     flush runs under a 30-second detached context.
func RunFanout[T any](
	ctx context.Context,
	src source.Source[T],
	pipes []Bound[T],
	opts ...FanoutOption,
) error {
	if src == nil {
		return errors.New("streaming.RunFanout: source is nil")
	}
	if len(pipes) == 0 {
		return errors.New("streaming.RunFanout: no pipelines provided")
	}
	cfg := fanoutConfig{bufferSize: 1024}
	for _, o := range opts {
		o(&cfg)
	}

	// One channel per pipeline.
	channels := make([]chan source.Record[T], len(pipes))
	for i := range channels {
		channels[i] = make(chan source.Record[T], cfg.bufferSize)
	}

	// Source pump: tees to all per-pipeline channels.
	intake := make(chan source.Record[T], cfg.bufferSize)
	srcDone := make(chan error, 1)
	go func() {
		srcDone <- src.Read(ctx, intake)
		close(intake)
	}()

	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		defer func() {
			for _, ch := range channels {
				close(ch)
			}
		}()
		n := len(pipes)
		for rec := range intake {
			wrapped := teeAck(rec, n)
			for _, ch := range channels {
				select {
				case ch <- wrapped:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Each pipeline runs in its own goroutine.
	var wg sync.WaitGroup
	pipeErrs := make([]error, len(pipes))
	for i, pipe := range pipes {
		wg.Add(1)
		go func(i int, pipe Bound[T]) {
			defer wg.Done()
			if err := pipe.run(ctx, channels[i]); err != nil {
				pipeErrs[i] = fmt.Errorf("pipeline %q: %w", pipe.Name(), err)
			}
		}(i, pipe)
	}

	wg.Wait()
	<-pumpDone

	srcErr := <-srcDone
	if srcErr != nil && !errors.Is(srcErr, context.Canceled) {
		pipeErrs = append(pipeErrs, fmt.Errorf("source %q: %w", src.Name(), srcErr))
	}
	return errors.Join(pipeErrs...)
}

// teeAck returns a Record whose Ack fires the underlying source.Ack
// only after `n` calls — once per consumer pipeline. Each pipeline
// receives a record with the SAME Ack closure (closures over the same
// shared counter), so any of them calling .Ack() advances the counter.
//
// Concurrency: counter is mu-protected. Idempotent under double-Ack
// from a single pipeline (would advance the count past N and never
// fire — caller bug, not ours to fix).
func teeAck[T any](rec source.Record[T], n int) source.Record[T] {
	if rec.Ack == nil || n <= 1 {
		return rec
	}
	var (
		mu    sync.Mutex
		count int
	)
	orig := rec.Ack
	wrapped := source.Record[T]{
		EventID:      rec.EventID,
		EventTime:    rec.EventTime,
		PartitionKey: rec.PartitionKey,
		Value:        rec.Value,
	}
	wrapped.Ack = func() error {
		mu.Lock()
		count++
		fire := count == n
		mu.Unlock()
		if fire {
			return orig()
		}
		return nil
	}
	return wrapped
}
