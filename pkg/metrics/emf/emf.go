// Package emf implements metrics.Recorder on top of the CloudWatch Embedded
// Metric Format.
//
// Why EMF rather than PutMetricData: EMF metrics are extracted from structured
// JSON written to stdout, so a worker needs no CloudWatch API permissions, no
// SDK client, and no network call on the hot path. Both deployment shapes
// Murmur targets already ship stdout to CloudWatch Logs — Lambda natively, ECS
// via the awslogs driver — so wiring this up is a one-line change with no IAM
// edit.
//
//	rec := emf.New(emf.Config{Namespace: "Murmur"})
//	defer rec.Close()
//	handler, _ := kinesis.NewHandler(pipe, dec, kinesis.WithMetrics(rec))
//
// # Aggregation
//
// The Recorder contract asks for nanosecond-cost on the hot path, and a
// pipeline running at even modest throughput would be ruinous to log
// per-event: one EMF document per record is one CloudWatch Logs ingestion
// charge per record, and CloudWatch bills ingestion by the byte.
//
// So calls only touch an in-memory aggregate under a sharded lock, and a
// background goroutine emits one document per flush interval carrying counters
// as sums and latencies as EMF StatisticSets (Max/Min/Sum/SampleCount, which
// CloudWatch expands into averages and percentile-capable statistics). At the
// default 60s interval a pipeline emits 1440 documents a day regardless of
// whether it processed a hundred records or a hundred million.
//
// # Sub-event names
//
// Murmur runtimes encode sub-events by suffixing the pipeline name, e.g.
// `RecordEvent("orders:dedup_skip")`. Emitting those verbatim would create a
// separate Pipeline dimension value per sub-event and fragment the dashboards.
// This recorder splits on the last colon instead, so `orders:dedup_skip`
// becomes the metric `DedupSkip` on Pipeline=orders — which is what makes
// dedup_skip, dedup_release, and dedup_release_failed visible at all.
package emf

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/metrics"
)

// DefaultNamespace is the CloudWatch namespace used when Config.Namespace is
// empty.
const DefaultNamespace = "Murmur"

// DefaultFlushInterval is how often aggregates are emitted when
// Config.FlushInterval is zero.
const DefaultFlushInterval = 60 * time.Second

// Config configures a Recorder.
type Config struct {
	// Namespace is the CloudWatch namespace. Defaults to DefaultNamespace.
	Namespace string

	// FlushInterval is how often aggregated metrics are written. Defaults to
	// DefaultFlushInterval. Shorter intervals cost proportionally more in
	// CloudWatch Logs ingestion for no extra resolution below one minute,
	// which is CloudWatch's own floor for standard metrics.
	FlushInterval time.Duration

	// Out is where EMF documents are written. Defaults to os.Stdout, which is
	// what both Lambda and the ECS awslogs driver forward to CloudWatch Logs.
	Out io.Writer

	// Dimensions are extra dimensions attached to every metric, e.g.
	// {"Env": "soak"}. Keep this small: every distinct combination of
	// dimension values is a separate CloudWatch custom metric, and they are
	// billed individually.
	Dimensions map[string]string
}

// Recorder is a metrics.Recorder that emits CloudWatch EMF documents.
//
// Safe for concurrent use. Call Close to flush pending aggregates and stop the
// background goroutine; without it, up to one flush interval of metrics is
// lost when the process exits.
type Recorder struct {
	cfg    Config
	mu     sync.Mutex
	agg    map[string]*pipelineAgg
	stop   chan struct{}
	done   chan struct{}
	closed sync.Once
	now    func() time.Time // swappable for tests
}

type pipelineAgg struct {
	counters map[string]int64
	lat      map[string]*statSet
}

type statSet struct {
	max, min, sum float64
	n             int64
}

func (s *statSet) add(v float64) {
	if s.n == 0 || v > s.max {
		s.max = v
	}
	if s.n == 0 || v < s.min {
		s.min = v
	}
	s.sum += v
	s.n++
}

// New constructs a Recorder and starts its flush goroutine.
func New(cfg Config) *Recorder {
	if cfg.Namespace == "" {
		cfg.Namespace = DefaultNamespace
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = DefaultFlushInterval
	}
	if cfg.Out == nil {
		cfg.Out = os.Stdout
	}
	r := &Recorder{
		cfg:  cfg,
		agg:  make(map[string]*pipelineAgg),
		stop: make(chan struct{}),
		done: make(chan struct{}),
		now:  time.Now,
	}
	go r.loop()
	return r
}

func (r *Recorder) loop() {
	defer close(r.done)
	t := time.NewTicker(r.cfg.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			r.Flush()
		case <-r.stop:
			r.Flush()
			return
		}
	}
}

// Close flushes any pending aggregates and stops the background goroutine. It
// is safe to call more than once.
func (r *Recorder) Close() error {
	r.closed.Do(func() {
		close(r.stop)
		<-r.done
	})
	return nil
}

// splitSub separates a "pipeline:sub_event" name into its parts. Splitting on
// the LAST colon keeps pipeline names that legitimately contain one intact.
func splitSub(name string) (pipeline, sub string) {
	if i := strings.LastIndex(name, ":"); i > 0 && i < len(name)-1 {
		return name[:i], name[i+1:]
	}
	return name, ""
}

func (r *Recorder) forPipeline(p string) *pipelineAgg {
	a, ok := r.agg[p]
	if !ok {
		a = &pipelineAgg{counters: map[string]int64{}, lat: map[string]*statSet{}}
		r.agg[p] = a
	}
	return a
}

func (r *Recorder) incr(pipeline, metric string) {
	r.mu.Lock()
	r.forPipeline(pipeline).counters[metric]++
	r.mu.Unlock()
}

// RecordEvent implements metrics.Recorder.
func (r *Recorder) RecordEvent(pipeline string) {
	p, sub := splitSub(pipeline)
	if sub == "" {
		r.incr(p, "EventsProcessed")
		return
	}
	r.incr(p, metricName(sub))
}

// RecordError implements metrics.Recorder.
func (r *Recorder) RecordError(pipeline string, _ error) {
	p, sub := splitSub(pipeline)
	if sub == "" {
		r.incr(p, "Errors")
		return
	}
	// A sub-scoped error still counts toward the pipeline's error total, or a
	// dashboard alarming on Errors would miss it entirely.
	r.incr(p, "Errors")
	r.incr(p, metricName(sub)+"Errors")
}

// RecordLatency implements metrics.Recorder.
func (r *Recorder) RecordLatency(pipeline, op string, d time.Duration) {
	p, _ := splitSub(pipeline)
	ms := float64(d) / float64(time.Millisecond)
	r.mu.Lock()
	a := r.forPipeline(p)
	name := metricName(op) + "Latency"
	s, ok := a.lat[name]
	if !ok {
		s = &statSet{}
		a.lat[name] = s
	}
	s.add(ms)
	r.mu.Unlock()
}

// RecordBatch implements metrics.Recorder.
func (r *Recorder) RecordBatch(pipeline, mode string, n int, d time.Duration) {
	p, _ := splitSub(pipeline)
	ms := float64(d) / float64(time.Millisecond)
	r.mu.Lock()
	a := r.forPipeline(p)
	a.counters["BatchesProcessed"]++
	a.counters["BatchRecords"] += int64(n)
	// Mode rides in the metric name rather than as a dimension: a dimension
	// would multiply the billed custom-metric count for every pipeline, and a
	// worker only ever emits one mode anyway.
	name := "Batch" + metricName(mode) + "Latency"
	s, ok := a.lat[name]
	if !ok {
		s = &statSet{}
		a.lat[name] = s
	}
	s.add(ms)
	r.mu.Unlock()
}

// metricName converts snake_case / kebab-case into the CamelCase CloudWatch
// convention: "dedup_skip" -> "DedupSkip", "store_merge" -> "StoreMerge".
func metricName(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	up := true
	for _, c := range s {
		if c == '_' || c == '-' || c == '.' || c == ' ' {
			up = true
			continue
		}
		if up {
			b.WriteString(strings.ToUpper(string(c)))
			up = false
			continue
		}
		b.WriteRune(c)
	}
	return b.String()
}

// Flush writes one EMF document per pipeline with everything aggregated since
// the last flush, and resets the aggregates. Called automatically on the flush
// interval and by Close; exported so callers can force a flush (e.g. a Lambda
// handler that wants metrics out before the freeze).
func (r *Recorder) Flush() {
	r.mu.Lock()
	agg := r.agg
	r.agg = make(map[string]*pipelineAgg, len(agg))
	r.mu.Unlock()

	if len(agg) == 0 {
		return
	}
	ts := r.now().UnixMilli()
	for pipeline, a := range agg {
		doc := r.document(pipeline, a, ts)
		if doc == nil {
			continue
		}
		b, err := json.Marshal(doc)
		if err != nil {
			// Nothing useful to do with a marshal failure here — writing an
			// error to the same stream would corrupt the metric stream. Drop.
			continue
		}
		b = append(b, '\n')
		_, _ = r.cfg.Out.Write(b)
	}
}

func (r *Recorder) document(pipeline string, a *pipelineAgg, ts int64) map[string]any {
	defs := make([]map[string]string, 0, len(a.counters)+len(a.lat))
	doc := map[string]any{"Pipeline": pipeline}

	for name, v := range a.counters {
		defs = append(defs, map[string]string{"Name": name, "Unit": "Count"})
		doc[name] = v
	}
	for name, s := range a.lat {
		if s.n == 0 {
			continue
		}
		defs = append(defs, map[string]string{"Name": name, "Unit": "Milliseconds"})
		doc[name] = map[string]any{
			"Max": s.max, "Min": s.min, "Sum": s.sum, "Count": s.n,
		}
	}
	if len(defs) == 0 {
		return nil
	}

	dimKeys := []string{"Pipeline"}
	for k, v := range r.cfg.Dimensions {
		doc[k] = v
		dimKeys = append(dimKeys, k)
	}

	doc["_aws"] = map[string]any{
		"Timestamp": ts,
		"CloudWatchMetrics": []map[string]any{{
			"Namespace":  r.cfg.Namespace,
			"Dimensions": [][]string{dimKeys},
			"Metrics":    defs,
		}},
	}
	return doc
}

// Compile-time check.
var _ metrics.Recorder = (*Recorder)(nil)
