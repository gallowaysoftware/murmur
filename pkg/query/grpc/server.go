// Package grpc serves Murmur's read-side query layer for an application data
// plane. The package name predates the Connect-RPC migration; the underlying
// implementation now uses Connect, which means a single mount-point speaks
// THREE protocols simultaneously:
//
//   - gRPC                  — for Go / JVM / Rust / Python clients using the
//     standard grpc-go / grpc-java / etc. clients
//   - gRPC-Web              — for browsers without a sidecar proxy
//   - Connect (HTTP + JSON) — for browsers and curl, no transport setup
//
// The wire contract is defined in proto/murmur/v1/query.proto. Anyone is
// welcome to point a different client at the same handler — Go's grpc-go,
// Connect's connect-go, browsers via @connectrpc/connect-web, and curl all
// hit the same routes.
//
// Phase 1 ships a generic Value (bytes) shape: the server takes a
// pipeline-typed Store, monoid, and windowing config, plus an Encoder[V] that
// converts the typed value into wire bytes. Clients are responsible for
// matching encoding (Int64LE for Sum/Count, raw bytes for sketches, etc.).
// Phase 2 will codegen pipeline-typed responses (CounterResponse,
// HLLResponse) from the pipeline definition and remove the caller-side
// decoding burden.
package grpc

import (
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/sync/singleflight"

	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/query"
	"github.com/gallowaysoftware/murmur/pkg/state"
	pb "github.com/gallowaysoftware/murmur/proto/gen/murmur/v1"
	"github.com/gallowaysoftware/murmur/proto/gen/murmur/v1/murmurv1connect"
)

// Encoder converts a typed aggregation value to wire bytes. Common encoders are
// available as Int64LE / BytesIdentity.
type Encoder[V any] func(V) []byte

// Int64LE encodes int64 values as 8-byte little-endian.
func Int64LE() Encoder[int64] {
	return func(v int64) []byte {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(v))
		return b
	}
}

// BytesIdentity encodes []byte values verbatim — for sketches whose marshaled
// form is already the desired wire format.
func BytesIdentity() Encoder[[]byte] {
	return func(v []byte) []byte { return v }
}

// Server bridges the generated Connect QueryServiceHandler to a pipeline's
// Store + monoid. Mount it on an http.ServeMux via Handler().
//
// Concurrent requests for the same (RPC, entity, bucket/window/range) are
// coalesced via a singleflight.Group keyed by the request shape, so a thousand
// simultaneous feed renders asking for the same hot counter become one
// underlying store call. The dedup window is the lifetime of the in-flight
// call — once the future resolves, the next request is fresh.
type Server[V any] struct {
	store    state.Store[V]
	mon      monoid.Monoid[V]
	window   *windowed.Config
	encode   Encoder[V]
	nowFn    func() time.Time
	recorder metrics.Recorder
	pipeline string

	// sf coalesces concurrent identical reads. Cheap when traffic is cold
	// (a no-op fastpath); huge wins on hot keys at feed-render time.
	sf singleflight.Group
}

// Config configures a query Server.
type Config[V any] struct {
	Store  state.Store[V]
	Monoid monoid.Monoid[V]
	Window *windowed.Config // optional; required only for GetWindow / GetRange
	Encode Encoder[V]

	// Now, if non-nil, overrides the time.Now used for sliding-window queries. Useful
	// for tests with deterministic clocks.
	Now func() time.Time

	// Recorder, if non-nil, receives per-RPC latency, error, and event
	// metrics. The streaming runtime records "store_merge" / "cache_merge"
	// latency under the pipeline name; the query side records
	// "<pipeline>:query_get", "<pipeline>:query_get_many",
	// "<pipeline>:query_get_window", "<pipeline>:query_get_range",
	// "<pipeline>:query_get_window_many", "<pipeline>:query_get_range_many".
	// Use a metrics.InMemory in development; a Prometheus / CloudWatch
	// adapter in production.
	Recorder metrics.Recorder

	// Pipeline names this query server's parent pipeline for metrics
	// labels. Defaults to "query" when unset; set explicitly when one
	// process serves multiple pipelines.
	Pipeline string
}

// NewServer constructs a query Server.
func NewServer[V any](cfg Config[V]) *Server[V] {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	rec := cfg.Recorder
	if rec == nil {
		rec = metrics.Noop{}
	}
	pipe := cfg.Pipeline
	if pipe == "" {
		pipe = "query"
	}
	return &Server[V]{
		store:    cfg.Store,
		mon:      cfg.Monoid,
		window:   cfg.Window,
		encode:   cfg.Encode,
		nowFn:    now,
		recorder: rec,
		pipeline: pipe,
	}
}

// coalescedResult holds the outcome of a single underlying store call so
// every singleflight participant can fan it back out.
type coalescedResult[V any] struct {
	value   V
	present bool
}

// coalesceGet runs fn at most once per concurrent group keyed by `key`. The
// first caller does the actual work; everyone else awaits the same result.
func coalesceGet[V any](sf *singleflight.Group, key string, fn func() (V, bool, error)) (V, bool, error) {
	out, err, _ := sf.Do(key, func() (any, error) {
		v, ok, err := fn()
		if err != nil {
			return nil, err
		}
		return coalescedResult[V]{value: v, present: ok}, nil
	})
	if err != nil {
		var zero V
		return zero, false, err
	}
	r := out.(coalescedResult[V])
	return r.value, r.present, nil
}

// Handler returns the Connect HTTP handler and its mount path. Wire it into a
// net/http server with `mux.Handle(path, h)`. The path follows the Connect
// convention `/murmur.v1.QueryService/`.
func (s *Server[V]) Handler() (string, http.Handler) {
	return murmurv1connect.NewQueryServiceHandler(s)
}

// --- QueryServiceHandler implementation ---
//
// Each method below implements the matching RPC defined in
// proto/murmur/v1/query.proto. The proto comments are the authoritative
// contract; the Go-side comments cover Go-specific behavior.

// Get implements murmur.v1.QueryService/Get. Returns the all-time aggregation
// value for entity (non-windowed pipelines). On a missing key, returns
// {present: false, data: nil}; clients should branch on `present` rather than
// on len(data).
//
// Concurrent identical Gets are coalesced via singleflight: under load on a
// hot entity, one underlying store.Get serves N waiters. Set
// `req.fresh_read = true` to bypass coalescing and force an authoritative
// read — used for read-your-writes ("user just liked this; show their
// like count").
func (s *Server[V]) Get(ctx context.Context, req *connect.Request[pb.GetRequest]) (*connect.Response[pb.GetResponse], error) {
	start := time.Now()
	defer func() {
		s.recorder.RecordLatency(s.pipeline, "query_get", time.Since(start))
	}()
	s.recorder.RecordEvent(s.pipeline + ":query_get")

	entity := req.Msg.GetEntity()
	doGet := func() (V, bool, error) { return query.Get(ctx, s.store, entity) }

	var (
		v   V
		ok  bool
		err error
	)
	if req.Msg.GetFreshRead() {
		v, ok, err = doGet()
	} else {
		v, ok, err = coalesceGet(&s.sf, "Get|"+entity, doGet)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	val := &pb.Value{Present: false}
	if ok {
		val = &pb.Value{Present: true, Data: s.encode(v)}
	}
	return connect.NewResponse(&pb.GetResponse{Value: val}), nil
}

// GetMany implements murmur.v1.QueryService/GetMany. Same shape as Get but
// for many entities in one round-trip; the response preserves request order
// so clients can zip without an extra index map.
func (s *Server[V]) GetMany(ctx context.Context, req *connect.Request[pb.GetManyRequest]) (*connect.Response[pb.GetManyResponse], error) {
	start := time.Now()
	defer func() {
		s.recorder.RecordLatency(s.pipeline, "query_get_many", time.Since(start))
	}()
	s.recorder.RecordEvent(s.pipeline + ":query_get_many")

	keys := make([]state.Key, len(req.Msg.GetEntities()))
	for i, e := range req.Msg.GetEntities() {
		keys[i] = state.Key{Entity: e}
	}
	vals, oks, err := s.store.GetMany(ctx, keys)
	if err != nil {
		s.recorder.RecordError(s.pipeline, err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	out := &pb.GetManyResponse{Values: make([]*pb.Value, len(req.Msg.GetEntities()))}
	for i := range req.Msg.GetEntities() {
		if !oks[i] {
			out.Values[i] = &pb.Value{Present: false}
			continue
		}
		out.Values[i] = &pb.Value{Present: true, Data: s.encode(vals[i])}
	}
	return connect.NewResponse(out), nil
}

// GetWindow implements murmur.v1.QueryService/GetWindow. Merges the N
// most-recent buckets covering `duration_seconds` ending at the server's now
// via the configured monoid. Returns CodeFailedPrecondition for non-windowed
// pipelines so clients can route to Get instead.
//
// Concurrent identical GetWindows are coalesced via singleflight; the
// coalesce key includes the bucketed `now` so two requests one second apart
// can share work, while requests across a bucket boundary do not.
func (s *Server[V]) GetWindow(ctx context.Context, req *connect.Request[pb.GetWindowRequest]) (*connect.Response[pb.GetWindowResponse], error) {
	start := time.Now()
	defer func() {
		s.recorder.RecordLatency(s.pipeline, "query_get_window", time.Since(start))
	}()
	s.recorder.RecordEvent(s.pipeline + ":query_get_window")

	if s.window == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("pipeline is not windowed; use Get instead"))
	}
	now := s.nowFn()
	d := time.Duration(req.Msg.GetDurationSeconds()) * time.Second
	entity := req.Msg.GetEntity()
	doFetch := func() (V, bool, error) {
		out, err := query.GetWindow(ctx, s.store, s.mon, *s.window, entity, d, now)
		return out, true, err
	}

	var (
		v   V
		err error
	)
	if req.Msg.GetFreshRead() {
		v, _, err = doFetch()
	} else {
		// Coalesce key: bucketed "now" means consecutive requests within the
		// same bucket reuse a single store call; first request in a new bucket
		// does the work. This bounds staleness to at most one bucket.
		bucket := s.window.BucketID(now)
		key := "GetWindow|" + entity + "|" + strconv.FormatInt(int64(d/time.Second), 10) + "|" + strconv.FormatInt(bucket, 10)
		v, _, err = coalesceGet(&s.sf, key, doFetch)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&pb.GetWindowResponse{
		Value: &pb.Value{Present: true, Data: s.encode(v)},
	}), nil
}

// GetRange implements murmur.v1.QueryService/GetRange. Merges every bucket
// whose ID falls in [start_unix, end_unix] inclusive. Same not-windowed
// failure mode as GetWindow.
//
// Coalesced via singleflight on (entity, start_unix, end_unix) — the range
// is fully specified by the caller, so identical concurrent ranges share
// work directly.
func (s *Server[V]) GetRange(ctx context.Context, req *connect.Request[pb.GetRangeRequest]) (*connect.Response[pb.GetRangeResponse], error) {
	t0 := time.Now()
	defer func() {
		s.recorder.RecordLatency(s.pipeline, "query_get_range", time.Since(t0))
	}()
	s.recorder.RecordEvent(s.pipeline + ":query_get_range")

	if s.window == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("pipeline is not windowed; use Get instead"))
	}
	startUnix := req.Msg.GetStartUnix()
	endUnix := req.Msg.GetEndUnix()
	entity := req.Msg.GetEntity()
	doFetch := func() (V, bool, error) {
		start := time.Unix(startUnix, 0).UTC()
		end := time.Unix(endUnix, 0).UTC()
		out, err := query.GetRange(ctx, s.store, s.mon, *s.window, entity, start, end)
		return out, true, err
	}

	var (
		v   V
		err error
	)
	if req.Msg.GetFreshRead() {
		v, _, err = doFetch()
	} else {
		key := "GetRange|" + entity + "|" + strconv.FormatInt(startUnix, 10) + "|" + strconv.FormatInt(endUnix, 10)
		v, _, err = coalesceGet(&s.sf, key, doFetch)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&pb.GetRangeResponse{
		Value: &pb.Value{Present: true, Data: s.encode(v)},
	}), nil
}

// GetWindowMany implements murmur.v1.QueryService/GetWindowMany. Batches
// windowed merges across many entities into a single underlying store
// fetch. Same windowed-pipeline precondition as GetWindow.
//
// fresh_read bypasses singleflight. The default path coalesces concurrent
// identical requests at the (sorted-entities, duration, bucket) granularity.
func (s *Server[V]) GetWindowMany(ctx context.Context, req *connect.Request[pb.GetWindowManyRequest]) (*connect.Response[pb.GetWindowManyResponse], error) {
	t0 := time.Now()
	defer func() {
		s.recorder.RecordLatency(s.pipeline, "query_get_window_many", time.Since(t0))
	}()
	s.recorder.RecordEvent(s.pipeline + ":query_get_window_many")

	if s.window == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("pipeline is not windowed; use GetMany instead"))
	}
	entities := req.Msg.GetEntities()
	now := s.nowFn()
	d := time.Duration(req.Msg.GetDurationSeconds()) * time.Second

	doFetch := func() ([]V, bool, error) {
		vs, err := query.GetWindowMany(ctx, s.store, s.mon, *s.window, entities, d, now)
		return vs, true, err
	}

	var (
		vs  []V
		err error
	)
	if req.Msg.GetFreshRead() {
		vs, _, err = doFetch()
	} else {
		// Coalesce key: hash the (sorted) entity list + duration + bucket.
		// Sorting normalizes equivalent permutations onto the same coalesce
		// key. For typical query shapes (a fixed candidate set per query),
		// concurrent identical reads collapse to one store fetch.
		bucket := s.window.BucketID(now)
		key := "GetWindowMany|" + sortedJoin(entities) + "|" + strconv.FormatInt(int64(d/time.Second), 10) + "|" + strconv.FormatInt(bucket, 10)
		vs, _, err = coalesceGetSlice(&s.sf, key, doFetch)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	out := &pb.GetWindowManyResponse{Values: make([]*pb.Value, len(vs))}
	for i, v := range vs {
		out.Values[i] = &pb.Value{Present: true, Data: s.encode(v)}
	}
	return connect.NewResponse(out), nil
}

// GetRangeMany implements murmur.v1.QueryService/GetRangeMany. Same shape
// as GetWindowMany over an absolute [start_unix, end_unix] range.
func (s *Server[V]) GetRangeMany(ctx context.Context, req *connect.Request[pb.GetRangeManyRequest]) (*connect.Response[pb.GetRangeManyResponse], error) {
	t0 := time.Now()
	defer func() {
		s.recorder.RecordLatency(s.pipeline, "query_get_range_many", time.Since(t0))
	}()
	s.recorder.RecordEvent(s.pipeline + ":query_get_range_many")

	if s.window == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("pipeline is not windowed; use GetMany instead"))
	}
	entities := req.Msg.GetEntities()
	startUnix := req.Msg.GetStartUnix()
	endUnix := req.Msg.GetEndUnix()

	doFetch := func() ([]V, bool, error) {
		start := time.Unix(startUnix, 0).UTC()
		end := time.Unix(endUnix, 0).UTC()
		vs, err := query.GetRangeMany(ctx, s.store, s.mon, *s.window, entities, start, end)
		return vs, true, err
	}

	var (
		vs  []V
		err error
	)
	if req.Msg.GetFreshRead() {
		vs, _, err = doFetch()
	} else {
		key := "GetRangeMany|" + sortedJoin(entities) + "|" + strconv.FormatInt(startUnix, 10) + "|" + strconv.FormatInt(endUnix, 10)
		vs, _, err = coalesceGetSlice(&s.sf, key, doFetch)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	out := &pb.GetRangeManyResponse{Values: make([]*pb.Value, len(vs))}
	for i, v := range vs {
		out.Values[i] = &pb.Value{Present: true, Data: s.encode(v)}
	}
	return connect.NewResponse(out), nil
}

// coalesceGetSlice is the slice-result analog of coalesceGet. Used by the
// "Many" endpoints whose return shape is []V instead of (V, bool).
func coalesceGetSlice[V any](sf *singleflight.Group, key string, fn func() ([]V, bool, error)) ([]V, bool, error) {
	out, err, _ := sf.Do(key, func() (any, error) {
		v, ok, err := fn()
		if err != nil {
			return nil, err
		}
		return coalescedSliceResult[V]{values: v, present: ok}, nil
	})
	if err != nil {
		return nil, false, err
	}
	r := out.(coalescedSliceResult[V])
	return r.values, r.present, nil
}

type coalescedSliceResult[V any] struct {
	values  []V
	present bool
}

// sortedJoin returns the entities sorted and joined by '|'. Used by the
// singleflight coalesce keys so two requests with the same set of entities
// in different orders collapse to the same key.
func sortedJoin(entities []string) string {
	if len(entities) == 0 {
		return ""
	}
	cp := make([]string, len(entities))
	copy(cp, entities)
	sort.Strings(cp)
	return strings.Join(cp, "|")
}
