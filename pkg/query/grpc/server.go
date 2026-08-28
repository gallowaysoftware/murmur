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
	"fmt"
	"math"
	"net/http"
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
	store           state.Store[V]
	mon             monoid.Monoid[V]
	window          *windowed.Config
	encode          Encoder[V]
	nowFn           func() time.Time
	recorder        metrics.Recorder
	pipeline        string
	coalesceTimeout time.Duration

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
	// "<pipeline>:query_get_window_many", "<pipeline>:query_get_range_many",
	// "<pipeline>:query_get_trailing", "<pipeline>:query_get_trailing_many".
	// Use a metrics.InMemory in development; a Prometheus / CloudWatch
	// adapter in production.
	Recorder metrics.Recorder

	// Pipeline names this query server's parent pipeline for metrics
	// labels. Defaults to "query" when unset; set explicitly when one
	// process serves multiple pipelines.
	Pipeline string

	// CoalesceTimeout bounds the store call a singleflight group runs on behalf
	// of its waiters. That call is deliberately detached from the context of
	// whichever caller happened to lead the group — otherwise one client hanging
	// up cancels the read out from under every peer coalesced onto it — so it
	// needs a deadline of its own or a wedged store pins the group forever.
	// Defaults to defaultCoalesceTimeout.
	CoalesceTimeout time.Duration
}

// defaultCoalesceTimeout bounds detached singleflight work when Config leaves
// CoalesceTimeout unset. Long enough for a multi-chunk BatchGetItem with retries,
// short enough that a wedged store frees the group inside one health-check interval.
const defaultCoalesceTimeout = 10 * time.Second

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
	coalesceTimeout := cfg.CoalesceTimeout
	if coalesceTimeout <= 0 {
		coalesceTimeout = defaultCoalesceTimeout
	}
	return &Server[V]{
		store:           cfg.Store,
		mon:             cfg.Monoid,
		window:          cfg.Window,
		encode:          cfg.Encode,
		nowFn:           now,
		recorder:        rec,
		pipeline:        pipe,
		coalesceTimeout: coalesceTimeout,
	}
}

// coalescedResult holds the outcome of a single underlying store call so
// every singleflight participant can fan it back out.
type coalescedResult[V any] struct {
	value   V
	present bool
}

// coalesce runs fn at most once per concurrent group keyed by `key`; every other
// caller in the group awaits the same result.
//
// Two things the plain singleflight.Do version got wrong. The shared work ran on
// the context of whichever caller happened to arrive first, so a single client
// hanging up cancelled the store read out from under every peer and failed all of
// them with CodeInternal — a failure that only appears under exactly the concurrent
// load coalescing exists to serve. And detaching that context outright would drop
// the client deadline with it, letting a wedged store call hold the group open
// indefinitely, so the detached work carries a server-side bound instead.
//
// Each waiter selects on its OWN context, so a caller that goes away leaves without
// disturbing the group.
func coalesce[R any](
	ctx context.Context,
	sf *singleflight.Group,
	key string,
	timeout time.Duration,
	fn func(context.Context) (R, error),
) (R, error) {
	var zero R
	ch := sf.DoChan(key, func() (any, error) {
		workCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer cancel()
		v, err := fn(workCtx)
		if err != nil {
			return nil, err
		}
		return v, nil
	})
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return zero, res.Err
		}
		v, ok := res.Val.(R)
		if !ok {
			return zero, fmt.Errorf("query: coalesced result for %q has unexpected type %T", key, res.Val)
		}
		return v, nil
	}
}

// fail maps err onto a Connect status and records it against the pipeline —
// except for the request-shaped rejections, which are the caller's mistake.
// Counting a malformed range as a pipeline error is the same conflation that
// sends the next operator looking for a store outage.
func (s *Server[V]) fail(err error) error {
	out := rpcError(err)
	if connect.CodeOf(out) == connect.CodeInternal {
		s.recorder.RecordError(s.pipeline, err)
	}
	return out
}

// rpcError maps a read-path error onto a Connect status code. Request-shaped
// rejections belong to the caller, not the store: reporting a swapped time range
// as CodeInternal sent operators hunting a DynamoDB outage that never happened.
func rpcError(err error) error {
	var connErr *connect.Error
	if errors.As(err, &connErr) {
		return err
	}
	switch {
	case errors.Is(err, query.ErrInvalidQuery):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}

// requireNonWindowed rejects an all-time read against a windowed pipeline. Get and
// GetMany read bucket 0, which on a windowed pipeline is both the all-time sentinel
// and the epoch bucket — so they can only ever report absent, however much data the
// pipeline has written. Four shipped runbooks pointed operators at Get for windowed
// counters before this became an error. A Granularity of zero legitimately writes
// bucket 0 even with a Window configured, so that shape stays allowed.
func (s *Server[V]) requireNonWindowed(alt string) error {
	if s.window != nil && s.window.Granularity > 0 {
		return connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("pipeline is windowed; bucket 0 holds no data — use %s instead", alt))
	}
	return nil
}

// durationFromSeconds converts a request's duration_seconds into a time.Duration.
// duration_seconds above ~9.2e9 overflows the nanosecond representation and wraps
// NEGATIVE, which then read as a perfectly ordinary tiny window rather than an error.
func durationFromSeconds(sec int64) (time.Duration, error) {
	const maxSeconds = int64(math.MaxInt64) / int64(time.Second)
	if sec <= 0 {
		return 0, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("duration_seconds must be > 0, got %d", sec))
	}
	if sec > maxSeconds {
		return 0, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("duration_seconds %d exceeds the representable maximum %d", sec, maxSeconds))
	}
	return time.Duration(sec) * time.Second, nil
}

// requireRangeBounds rejects an absolute range whose bounds were never set. proto3
// scalars have no presence, so an omitted start_unix/end_unix arrives as the epoch
// — previously answered with a fabricated Present:true zero over bucket 0.
func requireRangeBounds(startUnix, endUnix int64) error {
	if startUnix == 0 && endUnix == 0 {
		return connect.NewError(connect.CodeInvalidArgument,
			errors.New("start_unix and end_unix are required"))
	}
	return nil
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
// Returns CodeFailedPrecondition on a windowed pipeline — see
// requireNonWindowed. fresh_read does not bypass that check: a windowed
// pipeline has no all-time row to read freshly.
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

	if err := s.requireNonWindowed("GetWindow"); err != nil {
		return nil, err
	}
	entity := req.Msg.GetEntity()
	doGet := func(ctx context.Context) (coalescedResult[V], error) {
		v, ok, err := query.Get(ctx, s.store, entity)
		return coalescedResult[V]{value: v, present: ok}, err
	}

	var (
		r   coalescedResult[V]
		err error
	)
	if req.Msg.GetFreshRead() {
		r, err = doGet(ctx)
	} else {
		r, err = coalesce(ctx, &s.sf, "Get|"+encodeEntities([]string{entity}), s.coalesceTimeout, doGet)
	}
	if err != nil {
		return nil, s.fail(err)
	}
	val := &pb.Value{Present: false}
	if r.present {
		val = &pb.Value{Present: true, Data: s.encode(r.value)}
	}
	return connect.NewResponse(&pb.GetResponse{Value: val}), nil
}

// GetMany implements murmur.v1.QueryService/GetMany. Same shape as Get but
// for many entities in one round-trip; the response preserves request order
// so clients can zip without an extra index map. Same windowed-pipeline
// precondition as Get.
func (s *Server[V]) GetMany(ctx context.Context, req *connect.Request[pb.GetManyRequest]) (*connect.Response[pb.GetManyResponse], error) {
	start := time.Now()
	defer func() {
		s.recorder.RecordLatency(s.pipeline, "query_get_many", time.Since(start))
	}()
	s.recorder.RecordEvent(s.pipeline + ":query_get_many")

	if err := s.requireNonWindowed("GetWindowMany"); err != nil {
		return nil, err
	}
	keys := make([]state.Key, len(req.Msg.GetEntities()))
	for i, e := range req.Msg.GetEntities() {
		keys[i] = state.Key{Entity: e}
	}
	vals, oks, err := s.store.GetMany(ctx, keys)
	if err != nil {
		return nil, s.fail(err)
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
	v, err := s.windowedSingle(ctx, "query_get_window", "GetWindow", req.Msg.GetEntity(), req.Msg.GetDurationSeconds(), req.Msg.GetFreshRead())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&pb.GetWindowResponse{
		Value: &pb.Value{Present: true, Data: s.encode(v)},
	}), nil
}

// windowedSingle is the shared body of GetWindow and GetTrailing —
// both merge the most-recent buckets covering `durationSeconds` ending
// at the server's now. The RPC name (and coalesce-key prefix) is the
// only thing that differs; keeping that as a parameter rather than
// collapsing both RPCs onto one route preserves the documented intent
// at the wire layer.
func (s *Server[V]) windowedSingle(ctx context.Context, metric, coalescePrefix, entity string, durationSeconds int64, freshRead bool) (V, error) {
	start := time.Now()
	defer func() {
		s.recorder.RecordLatency(s.pipeline, metric, time.Since(start))
	}()
	s.recorder.RecordEvent(s.pipeline + ":" + metric)

	var zero V
	if s.window == nil {
		return zero, connect.NewError(connect.CodeFailedPrecondition, errors.New("pipeline is not windowed; use Get instead"))
	}
	d, err := durationFromSeconds(durationSeconds)
	if err != nil {
		return zero, err
	}
	now := s.nowFn()
	doFetch := func(ctx context.Context) (V, error) {
		return query.GetWindow(ctx, s.store, s.mon, *s.window, entity, d, now)
	}

	var v V
	if freshRead {
		v, err = doFetch(ctx)
	} else {
		// Coalesce key: bucketed "now" means consecutive requests within the
		// same bucket reuse a single store call; first request in a new bucket
		// does the work. This bounds staleness to at most one bucket.
		bucket := s.window.BucketID(now)
		key := coalescePrefix + "|" + strconv.FormatInt(durationSeconds, 10) + "|" +
			strconv.FormatInt(bucket, 10) + "|" + encodeEntities([]string{entity})
		v, err = coalesce(ctx, &s.sf, key, s.coalesceTimeout, doFetch)
	}
	if err != nil {
		return zero, s.fail(err)
	}
	return v, nil
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
	if err := requireRangeBounds(startUnix, endUnix); err != nil {
		return nil, err
	}
	entity := req.Msg.GetEntity()
	doFetch := func(ctx context.Context) (V, error) {
		start := time.Unix(startUnix, 0).UTC()
		end := time.Unix(endUnix, 0).UTC()
		return query.GetRange(ctx, s.store, s.mon, *s.window, entity, start, end)
	}

	var (
		v   V
		err error
	)
	if req.Msg.GetFreshRead() {
		v, err = doFetch(ctx)
	} else {
		key := "GetRange|" + strconv.FormatInt(startUnix, 10) + "|" +
			strconv.FormatInt(endUnix, 10) + "|" + encodeEntities([]string{entity})
		v, err = coalesce(ctx, &s.sf, key, s.coalesceTimeout, doFetch)
	}
	if err != nil {
		return nil, s.fail(err)
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
	vs, err := s.windowedMany(ctx, "query_get_window_many", "GetWindowMany", req.Msg.GetEntities(), req.Msg.GetDurationSeconds(), req.Msg.GetFreshRead())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&pb.GetWindowManyResponse{Values: s.encodeMany(vs)}), nil
}

// windowedMany is the shared body of GetWindowMany and GetTrailingMany.
// See windowedSingle for the design rationale.
func (s *Server[V]) windowedMany(ctx context.Context, metric, coalescePrefix string, entities []string, durationSeconds int64, freshRead bool) ([]V, error) {
	t0 := time.Now()
	defer func() {
		s.recorder.RecordLatency(s.pipeline, metric, time.Since(t0))
	}()
	s.recorder.RecordEvent(s.pipeline + ":" + metric)

	if s.window == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("pipeline is not windowed; use GetMany instead"))
	}
	d, err := durationFromSeconds(durationSeconds)
	if err != nil {
		return nil, err
	}
	now := s.nowFn()
	doFetch := func(ctx context.Context) ([]V, error) {
		return query.GetWindowMany(ctx, s.store, s.mon, *s.window, entities, d, now)
	}

	var vs []V
	if freshRead {
		vs, err = doFetch(ctx)
	} else {
		// Coalesce key: the entity list + duration + bucket. For typical query
		// shapes (a fixed candidate set per query), concurrent identical reads
		// collapse to one store fetch.
		bucket := s.window.BucketID(now)
		key := coalescePrefix + "|" + strconv.FormatInt(durationSeconds, 10) + "|" +
			strconv.FormatInt(bucket, 10) + "|" + encodeEntities(entities)
		vs, err = coalesce(ctx, &s.sf, key, s.coalesceTimeout, doFetch)
	}
	if err != nil {
		return nil, s.fail(err)
	}
	return vs, nil
}

// encodeMany wraps each typed value in a present=true pb.Value with the
// configured encoder. Shared by every "Many" response builder.
func (s *Server[V]) encodeMany(vs []V) []*pb.Value {
	out := make([]*pb.Value, len(vs))
	for i, v := range vs {
		out[i] = &pb.Value{Present: true, Data: s.encode(v)}
	}
	return out
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
	if err := requireRangeBounds(startUnix, endUnix); err != nil {
		return nil, err
	}

	doFetch := func(ctx context.Context) ([]V, error) {
		start := time.Unix(startUnix, 0).UTC()
		end := time.Unix(endUnix, 0).UTC()
		return query.GetRangeMany(ctx, s.store, s.mon, *s.window, entities, start, end)
	}

	var (
		vs  []V
		err error
	)
	if req.Msg.GetFreshRead() {
		vs, err = doFetch(ctx)
	} else {
		key := "GetRangeMany|" + strconv.FormatInt(startUnix, 10) + "|" +
			strconv.FormatInt(endUnix, 10) + "|" + encodeEntities(entities)
		vs, err = coalesce(ctx, &s.sf, key, s.coalesceTimeout, doFetch)
	}
	if err != nil {
		return nil, s.fail(err)
	}
	return connect.NewResponse(&pb.GetRangeManyResponse{Values: s.encodeMany(vs)}), nil
}

// GetTrailing implements murmur.v1.QueryService/GetTrailing. Semantically
// identical to GetWindow — both merge the most-recent buckets covering
// `duration_seconds` ending at the server's now — but exposed under a
// distinct RPC so callsites that think in "trailing windows" (last-7d,
// last-30d) don't have to translate intent. Same not-windowed
// precondition and same singleflight coalesce shape as GetWindow.
func (s *Server[V]) GetTrailing(ctx context.Context, req *connect.Request[pb.GetTrailingRequest]) (*connect.Response[pb.GetTrailingResponse], error) {
	v, err := s.windowedSingle(ctx, "query_get_trailing", "GetTrailing", req.Msg.GetEntity(), req.Msg.GetDurationSeconds(), req.Msg.GetFreshRead())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&pb.GetTrailingResponse{
		Value: &pb.Value{Present: true, Data: s.encode(v)},
	}), nil
}

// GetTrailingMany implements murmur.v1.QueryService/GetTrailingMany.
// Same shape as GetWindowMany; pairs with GetTrailing for the
// batched-trailing-windows case (e.g. trailing-7d engagement for
// 200 candidate posts in one round-trip).
func (s *Server[V]) GetTrailingMany(ctx context.Context, req *connect.Request[pb.GetTrailingManyRequest]) (*connect.Response[pb.GetTrailingManyResponse], error) {
	vs, err := s.windowedMany(ctx, "query_get_trailing_many", "GetTrailingMany", req.Msg.GetEntities(), req.Msg.GetDurationSeconds(), req.Msg.GetFreshRead())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&pb.GetTrailingManyResponse{Values: s.encodeMany(vs)}), nil
}

// encodeEntities builds the entity-list fragment of a singleflight coalesce key.
//
// Each entry is length-prefixed, and the list keeps its request order. Both parts
// are load-bearing:
//
// A plain '|' join is not injective over entity strings that may themselves
// contain '|' — and the shipped codegen key_template does exactly that
// (examples/typed-rpc-codegen/bot-interactions/pipeline-spec.yaml). ["a|b"] and
// ["a","b"] joined to the same key, as did ["a|b","c"] and ["a","b|c"]; a length
// check catches neither, since both pairs agree on total byte count. Two callers
// asking about different entities then shared one result slice.
//
// Order is preserved because responses are positional — value[i] belongs to
// entities[i]. Sorting made ["a","b"] and ["b","a"] one group, so the second
// caller received the first caller's slice with every value attributed to the
// wrong entity. Permutation coalescing is given up deliberately; silently
// transposed counters are far worse than a missed dedup.
func encodeEntities(entities []string) string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(len(entities)))
	for _, e := range entities {
		b.WriteByte('|')
		b.WriteString(strconv.Itoa(len(e)))
		b.WriteByte(':')
		b.WriteString(e)
	}
	return b.String()
}
