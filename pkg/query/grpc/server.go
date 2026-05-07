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
	"time"

	"connectrpc.com/connect"

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
type Server[V any] struct {
	store  state.Store[V]
	mon    monoid.Monoid[V]
	window *windowed.Config
	encode Encoder[V]
	nowFn  func() time.Time
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
}

// NewServer constructs a query Server.
func NewServer[V any](cfg Config[V]) *Server[V] {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Server[V]{
		store:  cfg.Store,
		mon:    cfg.Monoid,
		window: cfg.Window,
		encode: cfg.Encode,
		nowFn:  now,
	}
}

// Handler returns the Connect HTTP handler and its mount path. Wire it into a
// net/http server with `mux.Handle(path, h)`. The path follows the Connect
// convention `/murmur.v1.QueryService/`.
func (s *Server[V]) Handler() (string, http.Handler) {
	return murmurv1connect.NewQueryServiceHandler(s)
}

// --- QueryServiceHandler implementation ---

func (s *Server[V]) Get(ctx context.Context, req *connect.Request[pb.GetRequest]) (*connect.Response[pb.Value], error) {
	v, ok, err := query.Get(ctx, s.store, req.Msg.GetEntity())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if !ok {
		return connect.NewResponse(&pb.Value{Present: false}), nil
	}
	return connect.NewResponse(&pb.Value{Present: true, Data: s.encode(v)}), nil
}

func (s *Server[V]) GetMany(ctx context.Context, req *connect.Request[pb.GetManyRequest]) (*connect.Response[pb.Values], error) {
	keys := make([]state.Key, len(req.Msg.GetEntities()))
	for i, e := range req.Msg.GetEntities() {
		keys[i] = state.Key{Entity: e}
	}
	vals, oks, err := s.store.GetMany(ctx, keys)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	out := &pb.Values{Values: make([]*pb.Value, len(req.Msg.GetEntities()))}
	for i := range req.Msg.GetEntities() {
		if !oks[i] {
			out.Values[i] = &pb.Value{Present: false}
			continue
		}
		out.Values[i] = &pb.Value{Present: true, Data: s.encode(vals[i])}
	}
	return connect.NewResponse(out), nil
}

func (s *Server[V]) GetWindow(ctx context.Context, req *connect.Request[pb.GetWindowRequest]) (*connect.Response[pb.Value], error) {
	if s.window == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("pipeline is not windowed; use Get instead"))
	}
	d := time.Duration(req.Msg.GetDurationSeconds()) * time.Second
	v, err := query.GetWindow(ctx, s.store, s.mon, *s.window, req.Msg.GetEntity(), d, s.nowFn())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&pb.Value{Present: true, Data: s.encode(v)}), nil
}

func (s *Server[V]) GetRange(ctx context.Context, req *connect.Request[pb.GetRangeRequest]) (*connect.Response[pb.Value], error) {
	if s.window == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("pipeline is not windowed; use Get instead"))
	}
	start := time.Unix(req.Msg.GetStartUnix(), 0).UTC()
	end := time.Unix(req.Msg.GetEndUnix(), 0).UTC()
	v, err := query.GetRange(ctx, s.store, s.mon, *s.window, req.Msg.GetEntity(), start, end)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&pb.Value{Present: true, Data: s.encode(v)}), nil
}
