// Package grpc serves Murmur's read-side query layer over gRPC.
//
// Phase 1 ships a generic Value (bytes) shape: the server takes a pipeline-typed Store,
// monoid, and windowing config, plus an Encoder[V] that converts the typed value into
// wire bytes. Clients are responsible for matching encoding (Int64LE for Sum/Count, raw
// bytes for sketches, etc.). Phase 2 will codegen pipeline-typed responses
// (CounterResponse, HLLResponse) from the pipeline definition and remove the
// caller-side decoding burden.
package grpc

import (
	"context"
	"encoding/binary"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/query"
	"github.com/gallowaysoftware/murmur/pkg/state"
	pb "github.com/gallowaysoftware/murmur/proto/gen/murmur/v1"
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

// BytesIdentity encodes []byte values verbatim — for sketches whose marshaled form is
// already the desired wire format.
func BytesIdentity() Encoder[[]byte] {
	return func(v []byte) []byte { return v }
}

// Server bridges the generated gRPC interface to a pipeline's Store + monoid.
type Server[V any] struct {
	pb.UnimplementedQueryServiceServer

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

// NewServer constructs a query Server. Register it on a *grpc.Server via
// pb.RegisterQueryServiceServer.
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

// Get implements pb.QueryServiceServer.Get.
func (s *Server[V]) Get(ctx context.Context, req *pb.GetRequest) (*pb.Value, error) {
	v, ok, err := query.Get(ctx, s.store, req.GetEntity())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "store get: %v", err)
	}
	if !ok {
		return &pb.Value{Present: false}, nil
	}
	return &pb.Value{Present: true, Data: s.encode(v)}, nil
}

// GetMany implements pb.QueryServiceServer.GetMany.
func (s *Server[V]) GetMany(ctx context.Context, req *pb.GetManyRequest) (*pb.Values, error) {
	keys := make([]state.Key, len(req.GetEntities()))
	for i, e := range req.GetEntities() {
		keys[i] = state.Key{Entity: e}
	}
	vals, oks, err := s.store.GetMany(ctx, keys)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "store get-many: %v", err)
	}
	out := &pb.Values{Values: make([]*pb.Value, len(req.GetEntities()))}
	for i := range req.GetEntities() {
		if !oks[i] {
			out.Values[i] = &pb.Value{Present: false}
			continue
		}
		out.Values[i] = &pb.Value{Present: true, Data: s.encode(vals[i])}
	}
	return out, nil
}

// GetWindow implements pb.QueryServiceServer.GetWindow.
func (s *Server[V]) GetWindow(ctx context.Context, req *pb.GetWindowRequest) (*pb.Value, error) {
	if s.window == nil {
		return nil, status.Error(codes.FailedPrecondition, "pipeline is not windowed; use Get instead")
	}
	d := time.Duration(req.GetDurationSeconds()) * time.Second
	v, err := query.GetWindow(ctx, s.store, s.mon, *s.window, req.GetEntity(), d, s.nowFn())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get-window: %v", err)
	}
	return &pb.Value{Present: true, Data: s.encode(v)}, nil
}

// GetRange implements pb.QueryServiceServer.GetRange.
func (s *Server[V]) GetRange(ctx context.Context, req *pb.GetRangeRequest) (*pb.Value, error) {
	if s.window == nil {
		return nil, status.Error(codes.FailedPrecondition, "pipeline is not windowed; use Get instead")
	}
	start := time.Unix(req.GetStartUnix(), 0).UTC()
	end := time.Unix(req.GetEndUnix(), 0).UTC()
	v, err := query.GetRange(ctx, s.store, s.mon, *s.window, req.GetEntity(), start, end)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get-range: %v", err)
	}
	return &pb.Value{Present: true, Data: s.encode(v)}, nil
}
