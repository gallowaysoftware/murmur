// Package admin is Murmur's read-only control plane. It implements the
// AdminService defined in proto/murmur/admin/v1/admin.proto via Connect-RPC,
// which means the same endpoint speaks gRPC (for Go/JVM/Rust clients),
// gRPC-Web (for browsers without a proxy), and the Connect protocol (the
// browser UI's default — plain HTTP+JSON, no special transport).
//
// The .proto is the contract. Anyone is welcome to point a different UI at
// the same endpoint, or generate a client in another language with `buf
// generate`. The Go server below is one implementation among many.
//
// Pipelines register themselves with Server.Register; Server.Handler returns
// an http.Handler that serves the AdminService routes. Pair it with
// FullHandler if you also want the embedded UI on the same port.
package admin

import (
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"sort"
	"sync"
	"time"

	"connectrpc.com/connect"

	"github.com/gallowaysoftware/murmur/pkg/metrics"
	adminv1 "github.com/gallowaysoftware/murmur/proto/gen/murmur/admin/v1"
	"github.com/gallowaysoftware/murmur/proto/gen/murmur/admin/v1/adminv1connect"
)

// Server is the admin control surface. Concurrent-safe.
type Server struct {
	mu        sync.RWMutex
	pipelines map[string]registered
	recorder  *metrics.InMemory
}

type registered struct {
	Info  PipelineInfo
	Query QueryFn
}

// PipelineInfo is the Go-friendly form of murmur.admin.v1.PipelineInfo. We keep
// a separate struct so package consumers don't have to import generated code,
// and so JSON tags work for any caller that wants to log it.
type PipelineInfo struct {
	Name          string
	MonoidKind    string
	Windowed      bool
	WindowGranSec int64
	WindowRetSec  int64
	StoreType     string
	CacheType     string
	SourceType    string
}

// QueryFn is the read-side closure each registered pipeline supplies. The op
// argument is one of "get" / "window" / "range"; params carries the per-op
// arguments (entity, bucket, duration_s, start, end, decode). Returns the raw
// stored bytes plus a `present` flag.
type QueryFn func(op string, params map[string]string) (data []byte, present bool, err error)

// NewServer constructs an admin Server. The recorder is shared with whatever
// runtime you've installed it on (typically streaming.Run via WithMetrics).
func NewServer(recorder *metrics.InMemory) *Server {
	return &Server{
		pipelines: make(map[string]registered),
		recorder:  recorder,
	}
}

// Register installs a pipeline's metadata and query closure. Idempotent —
// re-registering the same name overwrites.
func (s *Server) Register(info PipelineInfo, query QueryFn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pipelines[info.Name] = registered{Info: info, Query: query}
}

// Handler returns an http.Handler implementing the AdminService routes. Mount
// at "/" or under a subpath via http.StripPrefix.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	path, h := adminv1connect.NewAdminServiceHandler(s)
	mux.Handle(path, h)
	return cors(mux)
}

// --- AdminServiceHandler implementation ---

func (s *Server) Health(_ context.Context, _ *connect.Request[adminv1.HealthRequest]) (*connect.Response[adminv1.HealthResponse], error) {
	return connect.NewResponse(&adminv1.HealthResponse{Status: "ok"}), nil
}

func (s *Server) ListPipelines(_ context.Context, _ *connect.Request[adminv1.ListPipelinesRequest]) (*connect.Response[adminv1.ListPipelinesResponse], error) {
	s.mu.RLock()
	infos := make([]*adminv1.PipelineInfo, 0, len(s.pipelines))
	for _, r := range s.pipelines {
		infos = append(infos, toProtoInfo(r.Info))
	}
	s.mu.RUnlock()
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return connect.NewResponse(&adminv1.ListPipelinesResponse{Pipelines: infos}), nil
}

func (s *Server) GetPipelineMetrics(_ context.Context, req *connect.Request[adminv1.GetPipelineMetricsRequest]) (*connect.Response[adminv1.PipelineStats], error) {
	name := req.Msg.GetName()
	if !s.exists(name) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("pipeline not registered"))
	}
	if s.recorder == nil {
		return connect.NewResponse(&adminv1.PipelineStats{Pipeline: name, Latencies: map[string]*adminv1.LatencyStats{}}), nil
	}
	snap := s.recorder.SnapshotOne(name)
	out := &adminv1.PipelineStats{
		Pipeline:        snap.Pipeline,
		EventsProcessed: snap.EventsProcessed,
		Errors:          snap.Errors,
		Latencies:       map[string]*adminv1.LatencyStats{},
	}
	if !snap.LastEventAt.IsZero() {
		out.LastEventAt = snap.LastEventAt.UTC().Format(time.RFC3339Nano)
	}
	if !snap.LastErrorAt.IsZero() {
		out.LastErrorAt = snap.LastErrorAt.UTC().Format(time.RFC3339Nano)
	}
	out.LastError = snap.LastError
	for op, lat := range snap.Latencies {
		out.Latencies[op] = &adminv1.LatencyStats{
			N:     int64(lat.N),
			P50Ms: lat.P50,
			P95Ms: lat.P95,
			P99Ms: lat.P99,
			MaxMs: lat.Max,
		}
	}
	return connect.NewResponse(out), nil
}

func (s *Server) GetState(_ context.Context, req *connect.Request[adminv1.GetStateRequest]) (*connect.Response[adminv1.StateValue], error) {
	reg, ok := s.lookup(req.Msg.GetName())
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("pipeline not registered"))
	}
	if req.Msg.GetEntity() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("entity is required"))
	}
	params := map[string]string{"entity": req.Msg.GetEntity()}
	if req.Msg.GetBucket() != 0 {
		params["bucket"] = formatInt(req.Msg.GetBucket())
	}
	return s.dispatch(reg, "get", params, req.Msg.GetDecode())
}

func (s *Server) GetWindow(_ context.Context, req *connect.Request[adminv1.GetWindowRequest]) (*connect.Response[adminv1.StateValue], error) {
	reg, ok := s.lookup(req.Msg.GetName())
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("pipeline not registered"))
	}
	if req.Msg.GetEntity() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("entity is required"))
	}
	if req.Msg.GetDurationSeconds() <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("duration_seconds must be positive"))
	}
	params := map[string]string{
		"entity":     req.Msg.GetEntity(),
		"duration_s": formatInt(req.Msg.GetDurationSeconds()),
	}
	return s.dispatch(reg, "window", params, req.Msg.GetDecode())
}

func (s *Server) GetRange(_ context.Context, req *connect.Request[adminv1.GetRangeRequest]) (*connect.Response[adminv1.StateValue], error) {
	reg, ok := s.lookup(req.Msg.GetName())
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("pipeline not registered"))
	}
	if req.Msg.GetEntity() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("entity is required"))
	}
	if req.Msg.GetEndUnix() < req.Msg.GetStartUnix() {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("end_unix must be ≥ start_unix"))
	}
	params := map[string]string{
		"entity": req.Msg.GetEntity(),
		"start":  formatInt(req.Msg.GetStartUnix()),
		"end":    formatInt(req.Msg.GetEndUnix()),
	}
	return s.dispatch(reg, "range", params, req.Msg.GetDecode())
}

// --- helpers ---

func (s *Server) lookup(name string) (registered, bool) {
	s.mu.RLock()
	r, ok := s.pipelines[name]
	s.mu.RUnlock()
	return r, ok
}

func (s *Server) exists(name string) bool {
	_, ok := s.lookup(name)
	return ok
}

func (s *Server) dispatch(reg registered, op string, params map[string]string, decode bool) (*connect.Response[adminv1.StateValue], error) {
	data, present, err := reg.Query(op, params)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	out := &adminv1.StateValue{Present: present, Data: data}
	if present && decode {
		out.Decoded = decodeForKind(reg.Info.MonoidKind, data)
	}
	return connect.NewResponse(out), nil
}

// decodeForKind renders stored bytes as a monoid-typed DecodedValue. Unknown
// kinds and sketches today produce an Opaque payload; native sketch decoding
// (HLL → cardinality, TopK → items, Bloom → size+FPR) is a planned addition.
func decodeForKind(kind string, data []byte) *adminv1.DecodedValue {
	switch kind {
	case "sum", "count", "min", "max":
		var v int64
		if len(data) >= 8 {
			v = int64(binary.LittleEndian.Uint64(data))
		}
		return &adminv1.DecodedValue{
			Value: &adminv1.DecodedValue_Int64Value{Int64Value: v},
		}
	default:
		return &adminv1.DecodedValue{
			Value: &adminv1.DecodedValue_Opaque{
				Opaque: &adminv1.OpaqueValue{ByteLen: int64(len(data)), Kind: kind},
			},
		}
	}
}

func toProtoInfo(p PipelineInfo) *adminv1.PipelineInfo {
	return &adminv1.PipelineInfo{
		Name:                     p.Name,
		MonoidKind:               p.MonoidKind,
		Windowed:                 p.Windowed,
		WindowGranularitySeconds: p.WindowGranSec,
		WindowRetentionSeconds:   p.WindowRetSec,
		StoreType:                p.StoreType,
		CacheType:                p.CacheType,
		SourceType:               p.SourceType,
	}
}

func formatInt(n int64) string {
	// Avoid strconv import-cycle paranoia in hot paths; this is fine for params.
	const digits = "0123456789"
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	buf := [20]byte{}
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = digits[n%10]
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// cors permits all origins for local dev. Tighten for production via a future
// WithAllowedOrigins option (tracked in STABILITY.md).
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Connect-Protocol-Version, Connect-Timeout-Ms, X-Grpc-Web, Grpc-Timeout")
		w.Header().Set("Access-Control-Expose-Headers", "Grpc-Status, Grpc-Message")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
