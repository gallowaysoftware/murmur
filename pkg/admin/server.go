// Package admin is Murmur's read-only HTTP control surface, designed to back the
// web UI. Endpoints return JSON; CORS is permissive for local-dev cross-origin
// access from the Vite dev server.
//
// Routes (all GET):
//
//	/api/pipelines                                      → []PipelineInfo
//	/api/pipelines/{name}/metrics                       → PipelineStatsJSON
//	/api/pipelines/{name}/state?entity=X[&bucket=N]     → StateValueJSON
//	/api/pipelines/{name}/window?entity=X&duration_s=N  → StateValueJSON (windowed merge)
//	/api/pipelines/{name}/range?entity=X&start=…&end=…  → StateValueJSON
//	/api/health                                          → "ok"
//
// PipelineInfo / StateValueJSON / etc. are the small DTOs documented inline.
//
// Pipelines are registered via Server.Register(name, info, queryFn). The queryFn is a
// pipeline-typed closure that knows how to read its store and produce []byte for
// the wire — same encoding pattern as the gRPC Server.Encode hook. This keeps the
// admin server agnostic to V.
package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid"
)

// Server is the admin HTTP server. Concurrent-safe.
type Server struct {
	mu        sync.RWMutex
	pipelines map[string]registered
	recorder  *metrics.InMemory
}

type registered struct {
	Info  PipelineInfo
	Query QueryFn
}

// PipelineInfo describes a pipeline for the admin UI.
type PipelineInfo struct {
	Name          string `json:"name"`
	MonoidKind    string `json:"monoid_kind"`
	Windowed      bool   `json:"windowed"`
	WindowGranSec int64  `json:"window_granularity_seconds,omitempty"`
	WindowRetSec  int64  `json:"window_retention_seconds,omitempty"`
	StoreType     string `json:"store_type"`
	CacheType     string `json:"cache_type,omitempty"`
	SourceType    string `json:"source_type,omitempty"`
}

// QueryFn is the read-side closure each registered pipeline supplies. Op is one of:
//
//	"get"    — params: entity[, bucket]
//	"window" — params: entity, duration_s
//	"range"  — params: entity, start_unix, end_unix
//
// Returns the encoded value (e.g. int64 little-endian, HLL bytes) and a "present"
// flag indicating whether data exists. Implementations should do any monoid merging
// internally — the admin server doesn't know V.
type QueryFn func(op string, params map[string]string) (data []byte, present bool, err error)

// NewServer constructs an admin Server. The recorder is shared with whatever runtime
// you've installed it on (typically streaming.Run).
func NewServer(recorder *metrics.InMemory) *Server {
	return &Server{
		pipelines: make(map[string]registered),
		recorder:  recorder,
	}
}

// Register installs a pipeline's metadata and query closure. Idempotent: re-registering
// the same name overwrites.
func (s *Server) Register(info PipelineInfo, query QueryFn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pipelines[info.Name] = registered{Info: info, Query: query}
}

// RegisterFromMonoid is a convenience that derives MonoidKind from a monoid.Monoid[V]
// instance. Equivalent to filling out PipelineInfo by hand.
func (s *Server) RegisterFromMonoid(name string, m monoid.Monoid[any], info PipelineInfo, query QueryFn) {
	if info.Name == "" {
		info.Name = name
	}
	if m != nil {
		info.MonoidKind = string(m.Kind())
	}
	s.Register(info, query)
}

// Handler returns the http.Handler with all routes registered. Mount at "/" or under
// a subpath via http.StripPrefix.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.handleHealth)
	mux.HandleFunc("GET /api/pipelines", s.handleListPipelines)
	mux.HandleFunc("GET /api/pipelines/{name}/metrics", s.handlePipelineMetrics)
	mux.HandleFunc("GET /api/pipelines/{name}/state", s.handleQuery("get"))
	mux.HandleFunc("GET /api/pipelines/{name}/window", s.handleQuery("window"))
	mux.HandleFunc("GET /api/pipelines/{name}/range", s.handleQuery("range"))
	return cors(mux)
}

// PipelineStatsJSON is the wire shape of a metrics snapshot.
type PipelineStatsJSON struct {
	Pipeline        string                 `json:"pipeline"`
	EventsProcessed uint64                 `json:"events_processed"`
	Errors          uint64                 `json:"errors"`
	LastEventAt     string                 `json:"last_event_at,omitempty"`
	LastErrorAt     string                 `json:"last_error_at,omitempty"`
	LastError       string                 `json:"last_error,omitempty"`
	Latencies       map[string]LatencyJSON `json:"latencies"`
}

// LatencyJSON is the wire shape of a latency histogram.
type LatencyJSON struct {
	N   int     `json:"n"`
	P50 float64 `json:"p50_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
	Max float64 `json:"max_ms"`
}

// StateValueJSON is the wire shape of a state read.
type StateValueJSON struct {
	Present bool `json:"present"`
	// DataB64 is the value bytes encoded as base64 (Go's encoding/json does this for []byte).
	DataB64 []byte `json:"data,omitempty"`
	// Decoded is a monoid-aware rendering of the bytes for UI consumption: int64 for
	// Sum/Count/Min/Max, an opaque "byteLen" object for sketches today (HLL/TopK/
	// Bloom decode to cardinality/items/size — wire-up planned in a follow-up).
	// nil unless the request includes ?decode=true.
	Decoded any `json:"decoded,omitempty"`
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleListPipelines(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	infos := make([]PipelineInfo, 0, len(s.pipelines))
	for _, r := range s.pipelines {
		infos = append(infos, r.Info)
	}
	s.mu.RUnlock()
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	writeJSON(w, infos)
}

func (s *Server) handlePipelineMetrics(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !s.exists(name) {
		http.NotFound(w, r)
		return
	}
	if s.recorder == nil {
		writeJSON(w, PipelineStatsJSON{Pipeline: name, Latencies: map[string]LatencyJSON{}})
		return
	}
	snap := s.recorder.SnapshotOne(name)
	out := PipelineStatsJSON{
		Pipeline:        snap.Pipeline,
		EventsProcessed: snap.EventsProcessed,
		Errors:          snap.Errors,
		Latencies:       map[string]LatencyJSON{},
	}
	if !snap.LastEventAt.IsZero() {
		out.LastEventAt = snap.LastEventAt.UTC().Format(time.RFC3339Nano)
	}
	if !snap.LastErrorAt.IsZero() {
		out.LastErrorAt = snap.LastErrorAt.UTC().Format(time.RFC3339Nano)
	}
	out.LastError = snap.LastError
	for op, lat := range snap.Latencies {
		out.Latencies[op] = LatencyJSON{N: lat.N, P50: lat.P50, P95: lat.P95, P99: lat.P99, Max: lat.Max}
	}
	writeJSON(w, out)
}

func (s *Server) handleQuery(op string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		s.mu.RLock()
		reg, ok := s.pipelines[name]
		s.mu.RUnlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		params := map[string]string{}
		for k, v := range r.URL.Query() {
			if len(v) > 0 {
				params[k] = v[0]
			}
		}
		// Light validation per op.
		if _, hasEntity := params["entity"]; !hasEntity {
			http.Error(w, "missing entity param", http.StatusBadRequest)
			return
		}
		if op == "window" {
			if _, ok := params["duration_s"]; !ok {
				http.Error(w, "missing duration_s param", http.StatusBadRequest)
				return
			}
			if _, err := strconv.ParseInt(params["duration_s"], 10, 64); err != nil {
				http.Error(w, "duration_s must be an integer (seconds)", http.StatusBadRequest)
				return
			}
		}
		if op == "range" {
			for _, k := range []string{"start", "end"} {
				v, ok := params[k]
				if !ok {
					http.Error(w, fmt.Sprintf("missing %s param (unix seconds)", k), http.StatusBadRequest)
					return
				}
				if _, err := strconv.ParseInt(v, 10, 64); err != nil {
					http.Error(w, fmt.Sprintf("%s must be an integer (unix seconds)", k), http.StatusBadRequest)
					return
				}
			}
		}
		data, present, err := reg.Query(op, params)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out := StateValueJSON{Present: present, DataB64: data}
		if present && r.URL.Query().Get("decode") == "true" {
			out.Decoded = decodeForKind(reg.Info.MonoidKind, data)
		}
		writeJSON(w, out)
	}
}

// decodeForKind returns a JSON-friendly rendering of a stored value based on the
// pipeline's monoid Kind. Unknown / sketch kinds return an opaque {byte_len: N}
// object today; native Spark / Valkey-backed sketch decoding is roadmap.
func decodeForKind(kind string, data []byte) any {
	switch kind {
	case "sum", "count", "min", "max":
		if len(data) < 8 {
			return map[string]any{"int64": 0}
		}
		var v int64
		for i := 0; i < 8; i++ {
			v |= int64(data[i]) << (i * 8)
		}
		return map[string]any{"int64": v}
	default:
		return map[string]any{"byte_len": len(data), "kind": kind}
	}
}

func (s *Server) exists(name string) bool {
	s.mu.RLock()
	_, ok := s.pipelines[name]
	s.mu.RUnlock()
	return ok
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// cors permits all origins for local dev. Tighten for production.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
