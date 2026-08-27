package grpc

import (
	"context"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/grpchealth"

	"github.com/gallowaysoftware/murmur/pkg/state"
)

// DefaultHealthCacheTTL is how long a readiness probe result is reused when
// HealthCacheTTL is not set.
//
// Probes arrive on a schedule the caller does not control — an ALB target
// group defaults to every 15s per target, and Kubernetes readiness probes are
// often tighter. Hitting DynamoDB on each one turns health checking into
// steady billed read traffic, and worse, couples probe latency to store
// latency: a slow-but-working table starts failing health checks and the
// orchestrator kills tasks that were fine.
const DefaultHealthCacheTTL = 10 * time.Second

// HealthOption configures the health handlers.
type HealthOption func(*healthConfig)

type healthConfig struct {
	ttl        time.Duration
	probe      func(context.Context) error
	sentinelPK string
}

// WithHealthCacheTTL sets how long a readiness result is reused. Zero or
// negative disables caching, which probes the store on every request — rarely
// what you want; see DefaultHealthCacheTTL.
func WithHealthCacheTTL(d time.Duration) HealthOption {
	return func(c *healthConfig) { c.ttl = d }
}

// WithHealthProbe replaces the default store round-trip with a custom check.
// Return nil for healthy.
func WithHealthProbe(fn func(context.Context) error) HealthOption {
	return func(c *healthConfig) { c.probe = fn }
}

// WithHealthSentinelKey sets the entity key the default probe reads. It never
// needs to exist — a clean "absent" answer proves the round-trip as well as a
// hit does. Override only if the default collides with a real key you would
// rather not touch.
func WithHealthSentinelKey(k string) HealthOption {
	return func(c *healthConfig) { c.sentinelPK = k }
}

// health is the cached readiness state shared by both handlers.
type health[V any] struct {
	cfg   healthConfig
	store state.Store[V]

	mu      sync.Mutex
	lastAt  time.Time
	lastErr error
}

func (s *Server[V]) newHealth(opts []HealthOption) *health[V] {
	cfg := healthConfig{ttl: DefaultHealthCacheTTL, sentinelPK: "__murmur_health__"}
	for _, o := range opts {
		o(&cfg)
	}
	return &health[V]{cfg: cfg, store: s.store}
}

// check returns nil when the backing store answered recently.
func (h *health[V]) check(ctx context.Context) error {
	if h.cfg.probe != nil {
		return h.cfg.probe(ctx)
	}
	h.mu.Lock()
	if h.cfg.ttl > 0 && !h.lastAt.IsZero() && time.Since(h.lastAt) < h.cfg.ttl {
		err := h.lastErr
		h.mu.Unlock()
		return err
	}
	h.mu.Unlock()

	// A Get on a key that need not exist. "Absent" proves the round-trip just
	// as well as a hit, and costs one read unit.
	_, _, err := h.store.Get(ctx, state.Key{Entity: h.cfg.sentinelPK})

	h.mu.Lock()
	h.lastAt, h.lastErr = time.Now(), err
	h.mu.Unlock()
	return err
}

// healthChecker adapts health to grpchealth.Checker.
type healthChecker[V any] struct{ h *health[V] }

func (c healthChecker[V]) Check(ctx context.Context, _ *grpchealth.CheckRequest) (*grpchealth.CheckResponse, error) {
	if err := c.h.check(ctx); err != nil {
		return &grpchealth.CheckResponse{Status: grpchealth.StatusNotServing}, nil
	}
	return &grpchealth.CheckResponse{Status: grpchealth.StatusServing}, nil
}

// HealthHandler returns the standard grpc.health.v1.Health service and its
// mount path, for wiring alongside Handler():
//
//	mux.Handle(srv.Handler())
//	mux.Handle(srv.HealthHandler())
//
// Until this existed, nothing in the tree implemented grpc.health.v1.Health,
// so an ALB target group probing /grpc.health.v1.Health/Check was matching on
// the gRPC UNIMPLEMENTED status (12) landing inside a permissive 0-99 matcher.
// That "passes" whether or not the service can reach its store — it only ever
// proved the port was open. With a real Health service the matcher can be
// narrowed to 0 (OK) and the probe means something.
func (s *Server[V]) HealthHandler(opts ...HealthOption) (string, http.Handler) {
	return grpchealth.NewHandler(healthChecker[V]{h: s.newHealth(opts)})
}

// HealthzHandler returns a plain HTTP handler for callers that cannot speak
// gRPC health — an ALB target group with protocol_version HTTP1, a k8s probe,
// or curl.
//
// It distinguishes the two questions orchestrators actually ask, which a
// single endpoint conflates:
//
//   - GET /healthz — LIVENESS. The process is running. Always 200. A failing
//     dependency must not answer this with 503, or the orchestrator restarts a
//     healthy task and turns a store blip into a crash loop.
//   - GET /readyz — READINESS. The backing store answered. 200 or 503. This is
//     the one a load balancer should use to decide whether to route traffic.
//
// Any other path under the mount point is 404.
func (s *Server[V]) HealthzHandler(opts ...HealthOption) http.Handler {
	h := s.newHealth(opts)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
		case "/readyz":
			if err := h.check(r.Context()); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("store unreachable\n"))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready\n"))
		default:
			http.NotFound(w, r)
		}
	})
}
