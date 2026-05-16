// murmur-ui is a single-binary admin server + embedded web UI for Murmur pipelines.
//
// In production it runs as a sidecar / standalone service that registered worker and
// query processes report to. For local development it has a --demo flag that
// registers two fake pipelines emitting synthetic metrics so you can drive the UI
// without bringing up the full docker-compose stack.
//
// Usage:
//
//	go run ./cmd/murmur-ui --demo --addr :8080
//	# open http://localhost:8080
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/admin"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/bloom"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/hll"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/topk"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	demo := flag.Bool("demo", false, "register synthetic demo pipelines for UI development")
	allowOrigins := flag.String("allow-origin", "",
		"comma-separated list of CORS-allowed origins; pass \"*\" for permissive (dev only). "+
			"Default empty = same-origin only, which is correct for the embedded UI.")
	authToken := flag.String("auth-token", "",
		"if set, require Authorization: Bearer <token> on every admin request. "+
			"Comma-separated list for rotation (any matching token is accepted). "+
			"Falls back to MURMUR_ADMIN_TOKEN env var when the flag is empty.")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	rec := metrics.NewInMemory()

	var serverOpts []admin.Option
	if *allowOrigins != "" {
		origins := strings.Split(*allowOrigins, ",")
		for i := range origins {
			origins[i] = strings.TrimSpace(origins[i])
		}
		serverOpts = append(serverOpts, admin.WithAllowedOrigins(origins...))
	}
	tokenSource := *authToken
	if tokenSource == "" {
		tokenSource = os.Getenv("MURMUR_ADMIN_TOKEN")
	}
	if tokenSource != "" {
		tokens := strings.Split(tokenSource, ",")
		for i := range tokens {
			tokens[i] = strings.TrimSpace(tokens[i])
		}
		serverOpts = append(serverOpts, admin.WithAuthToken(tokens...))
		logger.Info("admin auth enabled", "tokens", len(tokens))
	}
	srv := admin.NewServer(rec, serverOpts...)

	if *demo {
		registerDemoPipelines(srv)
		go runDemoTickers(rec)
		logger.Info("demo mode enabled: synthetic pipelines registered")
	}

	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           srv.FullHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		logger.Info("murmur-ui listening", "addr", *addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}

// registerDemoPipelines creates 3 synthetic pipelines so the UI has data to render
// without requiring a real Kafka/DDB stack. Useful for UI development and
// screenshots.
func registerDemoPipelines(srv *admin.Server) {
	srv.Register(admin.PipelineInfo{
		Name:          "page_views",
		MonoidKind:    "sum",
		Windowed:      true,
		WindowGranSec: 86400,
		WindowRetSec:  90 * 86400,
		StoreType:     "dynamodb",
		CacheType:     "valkey",
		SourceType:    "kafka",
	}, func(op string, params map[string]string) ([]byte, bool, error) {
		// Demo "store" — always returns synthetic int64.
		entity := params["entity"]
		switch entity {
		case "page-A":
			return int64LE(12345), true, nil
		case "page-B":
			return int64LE(987), true, nil
		default:
			return nil, false, nil
		}
	})

	// HLL — use a real sketch so the server's hll.Estimate decoder produces
	// a meaningful cardinality estimate in the Query Console.
	hllSketches := buildDemoHLLs()
	srv.Register(admin.PipelineInfo{
		Name:          "unique_visitors",
		MonoidKind:    "hll",
		Windowed:      true,
		WindowGranSec: 86400,
		WindowRetSec:  30 * 86400,
		StoreType:     "dynamodb",
		SourceType:    "kafka",
	}, func(op string, params map[string]string) ([]byte, bool, error) {
		b, ok := hllSketches[params["entity"]]
		return b, ok, nil
	})

	// TopK — real Misra-Gries sketch with realistic product counts.
	topkSketch := buildDemoTopK()
	srv.Register(admin.PipelineInfo{
		Name:       "top_products",
		MonoidKind: "topk",
		Windowed:   false,
		StoreType:  "dynamodb",
		SourceType: "kinesis",
	}, func(op string, params map[string]string) ([]byte, bool, error) {
		if params["entity"] != "global" {
			return nil, false, nil
		}
		return topkSketch, true, nil
	})

	// Bloom — a real filter populated with synthetic device IDs.
	bloomSketch := buildDemoBloom()
	srv.Register(admin.PipelineInfo{
		Name:       "seen_devices",
		MonoidKind: "bloom",
		Windowed:   false,
		StoreType:  "dynamodb",
		SourceType: "kafka",
	}, func(op string, params map[string]string) ([]byte, bool, error) {
		if params["entity"] != "global" {
			return nil, false, nil
		}
		return bloomSketch, true, nil
	})
}

// buildDemoHLLs constructs a marshaled HLL per "page-X" entity, each populated
// with a known number of synthetic user IDs. The Query Console will see the
// decoded cardinality estimate within ~1.6% of these counts.
func buildDemoHLLs() map[string][]byte {
	cases := map[string]int{
		"page-A": 53000,
		"page-B": 412,
		"page-C": 9876,
	}
	out := make(map[string][]byte, len(cases))
	m := hll.HLL()
	for entity, n := range cases {
		state := m.Identity()
		for i := 0; i < n; i++ {
			elem := []byte(fmt.Sprintf("%s-user-%d", entity, i))
			state = m.Combine(state, hll.Single(elem))
		}
		out[entity] = state
	}
	return out
}

// buildDemoTopK populates a Misra-Gries sketch with a heavy-tailed product
// distribution so the rendered top-K list reads as realistic rankings.
func buildDemoTopK() []byte {
	const k uint32 = 10
	m := topk.New(k)
	state := m.Identity()
	products := []struct {
		name  string
		count uint64
	}{
		{"sku-001-purple-widget", 12_847},
		{"sku-007-orange-gizmo", 9_312},
		{"sku-042-bluefin-cable", 7_506},
		{"sku-101-thunderclap", 4_220},
		{"sku-203-aurora-bundle", 3_117},
		{"sku-404-not-found", 2_055},
		{"sku-512-stardust-foam", 1_809},
		{"sku-731-quiet-fan", 1_104},
		{"sku-808-twilight-mug", 962},
		{"sku-911-dispatcher", 605},
	}
	for _, p := range products {
		state = m.Combine(state, topk.SingleN(k, p.name, p.count))
	}
	return state
}

// buildDemoBloom populates a Bloom filter with 5,000 synthetic device IDs so
// the Query Console can render capacity / hashes / approx-size from real shape.
//
// Uses the default-capacity monoid so the per-event Single() singletons share
// the same (m, k) parameters — Combine refuses to merge mismatched shapes
// (would silently return Identity, masking the bug as approx_size=0).
func buildDemoBloom() []byte {
	m := bloom.Bloom() // 100K-element capacity, p=0.01
	state := m.Identity()
	for i := 0; i < 5_000; i++ {
		state = m.Combine(state, bloom.Single([]byte(fmt.Sprintf("device-%06d", i))))
	}
	return state
}

// runDemoTickers feeds the recorder with synthetic metrics for the demo pipelines.
// page_views ticks fast (~80 events/s), unique_visitors slower (~20/s), top_products
// ~5/s with occasional bursts of errors.
func runDemoTickers(rec *metrics.InMemory) {
	go tick(rec, "page_views", 80, 0.0, "store_merge", 0.5, 4.0)
	go tick(rec, "page_views", 80, 0.0, "cache_merge", 0.05, 0.6)
	go tick(rec, "unique_visitors", 20, 0.001, "store_merge", 1.5, 8.0)
	go tick(rec, "top_products", 5, 0.05, "store_merge", 2.0, 12.0)
	go tick(rec, "seen_devices", 12, 0.0, "store_merge", 0.7, 3.5)
}

func tick(rec *metrics.InMemory, name string, rate int, errRate float64, op string, latP50ms, latP95ms float64) {
	if rate <= 0 {
		return
	}
	interval := time.Second / time.Duration(rate)
	t := time.NewTicker(interval)
	defer t.Stop()
	for range t.C {
		rec.RecordEvent(name)
		// Sample a latency from a triangular-ish distribution in [latP50/2, latP95*2].
		l := latP50ms + (latP95ms-latP50ms)*rand.Float64()*rand.Float64()
		rec.RecordLatency(name, op, time.Duration(l*float64(time.Millisecond)))
		if errRate > 0 && rand.Float64() < errRate {
			rec.RecordError(name, fmt.Errorf("synthetic demo error #%d", rand.IntN(1000)))
		}
	}
}

func int64LE(v int64) []byte {
	b := make([]byte, 8)
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (i * 8))
	}
	return b
}
