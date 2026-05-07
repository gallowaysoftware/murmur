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
	"syscall"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/admin"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	demo := flag.Bool("demo", false, "register synthetic demo pipelines for UI development")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	rec := metrics.NewInMemory()
	srv := admin.NewServer(rec)

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
		Name:                       "page_views",
		MonoidKind:                 "sum",
		Windowed:                   true,
		WindowGranSec:   86400,
		WindowRetSec:     90 * 86400,
		StoreType:                  "dynamodb",
		CacheType:                  "valkey",
		SourceType:                 "kafka",
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

	srv.Register(admin.PipelineInfo{
		Name:       "unique_visitors",
		MonoidKind: "hll",
		Windowed:   true,
		WindowGranSec: 86400,
		WindowRetSec:   30 * 86400,
		StoreType:                "dynamodb",
		SourceType:               "kafka",
	}, func(op string, params map[string]string) ([]byte, bool, error) {
		entity := params["entity"]
		if entity == "" {
			return nil, false, nil
		}
		// 256-byte synthetic "HLL" payload.
		out := make([]byte, 256)
		for i := range out {
			out[i] = byte(i ^ len(entity))
		}
		return out, true, nil
	})

	srv.Register(admin.PipelineInfo{
		Name:                     "top_products",
		MonoidKind:               "topk",
		Windowed:                 false,
		StoreType:                "dynamodb",
		SourceType:               "kinesis",
	}, func(op string, params map[string]string) ([]byte, bool, error) {
		return []byte("\x00\x00\x00\x0a\x00\x00\x00\x00"), true, nil
	})
}

// runDemoTickers feeds the recorder with synthetic metrics for the demo pipelines.
// page_views ticks fast (~80 events/s), unique_visitors slower (~20/s), top_products
// ~5/s with occasional bursts of errors.
func runDemoTickers(rec *metrics.InMemory) {
	go tick(rec, "page_views", 80, 0.0, "store_merge", 0.5, 4.0)
	go tick(rec, "page_views", 80, 0.0, "cache_merge", 0.05, 0.6)
	go tick(rec, "unique_visitors", 20, 0.001, "store_merge", 1.5, 8.0)
	go tick(rec, "top_products", 5, 0.05, "store_merge", 2.0, 12.0)
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
