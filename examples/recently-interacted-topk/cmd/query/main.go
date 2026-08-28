// Connect-RPC query server for the recently-interacted-topk example.
//
// Serves GetWindow / GetWindowMany / GetRange against the same DynamoDB
// row both writers (Lambda + ECS Kafka worker) merge into. The byte-encoded
// Misra-Gries summary is returned verbatim; clients decode via the
// pkg/monoid/sketch/topk package's Decode helper, or via the embedded
// admin UI which renders the sketch's items + counts directly.
//
// This server configures daily windowing, so Get / GetMany are rejected with
// FAILED_PRECONDITION: they read the all-time row at bucket 0, which the
// windowed writers never populate, and an empty answer there is
// indistinguishable from a genuinely idle pipeline.
//
// Run locally:
//
//	export DDB_ENDPOINT=http://localhost:8000
//	go run ./examples/recently-interacted-topk/cmd/query
//
// Then call it:
//
//	# top entities over the last 24 hours
//	grpcurl -plaintext -d '{"entity":"global","duration_seconds":86400}' \
//	    localhost:50051 murmur.v1.QueryService/GetWindow
//	# top entities over the last 7 days
//	grpcurl -plaintext -d '{"entity":"global","duration_seconds":604800}' \
//	    localhost:50051 murmur.v1.QueryService/GetWindow
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	example "github.com/gallowaysoftware/murmur/examples/recently-interacted-topk"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/topk"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	mgrpc "github.com/gallowaysoftware/murmur/pkg/query/grpc"
)

func main() {
	os.Exit(run())
}

func run() int {
	cfg := example.Config{
		DDBEndpoint:     os.Getenv("DDB_ENDPOINT"),
		DDBTable:        envOr("DDB_TABLE", "recently_interacted"),
		DDBRegion:       envOr("AWS_REGION", "us-east-1"),
		K:               envU32("TOPK_K", example.DefaultK),
		WindowRetention: 30 * 24 * time.Hour,
	}
	addr := envOr("GRPC_ADDR", ":50051")

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	_, store, _, err := example.Build(ctx, cfg)
	if err != nil {
		logger.Error("build pipeline", "err", err)
		return 2
	}
	defer func() { _ = store.Close() }()

	window := windowed.Daily(cfg.WindowRetention)
	srv := mgrpc.NewServer(mgrpc.Config[[]byte]{
		Store: store,
		// Same Config, same ResolveK: a query server whose K differs from the
		// writers' reads a sketch that refuses to merge, and the Top-N comes
		// back empty instead of erroring.
		Monoid: topk.New(cfg.ResolveK()),
		Window: &window,
		Encode: mgrpc.BytesIdentity(),
	})

	mux := http.NewServeMux()
	mux.Handle(srv.Handler())

	// Health endpoints. Without these an ALB target group probing
	// /grpc.health.v1.Health/Check was only ever matching on the gRPC
	// UNIMPLEMENTED status falling inside a permissive matcher — it proved the
	// port was open and nothing more.
	mux.Handle(srv.HealthHandler())
	mux.Handle("/healthz", srv.HealthzHandler()) // liveness: process is up
	mux.Handle("/readyz", srv.HealthzHandler())  // readiness: store answered

	// http.Server.Protocols enables HTTP/2-over-plaintext (replacement
	// for the deprecated golang.org/x/net/http2/h2c package; Go 1.24+).
	protocols := &http.Protocols{}
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		Protocols:         protocols,
		ReadHeaderTimeout: 10 * time.Second,
	}

	logger.Info("query server listening (gRPC + gRPC-Web + Connect)",
		"addr", addr, "ddb_table", cfg.DDBTable)

	serveErr := make(chan error, 1)
	go func() { serveErr <- httpSrv.ListenAndServe() }()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http Serve returned", "err", err)
			return 1
		}
	}
	return 0
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envU32 mirrors the writers' TOPK_K parsing so one env var pins K across
// every binary in the deployment.
func envU32(key string, fallback uint32) uint32 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil || n == 0 {
		slog.Warn("invalid TopK size, using default", "key", key, "value", v, "default", fallback, "err", err)
		return fallback
	}
	return uint32(n)
}
