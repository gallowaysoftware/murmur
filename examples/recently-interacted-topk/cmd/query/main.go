// Connect-RPC query server for the recently-interacted-topk example.
//
// Serves Get / GetMany / GetWindow / GetRange against the same DynamoDB
// row both writers (Lambda + ECS Kafka worker) merge into. The byte-encoded
// Misra-Gries summary is returned verbatim; clients decode via the
// pkg/monoid/sketch/topk package's Decode helper, or via the embedded
// admin UI which renders the sketch's items + counts directly.
//
// Run locally:
//
//	export DDB_ENDPOINT=http://localhost:8000
//	go run ./examples/recently-interacted-topk/cmd/query
//
// Then call it:
//
//	# all-time top entities (when running non-windowed)
//	grpcurl -plaintext -d '{"entity":"global"}' \
//	    localhost:50051 murmur.v1.QueryService/Get
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
	"syscall"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

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
		Store:  store,
		Monoid: topk.New(32), // K must match the Build() default; pin via env in production
		Window: &window,
		Encode: mgrpc.BytesIdentity(),
	})

	mux := http.NewServeMux()
	mux.Handle(srv.Handler())

	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           h2c.NewHandler(mux, &http2.Server{}),
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
