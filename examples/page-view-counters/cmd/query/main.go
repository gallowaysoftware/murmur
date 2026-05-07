// Connect-RPC query server for the page-view-counters example.
//
// Serves Get / GetMany / GetWindow / GetRange against the same DynamoDB table
// the streaming worker writes to. The endpoint speaks gRPC, gRPC-Web, and
// Connect (HTTP+JSON) on the same port — pick whichever your client supports.
//
// In production this binary runs as a separate ECS Fargate service behind an
// ALB (the Terraform pipeline-counter module's `query` service). Locally:
//
//	export DDB_LOCAL_ENDPOINT=http://localhost:8000
//	go run ./examples/page-view-counters/cmd/query
//
// Then call it with grpcurl, buf curl, or plain curl:
//
//	grpcurl -plaintext -d '{"entity": "page-A"}' \
//	    localhost:50051 murmur.v1.QueryService/Get
//	curl -X POST http://localhost:50051/murmur.v1.QueryService/Get \
//	    -H 'Content-Type: application/json' -d '{"entity": "page-A"}'
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

	pageviews "github.com/gallowaysoftware/murmur/examples/page-view-counters"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	mgrpc "github.com/gallowaysoftware/murmur/pkg/query/grpc"
)

func main() {
	cfg := pageviews.Config{
		DDBEndpoint:     os.Getenv("DDB_LOCAL_ENDPOINT"),
		DDBTable:        envOr("DDB_TABLE", "page_views"),
		DDBRegion:       envOr("AWS_REGION", "us-east-1"),
		ValkeyAddress:   os.Getenv("VALKEY_ADDRESS"),
		ValkeyKeyPrefix: envOr("VALKEY_KEY_PREFIX", "page_views"),
		GRPCAddr:        envOr("GRPC_ADDR", ":50051"),
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// withSource=false: query process doesn't need a Kafka consumer.
	_, store, cache, err := pageviews.Build(ctx, cfg, false)
	if err != nil {
		logger.Error("build pipeline", "err", err)
		os.Exit(2)
	}
	defer func() {
		if cache != nil {
			_ = cache.Close()
		}
		_ = store.Close()
	}()

	window := pageviews.Window
	srv := mgrpc.NewServer(mgrpc.Config[int64]{
		Store:  store,
		Monoid: core.Sum[int64](),
		Window: &window,
		Encode: mgrpc.Int64LE(),
	})

	mux := http.NewServeMux()
	mux.Handle(srv.Handler())

	// h2c gives us HTTP/2 over plaintext on the same port — required for native
	// gRPC clients. Connect (HTTP+JSON) and gRPC-Web work over plain HTTP/1.1
	// on the same listener.
	httpSrv := &http.Server{
		Addr:              cfg.GRPCAddr,
		Handler:           h2c.NewHandler(mux, &http2.Server{}),
		ReadHeaderTimeout: 10 * time.Second,
	}

	logger.Info("query server listening (gRPC + gRPC-Web + Connect)",
		"addr", cfg.GRPCAddr, "ddb_table", cfg.DDBTable)

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
			os.Exit(1)
		}
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
