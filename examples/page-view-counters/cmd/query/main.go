// gRPC query server for the page-view-counters example.
//
// Serves Get / GetMany / GetWindow / GetRange against the same DynamoDB table the
// streaming worker writes to.
//
// In production this binary runs as a separate ECS Fargate service behind an ALB.
// Locally:
//
//	export DDB_LOCAL_ENDPOINT=http://localhost:8000
//	go run ./examples/page-view-counters/cmd/query
//
// Then call it with grpcurl:
//
//	grpcurl -plaintext -d '{"entity": "page-A"}' \
//	    localhost:50051 murmur.v1.QueryService/Get
//	grpcurl -plaintext -d '{"entity": "page-A", "duration_seconds": 86400}' \
//	    localhost:50051 murmur.v1.QueryService/GetWindow
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	pageviews "github.com/gallowaysoftware/murmur/examples/page-view-counters"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	mgrpc "github.com/gallowaysoftware/murmur/pkg/query/grpc"
	pb "github.com/gallowaysoftware/murmur/proto/gen/murmur/v1"
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
	pipe, store, cache, err := pageviews.Build(ctx, cfg, false)
	if err != nil {
		logger.Error("build pipeline", "err", err)
		os.Exit(2)
	}
	_ = pipe // pipeline definition documents the wiring; the gRPC server reads through `store`.
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

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Error("listen", "addr", cfg.GRPCAddr, "err", err)
		os.Exit(2)
	}
	gs := grpc.NewServer()
	pb.RegisterQueryServiceServer(gs, srv)

	logger.Info("query server listening", "addr", cfg.GRPCAddr, "ddb_table", cfg.DDBTable)

	// Serve until context cancellation.
	serveErr := make(chan error, 1)
	go func() { serveErr <- gs.Serve(lis) }()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
		gs.GracefulStop()
	case err := <-serveErr:
		if err != nil {
			logger.Error("grpc Serve returned", "err", err)
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
