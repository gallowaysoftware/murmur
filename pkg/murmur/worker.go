package murmur

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
)

// RunStreamingWorker is the boilerplate-eating entry point for a streaming worker
// binary. It wires SIGINT/SIGTERM to the runtime context, runs streaming.Run, and
// returns the exit code (0 for clean shutdown, 1 for runtime error, 2 for invalid
// pipeline). Suitable for `os.Exit(murmur.RunStreamingWorker(ctx, pipe))` in main.
//
// Use this when you have a single pipeline per binary and want graceful-shutdown
// semantics. For multi-pipeline workers or custom error handling, drive
// streaming.Run yourself.
func RunStreamingWorker[T any, V any](ctx context.Context, p *pipeline.Pipeline[T, V]) int {
	logger := slog.Default()

	if p == nil {
		logger.Error("RunStreamingWorker: pipeline is nil")
		return 2
	}
	if err := p.Build(); err != nil {
		logger.Error("RunStreamingWorker: pipeline build failed", "err", err)
		return 2
	}

	runCtx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logger.Info("streaming worker starting", "pipeline", p.Name())
	if err := streaming.Run(runCtx, p); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("streaming runtime returned error", "pipeline", p.Name(), "err", err)
		return 1
	}
	logger.Info("streaming worker exited cleanly", "pipeline", p.Name())
	return 0
}
