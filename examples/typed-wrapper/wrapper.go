// Package wrapper is a count-core-shaped example showing how to expose
// Murmur counter pipelines through your application's OWN typed
// Connect-RPC service rather than the generic Value{bytes} shape.
//
// Pattern (the count-core integration plan from the review thread):
//
//   - Murmur pipelines do the aggregation. Their query layer is a
//     generic `QueryService.Get/GetMany/GetWindow → Value{bytes}`.
//   - Your application defines its OWN proto with typed responses
//     (e.g. count-core's `BotInteractionCountService.GetBotInteractionCount(...)`
//     returning `int64`).
//   - The application's server implementation is a thin wrapper:
//     it takes the typed request, calls the underlying Murmur
//     QueryService via `pkg/query/typed`, and returns a typed
//     response. No bytes-decoding boilerplate; no leakage of
//     Murmur's wire shape into the application's API.
//
// This file shows that wrapper for a fictional "BotInteractionCount"
// service. Real count-core code would substitute its own proto
// definition; the structure is the same.
package wrapper

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"

	"github.com/gallowaysoftware/murmur/pkg/query/typed"
	"github.com/gallowaysoftware/murmur/proto/gen/murmur/v1/murmurv1connect"
)

// BotInteractionRequest is what the application's protobuf would define
// — here in Go for example purposes. Real code would use the
// generated proto types.
type BotInteractionRequest struct {
	BotID  string
	UserID string
}

// BotInteractionResponse is the typed response shape — no Value{bytes},
// no decoder boilerplate at the call site.
type BotInteractionResponse struct {
	Count   int64
	Present bool
}

// BotInteractionWindowRequest carries the windowed-query inputs.
type BotInteractionWindowRequest struct {
	BotID    string
	UserID   string
	Duration time.Duration
}

// BotInteractionService is the application's typed Connect-RPC server
// shape (here as a Go interface for illustration). Real code would use
// a generated `connect.UnaryHandlerFunc` or the connect-go server stub.
type BotInteractionService interface {
	GetBotInteractionCount(ctx context.Context, req *BotInteractionRequest) (*BotInteractionResponse, error)
	GetBotInteractionCountWindow(ctx context.Context, req *BotInteractionWindowRequest) (*BotInteractionResponse, error)
}

// Server is the count-core-shaped wrapper. It holds two typed Murmur
// clients — one for the all-time counter, one for the windowed counter
// — and exposes the application's typed RPC surface on top of them.
//
// In production, count-core's BotInteractionService server would
// embed this struct (or one with more fields for per-counter
// pipelines) and serve it via `connect.NewBotInteractionServiceHandler`.
type Server struct {
	// likes is the typed wrapper around Murmur's QueryService for the
	// all-time bot-interaction counter pipeline. Pipeline name in
	// Murmur: "bot_interactions_total".
	likes *typed.SumClient

	// windowed is the same shape but for the windowed counter pipeline
	// — typically a separate Murmur pipeline using daily windowing,
	// e.g. "bot_interactions_daily".
	windowed *typed.SumClient
}

// NewServer constructs the wrapper given the two underlying Murmur
// QueryService clients. In production you'd wire each client to its
// own gRPC endpoint (one Murmur worker per pipeline).
func NewServer(likes, windowed murmurv1connect.QueryServiceClient) *Server {
	return &Server{
		likes:    typed.NewSumClient(likes),
		windowed: typed.NewSumClient(windowed),
	}
}

// composeKey is the count-core-style entity-key composition: the
// application picks the key shape (here: "bot:<botID>|user:<userID>")
// and Murmur stores under that string. Murmur is monoid-agnostic;
// the application owns the key shape.
func composeKey(botID, userID string) string {
	if botID == "" || userID == "" {
		return ""
	}
	return "bot:" + botID + "|user:" + userID
}

// GetBotInteractionCount returns the all-time interaction count for a
// (bot, user) pair. The server-side wire shape is just this typed
// shape; Murmur's bytes layer is hidden from the caller.
func (s *Server) GetBotInteractionCount(ctx context.Context, req *BotInteractionRequest) (*BotInteractionResponse, error) {
	key := composeKey(req.BotID, req.UserID)
	if key == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("bot_id and user_id are required"))
	}
	count, present, err := s.likes.Get(ctx, key)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return &BotInteractionResponse{Count: count, Present: present}, nil
}

// GetBotInteractionCountWindow returns the count over the last
// `duration` for a (bot, user) pair. Wraps the underlying Murmur
// GetWindow call with the typed shape.
func (s *Server) GetBotInteractionCountWindow(ctx context.Context, req *BotInteractionWindowRequest) (*BotInteractionResponse, error) {
	key := composeKey(req.BotID, req.UserID)
	if key == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("bot_id and user_id are required"))
	}
	count, err := s.windowed.GetWindow(ctx, key, req.Duration)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	// GetWindow doesn't carry a present flag — windowed-empty is a
	// legitimate result (zero events in the range), not absence.
	// Application protocol decides how to expose this; here we always
	// return present=true with the merged count (which is 0 when no
	// events landed in the window).
	return &BotInteractionResponse{Count: count, Present: true}, nil
}

// Compile-time check.
var _ BotInteractionService = (*Server)(nil)
