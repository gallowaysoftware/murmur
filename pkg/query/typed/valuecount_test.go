package typed_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/hll"
	"github.com/gallowaysoftware/murmur/pkg/query/typed"
	pb "github.com/gallowaysoftware/murmur/proto/gen/murmur/v1"
)

// overcountingClient is a QueryService client that answers every batched RPC with
// `values` regardless of how many entities were asked about. A real server should
// never do this — which is exactly why the clients trusted it and indexed the
// caller's output slice off the response length.
type overcountingClient struct {
	values []*pb.Value
}

func int64Value(n int64) *pb.Value {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(n))
	return &pb.Value{Present: true, Data: b}
}

func sketchValue(t *testing.T) *pb.Value {
	t.Helper()
	return &pb.Value{Present: true, Data: hll.Single([]byte("x"))}
}

func (c *overcountingClient) Get(context.Context, *connect.Request[pb.GetRequest]) (*connect.Response[pb.GetResponse], error) {
	return connect.NewResponse(&pb.GetResponse{Value: c.values[0]}), nil
}

func (c *overcountingClient) GetWindow(context.Context, *connect.Request[pb.GetWindowRequest]) (*connect.Response[pb.GetWindowResponse], error) {
	return connect.NewResponse(&pb.GetWindowResponse{Value: c.values[0]}), nil
}

func (c *overcountingClient) GetRange(context.Context, *connect.Request[pb.GetRangeRequest]) (*connect.Response[pb.GetRangeResponse], error) {
	return connect.NewResponse(&pb.GetRangeResponse{Value: c.values[0]}), nil
}

func (c *overcountingClient) GetMany(context.Context, *connect.Request[pb.GetManyRequest]) (*connect.Response[pb.GetManyResponse], error) {
	return connect.NewResponse(&pb.GetManyResponse{Values: c.values}), nil
}

func (c *overcountingClient) GetWindowMany(context.Context, *connect.Request[pb.GetWindowManyRequest]) (*connect.Response[pb.GetWindowManyResponse], error) {
	return connect.NewResponse(&pb.GetWindowManyResponse{Values: c.values}), nil
}

func (c *overcountingClient) GetRangeMany(context.Context, *connect.Request[pb.GetRangeManyRequest]) (*connect.Response[pb.GetRangeManyResponse], error) {
	return connect.NewResponse(&pb.GetRangeManyResponse{Values: c.values}), nil
}

func (c *overcountingClient) GetTrailing(context.Context, *connect.Request[pb.GetTrailingRequest]) (*connect.Response[pb.GetTrailingResponse], error) {
	return connect.NewResponse(&pb.GetTrailingResponse{Value: c.values[0]}), nil
}

func (c *overcountingClient) GetTrailingMany(context.Context, *connect.Request[pb.GetTrailingManyRequest]) (*connect.Response[pb.GetTrailingManyResponse], error) {
	return connect.NewResponse(&pb.GetTrailingManyResponse{Values: c.values}), nil
}

// TestSumClient_RejectsValueCountMismatch pins the fix for a remotely triggerable
// panic: SumClient sized `out` from the entity list but wrote out[i] for every
// value the SERVER returned, so three values for two entities panicked with
// index-out-of-range inside the calling application.
func TestSumClient_RejectsValueCountMismatch(t *testing.T) {
	inner := &overcountingClient{values: []*pb.Value{int64Value(1), int64Value(2), int64Value(3)}}
	c := typed.NewSumClient(inner)
	entities := []string{"a", "b"}
	ctx := context.Background()

	if _, _, err := c.GetMany(ctx, entities); err == nil {
		t.Error("GetMany: got nil error for 3 values over 2 entities, want a rejection")
	}
	if _, err := c.GetWindowMany(ctx, entities, 24*time.Hour); err == nil {
		t.Error("GetWindowMany: got nil error for 3 values over 2 entities, want a rejection")
	}
}

// TestHLLClient_RejectsValueCountMismatch covers the other half of the same
// defect. The sketch clients guarded the index with `if i >= len(out) { break }`,
// which stopped the panic but silently truncated instead — a short answer that
// still reported success. This asserts the mismatch is now surfaced.
func TestHLLClient_RejectsValueCountMismatch(t *testing.T) {
	v := sketchValue(t)
	ctx := context.Background()

	t.Run("too many values", func(t *testing.T) {
		c := typed.NewHLLClient(&overcountingClient{values: []*pb.Value{v, v, v}})
		if _, _, err := c.GetMany(ctx, []string{"a", "b"}); err == nil {
			t.Error("GetMany: got nil error for 3 values over 2 entities, want a rejection")
		}
		if _, err := c.GetWindowMany(ctx, []string{"a", "b"}, 24*time.Hour); err == nil {
			t.Error("GetWindowMany: got nil error for 3 values over 2 entities, want a rejection")
		}
	})

	t.Run("too few values", func(t *testing.T) {
		c := typed.NewHLLClient(&overcountingClient{values: []*pb.Value{v}})
		if _, err := c.GetWindowMany(ctx, []string{"a", "b", "c"}, 24*time.Hour); err == nil {
			t.Error("GetWindowMany: got nil error for 1 value over 3 entities, want a rejection")
		}
	})
}

func TestTopKClient_RejectsValueCountMismatch(t *testing.T) {
	inner := &overcountingClient{values: []*pb.Value{{Present: true}, {Present: true}, {Present: true}}}
	c := typed.NewTopKClient(inner)
	if _, _, err := c.GetMany(context.Background(), []string{"a"}); err == nil {
		t.Error("GetMany: got nil error for 3 values over 1 entity, want a rejection")
	}
}

func TestBloomClient_RejectsValueCountMismatch(t *testing.T) {
	inner := &overcountingClient{values: []*pb.Value{{Present: true}, {Present: true}, {Present: true}}}
	c := typed.NewBloomClient(inner)
	if _, _, err := c.GetMany(context.Background(), []string{"a"}); err == nil {
		t.Error("GetMany: got nil error for 3 values over 1 entity, want a rejection")
	}
}
