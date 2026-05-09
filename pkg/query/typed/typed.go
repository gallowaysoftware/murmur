// Package typed provides typed wrappers around the generic Murmur
// QueryService — the building block for application services that
// want to expose typed protos to their callers rather than the
// generic Value{bytes} shape.
//
// # Why this exists
//
// Murmur's `pkg/query/grpc.QueryService` returns `Value{present, data}`
// where `data` is the wire-encoded monoid value (int64-LE for
// Sum/Count/Min/Max, marshaled HLL/TopK/Bloom for sketches). Clients
// decode the bytes per the pipeline's monoid kind. This is right for
// Murmur — it keeps the service monoid-agnostic — but it pushes
// decoding boilerplate to every caller.
//
// The typed-wrapper pattern: an application service exposes its OWN
// Connect-RPC API with typed responses (e.g. count-core's
// `BotInteractionCountService.GetBotInteractionCount(...) → int64`).
// The implementation calls the underlying Murmur QueryService, decodes
// the bytes via this package, and returns the typed shape. Callers
// see typed protos; Murmur stays generic.
//
// This package ships the decoders + thin typed-client wrappers for the
// shipped monoid kinds: Sum/Count/Min/Max (int64), HLL, TopK, Bloom.
// Full per-pipeline codegen — generating a typed `*ServerStub` per
// pipeline — is Phase 2 roadmap; this package is the building block
// it would generate against.
//
// # Usage
//
// Wrap a generic Murmur QueryService client once:
//
//	rawClient := murmurv1connect.NewQueryServiceClient(httpClient, baseURL)
//	likes := typed.NewSumClient(rawClient)
//
//	// Now: typed Get / GetMany / GetWindow.
//	count, present, err := likes.Get(ctx, "post-123")
//	counts, err := likes.GetMany(ctx, []string{"post-123", "post-456"})
//	last24h, err := likes.GetWindow(ctx, "post-123", 24*time.Hour)
//
// Build your own Connect-RPC service against the typed client:
//
//	type CountCoreServer struct {
//	    likes  *typed.SumClient
//	    unique *typed.HLLClient
//	}
//	func (s *CountCoreServer) GetBotInteractionCount(ctx, req) (resp, error) {
//	    n, ok, err := s.likes.Get(ctx, req.GetEntity())
//	    if err != nil { return nil, connect.NewError(...) }
//	    return &countpb.GetBotInteractionCountResponse{
//	        Count: n,
//	        Present: ok,
//	    }, nil
//	}
//
// The application's Connect-RPC service is whatever proto shape it
// wants; the typed client hides the bytes-decoding from it.
package typed

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"

	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/bloom"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/hll"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/topk"
	pb "github.com/gallowaysoftware/murmur/proto/gen/murmur/v1"
	"github.com/gallowaysoftware/murmur/proto/gen/murmur/v1/murmurv1connect"
)

// Inner is the underlying Connect-RPC client shape this package wraps.
// Production: the generated `murmurv1connect.QueryServiceClient`.
// Tests: a fake satisfying the same shape.
type Inner = murmurv1connect.QueryServiceClient

// ----------------------------------------------------------------------------
// Sum / Count / Min / Max — int64 wire shape
// ----------------------------------------------------------------------------

// SumClient is the typed wrapper for Sum / Count / Min / Max pipelines
// whose state is `int64` and whose wire shape is 8-byte little-endian.
// The same client works against any of those four monoid kinds — they
// all encode int64 the same way.
type SumClient struct {
	inner Inner
}

// NewSumClient wraps the generic QueryService client with typed int64
// responses. Use for any pipeline whose monoid is Sum / Count / Min /
// Max.
func NewSumClient(inner Inner) *SumClient {
	return &SumClient{inner: inner}
}

// Get returns the all-time aggregation value for entity. The bool
// reports whether the entity exists in the store; missing entities
// return (0, false, nil). Use this for non-windowed pipelines.
func (c *SumClient) Get(ctx context.Context, entity string, opts ...Option) (int64, bool, error) {
	o := applyOpts(opts)
	resp, err := c.inner.Get(ctx, connect.NewRequest(&pb.GetRequest{
		Entity:    entity,
		FreshRead: o.freshRead,
	}))
	if err != nil {
		return 0, false, err
	}
	v := resp.Msg.GetValue()
	if !v.GetPresent() {
		return 0, false, nil
	}
	return DecodeInt64(v.GetData()), true, nil
}

// GetMany batches Get for many entities. Order matches input order;
// missing entities return 0 in the value slice with the corresponding
// bool false in the present slice.
func (c *SumClient) GetMany(ctx context.Context, entities []string, opts ...Option) ([]int64, []bool, error) {
	o := applyOpts(opts)
	resp, err := c.inner.GetMany(ctx, connect.NewRequest(&pb.GetManyRequest{
		Entities:  entities,
		FreshRead: o.freshRead,
	}))
	if err != nil {
		return nil, nil, err
	}
	values := resp.Msg.GetValues()
	out := make([]int64, len(entities))
	present := make([]bool, len(entities))
	for i, v := range values {
		if v.GetPresent() {
			out[i] = DecodeInt64(v.GetData())
			present[i] = true
		}
	}
	return out, present, nil
}

// GetWindow returns the merged value over the most-recent `duration`
// for the given entity. Pipeline must be windowed.
func (c *SumClient) GetWindow(ctx context.Context, entity string, duration time.Duration, opts ...Option) (int64, error) {
	o := applyOpts(opts)
	resp, err := c.inner.GetWindow(ctx, connect.NewRequest(&pb.GetWindowRequest{
		Entity:          entity,
		DurationSeconds: int64(duration / time.Second),
		FreshRead:       o.freshRead,
	}))
	if err != nil {
		return 0, err
	}
	return DecodeInt64(resp.Msg.GetValue().GetData()), nil
}

// GetWindowMany is the batched analog of GetWindow — fetches windowed
// values for N entities in one round-trip.
func (c *SumClient) GetWindowMany(ctx context.Context, entities []string, duration time.Duration, opts ...Option) ([]int64, error) {
	o := applyOpts(opts)
	resp, err := c.inner.GetWindowMany(ctx, connect.NewRequest(&pb.GetWindowManyRequest{
		Entities:        entities,
		DurationSeconds: int64(duration / time.Second),
		FreshRead:       o.freshRead,
	}))
	if err != nil {
		return nil, err
	}
	values := resp.Msg.GetValues()
	out := make([]int64, len(entities))
	for i, v := range values {
		if i < len(values) {
			out[i] = DecodeInt64(v.GetData())
		}
	}
	return out, nil
}

// GetRange returns the merged value over the absolute time range
// [start, end] for the given entity. Pipeline must be windowed.
func (c *SumClient) GetRange(ctx context.Context, entity string, start, end time.Time, opts ...Option) (int64, error) {
	o := applyOpts(opts)
	resp, err := c.inner.GetRange(ctx, connect.NewRequest(&pb.GetRangeRequest{
		Entity:    entity,
		StartUnix: start.Unix(),
		EndUnix:   end.Unix(),
		FreshRead: o.freshRead,
	}))
	if err != nil {
		return 0, err
	}
	return DecodeInt64(resp.Msg.GetValue().GetData()), nil
}

// ----------------------------------------------------------------------------
// HLL — sketch wire shape
// ----------------------------------------------------------------------------

// HLLClient is the typed wrapper for HLL pipelines whose state is the
// axiomhq/hyperloglog byte format. Returns cardinality estimates rather
// than raw sketch bytes.
type HLLClient struct {
	inner Inner
}

// NewHLLClient wraps the generic QueryService client with HLL
// cardinality-estimate responses.
func NewHLLClient(inner Inner) *HLLClient {
	return &HLLClient{inner: inner}
}

// HLLValue carries the estimated cardinality plus the byte length of
// the underlying sketch (useful for cluster-wide capacity tracking).
type HLLValue struct {
	Cardinality uint64
	ByteLen     int
}

// Get returns the cardinality estimate for the entity's all-time
// sketch.
func (c *HLLClient) Get(ctx context.Context, entity string, opts ...Option) (HLLValue, bool, error) {
	o := applyOpts(opts)
	resp, err := c.inner.Get(ctx, connect.NewRequest(&pb.GetRequest{
		Entity: entity, FreshRead: o.freshRead,
	}))
	if err != nil {
		return HLLValue{}, false, err
	}
	v := resp.Msg.GetValue()
	if !v.GetPresent() {
		return HLLValue{}, false, nil
	}
	est, err := hll.Estimate(v.GetData())
	if err != nil {
		return HLLValue{}, false, fmt.Errorf("typed HLL decode: %w", err)
	}
	return HLLValue{Cardinality: est, ByteLen: len(v.GetData())}, true, nil
}

// GetWindow returns the cardinality estimate over the most-recent
// `duration`.
func (c *HLLClient) GetWindow(ctx context.Context, entity string, duration time.Duration, opts ...Option) (HLLValue, error) {
	o := applyOpts(opts)
	resp, err := c.inner.GetWindow(ctx, connect.NewRequest(&pb.GetWindowRequest{
		Entity: entity, DurationSeconds: int64(duration / time.Second), FreshRead: o.freshRead,
	}))
	if err != nil {
		return HLLValue{}, err
	}
	data := resp.Msg.GetValue().GetData()
	est, err := hll.Estimate(data)
	if err != nil {
		return HLLValue{}, fmt.Errorf("typed HLL decode: %w", err)
	}
	return HLLValue{Cardinality: est, ByteLen: len(data)}, nil
}

// GetWindowMany is the batched analog of GetWindow — fetches windowed
// cardinalities for N entities in one round-trip. Returns parallel
// arrays: out[i] is the cardinality estimate for entities[i]; missing
// entities yield a zero-valued HLLValue.
func (c *HLLClient) GetWindowMany(ctx context.Context, entities []string, duration time.Duration, opts ...Option) ([]HLLValue, error) {
	o := applyOpts(opts)
	resp, err := c.inner.GetWindowMany(ctx, connect.NewRequest(&pb.GetWindowManyRequest{
		Entities:        entities,
		DurationSeconds: int64(duration / time.Second),
		FreshRead:       o.freshRead,
	}))
	if err != nil {
		return nil, err
	}
	values := resp.Msg.GetValues()
	out := make([]HLLValue, len(entities))
	for i, v := range values {
		if i >= len(out) {
			break
		}
		data := v.GetData()
		if len(data) == 0 {
			continue
		}
		est, err := hll.Estimate(data)
		if err != nil {
			return nil, fmt.Errorf("typed HLL[%d] decode: %w", i, err)
		}
		out[i] = HLLValue{Cardinality: est, ByteLen: len(data)}
	}
	return out, nil
}

// ----------------------------------------------------------------------------
// TopK — Misra-Gries wire shape
// ----------------------------------------------------------------------------

// TopKClient is the typed wrapper for Misra-Gries Top-K pipelines.
// Returns ranked items rather than raw sketch bytes.
type TopKClient struct {
	inner Inner
}

// NewTopKClient wraps the generic QueryService client with TopK
// item-list responses.
func NewTopKClient(inner Inner) *TopKClient {
	return &TopKClient{inner: inner}
}

// TopKItem is one (key, count) pair from a Misra-Gries summary.
type TopKItem struct {
	Key   string
	Count uint64
}

// Get returns the ranked top-K items for the entity's all-time sketch.
// Items are ordered by descending count.
func (c *TopKClient) Get(ctx context.Context, entity string, opts ...Option) ([]TopKItem, bool, error) {
	o := applyOpts(opts)
	resp, err := c.inner.Get(ctx, connect.NewRequest(&pb.GetRequest{
		Entity: entity, FreshRead: o.freshRead,
	}))
	if err != nil {
		return nil, false, err
	}
	v := resp.Msg.GetValue()
	if !v.GetPresent() {
		return nil, false, nil
	}
	items, err := topk.Items(v.GetData())
	if err != nil {
		return nil, false, fmt.Errorf("typed TopK decode: %w", err)
	}
	out := make([]TopKItem, len(items))
	for i, it := range items {
		out[i] = TopKItem{Key: it.Key, Count: it.Count}
	}
	return out, true, nil
}

// GetWindow returns the ranked top-K items over the most-recent
// `duration`.
func (c *TopKClient) GetWindow(ctx context.Context, entity string, duration time.Duration, opts ...Option) ([]TopKItem, error) {
	o := applyOpts(opts)
	resp, err := c.inner.GetWindow(ctx, connect.NewRequest(&pb.GetWindowRequest{
		Entity: entity, DurationSeconds: int64(duration / time.Second), FreshRead: o.freshRead,
	}))
	if err != nil {
		return nil, err
	}
	items, err := topk.Items(resp.Msg.GetValue().GetData())
	if err != nil {
		return nil, fmt.Errorf("typed TopK decode: %w", err)
	}
	out := make([]TopKItem, len(items))
	for i, it := range items {
		out[i] = TopKItem{Key: it.Key, Count: it.Count}
	}
	return out, nil
}

// GetWindowMany is the batched analog of GetWindow — fetches windowed
// ranked-item lists for N entities in one round-trip. Returns parallel
// arrays: out[i] is the ranked items for entities[i]; missing entities
// yield a nil slice.
func (c *TopKClient) GetWindowMany(ctx context.Context, entities []string, duration time.Duration, opts ...Option) ([][]TopKItem, error) {
	o := applyOpts(opts)
	resp, err := c.inner.GetWindowMany(ctx, connect.NewRequest(&pb.GetWindowManyRequest{
		Entities:        entities,
		DurationSeconds: int64(duration / time.Second),
		FreshRead:       o.freshRead,
	}))
	if err != nil {
		return nil, err
	}
	values := resp.Msg.GetValues()
	out := make([][]TopKItem, len(entities))
	for i, v := range values {
		if i >= len(out) {
			break
		}
		data := v.GetData()
		if len(data) == 0 {
			continue
		}
		items, err := topk.Items(data)
		if err != nil {
			return nil, fmt.Errorf("typed TopK[%d] decode: %w", i, err)
		}
		entry := make([]TopKItem, len(items))
		for j, it := range items {
			entry[j] = TopKItem{Key: it.Key, Count: it.Count}
		}
		out[i] = entry
	}
	return out, nil
}

// ----------------------------------------------------------------------------
// Bloom — sketch shape
// ----------------------------------------------------------------------------

// BloomClient is the typed wrapper for Bloom-filter pipelines.
type BloomClient struct {
	inner Inner
}

// NewBloomClient wraps the generic QueryService client with Bloom
// approximate-size responses.
func NewBloomClient(inner Inner) *BloomClient {
	return &BloomClient{inner: inner}
}

// BloomValue carries the Bloom filter's shape — capacity (bits), the
// number of hash functions, and the approximate set size derived from
// the populated bit count.
type BloomValue struct {
	CapacityBits  uint64
	HashFunctions uint32
	ApproxSize    uint64
}

// Get returns the Bloom filter's shape for the entity's all-time sketch.
func (c *BloomClient) Get(ctx context.Context, entity string, opts ...Option) (BloomValue, bool, error) {
	o := applyOpts(opts)
	resp, err := c.inner.Get(ctx, connect.NewRequest(&pb.GetRequest{
		Entity: entity, FreshRead: o.freshRead,
	}))
	if err != nil {
		return BloomValue{}, false, err
	}
	v := resp.Msg.GetValue()
	if !v.GetPresent() {
		return BloomValue{}, false, nil
	}
	capacity, hashes, approxSize, err := bloom.Inspect(v.GetData())
	if err != nil {
		return BloomValue{}, false, fmt.Errorf("typed Bloom decode: %w", err)
	}
	return BloomValue{
		CapacityBits:  capacity,
		HashFunctions: hashes,
		ApproxSize:    approxSize,
	}, true, nil
}

// GetWindow returns the merged Bloom-filter shape over the most-recent
// `duration`. The merged sketch's capacity / hash count match the
// per-bucket sketches' shape; ApproxSize reflects the union over the
// window's buckets.
func (c *BloomClient) GetWindow(ctx context.Context, entity string, duration time.Duration, opts ...Option) (BloomValue, error) {
	o := applyOpts(opts)
	resp, err := c.inner.GetWindow(ctx, connect.NewRequest(&pb.GetWindowRequest{
		Entity: entity, DurationSeconds: int64(duration / time.Second), FreshRead: o.freshRead,
	}))
	if err != nil {
		return BloomValue{}, err
	}
	data := resp.Msg.GetValue().GetData()
	if len(data) == 0 {
		return BloomValue{}, nil
	}
	capacity, hashes, approxSize, err := bloom.Inspect(data)
	if err != nil {
		return BloomValue{}, fmt.Errorf("typed Bloom decode: %w", err)
	}
	return BloomValue{
		CapacityBits:  capacity,
		HashFunctions: hashes,
		ApproxSize:    approxSize,
	}, nil
}

// GetWindowMany is the batched analog of GetWindow — fetches the
// merged Bloom-filter shape for N entities in one round-trip. Returns
// parallel arrays: out[i] is the BloomValue for entities[i]; missing
// entities yield a zero-valued BloomValue.
func (c *BloomClient) GetWindowMany(ctx context.Context, entities []string, duration time.Duration, opts ...Option) ([]BloomValue, error) {
	o := applyOpts(opts)
	resp, err := c.inner.GetWindowMany(ctx, connect.NewRequest(&pb.GetWindowManyRequest{
		Entities:        entities,
		DurationSeconds: int64(duration / time.Second),
		FreshRead:       o.freshRead,
	}))
	if err != nil {
		return nil, err
	}
	values := resp.Msg.GetValues()
	out := make([]BloomValue, len(entities))
	for i, v := range values {
		if i >= len(out) {
			break
		}
		data := v.GetData()
		if len(data) == 0 {
			continue
		}
		capacity, hashes, approxSize, err := bloom.Inspect(data)
		if err != nil {
			return nil, fmt.Errorf("typed Bloom[%d] decode: %w", i, err)
		}
		out[i] = BloomValue{
			CapacityBits:  capacity,
			HashFunctions: hashes,
			ApproxSize:    approxSize,
		}
	}
	return out, nil
}

// ----------------------------------------------------------------------------
// Decoders (shared, exported for callers that don't want the typed client)
// ----------------------------------------------------------------------------

// DecodeInt64 decodes an 8-byte little-endian int64. Mirrors the
// `mgrpc.Int64LE` encoder used server-side. Returns 0 for short inputs.
func DecodeInt64(data []byte) int64 {
	if len(data) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(data))
}

// ----------------------------------------------------------------------------
// Options
// ----------------------------------------------------------------------------

// Option configures a typed Get / GetMany / GetWindow / GetRange call.
type Option func(*queryOptions)

type queryOptions struct {
	freshRead bool
}

// WithFreshRead sets the underlying request's fresh_read flag,
// bypassing server-side singleflight + cache layers. Used for
// read-your-writes flows.
func WithFreshRead() Option {
	return func(o *queryOptions) { o.freshRead = true }
}

func applyOpts(opts []Option) queryOptions {
	var o queryOptions
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// ErrUnsupported is returned by adapters that don't implement an
// optional method (e.g., calling GetWindow on a SumClient that only
// wraps a non-windowed pipeline). Wrapped via fmt.Errorf at the call
// site.
var ErrUnsupported = errors.New("typed client: operation not supported by underlying pipeline")
