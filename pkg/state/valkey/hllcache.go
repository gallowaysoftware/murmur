package valkey

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	rueidis "github.com/redis/rueidis"

	"github.com/gallowaysoftware/murmur/pkg/state"
)

// HLLCache is a Valkey-native HyperLogLog accelerator, exposing the
// PFADD / PFCOUNT / PFMERGE primitives as a per-entity API.
//
// # Why this exists
//
// Murmur's BytesStore-authoritative HLL pipeline marshals
// axiomhq/hyperloglog sketches as opaque bytes into DDB. Reading
// cardinality requires:
//
//  1. DDB GetItem (5–15ms cold)
//  2. axiomhq UnmarshalBinary
//  3. Estimate()
//
// For dashboard-class queries this is fine. For hot-path queries
// (e.g. "show the unique-visitor count for this page on today's
// dashboard, refreshed every 2s"), it is not. Valkey-native PFCOUNT
// is single-digit ms and runs entirely server-side.
//
// # The side-by-side pattern
//
// Pipelines using the accelerator emit each event element to BOTH
// the BytesStore (as an axiomhq Single() sketch merged via
// MergeUpdate) AND to this HLLCache via Add. The two HLLs are
// independent estimators of the same set: each sits within HLL's
// ~1.6% standard error against true cardinality, with INDEPENDENT
// error realizations. This is acceptable for approximate-cardinality
// queries; if your application requires the two paths to agree
// bit-for-bit, this accelerator is not the right tool — use
// BytesCache instead, which stores the same axiomhq bytes the
// BytesStore stores and does the merge in Go.
//
// # Recovery
//
// On Valkey loss, the accelerator can only be repopulated by
// re-feeding events. There is no axiomhq → Valkey HYLL byte
// conversion path (axiomhq doesn't expose its register array, and
// Valkey's HLL encoding is incompatible with axiomhq's). If your
// pipeline cannot replay events, treat this as best-effort cache
// and route hot reads to the accelerator opportunistically with a
// fall-back to BytesStore on Valkey miss / unavailability.
//
// # Key layout
//
// Cache keys are "<keyPrefix>:<entity>:<bucket>" — same scheme as
// Int64Cache and BytesCache. This means a Windowed[HLL] pipeline's
// per-bucket accelerator keys are stable across restarts.
type HLLCache struct {
	client    rueidis.Client
	keyPrefix string
}

// HLLConfig configures an HLLCache.
type HLLConfig struct {
	// Address is the Valkey server endpoint. Defaults to "localhost:6379".
	Address string

	// KeyPrefix namespaces this accelerator's keys (e.g. "page_unique_visitors").
	// Required.
	KeyPrefix string

	// Extra lets callers append additional rueidis options (TLS, auth, etc).
	Extra []rueidis.ClientOption
}

// NewHLLCache constructs an HLLCache. The returned cache owns the
// underlying Valkey client; call Close to disconnect.
func NewHLLCache(cfg HLLConfig) (*HLLCache, error) {
	if cfg.KeyPrefix == "" {
		return nil, errors.New("valkey HLLCache: KeyPrefix is required")
	}
	opts := rueidis.ClientOption{InitAddress: []string{cfg.Address}}
	if cfg.Address == "" {
		opts.InitAddress = []string{"localhost:6379"}
	}
	client, err := rueidis.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("valkey new client: %w", err)
	}
	return &HLLCache{client: client, keyPrefix: cfg.KeyPrefix}, nil
}

// Add records that element was observed for entity k via PFADD.
// Concurrent Adds on the same key are safe — Valkey serializes them
// internally. If ttl > 0, the entry's expiration is set via EXPIRE
// (best-effort; failures don't propagate, since the next Add will
// re-set it).
//
// Mirrors the pipeline-runtime contract: the streaming worker calls
// Add per event after a successful BytesStore.MergeUpdate, in the
// same hot path. A failed Add is non-fatal (the BytesStore is the
// source of truth) but should be logged and counted via metrics.
func (c *HLLCache) Add(ctx context.Context, k state.Key, element []byte, ttl time.Duration) error {
	key := c.cacheKey(k)
	resp := c.client.Do(ctx, c.client.B().Pfadd().Key(key).Element(string(element)).Build())
	if resp.Error() != nil {
		return fmt.Errorf("valkey PFADD %s: %w", key, resp.Error())
	}
	if ttl > 0 {
		_ = c.client.Do(ctx, c.client.B().Expire().Key(key).Seconds(int64(ttl.Seconds())).Build()).Error()
	}
	return nil
}

// AddMany records multiple elements for the same entity in one round
// trip. PFADD is variadic in the wire protocol, so this is one
// command — useful for back-pressured batched workers and for
// bootstrap drivers that fold many events per key.
func (c *HLLCache) AddMany(ctx context.Context, k state.Key, elements [][]byte, ttl time.Duration) error {
	if len(elements) == 0 {
		return nil
	}
	key := c.cacheKey(k)
	strs := make([]string, len(elements))
	for i, e := range elements {
		strs[i] = string(e)
	}
	resp := c.client.Do(ctx, c.client.B().Pfadd().Key(key).Element(strs...).Build())
	if resp.Error() != nil {
		return fmt.Errorf("valkey PFADD %s (n=%d): %w", key, len(elements), resp.Error())
	}
	if ttl > 0 {
		_ = c.client.Do(ctx, c.client.B().Expire().Key(key).Seconds(int64(ttl.Seconds())).Build()).Error()
	}
	return nil
}

// Cardinality returns PFCOUNT for entity k. Missing keys return
// (0, false, nil) — Valkey's PFCOUNT on a missing key returns 0
// without error, but we surface the present/absent distinction
// explicitly via EXISTS.
func (c *HLLCache) Cardinality(ctx context.Context, k state.Key) (uint64, bool, error) {
	key := c.cacheKey(k)
	exists := c.client.Do(ctx, c.client.B().Exists().Key(key).Build())
	if exists.Error() != nil {
		return 0, false, fmt.Errorf("valkey EXISTS %s: %w", key, exists.Error())
	}
	n, err := exists.AsInt64()
	if err != nil {
		return 0, false, fmt.Errorf("valkey EXISTS %s decode: %w", key, err)
	}
	if n == 0 {
		return 0, false, nil
	}
	resp := c.client.Do(ctx, c.client.B().Pfcount().Key(key).Build())
	if resp.Error() != nil {
		return 0, false, fmt.Errorf("valkey PFCOUNT %s: %w", key, resp.Error())
	}
	v, err := resp.AsInt64()
	if err != nil {
		return 0, false, fmt.Errorf("valkey PFCOUNT %s decode: %w", key, err)
	}
	return uint64(v), true, nil
}

// CardinalityMany pipelines per-key PFCOUNT calls. Use
// CardinalityOver if you want the union cardinality across keys
// rather than per-key estimates.
func (c *HLLCache) CardinalityMany(ctx context.Context, ks []state.Key) ([]uint64, []bool, error) {
	vals := make([]uint64, len(ks))
	oks := make([]bool, len(ks))
	for i, k := range ks {
		v, ok, err := c.Cardinality(ctx, k)
		if err != nil {
			return nil, nil, err
		}
		vals[i] = v
		oks[i] = ok
	}
	return vals, oks, nil
}

// CardinalityOver returns PFCOUNT k1 k2 ... — Valkey's union
// cardinality across multiple keys, computed server-side without
// materializing an intermediate sketch. The canonical use case:
// "how many distinct visitors across this entity's last N daily
// buckets" against a Windowed[HLL] pipeline.
//
// Returns 0 (without error) if all keys are missing — Valkey's
// PFCOUNT semantics treat missing keys as empty sketches.
func (c *HLLCache) CardinalityOver(ctx context.Context, ks []state.Key) (uint64, error) {
	if len(ks) == 0 {
		return 0, nil
	}
	keys := make([]string, len(ks))
	for i, k := range ks {
		keys[i] = c.cacheKey(k)
	}
	resp := c.client.Do(ctx, c.client.B().Pfcount().Key(keys...).Build())
	if resp.Error() != nil {
		return 0, fmt.Errorf("valkey PFCOUNT union (n=%d): %w", len(keys), resp.Error())
	}
	v, err := resp.AsInt64()
	if err != nil {
		return 0, fmt.Errorf("valkey PFCOUNT union decode: %w", err)
	}
	return uint64(v), nil
}

// MergeInto computes PFMERGE dst <- srcs..., storing the merged HLL
// at dst. If ttl > 0, an EXPIRE is set on dst.
//
// Use case: pre-compute a "last-7-days" rollup HLL once per day from
// 7 daily buckets, then serve dashboard reads against the rollup
// with one PFCOUNT instead of 7. The trade-off is staleness — the
// rollup is N hours behind real-time depending on rebuild cadence.
//
// Note: PFMERGE writes a new HLL at dst, replacing whatever was
// there. If dst is one of srcs, Valkey handles it correctly (the
// destination is read first).
func (c *HLLCache) MergeInto(ctx context.Context, dst state.Key, srcs []state.Key, ttl time.Duration) error {
	if len(srcs) == 0 {
		return errors.New("valkey HLLCache MergeInto: srcs is empty")
	}
	dstKey := c.cacheKey(dst)
	srcKeys := make([]string, len(srcs))
	for i, k := range srcs {
		srcKeys[i] = c.cacheKey(k)
	}
	resp := c.client.Do(ctx, c.client.B().Pfmerge().Destkey(dstKey).Sourcekey(srcKeys...).Build())
	if resp.Error() != nil {
		return fmt.Errorf("valkey PFMERGE %s <- (n=%d): %w", dstKey, len(srcKeys), resp.Error())
	}
	if ttl > 0 {
		_ = c.client.Do(ctx, c.client.B().Expire().Key(dstKey).Seconds(int64(ttl.Seconds())).Build()).Error()
	}
	return nil
}

// Close releases the underlying Valkey client.
func (c *HLLCache) Close() error {
	c.client.Close()
	return nil
}

// cacheKey mirrors Int64Cache / BytesCache layout: "<keyPrefix>:<entity>:<bucket>".
func (c *HLLCache) cacheKey(k state.Key) string {
	var sb strings.Builder
	sb.Grow(len(c.keyPrefix) + len(k.Entity) + 24)
	sb.WriteString(c.keyPrefix)
	sb.WriteByte(':')
	sb.WriteString(k.Entity)
	sb.WriteByte(':')
	sb.WriteString(strconv.FormatInt(k.Bucket, 10))
	return sb.String()
}
