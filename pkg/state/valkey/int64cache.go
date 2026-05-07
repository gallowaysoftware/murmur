// Package valkey provides Valkey-backed cache implementations of state.Cache.
//
// Murmur's invariant: DynamoDB is the source of truth, Valkey is a read-cache and
// sketch-accelerator. If Valkey is lost, every pipeline can rebuild its accelerator
// state from DDB via Repopulate. The cache must never be trusted as ground truth.
//
// Phase 1 ships Int64Cache — write-through cache for KindSum / KindCount pipelines
// using atomic INCRBY. Bytes/sketch caches with native Valkey HLL/Bloom acceleration
// are a Phase 2 task; the encoding boundary between axiomhq HLL and Valkey-native
// PFADD/PFCOUNT requires conversion logic flagged in the architecture doc.
package valkey

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/gallowaysoftware/murmur/pkg/state"
)

// Int64Cache is a state.Cache[int64] backed by Valkey. MergeUpdate uses INCRBY (atomic
// at the Valkey side); Get / GetMany use GET / MGET. The cache key encodes the (entity,
// bucket) pair as "<keyPrefix>:<entity>:<bucket>".
type Int64Cache struct {
	client    valkey.Client
	keyPrefix string
}

// Config configures an Int64Cache.
type Config struct {
	// Address is the Valkey server endpoint (e.g. "localhost:6379"). For ElastiCache
	// Serverless or cluster mode use the cluster endpoint.
	Address string

	// KeyPrefix is the namespace prefix for cache keys (e.g. "page_views"). Allows
	// multiple pipelines to share a Valkey instance without colliding.
	KeyPrefix string

	// Extra lets callers append additional valkey-go options.
	Extra []valkey.ClientOption
}

// NewInt64Cache constructs an Int64Cache. The returned Cache owns the underlying client;
// call Close to disconnect.
func NewInt64Cache(cfg Config) (*Int64Cache, error) {
	if cfg.KeyPrefix == "" {
		return nil, errors.New("valkey int64cache: KeyPrefix is required")
	}
	opts := valkey.ClientOption{
		InitAddress: []string{cfg.Address},
	}
	if cfg.Address == "" {
		opts.InitAddress = []string{"localhost:6379"}
	}
	client, err := valkey.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("valkey new client: %w", err)
	}
	return &Int64Cache{client: client, keyPrefix: cfg.KeyPrefix}, nil
}

// Get returns the cached value at k. Missing keys return 0, false, nil.
func (c *Int64Cache) Get(ctx context.Context, k state.Key) (int64, bool, error) {
	key := c.cacheKey(k)
	resp := c.client.Do(ctx, c.client.B().Get().Key(key).Build())
	if resp.Error() != nil {
		// valkey-go returns Nil error for missing keys.
		if valkey.IsValkeyNil(resp.Error()) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("valkey GET %s: %w", key, resp.Error())
	}
	s, err := resp.ToString()
	if err != nil {
		return 0, false, fmt.Errorf("valkey GET %s decode: %w", key, err)
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("valkey GET %s parse: %w", key, err)
	}
	return v, true, nil
}

// GetMany batches reads via MGET.
func (c *Int64Cache) GetMany(ctx context.Context, ks []state.Key) ([]int64, []bool, error) {
	if len(ks) == 0 {
		return nil, nil, nil
	}
	keys := make([]string, len(ks))
	for i, k := range ks {
		keys[i] = c.cacheKey(k)
	}
	resp := c.client.Do(ctx, c.client.B().Mget().Key(keys...).Build())
	if resp.Error() != nil {
		return nil, nil, fmt.Errorf("valkey MGET: %w", resp.Error())
	}
	arr, err := resp.ToArray()
	if err != nil {
		return nil, nil, fmt.Errorf("valkey MGET decode: %w", err)
	}
	vals := make([]int64, len(ks))
	oks := make([]bool, len(ks))
	for i, item := range arr {
		s, err := item.ToString()
		if err != nil {
			// Nil → missing key.
			continue
		}
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			continue
		}
		vals[i] = v
		oks[i] = true
	}
	return vals, oks, nil
}

// MergeUpdate atomically adds delta via INCRBY. If ttl > 0, also sets EXPIRE so the
// cached row decays naturally for windowed buckets.
func (c *Int64Cache) MergeUpdate(ctx context.Context, k state.Key, delta int64, ttl time.Duration) error {
	key := c.cacheKey(k)
	// INCRBY is atomic at Valkey; idempotent at the source-of-truth (DDB) level via
	// per-event-ID dedup. Cache may drift on Valkey loss but is repopulatable.
	resp := c.client.Do(ctx, c.client.B().Incrby().Key(key).Increment(delta).Build())
	if resp.Error() != nil {
		return fmt.Errorf("valkey INCRBY %s: %w", key, resp.Error())
	}
	if ttl > 0 {
		// Best-effort: failures here just leave the row to be evicted naturally on
		// next write. Don't propagate.
		_ = c.client.Do(ctx, c.client.B().Expire().Key(key).Seconds(int64(ttl.Seconds())).Build()).Error()
	}
	return nil
}

// Repopulate rebuilds cache entries from the authoritative store. Used after Valkey
// node loss or cold-start to rehydrate hot keys before serving queries.
func (c *Int64Cache) Repopulate(ctx context.Context, src state.Store[int64], keys []state.Key) error {
	if len(keys) == 0 {
		return nil
	}
	vals, oks, err := src.GetMany(ctx, keys)
	if err != nil {
		return fmt.Errorf("repopulate read: %w", err)
	}
	for i, k := range keys {
		if !oks[i] {
			// Skip missing source entries — leave cache empty.
			continue
		}
		key := c.cacheKey(k)
		if err := c.client.Do(ctx, c.client.B().Set().Key(key).Value(strconv.FormatInt(vals[i], 10)).Build()).Error(); err != nil {
			return fmt.Errorf("valkey SET %s: %w", key, err)
		}
	}
	return nil
}

// Close releases the underlying Valkey client.
func (c *Int64Cache) Close() error {
	c.client.Close()
	return nil
}

// cacheKey renders a state.Key as "<keyPrefix>:<entity>:<bucket>".
func (c *Int64Cache) cacheKey(k state.Key) string {
	var sb strings.Builder
	sb.Grow(len(c.keyPrefix) + len(k.Entity) + 24)
	sb.WriteString(c.keyPrefix)
	sb.WriteByte(':')
	sb.WriteString(k.Entity)
	sb.WriteByte(':')
	sb.WriteString(strconv.FormatInt(k.Bucket, 10))
	return sb.String()
}

// Compile-time check.
var _ state.Cache[int64] = (*Int64Cache)(nil)
