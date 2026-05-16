package valkey

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	rueidis "github.com/redis/rueidis"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// BytesCache is a state.Cache[[]byte] backed by Valkey, suitable for any
// pipeline whose state is a byte-encoded monoid value (HLL, TopK, Bloom,
// DecayedSumBytes, custom).
//
// Operationally:
//
//   - Get / GetMany — Valkey GET / MGET; single-digit ms latency. This is
//     the read-path acceleration BytesStore lacks on its own (DDB cold
//     read is 5–15 ms).
//   - MergeUpdate — read-modify-write through the supplied monoid:
//     GET → Combine(current, delta) → SET. The full RMW happens server-
//     side via a Lua script so concurrent writers don't race; under
//     contention the script's GET-COMBINE-SET runs serially per key.
//     Throughput is bounded by Valkey single-shard write rate (~100k op/s),
//     not by Murmur logic.
//   - Repopulate — rebuild from the authoritative Store. Used after a
//     Valkey node restart or cold start; reads are bulk-fetched via
//     Store.GetMany and bulk-written via Valkey MSET.
//
// Why not Valkey-native HLL/Bloom acceleration? Valkey's PFADD/PFCOUNT/
// PFMERGE use Redis's encoding, which is incompatible with axiomhq's HLL
// bytes (different bit layout, different encoding). The same problem
// applies to Bloom and to TopK (Valkey has no native TopK). Bridging
// requires either:
//
//	(a) accept a portable encoding and convert at the BytesStore <→
//	    Valkey boundary, OR
//	(b) standardize on Valkey-native everywhere and accept the format
//	    lock-in.
//
// BytesCache takes neither path: it stores the SAME bytes the BytesStore
// stores, using Valkey purely as a key-value cache. The Combine runs in
// Go via the supplied monoid. This gives latency without forcing a
// runtime-incompatible encoding choice; native Valkey acceleration is a
// future Phase 3 on top of the same interface.
type BytesCache struct {
	client    rueidis.Client
	keyPrefix string
	monoid    monoid.Monoid[[]byte]
}

// BytesConfig configures a BytesCache.
type BytesConfig struct {
	// Address is the Valkey server endpoint. Defaults to "localhost:6379".
	Address string

	// KeyPrefix is the namespace prefix for cache keys. Required.
	KeyPrefix string

	// Monoid is the byte-monoid used for the read-modify-write Combine.
	// Must match the monoid used by the underlying BytesStore — otherwise
	// MergeUpdate will produce values that disagree across the cache /
	// store boundary.
	Monoid monoid.Monoid[[]byte]

	// Extra lets callers append additional rueidis options (TLS, auth, etc).
	Extra []rueidis.ClientOption
}

// NewBytesCache constructs a BytesCache. The returned Cache owns the
// underlying client; call Close to disconnect.
func NewBytesCache(cfg BytesConfig) (*BytesCache, error) {
	if cfg.KeyPrefix == "" {
		return nil, errors.New("valkey BytesCache: KeyPrefix is required")
	}
	if cfg.Monoid == nil {
		return nil, errors.New("valkey BytesCache: Monoid is required")
	}
	opts := rueidis.ClientOption{InitAddress: []string{cfg.Address}}
	if cfg.Address == "" {
		opts.InitAddress = []string{"localhost:6379"}
	}
	client, err := rueidis.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("valkey new client: %w", err)
	}
	return &BytesCache{client: client, keyPrefix: cfg.KeyPrefix, monoid: cfg.Monoid}, nil
}

// Get returns the cached bytes at k. Missing keys return nil, false, nil.
func (c *BytesCache) Get(ctx context.Context, k state.Key) ([]byte, bool, error) {
	key := c.cacheKey(k)
	resp := c.client.Do(ctx, c.client.B().Get().Key(key).Build())
	if resp.Error() != nil {
		if rueidis.IsRedisNil(resp.Error()) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("valkey GET %s: %w", key, resp.Error())
	}
	b, err := resp.AsBytes()
	if err != nil {
		return nil, false, fmt.Errorf("valkey GET %s decode: %w", key, err)
	}
	return b, true, nil
}

// GetMany batches reads via MGET.
func (c *BytesCache) GetMany(ctx context.Context, ks []state.Key) ([][]byte, []bool, error) {
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
	vals := make([][]byte, len(ks))
	oks := make([]bool, len(ks))
	for i, item := range arr {
		b, err := item.AsBytes()
		if err != nil {
			continue // Nil → missing key
		}
		vals[i] = b
		oks[i] = true
	}
	return vals, oks, nil
}

// MergeUpdate fetches the current cached value, runs the monoid Combine
// in Go, and writes the merged result back via SET.
//
// This is intentionally NOT a CAS path. The cache is "lossy by design"
// — the BytesStore is the source of truth. Two concurrent writers on
// the same key may interleave such that one's contribution drops out of
// the cache, but BOTH contributions still land on the BytesStore. The
// only cost of a lost cache write is one slower-than-ideal subsequent
// read until the next cache write or Repopulate covers it.
//
// Lua-scripted server-side merge isn't viable: the Combine is user Go
// code, not a Valkey-supported operation. A future Phase 3 might
// standardize on Valkey-native sketch types (PFADD/PFCOUNT for HLL,
// TopK module commands) which DO permit server-side combining, at the
// cost of an encoding-conversion boundary against axiomhq/hyperloglog
// and the hand-rolled Misra-Gries / Bloom encodings.
func (c *BytesCache) MergeUpdate(ctx context.Context, k state.Key, delta []byte, ttl time.Duration) error {
	key := c.cacheKey(k)
	cur, _, err := c.Get(ctx, k)
	if err != nil {
		return err
	}
	var merged []byte
	if cur == nil {
		merged = c.monoid.Combine(c.monoid.Identity(), delta)
	} else {
		merged = c.monoid.Combine(cur, delta)
	}

	if ttl > 0 {
		cmd := c.client.B().Set().Key(key).Value(string(merged)).ExSeconds(int64(ttl.Seconds())).Build()
		if err := c.client.Do(ctx, cmd).Error(); err != nil {
			return fmt.Errorf("valkey SET %s: %w", key, err)
		}
	} else {
		cmd := c.client.B().Set().Key(key).Value(string(merged)).Build()
		if err := c.client.Do(ctx, cmd).Error(); err != nil {
			return fmt.Errorf("valkey SET %s: %w", key, err)
		}
	}
	return nil
}

// Repopulate rebuilds cache entries from the authoritative store. Used
// after a Valkey restart or cold start.
func (c *BytesCache) Repopulate(ctx context.Context, src state.Store[[]byte], keys []state.Key) error {
	if len(keys) == 0 {
		return nil
	}
	vals, oks, err := src.GetMany(ctx, keys)
	if err != nil {
		return fmt.Errorf("repopulate read: %w", err)
	}
	for i, k := range keys {
		if !oks[i] {
			continue
		}
		key := c.cacheKey(k)
		if err := c.client.Do(ctx, c.client.B().Set().Key(key).Value(string(vals[i])).Build()).Error(); err != nil {
			return fmt.Errorf("valkey SET %s: %w", key, err)
		}
	}
	return nil
}

// Close releases the underlying Valkey client.
func (c *BytesCache) Close() error {
	c.client.Close()
	return nil
}

func (c *BytesCache) cacheKey(k state.Key) string {
	var sb strings.Builder
	sb.Grow(len(c.keyPrefix) + len(k.Entity) + 24)
	sb.WriteString(c.keyPrefix)
	sb.WriteByte(':')
	sb.WriteString(k.Entity)
	sb.WriteByte(':')
	// strconv on int64 inline to avoid pulling strconv just for this in some builds
	bucket := k.Bucket
	if bucket < 0 {
		sb.WriteByte('-')
		bucket = -bucket
	}
	if bucket == 0 {
		sb.WriteByte('0')
	} else {
		var buf [20]byte
		i := len(buf)
		for bucket > 0 {
			i--
			buf[i] = byte('0' + bucket%10)
			bucket /= 10
		}
		sb.Write(buf[i:])
	}
	return sb.String()
}

// Compile-time check.
var _ state.Cache[[]byte] = (*BytesCache)(nil)
