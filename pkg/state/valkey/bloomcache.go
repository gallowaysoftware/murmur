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

// BloomCache is a Valkey-native Bloom-filter accelerator built on the
// BF.* module commands (BF.ADD / BF.MADD / BF.EXISTS / BF.MEXISTS /
// BF.RESERVE / BF.INFO). It is the Bloom analog of [HLLCache].
//
// # Why this exists
//
// Murmur's BytesStore-authoritative Bloom pipeline marshals
// bits-and-blooms/bloom sketches as opaque bytes into DDB. Membership
// queries require:
//
//  1. DDB GetItem (5–15 ms cold)
//  2. bits-and-blooms UnmarshalBinary
//  3. Test()
//
// For dashboard-class queries this is fine. For hot-path
// membership checks ("has this user already received this notification?",
// "is this fingerprint already in our suppression list?"), single-digit
// millisecond BF.EXISTS via Valkey is preferable.
//
// # The side-by-side pattern
//
// Pipelines using the accelerator emit each event element to BOTH the
// BytesStore (as a bits-and-blooms Single() sketch merged via MergeUpdate)
// AND to this BloomCache via Add. The two filters have INDEPENDENT
// false-positive realizations; both sit at the same expected FPR but a
// specific element may be reported present in one and absent in the
// other. For applications where the discrepancy matters (e.g. strict
// deduplication), pin reads to one side or the other.
//
// # Recovery
//
// On Valkey loss the accelerator can only be repopulated by re-feeding
// events. There is no portable bits-and-blooms ↔ valkey-bloom byte
// conversion — the bit layouts and hash schemes differ. If your pipeline
// cannot replay events, treat this as best-effort cache and fall back to
// BytesStore on Valkey miss / unavailability.
//
// # Module requirement
//
// BF.* commands require a Bloom-filter module loaded into the Valkey
// (or Redis) instance: `valkey-bloom` for Valkey, RedisBloom for Redis.
// Both have been kept wire-compatible at the BF.* command surface; the
// accelerator works against either. A Valkey instance without the
// module returns "ERR unknown command 'BF.ADD'" on Add, so a single
// dev-time round-trip catches a misconfigured cluster.
//
// # Key layout
//
// Cache keys are "<keyPrefix>:<entity>:<bucket>" — same scheme as
// Int64Cache / BytesCache / HLLCache.
type BloomCache struct {
	client          valkey.Client
	keyPrefix       string
	defaultCapacity int64
	defaultError    float64
	autoReserve     bool
}

// BloomConfig configures a BloomCache.
type BloomConfig struct {
	// Address is the Valkey server endpoint. Defaults to "localhost:6379".
	Address string

	// KeyPrefix namespaces this accelerator's keys (e.g. "notification_sent").
	// Required.
	KeyPrefix string

	// DefaultCapacity is the filter capacity used when Add / AddMany
	// implicitly creates a filter (i.e. when AutoReserve is true and the
	// key doesn't exist). Ignored when AutoReserve is false. Defaults to
	// 100_000 — same as pkg/monoid/sketch/bloom.DefaultCapacity.
	DefaultCapacity int64

	// DefaultErrorRate is the target false-positive rate for auto-reserved
	// filters. Defaults to 0.01.
	DefaultErrorRate float64

	// AutoReserve controls whether Add / AddMany on a missing key
	// implicitly creates a filter sized at (DefaultCapacity,
	// DefaultErrorRate) before the BF.ADD / BF.MADD. When false, the
	// underlying module's defaults are used (typically 100 items at 1%
	// FPR), which is rarely what callers want — leave AutoReserve true
	// for production usage.
	AutoReserve bool

	// Extra lets callers append additional valkey-go options (TLS, auth, etc).
	Extra []valkey.ClientOption
}

// NewBloomCache constructs a BloomCache. The returned cache owns the
// underlying Valkey client; call Close to disconnect.
func NewBloomCache(cfg BloomConfig) (*BloomCache, error) {
	if cfg.KeyPrefix == "" {
		return nil, errors.New("valkey BloomCache: KeyPrefix is required")
	}
	if cfg.DefaultCapacity == 0 {
		cfg.DefaultCapacity = 100_000
	}
	if cfg.DefaultErrorRate == 0 {
		cfg.DefaultErrorRate = 0.01
	}
	opts := valkey.ClientOption{InitAddress: []string{cfg.Address}}
	if cfg.Address == "" {
		opts.InitAddress = []string{"localhost:6379"}
	}
	client, err := valkey.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("valkey new client: %w", err)
	}
	return &BloomCache{
		client:          client,
		keyPrefix:       cfg.KeyPrefix,
		defaultCapacity: cfg.DefaultCapacity,
		defaultError:    cfg.DefaultErrorRate,
		autoReserve:     cfg.AutoReserve,
	}, nil
}

// Reserve creates a filter at key k sized for (capacity, errorRate). Use
// before Add / AddMany when you want to control the shape explicitly
// (instead of relying on AutoReserve or the module's default 100/1%
// fallback). Returns nil on success; non-nil if the key already exists
// — Valkey's BF.RESERVE refuses to overwrite, which is the right safety
// default. If ttl > 0 an EXPIRE is set on the newly-created filter.
func (c *BloomCache) Reserve(ctx context.Context, k state.Key, capacity int64, errorRate float64, ttl time.Duration) error {
	key := c.cacheKey(k)
	resp := c.client.Do(ctx, c.client.B().BfReserve().Key(key).ErrorRate(errorRate).Capacity(capacity).Build())
	if resp.Error() != nil {
		return fmt.Errorf("valkey BF.RESERVE %s: %w", key, resp.Error())
	}
	if ttl > 0 {
		_ = c.client.Do(ctx, c.client.B().Expire().Key(key).Seconds(int64(ttl.Seconds())).Build()).Error()
	}
	return nil
}

// Add records that element was observed for entity k via BF.ADD. The
// return is the (added bool, error) tuple: added=true on first insert,
// added=false when element was already present (within the filter's FPR).
//
// If AutoReserve is set and the key doesn't yet exist, the filter is
// created at (DefaultCapacity, DefaultErrorRate) first via BF.INSERT
// (which atomically reserves + adds in a single round-trip).
//
// A failed Add is non-fatal at the pipeline level (BytesStore is source
// of truth) but should be logged and counted via metrics.
func (c *BloomCache) Add(ctx context.Context, k state.Key, element []byte, ttl time.Duration) (bool, error) {
	key := c.cacheKey(k)
	var resp valkey.ValkeyResult
	if c.autoReserve {
		// BF.INSERT auto-creates the filter at our preferred shape, then
		// adds the element. One round-trip; one path; no race with a
		// parallel writer's BF.ADD on the same key.
		resp = c.client.Do(ctx, c.client.B().BfInsert().Key(key).
			Capacity(c.defaultCapacity).
			Error(c.defaultError).
			Items().Item(string(element)).Build())
	} else {
		resp = c.client.Do(ctx, c.client.B().BfAdd().Key(key).Item(string(element)).Build())
	}
	if resp.Error() != nil {
		return false, fmt.Errorf("valkey BF.ADD %s: %w", key, resp.Error())
	}
	added, err := firstBool(resp)
	if err != nil {
		return false, fmt.Errorf("valkey BF.ADD %s decode: %w", key, err)
	}
	if ttl > 0 {
		_ = c.client.Do(ctx, c.client.B().Expire().Key(key).Seconds(int64(ttl.Seconds())).Build()).Error()
	}
	return added, nil
}

// AddMany records multiple elements for the same entity in one round
// trip via BF.MADD (or BF.INSERT when AutoReserve is set). Returns one
// added bool per input element, in input order.
func (c *BloomCache) AddMany(ctx context.Context, k state.Key, elements [][]byte, ttl time.Duration) ([]bool, error) {
	if len(elements) == 0 {
		return nil, nil
	}
	key := c.cacheKey(k)
	strs := make([]string, len(elements))
	for i, e := range elements {
		strs[i] = string(e)
	}
	var resp valkey.ValkeyResult
	if c.autoReserve {
		resp = c.client.Do(ctx, c.client.B().BfInsert().Key(key).
			Capacity(c.defaultCapacity).
			Error(c.defaultError).
			Items().Item(strs...).Build())
	} else {
		resp = c.client.Do(ctx, c.client.B().BfMadd().Key(key).Item(strs...).Build())
	}
	if resp.Error() != nil {
		return nil, fmt.Errorf("valkey BF.MADD %s (n=%d): %w", key, len(elements), resp.Error())
	}
	out, err := boolArray(resp)
	if err != nil {
		return nil, fmt.Errorf("valkey BF.MADD %s decode: %w", key, err)
	}
	if ttl > 0 {
		_ = c.client.Do(ctx, c.client.B().Expire().Key(key).Seconds(int64(ttl.Seconds())).Build()).Error()
	}
	return out, nil
}

// Contains reports whether element is (probably) present in the filter
// at k. Missing keys return (false, nil) — BF.EXISTS on a missing key
// is non-error and reports 0.
func (c *BloomCache) Contains(ctx context.Context, k state.Key, element []byte) (bool, error) {
	key := c.cacheKey(k)
	resp := c.client.Do(ctx, c.client.B().BfExists().Key(key).Item(string(element)).Build())
	if resp.Error() != nil {
		return false, fmt.Errorf("valkey BF.EXISTS %s: %w", key, resp.Error())
	}
	v, err := firstBool(resp)
	if err != nil {
		return false, fmt.Errorf("valkey BF.EXISTS %s decode: %w", key, err)
	}
	return v, nil
}

// ContainsMany reports per-element membership via BF.MEXISTS in one
// round-trip. Missing keys yield an all-false slice.
func (c *BloomCache) ContainsMany(ctx context.Context, k state.Key, elements [][]byte) ([]bool, error) {
	if len(elements) == 0 {
		return nil, nil
	}
	key := c.cacheKey(k)
	strs := make([]string, len(elements))
	for i, e := range elements {
		strs[i] = string(e)
	}
	resp := c.client.Do(ctx, c.client.B().BfMexists().Key(key).Item(strs...).Build())
	if resp.Error() != nil {
		return nil, fmt.Errorf("valkey BF.MEXISTS %s (n=%d): %w", key, len(elements), resp.Error())
	}
	out, err := boolArray(resp)
	if err != nil {
		return nil, fmt.Errorf("valkey BF.MEXISTS %s decode: %w", key, err)
	}
	return out, nil
}

// BloomInfo carries the BF.INFO triple: configured capacity, number of
// items inserted so far, and an `exists` flag distinguishing a missing
// key (false, no error) from an empty filter (true, items=0).
type BloomInfo struct {
	Capacity uint64
	Items    uint64
}

// Info returns BF.INFO Capacity + BF.INFO Items for entity k. Missing
// keys return (BloomInfo{}, false, nil). Useful for capacity-planning
// dashboards — when Items approaches Capacity, FPR climbs.
func (c *BloomCache) Info(ctx context.Context, k state.Key) (BloomInfo, bool, error) {
	key := c.cacheKey(k)
	// BF.INFO on a missing key returns an error mentioning "not found"; we
	// surface that as (zero, false, nil) rather than the raw error so
	// callers can branch on absent vs error like Cardinality does.
	capResp := c.client.Do(ctx, c.client.B().BfInfo().Key(key).Capacity().Build())
	if capResp.Error() != nil {
		if isNotFound(capResp.Error()) {
			return BloomInfo{}, false, nil
		}
		return BloomInfo{}, false, fmt.Errorf("valkey BF.INFO %s CAPACITY: %w", key, capResp.Error())
	}
	capV, err := capResp.AsInt64()
	if err != nil {
		return BloomInfo{}, false, fmt.Errorf("valkey BF.INFO %s CAPACITY decode: %w", key, err)
	}
	itemsResp := c.client.Do(ctx, c.client.B().BfInfo().Key(key).Items().Build())
	if itemsResp.Error() != nil {
		return BloomInfo{}, false, fmt.Errorf("valkey BF.INFO %s ITEMS: %w", key, itemsResp.Error())
	}
	itemsV, err := itemsResp.AsInt64()
	if err != nil {
		return BloomInfo{}, false, fmt.Errorf("valkey BF.INFO %s ITEMS decode: %w", key, err)
	}
	return BloomInfo{Capacity: uint64(capV), Items: uint64(itemsV)}, true, nil
}

// Close releases the underlying Valkey client.
func (c *BloomCache) Close() error {
	c.client.Close()
	return nil
}

func (c *BloomCache) cacheKey(k state.Key) string {
	var sb strings.Builder
	sb.Grow(len(c.keyPrefix) + len(k.Entity) + 24)
	sb.WriteString(c.keyPrefix)
	sb.WriteByte(':')
	sb.WriteString(k.Entity)
	sb.WriteByte(':')
	sb.WriteString(strconv.FormatInt(k.Bucket, 10))
	return sb.String()
}

// firstBool reads the first element of an integer array response (or
// the single integer response shape that BF.ADD may return) and treats
// non-zero as true. BF.ADD returns either 0 or 1; BF.INSERT returns an
// array of ints.
func firstBool(r valkey.ValkeyResult) (bool, error) {
	arr, err := r.ToArray()
	if err == nil {
		if len(arr) == 0 {
			return false, nil
		}
		n, err := arr[0].AsInt64()
		if err != nil {
			return false, err
		}
		return n != 0, nil
	}
	n, err := r.AsInt64()
	if err != nil {
		return false, err
	}
	return n != 0, nil
}

func boolArray(r valkey.ValkeyResult) ([]bool, error) {
	arr, err := r.ToArray()
	if err != nil {
		return nil, err
	}
	out := make([]bool, len(arr))
	for i, item := range arr {
		n, err := item.AsInt64()
		if err != nil {
			return nil, fmt.Errorf("element %d: %w", i, err)
		}
		out[i] = n != 0
	}
	return out, nil
}

// isNotFound recognizes the "key not found" shape returned by BF.INFO
// on a missing key. Module versions vary in the exact phrasing; we
// match the substrings the live versions of valkey-bloom and RedisBloom
// use today.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not found") || strings.Contains(msg, "no such key")
}
