package valkey_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/state"
	mvalkey "github.com/gallowaysoftware/murmur/pkg/state/valkey"
)

func newBloomCache(t *testing.T, prefix string) *mvalkey.BloomCache {
	t.Helper()
	c, err := mvalkey.NewBloomCache(mvalkey.BloomConfig{
		Address:          valkeyAddress(t),
		KeyPrefix:        prefix,
		DefaultCapacity:  1000,
		DefaultErrorRate: 0.01,
		AutoReserve:      true,
	})
	if err != nil {
		t.Fatalf("new bloom cache: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// skipIfNoBloomModule short-circuits the test when the connected Valkey
// instance doesn't have the BF.* module loaded. The error shape from
// rueidis on an unknown command is "ERR unknown command 'BF.ADD'" or
// similar; we detect that as a skip rather than a failure so the test
// suite still passes against vanilla Valkey (without valkey-bloom) used
// by some users.
func skipIfNoBloomModule(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "unknown command") || strings.Contains(msg, "module") {
		t.Skipf("BF.* module not loaded on this Valkey instance: %v (install valkey-bloom or RedisBloom)", err)
	}
}

func TestBloomCache_AddAndContains(t *testing.T) {
	if os.Getenv("VALKEY_BLOOM_ENABLED") == "" {
		// Bloom module isn't loaded in the default compose stack yet; gate
		// the integration tests behind an opt-in env var until it is. The
		// short_ unit gate is unchanged.
		t.Skip("VALKEY_BLOOM_ENABLED not set; install valkey-bloom and set the env to run")
	}
	c := newBloomCache(t, fmt.Sprintf("bf_basic_%d", time.Now().UnixNano()))
	ctx := context.Background()
	k := state.Key{Entity: "notif:1"}

	added, err := c.Add(ctx, k, []byte("user-A"), 0)
	skipIfNoBloomModule(t, err)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if !added {
		t.Errorf("first Add returned added=false; want true")
	}

	// Re-add same element: BF.ADD returns 0 (already-present).
	added2, _ := c.Add(ctx, k, []byte("user-A"), 0)
	if added2 {
		t.Errorf("second Add of same element returned added=true; want false")
	}

	present, err := c.Contains(ctx, k, []byte("user-A"))
	if err != nil {
		t.Fatalf("Contains: %v", err)
	}
	if !present {
		t.Errorf("Contains after Add: false; want true")
	}

	absent, _ := c.Contains(ctx, k, []byte("user-never-added"))
	if absent {
		t.Errorf("Contains on never-added: true; want false (within FPR)")
	}
}

func TestBloomCache_AddManyAndContainsMany(t *testing.T) {
	if os.Getenv("VALKEY_BLOOM_ENABLED") == "" {
		t.Skip("VALKEY_BLOOM_ENABLED not set")
	}
	c := newBloomCache(t, fmt.Sprintf("bf_many_%d", time.Now().UnixNano()))
	ctx := context.Background()
	k := state.Key{Entity: "notif:batch"}

	in := [][]byte{[]byte("u1"), []byte("u2"), []byte("u3"), []byte("u4")}
	added, err := c.AddMany(ctx, k, in, 0)
	skipIfNoBloomModule(t, err)
	if err != nil {
		t.Fatalf("AddMany: %v", err)
	}
	if len(added) != len(in) {
		t.Fatalf("AddMany returned %d, want %d", len(added), len(in))
	}
	for i, v := range added {
		if !v {
			t.Errorf("first AddMany[%d] added=false; want true", i)
		}
	}

	probes := [][]byte{[]byte("u1"), []byte("u-NOT-PRESENT"), []byte("u3")}
	got, err := c.ContainsMany(ctx, k, probes)
	if err != nil {
		t.Fatalf("ContainsMany: %v", err)
	}
	if !got[0] || got[1] || !got[2] {
		t.Errorf("ContainsMany %v: got %v, want [true false true]", probes, got)
	}
}

func TestBloomCache_MissingKey(t *testing.T) {
	if os.Getenv("VALKEY_BLOOM_ENABLED") == "" {
		t.Skip("VALKEY_BLOOM_ENABLED not set")
	}
	c := newBloomCache(t, fmt.Sprintf("bf_missing_%d", time.Now().UnixNano()))
	ctx := context.Background()

	present, err := c.Contains(ctx, state.Key{Entity: "never-created"}, []byte("anything"))
	skipIfNoBloomModule(t, err)
	if err != nil {
		t.Fatalf("Contains on missing key: %v", err)
	}
	if present {
		t.Errorf("Contains on missing key: true; want false")
	}

	info, ok, err := c.Info(ctx, state.Key{Entity: "never-created"})
	if err != nil {
		t.Fatalf("Info on missing key: %v", err)
	}
	if ok {
		t.Errorf("Info on missing key: ok=true; want false")
	}
	if info.Items != 0 || info.Capacity != 0 {
		t.Errorf("Info on missing key: got %+v; want zero", info)
	}
}

func TestBloomCache_ReserveThenInfo(t *testing.T) {
	if os.Getenv("VALKEY_BLOOM_ENABLED") == "" {
		t.Skip("VALKEY_BLOOM_ENABLED not set")
	}
	c := newBloomCache(t, fmt.Sprintf("bf_reserve_%d", time.Now().UnixNano()))
	ctx := context.Background()
	k := state.Key{Entity: "explicit"}

	if err := c.Reserve(ctx, k, 5000, 0.001, 0); err != nil {
		skipIfNoBloomModule(t, err)
		t.Fatalf("Reserve: %v", err)
	}
	if _, err := c.Add(ctx, k, []byte("x"), 0); err != nil {
		t.Fatalf("Add after Reserve: %v", err)
	}
	info, ok, err := c.Info(ctx, k)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if !ok {
		t.Fatalf("Info: ok=false; want true")
	}
	if info.Items != 1 {
		t.Errorf("Info Items: got %d; want 1", info.Items)
	}
	if info.Capacity < 5000 {
		t.Errorf("Info Capacity: got %d; want ≥5000", info.Capacity)
	}
}

func TestBloomCache_TTLApplied(t *testing.T) {
	if os.Getenv("VALKEY_BLOOM_ENABLED") == "" {
		t.Skip("VALKEY_BLOOM_ENABLED not set")
	}
	c := newBloomCache(t, fmt.Sprintf("bf_ttl_%d", time.Now().UnixNano()))
	ctx := context.Background()
	k := state.Key{Entity: "fleeting", Bucket: 999}

	if _, err := c.Add(ctx, k, []byte("blip"), 1*time.Second); err != nil {
		skipIfNoBloomModule(t, err)
		t.Fatalf("Add with TTL: %v", err)
	}
	if present, _ := c.Contains(ctx, k, []byte("blip")); !present {
		t.Errorf("immediate Contains: false; want true")
	}
	time.Sleep(2 * time.Second)
	present, err := c.Contains(ctx, k, []byte("blip"))
	if err != nil {
		t.Fatalf("post-TTL Contains: %v", err)
	}
	if present {
		t.Errorf("post-TTL Contains: true; key should be evicted")
	}
}

func TestBloomCache_NewRequiresKeyPrefix(t *testing.T) {
	_, err := mvalkey.NewBloomCache(mvalkey.BloomConfig{})
	if err == nil {
		t.Fatal("NewBloomCache: nil error, want failure on empty KeyPrefix")
	}
}
