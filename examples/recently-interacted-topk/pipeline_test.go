package recentlyinteracted_test

import (
	"context"
	"encoding/binary"
	"testing"

	example "github.com/gallowaysoftware/murmur/examples/recently-interacted-topk"
)

// sketchK reads the K a marshaled Misra-Gries sketch was sized for. The wire
// format opens with a little-endian uint32 K (pkg/monoid/sketch/topk).
func sketchK(t *testing.T, b []byte) uint32 {
	t.Helper()
	if len(b) < 4 {
		t.Fatalf("sketch too short to carry a K header: %d bytes", len(b))
	}
	return binary.LittleEndian.Uint32(b[:4])
}

// TestBuild_KDefaultsToDocumentedSize pins the K a caller gets when Config.K
// is left zero. Sketches with mismatched K refuse to merge, so a documented
// default that disagrees with the built one is not cosmetic: it puts a K=10
// store under the K=32 query server this example ships, and the Top-N comes
// back empty rather than erroring.
func TestBuild_KDefaultsToDocumentedSize(t *testing.T) {
	// 32 is the number the README, the Config doc, the TOPK_K default in both
	// writer binaries, and the query server all name.
	const documentedK uint32 = 32

	if example.DefaultK != documentedK {
		t.Fatalf("example.DefaultK = %d, want the documented %d", example.DefaultK, documentedK)
	}

	cases := []struct {
		name string
		cfg  uint32
		want uint32
	}{
		{"zero takes the documented default", 0, documentedK},
		{"explicit K is honored", 7, 7},
		{"explicit K equal to the default", documentedK, documentedK},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := example.Config{
				DDBTable:    "recently_interacted_test",
				DDBRegion:   "us-east-1",
				DDBEndpoint: "http://127.0.0.1:8000", // no call is made; keeps creds static
				K:           tc.cfg,
			}
			pipe, store, _, err := example.Build(context.Background(), cfg)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			t.Cleanup(func() { _ = store.Close() })

			// The per-event delta and the aggregating monoid must agree with
			// each other AND with what the query server would construct from
			// the same Config.
			delta := sketchK(t, pipe.ValueFn()(example.Interaction{EntityID: "entity-1"}))
			if delta != tc.want {
				t.Errorf("value extractor built a K=%d sketch, want K=%d", delta, tc.want)
			}
			if got := sketchK(t, pipe.Monoid().Identity()); got != tc.want {
				t.Errorf("aggregate monoid is K=%d, want K=%d", got, tc.want)
			}
			if got := cfg.ResolveK(); got != tc.want {
				t.Errorf("ResolveK (what the query server builds with) = %d, want %d", got, tc.want)
			}
		})
	}
}
