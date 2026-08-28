// Saturation: what the wire format does NOT tell you.
//
// These tests assert on the sketch API exactly as it stands today. They are
// here to pin down the cliff a caller falls off when a top-K saturates,
// because that cliff is invisible from the outside: a summary that retained 29
// of 45,932 events is byte-indistinguishable from an exact answer over 29
// events. Both are a short list of small counts, and nothing reachable from
// Items() separates them.
//
// Closing that gap needs the ingested weight in the header, which is a
// wire-format change and is deliberately NOT on this branch — it has to land
// as a staged read-both/write-old change so a rolling deploy cannot lose live
// rows. Until it does, these tests are the documentation.
package topk_test

import (
	"fmt"
	"testing"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/topk"
)

// roundRobin folds n events over d distinct entities through m, one Combine per
// event, the way a streaming worker would.
func roundRobin(m monoid.Monoid[[]byte], k uint32, d, n int) []byte {
	state := m.Identity()
	for i := 0; i < n; i++ {
		state = m.Combine(state, topk.SingleN(k, fmt.Sprintf("e-%d", i%d), 1))
	}
	return state
}

// retained sums the counts a sketch still holds. It is the numerator of a
// coverage ratio the caller cannot currently compute, because the denominator
// — the weight actually ingested — is not stored anywhere.
func retained(t *testing.T, sketch []byte) uint64 {
	t.Helper()
	items, err := topk.Items(sketch)
	if err != nil {
		t.Fatalf("topk.Items: %v", err)
	}
	var sum uint64
	for _, it := range items {
		sum += it.Count
	}
	return sum
}

func TestTopK_OneExtraEntityCollapsesTheSummary(t *testing.T) {
	// K=32 over 45,932 events. With exactly 32 distinct entities the summary is
	// exact: 32 counters summing to the whole stream. Add ONE more entity and
	// Misra-Gries eviction takes it down to 29 counters summing to 29 — 0.06%
	// of the stream — with no change in shape, size, or type.
	//
	// That is the whole hazard in one assertion pair. The caller who reads the
	// second sketch gets counts in the single digits and no way to tell they
	// are the residue of 45,932 events rather than all of them.
	const k uint32 = 32
	const n = 45_932
	m := topk.New(k)

	exact := roundRobin(m, k, 32, n)
	exactItems, err := topk.Items(exact)
	if err != nil {
		t.Fatalf("topk.Items: %v", err)
	}
	if exactRetained := retained(t, exact); len(exactItems) != 32 || exactRetained != n {
		t.Fatalf("32 distinct entities: got %d items summing to %d, want 32 summing to %d",
			len(exactItems), exactRetained, n)
	}

	saturated := roundRobin(m, k, 33, n)
	saturatedItems, err := topk.Items(saturated)
	if err != nil {
		t.Fatalf("topk.Items: %v", err)
	}
	saturatedRetained := retained(t, saturated)
	if len(saturatedItems) != 29 || saturatedRetained != 29 {
		t.Fatalf("33 distinct entities: got %d items summing to %d, want 29 summing to 29",
			len(saturatedItems), saturatedRetained)
	}

	// The cliff as a ratio: one extra entity took coverage from 100% to under a
	// tenth of a percent.
	if cov := float64(saturatedRetained) / float64(n); cov > 0.001 {
		t.Errorf("saturated coverage: got %v, want the sub-0.1%% truth", cov)
	}
	if cov := float64(retained(t, exact)) / float64(n); cov != 1 {
		t.Errorf("exact coverage: got %v, want 1", cov)
	}
}

func TestTopK_HeavyHitterSurvivesSaturationWithinTheErrorBound(t *testing.T) {
	// Misra-Gries' contract, asserted at a saturation level where it actually
	// bites: a key with true count above n/(K+1) survives eviction, and its
	// retained count understates the truth by at most n/(K+1).
	//
	// n is a constant here because the sketch cannot report it. That is the
	// point — the error bar a caller must draw around every count is n/(K+1),
	// and n is knowledge the caller has to carry out of band.
	const k uint32 = 32
	const (
		hot  = 5_000
		cold = 40_932
		n    = hot + cold
	)
	const bound = uint64(n) / uint64(k+1)

	m := topk.New(k)
	state := m.Identity()
	for i := 0; i < hot; i++ {
		state = m.Combine(state, topk.SingleN(k, "hot", 1))
	}
	for i := 0; i < cold; i++ {
		state = m.Combine(state, topk.SingleN(k, fmt.Sprintf("cold-%d", i), 1))
	}

	items, err := topk.Items(state)
	if err != nil {
		t.Fatalf("topk.Items: %v", err)
	}
	if hot <= int(bound) {
		t.Fatalf("fixture is not a heavy hitter: %d <= n/(K+1) = %d", hot, bound)
	}

	var got uint64
	var found bool
	for _, it := range items {
		if it.Key == "hot" {
			found, got = true, it.Count
		}
	}
	if !found {
		t.Fatalf("heavy hitter (%d of %d) missing from the summary: %+v", hot, n, items)
	}
	if got+bound < hot {
		t.Errorf("heavy hitter count %d understates the true %d by more than n/(K+1) = %d", got, hot, bound)
	}
	if got > hot {
		t.Errorf("Misra-Gries counts are lower bounds; got %d for a true count of %d", got, hot)
	}

	// Saturation, expressed the only way the current format allows: the
	// retained mass is far below the stream it summarizes.
	if r := retained(t, state); r >= n {
		t.Errorf("retained %d of %d: a K=%d summary of %d distinct keys must have shed mass", r, n, k, cold+1)
	}
}
