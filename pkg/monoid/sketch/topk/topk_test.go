package topk_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/topk"
)

func TestTopK_BasicCounting(t *testing.T) {
	m := topk.New(3)
	state := m.Identity()

	// alpha:5, beta:3, gamma:1, delta:2 — top 3 should be alpha, beta, delta.
	for _, k := range []string{"alpha", "alpha", "alpha", "alpha", "alpha"} {
		state = m.Combine(state, topk.SingleN(3, k, 1))
	}
	for _, k := range []string{"beta", "beta", "beta"} {
		state = m.Combine(state, topk.SingleN(3, k, 1))
	}
	for _, k := range []string{"delta", "delta"} {
		state = m.Combine(state, topk.SingleN(3, k, 1))
	}
	state = m.Combine(state, topk.SingleN(3, "gamma", 1))

	items, err := topk.Items(state)
	if err != nil {
		t.Fatalf("Items: %v", err)
	}
	t.Logf("top-3: %+v", items)

	if len(items) > 3 {
		t.Fatalf("got %d items, want at most 3", len(items))
	}
	// alpha must be present and rank 1.
	if items[0].Key != "alpha" {
		t.Errorf("rank 1: got %q, want alpha", items[0].Key)
	}
	// beta should be rank 2 (count higher than delta and gamma).
	if len(items) > 1 && items[1].Key != "beta" {
		t.Errorf("rank 2: got %q, want beta", items[1].Key)
	}
}

func TestTopK_AssociativityForSmallStreams(t *testing.T) {
	// When the stream fits within K, Misra-Gries reduces to exact counting and Combine
	// is straightforwardly associative.
	m := topk.New(10)
	a := topk.SingleN(10, "x", 1)
	b := topk.SingleN(10, "y", 1)
	c := topk.SingleN(10, "z", 1)

	left, _ := topk.Items(m.Combine(m.Combine(a, b), c))
	right, _ := topk.Items(m.Combine(a, m.Combine(b, c)))

	if len(left) != len(right) {
		t.Fatalf("associativity len: %d vs %d", len(left), len(right))
	}
	// Compare as sets of (Key, Count) pairs.
	leftMap := map[string]uint64{}
	rightMap := map[string]uint64{}
	for _, it := range left {
		leftMap[it.Key] = it.Count
	}
	for _, it := range right {
		rightMap[it.Key] = it.Count
	}
	for k, lc := range leftMap {
		if rightMap[k] != lc {
			t.Errorf("key %q: left %d, right %d", k, lc, rightMap[k])
		}
	}
}

func TestTopK_BoundedMemory(t *testing.T) {
	// Misra-Gries bounds memory at K — even with 1000 distinct keys we should never
	// exceed K items in the stored sketch.
	//
	// Bounded memory is only half the story: the counters that survive are also
	// nearly all of the stream gone. At K=32 over 1000 distinct keys the summary
	// keeps a handful of counts in the low single digits, and without the
	// ingested weight in the header that is byte-indistinguishable from a
	// complete answer over a handful of events.
	const K uint32 = 32
	m := topk.New(K)
	state := m.Identity()
	for i := 0; i < 1000; i++ {
		state = m.Combine(state, topk.SingleN(K, fmt.Sprintf("user-%d", i), 1))
	}
	s, err := topk.Inspect(state)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if uint32(len(s.Items)) > K {
		t.Fatalf("bounded memory broken: got %d items, K=%d", len(s.Items), K)
	}
	if s.Ingested != 1000 {
		t.Errorf("ingested weight: got %d, want 1000 — the stream size must survive eviction", s.Ingested)
	}
	if !s.Saturated() {
		t.Errorf("1000 distinct keys into K=%d must report saturation; retained %d of %d", K, s.Retained, s.Ingested)
	}
	if c := s.Coverage(); c > 0.05 {
		t.Errorf("coverage: got %v, want the sketch to admit it holds almost none of the stream", c)
	}
}

func TestTopK_HeavyHitterIdentified(t *testing.T) {
	// Misra-Gries guarantees: any element with true count > n/(K+1) is in the output.
	const K uint32 = 5
	m := topk.New(K)
	state := m.Identity()

	// Total stream size n = 600. n/(K+1) = 100. Any item with count > 100 must be retained.
	// "heavy" appears 200 times; "light-i" each 1 time, 400 of them.
	for i := 0; i < 200; i++ {
		state = m.Combine(state, topk.SingleN(K, "heavy", 1))
	}
	for i := 0; i < 400; i++ {
		state = m.Combine(state, topk.SingleN(K, fmt.Sprintf("light-%d", i), 1))
	}
	items, _ := topk.Items(state)
	found := false
	for _, it := range items {
		if it.Key == "heavy" {
			found = true
		}
	}
	if !found {
		t.Errorf("heavy hitter (200/600 = 33%%) missing from top-5 output: %+v", items)
	}
}

func TestTopK_Identity(t *testing.T) {
	m := topk.New(5)
	a := topk.SingleN(5, "x", 1)
	got, _ := topk.Items(m.Combine(m.Identity(), a))
	if len(got) != 1 || got[0].Key != "x" {
		t.Fatalf("Combine(Identity, a): got %+v", got)
	}
	got, _ = topk.Items(m.Combine(a, m.Identity()))
	if len(got) != 1 || got[0].Key != "x" {
		t.Fatalf("Combine(a, Identity): got %+v", got)
	}
}

// --- parameter safety: the wire K is state, not a hint ---

func saturatedRow(t *testing.T, k uint32, distinct int) []byte {
	t.Helper()
	m := topk.New(k)
	state := m.Identity()
	for i := 0; i < distinct; i++ {
		state = m.Combine(state, topk.SingleN(k, fmt.Sprintf("hitter-%d", i), uint64(i+1)))
	}
	return state
}

func TestTopK_LowerKMonoidDoesNotTruncateAWiderRow(t *testing.T) {
	// A K=10 client reading a K=32 row is a real configuration: the query layer
	// builds its own monoid, and nothing forces it to match the writer. The
	// wire K used to be decoded and thrown away, so the merge ran at 10 and
	// wrote the truncation back — 22 counters gone, unrecoverable from
	// anything downstream.
	row := saturatedRow(t, 32, 100)
	before, err := topk.Inspect(row)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if len(before.Items) < 30 {
		t.Fatalf("fixture is not a wide row: %d items", len(before.Items))
	}

	var errs []error
	m := topk.New(10, topk.WithDecodeErrorHandler(func(err error) { errs = append(errs, err) }))
	after, err := topk.Inspect(m.Combine(row, topk.SingleN(10, "newcomer", 1)))
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}

	if after.K != 32 {
		t.Errorf("merged sketch K: got %d, want 32 — the row's capacity is state, not a hint", after.K)
	}
	// One eviction round can shed the smallest counter; collapsing to the
	// monoid's own K=10 is the failure this guards.
	if len(after.Items) < len(before.Items)-1 {
		t.Errorf("K=10 monoid truncated a K=32 row: %d items in, %d out", len(before.Items), len(after.Items))
	}
	if len(errs) != 1 {
		t.Fatalf("K mismatch reports: got %d, want 1 — a mismatch must not be silent: %v", len(errs), errs)
	}
	msg := errs[0].Error()
	for _, want := range []string{"K=10", "K=32"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error should name both capacities: got %q, want it to contain %q", msg, want)
		}
	}
}

func TestTopK_WindowMergeAtLowerKKeepsTheWiderSummary(t *testing.T) {
	// The read-side shape of the same bug: GetWindow merges the N most recent
	// daily buckets through the client's monoid. Seven K=32 buckets merged by a
	// K=10 client used to come back as ten counters.
	m := topk.New(10)
	merged := m.Identity()
	for i := 0; i < 7; i++ {
		merged = m.Combine(merged, saturatedRow(t, 32, 60))
	}

	summary, err := topk.Inspect(merged)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if summary.K != 32 {
		t.Errorf("merged sketch K: got %d, want 32 — the buckets' capacity must survive the read", summary.K)
	}
	if len(summary.Items) <= 10 {
		t.Errorf("window merge truncated to the client's K: got %d items, want more than 10", len(summary.Items))
	}
}

// --- saturation is visible in the wire format ---

// roundRobin folds n events over d distinct entities through m, one Combine per
// event, the way a streaming worker would.
func roundRobin(m monoid.Monoid[[]byte], k uint32, d, n int) []byte {
	state := m.Identity()
	for i := 0; i < n; i++ {
		state = m.Combine(state, topk.SingleN(k, fmt.Sprintf("e-%d", i%d), 1))
	}
	return state
}

func TestTopK_IngestedWeightMakesSaturationVisible(t *testing.T) {
	// K=32 over 45,932 events. With 32 distinct entities the summary is exact.
	// Add ONE more entity and Misra-Gries eviction takes it down to 29 counters
	// summing to 29 — 0.06% of the stream. The two sketches are the same shape,
	// the same size, and the same kind of thing to every consumer of Items();
	// only the ingested weight in the header tells them apart.
	const k uint32 = 32
	const n = 45_932
	m := topk.New(k)

	exact, err := topk.Inspect(roundRobin(m, k, 32, n))
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if len(exact.Items) != 32 || exact.Retained != n {
		t.Fatalf("32 distinct entities: got %d items summing to %d, want 32 summing to %d",
			len(exact.Items), exact.Retained, n)
	}

	saturated, err := topk.Inspect(roundRobin(m, k, 33, n))
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if len(saturated.Items) != 29 || saturated.Retained != 29 {
		t.Fatalf("33 distinct entities: got %d items summing to %d, want 29 summing to 29",
			len(saturated.Items), saturated.Retained)
	}

	// Both ingested the same stream, and both now say so.
	if exact.Ingested != n || saturated.Ingested != n {
		t.Fatalf("ingested weight: exact=%d saturated=%d, want %d for both", exact.Ingested, saturated.Ingested, n)
	}
	if exact.Saturated() {
		t.Error("a summary that retained every event must not report saturation")
	}
	if !saturated.Saturated() {
		t.Error("a summary holding 29 of 45,932 must report saturation")
	}
	if exact.Coverage() != 1 {
		t.Errorf("exact coverage: got %v, want 1", exact.Coverage())
	}
	if c := saturated.Coverage(); c > 0.001 {
		t.Errorf("saturated coverage: got %v, want the sub-0.1%% truth", c)
	}
	if exact.PartialWeight || saturated.PartialWeight {
		t.Error("weights accumulated by this package are exact, not partial")
	}
	// n/(K+1) is the error bar a caller has to draw around every count.
	if got, want := saturated.MaxError(), uint64(n)/uint64(k+1); got != want {
		t.Errorf("MaxError: got %d, want %d", got, want)
	}
}

func TestTopK_HeavyHitterRetainsMassWithinTheErrorBound(t *testing.T) {
	// Misra-Gries' contract: a key with true count above n/(K+1) survives, and
	// its retained count understates the truth by at most n/(K+1). Assert both
	// against the n the sketch now reports for itself, rather than against an n
	// the test happens to remember.
	const k uint32 = 32
	const (
		hot  = 5_000
		cold = 40_932
		n    = hot + cold
	)
	m := topk.New(k)
	state := m.Identity()
	for i := 0; i < hot; i++ {
		state = m.Combine(state, topk.SingleN(k, "hot", 1))
	}
	for i := 0; i < cold; i++ {
		state = m.Combine(state, topk.SingleN(k, fmt.Sprintf("cold-%d", i), 1))
	}

	s, err := topk.Inspect(state)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if s.Ingested != n {
		t.Fatalf("ingested: got %d, want %d", s.Ingested, n)
	}
	bound := s.MaxError()
	if hot <= bound {
		t.Fatalf("fixture is not a heavy hitter: %d <= n/(K+1) = %d", hot, bound)
	}

	var got uint64
	var found bool
	for _, it := range s.Items {
		if it.Key == "hot" {
			found, got = true, it.Count
		}
	}
	if !found {
		t.Fatalf("heavy hitter (%d of %d) missing from the summary: %+v", hot, n, s.Items)
	}
	if got+bound < hot {
		t.Errorf("heavy hitter count %d understates the true %d by more than n/(K+1) = %d", got, hot, bound)
	}
	if got > hot {
		t.Errorf("Misra-Gries counts are lower bounds; got %d for a true count of %d", got, hot)
	}
	// The retained mass is what tells a caller how much of the stream these
	// counters actually speak for.
	if s.Retained < hot-bound {
		t.Errorf("retained mass %d is below the heavy hitter's guaranteed floor %d", s.Retained, hot-bound)
	}
	if !s.Saturated() {
		t.Errorf("a K=%d summary of %d distinct keys must report saturation", k, cold+1)
	}
}

func TestTopK_LegacySketchWithoutIngestedWeightIsFlagged(t *testing.T) {
	// Rows written before the header carried a weight still have to decode — a
	// rolling deploy reads them for as long as they live. Their weight can only
	// be reconstructed from what they retained, so it is a lower bound, and the
	// summary has to say so rather than reporting full coverage for a sketch
	// that may have evicted most of its stream.
	legacy := []byte{
		0x20, 0x00, 0x00, 0x00, // K = 32, no flags: the pre-weight header
		0x01, 0x00, 0x00, 0x00, // N = 1
		0x07, 0, 0, 0, 0, 0, 0, 0, // count = 7
		0x01, 0x00, 0x00, 0x00, // keyLen = 1
		'a',
	}

	s, err := topk.Inspect(legacy)
	if err != nil {
		t.Fatalf("Inspect legacy sketch: %v", err)
	}
	if s.K != 32 || len(s.Items) != 1 || s.Items[0].Key != "a" || s.Items[0].Count != 7 {
		t.Fatalf("legacy sketch decoded wrong: %+v", s)
	}
	if !s.PartialWeight {
		t.Error("a sketch with no recorded weight must be flagged partial, not reported as complete")
	}
	if s.Ingested != 7 {
		t.Errorf("legacy ingested weight: got %d, want the retained mass 7 as a lower bound", s.Ingested)
	}

	// And the flag is sticky: merging it into a current sketch must not launder
	// the missing weight into a confident total.
	m := topk.New(32)
	merged, err := topk.Inspect(m.Combine(legacy, topk.SingleN(32, "b", 3)))
	if err != nil {
		t.Fatalf("Inspect merged: %v", err)
	}
	if !merged.PartialWeight {
		t.Error("PartialWeight must survive a merge; coverage from a short total reads as complete")
	}
	if merged.Ingested != 10 {
		t.Errorf("merged ingested weight: got %d, want 10", merged.Ingested)
	}
}
