package topk_test

import (
	"fmt"
	"testing"

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
	const K uint32 = 5
	m := topk.New(K)
	state := m.Identity()
	for i := 0; i < 1000; i++ {
		state = m.Combine(state, topk.SingleN(K, fmt.Sprintf("user-%d", i), 1))
	}
	items, _ := topk.Items(state)
	if uint32(len(items)) > K {
		t.Fatalf("bounded memory broken: got %d items, K=%d", len(items), K)
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
