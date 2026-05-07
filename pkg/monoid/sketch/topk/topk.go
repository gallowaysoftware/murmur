// Package topk is a Misra-Gries top-K heavy-hitters implementation of monoid.Monoid[[]byte].
//
// Misra-Gries maintains at most K counters and offers a worst-case error bound of
// n/(K+1) on each tracked count. Combine is associative and commutative — proved by
// the algorithm's invariant: after merging two sketches, decrementing all counters by
// the (K+1)th-largest count yields a valid Misra-Gries summary of the union stream.
//
// Wire format (compact, deterministic):
//
//	uint32 K
//	uint32 N (number of items)
//	repeat N: uint64 count, uint32 keyLen, bytes keyLen
//
// Phase 1 implementation is deterministic-order: items are sorted by descending count,
// ties broken by lexicographic key. This makes Combine output bit-stable, useful for
// CAS conditional writes in DDB BytesStore.
package topk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
)

// Item is a (Key, Count) pair returned by Items.
type Item struct {
	Key   string
	Count uint64
}

// DefaultK is the K parameter (number of heavy hitters retained) used by the
// no-arg TopK() factory and the Single() helper. Override with New(k) /
// SingleN(k, …) for pipelines that want a different capacity.
const DefaultK = 10

// TopK returns a Misra-Gries top-K monoid with the default K.
func TopK() monoid.Monoid[[]byte] { return New(DefaultK) }

// New returns a Misra-Gries top-K monoid with capacity k.
func New(k uint32) monoid.Monoid[[]byte] {
	if k == 0 {
		k = DefaultK
	}
	return topKMonoid{k: k}
}

type topKMonoid struct{ k uint32 }

func (m topKMonoid) Identity() []byte {
	return encode(m.k, nil)
}

func (m topKMonoid) Combine(a, b []byte) []byte {
	switch {
	case len(a) == 0:
		return b
	case len(b) == 0:
		return a
	}
	sa, errA := decode(a)
	sb, errB := decode(b)
	if errA != nil {
		return b
	}
	if errB != nil {
		return a
	}
	// Sum on key collision.
	counts := make(map[string]uint64, len(sa)+len(sb))
	for _, it := range sa {
		counts[it.Key] += it.Count
	}
	for _, it := range sb {
		counts[it.Key] += it.Count
	}
	items := make([]Item, 0, len(counts))
	for k, c := range counts {
		items = append(items, Item{Key: k, Count: c})
	}
	sortByCountDesc(items)

	// Misra-Gries merge: if more than K items, decrement everything by the (K+1)th count.
	if uint32(len(items)) > m.k {
		threshold := items[m.k].Count
		kept := items[:0]
		for _, it := range items {
			if it.Count > threshold {
				kept = append(kept, Item{Key: it.Key, Count: it.Count - threshold})
			}
		}
		items = kept
		if uint32(len(items)) > m.k {
			items = items[:m.k]
		}
	}
	return encode(m.k, items)
}

func (topKMonoid) Kind() monoid.Kind { return monoid.KindTopK }

// Single returns the marshaled sketch with exactly one item: (key, count=1).
func Single(key string) []byte {
	return SingleN(DefaultK, key, 1)
}

// SingleN returns the marshaled sketch sized for k, containing one item with the given count.
func SingleN(k uint32, key string, count uint64) []byte {
	if k == 0 {
		k = DefaultK
	}
	return encode(k, []Item{{Key: key, Count: count}})
}

// Items returns the items in the marshaled sketch, sorted by descending count.
func Items(b []byte) ([]Item, error) {
	if len(b) == 0 {
		return nil, nil
	}
	return decode(b)
}

// --- wire format ---

func encode(k uint32, items []Item) []byte {
	// Sort for deterministic output.
	out := make([]Item, len(items))
	copy(out, items)
	sortByCountDesc(out)

	buf := bytes.NewBuffer(make([]byte, 0, 8+len(items)*32))
	binary.Write(buf, binary.LittleEndian, k)
	binary.Write(buf, binary.LittleEndian, uint32(len(out)))
	for _, it := range out {
		binary.Write(buf, binary.LittleEndian, it.Count)
		binary.Write(buf, binary.LittleEndian, uint32(len(it.Key)))
		buf.WriteString(it.Key)
	}
	return buf.Bytes()
}

func decode(b []byte) ([]Item, error) {
	r := bytes.NewReader(b)
	var k, n uint32
	if err := binary.Read(r, binary.LittleEndian, &k); err != nil {
		return nil, fmt.Errorf("topk decode K: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return nil, fmt.Errorf("topk decode N: %w", err)
	}
	items := make([]Item, 0, n)
	for i := uint32(0); i < n; i++ {
		var count uint64
		var keyLen uint32
		if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
			return nil, fmt.Errorf("topk decode count[%d]: %w", i, err)
		}
		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			return nil, fmt.Errorf("topk decode keyLen[%d]: %w", i, err)
		}
		keyBytes := make([]byte, keyLen)
		if _, err := r.Read(keyBytes); err != nil {
			return nil, fmt.Errorf("topk decode key[%d]: %w", i, err)
		}
		items = append(items, Item{Key: string(keyBytes), Count: count})
	}
	return items, nil
}

func sortByCountDesc(items []Item) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].Count != items[j].Count {
			return items[i].Count > items[j].Count
		}
		return items[i].Key < items[j].Key
	})
}
