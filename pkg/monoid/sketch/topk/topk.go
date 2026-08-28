// Package topk is a Misra-Gries top-K heavy-hitters implementation of monoid.Monoid[[]byte].
//
// Misra-Gries maintains at most K counters and offers a worst-case error bound of
// n/(K+1) on each tracked count. Combine is associative and commutative — proved by
// the algorithm's invariant: after merging two sketches, decrementing all counters by
// the (K+1)th-largest count yields a valid Misra-Gries summary of the union stream.
//
// Wire format (compact, deterministic):
//
//	uint32 K      — capacity in the low 30 bits, flags in the top two
//	uint32 N      — number of retained items
//	uint64 W      — total weight ingested (present iff flagWeight is set in K)
//	repeat N: uint64 count, uint32 keyLen, bytes keyLen
//
// W is what makes saturation visible. Without it a sketch that retained 0.1% of the
// stream is byte-indistinguishable from an exact one: both are N counters that sum to
// whatever survived. W is a plain associative sum of every weight ever fed in, so
// Inspect can divide the two and report coverage. Discarded mass is deliberately NOT
// accumulated — it is merge-order-dependent, and a merge-order-dependent field would
// break the associativity the monoid laws fuzz.
//
// Phase 1 implementation is deterministic-order: items are sorted by descending count,
// ties broken by lexicographic key. This makes Combine output bit-stable, useful for
// CAS conditional writes in DDB BytesStore.
package topk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
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

// Header flags live in the top two bits of the K word. K is a capacity, never
// anywhere near 2^30, so the room is free.
const (
	// flagWeight marks a header that carries the uint64 ingested-weight field.
	// Sketches written before the field existed have it clear, and decode reads
	// them without it — a rolling deploy must not have to throw away live rows.
	flagWeight uint32 = 1 << 31
	// flagPartialWeight marks a sketch that has absorbed at least one operand
	// written before flagWeight existed, so its weight is a lower bound. It is
	// sticky across merges: coverage derived from a short total would otherwise
	// read as "complete", which is the exact lie this field exists to prevent.
	flagPartialWeight uint32 = 1 << 30

	kMask uint32 = ^(flagWeight | flagPartialWeight)
)

// maxAdoptedK bounds the K that Combine will adopt from an operand's header. A
// wire K is untrusted input: one corrupt row claiming K=2^30 would switch off
// Misra-Gries eviction for that key entirely and let it grow until the store
// refuses the write. The monoid's own K is never capped — the caller chose it.
const maxAdoptedK uint32 = 1 << 16

// TopK returns a Misra-Gries top-K monoid with the default K.
func TopK() monoid.Monoid[[]byte] { return New(DefaultK) }

// New returns a Misra-Gries top-K monoid with capacity k. k is clamped to the
// wire format's 30-bit capacity field; zero means DefaultK.
func New(k uint32, opts ...Option) monoid.Monoid[[]byte] {
	mo := topKMonoid{k: normalizeK(k)}
	for _, o := range opts {
		o(&mo)
	}
	return mo
}

func normalizeK(k uint32) uint32 {
	switch {
	case k == 0:
		return DefaultK
	case k > kMask:
		return kMask
	default:
		return k
	}
}

// Option configures the top-K monoid.
type Option func(*topKMonoid)

// WithDecodeErrorHandler installs a callback invoked when Combine cannot
// decode one of its operands, or when the operands disagree with the monoid
// about K.
//
// Combine returns no error — the contract is Combine(a, b) V — so the only
// recovery on a decode failure is to return the operand that DID decode,
// silently discarding the other. That recovery is deliberate; doing it
// silently is not. Without this hook the affected key quietly loses counts
// with no error, no metric, and no log line.
//
// A K mismatch is reported through the same hook. It is not fatal — Combine
// merges at the widest K it can see rather than truncating the stored summary
// down to its own — but it means a writer and a reader were configured with
// different capacities, which is a bug worth a log line.
//
// The handler must be cheap and non-blocking; it runs on the merge path.
func WithDecodeErrorHandler(fn func(error)) Option {
	return func(m *topKMonoid) { m.onDecodeErr = fn }
}

type topKMonoid struct {
	k           uint32
	onDecodeErr func(error)
}

func (m topKMonoid) reportDecodeError(err error) {
	if m.onDecodeErr != nil {
		m.onDecodeErr(err)
	}
}

func (m topKMonoid) Identity() []byte {
	return encode(header{k: m.k}, nil)
}

func (m topKMonoid) Combine(a, b []byte) []byte {
	switch {
	case len(a) == 0:
		return b
	case len(b) == 0:
		return a
	}
	ha, sa, errA := decode(a)
	hb, sb, errB := decode(b)
	if errA != nil {
		// Keep the operand that decoded; report the one that didn't.
		m.reportDecodeError(fmt.Errorf("topk: decode left operand (%d bytes): %w", len(a), errA))
		return b
	}
	if errB != nil {
		m.reportDecodeError(fmt.Errorf("topk: decode right operand (%d bytes): %w", len(b), errB))
		return a
	}

	out := header{
		k:        m.effectiveK(ha.k, hb.k),
		ingested: addSaturating(ha.ingested, hb.ingested),
		partial:  ha.partial || hb.partial,
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
	if uint32(len(items)) > out.k {
		threshold := items[out.k].Count
		kept := items[:0]
		for _, it := range items {
			if it.Count > threshold {
				kept = append(kept, Item{Key: it.Key, Count: it.Count - threshold})
			}
		}
		items = kept
		if uint32(len(items)) > out.k {
			items = items[:out.k]
		}
	}
	return encode(out, items)
}

// effectiveK is the capacity the merge runs at: the widest of the monoid's own
// K and the two operands'.
//
// The wire K used to be read and thrown away, so a K=10 monoid merging a
// saturated K=32 row — a query client configured with the wrong capacity, or a
// pipeline whose K was lowered — truncated that row to 10 buckets and wrote the
// truncation back. The 22 discarded counters are not recoverable from anything
// downstream. Widening instead is safe: max() is itself associative, so the
// merge order still cannot change the result.
func (m topKMonoid) effectiveK(ka, kb uint32) uint32 {
	k := m.k
	if adopted := min(ka, maxAdoptedK); adopted > k {
		k = adopted
	}
	if adopted := min(kb, maxAdoptedK); adopted > k {
		k = adopted
	}
	if ka != m.k || kb != m.k {
		m.reportDecodeError(fmt.Errorf(
			"topk: K mismatch: monoid K=%d, left operand K=%d, right operand K=%d — merging at K=%d to avoid truncating the wider summary",
			m.k, ka, kb, k))
	}
	return k
}

func (topKMonoid) Kind() monoid.Kind { return monoid.KindTopK }

// Single returns the marshaled sketch with exactly one item: (key, count=1).
func Single(key string) []byte {
	return SingleN(DefaultK, key, 1)
}

// SingleN returns the marshaled sketch sized for k, containing one item with the given count.
func SingleN(k uint32, key string, count uint64) []byte {
	return encode(
		header{k: normalizeK(k), ingested: count},
		[]Item{{Key: key, Count: count}},
	)
}

// Items returns the items in the marshaled sketch, sorted by descending count.
//
// The counts are Misra-Gries lower bounds, not exact: each may understate the
// true count by up to n/(K+1), and a key absent from the result may still have
// occurred up to that many times. Use Inspect when you need n to size that
// bound, or to tell a complete answer from a saturated one.
func Items(b []byte) ([]Item, error) {
	if len(b) == 0 {
		return nil, nil
	}
	_, items, err := decode(b)
	return items, err
}

// Summary is the decoded view of a sketch: the heavy hitters it retained plus
// the mass accounting that says how much of the stream they actually cover.
//
// A sketch's item list alone cannot answer "is this the whole story?" — 32
// counters summing to 32 look exactly like an exact answer over 32 events, and
// are in fact what is left of 45,932. Ingested is the number that separates
// them.
type Summary struct {
	// K is the capacity recorded in the sketch's own header, which is not
	// necessarily the K of the monoid that read it.
	K uint32
	// Items are the retained heavy hitters, descending by count.
	Items []Item
	// Ingested is the total weight ever fed into this sketch, including the
	// mass that eviction has since discarded.
	Ingested uint64
	// Retained is the sum of Items' counts.
	Retained uint64
	// PartialWeight reports that Ingested is a lower bound: this sketch has
	// absorbed at least one operand written before the header carried a weight,
	// and that operand could only contribute what it had retained.
	PartialWeight bool
}

// Inspect decodes a marshaled sketch into a Summary. An empty input is the
// absent key and decodes to the zero Summary.
func Inspect(b []byte) (Summary, error) {
	if len(b) == 0 {
		return Summary{}, nil
	}
	h, items, err := decode(b)
	if err != nil {
		return Summary{}, err
	}
	s := Summary{K: h.k, Items: items, Ingested: h.ingested, PartialWeight: h.partial}
	for _, it := range items {
		s.Retained = addSaturating(s.Retained, it.Count)
	}
	return s, nil
}

// Coverage is the fraction of ingested weight the retained counters still
// account for, in [0, 1]. 1 means nothing was ever evicted and the counts are
// exact; 0.001 means the top-K is a sample of a stream 1000x its size and the
// counts below the heavy hitters are noise. An empty sketch reports 1.
//
// When PartialWeight is set this is an upper bound — the denominator is short.
func (s Summary) Coverage() float64 {
	if s.Ingested == 0 {
		return 1
	}
	c := float64(s.Retained) / float64(s.Ingested)
	if c > 1 {
		return 1
	}
	return c
}

// Saturated reports whether eviction has discarded any mass, i.e. whether the
// retained counts are estimates rather than exact counts.
func (s Summary) Saturated() bool { return s.Retained < s.Ingested }

// MaxError is the Misra-Gries error bound n/(K+1) for this sketch: no retained
// count understates its true count by more than this, and no key missing from
// Items occurred more than this many times.
func (s Summary) MaxError() uint64 {
	if s.K == 0 {
		return s.Ingested
	}
	return s.Ingested / (uint64(s.K) + 1)
}

// --- wire format ---

type header struct {
	k        uint32
	ingested uint64
	partial  bool
}

func encode(h header, items []Item) []byte {
	// Sort for deterministic output.
	out := make([]Item, len(items))
	copy(out, items)
	sortByCountDesc(out)

	kWord := (h.k & kMask) | flagWeight
	if h.partial {
		kWord |= flagPartialWeight
	}

	buf := bytes.NewBuffer(make([]byte, 0, 16+len(items)*32))
	// binary.Write to a bytes.Buffer never errors (Buffer.Write
	// always succeeds); the explicit _ = silences errcheck.
	_ = binary.Write(buf, binary.LittleEndian, kWord)
	_ = binary.Write(buf, binary.LittleEndian, uint32(len(out)))
	_ = binary.Write(buf, binary.LittleEndian, h.ingested)
	for _, it := range out {
		_ = binary.Write(buf, binary.LittleEndian, it.Count)
		_ = binary.Write(buf, binary.LittleEndian, uint32(len(it.Key)))
		buf.WriteString(it.Key)
	}
	return buf.Bytes()
}

func decode(b []byte) (header, []Item, error) {
	var h header
	r := bytes.NewReader(b)
	var kWord, n uint32
	if err := binary.Read(r, binary.LittleEndian, &kWord); err != nil {
		return h, nil, fmt.Errorf("topk decode K: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return h, nil, fmt.Errorf("topk decode N: %w", err)
	}
	h.k = kWord & kMask
	h.partial = kWord&flagPartialWeight != 0
	if kWord&flagWeight != 0 {
		if err := binary.Read(r, binary.LittleEndian, &h.ingested); err != nil {
			return h, nil, fmt.Errorf("topk decode W: %w", err)
		}
	} else {
		// Written before the header carried a weight. The retained counts are
		// the only evidence of stream size it has, so the total is filled in
		// from them below and flagged as the lower bound it is.
		h.partial = true
	}

	// n and keyLen come straight off the wire, so both must be bounded by what
	// the remaining input could possibly hold before they reach make().
	// Without this a corrupt or truncated sketch — a partially-written DDB
	// item, bytes from a different monoid — decodes n as up to 2^32-1 and
	// make([]Item, 0, n) attempts a ~100 GB allocation. That OOMs the worker
	// rather than degrading: Combine's error path exists precisely to survive
	// bad bytes, and it never gets reached.
	const minItemBytes = 8 + 4 // count + keyLen, with a zero-length key
	if maxItems := uint64(r.Len()) / minItemBytes; uint64(n) > maxItems {
		return h, nil, fmt.Errorf("topk decode: header claims %d items but only %d bytes remain (max %d)", n, r.Len(), maxItems)
	}

	items := make([]Item, 0, n)
	for i := uint32(0); i < n; i++ {
		var count uint64
		var keyLen uint32
		if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
			return h, nil, fmt.Errorf("topk decode count[%d]: %w", i, err)
		}
		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			return h, nil, fmt.Errorf("topk decode keyLen[%d]: %w", i, err)
		}
		if uint64(keyLen) > uint64(r.Len()) {
			return h, nil, fmt.Errorf("topk decode keyLen[%d]: claims %d bytes but only %d remain", i, keyLen, r.Len())
		}
		keyBytes := make([]byte, keyLen)
		// io.ReadFull, not r.Read: Reader.Read may return a short read without
		// error, which would silently truncate the key.
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return h, nil, fmt.Errorf("topk decode key[%d]: %w", i, err)
		}
		items = append(items, Item{Key: string(keyBytes), Count: count})
	}

	if kWord&flagWeight == 0 {
		for _, it := range items {
			h.ingested = addSaturating(h.ingested, it.Count)
		}
	}
	return h, items, nil
}

// addSaturating pins at MaxUint64 rather than wrapping. An ingested-weight
// counter that wrapped would report a coverage above 1 — worse than admitting
// it stopped counting.
func addSaturating(a, b uint64) uint64 {
	if s := a + b; s >= a {
		return s
	}
	return math.MaxUint64
}

func sortByCountDesc(items []Item) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].Count != items[j].Count {
			return items[i].Count > items[j].Count
		}
		return items[i].Key < items[j].Key
	})
}
