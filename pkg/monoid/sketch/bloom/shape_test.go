// Bloom's (m, k) shape is written into every marshaled filter and read back out
// of it, so the shape a monoid was constructed with never reaches the merge —
// UnmarshalBinary replaces it. These tests pin down what happens when the
// shapes disagree, which used to be nothing at all.
package bloom_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/bloom"
)

func TestBloom_IdentityMergesAnOperandOfAnyShape(t *testing.T) {
	// The pipeline that hits this: a monoid built with NewWithCapacity, a value
	// extractor that calls the default-sized Single. Every merge was
	// Combine(storedIdentity, delta) with two different shapes, which returned
	// the identity — so the stored row was the empty filter forever and every
	// membership question answered false.
	var errs []error
	m := bloom.NewWithCapacity(1_000, 0.01, bloom.WithDecodeErrorHandler(func(err error) {
		errs = append(errs, err)
	}))

	delta := bloom.Single([]byte("device-0"))
	if got := m.Combine(m.Identity(), delta); !bloom.Contains(got, []byte("device-0")) {
		t.Errorf("Combine(Identity, delta) dropped the element; identity must not impose a shape")
	}
	if got := m.Combine(delta, m.Identity()); !bloom.Contains(got, []byte("device-0")) {
		t.Errorf("Combine(delta, Identity) dropped the element")
	}
	if len(errs) != 0 {
		t.Errorf("merging against the identity is not a mismatch: %v", errs)
	}

	// And the default helpers agree with each other about shape, which is what
	// keeps a default-sized pipeline off this path entirely.
	capSingle, kSingle, _, err := bloom.Inspect(bloom.Single([]byte("x")))
	if err != nil {
		t.Fatalf("Inspect(Single): %v", err)
	}
	capNew, kNew, _, err := bloom.Inspect(bloom.NewSingle(bloom.DefaultCapacity, bloom.DefaultFPR, []byte("x")))
	if err != nil {
		t.Fatalf("Inspect(NewSingle): %v", err)
	}
	if capSingle != capNew || kSingle != kNew {
		t.Errorf("Single shape (m=%d, k=%d) != default NewSingle shape (m=%d, k=%d)", capSingle, kSingle, capNew, kNew)
	}
	var defaultErrs []error
	def := bloom.Bloom(bloom.WithDecodeErrorHandler(func(err error) { defaultErrs = append(defaultErrs, err) }))
	if got := def.Combine(bloom.Single([]byte("a")), bloom.Single([]byte("b"))); !bloom.Contains(got, []byte("a")) {
		t.Error("default monoid lost an element merging two default Singles")
	}
	if len(defaultErrs) != 0 {
		t.Errorf("default monoid merging default Singles must not report: %v", defaultErrs)
	}
}

func TestBloom_ShapeMismatchIsReported(t *testing.T) {
	// Two real filters of different shapes cannot be OR'd — the bit arrays are
	// different lengths. Combine keeps the left operand, which is defensible;
	// doing it without a word to anybody is what made a capacity typo look
	// like "the data just isn't there".
	var errs []error
	m := bloom.NewWithCapacity(1_000, 0.01, bloom.WithDecodeErrorHandler(func(err error) {
		errs = append(errs, err)
	}))

	small := bloom.NewSingle(1_000, 0.01, []byte("small"))
	large := bloom.Single([]byte("large")) // DefaultCapacity — a much wider filter

	smallCap, _, _, err := bloom.Inspect(small)
	if err != nil {
		t.Fatalf("Inspect(small): %v", err)
	}
	largeCap, _, _, err := bloom.Inspect(large)
	if err != nil {
		t.Fatalf("Inspect(large): %v", err)
	}
	if smallCap == largeCap {
		t.Fatalf("fixture is broken: both filters are m=%d", smallCap)
	}

	got := m.Combine(small, large)

	if len(errs) != 1 {
		t.Fatalf("shape-mismatch reports: got %d, want exactly 1 — %v", len(errs), errs)
	}
	msg := errs[0].Error()
	for _, want := range []string{strconv.FormatUint(smallCap, 10), strconv.FormatUint(largeCap, 10)} {
		if !strings.Contains(msg, want) {
			t.Errorf("error must name BOTH shapes: got %q, missing m=%s", msg, want)
		}
	}
	if !bloom.Contains(got, []byte("small")) {
		t.Error("Combine must return the left operand unmodified on a mismatch")
	}
}

func TestBloom_OperandsThatIgnoreTheConfiguredShapeAreReported(t *testing.T) {
	// The case that made NewWithCapacity's parameters decorative: the operands
	// agree with EACH OTHER, so the merge is a well-defined bitwise OR and
	// nothing goes wrong at the bit level — they just are not the shape the
	// caller configured. A pipeline built with NewWithCapacity(1_000, 0.01)
	// whose value extractor calls the default-sized Single() aggregates
	// DefaultCapacity filters forever, at a false-positive rate nothing in the
	// configuration predicts, and used to do it in total silence.
	var errs []error
	m := bloom.NewWithCapacity(1_000, 0.01, bloom.WithDecodeErrorHandler(func(err error) {
		errs = append(errs, err)
	}))

	// Both operands are DefaultCapacity — matching each other, not the monoid.
	got := m.Combine(bloom.Single([]byte("a")), bloom.Single([]byte("b")))

	// The merge still happens: refusing it would throw away real data over a
	// sizing disagreement.
	if !bloom.Contains(got, []byte("a")) || !bloom.Contains(got, []byte("b")) {
		t.Error("Combine must still OR two identically-shaped operands")
	}

	if len(errs) != 1 {
		t.Fatalf("configured-shape reports: got %d, want exactly 1 — %v", len(errs), errs)
	}
	msg := errs[0].Error()

	wantCap, wantK, _, err := bloom.Inspect(bloom.Single([]byte("x")))
	if err != nil {
		t.Fatalf("Inspect(Single): %v", err)
	}
	configuredCap, configuredK, _, err := bloom.Inspect(bloom.NewSingle(1_000, 0.01, []byte("x")))
	if err != nil {
		t.Fatalf("Inspect(NewSingle): %v", err)
	}
	if wantCap == configuredCap && wantK == configuredK {
		t.Fatalf("fixture is broken: both shapes are (m=%d, k=%d)", wantCap, wantK)
	}
	// The report has to name both shapes, or it cannot tell the reader which
	// half of the pipeline to go fix.
	for _, want := range []string{
		strconv.FormatUint(wantCap, 10),
		strconv.FormatUint(configuredCap, 10),
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error must name the operands' shape AND the configured one: got %q, missing m=%s", msg, want)
		}
	}

	// A correctly-sized pipeline stays silent — this must not fire on every
	// merge of a pipeline that is doing nothing wrong.
	var quiet []error
	ok := bloom.NewWithCapacity(1_000, 0.01, bloom.WithDecodeErrorHandler(func(err error) {
		quiet = append(quiet, err)
	}))
	sized := ok.Combine(
		bloom.NewSingle(1_000, 0.01, []byte("a")),
		bloom.NewSingle(1_000, 0.01, []byte("b")),
	)
	if !bloom.Contains(sized, []byte("a")) || !bloom.Contains(sized, []byte("b")) {
		t.Error("correctly-sized merge lost an element")
	}
	if len(quiet) != 0 {
		t.Errorf("a correctly-sized pipeline must not report: %v", quiet)
	}
}

// There is deliberately no TestBloom_ShapeMismatchWithoutHandlerStillRecovers.
// It asserted that a nil handler still keeps the left operand and does not
// panic, which is exactly what the code did before any of this — it passed
// against the unfixed package, so it could not have caught a regression in the
// fix it was filed under.
//
// The nil-handler path is not uncovered: TestBloom_NonDefaultCapacityMonoid in
// pkg/monoid/monoidlaws builds NewWithCapacity(2_000, 0.001) with no handler
// and merges operands sized (1_000, 0.01), so the associativity and identity
// fuzzers drive every reportDecodeError call site in Combine with
// onDecodeErr == nil, thousands of times per run.
