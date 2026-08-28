// Bloom's (m, k) shape is written into every marshaled filter and read back
// out of it, so the monoid's own parameters survive exactly as long as the call
// to bloom.New before UnmarshalBinary overwrites them. These tests pin down
// what happens when the shapes disagree — which used to be nothing at all.
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

func TestBloom_ShapeMismatchWithoutHandlerStillRecovers(t *testing.T) {
	// The hook is optional; with none installed the behaviour must be exactly
	// as before — keep the left operand, do not panic.
	m := bloom.NewWithCapacity(1_000, 0.01)
	small := bloom.NewSingle(1_000, 0.01, []byte("small"))
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Combine panicked with no handler installed: %v", r)
		}
	}()
	if got := m.Combine(small, bloom.Single([]byte("large"))); !bloom.Contains(got, []byte("small")) {
		t.Error("Combine must keep the left operand")
	}
}
