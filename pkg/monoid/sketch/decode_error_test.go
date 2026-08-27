// Package sketch's three monoids each recover from a decode failure by
// returning whichever operand decoded and discarding the other. These tests
// pin that behaviour down and, more importantly, assert it is now REPORTABLE.
//
// The recovery itself is defensible: Monoid.Combine has no error return, so
// the alternatives are corrupting the merged state or panicking a worker
// mid-batch. What was not defensible was doing it silently — STABILITY.md's
// sharp-edge #1 claimed this was closed while all three still dropped an
// operand with no error, no metric, and no log line.
package sketch_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/bloom"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/hll"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/topk"
)

// garbage is non-empty bytes that will not decode as any sketch. It must be
// non-empty: every Combine short-circuits on a zero-length operand, so an
// empty slice exercises the identity path rather than the decode path.
var garbage = []byte{0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0xf9, 0xf8}

type capture struct{ errs []error }

func (c *capture) fn(err error) { c.errs = append(c.errs, err) }

func TestSketchCombine_DecodeFailureIsReported(t *testing.T) {
	cases := []struct {
		name  string
		build func(report func(error)) monoid.Monoid[[]byte]
		good  func() []byte
		want  string
	}{
		{
			name:  "hll",
			build: func(r func(error)) monoid.Monoid[[]byte] { return hll.HLL(hll.WithDecodeErrorHandler(r)) },
			good:  func() []byte { return hll.Single([]byte("a")) },
			want:  "hll:",
		},
		{
			name:  "topk",
			build: func(r func(error)) monoid.Monoid[[]byte] { return topk.New(8, topk.WithDecodeErrorHandler(r)) },
			good:  func() []byte { return topk.SingleN(8, "a", 1) },
			want:  "topk:",
		},
		{
			name: "bloom",
			build: func(r func(error)) monoid.Monoid[[]byte] {
				return bloom.NewWithCapacity(1000, 0.01, bloom.WithDecodeErrorHandler(r))
			},
			good: func() []byte { return bloom.NewSingle(1000, 0.01, []byte("a")) },
			want: "bloom:",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name+"/left_operand_garbage", func(t *testing.T) {
			var c capture
			m := tc.build(c.fn)
			good := tc.good()

			got := m.Combine(garbage, good)

			if len(c.errs) != 1 {
				t.Fatalf("decode errors reported: got %d, want 1 — a dropped operand must not be silent", len(c.errs))
			}
			if !strings.Contains(c.errs[0].Error(), tc.want) {
				t.Errorf("error should name the sketch: got %q, want it to contain %q", c.errs[0], tc.want)
			}
			if !strings.Contains(c.errs[0].Error(), "left") {
				t.Errorf("error should say WHICH operand failed: got %q", c.errs[0])
			}
			// The surviving operand is returned so the merge degrades rather
			// than corrupting state.
			if string(got) != string(good) {
				t.Error("Combine must return the operand that decoded")
			}
		})

		t.Run(tc.name+"/right_operand_garbage", func(t *testing.T) {
			var c capture
			m := tc.build(c.fn)
			good := tc.good()

			got := m.Combine(good, garbage)

			if len(c.errs) != 1 {
				t.Fatalf("decode errors reported: got %d, want 1", len(c.errs))
			}
			if !strings.Contains(c.errs[0].Error(), "right") {
				t.Errorf("error should say WHICH operand failed: got %q", c.errs[0])
			}
			if string(got) != string(good) {
				t.Error("Combine must return the operand that decoded")
			}
		})

		t.Run(tc.name+"/no_handler_still_recovers", func(t *testing.T) {
			// The hook is optional. Without one the behaviour must be exactly
			// as before — recover silently rather than panic.
			m := tc.build(nil)
			good := tc.good()
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Combine panicked with no handler installed: %v", r)
				}
			}()
			if got := m.Combine(garbage, good); string(got) != string(good) {
				t.Error("Combine must still return the decodable operand")
			}
		})

		t.Run(tc.name+"/healthy_merge_reports_nothing", func(t *testing.T) {
			var c capture
			m := tc.build(c.fn)
			if got := m.Combine(tc.good(), tc.good()); len(got) == 0 {
				t.Fatal("healthy merge produced empty output")
			}
			if len(c.errs) != 0 {
				t.Errorf("handler fired on a healthy merge: %v", c.errs)
			}
		})
	}
}

func TestSketchCombine_EmptyOperandIsNotADecodeError(t *testing.T) {
	// A zero-length operand is the identity short-circuit, not a failure. If
	// this reported, every pipeline's first merge against an absent key would
	// emit a spurious error for the whole soak.
	var c capture
	m := topk.New(8, topk.WithDecodeErrorHandler(c.fn))
	_ = m.Combine(nil, topk.SingleN(8, "a", 1))
	_ = m.Combine(topk.SingleN(8, "a", 1), nil)
	if len(c.errs) != 0 {
		t.Errorf("empty operands must not report a decode error: %v", c.errs)
	}
}

func TestSketchCombine_HandlerErrorsAreWrapped(t *testing.T) {
	// errors.Unwrap must reach the underlying decode failure so callers can
	// branch on it rather than string-matching.
	var c capture
	m := hll.HLL(hll.WithDecodeErrorHandler(c.fn))
	_ = m.Combine(garbage, hll.Single([]byte("a")))
	if len(c.errs) != 1 {
		t.Fatalf("errors: got %d, want 1", len(c.errs))
	}
	if errors.Unwrap(c.errs[0]) == nil {
		t.Error("decode error should wrap the underlying cause")
	}
}
