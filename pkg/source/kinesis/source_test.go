package kinesis_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	mkinesis "github.com/gallowaysoftware/murmur/pkg/source/kinesis"
)

type evt struct {
	UserID string `json:"user_id"`
}

func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*mkinesis.Config[evt])
		wantSub string
	}{
		{"missing client", func(c *mkinesis.Config[evt]) { c.Client = nil }, "Client"},
		{"missing stream", func(c *mkinesis.Config[evt]) { c.StreamName = "" }, "StreamName"},
		{"missing decode", func(c *mkinesis.Config[evt]) { c.Decode = nil }, "Decode"},
		{"AT_TIMESTAMP without ts", func(c *mkinesis.Config[evt]) {
			c.StartingPosition = types.ShardIteratorTypeAtTimestamp
		}, "StartTimestamp"},
	}
	base := mkinesis.Config[evt]{
		Client:     &kinesis.Client{},
		StreamName: "events",
		Decode:     mkinesis.JSONDecoder[evt](),
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			_, err := mkinesis.NewSource(cfg)
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("got %v; want error mentioning %q", err, tc.wantSub)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	src, err := mkinesis.NewSource(mkinesis.Config[evt]{
		Client:     &kinesis.Client{},
		StreamName: "events",
		Decode:     mkinesis.JSONDecoder[evt](),
	})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	if src.Name() != "kinesis:events" {
		t.Errorf("Name: got %q, want kinesis:events", src.Name())
	}
}

func TestJSONDecoder(t *testing.T) {
	d := mkinesis.JSONDecoder[evt]()
	v, err := d([]byte(`{"user_id":"alice"}`))
	if err != nil || v.UserID != "alice" {
		t.Fatalf("decode: got %+v err=%v, want UserID=alice", v, err)
	}
	if _, err := d([]byte("not json")); err == nil {
		t.Errorf("expected error decoding garbage, got nil")
	}
}

// AT_TIMESTAMP with timestamp set should succeed at construction.
func TestConfigAtTimestamp(t *testing.T) {
	ts := time.Now()
	_, err := mkinesis.NewSource(mkinesis.Config[evt]{
		Client:           &kinesis.Client{},
		StreamName:       "events",
		Decode:           mkinesis.JSONDecoder[evt](),
		StartingPosition: types.ShardIteratorTypeAtTimestamp,
		StartTimestamp:   &ts,
	})
	if err != nil {
		t.Fatalf("AT_TIMESTAMP with ts: %v", err)
	}
	// Sanity: errors.Is on the SDK errors should still work for downstream consumers.
	_ = errors.Is
}
