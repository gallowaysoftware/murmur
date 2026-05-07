package sparkconnect_test

import (
	"context"
	"strings"
	"testing"

	"github.com/gallowaysoftware/murmur/pkg/exec/batch/sparkconnect"
)

func TestRunInt64Sum_Validation(t *testing.T) {
	cases := []struct {
		name    string
		cfg     sparkconnect.Config
		wantSub string
	}{
		{"missing remote", sparkconnect.Config{SQL: "SELECT 1"}, "Remote"},
		{"missing sql", sparkconnect.Config{Remote: "sc://localhost:15002"}, "SQL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := sparkconnect.RunInt64Sum(context.Background(), tc.cfg, nil, 0)
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("got %v, want error mentioning %q", err, tc.wantSub)
			}
		})
	}
}

// Note: full end-to-end testing requires a Spark Connect server backed by a Spark
// cluster. Intentionally not part of the docker-compose stack; integration tests
// run against the GSS persistent EMR cluster.
