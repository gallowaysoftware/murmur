package autoscale_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	"github.com/gallowaysoftware/murmur/pkg/observability/autoscale"
)

func TestNewCloudWatchEmitter_RequiresClient(t *testing.T) {
	_, err := autoscale.NewCloudWatchEmitter(autoscale.CWConfig{
		Namespace: "Murmur",
	})
	if err == nil {
		t.Error("expected error when Client is nil")
	}
}

func TestNewCloudWatchEmitter_RequiresNamespace(t *testing.T) {
	_, err := autoscale.NewCloudWatchEmitter(autoscale.CWConfig{
		Client: &cloudwatch.Client{},
	})
	if err == nil {
		t.Error("expected error when Namespace is empty")
	}
}

func TestNewCloudWatchEmitter_AcceptsExtraDimensions(t *testing.T) {
	em, err := autoscale.NewCloudWatchEmitter(autoscale.CWConfig{
		Client:    &cloudwatch.Client{},
		Namespace: "Murmur",
		Unit:      types.StandardUnitCount,
		ExtraDimensions: map[string]string{
			"Environment": "prod",
			"Region":      "us-east-1",
		},
	})
	if err != nil {
		t.Fatalf("NewCloudWatchEmitter: %v", err)
	}
	if em == nil {
		t.Fatal("nil emitter returned")
	}
	if err := em.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
