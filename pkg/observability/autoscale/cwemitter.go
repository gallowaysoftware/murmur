package autoscale

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

// CloudWatchEmitter publishes scaling-signal measurements to CloudWatch
// custom metrics. The reference Emitter implementation; production AWS
// users will typically wire this directly.
//
// Each Emit issues one PutMetricData call. For high-frequency emits
// (sub-second), batch via your own buffer in front. Default usage is
// once-per-second-or-slower, well under the API's 150 req/s default
// quota.
//
// Configure ECS Fargate target tracking against the namespace+metric
// you publish here:
//
//	resource "aws_appautoscaling_policy" "lag" {
//	  policy_type = "TargetTrackingScaling"
//	  target_tracking_scaling_policy_configuration {
//	    target_value = 1000   # target 1000 events lag per replica
//	    customized_metric_specification {
//	      metric_name = "ConsumerLag"
//	      namespace   = "Murmur"
//	      statistic   = "Average"
//	      dimensions {
//	        name  = "Pipeline"
//	        value = "page_views"
//	      }
//	    }
//	  }
//	}
type CloudWatchEmitter struct {
	client    *cloudwatch.Client
	namespace string
	unit      cwtypes.StandardUnit
	// extraDims is appended to every Emit's per-call dimensions. Useful
	// for static labels like environment / region / cluster.
	extraDims map[string]string
}

// CWConfig configures a CloudWatchEmitter.
type CWConfig struct {
	// Client is an SDK v2 CloudWatch client. The Emitter does not own
	// the client's lifecycle.
	Client *cloudwatch.Client

	// Namespace is the CloudWatch namespace for the published metric.
	// Convention: "Murmur" or "Murmur/<deployment>".
	Namespace string

	// Unit, when set, attaches a CloudWatch StandardUnit to each emit.
	// Common: cwtypes.StandardUnitCount (lag, queue depth),
	// cwtypes.StandardUnitMilliseconds (iterator age),
	// cwtypes.StandardUnitCountSecond (events/sec).
	// Defaults to None.
	Unit cwtypes.StandardUnit

	// ExtraDimensions are added to every emit. Use for static labels
	// (environment, region, cluster). Per-call dimensions from the
	// Signal are merged on top.
	ExtraDimensions map[string]string
}

// NewCloudWatchEmitter constructs the emitter. Validates required
// fields.
func NewCloudWatchEmitter(cfg CWConfig) (*CloudWatchEmitter, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("autoscale.NewCloudWatchEmitter: Client is required")
	}
	if cfg.Namespace == "" {
		return nil, fmt.Errorf("autoscale.NewCloudWatchEmitter: Namespace is required")
	}
	return &CloudWatchEmitter{
		client:    cfg.Client,
		namespace: cfg.Namespace,
		unit:      cfg.Unit,
		extraDims: cfg.ExtraDimensions,
	}, nil
}

// Emit publishes a single point to CloudWatch.
func (e *CloudWatchEmitter) Emit(ctx context.Context, name string, value float64, dims map[string]string) error {
	merged := e.mergeDims(dims)
	in := &cloudwatch.PutMetricDataInput{
		Namespace: aws.String(e.namespace),
		MetricData: []cwtypes.MetricDatum{{
			MetricName: aws.String(name),
			Value:      aws.Float64(value),
			Timestamp:  aws.Time(time.Now()),
			Unit:       e.unit,
			Dimensions: merged,
		}},
	}
	_, err := e.client.PutMetricData(ctx, in)
	if err != nil {
		return fmt.Errorf("cloudwatch PutMetricData %s/%s: %w", e.namespace, name, err)
	}
	return nil
}

// Close is a no-op; the underlying SDK client is owned by the caller.
func (e *CloudWatchEmitter) Close() error { return nil }

// mergeDims combines the emitter's static dimensions with the per-call
// ones. Per-call wins on conflict. Output is sorted by name for stable
// CloudWatch ordering — CW treats two identical-by-set-of-dimensions
// metrics as the same time series.
func (e *CloudWatchEmitter) mergeDims(perCall map[string]string) []cwtypes.Dimension {
	if len(e.extraDims) == 0 && len(perCall) == 0 {
		return nil
	}
	merged := make(map[string]string, len(e.extraDims)+len(perCall))
	for k, v := range e.extraDims {
		merged[k] = v
	}
	for k, v := range perCall {
		merged[k] = v
	}
	out := make([]cwtypes.Dimension, 0, len(merged))
	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := merged[k]
		out = append(out, cwtypes.Dimension{
			Name:  aws.String(k),
			Value: aws.String(v),
		})
	}
	return out
}

// Compile-time check.
var _ Emitter = (*CloudWatchEmitter)(nil)
