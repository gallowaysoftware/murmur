// Package replay defines the ReplayDriver contract for Murmur's backfill mode.
//
// A ReplayDriver feeds historical events into the same pipeline DSL that the live
// streaming runtime uses, applying the same monoid Combine. Replays write into a fresh
// state table (so the live path is undisturbed) and finish with an atomic state-table
// swap. Concrete implementations:
//
//   - replay/kafka: reads a Kafka topic from offset N to offset M (using franz-go).
//     With Kafka tiered storage the offsets can span historical S3-backed segments
//     transparently.
//   - replay/s3: reads Firehose- or MSK-Connect-partitioned S3 archives and feeds the
//     records through the pipeline at full throughput. The standard "kappa replay"
//     for Kinesis-backed pipelines whose source retention is shorter than the desired
//     replay window.
package replay

import (
	"context"

	"github.com/gallowaysoftware/murmur/pkg/source"
)

// Driver replays a historical range of events through a pipeline.
type Driver[T any] interface {
	// Replay yields historical records into out. The driver is expected to drive its
	// internal sources (S3 file iteration, Kafka offset seeks) to completion or until
	// ctx is canceled, then close out.
	Replay(ctx context.Context, out chan<- source.Record[T]) error

	// Name returns a stable identifier for logging and metrics.
	Name() string

	// Close releases any underlying resources.
	Close() error
}
