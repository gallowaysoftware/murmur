// Package source defines the live-event Source contract that Murmur pipelines read
// from. Concrete implementations live in subpackages (source/kinesis, source/kafka).
//
// Sources here are streaming/live. Snapshot bootstraps live in source/snapshot, and
// replay drivers (which feed historical events through the same pipeline DSL) live in
// pkg/replay.
package source

import (
	"context"
	"time"
)

// Record is the unit of work the streaming runtime hands to the pipeline. Implementations
// of Source decode their wire format into Records before yielding them.
type Record[T any] struct {
	// EventID is a stable identifier used for at-least-once dedup. Sources should derive
	// this from the underlying message (Kinesis sequence number, Kafka topic+partition+
	// offset) so duplicates are detectable.
	EventID string

	// EventTime is the time the upstream system attaches to this record. Used by windowed
	// aggregations for bucket assignment. Falls back to processing-time when missing.
	EventTime time.Time

	// PartitionKey, when nonempty, is the upstream partitioning key (Kinesis partition
	// key, Kafka message key). Useful for diagnostics and for sources that want to
	// surface partition affinity to the runtime.
	PartitionKey string

	// Value is the decoded payload.
	Value T

	// Ack must be called once the record has been fully processed (state-update committed,
	// dedup entry written). Sources use Ack to advance their checkpoint position.
	Ack func() error
}

// Source is a live event reader. Implementations must support graceful shutdown via the
// supplied context and yield records until the context is canceled.
type Source[T any] interface {
	// Read yields records to the consumer. The consumer must call Ack on each record after
	// committing its effects. Errors are returned to the caller; a Source may also surface
	// retriable errors as a sentinel and continue.
	Read(ctx context.Context, out chan<- Record[T]) error

	// Name returns a stable identifier for logging and metrics (e.g. "kinesis:events-stream").
	Name() string

	// Close releases any underlying resources.
	Close() error
}
