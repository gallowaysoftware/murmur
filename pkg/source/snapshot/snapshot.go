// Package snapshot defines the SnapshotSource contract used by Murmur's Bootstrap
// execution mode. SnapshotSources scan a source-of-truth datastore (Mongo, DynamoDB,
// JDBC, S3 dump) and emit synthetic events into the same monoid-combine logic the
// streaming runtime uses, populating initial state when the live event stream lacks
// full history (typical for CDC sources).
//
// Bootstrap mirrors Debezium's "incremental snapshot" pattern: chunked, watermarked,
// resumable, can run in parallel with live capture. Concrete implementations live in
// subpackages (snapshot/mongo, snapshot/dynamodb).
package snapshot

import (
	"context"

	"github.com/gallowaysoftware/murmur/pkg/source"
)

// HandoffToken is an opaque marker captured at the start of a snapshot that tells the
// live source where to resume from after bootstrap completes — without gap or duplicate.
//
//   - For Mongo: a Change Stream resume token captured before the collection scan.
//   - For DynamoDB: a Streams shard iterator position captured before the table scan.
//   - For JDBC + binlog CDC: a binlog position.
//
// The bootstrap runtime persists this token alongside its progress and hands it back to
// the live source on transition.
type HandoffToken []byte

// SnapshotSource scans a datastore and emits records as synthetic events. Implementations
// should support resumable, chunked progress so a long-running bootstrap can survive
// worker restarts.
type SnapshotSource[T any] interface {
	// CaptureHandoff captures the live-source resume position before scanning begins.
	// Called once at bootstrap start.
	CaptureHandoff(ctx context.Context) (HandoffToken, error)

	// Scan emits records via out until the snapshot is exhausted. Implementations should
	// honor ctx cancellation and yield records in stable, resumable chunks. Each record
	// must carry an EventID derived from the document's primary key so re-runs are
	// idempotent under at-least-once dedup.
	Scan(ctx context.Context, out chan<- source.Record[T]) error

	// Resume continues a previously-interrupted scan from the persisted progress marker.
	// Implementations that don't support resumption should restart from the beginning;
	// at-least-once dedup handles the duplicate emissions.
	Resume(ctx context.Context, marker []byte, out chan<- source.Record[T]) error

	// Name returns a stable identifier for logging and metrics.
	Name() string

	// Close releases any underlying resources.
	Close() error
}
