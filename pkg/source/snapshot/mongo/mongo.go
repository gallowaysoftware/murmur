// Package mongo provides a Mongo-backed implementation of snapshot.SnapshotSource for
// Murmur's Bootstrap execution mode.
//
// Pattern (Debezium-style):
//
//  1. CaptureHandoff opens a Change Stream against the database (no iteration), reads
//     the start-of-stream resume token, and closes. The token is the "where to pick up
//     live streaming from after bootstrap" marker — handed to the live source on
//     transition.
//  2. Scan iterates the entire collection via Find with default batch size. Each
//     document is decoded into T and emitted as a source.Record with EventID = the
//     document's _id (so re-runs are idempotent under at-least-once dedup).
//  3. Resume re-runs Scan; at-least-once dedup at the state store handles duplicates.
//
// The Mongo replica set must be initiated (rs.initiate) before Change Streams work.
// Standalone Mongo is fine for plain Find but does not support resume tokens.
package mongo

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot"
)

// Config configures a Mongo snapshot source.
type Config[T any] struct {
	URI        string
	Database   string
	Collection string
	// Filter, if non-nil, is passed as the Find filter and the Watch matchStage. Use
	// for partial snapshots (e.g., {"tenant": "x"}). nil means scan everything.
	Filter bson.M
	// Decode converts a raw BSON document to T. Use BSONDecoder[T]() for the default
	// driver-based decoding (struct tags); supply your own for custom shapes.
	Decode Decoder[T]
}

// Decoder converts a raw BSON document to a typed Record value.
type Decoder[T any] func(bson.Raw) (T, error)

// BSONDecoder returns a Decoder that uses the Mongo driver's reflection-based
// unmarshaling. T should carry `bson:"..."` tags as needed.
func BSONDecoder[T any]() Decoder[T] {
	return func(b bson.Raw) (T, error) {
		var v T
		if err := bson.Unmarshal(b, &v); err != nil {
			return v, fmt.Errorf("mongo bson unmarshal: %w", err)
		}
		return v, nil
	}
}

// Source implements snapshot.SnapshotSource for a single Mongo collection.
type Source[T any] struct {
	client   *mongo.Client
	database string
	collName string
	filter   bson.M
	decode   Decoder[T]
}

// NewSource connects to the Mongo cluster and returns a SnapshotSource for the given
// collection. The returned Source owns the underlying mongo.Client; call Close to
// disconnect.
func NewSource[T any](ctx context.Context, cfg Config[T]) (*Source[T], error) {
	if cfg.Decode == nil {
		return nil, errors.New("mongo snapshot: Decode is required")
	}
	if cfg.Database == "" || cfg.Collection == "" {
		return nil, errors.New("mongo snapshot: Database and Collection are required")
	}
	cl, err := mongo.Connect(options.Client().ApplyURI(cfg.URI))
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}
	return &Source[T]{
		client:   cl,
		database: cfg.Database,
		collName: cfg.Collection,
		filter:   cfg.Filter,
		decode:   cfg.Decode,
	}, nil
}

// CaptureHandoff opens a Change Stream against the collection (no iteration) just to
// read the start-of-stream resume token. Returns the token as a HandoffToken (BSON
// bytes). Requires the Mongo deployment to be a replica set.
func (s *Source[T]) CaptureHandoff(ctx context.Context) (snapshot.HandoffToken, error) {
	pipeline := mongo.Pipeline{}
	if s.filter != nil {
		pipeline = mongo.Pipeline{{{Key: "$match", Value: s.filter}}}
	}
	cs, err := s.client.Database(s.database).Collection(s.collName).Watch(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("mongo Watch: %w", err)
	}
	defer cs.Close(ctx)
	tok := cs.ResumeToken()
	if tok == nil {
		return nil, errors.New("mongo: change stream returned nil resume token")
	}
	return snapshot.HandoffToken(tok), nil
}

// Scan reads every document from the collection (filtered by cfg.Filter if set), decodes
// to T, and emits source.Records into out. The cursor's batch size is left at the driver
// default; callers concerned about memory or backpressure can wrap out in a buffered
// channel sized to taste. Each Record's Ack is a no-op — bootstrap state is committed by
// the Bootstrap runtime, not per-record at the source.
func (s *Source[T]) Scan(ctx context.Context, out chan<- source.Record[T]) error {
	coll := s.client.Database(s.database).Collection(s.collName)
	filter := s.filter
	if filter == nil {
		filter = bson.M{}
	}
	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		return fmt.Errorf("mongo Find: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		raw := cursor.Current
		v, err := s.decode(raw)
		if err != nil {
			// Skip undecodable documents; in production we'd push to a DLQ.
			continue
		}
		idStr := extractID(raw)
		rec := source.Record[T]{
			EventID:      fmt.Sprintf("%s/%s/%s", s.database, s.collName, idStr),
			PartitionKey: idStr,
			Value:        v,
			Ack:          func() error { return nil },
		}
		select {
		case out <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("mongo cursor: %w", err)
	}
	return nil
}

// Resume re-runs Scan from the beginning. At-least-once dedup at the state-store level
// (per-event-ID) handles re-emitted documents safely. A future enhancement could thread
// a primary-key watermark through marker for true incremental snapshots.
func (s *Source[T]) Resume(ctx context.Context, marker []byte, out chan<- source.Record[T]) error {
	_ = marker // placeholder for incremental-snapshot watermarking
	return s.Scan(ctx, out)
}

// Name returns "mongo:<db>.<collection>".
func (s *Source[T]) Name() string {
	return fmt.Sprintf("mongo:%s.%s", s.database, s.collName)
}

// Close disconnects the underlying client.
func (s *Source[T]) Close() error {
	return s.client.Disconnect(context.Background())
}

// extractID pulls the _id field out of a raw BSON document and renders it as a string.
// Used to derive EventID; falls back to the raw bytes if _id is missing.
func extractID(raw bson.Raw) string {
	idElem, err := raw.LookupErr("_id")
	if err != nil {
		return string(raw)
	}
	if oid, ok := idElem.ObjectIDOK(); ok {
		return oid.Hex()
	}
	if str, ok := idElem.StringValueOK(); ok {
		return str
	}
	if i, ok := idElem.Int64OK(); ok {
		return fmt.Sprintf("%d", i)
	}
	if i, ok := idElem.Int32OK(); ok {
		return fmt.Sprintf("%d", i)
	}
	// Fallback: raw bytes.
	return string(idElem.Value)
}

// Compile-time check.
var _ snapshot.SnapshotSource[any] = (*Source[any])(nil)
