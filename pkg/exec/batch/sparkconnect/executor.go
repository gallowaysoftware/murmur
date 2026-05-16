// Package sparkconnect dispatches batch backfill aggregations against a persistent
// Spark cluster via Spark Connect.
//
// Phase 1 ships a "user-supplied SQL" executor: callers provide a Spark SQL aggregation
// that produces (key, value) rows, the executor runs it through Spark Connect, and the
// resulting DataFrame is streamed back into the pipeline's state store. This trades
// monoid-to-SQL codegen (Phase 2) for a much smaller surface that's testable
// end-to-end against a real Spark cluster.
//
// Dependency: github.com/apache/spark-connect-go (replaced in go.mod with Kyle's fork
// at github.com/pequalsnp/spark-connect-go which is production-validated at GSS).
//
// Typical use:
//
//	ddbStore := dynamodb.NewInt64SumStore(client, "page_views_v2")  // shadow table
//	if err := sparkconnect.RunInt64Sum(ctx, sparkconnect.Config{
//	    Remote: "sc://emr-master.internal:15002",
//	    SQL: `
//	        SELECT page_id AS pk, SUM(1) AS v
//	        FROM events
//	        WHERE ymd BETWEEN '2026-01-01' AND '2026-04-30'
//	        GROUP BY page_id
//	    `,
//	}, ddbStore, 0); err != nil { ... }
//
// After completion, call swap.Manager.SetActive to atomically advance the alias pointer
// from page_views_v1 to page_views_v2.
package sparkconnect

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apache/spark-connect-go/spark/sql"
	"github.com/apache/spark-connect-go/spark/sql/types"

	"github.com/gallowaysoftware/murmur/pkg/state"
)

// Config configures a SparkConnect executor.
type Config struct {
	// Remote is the Spark Connect endpoint, e.g. "sc://emr-master.internal:15002".
	Remote string

	// SQL is the aggregation query. It must produce two columns:
	//   pk (string) — the entity key
	//   v  (long)   — the int64 sum value
	// Window-bucketed pipelines should produce a third column `sk` (long) — the bucket id.
	// Phase 1 supports KindSum / KindCount only; KindHLL / KindTopK / KindBloom require
	// passing sketches through Spark UDFs and are queued for Phase 2.
	SQL string

	// HasBucket signals whether the SQL produces a `sk` column. Set to true for
	// windowed pipelines where the SQL aggregates per (entity, bucket) tuple.
	HasBucket bool
}

// RunInt64Sum executes the SQL via Spark Connect and writes each (pk, [sk,] v) row to
// the supplied state.Store[int64] using MergeUpdate. The MergeUpdate is additive (DDB
// atomic ADD), so this is safe to retry — at-least-once semantics at the store level
// handle duplicate row deliveries.
//
// If ttl > 0, every MergeUpdate sets that TTL on the underlying row (windowed bucket
// eviction).
func RunInt64Sum(ctx context.Context, cfg Config, store state.Store[int64], ttl time.Duration) error {
	if cfg.Remote == "" {
		return errors.New("sparkconnect: Remote is required")
	}
	if cfg.SQL == "" {
		return errors.New("sparkconnect: SQL is required")
	}
	session, err := sql.NewSessionBuilder().Remote(cfg.Remote).Build(ctx)
	if err != nil {
		return fmt.Errorf("spark connect session: %w", err)
	}
	// session.Stop's error is purely cleanup (release the Spark
	// Connect session); ignore it.
	defer func() { _ = session.Stop() }()

	df, err := session.Sql(ctx, cfg.SQL)
	if err != nil {
		return fmt.Errorf("spark sql: %w", err)
	}
	for r, iterErr := range df.All(ctx) {
		if iterErr != nil {
			return fmt.Errorf("spark row iter: %w", iterErr)
		}
		entity, err := stringField(r, "pk")
		if err != nil {
			return err
		}
		v, err := int64Field(r, "v")
		if err != nil {
			return err
		}
		k := state.Key{Entity: entity}
		if cfg.HasBucket {
			b, err := int64Field(r, "sk")
			if err != nil {
				return err
			}
			k.Bucket = b
		}
		if err := store.MergeUpdate(ctx, k, v, ttl); err != nil {
			return fmt.Errorf("store MergeUpdate %s/%d: %w", k.Entity, k.Bucket, err)
		}
	}
	return nil
}

// stringField pulls a string column out of a Row by name. Helper isolates the
// spark-connect-go Row API so we can stub it for unit tests if needed.
func stringField(r types.Row, name string) (string, error) {
	v := r.Value(name)
	if v == nil {
		return "", fmt.Errorf("column %s missing or nil", name)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("column %s is %T, want string", name, v)
	}
	return s, nil
}

func int64Field(r types.Row, name string) (int64, error) {
	v := r.Value(name)
	if v == nil {
		return 0, fmt.Errorf("column %s missing or nil", name)
	}
	switch x := v.(type) {
	case int64:
		return x, nil
	case int32:
		return int64(x), nil
	case int:
		return int64(x), nil
	default:
		return 0, fmt.Errorf("column %s is %T, want int{32,64}", name, v)
	}
}
