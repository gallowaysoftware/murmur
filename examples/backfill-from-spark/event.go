// Package backfill defines the canonical S3 JSON-Lines schema that
// upstream Spark jobs emit for Murmur backfill, plus a decoder and
// EventID extractor wired to make `bootstrap.Run` idempotent across
// re-runs.
//
// Schema (one JSON object per line):
//
//	{
//	  "entity_id":   "<string>",        // the keying dimension
//	  "count":       <int64>,           // pre-aggregated count for the bucket
//	  "occurred_at": "<RFC3339 UTC>",   // bucket-mid timestamp (see README)
//	  "year":        <int>,             // partition components (informational)
//	  "month":       <int>,
//	  "day":         <int>,
//	  "hour":        <int>
//	}
//
// File layout: gzipped JSON-Lines (`*.jsonl.gz`) under an S3 prefix
// partitioned Hive-style, e.g.:
//
//	s3://my-bucket/counters/<counter-name>/year=2026/month=05/day=08/hour=14/part-0000.jsonl.gz
//
// See ./README.md for the full operator-facing description.
package backfill

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// CountEvent is the canonical event shape backfill rows decode into.
// It matches what a streaming source would produce when a Spark job
// pre-aggregates raw events into hourly bucket summaries: one event
// stands in for many, with Count carrying the pre-aggregated delta.
// Because the downstream Sum monoid is associative, "+1 forty-two
// times" and "+42 once" produce the same bucket value.
type CountEvent struct {
	EntityID   string    `json:"entity_id"`
	Count      int64     `json:"count"`
	OccurredAt time.Time `json:"occurred_at"`

	// Year/Month/Day/Hour are informational — the partition components
	// the upstream Spark job wrote the row under. The pipeline derives
	// its bucket from OccurredAt; these fields are kept on the struct
	// so they survive a round-trip through the decoder and are
	// available for logs or operator-facing tooling.
	Year  int `json:"year"`
	Month int `json:"month"`
	Day   int `json:"day"`
	Hour  int `json:"hour"`
}

// DecodeJSONL decodes one JSON-Lines record. Wire it as the
// `jsonl.Config.Decode` (or `s3.Config.Decode`) when the rows on disk
// match the canonical schema verbatim.
func DecodeJSONL(line []byte) (CountEvent, error) {
	var e CountEvent
	if err := json.Unmarshal(line, &e); err != nil {
		return CountEvent{}, fmt.Errorf("backfill decode: %w", err)
	}
	if e.EntityID == "" {
		return CountEvent{}, fmt.Errorf("backfill decode: missing entity_id")
	}
	if e.OccurredAt.IsZero() {
		return CountEvent{}, fmt.Errorf("backfill decode: missing occurred_at")
	}
	return e, nil
}

// StableEventID hashes (entity_id, hour-bucket) into a deterministic
// dedup key. Use it as `s3.Config.EventID` so re-runs of the same
// Spark output land idempotently against `bootstrap.Run`'s deduper:
//
//	pipeline definition + Spark output + bootstrap.Run with Dedup →
//	    re-running the bootstrap is a no-op
//
// Two rows for the same (entity, hour) collapse to one dedup entry,
// which is what we want when the Spark job re-emits a partition.
func StableEventID(e CountEvent, _ string, _ int) string {
	bucket := e.OccurredAt.UTC().Truncate(time.Hour).Format(time.RFC3339)
	h := sha256.Sum256([]byte(e.EntityID + "|" + bucket))
	return "backfill:" + hex.EncodeToString(h[:16])
}
