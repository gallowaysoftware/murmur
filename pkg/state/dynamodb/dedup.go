package dynamodb

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gallowaysoftware/murmur/pkg/state"
)

// attrClaimant holds the token of the MarkSeen call that wrote the row. See
// MarkSeen for why the claim needs a writer identity.
const attrClaimant = "claimant"

// Deduper is a DynamoDB-backed implementation of state.Deduper. It uses a
// dedicated table whose only job is to claim EventIDs atomically: the streaming
// runtime calls MarkSeen with each Source.Record's EventID before applying the
// monoid Combine, and a duplicate (a record re-delivered after a crash) is
// short-circuited cleanly.
//
// Schema:
//
//	pk       (S) — "<pipeline>#<EventID>"
//	claimant (S) — the token of the call that won the claim
//	ttl      (N) — Unix-epoch seconds when the entry should be evicted (DDB native TTL)
//
// Atomic claim: PutItem with a ConditionExpression that admits either an
// unclaimed key or a re-send of our own claim. Concurrent claims by two workers
// race; exactly one's PutItem succeeds and returns nil; the other gets
// ConditionalCheckFailedException and the wrapper returns firstSeen=false.
type Deduper struct {
	client   *dynamodb.Client
	table    string
	pipeline string
	ttl      time.Duration
}

// NewDeduper constructs a Deduper backed by the named table, scoped to the named
// pipeline. Pass the same name the pipeline was built with, so metrics, state
// tables and dedup claims all agree on what a pipeline is called.
//
// The scope matters because EventIDs are only unique within a source: two
// pipelines reading different topics can both produce "1234", and a dedup table
// shared between them (the layout doc/design.md §13.4 recommends) would let the
// first claim starve the second — one pipeline silently drops a first delivery
// because an unrelated pipeline saw that ID. An empty pipeline name is legal but
// shares one namespace with every other unnamed Deduper.
//
// ttl is how long each claim is retained before DDB's TTL feature evicts it;
// pick a value > the source's max delivery latency. 24h is a reasonable default
// for Kafka with bounded retention; longer for Kinesis with extended retention.
func NewDeduper(client *dynamodb.Client, table, pipeline string, ttl time.Duration) *Deduper {
	return &Deduper{client: client, table: table, pipeline: pipeline, ttl: ttl}
}

// ForPipeline returns a Deduper over the same table and TTL, scoped to a
// different pipeline. A worker process hosting several pipelines against one
// shared dedup table wires the client and table once and derives a scope per
// pipeline.
func (d *Deduper) ForPipeline(pipeline string) *Deduper {
	scoped := *d
	scoped.pipeline = pipeline
	return &scoped
}

// claimKey is the dedup table's partition key: the EventID namespaced by
// pipeline, so IDs that collide across pipelines claim different rows.
func (d *Deduper) claimKey(eventID string) string {
	return d.pipeline + "#" + eventID
}

// MarkSeen claims eventID. firstSeen=true means the caller wins and should
// proceed with processing; firstSeen=false means the EventID was already
// claimed by some prior call (i.e. the record is a duplicate).
func (d *Deduper) MarkSeen(ctx context.Context, eventID string) (bool, error) {
	if eventID == "" {
		// An empty EventID degrades to "always firstSeen" — we can't dedup
		// without an ID. Sources should always produce non-empty IDs; surface
		// as a non-error so a single odd record doesn't take down the worker.
		return true, nil
	}
	// A per-call token stamped into the row, and admitted by the condition.
	// Without it, a claim whose response is lost — connection reset after DDB
	// committed the write — comes back as a plain ConditionalCheckFailed on the
	// SDK's retry and is indistinguishable from a peer's claim, so the runtime
	// skips a merge that never happened and the delivery is gone. The token
	// works because the SDK's retry middleware sits after serialization: the
	// replayed request carries the identical claimant and recognizes its own row.
	claimant := rand.Text()
	item := map[string]types.AttributeValue{
		attrPK:       &types.AttributeValueMemberS{Value: d.claimKey(eventID)},
		attrClaimant: &types.AttributeValueMemberS{Value: claimant},
	}
	if d.ttl > 0 {
		item[attrTTL] = &types.AttributeValueMemberN{
			Value: strconv.FormatInt(time.Now().Add(d.ttl).Unix(), 10),
		}
	}
	cond := "attribute_not_exists(#pk) OR #claimant = :me"
	_, err := d.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           &d.table,
		Item:                item,
		ConditionExpression: &cond,
		ExpressionAttributeNames: map[string]string{
			"#pk":       attrPK,
			"#claimant": attrClaimant,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":me": &types.AttributeValueMemberS{Value: claimant},
		},
	})
	if err == nil {
		return true, nil
	}
	var ccf *types.ConditionalCheckFailedException
	if errors.As(err, &ccf) {
		return false, nil
	}
	return false, fmt.Errorf("ddb dedup PutItem %s: %w", d.table, err)
}

// Release deletes the claim row for eventID so a redelivery can re-claim it.
// The streaming runtime calls this when a merge fails after MarkSeen already
// won the claim; without it the claim outlives the failed write and the event
// is dropped permanently.
//
// DeleteItem is unconditional and idempotent — deleting a row that isn't there
// succeeds — which matches the interface's "releasing an unclaimed ID is a
// no-op" requirement. In particular a TTL eviction that beat us here is not an
// error.
func (d *Deduper) Release(ctx context.Context, eventID string) error {
	if eventID == "" {
		// Mirrors MarkSeen: an empty ID was never claimed.
		return nil
	}
	_, err := d.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &d.table,
		Key: map[string]types.AttributeValue{
			attrPK: &types.AttributeValueMemberS{Value: d.claimKey(eventID)},
		},
	})
	if err != nil {
		return fmt.Errorf("ddb dedup DeleteItem %s: %w", d.table, err)
	}
	return nil
}

// Close is a no-op; the underlying client is owned by the caller.
func (d *Deduper) Close() error { return nil }

// CreateDedupTable is a test/dev helper that creates a dedup table with the
// schema NewDeduper expects. Production should provision via Terraform with
// TTL enabled on the `ttl` attribute.
func CreateDedupTable(ctx context.Context, client *dynamodb.Client, table string) error {
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: &table,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	var inUse *types.ResourceInUseException
	if errors.As(err, &inUse) {
		return nil
	}
	return err
}

// Compile-time check.
var _ state.Deduper = (*Deduper)(nil)
