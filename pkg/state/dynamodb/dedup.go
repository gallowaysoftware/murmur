package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gallowaysoftware/murmur/pkg/state"
)

// Deduper is a DynamoDB-backed implementation of state.Deduper. It uses a
// dedicated table whose only job is to claim EventIDs atomically: the streaming
// runtime calls MarkSeen with each Source.Record's EventID before applying the
// monoid Combine, and a duplicate (a record re-delivered after a crash) is
// short-circuited cleanly.
//
// Schema:
//
//	pk  (S) — the EventID
//	ttl (N) — Unix-epoch seconds when the entry should be evicted (DDB native TTL)
//
// Atomic claim: PutItem with ConditionExpression "attribute_not_exists(pk)".
// Concurrent claims by two workers race; exactly one's PutItem succeeds and
// returns nil; the other gets ConditionalCheckFailedException and the wrapper
// returns firstSeen=false.
type Deduper struct {
	client *dynamodb.Client
	table  string
	ttl    time.Duration
}

// NewDeduper constructs a Deduper backed by the named table. ttl is how long
// each EventID claim is retained before DDB's TTL feature evicts it; pick a
// value > the source's max delivery latency. 24h is a reasonable default for
// Kafka with bounded retention; longer for Kinesis with extended retention.
func NewDeduper(client *dynamodb.Client, table string, ttl time.Duration) *Deduper {
	return &Deduper{client: client, table: table, ttl: ttl}
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
	item := map[string]types.AttributeValue{
		attrPK: &types.AttributeValueMemberS{Value: eventID},
	}
	if d.ttl > 0 {
		item[attrTTL] = &types.AttributeValueMemberN{
			Value: strconv.FormatInt(time.Now().Add(d.ttl).Unix(), 10),
		}
	}
	cond := "attribute_not_exists(#pk)"
	_, err := d.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                &d.table,
		Item:                     item,
		ConditionExpression:      &cond,
		ExpressionAttributeNames: map[string]string{"#pk": attrPK},
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
