// Package dynamodb provides a DynamoDB-backed implementation of state.Store, the
// source-of-truth state for Murmur pipelines.
//
// The store dispatches on monoid Kind to choose between native DDB primitives and
// optimistic-concurrency CAS:
//
//   - KindSum, KindCount with int64 / float64 values: atomic UpdateItem ADD. No read,
//     no contention, full DDB throughput. Ships in this package as Int64SumStore.
//   - All other kinds: read-modify-write with conditional write on a version attribute.
//     CAS retries up to MaxRetries on conflict. Ships in a follow-up.
//
// Table schema:
//
//	PK pk (S) — the entity key
//	SK sk (N) — the bucket ID (0 for non-windowed aggregations)
//	   v  (N or B) — the value
//	   ttl (N) — optional Unix-epoch-seconds TTL (DDB native TTL attribute)
//	   ver (N) — optimistic-concurrency version (CAS path only)
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

const (
	attrPK  = "pk"
	attrSK  = "sk"
	attrVal = "v"
	attrTTL = "ttl"
)

// Int64SumStore is a state.Store[int64] specialized for the KindSum monoid. MergeUpdate
// uses DynamoDB's atomic ADD UpdateExpression: no read, no CAS retry, no application-side
// race conditions. The fastest path for high-frequency counter pipelines.
type Int64SumStore struct {
	client *dynamodb.Client
	table  string
}

// NewInt64SumStore returns a Store backed by the given DDB table. The table must already
// exist with schema (pk: S, sk: N) — see CreateInt64Table for a helper used in tests.
func NewInt64SumStore(client *dynamodb.Client, table string) *Int64SumStore {
	return &Int64SumStore{client: client, table: table}
}

// Get returns the current sum for k. Missing keys return 0, false, nil.
func (s *Int64SumStore) Get(ctx context.Context, k state.Key) (int64, bool, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.table,
		Key:       keyAttr(k),
	})
	if err != nil {
		return 0, false, fmt.Errorf("ddb GetItem %s: %w", s.table, err)
	}
	if len(out.Item) == 0 {
		return 0, false, nil
	}
	v, ok := out.Item[attrVal].(*types.AttributeValueMemberN)
	if !ok {
		return 0, false, fmt.Errorf("ddb %s: %s attribute missing or not numeric", s.table, attrVal)
	}
	n, err := strconv.ParseInt(v.Value, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("ddb %s: parse %s: %w", s.table, attrVal, err)
	}
	return n, true, nil
}

// GetMany batches reads via BatchGetItem. Returns one entry per requested key in order;
// missing keys return zero value and ok=false at that index.
func (s *Int64SumStore) GetMany(ctx context.Context, ks []state.Key) ([]int64, []bool, error) {
	if len(ks) == 0 {
		return nil, nil, nil
	}
	keys := make([]map[string]types.AttributeValue, len(ks))
	for i, k := range ks {
		keys[i] = keyAttr(k)
	}
	out, err := s.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			s.table: {Keys: keys},
		},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("ddb BatchGetItem %s: %w", s.table, err)
	}
	// BatchGetItem returns rows in arbitrary order — index by (pk, sk).
	type pair struct{ entity string; bucket int64 }
	byPair := make(map[pair]int64, len(out.Responses[s.table]))
	for _, item := range out.Responses[s.table] {
		pk, _ := item[attrPK].(*types.AttributeValueMemberS)
		sk, _ := item[attrSK].(*types.AttributeValueMemberN)
		v, _ := item[attrVal].(*types.AttributeValueMemberN)
		if pk == nil || sk == nil || v == nil {
			continue
		}
		bucket, _ := strconv.ParseInt(sk.Value, 10, 64)
		val, _ := strconv.ParseInt(v.Value, 10, 64)
		byPair[pair{pk.Value, bucket}] = val
	}
	vals := make([]int64, len(ks))
	oks := make([]bool, len(ks))
	for i, k := range ks {
		val, found := byPair[pair{k.Entity, k.Bucket}]
		vals[i] = val
		oks[i] = found
	}
	return vals, oks, nil
}

// MergeUpdate atomically adds delta to the value at k via DynamoDB's ADD UpdateExpression.
// Idempotent under at-least-once dedup applied upstream — the DDB call itself is a single
// atomic operation, no application-side CAS required.
//
// If ttl is nonzero, sets the ttl attribute to now + ttl (Unix epoch seconds). DynamoDB's
// TTL feature evicts the row asynchronously when ttl elapses; useful for windowed
// aggregations to retire old buckets.
func (s *Int64SumStore) MergeUpdate(ctx context.Context, k state.Key, delta int64, ttl time.Duration) error {
	expr := "ADD #v :d"
	exprNames := map[string]string{"#v": attrVal}
	exprVals := map[string]types.AttributeValue{
		":d": &types.AttributeValueMemberN{Value: strconv.FormatInt(delta, 10)},
	}
	if ttl > 0 {
		expr += " SET #t = :t"
		exprNames["#t"] = attrTTL
		exprVals[":t"] = &types.AttributeValueMemberN{
			Value: strconv.FormatInt(time.Now().Add(ttl).Unix(), 10),
		}
	}
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 &s.table,
		Key:                       keyAttr(k),
		UpdateExpression:          &expr,
		ExpressionAttributeNames:  exprNames,
		ExpressionAttributeValues: exprVals,
	})
	if err != nil {
		return fmt.Errorf("ddb UpdateItem %s: %w", s.table, err)
	}
	return nil
}

// Close is a no-op — the underlying client is owned by the caller.
func (s *Int64SumStore) Close() error { return nil }

// Compile-time check that Int64SumStore satisfies the Store contract.
var _ state.Store[int64] = (*Int64SumStore)(nil)

// CreateInt64Table is a test/dev helper that creates a DDB table with the schema
// expected by Int64SumStore. Production tables should be created via Terraform; this
// exists to keep integration tests self-contained.
func CreateInt64Table(ctx context.Context, client *dynamodb.Client, table string) error {
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: &table,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(attrPK), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String(attrSK), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(attrPK), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(attrSK), KeyType: types.KeyTypeRange},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	var inUse *types.ResourceInUseException
	if errors.As(err, &inUse) {
		return nil
	}
	return err
}

func keyAttr(k state.Key) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		attrPK: &types.AttributeValueMemberS{Value: k.Entity},
		attrSK: &types.AttributeValueMemberN{Value: strconv.FormatInt(k.Bucket, 10)},
	}
}
