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

// Int64MaxStore is a state.Store[int64] specialized for the **monotonic
// counter** pattern: each `MergeUpdate(k, v)` sets the stored value to
// `v` only if `v > current`. Out-of-order events with values lower than
// what's already stored are silently dropped. Equivalent to the
// `SetCountIfGreater` pattern from count-core's review.
//
// Mechanism: a single DDB UpdateItem with a conditional expression:
//
//	UpdateExpression:    "SET #v = :v"
//	ConditionExpression: "attribute_not_exists(#v) OR #v < :v"
//
// On condition failure (existing value >= new), DDB returns
// ConditionalCheckFailedException — the store catches it and treats the
// operation as a no-op success. From the caller's perspective the
// MergeUpdate succeeded; the stored value is the higher of the two.
//
// # When to use this vs Int64SumStore
//
//   - Int64SumStore: events carry DELTAS. `MergeUpdate(k, +1)` adds 1
//     to the running sum. Atomic ADD; high throughput; correct under
//     at-least-once with monoid-Sum's commutativity.
//   - Int64MaxStore: events carry ABSOLUTE VALUES. `MergeUpdate(k, 42)`
//     sets the stored value to 42 if the current is < 42, otherwise
//     ignores. The natural fit for cache-fill / "I just observed this
//     value" / version-stamped counter patterns where the producer
//     knows the absolute count and downstream consumers might process
//     out of order.
//
// # Pairs with the Max monoid
//
// Pipelines using `core.Max[int64]()` semantically expect this store
// rather than Int64SumStore. The Murmur monoid framework's
// MergeUpdate contract is "combine delta into existing"; for Max
// semantics, "delta" IS "the new candidate value" and "combine" is
// "take the bigger one" — which is exactly what the conditional
// UpdateItem implements at the DDB level.
//
// # Out-of-order safety
//
// Two workers processing different events for the same key can write
// in any order; the higher value wins. This is the behavior count-core
// builds with its SetCountIfGreater idiom; using Int64MaxStore
// inherits the same guarantee at the storage layer.
//
// # Cost
//
// One DDB UpdateItem per MergeUpdate, conditional. CCF (condition fail)
// is NOT charged a write capacity unit per AWS docs (the conditional
// check is part of the read side); successful writes are charged 1
// WCU. So worst-case cost is ~1 WCU/event, same as Int64SumStore.
type Int64MaxStore struct {
	client *dynamodb.Client
	table  string
}

// NewInt64MaxStore constructs an Int64MaxStore against the named table.
// The table must exist with the standard schema (pk:S, sk:N, v:N) — use
// CreateInt64Table from this package as the test/DDB-local helper.
func NewInt64MaxStore(client *dynamodb.Client, table string) *Int64MaxStore {
	return &Int64MaxStore{client: client, table: table}
}

// Get returns the stored value at k. Identical implementation to
// Int64SumStore.Get — the storage shape is the same; only MergeUpdate
// differs.
func (s *Int64MaxStore) Get(ctx context.Context, k state.Key) (int64, bool, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &s.table,
		Key:            keyAttr(k),
		ConsistentRead: aws.Bool(true),
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
	val, err := strconv.ParseInt(v.Value, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("ddb %s: parse %s: %w", s.table, attrVal, err)
	}
	return val, true, nil
}

// GetMany delegates to the same BatchGetItem-with-UnprocessedKeys retry
// loop Int64SumStore uses. Implementation is identical.
func (s *Int64MaxStore) GetMany(ctx context.Context, ks []state.Key) ([]int64, []bool, error) {
	// Reuse Int64SumStore's logic by delegating; both stores share the
	// same read-path schema. (Inlined would also work; delegation keeps
	// the implementations in lockstep.)
	sum := &Int64SumStore{client: s.client, table: s.table}
	return sum.GetMany(ctx, ks)
}

// MergeUpdate sets the stored value at k to delta IF delta is strictly
// greater than the current stored value (or if no value yet exists).
// Otherwise it's a no-op success — the higher value already wins.
//
// Note: `delta` here is misleading wording carried over from the Store
// interface. For Int64MaxStore it's the new ABSOLUTE candidate value,
// not a delta-to-add. Callers passing a counter from the producer side
// (count-core's typical shape) pass the count itself.
func (s *Int64MaxStore) MergeUpdate(ctx context.Context, k state.Key, delta int64, ttl time.Duration) error {
	expr := "SET #v = :v"
	exprNames := map[string]string{"#v": attrVal}
	exprVals := map[string]types.AttributeValue{
		":v": &types.AttributeValueMemberN{Value: strconv.FormatInt(delta, 10)},
	}
	if ttl > 0 {
		expr += ", #t = :t"
		exprNames["#t"] = attrTTL
		exprVals[":t"] = &types.AttributeValueMemberN{
			Value: strconv.FormatInt(time.Now().Add(ttl).Unix(), 10),
		}
	}
	// The condition: insert if missing, OR replace if strictly greater.
	// Strictly greater (not >=) so re-emits of the SAME value short-circuit
	// to no-op rather than burning a write.
	condition := "attribute_not_exists(#v) OR #v < :v"

	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 &s.table,
		Key:                       keyAttr(k),
		UpdateExpression:          aws.String(expr),
		ConditionExpression:       aws.String(condition),
		ExpressionAttributeNames:  exprNames,
		ExpressionAttributeValues: exprVals,
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			// Out-of-order event: existing value is already >= delta.
			// This is the expected path for this store, NOT an error.
			return nil
		}
		return fmt.Errorf("ddb max-update %s/%s: %w", s.table, k.Entity, err)
	}
	return nil
}

// Close is a no-op; the underlying DDB client is owned by the caller.
func (s *Int64MaxStore) Close() error { return nil }

// Compile-time check.
var _ state.Store[int64] = (*Int64MaxStore)(nil)
