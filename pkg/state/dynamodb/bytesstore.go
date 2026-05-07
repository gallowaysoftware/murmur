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

	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

const attrVersion = "ver"

// BytesStore is a state.Store[[]byte] that uses optimistic-concurrency CAS for monoidal
// merge. Suitable for sketches (HLL, TopK, Bloom) and any other byte-encoded monoid that
// can't be expressed as a single DDB UpdateExpression.
//
// Update protocol:
//
//  1. GetItem to read current value + version.
//  2. Combine in-process via the user's monoid.
//  3. Conditional PutItem: insert if attribute_not_exists(ver), else update if
//     ver == expected. Increment ver atomically on success.
//  4. On ConditionalCheckFailedException, retry from step 1 up to MaxRetries.
//
// Throughput is lower than the atomic-ADD path; CAS contention scales with the
// per-key write rate. For very hot keys, configure a Valkey cache as accelerator (the
// cache absorbs the read-modify-write storm; we periodically snapshot back to DDB).
type BytesStore struct {
	client     *dynamodb.Client
	table      string
	monoid     monoid.Monoid[[]byte]
	maxRetries int
}

// NewBytesStore returns a CAS-backed Store[[]byte] for the given monoid. The table must
// already exist; CreateBytesTable is the test helper for the schema.
func NewBytesStore(client *dynamodb.Client, table string, m monoid.Monoid[[]byte]) *BytesStore {
	return &BytesStore{
		client:     client,
		table:      table,
		monoid:     m,
		maxRetries: 8,
	}
}

// ErrMaxRetriesExceeded is returned when CAS contention prevents a successful merge
// within MaxRetries attempts. Caller should retry at a higher level or shed load.
var ErrMaxRetriesExceeded = errors.New("dynamodb BytesStore: max CAS retries exceeded")

// Get reads the current sketch bytes at k.
func (s *BytesStore) Get(ctx context.Context, k state.Key) ([]byte, bool, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &s.table,
		Key:            keyAttr(k),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, false, fmt.Errorf("ddb GetItem %s: %w", s.table, err)
	}
	if len(out.Item) == 0 {
		return nil, false, nil
	}
	val, ok := out.Item[attrVal].(*types.AttributeValueMemberB)
	if !ok {
		return nil, false, fmt.Errorf("ddb %s: %s attribute missing or not bytes", s.table, attrVal)
	}
	return val.Value, true, nil
}

// GetMany batches reads via BatchGetItem. Loops on UnprocessedKeys with bounded
// exponential backoff so a throttled batch doesn't silently truncate results.
func (s *BytesStore) GetMany(ctx context.Context, ks []state.Key) ([][]byte, []bool, error) {
	if len(ks) == 0 {
		return nil, nil, nil
	}
	type pair struct {
		entity string
		bucket int64
	}
	byPair := make(map[pair][]byte, len(ks))

	const maxPerCall = 100
	for offset := 0; offset < len(ks); offset += maxPerCall {
		end := offset + maxPerCall
		if end > len(ks) {
			end = len(ks)
		}
		chunk := ks[offset:end]
		keys := make([]map[string]types.AttributeValue, len(chunk))
		for i, k := range chunk {
			keys[i] = keyAttr(k)
		}
		req := map[string]types.KeysAndAttributes{
			s.table: {Keys: keys, ConsistentRead: aws.Bool(true)},
		}
		for attempt := 0; len(req) > 0; attempt++ {
			if attempt > 0 {
				if err := backoffWait(ctx, attempt); err != nil {
					return nil, nil, err
				}
			}
			if attempt >= maxBatchAttempts {
				return nil, nil, fmt.Errorf("ddb BatchGetItem %s: unprocessed keys remain after %d attempts", s.table, maxBatchAttempts)
			}
			out, err := s.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: req})
			if err != nil {
				return nil, nil, fmt.Errorf("ddb BatchGetItem %s: %w", s.table, err)
			}
			for _, item := range out.Responses[s.table] {
				pk, _ := item[attrPK].(*types.AttributeValueMemberS)
				sk, _ := item[attrSK].(*types.AttributeValueMemberN)
				v, _ := item[attrVal].(*types.AttributeValueMemberB)
				if pk == nil || sk == nil || v == nil {
					continue
				}
				bucket, _ := strconv.ParseInt(sk.Value, 10, 64)
				byPair[pair{pk.Value, bucket}] = v.Value
			}
			req = out.UnprocessedKeys
		}
	}
	vals := make([][]byte, len(ks))
	oks := make([]bool, len(ks))
	for i, k := range ks {
		val, found := byPair[pair{k.Entity, k.Bucket}]
		vals[i] = val
		oks[i] = found
	}
	return vals, oks, nil
}

// MergeUpdate performs the read-combine-conditional-write loop.
func (s *BytesStore) MergeUpdate(ctx context.Context, k state.Key, delta []byte, ttl time.Duration) error {
	for attempt := 0; attempt < s.maxRetries; attempt++ {
		cur, version, exists, err := s.getWithVersion(ctx, k)
		if err != nil {
			return err
		}
		var merged []byte
		if exists {
			merged = s.monoid.Combine(cur, delta)
		} else {
			merged = s.monoid.Combine(s.monoid.Identity(), delta)
		}

		err = s.casWrite(ctx, k, merged, version, exists, ttl)
		if err == nil {
			return nil
		}
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			// Concurrent writer landed first; retry.
			continue
		}
		return err
	}
	return ErrMaxRetriesExceeded
}

// Close is a no-op; the underlying client is owned by the caller.
func (s *BytesStore) Close() error { return nil }

func (s *BytesStore) getWithVersion(ctx context.Context, k state.Key) (val []byte, version int64, exists bool, err error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &s.table,
		Key:            keyAttr(k),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, 0, false, fmt.Errorf("ddb GetItem %s: %w", s.table, err)
	}
	if len(out.Item) == 0 {
		return nil, 0, false, nil
	}
	v, _ := out.Item[attrVal].(*types.AttributeValueMemberB)
	ver, _ := out.Item[attrVersion].(*types.AttributeValueMemberN)
	if v == nil || ver == nil {
		return nil, 0, false, fmt.Errorf("ddb %s: row missing %s or %s attribute", s.table, attrVal, attrVersion)
	}
	parsedVer, err := strconv.ParseInt(ver.Value, 10, 64)
	if err != nil {
		return nil, 0, false, fmt.Errorf("ddb %s: parse %s: %w", s.table, attrVersion, err)
	}
	return v.Value, parsedVer, true, nil
}

func (s *BytesStore) casWrite(ctx context.Context, k state.Key, val []byte, expectedVer int64, exists bool, ttl time.Duration) error {
	item := map[string]types.AttributeValue{
		attrPK:      &types.AttributeValueMemberS{Value: k.Entity},
		attrSK:      &types.AttributeValueMemberN{Value: strconv.FormatInt(k.Bucket, 10)},
		attrVal:     &types.AttributeValueMemberB{Value: val},
		attrVersion: &types.AttributeValueMemberN{Value: strconv.FormatInt(expectedVer+1, 10)},
	}
	if ttl > 0 {
		item[attrTTL] = &types.AttributeValueMemberN{
			Value: strconv.FormatInt(time.Now().Add(ttl).Unix(), 10),
		}
	}

	var conditionExpression string
	exprNames := map[string]string{}
	exprVals := map[string]types.AttributeValue{}
	if exists {
		conditionExpression = "#ver = :v"
		exprNames["#ver"] = attrVersion
		exprVals[":v"] = &types.AttributeValueMemberN{Value: strconv.FormatInt(expectedVer, 10)}
	} else {
		conditionExpression = "attribute_not_exists(#ver)"
		exprNames["#ver"] = attrVersion
	}

	input := &dynamodb.PutItemInput{
		TableName:                &s.table,
		Item:                     item,
		ConditionExpression:      &conditionExpression,
		ExpressionAttributeNames: exprNames,
	}
	if len(exprVals) > 0 {
		input.ExpressionAttributeValues = exprVals
	}
	_, err := s.client.PutItem(ctx, input)
	if err != nil {
		return fmt.Errorf("ddb PutItem %s: %w", s.table, err)
	}
	return nil
}

// Compile-time check.
var _ state.Store[[]byte] = (*BytesStore)(nil)

// CreateBytesTable creates a table with the schema expected by BytesStore. The schema
// is identical to Int64SumStore's; the difference is purely in the value attribute type
// (number vs binary), which DynamoDB handles dynamically.
func CreateBytesTable(ctx context.Context, client *dynamodb.Client, table string) error {
	return CreateInt64Table(ctx, client, table)
}
