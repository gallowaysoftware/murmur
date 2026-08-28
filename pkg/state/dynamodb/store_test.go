package dynamodb_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/hll"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/gallowaysoftware/murmur/pkg/state"
	"github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

// localClient returns a DDB client pointed at dynamodb-local. Tests are skipped if the
// DDB_LOCAL_ENDPOINT environment variable is unset, so unit-test runs without the
// docker-compose stack stay green.
func localClient(t *testing.T) (*awsddb.Client, bool) {
	t.Helper()
	endpoint := os.Getenv("DDB_LOCAL_ENDPOINT")
	if endpoint == "" {
		t.Skip("DDB_LOCAL_ENDPOINT not set; skipping (run docker-compose up dynamodb-local and re-run)")
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	c := awsddb.NewFromConfig(cfg, func(o *awsddb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	return c, true
}

func TestInt64SumStore_AtomicAdd(t *testing.T) {
	client, _ := localClient(t)
	ctx := context.Background()
	table := "murmur_int64sum_test_" + t.Name()

	if err := dynamodb.CreateInt64Table(ctx, client, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteTable(ctx, &awsddb.DeleteTableInput{TableName: &table})
	})

	store := dynamodb.NewInt64SumStore(client, table)
	key := state.Key{Entity: "page-42", Bucket: 0}

	// Initial Get → missing.
	if v, ok, err := store.Get(ctx, key); err != nil || ok || v != 0 {
		t.Fatalf("initial Get: got (%d,%v,%v), want (0,false,nil)", v, ok, err)
	}

	// MergeUpdate +5, +3, -2 → expect 6.
	for _, d := range []int64{5, 3, -2} {
		if err := store.MergeUpdate(ctx, key, d, 0); err != nil {
			t.Fatalf("MergeUpdate %d: %v", d, err)
		}
	}
	if v, ok, err := store.Get(ctx, key); err != nil || !ok || v != 6 {
		t.Fatalf("after sums Get: got (%d,%v,%v), want (6,true,nil)", v, ok, err)
	}
}

func TestInt64SumStore_Windowed(t *testing.T) {
	client, _ := localClient(t)
	ctx := context.Background()
	table := "murmur_int64sum_window_test_" + t.Name()

	if err := dynamodb.CreateInt64Table(ctx, client, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteTable(ctx, &awsddb.DeleteTableInput{TableName: &table})
	})

	store := dynamodb.NewInt64SumStore(client, table)

	// Same entity, different buckets — verify they don't collide.
	for bucket, count := range map[int64]int64{100: 7, 101: 11, 102: 13} {
		key := state.Key{Entity: "user-1", Bucket: bucket}
		if err := store.MergeUpdate(ctx, key, count, 24*time.Hour); err != nil {
			t.Fatalf("MergeUpdate bucket=%d: %v", bucket, err)
		}
	}
	for bucket, want := range map[int64]int64{100: 7, 101: 11, 102: 13} {
		key := state.Key{Entity: "user-1", Bucket: bucket}
		v, ok, err := store.Get(ctx, key)
		if err != nil || !ok || v != want {
			t.Fatalf("Get bucket=%d: got (%d,%v,%v), want (%d,true,nil)", bucket, v, ok, err, want)
		}
	}
}

func TestInt64SumStore_GetMany(t *testing.T) {
	client, _ := localClient(t)
	ctx := context.Background()
	table := "murmur_int64sum_many_test_" + t.Name()

	if err := dynamodb.CreateInt64Table(ctx, client, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteTable(ctx, &awsddb.DeleteTableInput{TableName: &table})
	})

	store := dynamodb.NewInt64SumStore(client, table)

	// Populate three keys; one stays missing.
	for entity, count := range map[string]int64{"a": 1, "b": 2, "c": 3} {
		key := state.Key{Entity: entity, Bucket: 0}
		if err := store.MergeUpdate(ctx, key, count, 0); err != nil {
			t.Fatalf("seed %s: %v", entity, err)
		}
	}
	keys := []state.Key{
		{Entity: "a", Bucket: 0},
		{Entity: "b", Bucket: 0},
		{Entity: "missing", Bucket: 0},
		{Entity: "c", Bucket: 0},
	}
	want := []int64{1, 2, 0, 3}
	wantOK := []bool{true, true, false, true}
	vals, oks, err := store.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	for i := range keys {
		if vals[i] != want[i] || oks[i] != wantOK[i] {
			t.Fatalf("GetMany[%d]: got (%d,%v), want (%d,%v)", i, vals[i], oks[i], want[i], wantOK[i])
		}
	}
}

// --- Fake-transport unit tests ----------------------------------------------
//
// These tests build a real *dynamodb.Client whose HTTP transport is replaced
// with a handler we control. That lets us assert how many BatchGetItem RPCs
// the store issues for a given input — the entire point of M7 is that GetMany
// fans out via BatchGetItem (≤ 100 keys per RPC), not per-key GetItem.

// ddbReq mirrors the JSON 1.0 request body shape this code path cares about.
type ddbReq struct {
	RequestItems map[string]struct {
		Keys []map[string]map[string]string `json:"Keys"`
	} `json:"RequestItems"`
}

// ddbResp is the JSON 1.0 response body shape for BatchGetItem.
type ddbResp struct {
	Responses       map[string][]map[string]map[string]string `json:"Responses"`
	UnprocessedKeys map[string]struct {
		Keys []map[string]map[string]string `json:"Keys"`
	} `json:"UnprocessedKeys,omitempty"`
}

// fakeTransport is an http.RoundTripper that counts calls per X-Amz-Target op
// and delegates BatchGetItem handling to a user-supplied function.
type fakeTransport struct {
	batchGetCalls atomic.Int64
	getItemCalls  atomic.Int64
	putItemCalls  atomic.Int64
	deleteCalls   atomic.Int64
	otherCalls    atomic.Int64

	// handle is invoked for each BatchGetItem call with the parsed request; it
	// returns the response to encode. invocation is 1-indexed so test handlers
	// can vary behavior between attempts (e.g. surface UnprocessedKeys once,
	// then drain).
	handle func(invocation int, req ddbReq) ddbResp

	// handleOp, when set, answers every op other than BatchGetItem: the CAS and
	// dedup paths issue GetItem / PutItem / DeleteItem, and both need to shape
	// the response (a conditional-check failure, a dropped connection) rather
	// than just be counted. invocation is 1-indexed per op.
	handleOp func(target string, invocation int, body []byte) (*http.Response, error)
}

func (f *fakeTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	target := r.Header.Get("X-Amz-Target")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	var inv int
	switch target {
	case "DynamoDB_20120810.BatchGetItem":
		inv = int(f.batchGetCalls.Add(1))
		var req ddbReq
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("fakeTransport: decode BatchGetItem body: %w", err)
		}
		resp := f.handle(inv, req)
		buf, err := json.Marshal(resp)
		if err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: 200,
			Header:     http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}},
			Body:       io.NopCloser(bytes.NewReader(buf)),
		}, nil
	case "DynamoDB_20120810.GetItem":
		inv = int(f.getItemCalls.Add(1))
	case "DynamoDB_20120810.PutItem":
		inv = int(f.putItemCalls.Add(1))
	case "DynamoDB_20120810.DeleteItem":
		inv = int(f.deleteCalls.Add(1))
	default:
		inv = int(f.otherCalls.Add(1))
	}
	if f.handleOp != nil {
		return f.handleOp(target, inv, body)
	}
	// Unhandled ops: return an empty 200 — a GetItem answered this way reads as
	// "no such row", which is what the size-guard test wants; if a test trips
	// this path unintentionally, the call counter surfaces the surprise.
	return okResponse(`{}`), nil
}

// okResponse builds a 200 with a JSON 1.0 body.
func okResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(bytes.NewReader([]byte(body))),
	}
}

// ddbErrorResponse builds the 400 the service returns for a modeled exception.
// The SDK resolves the type from either the X-Amzn-Errortype header or the
// body's __type; we set both so the deserializer can't miss it.
func ddbErrorResponse(errType string) *http.Response {
	body := fmt.Sprintf(`{"__type":"com.amazonaws.dynamodb.v20120810#%s","message":"%s"}`, errType, errType)
	return &http.Response{
		StatusCode: 400,
		Header: http.Header{
			"Content-Type":     []string{"application/x-amz-json-1.0"},
			"X-Amzn-Errortype": []string{errType},
		},
		Body: io.NopCloser(bytes.NewReader([]byte(body))),
	}
}

// newFakeClient wires the given transport into a real *dynamodb.Client. Region,
// creds, and base endpoint are dummy values — the fake transport short-circuits
// before any real network call.
//
// maxAttempts is the SDK's own retry ceiling. Pass 1 to take the SDK's
// transparent retry middleware out of the picture, so a store's hand-rolled
// retry loop is what a test observes. Pass more to exercise the middleware
// itself; the backoff is stubbed to zero either way so a test that wants a
// retry doesn't pay for the jittered wait.
func newFakeClient(t *testing.T, ft *fakeTransport, maxAttempts int) *awsddb.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awsconfig.WithHTTPClient(&http.Client{Transport: ft}),
		awsconfig.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = maxAttempts
				o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) {
					return 0, nil
				})
			})
		}),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	return awsddb.NewFromConfig(cfg, func(o *awsddb.Options) {
		o.BaseEndpoint = aws.String("http://fake.local")
	})
}

// newFakeStore returns an Int64SumStore over newFakeClient, pointed at table
// "t". SDK retries are off so the store's own UnprocessedKeys retry loop is
// what's exercised.
func newFakeStore(t *testing.T, ft *fakeTransport) *dynamodb.Int64SumStore {
	t.Helper()
	return dynamodb.NewInt64SumStore(newFakeClient(t, ft, 1), "t")
}

// makeKeys returns n distinct keys with predictable entity names.
func makeKeys(n int) []state.Key {
	ks := make([]state.Key, n)
	for i := 0; i < n; i++ {
		ks[i] = state.Key{Entity: fmt.Sprintf("k-%04d", i), Bucket: 0}
	}
	return ks
}

// ddbItem builds the JSON-encoded item shape the fake transport returns.
func ddbItem(entity string, bucket, value int64) map[string]map[string]string {
	return map[string]map[string]string{
		"pk": {"S": entity},
		"sk": {"N": strconv.FormatInt(bucket, 10)},
		"v":  {"N": strconv.FormatInt(value, 10)},
	}
}

func TestInt64SumStore_GetMany_ChunksAt100(t *testing.T) {
	// 250 keys → exactly 3 BatchGetItem calls (100 + 100 + 50), zero GetItem.
	ft := &fakeTransport{
		handle: func(_ int, req ddbReq) ddbResp {
			items := make([]map[string]map[string]string, 0)
			for _, k := range req.RequestItems["t"].Keys {
				entity := k["pk"]["S"]
				bucket, _ := strconv.ParseInt(k["sk"]["N"], 10, 64)
				// Echo back every requested key with value=bucket+1 so the
				// assertion can verify the response was actually wired up.
				items = append(items, ddbItem(entity, bucket, 1))
			}
			return ddbResp{Responses: map[string][]map[string]map[string]string{"t": items}}
		},
	}
	store := newFakeStore(t, ft)

	keys := makeKeys(250)
	vals, oks, err := store.GetMany(context.Background(), keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if got := ft.batchGetCalls.Load(); got != 3 {
		t.Fatalf("BatchGetItem calls: got %d, want 3 (100+100+50)", got)
	}
	if got := ft.getItemCalls.Load(); got != 0 {
		t.Fatalf("GetItem calls: got %d, want 0 (must not fall back to per-key GetItem)", got)
	}
	if len(vals) != 250 || len(oks) != 250 {
		t.Fatalf("result lengths: vals=%d oks=%d, want 250 each", len(vals), len(oks))
	}
	for i, ok := range oks {
		if !ok || vals[i] != 1 {
			t.Fatalf("result[%d]: got (%d,%v), want (1,true)", i, vals[i], ok)
		}
	}
}

func TestInt64SumStore_GetMany_UnprocessedKeysRetry(t *testing.T) {
	// On the first call, half the keys come back via UnprocessedKeys. The
	// store's retry loop should re-issue exactly those keys on the next call.
	ft := &fakeTransport{
		handle: func(inv int, req ddbReq) ddbResp {
			keys := req.RequestItems["t"].Keys
			switch inv {
			case 1:
				// Return first half processed, second half unprocessed.
				half := len(keys) / 2
				items := make([]map[string]map[string]string, 0, half)
				for _, k := range keys[:half] {
					items = append(items, ddbItem(k["pk"]["S"], 0, 7))
				}
				return ddbResp{
					Responses: map[string][]map[string]map[string]string{"t": items},
					UnprocessedKeys: map[string]struct {
						Keys []map[string]map[string]string `json:"Keys"`
					}{
						"t": {Keys: keys[half:]},
					},
				}
			default:
				// All remaining keys process cleanly.
				items := make([]map[string]map[string]string, 0, len(keys))
				for _, k := range keys {
					items = append(items, ddbItem(k["pk"]["S"], 0, 7))
				}
				return ddbResp{Responses: map[string][]map[string]map[string]string{"t": items}}
			}
		},
	}
	store := newFakeStore(t, ft)

	keys := makeKeys(10)
	vals, oks, err := store.GetMany(context.Background(), keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if got := ft.batchGetCalls.Load(); got != 2 {
		t.Fatalf("BatchGetItem calls: got %d, want 2 (initial + UnprocessedKeys retry)", got)
	}
	for i, ok := range oks {
		if !ok || vals[i] != 7 {
			t.Fatalf("result[%d]: got (%d,%v), want (7,true)", i, vals[i], ok)
		}
	}
}

func TestInt64SumStore_GetMany_EmptyResponse(t *testing.T) {
	// Every requested key is missing: empty Responses, no UnprocessedKeys.
	// All results must come back with ok=false at the matching index, in input order.
	ft := &fakeTransport{
		handle: func(_ int, _ ddbReq) ddbResp {
			return ddbResp{Responses: map[string][]map[string]map[string]string{"t": nil}}
		},
	}
	store := newFakeStore(t, ft)

	keys := makeKeys(5)
	vals, oks, err := store.GetMany(context.Background(), keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if got := ft.batchGetCalls.Load(); got != 1 {
		t.Fatalf("BatchGetItem calls: got %d, want 1", got)
	}
	if len(vals) != 5 || len(oks) != 5 {
		t.Fatalf("result lengths: vals=%d oks=%d, want 5 each", len(vals), len(oks))
	}
	for i := range keys {
		if oks[i] || vals[i] != 0 {
			t.Fatalf("result[%d]: got (%d,%v), want (0,false)", i, vals[i], oks[i])
		}
	}
}

func TestInt64SumStore_GetMany_PartialResponseInInputOrder(t *testing.T) {
	// Server returns only some of the requested keys, *in scrambled order*.
	// The store must align results with the caller's input ordering and
	// mark the missing slots ok=false.
	//
	// Input keys: k-0000 .. k-0009; we'll return only odd-index entries
	// (k-0001, k-0003, k-0005, k-0007, k-0009) with value = index*10.
	ft := &fakeTransport{
		handle: func(_ int, req ddbReq) ddbResp {
			items := make([]map[string]map[string]string, 0)
			// Walk the request keys in reverse to scramble server-side order;
			// only emit entries whose index is odd.
			for j := len(req.RequestItems["t"].Keys) - 1; j >= 0; j-- {
				k := req.RequestItems["t"].Keys[j]
				entity := k["pk"]["S"]
				var idx int
				_, _ = fmt.Sscanf(entity, "k-%04d", &idx)
				if idx%2 == 1 {
					items = append(items, ddbItem(entity, 0, int64(idx*10)))
				}
			}
			return ddbResp{Responses: map[string][]map[string]map[string]string{"t": items}}
		},
	}
	store := newFakeStore(t, ft)

	keys := makeKeys(10)
	vals, oks, err := store.GetMany(context.Background(), keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if got := ft.batchGetCalls.Load(); got != 1 {
		t.Fatalf("BatchGetItem calls: got %d, want 1", got)
	}
	for i := range keys {
		if i%2 == 1 {
			if !oks[i] || vals[i] != int64(i*10) {
				t.Fatalf("result[%d]: got (%d,%v), want (%d,true)", i, vals[i], oks[i], i*10)
			}
		} else {
			if oks[i] || vals[i] != 0 {
				t.Fatalf("result[%d]: got (%d,%v), want (0,false)", i, vals[i], oks[i])
			}
		}
	}
}

func TestInt64SumStore_GetMany_EmptyInputNoCalls(t *testing.T) {
	// Zero keys must short-circuit — no HTTP traffic at all.
	ft := &fakeTransport{
		handle: func(_ int, _ ddbReq) ddbResp { return ddbResp{} },
	}
	store := newFakeStore(t, ft)

	vals, oks, err := store.GetMany(context.Background(), nil)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if vals != nil || oks != nil {
		t.Fatalf("expected nil results for empty input, got vals=%v oks=%v", vals, oks)
	}
	if got := ft.batchGetCalls.Load(); got != 0 {
		t.Fatalf("BatchGetItem calls: got %d, want 0", got)
	}
}

// duplicateKey reports the first (pk, sk) pair that appears twice in a single
// BatchGetItem request, across all tables in it.
func duplicateKey(req ddbReq) (string, bool) {
	for table, ka := range req.RequestItems {
		seen := make(map[string]struct{}, len(ka.Keys))
		for _, k := range ka.Keys {
			id := table + "/" + k["pk"]["S"] + "/" + k["sk"]["N"]
			if _, dup := seen[id]; dup {
				return id, true
			}
			seen[id] = struct{}{}
		}
	}
	return "", false
}

// validationException builds the 400 the DynamoDB API returns for a malformed
// request, in the JSON 1.0 shape the SDK decodes.
func validationException(message string) *http.Response {
	body, _ := json.Marshal(map[string]string{
		"__type":  "com.amazon.coral.validate#ValidationException",
		"message": message,
	})
	return &http.Response{
		StatusCode: 400,
		Header:     http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

// echoHandler answers a BatchGetItem with every requested key, valued from the
// lookup map. Keys absent from the map are omitted from the response, which is
// how DynamoDB reports a miss.
func echoHandler(values map[string]int64) func(int, ddbReq) ddbResp {
	return func(_ int, req ddbReq) ddbResp {
		items := make([]map[string]map[string]string, 0, len(req.RequestItems["t"].Keys))
		for _, k := range req.RequestItems["t"].Keys {
			entity := k["pk"]["S"]
			bucket, _ := strconv.ParseInt(k["sk"]["N"], 10, 64)
			v, ok := values[entity]
			if !ok {
				continue
			}
			items = append(items, ddbItem(entity, bucket, v))
		}
		return ddbResp{Responses: map[string][]map[string]map[string]string{"t": items}}
	}
}

// TestInt64SumStore_GetManyDeduplicatesKeys pins the duplicate handling.
// DynamoDB rejects a BatchGetItem whose key list repeats a key and fails the
// whole request, so a batched read over a candidate list naming the same entity
// twice took the entire RPC down with it.
func TestInt64SumStore_GetManyDeduplicatesKeys(t *testing.T) {
	ft := &fakeTransport{handle: echoHandler(map[string]int64{"a": 10, "b": 20})}
	store := newFakeStore(t, ft)

	keys := []state.Key{
		{Entity: "a", Bucket: 0},
		{Entity: "b", Bucket: 0},
		{Entity: "a", Bucket: 0}, // same entity twice, same chunk
		{Entity: "missing", Bucket: 0},
		{Entity: "b", Bucket: 0},
	}
	vals, oks, err := store.GetMany(context.Background(), keys)
	if err != nil {
		t.Fatalf("GetMany with duplicate keys: %v", err)
	}
	want := []int64{10, 20, 10, 0, 20}
	wantOK := []bool{true, true, true, false, true}
	for i := range keys {
		if vals[i] != want[i] || oks[i] != wantOK[i] {
			t.Errorf("result[%d] (%s): got (%d,%v), want (%d,%v)",
				i, keys[i].Entity, vals[i], oks[i], want[i], wantOK[i])
		}
	}
}

// TestInt64SumStore_GetManyDeduplicatesAcrossChunks covers the chunk-boundary
// dependence directly: 100 distinct keys followed by a repeat of the first one
// puts the duplicate in a different 100-key chunk, which is why the failure was
// intermittent on candidate-set size rather than reliable.
func TestInt64SumStore_GetManyDeduplicatesAcrossChunks(t *testing.T) {
	keys := makeKeys(100)
	values := make(map[string]int64, len(keys))
	for i, k := range keys {
		values[k.Entity] = int64(i + 1)
	}
	keys = append(keys, keys[0])

	ft := &fakeTransport{handle: echoHandler(values)}
	store := newFakeStore(t, ft)

	vals, oks, err := store.GetMany(context.Background(), keys)
	if err != nil {
		t.Fatalf("GetMany with a cross-chunk duplicate: %v", err)
	}
	// 101 keys collapse to 100 distinct ones — a single BatchGetItem, not two.
	if got := ft.batchGetCalls.Load(); got != 1 {
		t.Errorf("BatchGetItem calls: got %d, want 1", got)
	}
	if !oks[0] || vals[0] != 1 {
		t.Errorf("result[0]: got (%d,%v), want (1,true)", vals[0], oks[0])
	}
	if !oks[100] || vals[100] != 1 {
		t.Errorf("duplicate at result[100]: got (%d,%v), want (1,true)", vals[100], oks[100])
	}
}

// TestBytesStore_GetManyDeduplicatesKeys is the sketch-state counterpart —
// BytesStore carries its own copy of the batch loop.
func TestBytesStore_GetManyDeduplicatesKeys(t *testing.T) {
	ft := &fakeTransport{
		handle: func(_ int, req ddbReq) ddbResp {
			items := make([]map[string]map[string]string, 0)
			for _, k := range req.RequestItems["t"].Keys {
				entity := k["pk"]["S"]
				if entity != "a" {
					continue
				}
				items = append(items, map[string]map[string]string{
					"pk": {"S": entity},
					"sk": {"N": k["sk"]["N"]},
					"v":  {"B": base64.StdEncoding.EncodeToString([]byte("sketch"))},
				})
			}
			return ddbResp{Responses: map[string][]map[string]map[string]string{"t": items}}
		},
	}
	store := dynamodb.NewBytesStore(newFakeClient(t, ft, 1), "t", hll.HLL())

	keys := []state.Key{
		{Entity: "a", Bucket: 7},
		{Entity: "a", Bucket: 7},
		{Entity: "b", Bucket: 7},
	}
	vals, oks, err := store.GetMany(context.Background(), keys)
	if err != nil {
		t.Fatalf("GetMany with duplicate keys: %v", err)
	}
	for _, i := range []int{0, 1} {
		if !oks[i] || string(vals[i]) != "sketch" {
			t.Errorf("result[%d]: got (%q,%v), want (\"sketch\", true)", i, vals[i], oks[i])
		}
	}
	if oks[2] {
		t.Errorf("result[2] (b): reported present, want absent")
	}
}
