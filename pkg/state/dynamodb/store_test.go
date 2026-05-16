package dynamodb_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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
	otherCalls    atomic.Int64

	// handle is invoked for each BatchGetItem call with the parsed request; it
	// returns the response to encode. invocation is 1-indexed so test handlers
	// can vary behavior between attempts (e.g. surface UnprocessedKeys once,
	// then drain).
	handle func(invocation int, req ddbReq) ddbResp
}

func (f *fakeTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	target := r.Header.Get("X-Amz-Target")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	switch target {
	case "DynamoDB_20120810.BatchGetItem":
		inv := int(f.batchGetCalls.Add(1))
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
		f.getItemCalls.Add(1)
	default:
		f.otherCalls.Add(1)
	}
	// Unhandled ops: return an empty 200 — none of the tests below trigger this
	// path; if one does, the call counter surfaces the surprise.
	return &http.Response{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(bytes.NewReader([]byte(`{}`))),
	}, nil
}

// newFakeStore wires the given transport into a real *dynamodb.Client and
// returns a store pointed at table "t". Region, creds, and base endpoint are
// set to dummy values — the fake transport short-circuits before any real
// network call.
func newFakeStore(t *testing.T, ft *fakeTransport) *dynamodb.Int64SumStore {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awsconfig.WithHTTPClient(&http.Client{Transport: ft}),
		// Disable retries inside the SDK so our own UnprocessedKeys retry loop
		// is what's exercised — not the SDK's transparent retry middleware.
		awsconfig.WithRetryMaxAttempts(1),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	client := awsddb.NewFromConfig(cfg, func(o *awsddb.Options) {
		o.BaseEndpoint = aws.String("http://fake.local")
	})
	return dynamodb.NewInt64SumStore(client, "t")
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
