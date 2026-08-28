package dynamodb_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

func TestDeduper_FirstAndSubsequent(t *testing.T) {
	client, _ := localClient(t)
	ctx := context.Background()
	table := fmt.Sprintf("murmur_dedup_basic_%d", time.Now().UnixNano())
	if err := dynamodb.CreateDedupTable(ctx, client, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteTable(ctx, &awsddb.DeleteTableInput{TableName: &table})
	})
	d := dynamodb.NewDeduper(client, table, "test_pipeline", 1*time.Hour)

	first, err := d.MarkSeen(ctx, "evt-001")
	if err != nil || !first {
		t.Fatalf("first MarkSeen: got (%v, %v), want (true, nil)", first, err)
	}

	again, err := d.MarkSeen(ctx, "evt-001")
	if err != nil || again {
		t.Fatalf("second MarkSeen of same id: got (%v, %v), want (false, nil)", again, err)
	}

	other, err := d.MarkSeen(ctx, "evt-002")
	if err != nil || !other {
		t.Fatalf("first MarkSeen of different id: got (%v, %v), want (true, nil)", other, err)
	}
}

func TestDeduper_RaceExactlyOneWins(t *testing.T) {
	// Even if N goroutines race to claim the same EventID, exactly one
	// should win. The DDB ConditionExpression is the contract here.
	client, _ := localClient(t)
	ctx := context.Background()
	table := fmt.Sprintf("murmur_dedup_race_%d", time.Now().UnixNano())
	if err := dynamodb.CreateDedupTable(ctx, client, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteTable(ctx, &awsddb.DeleteTableInput{TableName: &table})
	})
	d := dynamodb.NewDeduper(client, table, "test_pipeline", 1*time.Hour)

	const N = 16
	var winners atomic.Int32
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			first, err := d.MarkSeen(ctx, "raced-id")
			if err != nil {
				t.Errorf("MarkSeen: %v", err)
				return
			}
			if first {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := winners.Load(); got != 1 {
		t.Fatalf("race winners: got %d, want exactly 1", got)
	}
}

func TestDeduper_EmptyIDIsAlwaysFirstSeen(t *testing.T) {
	client, _ := localClient(t)
	ctx := context.Background()
	table := fmt.Sprintf("murmur_dedup_empty_%d", time.Now().UnixNano())
	if err := dynamodb.CreateDedupTable(ctx, client, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteTable(ctx, &awsddb.DeleteTableInput{TableName: &table})
	})
	d := dynamodb.NewDeduper(client, table, "test_pipeline", 1*time.Hour)

	// Empty IDs degrade to "always firstSeen" so a malformed source record
	// doesn't get silently deduplicated against itself or other empties.
	for i := 0; i < 3; i++ {
		first, err := d.MarkSeen(ctx, "")
		if err != nil {
			t.Fatalf("MarkSeen empty: %v", err)
		}
		if !first {
			t.Errorf("MarkSeen empty (#%d): got firstSeen=false, want true", i)
		}
	}
}

// --- Fake-transport unit tests ----------------------------------------------
//
// These drive the claim protocol without dynamodb-local, because the failures
// they cover live in the ConditionExpression and the partition key rather than
// in DynamoDB itself. fakeDedupTable holds the rows in memory and evaluates the
// two conditions Deduper actually sends.

// dedupPKAttr mirrors the package's unexported partition-key attribute name;
// these tests live in the _test package and can't reach the constant.
const dedupPKAttr = "pk"

// dedupItemReq is the slice of the PutItem / DeleteItem JSON body the dedup
// path uses. Every value here is S or N, so map[string]string is enough.
type dedupItemReq struct {
	Item                      map[string]map[string]string `json:"Item"`
	Key                       map[string]map[string]string `json:"Key"`
	ConditionExpression       string                       `json:"ConditionExpression"`
	ExpressionAttributeValues map[string]map[string]string `json:"ExpressionAttributeValues"`
}

// fakeDedupTable is an in-memory stand-in for the dedup table: pk → claimant.
type fakeDedupTable struct {
	mu        sync.Mutex
	rows      map[string]string
	claimants []string

	// onPut runs after the write has been applied and before the response is
	// built. Returning a non-nil response or error replaces the answer — which
	// is how a test loses the response to a claim DynamoDB already committed.
	onPut func(invocation int, pk string, written bool) (*http.Response, error)
}

func newFakeDedupTable() *fakeDedupTable {
	return &fakeDedupTable{rows: make(map[string]string)}
}

// seenClaimants returns the claimant token from every PutItem, in order.
func (f *fakeDedupTable) seenClaimants() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.claimants...)
}

// evaluate is a mini ConditionExpression evaluator covering exactly the two
// forms Deduper sends. An expression it doesn't recognize is an error rather
// than a silent pass — otherwise a rewritten condition would quietly make these
// tests vacuous.
func (f *fakeDedupTable) evaluate(req dedupItemReq, cur string, exists bool) (bool, error) {
	expr := req.ConditionExpression
	if !strings.Contains(expr, "attribute_not_exists(#pk)") {
		return false, fmt.Errorf("fakeDedupTable: unhandled ConditionExpression %q", expr)
	}
	if !exists {
		return true, nil
	}
	if strings.Contains(expr, "#claimant = :me") {
		me, ok := req.ExpressionAttributeValues[":me"]
		return ok && cur != "" && cur == me["S"], nil
	}
	return false, nil
}

func (f *fakeDedupTable) put(inv int, body []byte) (*http.Response, error) {
	var req dedupItemReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("fakeDedupTable: decode PutItem body: %w", err)
	}
	pk := req.Item[dedupPKAttr]["S"]
	claimant := req.Item["claimant"]["S"]

	f.mu.Lock()
	f.claimants = append(f.claimants, claimant)
	cur, exists := f.rows[pk]
	written, err := f.evaluate(req, cur, exists)
	if written {
		f.rows[pk] = claimant
	}
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}

	if f.onPut != nil {
		if resp, hookErr := f.onPut(inv, pk, written); resp != nil || hookErr != nil {
			return resp, hookErr
		}
	}
	if !written {
		return ddbErrorResponse("ConditionalCheckFailedException"), nil
	}
	return okResponse(`{}`), nil
}

func (f *fakeDedupTable) del(body []byte) (*http.Response, error) {
	var req dedupItemReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("fakeDedupTable: decode DeleteItem body: %w", err)
	}
	f.mu.Lock()
	delete(f.rows, req.Key[dedupPKAttr]["S"])
	f.mu.Unlock()
	return okResponse(`{}`), nil
}

func newFakeDedupTransport(f *fakeDedupTable) *fakeTransport {
	return &fakeTransport{
		handleOp: func(target string, inv int, body []byte) (*http.Response, error) {
			switch target {
			case "DynamoDB_20120810.PutItem":
				return f.put(inv, body)
			case "DynamoDB_20120810.DeleteItem":
				return f.del(body)
			}
			return okResponse(`{}`), nil
		},
	}
}

// runTwoPipelines feeds the same 100 EventIDs through both dedupers at once and
// returns how many records each pipeline's store would have merged.
func runTwoPipelines(t *testing.T, a, b *dynamodb.Deduper) (int64, int64) {
	t.Helper()
	const n = 100
	var mergedA, mergedB atomic.Int64
	var wg sync.WaitGroup
	for _, p := range []struct {
		d      *dynamodb.Deduper
		merged *atomic.Int64
	}{{a, &mergedA}, {b, &mergedB}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				first, err := p.d.MarkSeen(context.Background(), fmt.Sprintf("evt-%03d", i))
				if err != nil {
					t.Errorf("MarkSeen: %v", err)
					return
				}
				if first {
					// Stands in for this pipeline's own state store.
					p.merged.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	return mergedA.Load(), mergedB.Load()
}

func TestDeduper_PipelineScopesShareATableWithoutColliding(t *testing.T) {
	// One dedup table shared across pipelines is the layout doc/design.md
	// §13.4 recommends. EventIDs are only unique within a source, so two
	// pipelines reading different topics both emit "evt-007" — and unless the
	// claim key carries the pipeline name, whichever one claims it first makes
	// the other skip a merge that never happened.
	tbl := newFakeDedupTable()
	client := newFakeClient(t, newFakeDedupTransport(tbl), 1)

	orders := dynamodb.NewDeduper(client, "shared_dedup", "orders", time.Hour)
	sessions := orders.ForPipeline("sessions")

	mergedOrders, mergedSessions := runTwoPipelines(t, orders, sessions)
	if mergedOrders != 100 || mergedSessions != 100 {
		t.Fatalf("merged per pipeline: orders=%d sessions=%d, want 100 each "+
			"(a peer pipeline's claims are starving this one)", mergedOrders, mergedSessions)
	}
}

func TestDeduper_SeparateInstancesShareATableWithoutColliding(t *testing.T) {
	// The same collision, separate Go objects: the shared namespace is the
	// table's key space, not the Deduper struct, so two independently
	// constructed dedupers — the shape two worker processes actually deploy in
	// — have to stay out of each other's way too.
	tbl := newFakeDedupTable()
	client := newFakeClient(t, newFakeDedupTransport(tbl), 1)

	orders := dynamodb.NewDeduper(client, "shared_dedup", "orders", time.Hour)
	sessions := dynamodb.NewDeduper(client, "shared_dedup", "sessions", time.Hour)

	mergedOrders, mergedSessions := runTwoPipelines(t, orders, sessions)
	if mergedOrders != 100 || mergedSessions != 100 {
		t.Fatalf("merged per pipeline: orders=%d sessions=%d, want 100 each "+
			"(a peer pipeline's claims are starving this one)", mergedOrders, mergedSessions)
	}
}

func TestDeduper_LostClaimResponseIsNotADuplicate(t *testing.T) {
	// DynamoDB commits the claim, then the connection drops before the
	// response gets home. The SDK replays the request and finds the row
	// present. Without a writer identity on that row the replay is
	// indistinguishable from a peer's claim, so the runtime skips a merge that
	// never ran and the delivery is gone — no error, no metric.
	tbl := newFakeDedupTable()
	ft := newFakeDedupTransport(tbl)
	tbl.onPut = func(inv int, _ string, _ bool) (*http.Response, error) {
		if inv == 1 {
			return nil, &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}
		}
		return nil, nil
	}
	// SDK retries on, so the middleware replays the serialized request itself.
	client := newFakeClient(t, ft, 3)
	d := dynamodb.NewDeduper(client, "dedup", "orders", time.Hour)

	first, err := d.MarkSeen(context.Background(), "evt-1")
	if err != nil {
		t.Fatalf("MarkSeen: %v", err)
	}
	if !first {
		t.Fatalf("MarkSeen after a lost claim response: got firstSeen=false, want true " +
			"(the replayed claim must recognize its own row rather than read as a peer's)")
	}
	if got := ft.putItemCalls.Load(); got != 2 {
		t.Fatalf("PutItem calls: got %d, want 2 (attempt 1 committed, its response was lost)", got)
	}
	// The fix rests on the retry middleware sitting after serialization, so the
	// replay carries the identical claimant. If that ever stops holding, the
	// condition stops recognizing our own row and this assertion is the canary.
	tokens := tbl.seenClaimants()
	if len(tokens) != 2 || tokens[0] == "" || tokens[0] != tokens[1] {
		t.Errorf("claimant tokens across attempts: got %q, want two identical non-empty tokens", tokens)
	}
}
