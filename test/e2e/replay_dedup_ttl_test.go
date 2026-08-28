// End-to-end test for the HORIZON on replay idempotency:
//
//	in-memory archive → replay.Run(WithDedup(real dynamodb.Deduper)) →
//	  DynamoDB Int64SumStore
//
// pkg/exec/replay's own tests prove the runtime consults the Deduper and
// skips claimed records. They cannot prove what happens when the claims go
// away, because nothing in murmur expires them — DynamoDB's native TTL does,
// against the real dedup table. A unit test with a hand-rolled expiring fake
// would only assert against its own fake, so the claim lives here, against
// pkg/state/dynamodb.Deduper.
//
// DynamoDB Local runs no TTL sweeper on a schedule a test can wait for (real
// DynamoDB takes up to 48h), so the eviction is performed the way TTL
// performs it: DeleteItem on the claim rows, issued through the raw client
// rather than through the Deduper's own Release, so the assertion does not
// lean on the type under test to set itself up.
//
// Skipped unless DDB_LOCAL_ENDPOINT is set.
package e2e_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gallowaysoftware/murmur/pkg/exec/replay"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

// archiveDriver replays a fixed number of records with stable, positional
// EventIDs — what a real S3-archive or Kafka-offset driver produces, and the
// whole basis for a Deduper catching a re-run of the same archive.
type archiveDriver struct{ n int }

func (d *archiveDriver) Replay(_ context.Context, out chan<- source.Record[int]) error {
	for i := 0; i < d.n; i++ {
		out <- source.Record[int]{
			EventID: "archive-line-" + strconv.Itoa(i),
			Value:   1,
			Ack:     func() error { return nil },
		}
	}
	return nil
}
func (*archiveDriver) Name() string { return "archive-driver" }
func (*archiveDriver) Close() error { return nil }

// TestE2E_ReplayDedupExpiryDoubleCounts pins the horizon on replay
// idempotency against the REAL DynamoDB-backed Deduper: the protection is
// exactly as durable as the claim rows, and once DynamoDB's TTL sweeper has
// taken them, the identical archive is indistinguishable from new data and
// merges a second time.
//
// 2N, not N, is the intended contract. An operator re-running a backfill a
// day later behind a 1h dedup TTL is not protected, and neither the runtime
// nor the Deduper can tell that re-run from fresh input. Sizing the TTL to
// span the longest re-run window is the only defence.
func TestE2E_ReplayDedupExpiryDoubleCounts(t *testing.T) {
	endpoint := os.Getenv("DDB_LOCAL_ENDPOINT")
	if endpoint == "" {
		t.Skip("DDB_LOCAL_ENDPOINT must be set")
	}

	const records = 100

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	ddbClient := awsddb.NewFromConfig(awsCfg, func(o *awsddb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	suffix := time.Now().UnixNano()
	stateTable := fmt.Sprintf("murmur_e2e_replayttl_state_%d", suffix)
	dedupTable := fmt.Sprintf("murmur_e2e_replayttl_dedup_%d", suffix)

	if err := mddb.CreateInt64Table(ctx, ddbClient, stateTable); err != nil {
		t.Fatalf("create state table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &stateTable})
	})
	if err := mddb.CreateDedupTable(ctx, ddbClient, dedupTable); err != nil {
		t.Fatalf("create dedup table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &dedupTable})
	})

	store := mddb.NewInt64SumStore(ddbClient, stateTable)
	t.Cleanup(func() { _ = store.Close() })

	// A generous TTL: this test never waits it out, it evicts the rows the
	// way DynamoDB's sweeper eventually would.
	// The pipeline name scopes the claim key ("<pipeline>#<EventID>"), so it
	// must match the pipeline built below or the claims land in a namespace
	// nothing reads.
	deduper := mddb.NewDeduper(ddbClient, dedupTable, "replay_dedup_ttl", time.Hour)

	newPipe := func() *pipeline.Pipeline[int, int64] {
		return pipeline.NewPipeline[int, int64]("replay_dedup_ttl").
			Key(func(int) string { return "all" }).
			Value(func(int) int64 { return 1 }).
			Aggregate(core.Sum[int64]()).
			StoreIn(store)
	}
	total := func() int64 {
		t.Helper()
		v, ok, err := store.Get(ctx, state.Key{Entity: "all"})
		if err != nil {
			t.Fatalf("store Get: %v", err)
		}
		if !ok {
			return 0
		}
		return v
	}

	// --- Pass 1: the backfill itself. ---
	if err := replay.Run(ctx, newPipe(), &archiveDriver{n: records}, replay.WithDedup(deduper)); err != nil {
		t.Fatalf("first replay: %v", err)
	}
	if got := total(); got != records {
		t.Fatalf("after first replay: got %d, want %d", got, records)
	}

	// --- Pass 2: an immediate re-run, claims still live. ---
	if err := replay.Run(ctx, newPipe(), &archiveDriver{n: records}, replay.WithDedup(deduper)); err != nil {
		t.Fatalf("second replay: %v", err)
	}
	if got := total(); got != records {
		t.Fatalf("re-run inside the dedup TTL: got %d, want %d (the claims should suppress every merge)",
			got, records)
	}

	// --- Evict the claims, exactly as DynamoDB's TTL sweeper does. ---
	if evicted := deleteAllDedupClaims(ctx, t, ddbClient, dedupTable); evicted != records {
		t.Fatalf("evicted %d dedup claims, want %d — the Deduper did not write one row per record",
			evicted, records)
	}

	// --- Pass 3: same archive, no claims. It looks brand new. ---
	if err := replay.Run(ctx, newPipe(), &archiveDriver{n: records}, replay.WithDedup(deduper)); err != nil {
		t.Fatalf("third replay: %v", err)
	}
	if got := total(); got != 2*records {
		t.Errorf("re-run past the dedup TTL: got %d, want %d (claims expire; the merge repeats)",
			got, 2*records)
	}
}

// deleteAllDedupClaims scans the dedup table and deletes every claim row,
// standing in for DynamoDB's TTL sweeper. It goes through the raw client
// rather than Deduper.Release so the eviction does not depend on the
// behaviour of the type under test. Returns the number of rows removed.
func deleteAllDedupClaims(ctx context.Context, t *testing.T, client *awsddb.Client, table string) int {
	t.Helper()
	var deleted int
	var start map[string]ddbtypes.AttributeValue
	for {
		out, err := client.Scan(ctx, &awsddb.ScanInput{
			TableName:         &table,
			ExclusiveStartKey: start,
		})
		if err != nil {
			t.Fatalf("scan dedup table: %v", err)
		}
		for _, item := range out.Items {
			pk, ok := item["pk"]
			if !ok {
				t.Fatalf("dedup row has no pk attribute: %+v", item)
			}
			if _, err := client.DeleteItem(ctx, &awsddb.DeleteItemInput{
				TableName: &table,
				Key:       map[string]ddbtypes.AttributeValue{"pk": pk},
			}); err != nil {
				t.Fatalf("delete dedup row: %v", err)
			}
			deleted++
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		start = out.LastEvaluatedKey
	}
	return deleted
}
