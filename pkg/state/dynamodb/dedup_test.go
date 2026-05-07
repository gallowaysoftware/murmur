package dynamodb_test

import (
	"context"
	"fmt"
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
	d := dynamodb.NewDeduper(client, table, 1*time.Hour)

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
	d := dynamodb.NewDeduper(client, table, 1*time.Hour)

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
	d := dynamodb.NewDeduper(client, table, 1*time.Hour)

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
