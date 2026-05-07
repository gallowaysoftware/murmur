package swap_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/gallowaysoftware/murmur/pkg/swap"
)

func ddbClient(t *testing.T) *awsddb.Client {
	t.Helper()
	endpoint := os.Getenv("DDB_LOCAL_ENDPOINT")
	if endpoint == "" {
		t.Skip("DDB_LOCAL_ENDPOINT not set")
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	return awsddb.NewFromConfig(cfg, func(o *awsddb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func TestManager_FirstSetSucceeds(t *testing.T) {
	c := ddbClient(t)
	ctx := context.Background()
	control := fmt.Sprintf("murmur_swap_first_%d", time.Now().UnixNano())
	if err := swap.CreateControlTable(ctx, c, control); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _, _ = c.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &control}) })

	m := swap.New(c, control)

	if _, err := m.GetActive(ctx, "page_views"); !errors.Is(err, swap.ErrNoActiveVersion) {
		t.Fatalf("initial GetActive: got %v, want ErrNoActiveVersion", err)
	}
	if name, err := m.Resolve(ctx, "page_views"); err != nil || name != "page_views_v1" {
		t.Fatalf("Resolve before SetActive: got (%q,%v), want (page_views_v1,nil)", name, err)
	}
	if err := m.SetActive(ctx, "page_views", 1); err != nil {
		t.Fatalf("first SetActive: %v", err)
	}
	if v, err := m.GetActive(ctx, "page_views"); err != nil || v != 1 {
		t.Fatalf("GetActive after first SetActive: got (%d,%v), want (1,nil)", v, err)
	}
}

func TestManager_AdvancesMonotonically(t *testing.T) {
	c := ddbClient(t)
	ctx := context.Background()
	control := fmt.Sprintf("murmur_swap_mono_%d", time.Now().UnixNano())
	if err := swap.CreateControlTable(ctx, c, control); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _, _ = c.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &control}) })

	m := swap.New(c, control)
	for _, v := range []int64{1, 2, 3, 5, 10} {
		if err := m.SetActive(ctx, "orders", v); err != nil {
			t.Fatalf("SetActive(%d): %v", v, err)
		}
	}
	if got, err := m.GetActive(ctx, "orders"); err != nil || got != 10 {
		t.Fatalf("after sequence: got (%d,%v), want (10,nil)", got, err)
	}
	if name, err := m.Resolve(ctx, "orders"); err != nil || name != "orders_v10" {
		t.Fatalf("Resolve: got (%q,%v), want (orders_v10,nil)", name, err)
	}
}

func TestManager_RegressionRefused(t *testing.T) {
	c := ddbClient(t)
	ctx := context.Background()
	control := fmt.Sprintf("murmur_swap_regress_%d", time.Now().UnixNano())
	if err := swap.CreateControlTable(ctx, c, control); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _, _ = c.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &control}) })

	m := swap.New(c, control)
	if err := m.SetActive(ctx, "x", 5); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := m.SetActive(ctx, "x", 4); !errors.Is(err, swap.ErrVersionRegressed) {
		t.Fatalf("regress to 4 from 5: got %v, want ErrVersionRegressed", err)
	}
	if err := m.SetActive(ctx, "x", 5); !errors.Is(err, swap.ErrVersionRegressed) {
		// Same version should also be refused (strict >).
		t.Fatalf("re-set to 5: got %v, want ErrVersionRegressed", err)
	}
	// Should still be at 5.
	if got, _ := m.GetActive(ctx, "x"); got != 5 {
		t.Errorf("after refused regress: got %d, want 5", got)
	}
}

func TestTableName(t *testing.T) {
	if got := swap.TableName("page_views", 7); got != "page_views_v7" {
		t.Errorf("TableName: got %q, want page_views_v7", got)
	}
}
