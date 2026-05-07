// End-to-end smoke test for the Spark Connect batch executor.
//
//	Spark Connect (local[*]) → SQL aggregation → DDB Int64SumStore (shadow table)
//
// Skipped unless SPARK_CONNECT_REMOTE and DDB_LOCAL_ENDPOINT are set:
//
//	docker compose up -d spark-connect dynamodb-local
//	SPARK_CONNECT_REMOTE=sc://localhost:15002 \
//	DDB_LOCAL_ENDPOINT=http://localhost:8000 \
//	  go test ./test/e2e/... -run TestE2E_SparkConnect -v
//
// The test uses inline VALUES rather than reading from S3/MinIO so we exercise the
// row-iteration + DDB write path independently of any S3-Hadoop classpath setup.
package e2e_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/gallowaysoftware/murmur/pkg/exec/batch/sparkconnect"
	"github.com/gallowaysoftware/murmur/pkg/state"
	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

func TestE2E_SparkConnect(t *testing.T) {
	remote := os.Getenv("SPARK_CONNECT_REMOTE")
	if remote == "" {
		t.Skip("SPARK_CONNECT_REMOTE not set; run docker-compose up spark-connect")
	}
	ddbEndpoint := os.Getenv("DDB_LOCAL_ENDPOINT")
	if ddbEndpoint == "" {
		t.Skip("DDB_LOCAL_ENDPOINT not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	ddbClient := awsddb.NewFromConfig(awsCfg, func(o *awsddb.Options) {
		o.BaseEndpoint = aws.String(ddbEndpoint)
	})
	table := fmt.Sprintf("murmur_e2e_spark_%d", time.Now().UnixNano())
	if err := mddb.CreateInt64Table(ctx, ddbClient, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &table})
	})
	store := mddb.NewInt64SumStore(ddbClient, table)

	// Inline aggregation — synthesize a stream of "events" via VALUES, group by
	// the would-be entity key (pk), sum the would-be delta column.
	sql := `
		SELECT pk, SUM(v) AS v
		FROM VALUES
			('page-A', CAST(1 AS BIGINT)),
			('page-A', CAST(1 AS BIGINT)),
			('page-A', CAST(1 AS BIGINT)),
			('page-B', CAST(1 AS BIGINT)),
			('page-B', CAST(1 AS BIGINT)),
			('page-C', CAST(7 AS BIGINT))
		AS t(pk, v)
		GROUP BY pk
	`

	if err := sparkconnect.RunInt64Sum(ctx, sparkconnect.Config{
		Remote: remote,
		SQL:    sql,
	}, store, 0); err != nil {
		t.Fatalf("RunInt64Sum: %v", err)
	}

	want := map[string]int64{"page-A": 3, "page-B": 2, "page-C": 7}
	for entity, expected := range want {
		v, ok, err := store.Get(ctx, state.Key{Entity: entity})
		if err != nil || !ok {
			t.Errorf("Get %s: ok=%v err=%v", entity, ok, err)
			continue
		}
		if v != expected {
			t.Errorf("entity %q: got %d, want %d", entity, v, expected)
		}
	}
	t.Logf("Spark Connect aggregated 6 rows into 3 entity totals via DDB atomic ADD")
}
