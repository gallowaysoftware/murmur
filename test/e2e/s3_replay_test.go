// End-to-end smoke test for S3 Replay mode:
//
//	MinIO (S3-compatible) JSON-Lines archive →
//	  S3 ReplayDriver →
//	  Sum monoid →
//	  DynamoDB Int64SumStore (shadow table)
//
// This is the standard kappa-style backfill: archive lives in S3 (typically written by
// Kinesis Firehose), replay reads it back through the same pipeline DSL, results land
// in a fresh state table that the deployment system can atomically swap into the live
// query path.
//
// Skipped unless DDB_LOCAL_ENDPOINT and S3_ENDPOINT are set.
package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/gallowaysoftware/murmur/pkg/exec/replay"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	s3replay "github.com/gallowaysoftware/murmur/pkg/replay/s3"
	"github.com/gallowaysoftware/murmur/pkg/state"
	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

func TestE2E_S3Replay(t *testing.T) {
	ddbEndpoint := os.Getenv("DDB_LOCAL_ENDPOINT")
	s3Endpoint := os.Getenv("S3_ENDPOINT")
	if s3Endpoint == "" {
		s3Endpoint = "http://localhost:9000"
	}
	if ddbEndpoint == "" {
		t.Skip("DDB_LOCAL_ENDPOINT must be set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}

	// --- DDB shadow table ---
	ddbClient := awsddb.NewFromConfig(awsCfg, func(o *awsddb.Options) {
		o.BaseEndpoint = aws.String(ddbEndpoint)
	})
	table := fmt.Sprintf("murmur_e2e_replay_%d", time.Now().UnixNano())
	if err := mddb.CreateInt64Table(ctx, ddbClient, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &table})
	})
	store := mddb.NewInt64SumStore(ddbClient, table)

	// --- MinIO bucket + JSON-Lines archive ---
	s3Client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(s3Endpoint)
		o.UsePathStyle = true // MinIO requires path-style; AWS prefers virtual-host
	})
	bucket := fmt.Sprintf("murmur-e2e-replay-%d", time.Now().UnixNano())
	if _, err := s3Client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: &bucket}); err != nil {
		// Some MinIO versions return BucketAlreadyOwnedByYou for repeats; tolerate.
		if !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
			t.Fatalf("CreateBucket: %v", err)
		}
	}
	t.Cleanup(func() {
		// Best-effort cleanup: list and delete objects, then bucket.
		out, _ := s3Client.ListObjectsV2(context.Background(), &awss3.ListObjectsV2Input{Bucket: &bucket})
		for _, obj := range out.Contents {
			_, _ = s3Client.DeleteObject(context.Background(), &awss3.DeleteObjectInput{Bucket: &bucket, Key: obj.Key})
		}
		_, _ = s3Client.DeleteBucket(context.Background(), &awss3.DeleteBucketInput{Bucket: &bucket})
	})

	// Upload two JSON-Lines files: file1 covers customers A and B; file2 adds more A.
	want := map[string]int64{"customer-A": 60, "customer-B": 15}
	uploads := map[string][]order{
		"events/year=2026/month=05/file-1.jsonl": {
			{ID: "r1", CustomerID: "customer-A", Amount: 10},
			{ID: "r2", CustomerID: "customer-A", Amount: 20},
			{ID: "r3", CustomerID: "customer-B", Amount: 5},
			{ID: "r4", CustomerID: "customer-B", Amount: 5},
			{ID: "r5", CustomerID: "customer-B", Amount: 5},
		},
		"events/year=2026/month=05/file-2.jsonl": {
			{ID: "r6", CustomerID: "customer-A", Amount: 30},
		},
	}
	for key, orders := range uploads {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		for _, o := range orders {
			if err := enc.Encode(o); err != nil {
				t.Fatalf("encode: %v", err)
			}
		}
		if _, err := s3Client.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: &bucket,
			Key:    aws.String(key),
			Body:   bytes.NewReader(buf.Bytes()),
		}); err != nil {
			t.Fatalf("PutObject %s: %v", key, err)
		}
	}

	// --- Replay driver + pipeline ---
	drv, err := s3replay.NewDriver(s3replay.Config[order]{
		Client: s3Client,
		Bucket: bucket,
		Prefix: "events/",
		Decode: func(b []byte) (order, error) {
			var o order
			err := json.Unmarshal(b, &o)
			return o, err
		},
	})
	if err != nil {
		t.Fatalf("s3 driver: %v", err)
	}

	pipe := pipeline.NewPipeline[order, int64]("e2e_replay_orders").
		Key(func(o order) string { return o.CustomerID }).
		Value(func(o order) int64 { return o.Amount }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	if err := replay.Run(ctx, pipe, drv); err != nil {
		t.Fatalf("replay.Run: %v", err)
	}

	// --- Verify ---
	for customer, expected := range want {
		v, ok, err := store.Get(ctx, state.Key{Entity: customer})
		if err != nil || !ok {
			t.Errorf("Get %s: ok=%v err=%v", customer, ok, err)
			continue
		}
		if v != expected {
			t.Errorf("entity %q: got %d, want %d", customer, v, expected)
		}
	}
	t.Logf("S3 replay processed %d objects, %d total records into shadow table %q",
		len(uploads), 6, table)
}
