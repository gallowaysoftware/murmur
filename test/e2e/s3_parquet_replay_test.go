// End-to-end smoke test for pkg/replay/s3.ParquetDriver:
//
//	MinIO (S3-compatible) Parquet archive →
//	  S3 ParquetDriver →
//	  Sum monoid →
//	  DynamoDB Int64SumStore (shadow table)
//
// The Parquet companion to s3_replay_test.go's JSON-Lines smoke test.
// Skipped unless DDB_LOCAL_ENDPOINT and S3_ENDPOINT are set.
package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
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

type parquetOrder struct {
	CustomerID string
	Amount     int64
}

func TestE2E_S3ParquetReplay(t *testing.T) {
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
	table := fmt.Sprintf("murmur_e2e_parquet_%d", time.Now().UnixNano())
	if err := mddb.CreateInt64Table(ctx, ddbClient, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &table})
	})
	store := mddb.NewInt64SumStore(ddbClient, table)

	// --- MinIO bucket + Parquet archive ---
	s3Client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(s3Endpoint)
		o.UsePathStyle = true
	})
	bucket := fmt.Sprintf("murmur-e2e-parquet-%d", time.Now().UnixNano())
	if _, err := s3Client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: &bucket}); err != nil {
		if !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
			t.Fatalf("CreateBucket: %v", err)
		}
	}
	t.Cleanup(func() {
		out, _ := s3Client.ListObjectsV2(context.Background(), &awss3.ListObjectsV2Input{Bucket: &bucket})
		for _, obj := range out.Contents {
			_, _ = s3Client.DeleteObject(context.Background(), &awss3.DeleteObjectInput{Bucket: &bucket, Key: obj.Key})
		}
		_, _ = s3Client.DeleteBucket(context.Background(), &awss3.DeleteBucketInput{Bucket: &bucket})
	})

	// Two parquet "part files" covering customers A + B. The driver
	// should pick them up via the default *.parquet KeyFilter; sibling
	// .json files in the same prefix exercise the filter (they should
	// be skipped, not error).
	uploads := map[string][]parquetOrder{
		"events/year=2026/month=05/part-0.parquet": {
			{CustomerID: "customer-A", Amount: 10},
			{CustomerID: "customer-A", Amount: 20},
			{CustomerID: "customer-B", Amount: 5},
			{CustomerID: "customer-B", Amount: 5},
			{CustomerID: "customer-B", Amount: 5},
		},
		"events/year=2026/month=05/part-1.parquet": {
			{CustomerID: "customer-A", Amount: 30},
		},
	}
	want := map[string]int64{"customer-A": 60, "customer-B": 15}

	for key, orders := range uploads {
		body, err := writeParquetOrders(orders)
		if err != nil {
			t.Fatalf("build parquet %s: %v", key, err)
		}
		if _, err := s3Client.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: &bucket,
			Key:    aws.String(key),
			Body:   bytes.NewReader(body),
		}); err != nil {
			t.Fatalf("PutObject %s: %v", key, err)
		}
	}
	// Drop a non-parquet sibling so the KeyFilter's prune is exercised.
	if _, err := s3Client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: &bucket,
		Key:    aws.String("events/year=2026/month=05/_SUCCESS"),
		Body:   bytes.NewReader([]byte{}),
	}); err != nil {
		t.Fatalf("PutObject _SUCCESS: %v", err)
	}

	// --- Replay driver + pipeline ---
	drv, err := s3replay.NewParquetDriver(s3replay.ParquetConfig[parquetOrder]{
		Client: s3Client,
		Bucket: bucket,
		Prefix: "events/",
		Decode: decodeParquetOrder,
	})
	if err != nil {
		t.Fatalf("parquet driver: %v", err)
	}

	pipe := pipeline.NewPipeline[parquetOrder, int64]("e2e_parquet_replay").
		Key(func(o parquetOrder) string { return o.CustomerID }).
		Value(func(o parquetOrder) int64 { return o.Amount }).
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
	t.Logf("Parquet replay processed %d parquet objects + skipped 1 _SUCCESS into shadow table %q",
		len(uploads), table)
}

// decodeParquetOrder pulls (customer_id, amount) out of one row of a
// parquet RecordBatch. Mirrors the ParquetDecoder contract:
// allocate-once column refs, index per-row.
func decodeParquetOrder(rec arrow.RecordBatch, row int) (parquetOrder, error) {
	sc := rec.Schema()
	var o parquetOrder

	idx := sc.FieldIndices("customer_id")
	if len(idx) == 0 {
		return o, errors.New("missing customer_id column")
	}
	customerArr, ok := rec.Column(idx[0]).(*array.String)
	if !ok {
		return o, errors.New("customer_id is not String")
	}
	o.CustomerID = customerArr.Value(row)

	idx = sc.FieldIndices("amount")
	if len(idx) == 0 {
		return o, errors.New("missing amount column")
	}
	amountArr, ok := rec.Column(idx[0]).(*array.Int64)
	if !ok {
		return o, errors.New("amount is not Int64")
	}
	o.Amount = amountArr.Value(row)

	return o, nil
}

// writeParquetOrders builds an in-memory Parquet byte buffer with the
// given orders under a fixed schema ({customer_id: string, amount: int64}).
func writeParquetOrders(orders []parquetOrder) ([]byte, error) {
	mem := memory.DefaultAllocator
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "customer_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	customerB := array.NewStringBuilder(mem)
	defer customerB.Release()
	amountB := array.NewInt64Builder(mem)
	defer amountB.Release()
	for _, o := range orders {
		customerB.Append(o.CustomerID)
		amountB.Append(o.Amount)
	}

	customerArr := customerB.NewArray()
	defer customerArr.Release()
	amountArr := amountB.NewArray()
	defer amountArr.Release()

	rec := array.NewRecordBatch(sc, []arrow.Array{customerArr, amountArr}, int64(len(orders)))
	defer rec.Release()

	var buf bytes.Buffer
	if err := pqarrow.WriteTable(
		array.NewTableFromRecords(sc, []arrow.RecordBatch{rec}),
		&buf, int64(len(orders)), nil, pqarrow.DefaultWriterProps(),
	); err != nil {
		return nil, fmt.Errorf("WriteTable: %w", err)
	}
	return buf.Bytes(), nil
}
