// Lambda binary for the recently-interacted-topk example.
//
// Reads Segment-style interaction events from Kinesis (via Lambda's Kinesis
// trigger) and folds them into the shared TopK sketch in DynamoDB. Pair with
// the cmd/worker binary that consumes a parallel Kafka topic — both write to
// the same DDB row, so the merged TopK reflects interactions from BOTH
// channels.
//
// Build (Lambda runs the `provided.al2` runtime):
//
//	GOOS=linux GOARCH=arm64 go build -tags lambda.norpc \
//	    -o bootstrap ./examples/recently-interacted-topk/cmd/lambda
//	zip lambda.zip bootstrap
//
// Deploy via your Lambda toolchain of choice (SAM / CDK / Terraform). The
// event-source mapping should set:
//
//	FunctionResponseTypes = ["ReportBatchItemFailures"]
//	BatchSize             = 100..1000 (tune per processing time)
//	StartingPosition      = LATEST  (or TRIM_HORIZON for full backfill)
package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"

	example "github.com/gallowaysoftware/murmur/examples/recently-interacted-topk"
	mkinesis "github.com/gallowaysoftware/murmur/pkg/exec/lambda/kinesis"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
)

func main() {
	cfg := example.Config{
		DDBEndpoint:     os.Getenv("DDB_ENDPOINT"),
		DDBTable:        envOr("DDB_TABLE", "recently_interacted"),
		DDBRegion:       envOr("AWS_REGION", "us-east-1"),
		DDBDedupTable:   os.Getenv("DDB_DEDUP_TABLE"), // recommended in production
		DedupTTL:        24 * time.Hour,
		K:               envU32("TOPK_K", 32),
		WindowRetention: 30 * 24 * time.Hour,
	}

	ctx := context.Background()
	pipe, store, deduper, err := example.Build(ctx, cfg)
	if err != nil {
		log.Fatalf("build pipeline: %v", err)
	}
	defer func() { _ = store.Close() }()
	if deduper != nil {
		defer func() { _ = deduper.Close() }()
	}

	rec := metrics.NewInMemory()
	opts := []mkinesis.HandlerOption{
		mkinesis.WithMetrics(rec),
		mkinesis.WithMaxAttempts(4),
	}
	if deduper != nil {
		opts = append(opts, mkinesis.WithDedup(deduper))
	}
	opts = append(opts, mkinesis.WithDecodeErrorCallback(func(_ []byte, seq, _ string, err error) {
		// Production code should push poison pills to a DLQ topic. For the
		// example, log and move on.
		log.Printf("decode error: seq=%s err=%v", seq, err)
	}))

	handler, err := mkinesis.NewHandler(pipe, mkinesis.JSONDecoder[example.Interaction](), opts...)
	if err != nil {
		log.Fatalf("build kinesis handler: %v", err)
	}
	lambda.Start(handler)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envU32(key string, fallback uint32) uint32 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil || n == 0 {
		log.Printf("invalid %s=%q, using default %d (err: %v)", key, v, fallback, err)
		return fallback
	}
	return uint32(n)
}
