// Package s3 provides a replay.Driver that reads JSON-Lines event archives from S3
// (or any S3-compatible store like MinIO) and feeds the records into the pipeline.
//
// This is the standard "kappa replay" path for Kinesis-backed pipelines whose live
// retention is shorter than the desired backfill window. Events are typically archived
// to S3 by Kinesis Data Firehose with date-partitioning (e.g. year=/month=/day=/), or
// by Kafka Connect's S3 sink for Kafka topics.
//
// Phase 1 supports JSON-Lines (one JSON object per line). Parquet support is a Phase 2
// addition gated on real demand.
package s3

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/gallowaysoftware/murmur/pkg/source"
)

// Config configures an S3 replay driver.
type Config[T any] struct {
	Client *s3.Client
	Bucket string
	// Prefix narrows the object scan (e.g. "events/year=2026/month=05/"). Empty means
	// the entire bucket.
	Prefix string
	// Decode converts a single JSON-Lines line to T.
	Decode Decoder[T]
}

// Decoder converts a single JSON line to a typed Record value.
type Decoder[T any] func([]byte) (T, error)

// Driver implements replay.Driver for S3-archived JSON-Lines events.
type Driver[T any] struct {
	client *s3.Client
	bucket string
	prefix string
	decode Decoder[T]
}

// NewDriver returns a Driver. The client is owned by the caller.
func NewDriver[T any](cfg Config[T]) (*Driver[T], error) {
	if cfg.Client == nil {
		return nil, errors.New("s3 replay: Client is required")
	}
	if cfg.Bucket == "" {
		return nil, errors.New("s3 replay: Bucket is required")
	}
	if cfg.Decode == nil {
		return nil, errors.New("s3 replay: Decode is required")
	}
	return &Driver[T]{
		client: cfg.Client,
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
		decode: cfg.Decode,
	}, nil
}

// Replay enumerates all objects under (Bucket, Prefix), reads each as JSON-Lines, and
// emits one source.Record per line. Returns when all objects have been consumed or ctx
// is canceled. Object enumeration is paged via ListObjectsV2.
func (d *Driver[T]) Replay(ctx context.Context, out chan<- source.Record[T]) error {
	keys, err := d.listAll(ctx)
	if err != nil {
		return fmt.Errorf("s3 replay list: %w", err)
	}
	for _, key := range keys {
		if err := d.replayOne(ctx, key, out); err != nil {
			return err
		}
	}
	return nil
}

func (d *Driver[T]) listAll(ctx context.Context) ([]string, error) {
	var keys []string
	var token *string
	for {
		out, err := d.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &d.bucket,
			Prefix:            stringPtrOrNil(d.prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, obj := range out.Contents {
			if obj.Key == nil {
				continue
			}
			// Skip "directory" placeholder objects.
			if strings.HasSuffix(aws.ToString(obj.Key), "/") {
				continue
			}
			keys = append(keys, aws.ToString(obj.Key))
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		token = out.NextContinuationToken
	}
	return keys, nil
}

func (d *Driver[T]) replayOne(ctx context.Context, key string, out chan<- source.Record[T]) error {
	resp, err := d.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &d.bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("s3 GetObject %s/%s: %w", d.bucket, key, err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	// Allow large lines; default 64KB is too small for some Firehose buffer sizes.
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		v, err := d.decode(line)
		if err != nil {
			// Poison line: skip. Production: DLQ.
			continue
		}
		rec := source.Record[T]{
			EventID:      fmt.Sprintf("s3:%s/%s:%d", d.bucket, key, lineNum),
			PartitionKey: key,
			Value:        v,
			Ack:          func() error { return nil },
		}
		select {
		case out <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("s3 read %s/%s: %w", d.bucket, key, err)
	}
	return nil
}

// Name returns "s3:<bucket>/<prefix>".
func (d *Driver[T]) Name() string {
	return fmt.Sprintf("s3:%s/%s", d.bucket, d.prefix)
}

// Close is a no-op; the underlying client is owned by the caller.
func (d *Driver[T]) Close() error { return nil }

func stringPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
