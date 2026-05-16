package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// DLQProducer ships poison-pill records — those that Decode rejected — to a
// Kafka dead-letter topic so they can be inspected, replayed, or simply
// counted. It is a thin convenience over a franz-go producer wired into the
// Source's OnDecodeError callback shape.
//
// The Source itself never knows about the DLQ: callers compose this producer
// with their Config[T] via the OnDecodeError field:
//
//	dlq, err := kafka.NewDLQProducer(kafka.DLQConfig{
//	    Brokers: brokers,
//	    Topic:   "page_views.dlq",
//	    Source:  "page_views",
//	})
//	if err != nil { ... }
//	defer dlq.Close(ctx)
//
//	src, _ := kafka.NewSource(kafka.Config[PageView]{
//	    Brokers:       brokers,
//	    Topic:         "page_views",
//	    ConsumerGroup: "page_views_worker",
//	    Decode:        kafka.JSONDecoder[PageView](),
//	    OnDecodeError: dlq.OnDecodeError,
//	    OnFetchError:  dlq.OnFetchError, // optional — fetch errors are usually transient
//	})
//
// Records are produced asynchronously. Close calls Flush so in-flight
// publishes are awaited; a returned error wraps the first publish that
// failed irreversibly. The producer does not retry forever — kgo's defaults
// apply (RecordPartitioner, RequiredAcks=leader, idempotent producer
// disabled). Override via Extra if you need different semantics.
//
// Wire-format: the payload is the raw bytes the source observed; headers
// carry diagnostic context:
//
//	x-murmur-source-topic       — originating topic (string)
//	x-murmur-source-partition   — partition number (string-formatted int32)
//	x-murmur-source-offset      — Kafka offset (string-formatted int64)
//	x-murmur-source-name        — caller-supplied source identifier (DLQConfig.Source)
//	x-murmur-error              — error message from Decode / fetch
//	x-murmur-error-kind         — "decode" | "fetch"
//
// Downstream consumers (DLQ tail processes, dashboards) can filter on the
// kind header to separate poison records from transient fetch failures.
type DLQProducer struct {
	client *kgo.Client
	topic  string
	source string

	mu       sync.Mutex
	firstErr error
}

// DLQConfig configures a DLQProducer.
type DLQConfig struct {
	// Brokers is the seed broker list for the DLQ cluster — typically the
	// same as the source cluster, but not required to be.
	Brokers []string

	// Topic is the dead-letter topic. Must exist (auto-create is disabled by
	// default on most production clusters). Pre-provision with retention
	// suited to your operational workflow.
	Topic string

	// Source is a short caller-supplied identifier (usually the source
	// pipeline / topic name) that downstream DLQ consumers use to attribute
	// records back to their origin.
	Source string

	// Extra lets callers append additional franz-go producer options (TLS,
	// SASL, RequiredAcks=all, idempotent producer, custom partitioner, …).
	Extra []kgo.Opt
}

// NewDLQProducer constructs a DLQProducer.
func NewDLQProducer(cfg DLQConfig) (*DLQProducer, error) {
	if cfg.Topic == "" {
		return nil, errors.New("kafka DLQ: Topic is required")
	}
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka DLQ: Brokers is required")
	}
	if cfg.Source == "" {
		return nil, errors.New("kafka DLQ: Source is required")
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
	}
	opts = append(opts, cfg.Extra...)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka DLQ new client: %w", err)
	}
	return &DLQProducer{client: client, topic: cfg.Topic, source: cfg.Source}, nil
}

// OnDecodeError is the callback shape consumed by Config.OnDecodeError.
// Publishes the raw bytes plus diagnostic headers to the DLQ topic
// asynchronously.
func (d *DLQProducer) OnDecodeError(raw []byte, partition int32, offset int64, err error) {
	rec := &kgo.Record{
		Topic: d.topic,
		Value: raw,
		Headers: []kgo.RecordHeader{
			{Key: "x-murmur-source-topic", Value: []byte(d.source)},
			{Key: "x-murmur-source-partition", Value: []byte(fmt.Sprintf("%d", partition))},
			{Key: "x-murmur-source-offset", Value: []byte(fmt.Sprintf("%d", offset))},
			{Key: "x-murmur-source-name", Value: []byte(d.source)},
			{Key: "x-murmur-error", Value: []byte(err.Error())},
			{Key: "x-murmur-error-kind", Value: []byte("decode")},
		},
	}
	d.client.Produce(context.Background(), rec, d.onPublish)
}

// OnFetchError is the callback shape consumed by Config.OnFetchError.
// Publishes a zero-byte marker record carrying the topic/partition/error
// in headers — useful for catching repeated fetch failures (a broken
// broker, a missing topic) in a single stream alongside poison-pill
// content.
//
// Most fetch errors are transient and the franz-go client retries them
// internally, so this can be noisy on real clusters; wire it only if you
// want to surface every retry.
func (d *DLQProducer) OnFetchError(topic string, partition int32, err error) {
	rec := &kgo.Record{
		Topic: d.topic,
		Value: nil,
		Headers: []kgo.RecordHeader{
			{Key: "x-murmur-source-topic", Value: []byte(topic)},
			{Key: "x-murmur-source-partition", Value: []byte(fmt.Sprintf("%d", partition))},
			{Key: "x-murmur-source-name", Value: []byte(d.source)},
			{Key: "x-murmur-error", Value: []byte(err.Error())},
			{Key: "x-murmur-error-kind", Value: []byte("fetch")},
		},
	}
	d.client.Produce(context.Background(), rec, d.onPublish)
}

// Close flushes any in-flight publishes and shuts down the producer.
// Returns the first irrecoverable publish error observed during the
// producer's lifetime, if any.
func (d *DLQProducer) Close(ctx context.Context) error {
	if err := d.client.Flush(ctx); err != nil {
		d.recordErr(err)
	}
	d.client.Close()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.firstErr
}

func (d *DLQProducer) onPublish(_ *kgo.Record, err error) {
	if err != nil {
		d.recordErr(err)
	}
}

func (d *DLQProducer) recordErr(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.firstErr == nil {
		d.firstErr = err
	}
}
