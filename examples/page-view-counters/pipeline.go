// Package pageviews defines the page-view-counters Murmur pipeline. It's imported by
// both cmd/worker (streaming runtime) and cmd/query (gRPC server) so the two
// processes share an identical pipeline definition — the same Source/Key/Value/
// Aggregate/StoreIn that streamed events get queried with.
package pageviews

import (
	"context"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/murmur"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	mkafka "github.com/gallowaysoftware/murmur/pkg/source/kafka"
	"github.com/gallowaysoftware/murmur/pkg/state"
	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
	mvalkey "github.com/gallowaysoftware/murmur/pkg/state/valkey"
)

// PageView is the event shape produced by upstream services.
type PageView struct {
	PageID string `json:"page_id"`
	UserID string `json:"user_id"`
}

// Config bundles deployment-time settings the worker and query binaries both need.
type Config struct {
	KafkaBrokers  string // comma-separated
	KafkaTopic    string
	ConsumerGroup string

	DDBEndpoint string // empty for real AWS; "http://localhost:8000" for dynamodb-local
	DDBTable    string
	DDBRegion   string

	ValkeyAddress   string // empty to disable cache layer
	ValkeyKeyPrefix string

	GRPCAddr string // e.g. ":50051"
}

// Window is the daily-bucket configuration shared by both binaries (otherwise the
// query side and the streaming side would compute different bucket IDs).
var Window = windowed.Daily(90 * 24 * time.Hour)

// Build constructs the pipeline. The Source is set by the streaming worker; the query
// server doesn't need a live source and can omit it.
func Build(ctx context.Context, cfg Config, withSource bool) (*pipeline.Pipeline[PageView, int64], state.Store[int64], state.Cache[int64], error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.DDBRegion),
	)
	if err != nil {
		return nil, nil, nil, err
	}
	// dynamodb-local convenience: route to the configured endpoint with dummy creds.
	if cfg.DDBEndpoint != "" {
		awsCfg.Credentials = credentials.NewStaticCredentialsProvider("test", "test", "")
	}
	ddbClient := awsddb.NewFromConfig(awsCfg, func(o *awsddb.Options) {
		if cfg.DDBEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.DDBEndpoint)
		}
	})
	store := mddb.NewInt64SumStore(ddbClient, cfg.DDBTable)

	var cache state.Cache[int64]
	if cfg.ValkeyAddress != "" {
		c, err := mvalkey.NewInt64Cache(mvalkey.Config{
			Address:   cfg.ValkeyAddress,
			KeyPrefix: cfg.ValkeyKeyPrefix,
		})
		if err != nil {
			return nil, nil, nil, err
		}
		cache = c
	}

	builder := murmur.Counter[PageView]("page_views").
		KeyBy(func(e PageView) string { return e.PageID }).
		Daily(90 * 24 * time.Hour).
		StoreIn(store)
	if cache != nil {
		builder = builder.Cache(cache)
	}

	if withSource {
		src, err := mkafka.NewSource(mkafka.Config[PageView]{
			Brokers:       splitAndTrim(cfg.KafkaBrokers, ","),
			Topic:         cfg.KafkaTopic,
			ConsumerGroup: cfg.ConsumerGroup,
			Decode:        mkafka.JSONDecoder[PageView](),
		})
		if err != nil {
			return nil, nil, nil, err
		}
		builder = builder.From(src)
	}

	return builder.Build(), store, cache, nil
}

func splitAndTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
