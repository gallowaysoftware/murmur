// Lambda binary for the search-projector example. Tails a Murmur counter
// table via DDB Streams and projects bucket transitions into OpenSearch.
//
// Build (Lambda runs the `provided.al2` runtime):
//
//	GOOS=linux GOARCH=arm64 go build -tags lambda.norpc \
//	    -o bootstrap ./examples/search-projector/cmd/projector
//	zip lambda.zip bootstrap
//
// Configure the event-source mapping with:
//
//	StreamViewType        = NEW_AND_OLD_IMAGES   (both images required)
//	FunctionResponseTypes = [ReportBatchItemFailures]
//	BatchSize             = 100..1000
//
// Environment:
//
//	OPENSEARCH_ENDPOINT  — https://your-domain.us-east-1.es.amazonaws.com
//	OPENSEARCH_INDEX     — target index, e.g. "posts"
//	OPENSEARCH_FIELD     — field name (default: popularity_bucket)
//	HYSTERESIS           — fractional band, e.g. "0.10" (default: 0)
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	projector "github.com/gallowaysoftware/murmur/examples/search-projector"
)

func main() {
	cfg := projector.Config{
		Index: mustEnv("OPENSEARCH_INDEX"),
		Field: envOr("OPENSEARCH_FIELD", "popularity_bucket"),
	}
	if h := os.Getenv("HYSTERESIS"); h != "" {
		v, err := strconv.ParseFloat(h, 64)
		if err == nil && v > 0 {
			cfg.Hysteresis = v
		}
	}
	endpoint := mustEnv("OPENSEARCH_ENDPOINT")

	client := newOpenSearchClient(endpoint)
	p := projector.New(cfg, client)
	log.Printf("projector starting: index=%s field=%s hysteresis=%v endpoint=%s",
		cfg.Index, cfg.Field, cfg.Hysteresis, endpoint)

	handler := func(ctx context.Context, evt events.DynamoDBEvent) (events.DynamoDBEventResponse, error) {
		failures := p.HandleEvent(ctx, evt)
		return events.DynamoDBEventResponse{BatchItemFailures: failures}, nil
	}
	lambda.Start(handler)
}

// openSearchClient is a minimal projector.IndexClient backed by net/http
// against the OpenSearch _update API. Not a full SDK — just enough to
// issue partial-update requests, which is the only operation the
// projector needs.
//
// Production deployments will likely swap this for opensearch-go/v3 or
// the AWS SDK's IAM-aware variant; the IndexClient interface in
// projector.go is the seam.
type openSearchClient struct {
	endpoint string
	http     *http.Client
}

func newOpenSearchClient(endpoint string) *openSearchClient {
	return &openSearchClient{
		endpoint: strings.TrimRight(endpoint, "/"),
		http:     http.DefaultClient,
	}
}

func (c *openSearchClient) UpdateDoc(ctx context.Context, index, docID string, fields map[string]any) error {
	url := fmt.Sprintf("%s/%s/_update/%s", c.endpoint, index, docID)
	body := buildUpdateBody(fields)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("opensearch POST %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("opensearch %s returned %d", url, resp.StatusCode)
	}
	return nil
}

// buildUpdateBody renders an OpenSearch partial-update request body.
//
// Format: `{"doc": {"<field1>": <v1>, "<field2>": <v2>, ...}, "doc_as_upsert": true}`
//
// `doc_as_upsert: true` ensures a missing target document is created
// rather than the update returning 404 — important for the projector's
// first-observation case where the document might not exist yet (the
// slow-moving-field projector creates it later).
func buildUpdateBody(fields map[string]any) string {
	var b strings.Builder
	b.WriteString(`{"doc":{`)
	first := true
	for k, v := range fields {
		if !first {
			b.WriteString(",")
		}
		first = false
		fmt.Fprintf(&b, `%q:`, k)
		switch t := v.(type) {
		case int:
			fmt.Fprintf(&b, "%d", t)
		case int64:
			fmt.Fprintf(&b, "%d", t)
		case string:
			fmt.Fprintf(&b, "%q", t)
		case float64:
			fmt.Fprintf(&b, "%g", t)
		default:
			fmt.Fprintf(&b, "%q", fmt.Sprint(v))
		}
	}
	b.WriteString(`},"doc_as_upsert":true}`)
	return b.String()
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("%s not set", key)
	}
	return v
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
