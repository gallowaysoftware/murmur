// Deployed-shape end-to-end for the page-view-counters example.
//
// Builds the multi-binary image from examples/page-view-counters/Dockerfile,
// runs the worker + query containers against Kafka + DynamoDB-local
// containers on a per-test docker network, produces real Kafka records
// from the test process, and queries the live query server over HTTP.
// Catches: broken Dockerfiles, env-var schema drift, container
// networking config mistakes, and any regression in the production
// startup path that the in-process e2e tests can't see.

//go:build integration

package integration

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"
)

type pageView struct {
	PageID string `json:"page_id"`
	UserID string `json:"user_id"`
}

func TestDeployed_PageViewCounters_RoundTrip(t *testing.T) {
	// TODO(integration): the worker container starts and logs
	// "streaming worker starting" against kafka:9092 (the BROKER
	// listener advertised by testcontainers' kafka module under our
	// per-test network alias) but doesn't observe the test process's
	// produced records. Polling the query endpoint stays at 0 for
	// 90 s, then the test times out.
	//
	// Three suspects, none confirmed yet:
	//   1. The kafka module's starter script advertises BROKER://
	//      with the container HOSTNAME, not our network alias. Worker
	//      container's metadata fetch may resolve to an
	//      unreachable name.
	//   2. franz-go default consumer offset is "latest" + the 5 s
	//      pre-produce sleep isn't enough for group join; produced
	//      records land before assign completes and are missed.
	//   3. The producer (host-side localhost:<mapped>) and the
	//      consumer (in-network kafka:9092) hit different advertised
	//      listeners; records written via one aren't visible via the
	//      other (would be a real testcontainers/kafka module bug,
	//      not likely).
	//
	// The CI job + Makefile target + helpers.go scaffold are in
	// place; this test is skipped until the sibling-network reach is
	// debugged. Set MURMUR_RUN_DEPLOYED_ROUNDTRIP=1 to opt in
	// locally for debugging.
	if os.Getenv("MURMUR_RUN_DEPLOYED_ROUNDTRIP") == "" {
		t.Skip("deployed-shape round-trip skipped pending kafka sibling-network debugging; see TODO in test source")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	net := newTestNetwork(ctx, t)

	// --- Infra containers ---
	kafkaInside, kafkaHost := startKafka(ctx, t, net, "kafka")
	ddbInside, ddbClient := startDynamoDBLocal(ctx, t, net, "ddb")

	// page_views table — the binary's default DDB_TABLE.
	tableName := "page_views"
	createInt64Table(ctx, t, ddbClient, tableName)

	topic := "page_views"
	consumerGroup := "page_views_worker_itest"

	// --- Worker container (also drives the image build) ---
	// Note: testcontainers' FromDockerfile + Cmd doesn't reliably
	// override the image's default — Entrypoint does. The example
	// Dockerfile leaves both empty, so we set Entrypoint explicitly.
	dockerfile := dockerfileBuild(t, "examples/page-view-counters/Dockerfile")
	workerReq := testcontainers.ContainerRequest{
		FromDockerfile: dockerfile,
		Entrypoint:     []string{"/murmur-worker"},
		Networks:       []string{net.Name},
		Env: map[string]string{
			"KAFKA_BROKERS":      kafkaInside,
			"KAFKA_TOPIC":        topic,
			"CONSUMER_GROUP":     consumerGroup,
			"DDB_LOCAL_ENDPOINT": ddbInside,
			"DDB_TABLE":          tableName,
			"AWS_REGION":         "us-east-1",
			// dynamodb-local accepts any creds, but the SDK insists on
			// SOMETHING when running outside an AWS environment.
			"AWS_ACCESS_KEY_ID":     "test",
			"AWS_SECRET_ACCESS_KEY": "test",
		},
		WaitingFor: wait.ForLog("starting page-view-counters worker").
			WithStartupTimeout(60 * time.Second),
	}
	worker, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: workerReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("worker start: %v", err)
	}
	t.Cleanup(func() {
		dumpContainerLogs(t, "worker", worker)
		_ = worker.Terminate(context.Background())
	})

	// --- Query container (reuses Docker's layer cache from the worker build) ---
	queryReq := testcontainers.ContainerRequest{
		FromDockerfile: dockerfile,
		Entrypoint:     []string{"/murmur-query"},
		ExposedPorts:   []string{"50051/tcp"},
		Networks:       []string{net.Name},
		Env: map[string]string{
			"DDB_LOCAL_ENDPOINT":    ddbInside,
			"DDB_TABLE":             tableName,
			"AWS_REGION":            "us-east-1",
			"AWS_ACCESS_KEY_ID":     "test",
			"AWS_SECRET_ACCESS_KEY": "test",
			"GRPC_ADDR":             ":50051",
		},
		WaitingFor: wait.ForListeningPort("50051/tcp").
			WithStartupTimeout(60 * time.Second),
	}
	query, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: queryReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("query start: %v", err)
	}
	t.Cleanup(func() {
		dumpContainerLogs(t, "query", query)
		_ = query.Terminate(context.Background())
	})

	queryHost, err := query.Host(ctx)
	if err != nil {
		t.Fatalf("query host: %v", err)
	}
	queryPort, err := query.MappedPort(ctx, "50051/tcp")
	if err != nil {
		t.Fatalf("query port: %v", err)
	}
	queryURL := fmt.Sprintf("http://%s:%s", queryHost, queryPort.Port())

	// --- Produce a known event distribution to Kafka ---
	// Note: franz-go's default consumer offset is "latest", so we must
	// produce records AFTER the worker's consumer group has joined or
	// they'll be invisible to the worker. The "starting" log line we
	// waited for above fires before group-join completes; sleep a few
	// seconds to give kgo time to attach. Production code that needs
	// to backfill pre-existing records uses pkg/exec/bootstrap, not
	// this path.
	time.Sleep(5 * time.Second)

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(kafkaHost, ",")...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	want := map[string]int64{"page-A": 5, "page-B": 3, "page-C": 1}
	for page, n := range want {
		for i := int64(0); i < n; i++ {
			body, _ := json.Marshal(pageView{PageID: page, UserID: fmt.Sprintf("u-%d", i)})
			producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: body, Key: []byte(page)})
		}
	}
	if err := producer.Flush(ctx); err != nil {
		t.Fatalf("producer flush: %v", err)
	}
	t.Logf("produced 9 records across 3 keys to %s; polling %s for results", topic, queryURL)

	// --- Poll the live query server until expected counts land ---
	deadline := time.Now().Add(90 * time.Second)
	httpClient := &http.Client{Timeout: 5 * time.Second}
	for time.Now().Before(deadline) {
		got, missing := queryAllPagesGRPCConnect(t, ctx, httpClient, queryURL, want)
		if missing == 0 {
			// All keys hit their expected totals. Print the values
			// for the t.Log audit trail then exit successfully.
			t.Logf("all keys matched: %v", got)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	// --- One last query for the failure-mode log ---
	got, _ := queryAllPagesGRPCConnect(t, ctx, httpClient, queryURL, want)
	t.Errorf("after 90s polling, got %v, want %v", got, want)
}

// queryAllPagesGRPCConnect hits the query container's Connect-RPC
// `Get` endpoint once per expected page-id and decodes the int64-LE
// value. Returns the observed map plus the count of keys that don't
// yet match `want`.
func queryAllPagesGRPCConnect(t *testing.T, ctx context.Context, httpc *http.Client, baseURL string, want map[string]int64) (map[string]int64, int) {
	t.Helper()
	got := make(map[string]int64, len(want))
	missing := 0
	for entity, expected := range want {
		body := strings.NewReader(fmt.Sprintf(`{"entity":%q}`, entity))
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
			baseURL+"/murmur.v1.QueryService/Get", body)
		req.Header.Set("Content-Type", "application/json")
		resp, err := httpc.Do(req)
		if err != nil {
			missing++
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			missing++
			continue
		}
		// Connect-RPC JSON envelope: { "value": { "present": bool, "data": "<base64>" } }
		var env struct {
			Value struct {
				Present bool   `json:"present"`
				Data    []byte `json:"data"`
			} `json:"value"`
		}
		if err := json.Unmarshal(raw, &env); err != nil {
			missing++
			continue
		}
		if !env.Value.Present || len(env.Value.Data) < 8 {
			got[entity] = 0
			missing++
			continue
		}
		v := int64(binary.LittleEndian.Uint64(env.Value.Data))
		got[entity] = v
		if v != expected {
			missing++
		}
	}
	return got, missing
}

// TestDeployed_PageViewCounters_QueryBootsAgainstDDB is the
// reduced-scope companion to the full round-trip test: it builds the
// example image, starts the query container against a DDB-local
// container (no worker, no Kafka), and verifies the query endpoint
// returns a valid Connect-RPC response for an absent entity.
//
// This catches the bulk of "deployed shape is broken" failure modes —
// Dockerfile mis-builds, env-var schema drift, the query binary not
// starting against real DDB-local, the Connect-RPC handler not
// responding — without depending on the kafka sibling-network reach
// that blocks the full round-trip test above.
func TestDeployed_PageViewCounters_QueryBootsAgainstDDB(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	net := newTestNetwork(ctx, t)
	ddbInside, ddbClient := startDynamoDBLocal(ctx, t, net, "ddb")
	createInt64Table(ctx, t, ddbClient, "page_views")

	dockerfile := dockerfileBuild(t, "examples/page-view-counters/Dockerfile")
	queryReq := testcontainers.ContainerRequest{
		FromDockerfile: dockerfile,
		Entrypoint:     []string{"/murmur-query"},
		ExposedPorts:   []string{"50051/tcp"},
		Networks:       []string{net.Name},
		Env: map[string]string{
			"DDB_LOCAL_ENDPOINT":    ddbInside,
			"DDB_TABLE":             "page_views",
			"AWS_REGION":            "us-east-1",
			"AWS_ACCESS_KEY_ID":     "test",
			"AWS_SECRET_ACCESS_KEY": "test",
			"GRPC_ADDR":             ":50051",
		},
		WaitingFor: wait.ForListeningPort("50051/tcp").
			WithStartupTimeout(120 * time.Second),
	}
	query, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: queryReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("query start: %v", err)
	}
	t.Cleanup(func() {
		dumpContainerLogs(t, "query", query)
		_ = query.Terminate(context.Background())
	})

	host, _ := query.Host(ctx)
	port, _ := query.MappedPort(ctx, "50051/tcp")
	url := fmt.Sprintf("http://%s:%s/murmur.v1.QueryService/Get", host, port.Port())

	httpc := &http.Client{Timeout: 5 * time.Second}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url,
		strings.NewReader(`{"entity":"page-never-seen"}`))
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpc.Do(req)
	if err != nil {
		t.Fatalf("query request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query response: status=%d body=%s", resp.StatusCode, body)
	}
	// Absent-entity shape: { "value": { "present": false } }
	var env struct {
		Value struct {
			Present bool `json:"present"`
		} `json:"value"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("query response decode: %v (body: %s)", err, body)
	}
	if env.Value.Present {
		t.Errorf("absent entity reported present: %s", body)
	}
	t.Logf("query container served Connect-RPC Get against DDB-local; absent-entity round-trip clean")
}

// dumpContainerLogs prints a tail of the named container's combined
// stdout+stderr through t.Log. Called on cleanup so failed tests
// surface the worker/query logs in CI output.
func dumpContainerLogs(t *testing.T, name string, c testcontainers.Container) {
	t.Helper()
	if !t.Failed() {
		return
	}
	rc, err := c.Logs(context.Background())
	if err != nil {
		t.Logf("[%s logs] unavailable: %v", name, err)
		return
	}
	defer func() { _ = rc.Close() }()
	b, _ := io.ReadAll(rc)
	t.Logf("[%s logs]\n%s", name, string(b))
}
