// Package integration holds testcontainers-driven end-to-end tests
// that exercise Murmur in its DEPLOYED shape — built binaries running
// in containers, talking to real Kafka / DynamoDB-local containers,
// configured by env vars.
//
// In contrast, test/e2e/ exercises Murmur as a Go LIBRARY:
// `streaming.Run` called in-process against a docker-compose stack the
// operator brings up. Those tests catch library-API bugs; these tests
// catch broken Dockerfiles, env-var schema drift, bootstrap-to-live
// handoff bugs, and multi-binary races that pure-in-process tests
// can't see.
//
// All tests in this package are gated by the `integration` build tag
// so `make test-unit` / `go test ./...` skips them by default. Run:
//
//	go test -tags=integration -timeout 15m ./test/integration/...
//
// or via the Make target:
//
//	make test-deployed
//
// Requires a working Docker daemon (~3 GB RAM headroom for Kafka +
// the built images). CI runs this in a parallel job; locally expect
// 2–5 min for the first run (Docker layer cache is empty), then
// 30–90 s for subsequent runs.

//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dockercontainer "github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

// repoRoot returns the absolute path to the repo root so testcontainers'
// FromDockerfile.Context can resolve the example Dockerfiles.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// test/integration/helpers.go → ../..
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

// newTestNetwork creates a docker network for one test's containers so
// the worker / query containers can reach kafka / ddb by service name
// without colliding with parallel tests.
func newTestNetwork(ctx context.Context, t *testing.T) *testcontainers.DockerNetwork {
	t.Helper()
	net, err := network.New(ctx)
	if err != nil {
		t.Fatalf("create network: %v", err)
	}
	t.Cleanup(func() { _ = net.Remove(context.Background()) })
	return net
}

// startKafka launches a single-node KRaft Kafka container on the given
// network via the testcontainers/kafka module. The module's image
// (confluentinc/confluent-local) ships a starter script that
// configures two PLAINTEXT listeners automatically:
//
//   - PLAINTEXT://<host>:<mapped-port> — for the test process producing
//     records directly. Returned as `hostBootstrap`.
//   - BROKER://<hostname>:9092 — for sibling containers reaching via the
//     docker network. The container's hostname is set to `alias` so
//     sibling containers reach it as `<alias>:9092`. Returned as
//     `inContainerBootstrap`.
//
// Both listeners are mapped to PLAINTEXT under the hood.
func startKafka(ctx context.Context, t *testing.T, net *testcontainers.DockerNetwork, alias string) (inContainerBootstrap, hostBootstrap string) {
	t.Helper()

	kc, err := tckafka.Run(ctx,
		"confluentinc/confluent-local:7.6.1",
		network_alias(net.Name, alias),
		// Override the container's hostname so the module's starter
		// script advertises BROKER://<alias>:9092 — which is what
		// sibling containers on `net` actually use to reach the broker.
		// (testcontainers-go has no WithHostname helper; the container
		// config's Hostname field is the underlying knob.)
		testcontainers.WithConfigModifier(func(c *dockercontainer.Config) {
			c.Hostname = alias
		}),
	)
	if err != nil {
		t.Fatalf("kafka start: %v", err)
	}
	t.Cleanup(func() { _ = kc.Terminate(context.Background()) })

	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}
	hostBootstrap = brokers[0]
	inContainerBootstrap = fmt.Sprintf("%s:9092", alias)
	return
}

// network_alias attaches the given network + alias to a container
// request. Used by startKafka (via the kafka module's customizer
// interface) and other helpers.
func network_alias(networkName, alias string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Networks = append(req.Networks, networkName)
		if req.NetworkAliases == nil {
			req.NetworkAliases = map[string][]string{}
		}
		req.NetworkAliases[networkName] = append(req.NetworkAliases[networkName], alias)
		return nil
	}
}

// startDynamoDBLocal launches Amazon's DynamoDB Local in shared-DB +
// in-memory mode on the given network. Returns the in-container URL
// (for sibling containers) and a host-side *dynamodb.Client the test
// uses to create tables and assert state.
func startDynamoDBLocal(ctx context.Context, t *testing.T, net *testcontainers.DockerNetwork, alias string) (inContainerURL string, hostClient *awsddb.Client) {
	t.Helper()

	req := testcontainers.ContainerRequest{
		Image:        "amazon/dynamodb-local:latest",
		ExposedPorts: []string{"8000/tcp"},
		Cmd:          []string{"-jar", "DynamoDBLocal.jar", "-sharedDb", "-inMemory"},
		WaitingFor:   wait.ForListeningPort("8000/tcp").WithStartupTimeout(60 * time.Second),
		Networks:     []string{net.Name},
		NetworkAliases: map[string][]string{
			net.Name: {alias},
		},
	}
	dc, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("ddb start: %v", err)
	}
	t.Cleanup(func() { _ = dc.Terminate(context.Background()) })

	host, err := dc.Host(ctx)
	if err != nil {
		t.Fatalf("ddb host: %v", err)
	}
	port, err := dc.MappedPort(ctx, "8000/tcp")
	if err != nil {
		t.Fatalf("ddb port: %v", err)
	}
	hostURL := fmt.Sprintf("http://%s:%s", host, port.Port())
	inContainerURL = fmt.Sprintf("http://%s:8000", alias)

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	hostClient = awsddb.NewFromConfig(cfg, func(o *awsddb.Options) {
		o.BaseEndpoint = aws.String(hostURL)
	})
	return
}

// dockerfileBuild returns a FromDockerfile spec configured for one of
// the example Dockerfiles. Tests pass this into a GenericContainerRequest's
// FromDockerfile field. Docker's layer cache makes the second build
// (the query container building from the same Dockerfile the worker
// just built) effectively instant.
//
// Image is removed by testcontainers on container Terminate (the
// KeepImage default is false). To survive failed-test images for
// `docker run` poking, set MURMUR_KEEP_TEST_IMAGES=1 in the
// environment.
func dockerfileBuild(t *testing.T, dockerfileRelPath string) testcontainers.FromDockerfile {
	t.Helper()
	keep := false
	if v, ok := os.LookupEnv("MURMUR_KEEP_TEST_IMAGES"); ok && v != "" && v != "0" {
		keep = true
	}
	return testcontainers.FromDockerfile{
		Context:    repoRoot(t),
		Dockerfile: dockerfileRelPath,
		Repo:       "murmur-itest",
		Tag:        fmt.Sprintf("%d", time.Now().UnixNano()),
		KeepImage:  keep,
		BuildArgs: map[string]*string{
			"TARGETARCH": strPtr(runtime.GOARCH),
		},
	}
}

func strPtr(s string) *string { return &s }

// createInt64Table creates a sum-shaped DDB table and registers
// cleanup. Thin wrapper over mddb.CreateInt64Table for symmetry with
// the other helpers.
func createInt64Table(ctx context.Context, t *testing.T, client *awsddb.Client, name string) {
	t.Helper()
	if err := mddb.CreateInt64Table(ctx, client, name); err != nil {
		t.Fatalf("create table %s: %v", name, err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &name})
	})
}
