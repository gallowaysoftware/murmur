# `pipeline-counter` Terraform module

Deploys a Murmur counter pipeline (Kafka source, Sum monoid, DynamoDB state, gRPC query)
to AWS as two ECS Fargate services backed by one DDB table and one ALB.

This module covers the steady-state production shape:

- **DynamoDB state table** with TTL on the `ttl` attribute (windowed bucket eviction)
  and Point-in-Time-Recovery enabled.
- **Worker ECS Fargate service** running the streaming runtime against your existing
  Kafka brokers (the module does not provision MSK; pass `worker_env.KAFKA_BROKERS` and
  related vars as inputs).
- **Query ECS Fargate service** behind an Application Load Balancer with gRPC target
  group (`protocol_version = "GRPC"`, health check via `/grpc.health.v1.Health/Check`).
- **IAM**: shared task-execution role + per-service task role with DDB R/W on the
  state table only.
- **CloudWatch log groups** for each service.

What it does NOT cover (intentionally — these are deployment-environment-specific):

- VPC, subnets, ECS cluster — pass IDs/ARNs as inputs.
- MSK / self-managed Kafka.
- ElastiCache (Valkey) — pass `worker_env.VALKEY_ADDRESS` if you have one.
- Kinesis Firehose to S3 archive (used by the kappa-replay path).
- Auto-scaling on consumer-group lag — the module ships fixed `desired_count`; add an
  Application Auto Scaling target outside the module if you need lag-based scaling.

## Usage

```hcl
module "page_views" {
  source = "github.com/gallowaysoftware/murmur//deploy/terraform/modules/pipeline-counter"

  name               = "page_views"
  vpc_id             = aws_vpc.main.id
  private_subnet_ids = aws_subnet.private[*].id
  public_subnet_ids  = aws_subnet.public[*].id
  ecs_cluster_arn    = aws_ecs_cluster.main.arn

  image          = "ghcr.io/gallowaysoftware/murmur-page-views:v1"
  worker_command = ["/murmur-worker"]
  query_command  = ["/murmur-query"]

  worker_env = {
    KAFKA_BROKERS  = "b-1.msk.example.com:9094,b-2.msk.example.com:9094"
    KAFKA_TOPIC    = "page_views"
    CONSUMER_GROUP = "page_views_worker_prod"
    VALKEY_ADDRESS = "page-views.cache.amazonaws.com:6379"
  }

  worker_desired_count = 2
  query_desired_count  = 3
}

# gRPC clients dial:
output "page_views_query_endpoint" {
  value = "${module.page_views.query_alb_dns_name}:80"
}
```

## Image expectations

The same container image is used for both services. The image must:

1. Contain a `/murmur-worker` binary built from `examples/.../cmd/worker` (or your
   own pipeline package's worker `main`).
2. Contain a `/murmur-query` binary built from `examples/.../cmd/query`.
3. Be entirely env-var configured — no command-line flags needed for production.

A stock multi-binary Dockerfile is in `examples/page-view-counters/Dockerfile`
(forthcoming).
