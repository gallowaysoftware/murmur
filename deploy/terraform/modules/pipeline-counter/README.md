# `pipeline-counter` Terraform module

Deploys a Murmur counter pipeline (Kafka source -> Sum monoid -> DynamoDB state ->
gRPC query) to AWS as two ECS Fargate services plus a one-shot bootstrap task,
all sharing a single DDB state table and an internal ALB.

## What the module creates

- **DynamoDB state table** (`pk` string, `sk` numeric, TTL on `ttl`, PITR enabled).
- **Streaming worker** ECS Fargate service — long-running Kafka consumer that
  folds events into the state table. Scale `worker_desired_count` with Kafka
  partition count.
- **Query service** ECS Fargate service — gRPC server behind an **internal**
  Application Load Balancer (gRPC-aware health checks via
  `/grpc.health.v1.Health/Check`).
- **Bootstrap runner** ECS Fargate task definition (no service) — launched on
  demand via `aws ecs run-task` for kappa-style backfill from S3 / archive.
- **IAM** — one shared execution role, plus three separate task roles
  (worker / query / bootstrap) so consumers can attach different downstream
  policies. The worker and bootstrap roles get DDB read+write on the state
  table; the query role gets DDB read-only.
- **CloudWatch log groups** — one per workload, retention controlled by
  `log_retention_days`.

## What the module does NOT cover

These are intentionally left to the consumer because they are deployment-shape
specific:

- VPC, subnets, ECS cluster (pass IDs/ARNs as inputs).
- MSK / self-managed Kafka (pass `worker_env.KAFKA_BROKERS`).
- ElastiCache (Valkey) — pass `valkey_uri` to inject `VALKEY_ADDRESS` into all
  three tasks, or set it directly via `worker_env`.
- Kinesis Firehose -> S3 archive (the kappa-replay source).
- Auto-scaling on consumer-group lag — `desired_count` is fixed; attach an
  Application Auto Scaling target outside the module if needed.
- Public exposure of the query service — the ALB is internal-only by design;
  reach it via VPC peering, Transit Gateway, or PrivateLink.

## End-to-end usage

```hcl
module "page_views" {
  source = "github.com/gallowaysoftware/murmur//deploy/terraform/modules/pipeline-counter"

  name               = "page_views"
  vpc_id             = aws_vpc.main.id
  private_subnet_ids = aws_subnet.private[*].id
  ecs_cluster_arn    = aws_ecs_cluster.main.arn

  image = "ghcr.io/gallowaysoftware/murmur-page-views:v1"
  # worker_command / query_command / bootstrap_command default to
  # /murmur-worker, /murmur-query, /murmur-bootstrap respectively.

  worker_env = {
    KAFKA_BROKERS  = "b-1.msk.example.com:9094,b-2.msk.example.com:9094"
    KAFKA_TOPIC    = "page_views"
    CONSUMER_GROUP = "page_views_worker_prod"
  }

  bootstrap_env = {
    REPLAY_S3_BUCKET = "page-views-archive-prod"
    REPLAY_FROM      = "2026-01-01T00:00:00Z"
  }

  valkey_uri = "page-views.cache.amazonaws.com:6379"

  worker_desired_count = 4
  query_desired_count  = 3

  # Grant the worker SG ingress to MSK / Valkey from outside the module:
  # extra_worker_security_group_ids = [aws_security_group.msk_clients.id]

  tags = {
    Service = "page_views"
    Env     = "prod"
  }
}

# gRPC clients dial this:
output "page_views_query_endpoint" {
  value = module.page_views.query_service_endpoint
}
```

### Launching the bootstrap runner

The bootstrap task definition is created but never run automatically. After
seeding `bootstrap_env` and deploying, invoke it on demand:

```bash
TASK_DEF=$(terraform output -raw page_views_bootstrap_task_definition_arn)
BOOT_SG=$(terraform output -raw page_views_bootstrap_security_group_id)

aws ecs run-task \
  --cluster "$CLUSTER_ARN" \
  --task-definition "$TASK_DEF" \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$BOOT_SG],assignPublicIp=DISABLED}"
```

(The `page_views_` prefix above assumes you re-export module outputs at the
root — substitute your own naming.)

Or wire the task definition into a Step Function / EventBridge schedule for
recurring rematerialization.

## Image expectations

The same container image is used for all three workloads. The image must:

1. Contain a `/murmur-worker` binary (streaming runtime).
2. Contain a `/murmur-query` binary (gRPC server).
3. Contain a `/murmur-bootstrap` binary (replay / backfill runner).
4. Be entirely env-var configured — no command-line flags needed.

A reference multi-binary Dockerfile lives at
`examples/page-view-counters/Dockerfile`.

## Inputs

See [`variables.tf`](./variables.tf) for the full set. Required inputs:

| Variable             | Description                            |
| -------------------- | -------------------------------------- |
| `name`               | DDB table name + ECS resource prefix.  |
| `vpc_id`             | VPC for the services and ALB.          |
| `private_subnet_ids` | Private subnets for tasks and ALB.     |
| `ecs_cluster_arn`    | Existing ECS cluster to deploy onto.   |
| `image`              | Container image with all three binaries. |

Notable optionals: `valkey_uri`, `worker_env`, `query_env`, `bootstrap_env`,
`query_allowed_cidrs`, `extra_worker_security_group_ids`,
`extra_query_security_group_ids`, `log_group_name`, `log_retention_days`,
`*_cpu` / `*_memory`, `*_desired_count`, `grpc_port`, `tags`.

## Outputs

| Output                          | Description                                                  |
| ------------------------------- | ------------------------------------------------------------ |
| `ddb_table_name` / `ddb_table_arn` | Underlying state table.                                   |
| `query_service_dns`             | Internal ALB DNS name.                                       |
| `query_service_endpoint`        | `<dns>:<grpc_port>` convenience string.                      |
| `worker_service_name`           | ECS service name for the streaming worker.                   |
| `query_service_name`            | ECS service name for the query server.                       |
| `bootstrap_task_definition_arn` | Task definition ARN to launch with `aws ecs run-task`.       |
| `streaming_worker_iam_role_arn` | Worker task role — attach Kafka/Kinesis/S3 policies here.    |
| `query_service_iam_role_arn`    | Query task role (DDB read-only by default).                  |
| `bootstrap_iam_role_arn`        | Bootstrap task role — attach S3 archive read here.           |
| `task_execution_role_arn`       | Shared image-pull / log-write role.                          |
| `worker_security_group_id`      | Worker ENI SG — reference from MSK / Valkey SGs for ingress. |
| `query_alb_security_group_id`   | Internal ALB SG.                                             |
| `bootstrap_security_group_id`   | Bootstrap-task ENI SG.                                       |

## Atomic state-table swap (optional)

Enable `swap_enabled = true` to provision a [`pkg/swap`](../../../../pkg/swap)
control table and inject `SWAP_CONTROL_TABLE` + `SWAP_ALIAS` env vars into all
three tasks. The state table at `var.name` remains the active table for
day-to-day reads; subsequent backfill versions (`<name>_v2`, `<name>_v3`, …)
are created outside the module by the backfill workflow, and atomically
cut-over via `swap.Manager.SetActive(ctx, alias, newVersion)`.

```hcl
module "page_views" {
  source = "github.com/gallowaysoftware/murmur//deploy/terraform/modules/pipeline-counter"

  name = "page_views"
  # ...everything else as before...

  swap_enabled         = true
  swap_initial_version = 1   # seed the alias pointer at deploy time
}
```

What the module does in swap mode:

1. Creates `<name>_swap` (override via `swap_control_table_name`) — a tiny DDB
   table with one row per alias (`pk: <alias>, ver: <int>, at: <unix-ms>`).
2. If `swap_initial_version` is set, seeds the alias row to that version. After
   that, Terraform stops tracking the row — `SetActive` updates from the
   application are NOT treated as drift.
3. Adds DDB `GetItem` to the worker + query task roles on the control table,
   and `GetItem` + `UpdateItem` to the bootstrap task role (so the backfill
   runner can advance the pointer when its replay completes).
4. Adds `SWAP_CONTROL_TABLE` + `SWAP_ALIAS` to every task's environment. The
   binaries are expected to call `swap.New(client, SWAP_CONTROL_TABLE)` and
   `Manager.Resolve(ctx, SWAP_ALIAS)` at startup (and on a refresh cadence,
   for query servers that should pick up cutovers without a redeploy).

Cutover from v1 → v2:

```bash
# 1. Provision the new state table out-of-module (or via a second instance of
#    this module with a different `name`).
# 2. Run the backfill workload writing into <name>_v2 in parallel with the
#    live v1 worker.
# 3. When the backfill catches up, advance the pointer:

aws dynamodb update-item \
  --table-name "$(terraform output -raw swap_control_table_name)" \
  --key '{"pk":{"S":"page_views"}}' \
  --update-expression "SET ver = :v, at = :now" \
  --condition-expression "attribute_not_exists(ver) OR ver < :v" \
  --expression-attribute-values '{":v":{"N":"2"},":now":{"N":"1715731200000"}}'

# Or from Go inside a one-shot cutover binary:
#   m := swap.New(ddb, os.Getenv("SWAP_CONTROL_TABLE"))
#   _ = m.SetActive(ctx, os.Getenv("SWAP_ALIAS"), 2)
```

Query servers pick up the new pointer on their next refresh (or restart).
The old v1 table can be deleted after a grace period — typically once the
oldest live read against v1 has settled.

## File layout

```
deploy/terraform/modules/pipeline-counter/
  main.tf        # provider, DDB, IAM, log groups, locals
  ecs.tf         # streaming worker task + service
  query.tf       # query task + service + ALB + target group + listener
  bootstrap.tf   # bootstrap-runner task definition
  swap.tf        # optional pkg/swap control table + IAM + seed
  variables.tf   # all inputs
  outputs.tf     # all outputs
  README.md      # this file
```
