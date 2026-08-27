# Deploy: `recently-interacted-topk` on real AWS

End-to-end Terraform composition that stands up the multi-source TopK example
on real AWS. Composes:

- [`pipeline-counter`](../../../deploy/terraform/modules/pipeline-counter) —
  ECS Fargate worker (Kafka), query service, bootstrap runner, state DDB,
  dedup DDB.
- [`pipeline-lambda-kinesis`](../../../deploy/terraform/modules/pipeline-lambda-kinesis) —
  Kinesis data stream, Lambda function, event-source mapping. Lambda writes
  into the *same* state + dedup tables the ECS worker uses.
- [`alarms.tf`](./alarms.tf) — CloudWatch alarms for Lambda errors,
  IteratorAge, throttling, DDB write throttling. SNS-routed when
  `alarm_sns_topic_arn` is set.

This is the canonical target for the v1 real-AWS soak — it exercises every
package still flagged `experimental` in [`STABILITY.md`](../../../STABILITY.md)
that's gated on operational evidence (Lambda runtimes, processor core, and
Kafka source's per-partition concurrency).

## Runbook

### 0. Prerequisites

You need to have provisioned (out-of-band):

- An AWS account with the AWS CLI configured.
- A VPC with at least two private subnets in different AZs.
- An ECS cluster in that VPC.
- An MSK cluster (or self-managed Kafka) reachable from the worker SG. Create
  the `interactions` topic before applying.
- An ECR repository (or any registry the ECS task-execution role can pull
  from).
- An SNS topic for alarm notifications. Subscribe an email / PagerDuty /
  Slack relay before the soak begins.

Tools: `terraform >= 1.5`, `aws-cli >= 2`, `go >= 1.22`, `docker`, `zip`.

### 1. Build and push the container image

```sh
cd examples/recently-interacted-topk
docker build -t recently-interacted:v1 .

# Tag + push to ECR (substitute account + region):
ACCOUNT=123456789012
REGION=us-west-2
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"
docker tag recently-interacted:v1 \
  "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/murmur-recently-interacted:v1"
docker push "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/murmur-recently-interacted:v1"
```

### 2. Build the Lambda deployment zip

```sh
cd examples/recently-interacted-topk/cmd/lambda
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build \
  -tags lambda.norpc -o bootstrap -ldflags="-s -w" .
zip lambda.zip bootstrap
rm bootstrap
```

The zip is referenced from Terraform via `lambda_zip_path`. Re-running these
two commands and re-applying refreshes the function (the module hashes the zip
content, so apply is a no-op when nothing changed).

### 3. Configure Terraform variables

```sh
cd examples/recently-interacted-topk/terraform
cp terraform.tfvars.example terraform.tfvars
$EDITOR terraform.tfvars
```

Fill in `vpc_id`, `private_subnet_ids`, `ecs_cluster_arn`, `image`,
`kafka_brokers`, `extra_worker_security_group_ids`, and
`alarm_sns_topic_arn`. Other values have working defaults.

### 4. Plan and apply

```sh
terraform init
terraform plan -out plan.tfplan
terraform apply plan.tfplan
```

Apply takes ~3-5 minutes. The Lambda function is live as soon as apply
completes; the ECS worker and query service take another ~30s to pull the
image and pass health checks.

### 5. Smoke test

Produce one event to each ingest path:

```sh
# Kafka — using kafkacat / kcat (whichever is on $PATH)
echo '{"entity_id":"product-42","user_id":"alice","source":"kafka"}' \
  | kcat -b "$KAFKA_BROKERS" -t interactions -P

# Kinesis
STREAM=$(terraform output -raw kinesis_stream_name)
aws kinesis put-record \
  --stream-name "$STREAM" \
  --partition-key alice \
  --data "$(echo -n '{"entity_id":"product-42","user_id":"alice","source":"kinesis"}' | base64)"
```

Both events should land in the same DDB row. Query the merged Top-N from
inside the VPC (jump host, bastion, EC2 instance in the same VPC):

```sh
ENDPOINT=$(terraform output -raw query_service_endpoint)

grpcurl -plaintext -d '{"entity":"global"}' \
  "$ENDPOINT" murmur.v1.QueryService/Get
# → Top-N with product-42 at count = 2 (one from each source).

grpcurl -plaintext -d '{"entity":"global","duration_seconds":604800}' \
  "$ENDPOINT" murmur.v1.QueryService/GetWindow
# → 7-day windowed Top-N (merged across daily Misra-Gries summaries).
```

### 6. Observability during the soak

CloudWatch namespaces to watch:

- `AWS/Lambda` — `<name>_kinesis` function. Watch `Errors`, `IteratorAge`,
  `Duration`, `ConcurrentExecutions`.
- `AWS/ECS` — `<name>` worker / query services. Watch `CPUUtilization`,
  `MemoryUtilization`, task count.
- `AWS/DynamoDB` — `<name>` and `<name>_dedup` tables. Watch
  `ConsumedWriteCapacityUnits`, `WriteThrottleEvents`, `SuccessfulRequestLatency`.
- `AWS/Kinesis` — `<name>_kinesis` stream. Watch `IncomingRecords`,
  `WriteProvisionedThroughputExceeded` (provisioned mode only).

The alarms in `alarms.tf` cover the load-bearing signals; everything else is
context.

### 7. Teardown

```sh
terraform destroy
```

`destroy` is safe — no resource has lifecycle preventions. The state DDB
table is deleted; PITR retains a recovery window if you change your mind
within the recovery TTL (35 days by default for new tables).

If you fronted MSK with this composition, destroy the MSK cluster separately;
it isn't owned by this Terraform.

## Inputs

See [`variables.tf`](./variables.tf). Required: `aws_region`, `vpc_id`,
`private_subnet_ids`, `ecs_cluster_arn`, `image`, `lambda_zip_path`,
`kafka_brokers`. Everything else has a default.

## Outputs

| Output                          | What it is                                                  |
| ------------------------------- | ----------------------------------------------------------- |
| `query_service_endpoint`        | gRPC host:port for the query ALB.                           |
| `state_table_name`              | DDB table both ingest paths write into.                     |
| `dedup_table_name`              | DDB table for at-least-once dedup.                          |
| `kinesis_stream_name`           | Kinesis stream the Lambda consumes.                         |
| `lambda_function_name`          | Lambda — for log tailing / ad-hoc invoke.                   |
| `worker_iam_role_arn`           | ECS worker task role — attach MSK SASL policies here.       |
| `lambda_role_arn`               | Lambda execution role.                                      |
| `bootstrap_task_definition_arn` | For S3-archive backfill via `aws ecs run-task`.             |

## Cost

Two configurations, priced bottom-up. The **standing** column bills 24/7
whether or not a single event flows — it is the half that keeps accruing in
exactly the failure mode where the pipeline is dead and nobody noticed.

### Production-shape defaults (~10 events/sec, `terraform.tfvars.example`)

| Item | Standing | Usage | Notes |
|---|---|---|---|
| ECS Fargate — 2× worker + 2× query | ~$60 | — | Four tasks at default sizes |
| Query ALB | ~$17–22 | + LCU | **Hourly charge just to exist**; unconditional in `pipeline-counter` |
| NAT gateway (if `assign_public_ip = false`) | ~$33 | + $0.045/GB | One per AZ; the classic surprise |
| Kinesis — 2 provisioned shards | ~$22 | + PUT payload | |
| Lambda — arm64 512MB | — | ~$2 | |
| DynamoDB — PAY_PER_REQUEST | — | **~$36–75** | See below |
| CloudWatch logs + 11 alarms | — | ~$10 | |
| **Total** | **~$132–137** | **~$48–87** | **≈ $180–225/mo** |

The earlier "~$100/mo" estimate in this file was wrong in the direction that
matters: it omitted the ALB and NAT hourly lines entirely, and understated
DynamoDB by 7–15×.

**Why DynamoDB is not ~$5.** The pipeline aggregates TopK — a non-coalescable
CAS monoid — onto a **single `"global"` row**. Every event costs a
strongly-consistent read plus two conditional writes, multiplied by the CAS
attempt count under contention. And a `PAY_PER_REQUEST` table under CAS
contention does not throttle; it just bills. `WriteThrottleEvents` therefore
cannot detect a runaway, which is why `ddb-write-runaway` (on
`ConsumedWriteCapacityUnits`) exists in `alarms.tf`.

### Soak configuration (`soak.tfvars`)

The soak's job is to prove the code paths work and stay working, not to
measure throughput. One task exercises the same code as two.

| Change | Saves |
|---|---|
| `worker_desired_count = 1`, `query_desired_count = 1` | ~$30 |
| `kinesis_shard_count = 1` | ~$11 |
| `assign_public_ip = true` — public subnet, no NAT | ~$18 |
| ~1 event/sec instead of 10 | ~$32–68 |
| **Total** | **≈ $60–80/mo** |

Tasks stay unreachable inbound with a public IP: the worker SG has no ingress
and the query SG admits only the ALB.

**Set an AWS Budget with an email action.** For an unattended personal-account
soak that is the real backstop — every alarm here watches correctness, not
spend, and the DynamoDB line is the one that can run away quietly.
