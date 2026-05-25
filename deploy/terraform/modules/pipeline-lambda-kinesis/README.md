# `pipeline-lambda-kinesis` Terraform module

Deploys a Murmur Lambda-Kinesis pipeline: a Kinesis data stream feeding a
Lambda function (event-source mapping) that folds events into a caller-supplied
DynamoDB state table.

Pairs with [`pipeline-counter`](../pipeline-counter) for multi-source
deployments where Kafka (via ECS worker) and Kinesis (via Lambda) write into
the same state table. See [`examples/recently-interacted-topk/terraform/`](../../../../examples/recently-interacted-topk/terraform/)
for the composition pattern.

## What the module creates

- **Kinesis data stream** — provisioned or on-demand, KMS-encrypted with the
  AWS-managed key. Skip stream creation by passing `kinesis_stream_arn` to
  share ingest with non-Murmur consumers.
- **Lambda function** — `provided.al2` (arm64 by default), driven from a
  caller-supplied zip containing a `bootstrap` binary. The example
  [`cmd/lambda/main.go`](../../../../examples/recently-interacted-topk/cmd/lambda/main.go)
  documents the build invocation.
- **Event-source mapping** — Kinesis → Lambda, with `BatchItemFailures` enabled
  so the Murmur handler's per-record failure reports surface to Lambda
  natively. Includes `bisect_batch_on_function_error` by default for
  poison-pill containment.
- **IAM** — one Lambda execution role with: AWSLambdaBasicExecutionRole,
  Kinesis consumer perms on the stream, DDB RW on the state table, optional
  DDB RW on the dedup table, optional SQS/SNS Publish for the on-failure
  destination.
- **CloudWatch log group** — `/aws/lambda/<name>` with caller-controlled
  retention.

## What the module does NOT cover

- VPC config — the Lambda runs outside any VPC by default. Add
  `vpc_config { … }` outside the module if it needs to reach ElastiCache or
  RDS. (Murmur's recommended deploy keeps Lambda VPC-less and reaches DDB via
  the public endpoint.)
- The dedup table itself — that's owned by the sibling `pipeline-counter`
  module. Pass `dedup_table_arn` + `dedup_table_name` to grant the Lambda
  access; the module wires the env var.
- Application-level dead-letter queues for poison pills inside the handler.
  The module *can* attach a Lambda on-failure SQS/SNS destination via
  `on_failure_destination_arn`, which catches records that exhausted Lambda's
  retry budget. In-handler DLQs (e.g. publishing decode failures to a separate
  topic) are wired through the function code, not Terraform.

## End-to-end usage

```hcl
module "recently_interacted" {
  source = "github.com/gallowaysoftware/murmur//deploy/terraform/modules/pipeline-counter"

  name = "recently_interacted"
  # ...ECS / VPC config...

  dedup_enabled = true
}

module "recently_interacted_kinesis" {
  source = "github.com/gallowaysoftware/murmur//deploy/terraform/modules/pipeline-lambda-kinesis"

  name = "recently_interacted_kinesis"

  lambda_zip_path = "${path.module}/lambda.zip"
  lambda_memory   = 512

  kinesis_shard_count     = 4
  batch_size              = 100
  parallelization_factor  = 2
  starting_position       = "LATEST"

  state_table_arn   = module.recently_interacted.ddb_table_arn
  state_table_name  = module.recently_interacted.ddb_table_name
  dedup_table_arn   = module.recently_interacted.dedup_table_arn
  dedup_table_name  = module.recently_interacted.dedup_table_name

  tags = {
    Service = "recently_interacted"
    Env     = "prod"
  }
}
```

## Building the Lambda zip

For the bundled example:

```sh
cd examples/recently-interacted-topk/cmd/lambda
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc \
  -o bootstrap -ldflags="-s -w" .
zip lambda.zip bootstrap
```

Set `lambda_zip_path = "./examples/recently-interacted-topk/cmd/lambda/lambda.zip"` and
re-`terraform apply` to roll the function. The module hashes the zip via
`filebase64sha256`, so re-builds without a content change won't trigger an
update.

For non-trivial deploys, push the zip to S3 and use `s3_bucket` /
`s3_key` instead — left as an extension point in the next iteration.

## Inputs

See [`variables.tf`](./variables.tf) for the full set. Required inputs:

| Variable          | Description                                                       |
| ----------------- | ----------------------------------------------------------------- |
| `name`            | Lambda function name + Kinesis stream name + IAM role name prefix. |
| `lambda_zip_path` | Local path to the deployment zip.                                 |
| `state_table_arn` | ARN of the DDB state table the Lambda writes into.                |
| `state_table_name`| Name of the same table (populated into the Lambda env).           |

Notable optionals: `kinesis_stream_arn`, `kinesis_shard_count`,
`dedup_table_arn` + `dedup_table_name`, `batch_size`,
`parallelization_factor`, `starting_position` /
`starting_position_timestamp`, `maximum_retry_attempts`,
`on_failure_destination_arn`, `reserved_concurrency`.

## Outputs

| Output                       | Description                                              |
| ---------------------------- | -------------------------------------------------------- |
| `lambda_function_name`/`arn` | The function.                                            |
| `lambda_role_arn`            | The role — extend with VPC / KMS policies as needed.     |
| `kinesis_stream_name`/`arn`  | The stream (name is null when caller supplied one).      |
| `event_source_mapping_uuid`  | UUID for runtime `update-event-source-mapping` tweaks.   |
| `log_group_name`             | For metric filters / subscription filters / alarms.      |

## File layout

```
deploy/terraform/modules/pipeline-lambda-kinesis/
  main.tf        # Kinesis stream + IAM + Lambda + event-source mapping
  variables.tf   # all inputs
  outputs.tf     # all outputs
  README.md      # this file
```
