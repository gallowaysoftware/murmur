terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

data "aws_region" "current" {}

locals {
  create_stream = var.kinesis_stream_arn == null
  stream_arn    = local.create_stream ? aws_kinesis_stream.source[0].arn : var.kinesis_stream_arn

  dedup_enabled = var.dedup_table_arn != null

  base_env = merge(
    {
      DDB_TABLE  = var.state_table_name
      AWS_REGION = data.aws_region.current.region
    },
    local.dedup_enabled ? { DDB_DEDUP_TABLE = var.dedup_table_name } : {},
    var.lambda_env,
  )
}

# ----------------------------------------------------------------------------
# Kinesis stream (optional — skipped when caller supplies kinesis_stream_arn)
# ----------------------------------------------------------------------------

resource "aws_kinesis_stream" "source" {
  count = local.create_stream ? 1 : 0

  name             = var.name
  shard_count      = var.kinesis_shard_count
  retention_period = var.kinesis_retention_hours

  stream_mode_details {
    stream_mode = var.kinesis_shard_count == null ? "ON_DEMAND" : "PROVISIONED"
  }

  encryption_type = "KMS"
  kms_key_id      = "alias/aws/kinesis"

  tags = var.tags
}

# ----------------------------------------------------------------------------
# IAM
# ----------------------------------------------------------------------------

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.name}-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Kinesis consumer permissions for the event-source mapping.
data "aws_iam_policy_document" "lambda_kinesis" {
  statement {
    effect = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
      "kinesis:ListStreams",
      "kinesis:SubscribeToShard",
    ]
    resources = [local.stream_arn]
  }
}

resource "aws_iam_role_policy" "lambda_kinesis" {
  name   = "${var.name}-kinesis"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_kinesis.json
}

# DDB state-table read+write.
data "aws_iam_policy_document" "lambda_state" {
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:BatchGetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:Query",
    ]
    resources = [var.state_table_arn]
  }
}

resource "aws_iam_role_policy" "lambda_state" {
  name   = "${var.name}-state"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_state.json
}

# DDB dedup-table read+write (optional).
data "aws_iam_policy_document" "lambda_dedup" {
  count = local.dedup_enabled ? 1 : 0
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:DeleteItem",
    ]
    resources = [var.dedup_table_arn]
  }
}

resource "aws_iam_role_policy" "lambda_dedup" {
  count  = local.dedup_enabled ? 1 : 0
  name   = "${var.name}-dedup"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_dedup[0].json
}

# On-failure destination (SQS / SNS).
data "aws_iam_policy_document" "lambda_on_failure" {
  count = var.on_failure_destination_arn == null ? 0 : 1
  statement {
    effect    = "Allow"
    actions   = ["sqs:SendMessage", "sns:Publish"]
    resources = [var.on_failure_destination_arn]
  }
}

resource "aws_iam_role_policy" "lambda_on_failure" {
  count  = var.on_failure_destination_arn == null ? 0 : 1
  name   = "${var.name}-on-failure"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_on_failure[0].json
}

# ----------------------------------------------------------------------------
# CloudWatch log group (Lambda would create one implicitly, but we provision
# it explicitly to control retention).
# ----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.name}"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# ----------------------------------------------------------------------------
# Lambda function
# ----------------------------------------------------------------------------

resource "aws_lambda_function" "handler" {
  function_name = var.name
  role          = aws_iam_role.lambda.arn

  filename         = var.lambda_zip_path
  source_code_hash = filebase64sha256(var.lambda_zip_path)

  runtime       = "provided.al2"
  handler       = "bootstrap"
  architectures = [var.lambda_architecture]

  memory_size                    = var.lambda_memory
  timeout                        = var.lambda_timeout
  reserved_concurrent_executions = var.reserved_concurrency

  environment {
    variables = local.base_env
  }

  tags = var.tags

  depends_on = [aws_cloudwatch_log_group.lambda]
}

# ----------------------------------------------------------------------------
# Event source mapping
# ----------------------------------------------------------------------------

resource "aws_lambda_event_source_mapping" "kinesis" {
  function_name     = aws_lambda_function.handler.arn
  event_source_arn  = local.stream_arn
  starting_position = var.starting_position
  starting_position_timestamp = (
    var.starting_position == "AT_TIMESTAMP" ? var.starting_position_timestamp : null
  )

  batch_size                         = var.batch_size
  maximum_batching_window_in_seconds = var.batch_window_seconds
  parallelization_factor             = var.parallelization_factor

  maximum_retry_attempts         = var.maximum_retry_attempts
  maximum_record_age_in_seconds  = var.maximum_record_age_seconds
  bisect_batch_on_function_error = var.bisect_batch_on_function_error

  # Murmur's Lambda handler returns BatchItemFailures so failed records are
  # isolated rather than redelivering the entire batch.
  function_response_types = ["ReportBatchItemFailures"]

  dynamic "destination_config" {
    for_each = var.on_failure_destination_arn == null ? [] : [1]
    content {
      on_failure {
        destination_arn = var.on_failure_destination_arn
      }
    }
  }
}
