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

# ----------------------------------------------------------------------------
# DynamoDB state table
# ----------------------------------------------------------------------------

resource "aws_dynamodb_table" "state" {
  name         = var.name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "N"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = var.tags
}

# ----------------------------------------------------------------------------
# IAM
#
# One execution role (image pull + log writes) shared by all task definitions.
# Three task roles — worker, query, bootstrap — so each can be granted
# different downstream policies by the consumer of the module. The worker
# and bootstrap roles get DDB read+write on the state table; the query role
# gets DDB read-only. All three role ARNs are exposed as outputs.
# ----------------------------------------------------------------------------

data "aws_iam_policy_document" "ecs_tasks_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "task_execution" {
  name               = "${var.name}-task-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "task_execution_managed" {
  role       = aws_iam_role.task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "worker_task" {
  name               = "${var.name}-worker-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
  tags               = var.tags
}

resource "aws_iam_role" "query_task" {
  name               = "${var.name}-query-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
  tags               = var.tags
}

resource "aws_iam_role" "bootstrap_task" {
  name               = "${var.name}-bootstrap-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "ddb_rw" {
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:BatchGetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
      "dynamodb:Query",
      "dynamodb:BatchWriteItem",
    ]
    resources = [aws_dynamodb_table.state.arn]
  }
}

data "aws_iam_policy_document" "ddb_ro" {
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:BatchGetItem",
      "dynamodb:Query",
    ]
    resources = [aws_dynamodb_table.state.arn]
  }
}

resource "aws_iam_role_policy" "worker_ddb" {
  name   = "${var.name}-worker-ddb"
  role   = aws_iam_role.worker_task.id
  policy = data.aws_iam_policy_document.ddb_rw.json
}

resource "aws_iam_role_policy" "query_ddb" {
  name   = "${var.name}-query-ddb"
  role   = aws_iam_role.query_task.id
  policy = data.aws_iam_policy_document.ddb_ro.json
}

resource "aws_iam_role_policy" "bootstrap_ddb" {
  name   = "${var.name}-bootstrap-ddb"
  role   = aws_iam_role.bootstrap_task.id
  policy = data.aws_iam_policy_document.ddb_rw.json
}

# ----------------------------------------------------------------------------
# CloudWatch log groups
# ----------------------------------------------------------------------------

locals {
  log_group_prefix = coalesce(var.log_group_name, "/ecs/${var.name}")
}

resource "aws_cloudwatch_log_group" "worker" {
  name              = "${local.log_group_prefix}-worker"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_cloudwatch_log_group" "query" {
  name              = "${local.log_group_prefix}-query"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_cloudwatch_log_group" "bootstrap" {
  name              = "${local.log_group_prefix}-bootstrap"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# ----------------------------------------------------------------------------
# Shared task environment
# ----------------------------------------------------------------------------

locals {
  base_env = merge(
    {
      DDB_TABLE  = aws_dynamodb_table.state.name
      AWS_REGION = data.aws_region.current.region
    },
    var.valkey_uri == null ? {} : { VALKEY_ADDRESS = var.valkey_uri },
  )

  worker_env_merged    = merge(local.base_env, var.worker_env)
  query_env_merged     = merge(local.base_env, { GRPC_ADDR = ":${var.grpc_port}" }, var.query_env)
  bootstrap_env_merged = merge(local.base_env, var.bootstrap_env)
}
