terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Fail the plan if Container Insights is off on the caller-owned cluster. The
# worker/query liveness alarms read ECS/ContainerInsights, which publishes
# nothing when it is disabled — and since those alarms treat missing data as
# breaching (so a dead service alarms instead of looking healthy), a disabled
# cluster puts them in permanent ALARM. That is worse than having no alarm at
# all: it teaches the operator to ignore the two that watch the Kafka half.
data "aws_ecs_cluster" "target" {
  cluster_name = var.ecs_cluster_name

  lifecycle {
    postcondition {
      condition = !var.require_container_insights || anytrue([
        for s in self.setting : s.value == "enabled" if s.name == "containerInsights"
      ])
      error_message = "Container Insights is disabled on ECS cluster '${var.ecs_cluster_name}'. The worker/query liveness alarms would sit in ALARM forever. Enable it with: aws ecs update-cluster-settings --cluster ${var.ecs_cluster_name} --settings name=containerInsights,value=enabled  (then force a redeploy). Set require_container_insights = false to bypass."
    }
  }
}

# ----------------------------------------------------------------------------
# ECS side: Kafka worker + query + bootstrap + state DDB + dedup DDB.
# ----------------------------------------------------------------------------

module "ecs" {
  source = "../../../deploy/terraform/modules/pipeline-counter"

  name               = var.name
  vpc_id             = var.vpc_id
  private_subnet_ids = var.private_subnet_ids
  ecs_cluster_arn    = var.ecs_cluster_arn
  image              = var.image

  worker_env = {
    KAFKA_BROKERS  = var.kafka_brokers
    KAFKA_TOPIC    = var.kafka_topic
    CONSUMER_GROUP = var.consumer_group
  }

  worker_desired_count = var.worker_desired_count
  query_desired_count  = var.query_desired_count
  assign_public_ip     = var.assign_public_ip
  query_alb_enabled    = var.query_alb_enabled

  extra_worker_security_group_ids = var.extra_worker_security_group_ids

  # The Lambda side shares the same state DDB row, and Kinesis BatchItemFailures
  # may redeliver records adjacent to a failure — dedup is required.
  dedup_enabled = true

  tags = var.tags
}

# ----------------------------------------------------------------------------
# Lambda side: Kinesis stream + Lambda + event-source mapping. Writes into the
# state + dedup tables provisioned by the ECS module above.
# ----------------------------------------------------------------------------

module "lambda" {
  source = "../../../deploy/terraform/modules/pipeline-lambda-kinesis"

  name = "${var.name}_kinesis"

  lambda_zip_path = var.lambda_zip_path

  kinesis_shard_count    = var.kinesis_shard_count
  batch_size             = var.lambda_batch_size
  parallelization_factor = var.lambda_parallelization_factor
  starting_position      = "LATEST"

  state_table_arn  = module.ecs.ddb_table_arn
  state_table_name = module.ecs.ddb_table_name

  # `dedup_enabled` is a literal, deliberately. The module gates `count` on it,
  # and a count derived from module.ecs.dedup_table_arn (unknown until apply)
  # fails the whole plan. The ARN/name below only scope the IAM policy, where
  # apply-time values are fine.
  dedup_enabled    = true
  dedup_table_arn  = module.ecs.dedup_table_arn
  dedup_table_name = module.ecs.dedup_table_name

  tags = var.tags
}
