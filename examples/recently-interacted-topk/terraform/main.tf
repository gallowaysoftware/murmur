terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
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
  dedup_table_arn  = module.ecs.dedup_table_arn
  dedup_table_name = module.ecs.dedup_table_name

  tags = var.tags
}
