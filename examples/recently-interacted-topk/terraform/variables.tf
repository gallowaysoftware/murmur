variable "name" {
  description = "Logical pipeline name. Used as a prefix for every resource created by this composition."
  type        = string
  default     = "recently_interacted"
}

variable "ecs_cluster_name" {
  description = "ECS cluster NAME (not ARN) — CloudWatch alarm dimensions need the bare name. Must refer to the same cluster as ecs_cluster_arn."
  type        = string
}

variable "ddb_write_capacity_alarm_threshold" {
  description = "Alarm when the state table consumes more than this many WCUs in a 5-minute period. Default 9000 ~= 30 WCU/s sustained, comfortably above a ~1-10 event/sec soak but low enough to catch a CAS retry storm before it shows up on the bill."
  type        = number
  default     = 9000
}

variable "assign_public_ip" {
  description = "Give the Fargate tasks public IPs instead of requiring a NAT gateway. Set true for a low-cost soak in a default VPC (~$14.60/mo of public IPv4 vs ~$32.85/mo for one NAT). Tasks remain unreachable inbound."
  type        = bool
  default     = false
}

variable "aws_region" {
  description = "AWS region. Passed to the AWS provider; module resources inherit it."
  type        = string
}

# ----------------------------------------------------------------------------
# Networking (caller-owned)
# ----------------------------------------------------------------------------

variable "vpc_id" {
  description = "VPC ID. Houses the ECS services and the internal query ALB."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for ECS tasks and the internal query ALB."
  type        = list(string)
}

variable "ecs_cluster_arn" {
  description = "Existing ECS cluster ARN. The worker / query / bootstrap services attach here."
  type        = string
}

variable "extra_worker_security_group_ids" {
  description = "Extra SGs attached to the streaming worker ENI (typically the MSK clients SG)."
  type        = list(string)
  default     = []
}

# ----------------------------------------------------------------------------
# Container image (ECS) + Lambda zip
# ----------------------------------------------------------------------------

variable "image" {
  description = "Container image for the ECS worker / query / bootstrap. Build from examples/recently-interacted-topk/Dockerfile and push to ECR (or any registry the cluster's pull role can reach)."
  type        = string
}

variable "lambda_zip_path" {
  description = "Local path to the Lambda deployment zip. See examples/recently-interacted-topk/cmd/lambda/main.go's leading comment for the exact build invocation."
  type        = string
}

# ----------------------------------------------------------------------------
# Kafka source (caller-owned MSK or self-managed)
# ----------------------------------------------------------------------------

variable "kafka_brokers" {
  description = "Comma-separated Kafka broker addresses for the ECS worker. Passed to the worker as KAFKA_BROKERS."
  type        = string
}

variable "kafka_topic" {
  description = "Kafka topic the worker subscribes to."
  type        = string
  default     = "interactions"
}

variable "consumer_group" {
  description = "Kafka consumer group ID for the worker."
  type        = string
  default     = "recently_interacted_worker"
}

# ----------------------------------------------------------------------------
# Kinesis source (provisioned by the lambda-kinesis module)
# ----------------------------------------------------------------------------

variable "kinesis_shard_count" {
  description = "Kinesis stream shard count. Each shard handles ~1 MB/s in / ~2 MB/s out."
  type        = number
  default     = 2
}

variable "lambda_batch_size" {
  description = "Max records per Lambda invocation."
  type        = number
  default     = 100
}

variable "lambda_parallelization_factor" {
  description = "Concurrent invocations per shard."
  type        = number
  default     = 2
}

# ----------------------------------------------------------------------------
# Sizing
# ----------------------------------------------------------------------------

variable "worker_desired_count" {
  description = "ECS worker task count. Match to Kafka partition count for full parallelism."
  type        = number
  default     = 2
}

variable "query_desired_count" {
  description = "ECS query task count."
  type        = number
  default     = 2
}

# ----------------------------------------------------------------------------
# Observability
# ----------------------------------------------------------------------------

variable "alarm_sns_topic_arn" {
  description = "SNS topic ARN to receive alarm notifications. null disables the alarms entirely (they still exist as metric filters but route nowhere — set this for the soak)."
  type        = string
  default     = null
}

variable "kinesis_iterator_age_threshold_ms" {
  description = "Threshold for the Lambda IteratorAge alarm in milliseconds. Default 60s — IteratorAge above this means Lambda isn't keeping up with the stream."
  type        = number
  default     = 60000
}

# ----------------------------------------------------------------------------
# Tagging
# ----------------------------------------------------------------------------

variable "tags" {
  description = "Tags applied to all resources."
  type        = map(string)
  default     = {}
}
