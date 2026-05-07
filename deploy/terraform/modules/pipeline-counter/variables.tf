variable "name" {
  description = "Pipeline name. Used as the DDB table name and as a prefix for ECS resources."
  type        = string
}

variable "vpc_id" {
  description = "VPC the ECS services run in."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnets for the ECS Fargate tasks."
  type        = list(string)
}

variable "public_subnet_ids" {
  description = "Public subnets for the query ALB. Use private subnets + private ALB for internal-only deployments."
  type        = list(string)
}

variable "ecs_cluster_arn" {
  description = "Existing ECS cluster ARN."
  type        = string
}

variable "image" {
  description = "Container image (worker and query bind to the same image; the entrypoint differs)."
  type        = string
}

variable "worker_command" {
  description = "Container command for the streaming worker."
  type        = list(string)
  default     = ["/murmur-worker"]
}

variable "query_command" {
  description = "Container command for the gRPC query server."
  type        = list(string)
  default     = ["/murmur-query"]
}

variable "worker_env" {
  description = "Additional env vars passed to the worker (KAFKA_BROKERS, KAFKA_TOPIC, CONSUMER_GROUP, VALKEY_ADDRESS, etc.)."
  type        = map(string)
  default     = {}
}

variable "query_env" {
  description = "Additional env vars passed to the query server."
  type        = map(string)
  default     = {}
}

variable "worker_cpu" {
  description = "CPU units for the worker task (256 = 0.25 vCPU)."
  type        = number
  default     = 512
}

variable "worker_memory" {
  description = "Memory for the worker task in MiB."
  type        = number
  default     = 1024
}

variable "query_cpu" {
  description = "CPU units for the query task."
  type        = number
  default     = 256
}

variable "query_memory" {
  description = "Memory for the query task in MiB."
  type        = number
  default     = 512
}

variable "worker_desired_count" {
  description = "Desired ECS task count for the worker. Scales with Kafka partition count typically."
  type        = number
  default     = 1
}

variable "query_desired_count" {
  description = "Desired ECS task count for the query service."
  type        = number
  default     = 2
}

variable "grpc_port" {
  description = "TCP port the query server listens on inside the container."
  type        = number
  default     = 50051
}

variable "log_retention_days" {
  description = "CloudWatch log retention for both services."
  type        = number
  default     = 14
}

variable "tags" {
  description = "Tags applied to all resources."
  type        = map(string)
  default     = {}
}
