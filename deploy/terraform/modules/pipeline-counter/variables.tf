variable "name" {
  description = "Pipeline name. Used as the DDB table name and as a prefix for ECS resources."
  type        = string
}

# ----------------------------------------------------------------------------
# Networking
# ----------------------------------------------------------------------------

variable "vpc_id" {
  description = "VPC the ECS services run in."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnets used for the ECS Fargate tasks and the internal query ALB."
  type        = list(string)
}

variable "query_allowed_cidrs" {
  description = "CIDR blocks permitted to reach the internal query ALB on the gRPC port. Default allows only the VPC's private space; widen for cross-VPC consumers reaching in via peering / TGW."
  type        = list(string)
  default     = ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]
}

variable "extra_worker_security_group_ids" {
  description = "Additional security groups attached to the streaming worker task ENI (e.g. to grant access to MSK / Valkey)."
  type        = list(string)
  default     = []
}

variable "extra_query_security_group_ids" {
  description = "Additional security groups attached to the query task ENI."
  type        = list(string)
  default     = []
}

# ----------------------------------------------------------------------------
# ECS cluster + image
# ----------------------------------------------------------------------------

variable "ecs_cluster_arn" {
  description = "Existing ECS cluster ARN."
  type        = string
}

variable "image" {
  description = "Container image. Worker, query, and bootstrap all bind to the same image; the entrypoint differs."
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

variable "bootstrap_command" {
  description = "Container command for the bootstrap / kappa-replay runner."
  type        = list(string)
  default     = ["/murmur-bootstrap"]
}

# ----------------------------------------------------------------------------
# Task environment variables
# ----------------------------------------------------------------------------

variable "worker_env" {
  description = "Additional env vars passed to the worker (KAFKA_BROKERS, KAFKA_TOPIC, CONSUMER_GROUP, etc.). Overrides the module's defaults on key collision."
  type        = map(string)
  default     = {}
}

variable "query_env" {
  description = "Additional env vars passed to the query server."
  type        = map(string)
  default     = {}
}

variable "bootstrap_env" {
  description = "Additional env vars passed to the bootstrap runner (typically S3 bucket / replay range)."
  type        = map(string)
  default     = {}
}

variable "valkey_uri" {
  description = "Optional Valkey (ElastiCache) URI. When set, the module injects VALKEY_ADDRESS into all three tasks' environments."
  type        = string
  default     = null
}

# ----------------------------------------------------------------------------
# Task sizing
# ----------------------------------------------------------------------------

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

variable "bootstrap_cpu" {
  description = "CPU units for the bootstrap runner. Bootstrap is bursty; size for the replay throughput you need."
  type        = number
  default     = 1024
}

variable "bootstrap_memory" {
  description = "Memory for the bootstrap runner in MiB."
  type        = number
  default     = 2048
}

variable "worker_desired_count" {
  description = "Desired ECS task count for the worker. Scale with Kafka partition count."
  type        = number
  default     = 1
}

variable "query_desired_count" {
  description = "Desired ECS task count for the query service."
  type        = number
  default     = 2
}

# ----------------------------------------------------------------------------
# Misc
# ----------------------------------------------------------------------------

variable "grpc_port" {
  description = "TCP port the query server listens on inside the container and on the ALB."
  type        = number
  default     = 50051
}

variable "log_group_name" {
  description = "Override the CloudWatch log group prefix. Defaults to /ecs/<name>. The module appends -worker, -query, and -bootstrap."
  type        = string
  default     = null
}

variable "log_retention_days" {
  description = "CloudWatch log retention for all three log groups."
  type        = number
  default     = 14
}

variable "tags" {
  description = "Tags applied to all resources."
  type        = map(string)
  default     = {}
}

# ----------------------------------------------------------------------------
# Atomic state-table swap (pkg/swap)
# ----------------------------------------------------------------------------

variable "swap_enabled" {
  description = "When true, provision a pkg/swap control table and inject SWAP_CONTROL_TABLE / SWAP_ALIAS into all three tasks' environments. The state table at var.name continues to exist (and serves as v1 of the alias by convention); subsequent versions are created outside the module by the backfill workflow. Default false — single-table mode."
  type        = bool
  default     = false
}

variable "swap_control_table_name" {
  description = "Override the control-table name. Defaults to \"<name>_swap\". Honored only when swap_enabled is true."
  type        = string
  default     = null
}

variable "swap_initial_version" {
  description = "Seed the alias pointer to this version on first apply. Set to 1 to make var.name the active table at deploy time; leave null to defer pointer initialization until the application calls swap.Manager.SetActive. Honored only when swap_enabled is true."
  type        = number
  default     = null
}
