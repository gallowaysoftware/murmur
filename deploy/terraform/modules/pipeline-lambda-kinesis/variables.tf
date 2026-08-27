variable "name" {
  description = "Resource prefix. Used for the Lambda function, Kinesis stream, IAM role, and CloudWatch log group."
  type        = string
}

# ----------------------------------------------------------------------------
# Kinesis source
# ----------------------------------------------------------------------------

variable "kinesis_shard_count" {
  description = "Number of shards for the Kinesis stream. Each shard is ~1 MB/s in. Use ON_DEMAND mode by setting this to null."
  type        = number
  default     = 2
}

variable "kinesis_retention_hours" {
  description = "Kinesis stream retention in hours (24-8760). Longer retention lets you replay further back; costs more."
  type        = number
  default     = 24
}

variable "kinesis_stream_arn" {
  description = "Optional pre-existing Kinesis stream ARN. When set, the module skips stream creation and only wires the event-source mapping + IAM. Use this when ingest is shared with non-Murmur consumers."
  type        = string
  default     = null
}

# ----------------------------------------------------------------------------
# Lambda function
# ----------------------------------------------------------------------------

variable "lambda_zip_path" {
  description = "Local path to a deployment zip containing a `bootstrap` binary built for provided.al2 (linux arm64 or amd64). The example's cmd/lambda/main.go ships the build invocation in its leading comment."
  type        = string
}

variable "lambda_architecture" {
  description = "Lambda CPU architecture. arm64 is cheaper and matches the example's build instructions."
  type        = string
  default     = "arm64"
  validation {
    condition     = contains(["arm64", "x86_64"], var.lambda_architecture)
    error_message = "lambda_architecture must be arm64 or x86_64."
  }
}

variable "lambda_memory" {
  description = "Lambda memory in MB. CPU scales with memory; 512 MB is plenty for decode-heavy formats."
  type        = number
  default     = 512
}

variable "lambda_timeout" {
  description = "Lambda per-invocation timeout in seconds. Must exceed worst-case batch processing time including retries; default 60s is conservative for typical batch_size."
  type        = number
  default     = 60
}

variable "lambda_env" {
  description = "Additional Lambda environment variables. Module pre-populates DDB_TABLE / DDB_DEDUP_TABLE / AWS_REGION."
  type        = map(string)
  default     = {}
}

variable "reserved_concurrency" {
  description = "Lambda reserved concurrency. Cap to throttle Kinesis-driven invocations during incident response. -1 = unreserved (default account pool)."
  type        = number
  default     = -1
}

# ----------------------------------------------------------------------------
# Event source mapping
# ----------------------------------------------------------------------------

variable "batch_size" {
  description = "Max records per invocation (1-10000). Higher batch sizes amortize Lambda cold-start + DDB BatchGet over more events but raise per-invocation timeout risk."
  type        = number
  default     = 100
}

variable "batch_window_seconds" {
  description = "MaximumBatchingWindowInSeconds — how long Lambda waits to accumulate a batch (0-300). Set > 0 for low-throughput streams; 0 invokes as records arrive."
  type        = number
  default     = 5
}

variable "parallelization_factor" {
  description = "Concurrent invocations per shard (1-10). Lambda's autoscaling axis for Kinesis."
  type        = number
  default     = 1
}

variable "starting_position" {
  description = "Where the event-source mapping begins reading on first deploy. TRIM_HORIZON for backfill from retention start, LATEST for live-only, AT_TIMESTAMP with starting_position_timestamp for handoff from a snapshot bootstrap."
  type        = string
  default     = "LATEST"
}

variable "starting_position_timestamp" {
  description = "RFC3339 timestamp; honored only when starting_position = AT_TIMESTAMP. Set this from the CaptureHandoff timestamp returned by your snapshot bootstrap source for gap-and-duplicate-free transitions."
  type        = string
  default     = null
}

variable "maximum_retry_attempts" {
  description = "Max retries before a record is sent to the on-failure destination (or dropped if no destination). -1 = retry until retention expires."
  type        = number
  default     = 10
}

variable "maximum_record_age_seconds" {
  description = "Max age of a record before Lambda gives up retrying it (60-604800). -1 = retention max."
  type        = number
  default     = -1
}

variable "bisect_batch_on_function_error" {
  description = "On function error, bisect the failed batch and retry — drives down the blast radius of poison pills paired with maximum_retry_attempts. Murmur's BatchItemFailures path also surfaces the specific failing record; both can coexist."
  type        = bool
  default     = true
}

variable "on_failure_destination_arn" {
  description = "ARN of an SQS queue or SNS topic to receive records that exhausted retries. null = drop (the BatchItemFailures sentinel will already have surfaced them in CloudWatch)."
  type        = string
  default     = null
}

# ----------------------------------------------------------------------------
# State / dedup IAM grants
# ----------------------------------------------------------------------------

variable "state_table_arn" {
  description = "ARN of the DynamoDB state table the Lambda writes into. Typically the `ddb_table_arn` output of a sibling pipeline-counter module that shares state with this Lambda."
  type        = string
}

variable "dedup_enabled" {
  description = "Whether to wire at-least-once dedup: grants the Lambda role GetItem / PutItem / DeleteItem on the dedup table and injects DDB_DEDUP_TABLE into the function env. Must be a literal known at plan time — do NOT derive it from another module's output. Set dedup_table_arn and dedup_table_name alongside it. Strongly recommended for Kinesis, whose BatchItemFailures semantics redeliver records adjacent to a failure."
  type        = bool
  default     = false
}

variable "dedup_table_arn" {
  description = "ARN of the at-least-once dedup table. Required when dedup_enabled is true; used only to scope the IAM policy, so it may be a value that is unknown until apply (e.g. a sibling module's output)."
  type        = string
  default     = null

  validation {
    condition     = !var.dedup_enabled || var.dedup_table_arn != null
    error_message = "dedup_table_arn must be set when dedup_enabled is true."
  }
}

variable "dedup_table_name" {
  description = "Dedup table name. Required when dedup_enabled is true so the module can populate DDB_DEDUP_TABLE in the Lambda env."
  type        = string
  default     = null

  validation {
    condition     = !var.dedup_enabled || var.dedup_table_name != null
    error_message = "dedup_table_name must be set when dedup_enabled is true."
  }
}

variable "state_table_name" {
  description = "State table name. Populated into the Lambda env as DDB_TABLE."
  type        = string
}

# ----------------------------------------------------------------------------
# Misc
# ----------------------------------------------------------------------------

variable "log_retention_days" {
  description = "CloudWatch Logs retention for the Lambda's log group."
  type        = number
  default     = 14
}

variable "tags" {
  description = "Tags applied to all resources."
  type        = map(string)
  default     = {}
}
