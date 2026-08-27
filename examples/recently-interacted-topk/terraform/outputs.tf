output "query_service_endpoint" {
  description = "Internal ALB host:port for the gRPC query service. Dial this from anything inside the VPC (or reachable via peering / TGW)."
  value       = module.ecs.query_service_endpoint
}

output "state_table_name" {
  description = "DynamoDB state table both ingest paths write into."
  value       = module.ecs.ddb_table_name
}

output "dedup_table_name" {
  description = "DynamoDB dedup table backing pkg/state/dynamodb.Deduper."
  value       = module.ecs.dedup_table_name
}

output "kinesis_stream_name" {
  description = "Kinesis stream the Lambda consumes."
  value       = module.lambda.kinesis_stream_name
}

output "lambda_function_name" {
  description = "Lambda function name. Use for ad-hoc invocation / log tailing."
  value       = module.lambda.lambda_function_name
}

output "worker_iam_role_arn" {
  description = "ECS worker task role. Attach the MSK / SASL policies the worker needs."
  value       = module.ecs.streaming_worker_iam_role_arn
}

output "lambda_role_arn" {
  description = "Lambda execution role. Attach VPC / KMS / additional policies as needed."
  value       = module.lambda.lambda_role_arn
}

output "bootstrap_task_definition_arn" {
  description = "ECS task definition for the bootstrap runner. Launch with `aws ecs run-task` when seeding from an S3 archive."
  value       = module.ecs.bootstrap_task_definition_arn
}
