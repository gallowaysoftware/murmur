output "lambda_function_name" {
  description = "Lambda function name. Use for CloudWatch metrics / alarms (Errors, Throttles, Duration, IteratorAge)."
  value       = aws_lambda_function.handler.function_name
}

output "lambda_function_arn" {
  description = "Lambda function ARN."
  value       = aws_lambda_function.handler.arn
}

output "lambda_role_arn" {
  description = "IAM role assumed by the Lambda. Extend with additional policies (Valkey VPC access, KMS decrypt, etc.) as needed."
  value       = aws_iam_role.lambda.arn
}

output "kinesis_stream_name" {
  description = "Kinesis stream name. null when caller supplied a pre-existing stream via kinesis_stream_arn."
  value       = local.create_stream ? aws_kinesis_stream.source[0].name : null
}

output "kinesis_stream_arn" {
  description = "Kinesis stream ARN — either the created stream or the caller-supplied one."
  value       = local.stream_arn
}

output "event_source_mapping_uuid" {
  description = "UUID of the event-source mapping. Use with `aws lambda update-event-source-mapping` for runtime tweaks (BatchSize / ParallelizationFactor) without a Terraform apply."
  value       = aws_lambda_event_source_mapping.kinesis.uuid
}

output "log_group_name" {
  description = "CloudWatch log group for the Lambda. Use for metric filters / subscription filters / alarms."
  value       = aws_cloudwatch_log_group.lambda.name
}
