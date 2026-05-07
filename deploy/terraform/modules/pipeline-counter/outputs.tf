output "ddb_table_name" {
  description = "DynamoDB state table name."
  value       = aws_dynamodb_table.state.name
}

output "ddb_table_arn" {
  description = "DynamoDB state table ARN."
  value       = aws_dynamodb_table.state.arn
}

output "worker_service_name" {
  description = "Streaming worker ECS service name."
  value       = aws_ecs_service.worker.name
}

output "query_service_name" {
  description = "Query ECS service name."
  value       = aws_ecs_service.query.name
}

output "query_alb_dns_name" {
  description = "DNS name of the query ALB. gRPC clients dial this on port 80."
  value       = aws_lb.query.dns_name
}

output "task_role_arn" {
  description = "Task IAM role; extend with additional policies (Kinesis, MSK, S3 archive) as needed."
  value       = aws_iam_role.task.arn
}
