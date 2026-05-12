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

output "query_service_dns" {
  description = "DNS name of the internal query ALB. gRPC clients dial \"<dns>:<grpc_port>\"."
  value       = aws_lb.query.dns_name
}

output "query_service_endpoint" {
  description = "Convenience host:port string for gRPC clients."
  value       = "${aws_lb.query.dns_name}:${var.grpc_port}"
}

output "bootstrap_task_definition_arn" {
  description = "Task definition ARN for the bootstrap / kappa-replay runner. Launch with `aws ecs run-task`."
  value       = aws_ecs_task_definition.bootstrap.arn
}

output "streaming_worker_iam_role_arn" {
  description = "Task IAM role for the streaming worker. Extend with additional policies (Kinesis, MSK, S3 archive) as needed."
  value       = aws_iam_role.worker_task.arn
}

output "query_service_iam_role_arn" {
  description = "Task IAM role for the query service (read-only DDB by default)."
  value       = aws_iam_role.query_task.arn
}

output "bootstrap_iam_role_arn" {
  description = "Task IAM role for the bootstrap runner. Extend with S3 read perms for replay sources."
  value       = aws_iam_role.bootstrap_task.arn
}

output "task_execution_role_arn" {
  description = "Shared task-execution role used to pull images and ship logs."
  value       = aws_iam_role.task_execution.arn
}

output "worker_security_group_id" {
  description = "Security group attached to the streaming worker ENI. Reference from MSK / Valkey SGs to grant ingress."
  value       = aws_security_group.worker.id
}

output "query_alb_security_group_id" {
  description = "Security group attached to the internal query ALB."
  value       = aws_security_group.query_alb.id
}

output "bootstrap_security_group_id" {
  description = "Security group attached to bootstrap-runner tasks."
  value       = aws_security_group.bootstrap.id
}
