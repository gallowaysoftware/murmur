# ----------------------------------------------------------------------------
# Streaming worker — ECS Fargate service
#
# Long-running Kafka consumer that folds events into the DDB state table.
# Scale `worker_desired_count` with Kafka partition count (one task per
# partition is typical for franz-go consumer groups).
# ----------------------------------------------------------------------------

resource "aws_security_group" "worker" {
  name        = "${var.name}-worker"
  description = "${var.name} streaming worker"
  vpc_id      = var.vpc_id

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = var.tags
}

resource "aws_ecs_task_definition" "worker" {
  family                   = "${var.name}-worker"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.worker_cpu
  memory                   = var.worker_memory
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.worker_task.arn

  container_definitions = jsonencode([{
    name      = "worker"
    image     = var.image
    command   = var.worker_command
    essential = true
    environment = [
      for k, v in local.worker_env_merged : { name = k, value = v }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.worker.name
        awslogs-region        = data.aws_region.current.region
        awslogs-stream-prefix = "worker"
      }
    }
  }])

  tags = var.tags
}

resource "aws_ecs_service" "worker" {
  name            = "${var.name}-worker"
  cluster         = var.ecs_cluster_arn
  task_definition = aws_ecs_task_definition.worker.arn
  desired_count   = var.worker_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = concat([aws_security_group.worker.id], var.extra_worker_security_group_ids)
    assign_public_ip = false
  }

  deployment_minimum_healthy_percent = 50
  deployment_maximum_percent         = 200

  tags = var.tags
}
