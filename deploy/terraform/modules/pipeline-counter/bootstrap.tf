# ----------------------------------------------------------------------------
# Bootstrap runner — ECS Fargate task definition (no service)
#
# A one-shot task definition for kappa-style backfill: replay archived source
# events (S3 / Firehose) into the same DDB state table the streaming worker
# writes to. Operators launch it via `aws ecs run-task` (or a Step Function)
# when seeding a new pipeline or rematerializing a counter.
#
# No `aws_ecs_service` is created — bootstrap is run on demand, not held
# continuously. The task is granted the same DDB R/W policy as the streaming
# worker so it can perform idempotent merges.
# ----------------------------------------------------------------------------

resource "aws_security_group" "bootstrap" {
  name        = "${var.name}-bootstrap"
  description = "${var.name} bootstrap runner"
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

resource "aws_ecs_task_definition" "bootstrap" {
  family                   = "${var.name}-bootstrap"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.bootstrap_cpu
  memory                   = var.bootstrap_memory
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.bootstrap_task.arn

  container_definitions = jsonencode([{
    name      = "bootstrap"
    image     = var.image
    command   = var.bootstrap_command
    essential = true
    environment = [
      for k, v in local.bootstrap_env_merged : { name = k, value = v }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.bootstrap.name
        awslogs-region        = data.aws_region.current.region
        awslogs-stream-prefix = "bootstrap"
      }
    }
  }])

  tags = var.tags
}
