# ----------------------------------------------------------------------------
# Query service — ECS Fargate gRPC server behind an internal ALB
#
# Internal ALB is used (not NLB) because it natively understands HTTP/2 + gRPC
# trailers and supports gRPC-aware health checks via `/grpc.health.v1.Health/Check`.
# Cross-VPC consumers should reach this service through PrivateLink / VPC peering
# / Transit Gateway — exposing the ALB publicly is intentionally not supported
# (the gRPC server has no built-in auth).
# ----------------------------------------------------------------------------

resource "aws_security_group" "query_alb" {
  name        = "${var.name}-query-alb"
  description = "${var.name} internal query ALB"
  vpc_id      = var.vpc_id

  ingress {
    description = "gRPC clients within the VPC / connected networks"
    from_port   = var.grpc_port
    to_port     = var.grpc_port
    protocol    = "tcp"
    cidr_blocks = var.query_allowed_cidrs
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = var.tags
}

resource "aws_security_group" "query_task" {
  name        = "${var.name}-query-task"
  description = "${var.name} query task"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = var.grpc_port
    to_port         = var.grpc_port
    protocol        = "tcp"
    security_groups = [aws_security_group.query_alb.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = var.tags
}

resource "aws_lb" "query" {
  name               = substr("${var.name}-q", 0, 32)
  internal           = true
  load_balancer_type = "application"
  security_groups    = [aws_security_group.query_alb.id]
  subnets            = var.private_subnet_ids

  tags = var.tags
}

resource "aws_lb_target_group" "query" {
  name             = substr("${var.name}-q", 0, 32)
  port             = var.grpc_port
  protocol         = "HTTP"
  protocol_version = "GRPC"
  target_type      = "ip"
  vpc_id           = var.vpc_id

  health_check {
    enabled             = true
    healthy_threshold   = 2
    interval            = 15
    matcher             = "0-99"
    path                = "/grpc.health.v1.Health/Check"
    protocol            = "HTTP"
    timeout             = 5
    unhealthy_threshold = 3
  }

  tags = var.tags
}

resource "aws_lb_listener" "query" {
  load_balancer_arn = aws_lb.query.arn
  port              = var.grpc_port
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.query.arn
  }
}

resource "aws_ecs_task_definition" "query" {
  family                   = "${var.name}-query"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.query_cpu
  memory                   = var.query_memory
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.query_task.arn

  container_definitions = jsonencode([{
    name      = "query"
    image     = var.image
    command   = var.query_command
    essential = true
    portMappings = [{
      containerPort = var.grpc_port
      protocol      = "tcp"
    }]
    environment = [
      for k, v in local.query_env_merged : { name = k, value = v }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.query.name
        awslogs-region        = data.aws_region.current.region
        awslogs-stream-prefix = "query"
      }
    }
  }])

  tags = var.tags
}

resource "aws_ecs_service" "query" {
  name            = "${var.name}-query"
  cluster         = var.ecs_cluster_arn
  task_definition = aws_ecs_task_definition.query.arn
  desired_count   = var.query_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = concat([aws_security_group.query_task.id], var.extra_query_security_group_ids)
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.query.arn
    container_name   = "query"
    container_port   = var.grpc_port
  }

  deployment_minimum_healthy_percent = 50
  deployment_maximum_percent         = 200

  depends_on = [aws_lb_listener.query]
  tags       = var.tags
}
