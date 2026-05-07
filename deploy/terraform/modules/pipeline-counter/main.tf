terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

# ----------------------------------------------------------------------------
# DynamoDB state table
# ----------------------------------------------------------------------------

resource "aws_dynamodb_table" "state" {
  name         = var.name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "N"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = var.tags
}

# ----------------------------------------------------------------------------
# IAM: shared task-execution role + per-service task role
# ----------------------------------------------------------------------------

resource "aws_iam_role" "task_execution" {
  name = "${var.name}-task-execution"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "task_execution_managed" {
  role       = aws_iam_role.task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "task" {
  name = "${var.name}-task"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = var.tags
}

resource "aws_iam_role_policy" "task_ddb" {
  name = "${var.name}-ddb-rw"
  role = aws_iam_role.task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "dynamodb:GetItem",
        "dynamodb:BatchGetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem",
        "dynamodb:Query",
      ]
      Resource = aws_dynamodb_table.state.arn
    }]
  })
}

# ----------------------------------------------------------------------------
# CloudWatch log groups
# ----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "worker" {
  name              = "/ecs/${var.name}-worker"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_cloudwatch_log_group" "query" {
  name              = "/ecs/${var.name}-query"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# ----------------------------------------------------------------------------
# Worker: ECS Fargate streaming task
# ----------------------------------------------------------------------------

locals {
  base_env = {
    DDB_TABLE  = aws_dynamodb_table.state.name
    AWS_REGION = data.aws_region.current.region
  }
  worker_env_merged = merge(local.base_env, var.worker_env)
  query_env_merged  = merge(local.base_env, var.query_env)
}

data "aws_region" "current" {}

resource "aws_ecs_task_definition" "worker" {
  family                   = "${var.name}-worker"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.worker_cpu
  memory                   = var.worker_memory
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

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

resource "aws_ecs_service" "worker" {
  name            = "${var.name}-worker"
  cluster         = var.ecs_cluster_arn
  task_definition = aws_ecs_task_definition.worker.arn
  desired_count   = var.worker_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.worker.id]
    assign_public_ip = false
  }

  deployment_minimum_healthy_percent = 50
  deployment_maximum_percent         = 200

  tags = var.tags
}

# ----------------------------------------------------------------------------
# Query: ECS Fargate gRPC service behind ALB
# ----------------------------------------------------------------------------

resource "aws_security_group" "alb" {
  name        = "${var.name}-query-alb"
  description = "${var.name} query ALB"
  vpc_id      = var.vpc_id
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
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
    security_groups = [aws_security_group.alb.id]
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
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = var.public_subnet_ids
  tags               = var.tags
}

resource "aws_lb_target_group" "query" {
  name        = substr("${var.name}-q", 0, 32)
  port        = var.grpc_port
  protocol    = "HTTP"
  protocol_version = "GRPC"
  target_type = "ip"
  vpc_id      = var.vpc_id

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
  port              = 80
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
  task_role_arn            = aws_iam_role.task.arn

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
      for k, v in merge(local.query_env_merged, { GRPC_ADDR = ":${var.grpc_port}" }) : { name = k, value = v }
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
    security_groups  = [aws_security_group.query_task.id]
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
