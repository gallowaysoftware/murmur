# CloudWatch alarms for the real-AWS soak. Each alarm routes to
# var.alarm_sns_topic_arn when set; otherwise the alarms exist but don't
# notify (still visible in the console, still record state transitions).

locals {
  alarm_actions = var.alarm_sns_topic_arn == null ? [] : [var.alarm_sns_topic_arn]
}

# ----------------------------------------------------------------------------
# Lambda alarms (Kinesis side)
# ----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${var.name}-lambda-errors"
  alarm_description   = "Lambda function returned an error. Anything > 0 over 5 minutes is worth investigating during a soak."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = module.lambda.lambda_function_name
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}

resource "aws_cloudwatch_metric_alarm" "lambda_iterator_age" {
  alarm_name          = "${var.name}-lambda-iterator-age"
  alarm_description   = "Lambda is falling behind the Kinesis stream. IteratorAge above ${var.kinesis_iterator_age_threshold_ms}ms means consumption rate < ingest rate."
  namespace           = "AWS/Lambda"
  metric_name         = "IteratorAge"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  threshold           = var.kinesis_iterator_age_threshold_ms
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = module.lambda.lambda_function_name
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}

resource "aws_cloudwatch_metric_alarm" "lambda_throttles" {
  alarm_name          = "${var.name}-lambda-throttles"
  alarm_description   = "Lambda is being throttled by account / reserved concurrency. Raise reserved_concurrency or the account limit."
  namespace           = "AWS/Lambda"
  metric_name         = "Throttles"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = module.lambda.lambda_function_name
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}

# ----------------------------------------------------------------------------
# DDB alarms (state + dedup tables)
# ----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "ddb_state_throttles" {
  alarm_name          = "${var.name}-ddb-state-throttles"
  alarm_description   = "DynamoDB write throttling on the state table. PAY_PER_REQUEST should not throttle at moderate scale; anything > 0 is a partition-key hot spot or a true capacity ceiling."
  namespace           = "AWS/DynamoDB"
  metric_name         = "WriteThrottleEvents"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    TableName = module.ecs.ddb_table_name
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}

resource "aws_cloudwatch_metric_alarm" "ddb_dedup_throttles" {
  alarm_name          = "${var.name}-ddb-dedup-throttles"
  alarm_description   = "DynamoDB write throttling on the dedup table. Same diagnostic as the state-table alarm; usually different hot spots (event IDs vs entity keys)."
  namespace           = "AWS/DynamoDB"
  metric_name         = "WriteThrottleEvents"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    TableName = module.ecs.dedup_table_name
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}

# ----------------------------------------------------------------------------
# Kinesis alarms (provisioned mode only)
# ----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "kinesis_write_throttles" {
  count = var.kinesis_shard_count == null ? 0 : 1

  alarm_name          = "${var.name}-kinesis-write-throttles"
  alarm_description   = "Producers are being throttled writing to Kinesis. Raise shard_count or switch to ON_DEMAND."
  namespace           = "AWS/Kinesis"
  metric_name         = "WriteProvisionedThroughputExceeded"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    StreamName = module.lambda.kinesis_stream_name
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}
