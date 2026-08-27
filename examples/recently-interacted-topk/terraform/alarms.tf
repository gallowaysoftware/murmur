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

# ----------------------------------------------------------------------------
# Liveness alarms — the inverse polarity of everything above
# ----------------------------------------------------------------------------
#
# Every alarm above fires on "a bad thing happened, count > 0" and sets
# treat_missing_data = "notBreaching". That combination cannot detect the most
# likely quiet failure of a long unattended soak: the pipeline simply STOPS.
#
# If the event-source mapping is disabled, the producer dies, or the worker
# crash-loops, then Errors / Throttles / IteratorAge all report *no datapoints*
# and every alarm above sits at OK. Ten weeks later an all-green console gets
# read as "the soak ran clean" — the exact inverse of the truth.
#
# These alarms are the mirror image: LessThanThreshold, with
# treat_missing_data = "breaching" so absence of data IS the alarm condition.

resource "aws_cloudwatch_metric_alarm" "lambda_silent" {
  alarm_name          = "${var.name}-lambda-silent"
  alarm_description   = "Lambda has not been invoked recently. Detects a disabled/failed event-source mapping or a dead upstream producer — neither of which moves Errors, Throttles, or IteratorAge."
  namespace           = "AWS/Lambda"
  metric_name         = "Invocations"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 3
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"

  dimensions = {
    FunctionName = module.lambda.lambda_function_name
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}

resource "aws_cloudwatch_metric_alarm" "kinesis_silent" {
  alarm_name          = "${var.name}-kinesis-silent"
  alarm_description   = "No records arriving on the Kinesis stream. Distinguishes 'the producer stopped' from 'the consumer stopped' when paired with the lambda-silent alarm."
  namespace           = "AWS/Kinesis"
  metric_name         = "IncomingRecords"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 3
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"

  dimensions = {
    StreamName = module.lambda.kinesis_stream_name
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}

# The ECS/Kafka half had NO alarm coverage of any kind. Without these, the
# Kafka source can be dead for the whole soak while the Kinesis path keeps the
# query service returning a healthy-looking Top-N — and the soak would then
# certify exactly one of the two sources it exists to exercise.
#
# PREREQUISITE: `ECS/ContainerInsights` only publishes when Container Insights
# is enabled on the cluster, and the cluster is caller-owned so this module
# cannot enable it. With it off the metric does not exist, and because these
# alarms treat missing data as breaching — deliberately, so a dead service
# alarms rather than looking healthy — they sit in ALARM forever and train the
# operator to ignore them. Enable it before applying:
#
#   aws ecs update-cluster-settings --cluster <name> \
#       --settings name=containerInsights,value=enabled
#   # then force a redeploy so running tasks begin reporting
#
# `var.ecs_container_insights_required` asserts this at plan time.

resource "aws_cloudwatch_metric_alarm" "worker_not_running" {
  alarm_name          = "${var.name}-worker-not-running"
  alarm_description   = "ECS Kafka worker has fewer running tasks than desired. Catches crash-loops, image-pull failures, and the no-route-to-internet case where apply succeeded but tasks never start."
  namespace           = "ECS/ContainerInsights"
  metric_name         = "RunningTaskCount"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 2
  threshold           = var.worker_desired_count
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"

  dimensions = {
    ClusterName = var.ecs_cluster_name
    ServiceName = module.ecs.worker_service_name
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}

resource "aws_cloudwatch_metric_alarm" "query_not_running" {
  alarm_name          = "${var.name}-query-not-running"
  alarm_description   = "ECS query service has fewer running tasks than desired."
  namespace           = "ECS/ContainerInsights"
  metric_name         = "RunningTaskCount"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 2
  threshold           = var.query_desired_count
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"

  dimensions = {
    ClusterName = var.ecs_cluster_name
    ServiceName = module.ecs.query_service_name
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}

# DynamoDB write cost is the one soak line item that scales with time rather
# than sitting flat, and a PAY_PER_REQUEST table under CAS contention does not
# throttle — it just bills. WriteThrottleEvents therefore cannot catch a
# runaway. This watches consumption directly.

resource "aws_cloudwatch_metric_alarm" "ddb_write_capacity_runaway" {
  alarm_name          = "${var.name}-ddb-write-runaway"
  alarm_description   = "State-table write consumption is far above the expected soak rate. TopK is a CAS monoid on a single row, so retry storms burn WCUs without ever throttling. This is the cost backstop, not a correctness alarm."
  namespace           = "AWS/DynamoDB"
  metric_name         = "ConsumedWriteCapacityUnits"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 2
  threshold           = var.ddb_write_capacity_alarm_threshold
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    TableName = module.ecs.ddb_table_name
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}
