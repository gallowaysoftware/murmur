# ----------------------------------------------------------------------------
# Atomic state-table swap (pkg/swap)
#
# Optional. When `swap_enabled = true`, the module provisions a control
# DynamoDB table that the application's pkg/swap.Manager reads to resolve
# the active state-table version for a given alias (typically `var.name`).
#
# The state-table naming convention pkg/swap uses is "<alias>_v<N>" — see
# pkg/swap.TableName. The streaming worker, query server, and bootstrap
# runner all read SWAP_CONTROL_TABLE + SWAP_ALIAS from the environment
# and call Manager.Resolve(ctx, alias) at startup (or on a refresh cadence)
# to pick the correct underlying table.
#
# Two operating models:
#
#   (a) Single-table mode (default, swap_enabled=false). The module
#       creates aws_dynamodb_table.state at `var.name` and the binaries
#       talk to it directly via DDB_TABLE. No control table, no aliasing.
#
#   (b) Aliased mode (swap_enabled=true). The module additionally creates
#       a control table (default name "<var.name>_swap"). The state table
#       at `var.name` is treated as version 1 of the alias by convention,
#       and the alias pointer is initialized to var.swap_initial_version
#       (default 1). Subsequent backfill cutovers create "<var.name>_v2",
#       "_v3", … OUTSIDE this module and call Manager.SetActive to flip
#       the pointer atomically.
#
# Both task roles (worker, query, bootstrap) get DDB read on the control
# table; bootstrap additionally gets write so the backfill runner can
# advance the pointer when its replay completes.
# ----------------------------------------------------------------------------

resource "aws_dynamodb_table" "swap_control" {
  count = var.swap_enabled ? 1 : 0

  name         = coalesce(var.swap_control_table_name, "${var.name}_swap")
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = var.tags
}

# Seed the initial alias pointer when swap_initial_version is set. Without
# this, the swap.Manager.Resolve helper still returns "<alias>_v1" via the
# implicit-bootstrap convention (see pkg/swap.Resolve), but having an
# explicit row in the control table makes the active version visible to
# `aws dynamodb get-item` for ops dashboards and removes ambiguity for the
# first SetActive call.
resource "aws_dynamodb_table_item" "swap_initial_version" {
  count = var.swap_enabled && var.swap_initial_version != null ? 1 : 0

  table_name = aws_dynamodb_table.swap_control[0].name
  hash_key   = aws_dynamodb_table.swap_control[0].hash_key

  item = jsonencode({
    pk  = { S = var.name }
    ver = { N = tostring(var.swap_initial_version) }
    at  = { N = tostring(0) } # ops can spot "never updated by SetActive"
  })

  # The application's Manager.SetActive will overwrite ver+at on every
  # cutover. Terraform should leave the row alone after creation rather
  # than treat application updates as drift.
  lifecycle {
    ignore_changes = [item]
  }
}

# IAM: worker + query need read access on the control table; bootstrap
# needs write so it can advance the pointer at the end of a replay.
data "aws_iam_policy_document" "swap_control_ro" {
  count = var.swap_enabled ? 1 : 0

  statement {
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
    ]
    resources = [aws_dynamodb_table.swap_control[0].arn]
  }
}

data "aws_iam_policy_document" "swap_control_rw" {
  count = var.swap_enabled ? 1 : 0

  statement {
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.swap_control[0].arn]
  }
}

resource "aws_iam_role_policy" "worker_swap_control" {
  count = var.swap_enabled ? 1 : 0

  name   = "${var.name}-worker-swap-control"
  role   = aws_iam_role.worker_task.id
  policy = data.aws_iam_policy_document.swap_control_ro[0].json
}

resource "aws_iam_role_policy" "query_swap_control" {
  count = var.swap_enabled ? 1 : 0

  name   = "${var.name}-query-swap-control"
  role   = aws_iam_role.query_task.id
  policy = data.aws_iam_policy_document.swap_control_ro[0].json
}

resource "aws_iam_role_policy" "bootstrap_swap_control" {
  count = var.swap_enabled ? 1 : 0

  name   = "${var.name}-bootstrap-swap-control"
  role   = aws_iam_role.bootstrap_task.id
  policy = data.aws_iam_policy_document.swap_control_rw[0].json
}
