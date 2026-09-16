# Alerting for the conditions the runbook says to watch. The collectors are
# designed to stop when they cannot proceed safely, so a stopping task and a
# restart loop are the signals that matter most.

resource "aws_sns_topic" "alarms" {
  name              = "${var.name}-alarms"
  kms_master_key_id = aws_kms_key.alarms.arn
}

# Alarm messages describe the archive's failures, so the topic is encrypted.
# The AWS managed SNS key cannot be used: EventBridge and CloudWatch need a key
# policy that lets them encrypt messages they publish.
resource "aws_kms_key" "alarms" {
  description             = "${var.name} alarm topic encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  policy                  = data.aws_iam_policy_document.alarms_key.json
}

data "aws_iam_policy_document" "alarms_key" {
  statement {
    sid       = "AccountAdministration"
    actions   = ["kms:*"]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = ["arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
  }
  statement {
    sid       = "AlarmPublishers"
    actions   = ["kms:GenerateDataKey*", "kms:Decrypt"]
    resources = ["*"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com", "cloudwatch.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_sns_topic_subscription" "alarms_email" {
  count     = var.alarm_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

# A collector task that exits non-zero has hit a condition needing a human:
# a rejected replay ID, a lost lease, or an archive that stayed unreachable.
resource "aws_cloudwatch_event_rule" "task_stopped" {
  name        = "${var.name}-task-stopped"
  description = "Collector task stopped with a non-zero exit code"
  event_pattern = jsonencode({
    source      = ["aws.ecs"]
    detail-type = ["ECS Task State Change"]
    detail = {
      clusterArn    = [aws_ecs_cluster.this.arn]
      lastStatus    = ["STOPPED"]
      desiredStatus = ["STOPPED"]
      containers = {
        exitCode = [{ "anything-but" = 0 }]
      }
    }
  })
}

resource "aws_cloudwatch_event_target" "task_stopped" {
  rule      = aws_cloudwatch_event_rule.task_stopped.name
  target_id = "sns"
  arn       = aws_sns_topic.alarms.arn
}

# Each service may publish only from this stack's own rule or alarms, so another
# account's rule or alarm cannot use the topic (confused deputy).
data "aws_iam_policy_document" "alarms_topic" {
  statement {
    sid       = "TaskStoppedRule"
    actions   = ["SNS:Publish"]
    resources = [aws_sns_topic.alarms.arn]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_cloudwatch_event_rule.task_stopped.arn]
    }
  }
  statement {
    sid       = "LogSignalAlarms"
    actions   = ["SNS:Publish"]
    resources = [aws_sns_topic.alarms.arn]
    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }
    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:${data.aws_partition.current.partition}:cloudwatch:${var.region}:${data.aws_caller_identity.current.account_id}:alarm:${var.name}-*"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_sns_topic_policy" "alarms" {
  arn    = aws_sns_topic.alarms.arn
  policy = data.aws_iam_policy_document.alarms_topic.json
}

# Log-based signals. The collectors log these at ERROR with stable wording.
locals {
  log_alarms = {
    checkpoint_reset = {
      pattern     = "CHECKPOINT RESET"
      description = "A replay checkpoint was reset: events after the previous replay ID may need backfilling."
    }
    quarantined = {
      pattern     = "QUARANTINED"
      description = "An EventLogFile could not be parsed and was archived as raw lines."
    }
    unavailable = {
      pattern     = "UNAVAILABLE EventLogFile"
      description = "Salesforce permanently refused an EventLogFile; a tombstone was archived and it was skipped."
    }
    replay_rejected = {
      pattern     = "stored replay ID was rejected"
      description = "Salesforce rejected a stored replay ID; the collector stopped rather than skipping data."
    }
  }
}

resource "aws_cloudwatch_log_metric_filter" "signals" {
  for_each       = local.log_alarms
  name           = "${var.name}-${each.key}"
  log_group_name = aws_cloudwatch_log_group.this.name
  pattern        = "\"${each.value.pattern}\""
  metric_transformation {
    name      = "${var.name}-${each.key}"
    namespace = "SalesforceArchiver"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "signals" {
  for_each            = local.log_alarms
  alarm_name          = "${var.name}-${each.key}"
  alarm_description   = each.value.description
  namespace           = "SalesforceArchiver"
  metric_name         = "${var.name}-${each.key}"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]
}
