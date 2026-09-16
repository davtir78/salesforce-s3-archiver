# Alerting for the conditions the runbook says to watch. The collectors are
# designed to stop when they cannot proceed safely, so a stopping task and a
# restart loop are the signals that matter most.

resource "aws_sns_topic" "alarms" {
  name = "${var.name}-alarms"
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

data "aws_iam_policy_document" "alarms_topic" {
  statement {
    actions   = ["SNS:Publish"]
    resources = [aws_sns_topic.alarms.arn]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com", "cloudwatch.amazonaws.com"]
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
