resource "aws_cloudwatch_event_rule" "console" {
  name        = "event-listener-${var.env}"
  description = "AWS events listener ${var.env}"

  event_pattern = <<EOF
{
  "source": [
    "aws.ecs"
  ],
  "detail-type": [
    "ECS Task State Change",
    "ECS Container Instance State Change",
    "ECS Deployment State Change"
  ],
  "detail": {
    "clusterArn": [
      ${var.ecs_cluster_arn}
    ]
  }
}
EOF
}

resource "aws_cloudwatch_event_target" "sns" {
  rule      = aws_cloudwatch_event_rule.console.name
  target_id = "SendToSNS"
  arn       = aws_sns_topic.aws_events.arn
}

resource "aws_sns_topic" "aws_events" {
  name = "aws-events-${var.env}"
}

resource "aws_sns_topic_policy" "default" {
  arn    = aws_sns_topic.aws_events.arn
  policy = data.aws_iam_policy_document.sns_topic_policy.json
}

data "aws_iam_policy_document" "sns_topic_policy" {
  statement {
    effect  = "Allow"
    actions = ["SNS:Publish"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    resources = [aws_sns_topic.aws_events.arn]
  }
}