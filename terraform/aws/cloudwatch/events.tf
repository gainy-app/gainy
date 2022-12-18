resource "aws_cloudwatch_event_rule" "console" {
  name        = "event-listener-${var.env}"
  description = "AWS events listener ${var.env}"

  event_pattern = jsonencode({
    "source" : [
      "aws.ecs"
    ],
    "detail-type" : [
      "ECS Task State Change",
      "ECS Container Instance State Change",
      "ECS Deployment State Change"
    ],
    "detail" : {
      "clusterArn" : [
        var.ecs_cluster_arn
      ]
    }
  })
}

resource "aws_cloudwatch_event_target" "sqs" {
  rule = aws_cloudwatch_event_rule.console.name
  arn  = aws_sqs_queue.aws_events.arn
}

resource "aws_sqs_queue" "aws_events" {
  name                      = "aws-events-${var.env}"
  message_retention_seconds = 3600

  tags = {
    Environment = var.env
  }
}

output "aws_events_sqs_arn" {
  value = aws_sqs_queue.aws_events.arn
}
