resource "aws_cloudwatch_event_rule" "ecs_events" {
  name        = "event-listener-${var.env}"
  description = "AWS events listener ${var.env}"

  event_pattern = jsonencode({
    "source" : [
      "aws.ecs"
    ],
    "detail-type" : [
      "ECS Deployment State Change",
    ]
    #    "detail" : {
    #      "clusterArn" : [
    #        var.ecs_cluster_arn
    #      ]
    #    }
  })
}

resource "aws_cloudwatch_event_target" "sqs" {
  rule = aws_cloudwatch_event_rule.ecs_events.name
  arn  = aws_sqs_queue.aws_events.arn
}

resource "aws_sqs_queue" "aws_events" {
  name                      = "aws-events-${var.env}"
  message_retention_seconds = 3600

  tags = {
    Environment = var.env
  }
}

resource "aws_sqs_queue_policy" "aws_events" {
  queue_url = aws_sqs_queue.aws_events.id

  policy = jsonencode({
    "Version" : "2012-10-17",
    "Id" : "aws_cloudwatch_event_rule_${var.env}",
    "Statement" : [
      {
        "Sid" : "Stmt1671363215332",
        "Effect" : "Allow",
        "Principal" : {
          "AWS" : "*"
        },
        "Action" : "sqs:SendMessage",
        "Resource" : aws_sqs_queue.aws_events.arn,
        "Condition" : {
          "ArnEquals" : {
            "aws:SourceArn" : aws_cloudwatch_event_rule.ecs_events.arn
          }
        }
      }
    ]
  })
}

output "aws_events_sqs_arn" {
  value = aws_sqs_queue.aws_events.arn
}
