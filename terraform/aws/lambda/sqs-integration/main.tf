variable "aws_lambda_invoke_arn" {}
variable "aws_iam_role_lambda_exec_role" {}
variable "sqs_batch_size" {}
variable "sqs_queue_arns" {
  type = list(string)
}

resource "aws_lambda_event_source_mapping" "lambda_via_sqs" {
  for_each = toset(var.sqs_queue_arns)

  batch_size       = var.sqs_batch_size
  event_source_arn = each.key
  function_name    = var.aws_lambda_invoke_arn
}

data "aws_iam_policy_document" "lambda_sqs_policy_document" {
  count = length(var.sqs_queue_arns) > 0 ? 1 : 0
  statement {
    sid = "ProcessSQSMessages"

    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ReceiveMessage",
    ]

    resources = var.sqs_queue_arns
  }
}

resource "aws_iam_role_policy" "lambda_sqs_policy" {
  count  = length(var.sqs_queue_arns) > 0 ? 1 : 0
  name   = "lambda_sqs_policy"
  role   = var.aws_iam_role_lambda_exec_role.name
  policy = data.aws_iam_policy_document.lambda_sqs_policy_document[count.index].json
}
