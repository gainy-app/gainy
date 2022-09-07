variable "env" {}
variable "function_name" {}
variable "handler" {}
variable "env_vars" {
  default = {}
}
variable "timeout" {
  default = 3
}
variable "aws_iam_role_lambda_exec_role" {}
variable "image_uri" {}
variable "vpc_security_group_ids" {}
variable "vpc_subnet_ids" {}
variable "memory_size" {
  default = 128
}

resource "aws_iam_role_policy_attachment" "iam_role_policy_attachment_lambda_vpc_access_execution" {
  role       = var.aws_iam_role_lambda_exec_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_lambda_function" "lambda" {
  function_name = "${var.function_name}_${var.env}"

  package_type = "Image"

  image_uri = var.image_uri
  image_config {
    command = [var.handler]
  }

  timeout = var.timeout
  publish = true

  memory_size = var.memory_size

  role = var.aws_iam_role_lambda_exec_role.arn

  vpc_config {
    security_group_ids = var.vpc_security_group_ids
    subnet_ids         = var.vpc_subnet_ids
  }

  environment {
    variables = var.env_vars
  }
}

variable "sqs_batch_size" {}
variable "sqs_queue_arns" {
  type = list(string)
}

resource "aws_lambda_event_source_mapping" "lambda_via_sqs" {
  for_each = toset(var.sqs_queue_arns)

  batch_size       = var.sqs_batch_size
  event_source_arn = each.key
  function_name    = aws_lambda_function.lambda.function_name
}

data "aws_iam_policy_document" "lambda_sqs_policy_document" {
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
  name   = "lambda_sqs_policy"
  role   = var.aws_iam_role_lambda_exec_role.arn
  policy = data.aws_iam_policy_document.lambda_sqs_policy_document.json
}
