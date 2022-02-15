variable "env" {}
variable "function_name" {}
variable "handler" {}
variable "env_vars" {
  default = {}
}
variable "timeout" {
  default = 3
}
variable "url" {}
variable "aws_apigatewayv2_api_lambda_id" {}
variable "aws_apigatewayv2_api_lambda_execution_arn" {}
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

module "route" {
  source                                    = "../route"
  aws_apigatewayv2_api_lambda_id            = var.aws_apigatewayv2_api_lambda_id
  aws_apigatewayv2_api_lambda_execution_arn = var.aws_apigatewayv2_api_lambda_execution_arn
  aws_iam_role_lambda_exec_role             = var.aws_iam_role_lambda_exec_role
  aws_lambda_invoke_arn                     = "${aws_lambda_function.lambda.arn}:${aws_lambda_function.lambda.version}"
  url                                       = var.url
}
