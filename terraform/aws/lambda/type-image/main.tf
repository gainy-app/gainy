variable "env" {}
variable "function_name" {}
variable "source_code_hash" {}
variable "handler" {}
variable "env_vars" {
  default = {}
}
variable "timeout" {
  default = 3
}
variable "route" {}
variable "aws_apigatewayv2_api_lambda_id" {}
variable "aws_apigatewayv2_api_lambda_execution_arn" {}
variable "aws_iam_role_lambda_exec_role" {}
variable "image_uri" {}

resource "aws_lambda_function" "lambda" {
  function_name = "${var.function_name}_${var.env}"

  package_type = "Image"

  image_uri = var.image_uri
  image_config {
    command = [var.handler]
  }

  timeout = var.timeout

  source_code_hash = var.source_code_hash

  role = var.aws_iam_role_lambda_exec_role

  environment {
    variables = var.env_vars
  }
}

module "route" {
  source                                    = "../route"
  route                                     = var.route
  aws_apigatewayv2_api_lambda_id            = var.aws_apigatewayv2_api_lambda_id
  aws_apigatewayv2_api_lambda_execution_arn = var.aws_apigatewayv2_api_lambda_execution_arn
  aws_iam_role_lambda_exec_role             = var.aws_iam_role_lambda_exec_role
  aws_lambda_invoke_arn                     = aws_lambda_function.lambda.invoke_arn
  aws_lambda_function_name                  = aws_lambda_function.lambda.function_name
}
