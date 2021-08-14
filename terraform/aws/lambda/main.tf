variable "env" {}
variable "function_name" {}
variable "route" {}
variable "aws_apigatewayv2_api_lambda_id" {}
variable "aws_apigatewayv2_api_lambda_name" {}
variable "aws_apigatewayv2_api_lambda_execution_arn" {}
variable "aws_s3_bucket" {}
variable "aws_s3_key" {}
variable "aws_iam_role_lambda_exec_role" {}
variable "source_code_hash" {}
variable "env_vars" {
  default = {}
}

resource "aws_lambda_function" "lambda" {
  function_name = "${var.function_name}_${var.env}"

  s3_bucket = var.aws_s3_bucket
  s3_key    = var.aws_s3_key

  runtime = "nodejs12.x"
  handler = "index.${var.function_name}"

  source_code_hash = var.source_code_hash

  role = var.aws_iam_role_lambda_exec_role

  environment {
    variables = var.env_vars
  }
}

resource "aws_apigatewayv2_integration" "lambda" {
  api_id = var.aws_apigatewayv2_api_lambda_id

  integration_uri    = aws_lambda_function.lambda.invoke_arn
  integration_type   = "AWS_PROXY"
  integration_method = "POST"
}

resource "aws_apigatewayv2_route" "route" {
  api_id = var.aws_apigatewayv2_api_lambda_id

  route_key = var.route
  target    = "integrations/${aws_apigatewayv2_integration.lambda.id}"
}

resource "aws_lambda_permission" "api_gw" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda.function_name
  principal     = "apigateway.amazonaws.com"

  source_arn = "${var.aws_apigatewayv2_api_lambda_execution_arn}/*/*"
}
