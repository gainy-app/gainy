variable "url" {}
variable "aws_apigatewayv2_api_lambda_id" {}
variable "aws_apigatewayv2_api_lambda_execution_arn" {}
variable "aws_iam_role_lambda_exec_role" {}
variable "aws_lambda_invoke_arn" {}

resource "aws_apigatewayv2_integration" "lambda" {
  api_id = var.aws_apigatewayv2_api_lambda_id

  integration_uri    = var.aws_lambda_invoke_arn
  integration_type   = "AWS_PROXY"
  integration_method = "POST"
}

resource "aws_apigatewayv2_route" "route" {
  api_id = var.aws_apigatewayv2_api_lambda_id

  route_key = "POST ${var.url}"
  target    = "integrations/${aws_apigatewayv2_integration.lambda.id}"
}
resource "aws_lambda_permission" "api_gw" {
  action        = "lambda:InvokeFunction"
  function_name = var.aws_lambda_invoke_arn
  principal     = "apigateway.amazonaws.com"

  source_arn = "${var.aws_apigatewayv2_api_lambda_execution_arn}/*/*${var.url}"
}
