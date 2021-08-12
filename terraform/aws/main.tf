variable "env" {}
variable "eodhistoricaldata_api_token" {}

output "aws_apigatewayv2_api_endpoint" {
  value = "${aws_apigatewayv2_api.lambda.api_endpoint}/${aws_apigatewayv2_stage.lambda.name}"
}

resource "aws_s3_bucket" "collections" {
  bucket = "gainy-collections"
  acl    = "private"

  tags = {
    Name = "Gainy collections"
  }
}

# gateway
resource "aws_cloudwatch_log_group" "api_gw" {
  name = "/aws/api_gw/gainy-lambda"

  retention_in_days = 30
}

resource "aws_apigatewayv2_api" "lambda" {
  name          = "serverless_lambda_gw"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_stage" "lambda" {
  api_id = aws_apigatewayv2_api.lambda.id

  name        = "serverless_lambda_stage_${var.env}"
  auto_deploy = true

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_gw.arn

    format = jsonencode({
      requestId               = "$context.requestId"
      sourceIp                = "$context.identity.sourceIp"
      requestTime             = "$context.requestTime"
      protocol                = "$context.protocol"
      httpMethod              = "$context.httpMethod"
      resourcePath            = "$context.resourcePath"
      routeKey                = "$context.routeKey"
      status                  = "$context.status"
      responseLength          = "$context.responseLength"
      integrationErrorMessage = "$context.integrationErrorMessage"
    }
    )
  }
}

module "lambda-fetchChartData" {
  source = "./lambda"
  env=var.env
  function_name = "fetchChartData"
  route = "POST /fetchChartData"
  aws_apigatewayv2_api_lambda_id = aws_apigatewayv2_api.lambda.id
  aws_apigatewayv2_api_lambda_name = aws_apigatewayv2_api.lambda.name
  aws_apigatewayv2_api_lambda_execution_arn = aws_apigatewayv2_api.lambda.execution_arn
  env_vars = {
    eodhistoricaldata_api_token = var.eodhistoricaldata_api_token
  }
}
