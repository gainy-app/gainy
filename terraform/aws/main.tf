variable "env" {}
variable "eodhistoricaldata_api_token" {}
variable "gnews_api_token" {}

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

resource "aws_s3_bucket" "categories" {
  bucket = "gainy-categories"
  acl    = "private"

  tags = {
    Name = "Gainy categories"
  }
}

resource "aws_s3_bucket" "interests" {
  bucket = "gainy-interests"
  acl    = "private"

  tags = {
    Name = "Gainy interests"
  }
}

# Compress source code
locals {
  timestamp = formatdate("YYMMDDhhmmss", timestamp())
  root_dir  = abspath("../src/aws/lambda")
}

data "archive_file" "source" {
  type        = "zip"
  source_dir  = local.root_dir
  output_path = "/tmp/lambda.zip"
  excludes    = ["${local.root_dir}/node_modules"]
}

resource "aws_s3_bucket" "build" {
  bucket = "gainy-lambda-builds"
  acl    = "private"
}

resource "aws_s3_bucket_object" "object" {
  bucket = aws_s3_bucket.build.id
  key    = "source.${data.archive_file.source.output_md5}.zip"
  source = data.archive_file.source.output_path
}

resource "aws_iam_role" "lambda_exec" {
  name = "serverless_lambda"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Sid    = ""
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_policy" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
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
  source                                    = "./lambda"
  env                                       = var.env
  function_name                             = "fetchChartData"
  route                                     = "POST /fetchChartData"
  aws_apigatewayv2_api_lambda_id            = aws_apigatewayv2_api.lambda.id
  aws_apigatewayv2_api_lambda_name          = aws_apigatewayv2_api.lambda.name
  aws_apigatewayv2_api_lambda_execution_arn = aws_apigatewayv2_api.lambda.execution_arn
  aws_s3_bucket                             = aws_s3_bucket.build.id
  aws_s3_key                                = aws_s3_bucket_object.object.id
  aws_iam_role_lambda_exec_role             = aws_iam_role.lambda_exec.arn
  source_code_hash                          = data.archive_file.source.output_base64sha256
  env_vars = {
    eodhistoricaldata_api_token = var.eodhistoricaldata_api_token
  }
}

module "lambda-fetchNewsData" {
  source                                    = "./lambda"
  env                                       = var.env
  function_name                             = "fetchNewsData"
  route                                     = "POST /fetchNewsData"
  aws_apigatewayv2_api_lambda_id            = aws_apigatewayv2_api.lambda.id
  aws_apigatewayv2_api_lambda_name          = aws_apigatewayv2_api.lambda.name
  aws_apigatewayv2_api_lambda_execution_arn = aws_apigatewayv2_api.lambda.execution_arn
  aws_s3_bucket                             = aws_s3_bucket.build.id
  aws_s3_key                                = aws_s3_bucket_object.object.id
  aws_iam_role_lambda_exec_role             = aws_iam_role.lambda_exec.arn
  source_code_hash                          = data.archive_file.source.output_base64sha256
  env_vars = {
    gnews_api_token = var.gnews_api_token
  }
}
