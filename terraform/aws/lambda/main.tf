variable "function_name" {}
variable "route" {}
variable "aws_apigatewayv2_api_lambda_id" {}
variable "aws_apigatewayv2_api_lambda_name" {}
variable "aws_apigatewayv2_api_lambda_execution_arn" {}
variable "env_vars" {
  default = {}
}

locals {
  timestamp = formatdate("YYMMDDhhmmss", timestamp())
  root_dir = abspath("../src/aws/lambda")
}

# Compress source code
data "archive_file" "source" {
  type        = "zip"
  source_dir  = local.root_dir
  output_path = "/tmp/lambda.zip"
}

resource "aws_s3_bucket" "build" {
  bucket = "gainy-lambda-builds"
  acl    = "private"
}

resource "aws_s3_bucket_object" "object" {
  bucket = aws_s3_bucket.build.id
  key   = "source.${data.archive_file.source.output_md5}.zip"
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
  role       = "${aws_iam_role.lambda_exec.name}"
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_lambda_function" "lambda" {
  function_name = var.function_name

  s3_bucket = aws_s3_bucket.build.id
  s3_key    = aws_s3_bucket_object.object.id

  runtime = "nodejs12.x"
  handler       = "index.${var.function_name}"

  source_code_hash = data.archive_file.source.output_base64sha256

  role = aws_iam_role.lambda_exec.arn

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

resource "aws_apigatewayv2_route" "hello_world" {
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