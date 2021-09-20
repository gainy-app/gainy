variable "env" {}
variable "eodhistoricaldata_api_token" {}
variable "gnews_api_token" {}
variable "pg_host" {}
variable "pg_port" {}
variable "pg_dbname" {}
variable "pg_username" {}
variable "pg_password" {}
variable "container_repository" {}
variable "vpc_security_group_ids" {}
variable "vpc_subnet_ids" {}

output "aws_apigatewayv2_api_endpoint" {
  value = "${aws_apigatewayv2_api.lambda.api_endpoint}/${aws_apigatewayv2_stage.lambda.name}"
}

# gateway

resource "aws_cloudwatch_log_group" "api_gw" {
  name = "/aws/api_gw/gainy-lambda-${var.env}"

  retention_in_days = 30
}

resource "aws_apigatewayv2_api" "lambda" {
  name          = "serverless_lambda_gw_${var.env}"
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

#################################### Node.js lambdas ####################################

locals {
  timestamp       = formatdate("YYMMDDhhmmss", timestamp())
  nodejs_root_dir = abspath("${path.cwd}/../src/aws/lambda-nodejs")
  python_root_dir = abspath("${path.cwd}/../src/aws/lambda-python")
}
data "archive_file" "nodejs_source" {
  type        = "zip"
  source_dir  = local.nodejs_root_dir
  output_path = "/tmp/lambda-nodejs.zip"
  excludes    = ["node_modules"]
}

resource "aws_s3_bucket" "build" {
  bucket = "gainy-lambda-builds"
  acl    = "private"
}

resource "aws_s3_bucket_object" "nodejs" {
  bucket = aws_s3_bucket.build.id
  key    = "source.${data.archive_file.nodejs_source.output_md5}.zip"
  source = data.archive_file.nodejs_source.output_path
}

resource "aws_iam_role" "lambda_exec" {
  name = "serverless_lambda_${var.env}"

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

module "fetchChartData" {
  source                                    = "./type-zip"
  env                                       = var.env
  function_name                             = "fetchChartData"
  route                                     = "POST /fetchChartData"
  aws_apigatewayv2_api_lambda_id            = aws_apigatewayv2_api.lambda.id
  aws_apigatewayv2_api_lambda_execution_arn = aws_apigatewayv2_api.lambda.execution_arn
  aws_s3_bucket                             = aws_s3_bucket.build.id
  aws_s3_key                                = aws_s3_bucket_object.nodejs.id
  aws_iam_role_lambda_exec_role             = aws_iam_role.lambda_exec.arn
  source_code_hash                          = data.archive_file.nodejs_source.output_base64sha256
  env_vars = {
    eodhistoricaldata_api_token = var.eodhistoricaldata_api_token
  }
  runtime = "nodejs12.x"
  handler = "index.fetchChartData"
}

module "fetchNewsData" {
  source                                    = "./type-zip"
  env                                       = var.env
  function_name                             = "fetchNewsData"
  route                                     = "POST /fetchNewsData"
  aws_apigatewayv2_api_lambda_id            = aws_apigatewayv2_api.lambda.id
  aws_apigatewayv2_api_lambda_execution_arn = aws_apigatewayv2_api.lambda.execution_arn
  aws_s3_bucket                             = aws_s3_bucket.build.id
  aws_s3_key                                = aws_s3_bucket_object.nodejs.id
  aws_iam_role_lambda_exec_role             = aws_iam_role.lambda_exec.arn
  source_code_hash                          = data.archive_file.nodejs_source.output_base64sha256
  env_vars = {
    gnews_api_token = var.gnews_api_token
  }
  runtime = "nodejs12.x"
  handler = "index.fetchNewsData"
}

module "fetchLivePrices" {
  source                                    = "./type-zip"
  env                                       = var.env
  function_name                             = "fetchLivePrices"
  route                                     = "POST /fetchLivePrices"
  aws_apigatewayv2_api_lambda_id            = aws_apigatewayv2_api.lambda.id
  aws_apigatewayv2_api_lambda_execution_arn = aws_apigatewayv2_api.lambda.execution_arn
  aws_s3_bucket                             = aws_s3_bucket.build.id
  aws_s3_key                                = aws_s3_bucket_object.nodejs.id
  aws_iam_role_lambda_exec_role             = aws_iam_role.lambda_exec.arn
  source_code_hash                          = data.archive_file.nodejs_source.output_base64sha256
  timeout                                   = 10
  env_vars = {
    eodhistoricaldata_api_token = var.eodhistoricaldata_api_token
  }
  runtime = "nodejs12.x"
  handler = "index.fetchLivePrices"
}

#################################### Python lambdas ####################################

data "archive_file" "python_source" {
  type        = "zip"
  source_dir  = local.python_root_dir
  output_path = "/tmp/lambda-python.zip"
}

data "aws_region" "current" {}
data "aws_caller_identity" "this" {}
data "aws_ecr_authorization_token" "token" {}
locals {
  ecr_address                  = format("%v.dkr.ecr.%v.amazonaws.com", data.aws_caller_identity.this.account_id, data.aws_region.current.name)
  ecr_repo                     = var.container_repository
  python_lambda_image_tag      = format("lambda-python-%s-%s", var.env, data.archive_file.python_source.output_md5)
  python_lambda_ecr_image_name = format("%v/%v:%v", local.ecr_address, local.ecr_repo, local.python_lambda_image_tag)
}
resource "docker_registry_image" "lambda_python" {
  name = local.python_lambda_ecr_image_name
  build {
    context    = local.python_root_dir
    dockerfile = "Dockerfile"
  }
}

module "setUserCategories" {
  source                                    = "./type-image"
  env                                       = var.env
  function_name                             = "setUserCategories"
  handler                                   = "set_user_categories.handle"
  timeout                                   = 10
  route                                     = "POST /setUserCategories"
  aws_apigatewayv2_api_lambda_id            = aws_apigatewayv2_api.lambda.id
  aws_apigatewayv2_api_lambda_execution_arn = aws_apigatewayv2_api.lambda.execution_arn
  aws_iam_role_lambda_exec_role             = aws_iam_role.lambda_exec
  image_uri                                 = docker_registry_image.lambda_python.name

  env_vars = {
    pg_host     = var.pg_host
    pg_port     = var.pg_port
    pg_dbname   = var.pg_dbname
    pg_username = var.pg_username
    pg_password = var.pg_password
  }
  vpc_security_group_ids = var.vpc_security_group_ids
  vpc_subnet_ids = var.vpc_subnet_ids
}
