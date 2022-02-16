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
variable "datadog_api_key" {}
variable "datadog_app_key" {}
variable "base_image_registry_address" {}
variable "base_image_version" {}
variable "plaid_client_id" {}
variable "plaid_secret" {}
variable "plaid_development_secret" {}
variable "plaid_env" {}
variable "algolia_tickers_index" {}
variable "algolia_collections_index" {}
variable "algolia_app_id" {}
variable "algolia_search_key" {}
variable "hasura_url" {}
variable "hubspot_api_key" {}
variable "deployment_key" {}
variable "redis_cache_host" {}
variable "redis_cache_port" {}
variable "public_schema_name" {}

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

  name        = "default_${var.env}"
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

#################################### Role ####################################

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

#################################### Build ####################################

locals {
  timestamp       = formatdate("YYMMDDhhmmss", timestamp())
  nodejs_root_dir = abspath("${path.cwd}/../src/aws/lambda-nodejs")
  python_root_dir = abspath("${path.cwd}/../src/aws/lambda-python")
}

data "aws_region" "current" {}
data "aws_caller_identity" "this" {}
data "aws_ecr_authorization_token" "token" {}
locals {
  ecr_address                  = format("%v.dkr.ecr.%v.amazonaws.com", data.aws_caller_identity.this.account_id, data.aws_region.current.name)
  ecr_repo                     = var.container_repository
  nodejs_lambda_image_tag      = format("lambda-nodejs-%s-%s", var.env, data.archive_file.nodejs_source.output_md5)
  nodejs_lambda_ecr_image_name = format("%v/%v:%v", local.ecr_address, local.ecr_repo, local.nodejs_lambda_image_tag)
  python_lambda_image_tag      = format("lambda-python-%s-%s-%s", var.env, var.base_image_version, data.archive_file.python_source.output_md5)
  python_lambda_ecr_image_name = format("%v/%v:%v", local.ecr_address, local.ecr_repo, local.python_lambda_image_tag)
}

#################################### Node.js lambdas ####################################

data "archive_file" "nodejs_source" {
  type        = "zip"
  source_dir  = local.nodejs_root_dir
  output_path = "/tmp/lambda-nodejs.zip"
}
resource "docker_registry_image" "lambda_nodejs" {
  name = local.nodejs_lambda_ecr_image_name
  build {
    context    = local.nodejs_root_dir
    dockerfile = "Dockerfile"
  }

  lifecycle {
    ignore_changes = [build["context"]]
  }
}

module "fetchChartData" {
  source                                    = "./type-image"
  env                                       = var.env
  function_name                             = "fetchChartData"
  handler                                   = "index.fetchChartData"
  timeout                                   = 10
  url                                       = "/${var.deployment_key}/fetchChartData"
  aws_apigatewayv2_api_lambda_id            = aws_apigatewayv2_api.lambda.id
  aws_apigatewayv2_api_lambda_execution_arn = aws_apigatewayv2_api.lambda.execution_arn
  aws_iam_role_lambda_exec_role             = aws_iam_role.lambda_exec
  image_uri                                 = docker_registry_image.lambda_nodejs.name

  env_vars = {
    eodhistoricaldata_api_token = var.eodhistoricaldata_api_token
  }
  vpc_security_group_ids = var.vpc_security_group_ids
  vpc_subnet_ids         = var.vpc_subnet_ids
}

module "fetchNewsData" {
  source                                    = "./type-image"
  env                                       = var.env
  function_name                             = "fetchNewsData"
  handler                                   = "index.fetchNewsData"
  timeout                                   = 10
  url                                       = "/${var.deployment_key}/fetchNewsData"
  aws_apigatewayv2_api_lambda_id            = aws_apigatewayv2_api.lambda.id
  aws_apigatewayv2_api_lambda_execution_arn = aws_apigatewayv2_api.lambda.execution_arn
  aws_iam_role_lambda_exec_role             = aws_iam_role.lambda_exec
  image_uri                                 = docker_registry_image.lambda_nodejs.name

  env_vars = {
    gnews_api_token = var.gnews_api_token
  }
  vpc_security_group_ids = var.vpc_security_group_ids
  vpc_subnet_ids         = var.vpc_subnet_ids
}

module "fetchLivePrices" {
  source                                    = "./type-image"
  env                                       = var.env
  function_name                             = "fetchLivePrices"
  handler                                   = "index.fetchLivePrices"
  timeout                                   = 10
  url                                       = "/${var.deployment_key}/fetchLivePrices"
  aws_apigatewayv2_api_lambda_id            = aws_apigatewayv2_api.lambda.id
  aws_apigatewayv2_api_lambda_execution_arn = aws_apigatewayv2_api.lambda.execution_arn
  aws_iam_role_lambda_exec_role             = aws_iam_role.lambda_exec
  image_uri                                 = docker_registry_image.lambda_nodejs.name

  env_vars = {
    eodhistoricaldata_api_token = var.eodhistoricaldata_api_token
  }
  vpc_security_group_ids = var.vpc_security_group_ids
  vpc_subnet_ids         = var.vpc_subnet_ids
}

#################################### Python lambdas ####################################

data "archive_file" "python_source" {
  type        = "zip"
  source_dir  = local.python_root_dir
  output_path = "/tmp/lambda-python.zip"
}
resource "docker_registry_image" "lambda_python" {
  name = local.python_lambda_ecr_image_name
  build {
    context    = local.python_root_dir
    dockerfile = "Dockerfile"
    build_args = {
      BASE_IMAGE_REGISTRY_ADDRESS = var.base_image_registry_address
      BASE_IMAGE_VERSION          = var.base_image_version
    }

    auth_config {
      host_name = local.ecr_address
      user_name = data.aws_ecr_authorization_token.token.user_name
      password  = data.aws_ecr_authorization_token.token.password
    }
  }

  lifecycle {
    ignore_changes = [build["context"]]
  }
}

module "hasuraTrigger" {
  source                                    = "./type-image"
  env                                       = var.env
  function_name                             = "hasuraTrigger"
  handler                                   = "hasura_handler.handle_trigger"
  timeout                                   = 150
  url                                       = "/${var.deployment_key}/hasuraTrigger"
  aws_apigatewayv2_api_lambda_id            = aws_apigatewayv2_api.lambda.id
  aws_apigatewayv2_api_lambda_execution_arn = aws_apigatewayv2_api.lambda.execution_arn
  aws_iam_role_lambda_exec_role             = aws_iam_role.lambda_exec
  image_uri                                 = docker_registry_image.lambda_python.name
  memory_size                               = var.env == "production" ? 512 : 256

  env_vars = {
    pg_host                   = var.pg_host
    pg_port                   = var.pg_port
    pg_dbname                 = var.pg_dbname
    pg_username               = var.pg_username
    pg_password               = var.pg_password
    PUBLIC_SCHEMA_NAME        = var.public_schema_name
    DATADOG_API_KEY           = var.datadog_api_key
    DATADOG_APP_KEY           = var.datadog_app_key
    ENV                       = var.env
    PLAID_CLIENT_ID           = var.plaid_client_id
    PLAID_SECRET              = var.plaid_secret
    PLAID_DEVELOPMENT_SECRET  = var.plaid_development_secret
    PLAID_ENV                 = var.plaid_env
    ALGOLIA_APP_ID            = var.algolia_app_id
    ALGOLIA_TICKERS_INDEX     = var.algolia_tickers_index
    ALGOLIA_COLLECTIONS_INDEX = var.algolia_collections_index
    ALGOLIA_SEARCH_API_KEY    = var.algolia_search_key
    HUBSPOT_API_KEY           = var.hubspot_api_key
  }
  vpc_security_group_ids = var.vpc_security_group_ids
  vpc_subnet_ids         = var.vpc_subnet_ids
}

module "hasuraAction" {
  source                                    = "./type-image"
  env                                       = var.env
  function_name                             = "hasuraAction"
  handler                                   = "hasura_handler.handle_action"
  timeout                                   = 30
  url                                       = "/${var.deployment_key}/hasuraAction"
  aws_apigatewayv2_api_lambda_id            = aws_apigatewayv2_api.lambda.id
  aws_apigatewayv2_api_lambda_execution_arn = aws_apigatewayv2_api.lambda.execution_arn
  aws_iam_role_lambda_exec_role             = aws_iam_role.lambda_exec
  image_uri                                 = docker_registry_image.lambda_python.name
  memory_size                               = var.env == "production" ? 256 : 128

  env_vars = {
    pg_host                   = var.pg_host
    pg_port                   = var.pg_port
    pg_dbname                 = var.pg_dbname
    pg_username               = var.pg_username
    pg_password               = var.pg_password
    PUBLIC_SCHEMA_NAME        = var.public_schema_name
    DATADOG_API_KEY           = var.datadog_api_key
    DATADOG_APP_KEY           = var.datadog_app_key
    ENV                       = var.env
    PLAID_CLIENT_ID           = var.plaid_client_id
    PLAID_SECRET              = var.plaid_secret
    PLAID_DEVELOPMENT_SECRET  = var.plaid_development_secret
    PLAID_ENV                 = var.plaid_env
    ALGOLIA_APP_ID            = var.algolia_app_id
    ALGOLIA_TICKERS_INDEX     = var.algolia_tickers_index
    ALGOLIA_COLLECTIONS_INDEX = var.algolia_collections_index
    ALGOLIA_SEARCH_API_KEY    = var.algolia_search_key
    ALGOLIA_SEARCH_API_KEY    = var.algolia_search_key
    GNEWS_API_TOKEN           = var.gnews_api_token
    REDIS_CACHE_HOST          = var.redis_cache_host
    REDIS_CACHE_PORT          = var.redis_cache_port
    PLAID_WEBHOOK_URL         = "https://${var.hasura_url}/api/rest/plaid_webhook"
  }
  vpc_security_group_ids = var.vpc_security_group_ids
  vpc_subnet_ids         = var.vpc_subnet_ids
}
