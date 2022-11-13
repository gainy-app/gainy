variable "env" {}
variable "gnews_api_token" {}
variable "pg_host" {}
variable "pg_port" {}
variable "pg_dbname" {}
variable "pg_username" {}
variable "pg_password" {}
variable "docker_repository_name" {}
variable "vpc_security_group_ids" {}
variable "vpc_subnet_ids" {}
variable "datadog_api_key" {}
variable "datadog_app_key" {}
variable "base_image_registry_address" {}
variable "base_image_version" {}
variable "plaid_client_id" {}
variable "plaid_secret" {}
variable "plaid_development_secret" {}
variable "plaid_sandbox_secret" {}
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
variable "codeartifact_pipy_url" {}
variable "gainy_compute_version" {}
variable "revenuecat_api_key" {}
variable "stripe_api_key" {}
variable "stripe_publishable_key" {}
variable "drivewealth_is_uat" {}
variable "drivewealth_app_key" {}
variable "drivewealth_wlp_id" {}
variable "drivewealth_parent_ibid" {}
variable "drivewealth_ria_id" {}
variable "drivewealth_ria_product_id" {}
variable "drivewealth_api_username" {}
variable "drivewealth_api_password" {}
variable "drivewealth_api_url" {}
variable "drivewealth_sqs_arn" {}
variable "s3_bucket_uploads_kyc" {}
variable "aws_access_key" {}
variable "aws_secret_key" {}
variable "aws_region" {}
variable "google_places_api_key" {}

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

resource "aws_iam_policy" "kyc_uploads" {
  name        = "s3_kyc_upload_${var.env}"
  description = "S3 KYC uploads policy ${var.env}"

  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:PutObject",
          "s3:GetObject"
        ],
        "Resource" : [
          "arn:aws:s3:::${var.s3_bucket_uploads_kyc}/*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:GetBucketLocation"
        ],
        "Resource" : [
          "arn:aws:s3:::${var.s3_bucket_uploads_kyc}"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "kyc_uploads" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.kyc_uploads.arn
}

resource "aws_iam_role_policy_attachment" "lambda_policy" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "iam_role_policy_attachment_lambda_vpc_access_execution" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

#################################### Build ####################################

locals {
  timestamp       = formatdate("YYMMDDhhmmss", timestamp())
  python_root_dir = abspath("${path.cwd}/../src/aws/lambda-python")
}

data "aws_region" "current" {}
data "aws_caller_identity" "this" {}
data "aws_ecr_authorization_token" "token" {}
locals {
  docker_registry_address = format("%v.dkr.ecr.%v.amazonaws.com", data.aws_caller_identity.this.account_id, data.aws_region.current.name)

  python_lambda_build_args_force_build = {
    BASE_IMAGE_VERSION       = var.base_image_version
    GAINY_COMPUTE_VERSION    = var.gainy_compute_version
    PYTHON_LAMBDA_SOURCE_MD5 = data.archive_file.python_source.output_md5
  }
  python_lambda_build_args = merge(local.python_lambda_build_args_force_build, {
    BASE_IMAGE_REGISTRY_ADDRESS = var.base_image_registry_address
    CODEARTIFACT_PIPY_URL       = var.codeartifact_pipy_url
  })
  python_lambda_image_tag  = format("lambda-python-%s-%s-%s", var.env, var.base_image_version, md5(jsonencode(local.python_lambda_build_args_force_build)))
  python_lambda_image_name = format("%v/%v:%v", local.docker_registry_address, var.docker_repository_name, local.python_lambda_image_tag)

  env_vars = {
    PG_HOST                    = var.pg_host
    PG_PORT                    = var.pg_port
    PG_DBNAME                  = var.pg_dbname
    PG_USERNAME                = var.pg_username
    PG_PASSWORD                = var.pg_password
    PUBLIC_SCHEMA_NAME         = var.public_schema_name
    DATADOG_API_KEY            = var.datadog_api_key
    DATADOG_APP_KEY            = var.datadog_app_key
    ENV                        = var.env
    PLAID_CLIENT_ID            = var.plaid_client_id
    PLAID_SECRET               = var.plaid_secret
    PLAID_DEVELOPMENT_SECRET   = var.plaid_development_secret
    PLAID_SANDBOX_SECRET       = var.plaid_sandbox_secret
    PLAID_ENV                  = var.plaid_env
    PLAID_WEBHOOK_URL          = "https://${var.hasura_url}/api/rest/plaid_webhook"
    ALGOLIA_APP_ID             = var.algolia_app_id
    ALGOLIA_TICKERS_INDEX      = var.algolia_tickers_index
    ALGOLIA_COLLECTIONS_INDEX  = var.algolia_collections_index
    ALGOLIA_SEARCH_API_KEY     = var.algolia_search_key
    HUBSPOT_API_KEY            = var.hubspot_api_key
    REVENUECAT_API_KEY         = var.revenuecat_api_key
    GNEWS_API_TOKEN            = var.gnews_api_token
    REDIS_CACHE_HOST           = var.redis_cache_host
    REDIS_CACHE_PORT           = var.redis_cache_port
    DRIVEWEALTH_IS_UAT         = var.drivewealth_is_uat
    DRIVEWEALTH_APP_KEY        = var.drivewealth_app_key
    DRIVEWEALTH_WLP_ID         = var.drivewealth_wlp_id
    DRIVEWEALTH_PARENT_IBID    = var.drivewealth_parent_ibid
    DRIVEWEALTH_RIA_ID         = var.drivewealth_ria_id
    DRIVEWEALTH_RIA_PRODUCT_ID = var.drivewealth_ria_product_id
    DRIVEWEALTH_API_USERNAME   = var.drivewealth_api_username
    DRIVEWEALTH_API_PASSWORD   = var.drivewealth_api_password
    DRIVEWEALTH_API_URL        = var.drivewealth_api_url
    DRIVEWEALTH_SQS_ARN        = var.drivewealth_sqs_arn
    S3_BUCKET_UPLOADS_KYC      = var.s3_bucket_uploads_kyc
    STRIPE_API_KEY             = var.stripe_api_key
    STRIPE_PUBLISHABLE_KEY     = var.stripe_publishable_key
    GOOGLE_PLACES_API_KEY      = var.google_places_api_key
  }
}

#################################### Python lambdas ####################################

data "archive_file" "python_source" {
  type        = "zip"
  source_dir  = local.python_root_dir
  output_path = "/tmp/lambda-python.zip"
}
resource "docker_registry_image" "lambda_python" {
  name = local.python_lambda_image_name
  build {
    context    = local.python_root_dir
    dockerfile = "Dockerfile"
    build_args = local.python_lambda_build_args

    auth_config {
      host_name = local.docker_registry_address
      user_name = data.aws_ecr_authorization_token.token.user_name
      password  = data.aws_ecr_authorization_token.token.password
    }
  }

  lifecycle {
    ignore_changes = [build["context"]]
  }
}

##################################################################################

module "hasura_trigger" {
  source                 = "./function"
  env                    = var.env
  function_name          = "hasuraTrigger"
  handler                = "hasura_handler.handle_trigger"
  timeout                = 150
  exec_role_arn          = aws_iam_role.lambda_exec.arn
  image_uri              = docker_registry_image.lambda_python.name
  memory_size            = var.env == "production" ? 512 : 256
  env_vars               = local.env_vars
  vpc_security_group_ids = var.vpc_security_group_ids
  vpc_subnet_ids         = var.vpc_subnet_ids
}

module "hasura_trigger_integration" {
  source                                    = "./route-integration"
  url                                       = "/${var.deployment_key}/hasuraTrigger"
  aws_apigatewayv2_api_lambda_id            = aws_apigatewayv2_api.lambda.id
  aws_apigatewayv2_api_lambda_execution_arn = aws_apigatewayv2_api.lambda.execution_arn
  aws_lambda_invoke_arn                     = "${module.hasura_trigger.arn}:${module.hasura_trigger.version}"
}

##################################################################################

module "hasura_action" {
  source                 = "./function"
  env                    = var.env
  function_name          = "hasuraAction"
  handler                = "hasura_handler.handle_action"
  timeout                = 90
  exec_role_arn          = aws_iam_role.lambda_exec.arn
  image_uri              = docker_registry_image.lambda_python.name
  memory_size            = var.env == "production" ? 256 : 256
  env_vars               = local.env_vars
  vpc_security_group_ids = var.vpc_security_group_ids
  vpc_subnet_ids         = var.vpc_subnet_ids
}

module "hasura_action_integration" {
  source                                    = "./route-integration"
  url                                       = "/${var.deployment_key}/hasuraAction"
  aws_apigatewayv2_api_lambda_id            = aws_apigatewayv2_api.lambda.id
  aws_apigatewayv2_api_lambda_execution_arn = aws_apigatewayv2_api.lambda.execution_arn
  aws_lambda_invoke_arn                     = "${module.hasura_action.arn}:${module.hasura_action.version}"
}

##################################################################################

module "sqs_listener" {
  source                 = "./function"
  env                    = var.env
  function_name          = "sqsListener"
  handler                = "sqs_listener.handle"
  timeout                = 30
  exec_role_arn          = aws_iam_role.lambda_exec.arn
  image_uri              = docker_registry_image.lambda_python.name
  memory_size            = var.env == "production" ? 256 : 128
  env_vars               = local.env_vars
  vpc_security_group_ids = var.vpc_security_group_ids
  vpc_subnet_ids         = var.vpc_subnet_ids
}

module "sqs_listener_integration" {
  source                        = "./sqs-integration"
  aws_iam_role_lambda_exec_role = aws_iam_role.lambda_exec
  aws_lambda_invoke_arn         = "${module.sqs_listener.arn}:${module.sqs_listener.version}"

  sqs_batch_size = 10
  sqs_queue_arns = var.drivewealth_sqs_arn != "" ? [var.drivewealth_sqs_arn] : []
}

##################################################################################

output "aws_apigatewayv2_api_endpoint" {
  value      = "${aws_apigatewayv2_api.lambda.api_endpoint}/${aws_apigatewayv2_stage.lambda.name}"
  depends_on = [module.hasura_action, module.hasura_trigger]
}
