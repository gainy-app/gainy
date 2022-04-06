locals {
  deployment_key = formatdate("YYMMDDhhmmss", timestamp())
}

resource "random_integer" "db_external_access_port" {
  min = 10000
  max = 60000
}

module "s3" {
  source = "./s3"
  env    = var.env
}

resource "aws_ecr_repository" "default" {
  name                 = "gainy-${var.env}"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

module "lambda" {
  source                 = "./lambda"
  env                    = var.env
  gnews_api_token        = var.gnews_api_token
  pg_dbname              = module.rds.db_instance.name
  pg_host                = module.rds.db_instance.address
  pg_password            = module.rds.db_instance.password
  pg_port                = module.rds.db_instance.port
  pg_username            = module.rds.db_instance.username
  public_schema_name     = module.ecs-service.public_schema_name
  container_repository   = aws_ecr_repository.default.name
  vpc_security_group_ids = [module.ecs.vpc_default_sg_id]
  vpc_subnet_ids         = module.ecs.private_subnet_ids
  deployment_key         = local.deployment_key
  datadog_api_key        = var.datadog_api_key
  datadog_app_key        = var.datadog_app_key
  hasura_url             = module.ecs-service.hasura_url
  hubspot_api_key        = var.hubspot_api_key

  base_image_registry_address = var.base_image_registry_address
  base_image_version          = var.base_image_version

  plaid_client_id          = var.plaid_client_id
  plaid_secret             = var.plaid_secret
  plaid_development_secret = var.plaid_development_secret
  plaid_env                = var.plaid_env

  algolia_tickers_index     = var.algolia_tickers_index
  algolia_collections_index = var.algolia_collections_index
  algolia_app_id            = var.algolia_app_id
  algolia_search_key        = var.algolia_search_key

  redis_cache_host = module.elasticache.redis_cache_host
  redis_cache_port = module.elasticache.redis_cache_port

  codeartifact_pipy_url = var.codeartifact_pipy_url
  gainy_compute_version = var.gainy_compute_version
}

module "ecs" {
  source                  = "./ecs"
  env                     = var.env
  vpc_index               = index(["production", "test"], var.env)
  db_external_access_port = random_integer.db_external_access_port.result
  mlflow_artifact_bucket  = module.s3.mlflow_artifact_bucket
}

module "rds" {
  source                    = "./rds"
  env                       = var.env
  private_subnet_group_name = module.ecs.private_subnet_group_name
  public_subnet_group_name  = module.ecs.public_subnet_group_name
  name                      = "gainy"
  vpc_default_sg_id         = module.ecs.vpc_default_sg_id
  db_external_access_port   = random_integer.db_external_access_port.result
}

module "elasticache" {
  source             = "./elasticache"
  env                = var.env
  vpc_default_sg_id  = module.ecs.vpc_default_sg_id
  private_subnet_ids = module.ecs.private_subnet_ids
}

module "vpc_bridge" {
  source             = "./ec2/vpc_bridge"
  env                = var.env
  vpc_default_sg_id  = module.ecs.vpc_default_sg_id
  public_subnet_id   = module.ecs.public_subnet_ids.0
  vpc_id             = module.ecs.vpc_id
  cloudflare_zone_id = var.cloudflare_zone_id
  datadog_api_key    = var.datadog_api_key

  pg_host     = module.rds.db_instance.address
  pg_password = module.rds.db_instance.password
  pg_port     = module.rds.db_instance.port
  pg_username = module.rds.db_instance.username
  pg_dbname   = module.rds.db_instance.name

  pg_production_internal_sync_username = var.pg_production_internal_sync_username
}

module "ecs-service" {
  source             = "./ecs/services"
  env                = var.env
  aws_region         = var.aws_region
  aws_access_key     = var.aws_access_key
  aws_secret_key     = var.aws_secret_key
  ecr_address        = local.ecr_address
  repository_name    = aws_ecr_repository.default.name
  aws_log_region     = data.aws_region.current.name
  vpc_id             = module.ecs.vpc_id
  vpc_default_sg_id  = module.ecs.vpc_default_sg_id
  public_https_sg_id = module.ecs.public_https_sg_id
  public_http_sg_id  = module.ecs.public_http_sg_id
  public_subnet_ids  = module.ecs.public_subnet_ids
  ecs_cluster_name   = module.ecs.ecs_cluster.name
  cloudflare_zone_id = var.cloudflare_zone_id
  domain             = var.domain
  private_subnet_ids = module.ecs.private_subnet_ids

  aws_lambda_api_gateway_endpoint = "${module.lambda.aws_apigatewayv2_api_endpoint}/${local.deployment_key}"
  hasura_enable_console           = "true"
  hasura_enable_dev_mode          = "true"
  hasura_jwt_secret               = var.hasura_jwt_secret

  eodhistoricaldata_api_token = var.eodhistoricaldata_api_token
  pg_host                     = module.rds.db_instance.address
  pg_password                 = module.rds.db_instance.password
  pg_port                     = module.rds.db_instance.port
  pg_username                 = module.rds.db_instance.username
  pg_dbname                   = module.rds.db_instance.name
  pg_replica_uris             = sensitive(join(",", [for index, replica in module.rds.db_replica[*] : format("postgres://%s:%s@%s:%d/%s", replica.username, module.rds.db_instance.password, replica.address, replica.port, replica.name)]))

  pg_production_host                   = var.pg_production_host
  pg_production_port                   = var.pg_production_port
  pg_production_internal_sync_username = var.pg_production_internal_sync_username
  pg_production_internal_sync_password = var.pg_production_internal_sync_password

  pg_external_access_host     = length(module.rds.db_external_access) > 0 ? module.rds.db_external_access[0].address : ""
  pg_external_access_port     = length(module.rds.db_external_access) > 0 ? module.rds.db_external_access[0].port : ""
  pg_external_access_username = length(module.rds.db_external_access) > 0 ? module.rds.db_external_access[0].username : ""
  pg_external_access_password = length(module.rds.db_external_access) > 0 ? module.rds.db_external_access[0].password : ""
  pg_external_access_dbname   = length(module.rds.db_external_access) > 0 ? module.rds.db_external_access[0].name : ""
  pg_analytics_schema         = var.pg_analytics_schema
  pg_website_schema           = var.pg_website_schema

  base_image_registry_address = var.base_image_registry_address
  base_image_version          = var.base_image_version

  algolia_tickers_index     = var.algolia_tickers_index
  algolia_collections_index = var.algolia_collections_index
  algolia_app_id            = var.algolia_app_id
  algolia_indexing_key      = var.algolia_indexing_key

  datadog_api_key = var.datadog_api_key
  datadog_app_key = var.datadog_app_key

  polygon_api_token = var.polygon_api_token
  coingecko_api_key = var.coingecko_api_key

  mlflow_artifact_bucket = module.s3.mlflow_artifact_bucket
  codeartifact_pipy_url  = var.codeartifact_pipy_url
  gainy_compute_version  = var.gainy_compute_version
}


module "cloudwatch" {
  source              = "./cloudwatch"
  env                 = var.env
  hasura_admin_secret = module.ecs-service.hasura_admin_secret
  hasura_url          = module.ecs-service.hasura_url
}

output "bridge_instance_url" {
  value = module.vpc_bridge.bridge_instance_url
}

output "meltano_url" {
  value = module.ecs-service.meltano_url
}
output "hasura_url" {
  value = module.ecs-service.hasura_url
}
output "ecs_service_name" {
  value = module.ecs-service.name
}
output "additional_forwarded_log_groups" {
  value = module.ecs-service.additional_forwarded_log_groups
}

output "aws_apigatewayv2_api_endpoint" {
  value = module.lambda.aws_apigatewayv2_api_endpoint
}
output "aws_rds" {
  value = module.rds
}
output "aws_ecs" {
  value = module.ecs
}