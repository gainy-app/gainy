

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
  source                      = "./lambda"
  env                         = var.env
  eodhistoricaldata_api_token = var.eodhistoricaldata_api_token
  gnews_api_token             = var.gnews_api_token
  pg_dbname                   = module.rds.db_instance.name
  pg_host                     = module.rds.db_instance.address
  pg_password                 = module.rds.db_instance.password
  pg_port                     = module.rds.db_instance.port
  pg_username                 = module.rds.db_instance.username
  container_repository        = aws_ecr_repository.default.name
  vpc_security_group_ids      = [module.ecs.vpc_default_sg_id]
  vpc_subnet_ids              = module.ecs.private_subnet_ids
}

module "ecs" {
  source        = "./ecs"
  env           = var.env
  instance_type = local.ecs_instance_type
}

module "rds" {
  source               = "./rds"
  env                  = var.env
  db_subnet_group_name = module.ecs.db_subnet_group_name
  name                 = "gainy"
  vpc_default_sg_id    = module.ecs.vpc_default_sg_id
}

module "vpc_bridge" {
  source             = "./ec2/vpc_bridge"
  env                = var.env
  vpc_default_sg_id  = module.ecs.vpc_default_sg_id
  public_subnet_id   = module.ecs.public_subnet_ids.0
  vpc_id             = module.ecs.vpc_id
  cloudflare_zone_id = var.cloudflare_zone_id
}

module "meltano" {
  source               = "./ecs/services/meltano"
  env                  = var.env
  ecr_address          = local.ecr_address
  repository_name      = aws_ecr_repository.default.name
  aws_log_group_name   = module.ecs.aws_cloudwatch_log_group.name
  aws_log_region       = data.aws_region.current.name
  vpc_id               = module.ecs.vpc_id
  vpc_default_sg_id    = module.ecs.vpc_default_sg_id
  public_https_sg_id   = module.ecs.public_https_sg_id
  public_http_sg_id    = module.ecs.public_http_sg_id
  public_subnet_ids    = module.ecs.public_subnet_ids
  ecs_cluster_name     = module.ecs.ecs_cluster.name
  ecs_service_role_arn = module.ecs.ecsServiceRole_arn
  cloudflare_zone_id   = var.cloudflare_zone_id
  domain               = var.domain

  eodhistoricaldata_api_token = var.eodhistoricaldata_api_token
  pg_host                     = module.rds.db_instance.address
  pg_password                 = module.rds.db_instance.password
  pg_port                     = module.rds.db_instance.port
  pg_username                 = module.rds.db_instance.username
  pg_dbname                   = module.rds.db_instance.name

  eodhistoricaldata_jobs_count = local.meltano_eodhistoricaldata_jobs_count
  scheduler_cpu_credits        = local.meltano_scheduler_cpu_credits
  scheduler_memory_credits     = local.meltano_scheduler_memory_credits
  ui_memory_credits            = local.meltano_ui_memory_credits

  base_image_registry_address = var.base_image_registry_address
  base_image_version          = var.base_image_version
}

module "hasura" {
  source               = "./ecs/services/hasura"
  env                  = var.env
  ecr_address          = local.ecr_address
  repository_name      = aws_ecr_repository.default.name
  aws_log_group_name   = module.ecs.aws_cloudwatch_log_group.name
  aws_log_region       = data.aws_region.current.name
  vpc_id               = module.ecs.vpc_id
  vpc_default_sg_id    = module.ecs.vpc_default_sg_id
  public_https_sg_id   = module.ecs.public_https_sg_id
  public_http_sg_id    = module.ecs.public_http_sg_id
  public_subnet_ids    = module.ecs.public_subnet_ids
  ecs_cluster_name     = module.ecs.ecs_cluster.name
  ecs_service_role_arn = module.ecs.ecsServiceRole_arn
  cloudflare_zone_id   = var.cloudflare_zone_id
  domain               = var.domain

  pg_host                         = module.rds.db_instance.address
  pg_password                     = module.rds.db_instance.password
  pg_port                         = module.rds.db_instance.port
  pg_username                     = module.rds.db_instance.username
  pg_dbname                       = module.rds.db_instance.name
  aws_lambda_api_gateway_endpoint = module.lambda.aws_apigatewayv2_api_endpoint
  hasura_enable_console           = "true"
  hasura_enable_dev_mode          = "true"
  hasura_jwt_secret               = var.hasura_jwt_secret

  cpu_credits    = local.hasura_cpu_credits
  memory_credits = local.hasura_memory_credits

  base_image_registry_address = var.base_image_registry_address
  base_image_version          = var.base_image_version
}

output "bridge_instance_url" {
  value = module.vpc_bridge.bridge_instance_url
}

output "meltano_url" {
  value = module.meltano.service_url
}

output "hasura_url" {
  value = module.hasura.service_url
}

output "aws_apigatewayv2_api_endpoint" {
  value = module.lambda.aws_apigatewayv2_api_endpoint
}
output "aws_rds" {
  value = module.rds
}