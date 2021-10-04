locals {
  ecr_repo              = var.repository_name
  hasura_root_dir       = abspath("${path.cwd}/../src/hasura")
  hasura_image_tag      = format("hasura-%s-%s", var.env, data.archive_file.hasura_source.output_md5)
  hasura_ecr_image_name = format("%v/%v:%v", var.ecr_address, local.ecr_repo, local.hasura_image_tag)
}

/*
 * Create an image
 */
data "archive_file" "hasura_source" {
  type        = "zip"
  source_dir  = local.hasura_root_dir
  output_path = "/tmp/hasura-source.zip"
}
resource "docker_registry_image" "hasura" {
  name = local.hasura_ecr_image_name
  build {
    context    = local.hasura_root_dir
    dockerfile = "Dockerfile"
    build_args = {
      BASE_IMAGE_REGISTRY_ADDRESS  = var.base_image_registry_address
      BASE_IMAGE_VERSION = var.base_image_version
    }
  }
}
/*
 * Create task definition
 */
resource "random_password" "hasura" {
  length           = 16
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}
resource "aws_ecs_task_definition" "hasura" {
  family                   = "hasura-${var.env}"
  network_mode             = "bridge"
  requires_compatibilities = []
  tags                     = {}

  container_definitions = templatefile(
    "${path.module}/container-definitions.json",
    {
      pg_host                         = var.pg_host
      pg_password                     = var.pg_password
      pg_port                         = var.pg_port
      pg_username                     = var.pg_username
      pg_dbname                       = var.pg_dbname
      hasura_port                     = 8080
      hasura_enable_console           = var.hasura_enable_console
      hasura_enable_dev_mode          = var.hasura_enable_dev_mode
      hasura_admin_secret             = random_password.hasura.result
      hasura_jwt_secret               = var.hasura_jwt_secret
      aws_lambda_api_gateway_endpoint = var.aws_lambda_api_gateway_endpoint
      aws_log_group_name              = var.aws_log_group_name
      aws_log_region                  = var.aws_log_region
      image                           = docker_registry_image.hasura.name
      memory_credits                  = var.memory_credits
      cpu_credits                     = var.cpu_credits
    }
  )
}
module "service-hasura" {
  source                           = "../"
  name                             = "hasura"
  container_name                   = "hasura"
  env                              = var.env
  domain                           = var.domain
  vpc_id                           = var.vpc_id
  vpc_default_sg_id                = var.vpc_default_sg_id
  public_https_sg_id               = var.public_https_sg_id
  public_http_sg_id                = var.public_http_sg_id
  public_subnet_ids                = var.public_subnet_ids
  ecs_cluster_name                 = var.ecs_cluster_name
  ecs_service_role_arn             = var.ecs_service_role_arn
  cloudflare_zone_id               = var.cloudflare_zone_id
  aws_ecs_task_definition_family   = aws_ecs_task_definition.hasura.family
  aws_ecs_task_definition_revision = aws_ecs_task_definition.hasura.revision
  container_port                   = 8080
  task_definition                  = "${aws_ecs_task_definition.hasura.family}:${aws_ecs_task_definition.hasura.revision}"
}

output "service_url" {
  value = module.service-hasura.url
}