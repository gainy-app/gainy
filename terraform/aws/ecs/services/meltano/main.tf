locals {
  ecr_repo               = var.repository_name
  meltano_root_dir       = abspath("${path.cwd}/../src/gainy-fetch")
  meltano_image_tag      = format("meltano-%s-%s", var.env, data.archive_file.meltano_source.output_md5)
  meltano_ecr_image_name = format("%v/%v:%v", var.ecr_address, local.ecr_repo, local.meltano_image_tag)
}

/*
 * Create an image
 */
data "archive_file" "meltano_source" {
  type        = "zip"
  source_dir  = local.meltano_root_dir
  output_path = "/tmp/meltano-source.zip"
  excludes    = ["meltano/.meltano"]
}
data "aws_ecr_authorization_token" "token" {}
resource "docker_registry_image" "meltano" {
  name = local.meltano_ecr_image_name
  build {
    context    = local.meltano_root_dir
    dockerfile = "Dockerfile"
    build_args = {
      BASE_IMAGE_REGISTRY_ADDRESS = var.base_image_registry_address
      BASE_IMAGE_VERSION          = var.base_image_version
    }

    auth_config {
      host_name = var.ecr_address
      user_name = data.aws_ecr_authorization_token.token.user_name
      password  = data.aws_ecr_authorization_token.token.password
    }
  }
}
/*
 * Create task definition
 */
resource "random_password" "airflow" {
  length           = 16
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}
resource "aws_ecs_task_definition" "meltano" {
  family                   = "meltano-${var.env}"
  network_mode             = "bridge"
  requires_compatibilities = []
  tags                     = {}
  volume {
    name = "meltano-data"
  }

  container_definitions = templatefile(
    "${path.module}/container-definitions.json",
    {
      env                                  = var.env
      eodhistoricaldata_api_token          = var.eodhistoricaldata_api_token
      pg_host                              = var.pg_host
      pg_password                          = var.pg_password
      pg_port                              = var.pg_port
      pg_username                          = var.pg_username
      pg_dbname                            = var.pg_dbname
      pg_load_schema                       = "raw_data"
      pg_transform_schema                  = "public_${var.versioned_schema_suffix}"
      pg_production_host                   = var.pg_production_host
      pg_production_port                   = var.pg_production_port
      pg_production_internal_sync_username = var.pg_production_internal_sync_username
      pg_production_internal_sync_password = var.pg_production_internal_sync_password
      pg_meltano_schema                    = "meltano"
      pg_airflow_schema                    = "airflow"
      airflow_password                     = random_password.airflow.result
      image                                = docker_registry_image.meltano.name
      aws_log_group_name                   = var.aws_log_group_name
      aws_log_region                       = var.aws_log_region
      airflow_port                         = 5001
      eodhistoricaldata_jobs_count         = var.eodhistoricaldata_jobs_count
      ui_memory_credits                    = var.ui_memory_credits
      scheduler_memory_credits             = var.scheduler_memory_credits
      scheduler_cpu_credits                = var.scheduler_cpu_credits
    }
  )
}
module "service-meltano" {
  source                           = "../"
  name                             = "meltano-airflow"
  container_name                   = "meltano-airflow-ui"
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
  aws_ecs_task_definition_family   = aws_ecs_task_definition.meltano.family
  aws_ecs_task_definition_revision = aws_ecs_task_definition.meltano.revision
  container_port                   = 5001
  task_definition                  = "${aws_ecs_task_definition.meltano.family}:${aws_ecs_task_definition.meltano.revision}"
  minimum_healthy_tasks_percent    = 0
}

output "service_url" {
  value = module.service-meltano.url
}