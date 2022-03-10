locals {
  ecr_repo = var.repository_name

  meltano_build_args = {
    BASE_IMAGE_REGISTRY_ADDRESS = var.base_image_registry_address
    BASE_IMAGE_VERSION          = var.base_image_version
    CODEARTIFACT_PIPY_URL       = var.codeartifact_pipy_url
    GAINY_COMPUTE_VERSION       = var.gainy_compute_version
    MELTANO_SOURCE_MD5          = data.archive_file.meltano_source.output_md5
  }

  meltano_root_dir       = abspath("${path.cwd}/../src/gainy-fetch")
  meltano_image_tag      = format("meltano-%s-%s-%s", var.env, var.base_image_version, md5(jsonencode(local.meltano_build_args)))
  meltano_ecr_image_name = format("%v/%v:%v", var.ecr_address, local.ecr_repo, local.meltano_image_tag)

  hasura_root_dir       = abspath("${path.cwd}/../src/hasura")
  hasura_image_tag      = format("hasura-%s-%s-%s", var.env, var.base_image_version, data.archive_file.hasura_source.output_md5)
  hasura_ecr_image_name = format("%v/%v:%v", var.ecr_address, local.ecr_repo, local.hasura_image_tag)

  websockets_root_dir       = abspath("${path.cwd}/../src/websockets")
  websockets_image_tag      = format("websockets-%s-%s", var.env, data.archive_file.websockets_source.output_md5)
  websockets_ecr_image_name = format("%v/%v:%v", var.ecr_address, local.ecr_repo, local.websockets_image_tag)

  public_schema_name = "public_${var.versioned_schema_suffix}"

  hasura_default_params = {
    hasura_enable_console           = var.hasura_enable_console
    hasura_enable_dev_mode          = var.hasura_enable_dev_mode
    hasura_admin_secret             = random_password.hasura.result
    hasura_jwt_secret               = var.hasura_jwt_secret
    aws_lambda_api_gateway_endpoint = "${var.aws_lambda_api_gateway_endpoint}/${var.deployment_key}"
    hasura_image                    = docker_registry_image.hasura.name
    hasura_memory_credits           = var.hasura_memory_credits
    hasura_cpu_credits              = var.hasura_cpu_credits
    hasura_healthcheck_interval     = var.hasura_healthcheck_interval
    hasura_healthcheck_retries      = var.hasura_healthcheck_retries

    pg_host             = var.pg_host
    pg_password         = var.pg_password
    pg_port             = var.pg_port
    pg_username         = var.pg_username
    pg_dbname           = var.pg_dbname
    pg_replica_uris     = var.pg_replica_uris
    pg_transform_schema = local.public_schema_name
    aws_log_group_name  = var.aws_log_group_name
    aws_log_region      = var.aws_log_region
  }
  hasura_task_description = jsondecode(templatefile(
    "${path.module}/task_definitions/hasura.json",
    local.hasura_default_params
  ))
  hasura_replica_task_description = jsondecode(templatefile(
    "${path.module}/task_definitions/hasura.json",
    merge(local.hasura_default_params, {
      hasura_healthcheck_interval = 30
      hasura_healthcheck_retries  = 2
    })
  ))

  meltano_default_params = {
    env                          = var.env
    pg_host                      = var.pg_host
    pg_dbname                    = var.pg_dbname
    pg_password                  = var.pg_password
    pg_port                      = var.pg_port
    pg_username                  = var.pg_username
    pg_meltano_schema            = "meltano"
    pg_airflow_schema            = "airflow"
    airflow_password             = random_password.airflow.result
    airflow_port                 = 5001
    eodhistoricaldata_jobs_count = var.eodhistoricaldata_jobs_count
    pg_transform_schema          = local.public_schema_name
    meltano_image                = docker_registry_image.meltano.name
    aws_log_group_name           = var.aws_log_group_name
    aws_log_region               = var.aws_log_region
  }
  meltano_airflow_ui_task_description = jsondecode(templatefile(
    "${path.module}/task_definitions/meltano-airflow-ui.json",
    merge(local.meltano_default_params, {
      airflow_ui_memory_credits = var.ui_memory_credits
    })
  ))
  meltano_airflow_scheduler_description = jsondecode(templatefile(
    "${path.module}/task_definitions/meltano-airflow-scheduler.json",
    merge(local.meltano_default_params, {
      eodhistoricaldata_api_token          = var.eodhistoricaldata_api_token
      eodhistoricaldata_exchanges          = jsonencode(["US", "CC", "INDX"])
      pg_load_schema                       = "raw_data"
      dbt_threads                          = var.env == "production" ? 4 : 4
      pg_production_host                   = var.pg_production_host
      pg_production_port                   = var.pg_production_port
      pg_production_internal_sync_username = var.pg_production_internal_sync_username
      pg_production_internal_sync_password = var.pg_production_internal_sync_password
      airflow_scheduler_memory_credits     = var.scheduler_memory_credits
      airflow_scheduler_cpu_credits        = var.scheduler_cpu_credits
      algolia_tickers_index                = var.algolia_tickers_index
      algolia_collections_index            = var.algolia_collections_index
      algolia_app_id                       = var.algolia_app_id
      algolia_indexing_key                 = var.algolia_indexing_key

      pg_external_access_host     = var.pg_external_access_host
      pg_external_access_port     = var.pg_external_access_port
      pg_external_access_username = var.pg_external_access_username
      pg_external_access_password = var.pg_external_access_password
      pg_external_access_dbname   = var.pg_external_access_dbname
      pg_analytics_schema         = var.pg_analytics_schema
      pg_website_schema           = var.pg_website_schema

      datadog_api_key = var.datadog_api_key
      datadog_app_key = var.datadog_app_key

      polygon_api_token = var.polygon_api_token

      # mlflow
      aws_region               = var.aws_region
      aws_access_key           = var.aws_access_key
      aws_secret_key           = var.aws_secret_key
      mlflow_artifact_location = "s3://${var.mlflow_artifact_bucket}"
      pg_mlflow_schema         = "mlflow"
    })
  ))

  websockets_default_params = {
    env                 = var.env
    pg_host             = var.pg_host
    pg_dbname           = var.pg_dbname
    pg_password         = var.pg_password
    pg_port             = var.pg_port
    pg_username         = var.pg_username
    pg_transform_schema = local.public_schema_name
    datadog_api_key     = var.datadog_api_key
    aws_log_group_name  = var.aws_log_group_name
    aws_log_region      = var.aws_log_region
    websockets_image    = docker_registry_image.websockets.name
  }
  websockets_eod_task_description = jsondecode(templatefile(
    "${path.module}/task_definitions/websockets-eod.json",
    merge(local.websockets_default_params, {
      eodhistoricaldata_api_token   = var.eodhistoricaldata_api_token
      eod_websockets_memory_credits = var.eod_websockets_memory_credits
    })
  ))
  websockets_polygon_task_description = jsondecode(templatefile(
    "${path.module}/task_definitions/websockets-polygon.json",
    merge(local.websockets_default_params, {
      polygon_websockets_memory_credits = var.polygon_websockets_memory_credits
      polygon_api_token                 = var.polygon_api_token
      polygon_realtime_streaming_host   = "delayed.polygon.io" # socket.polygon.io for real-time
    })
  ))
}

resource "random_password" "hasura" {
  length           = 16
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

resource "random_password" "airflow" {
  length           = 16
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

###### Create images #####

data "archive_file" "meltano_source" {
  type        = "zip"
  source_dir  = local.meltano_root_dir
  output_path = "/tmp/meltano-source.zip"
  excludes    = ["meltano/.meltano"]
}
data "archive_file" "hasura_source" {
  type        = "zip"
  source_dir  = local.hasura_root_dir
  output_path = "/tmp/hasura-source.zip"
}
data "archive_file" "websockets_source" {
  type        = "zip"
  source_dir  = local.websockets_root_dir
  output_path = "/tmp/websockets-source.zip"
}
data "aws_ecr_authorization_token" "token" {}
resource "docker_registry_image" "meltano" {
  name = local.meltano_ecr_image_name
  build {
    context    = local.meltano_root_dir
    dockerfile = "Dockerfile"
    build_args = local.meltano_build_args

    auth_config {
      host_name = var.ecr_address
      user_name = data.aws_ecr_authorization_token.token.user_name
      password  = data.aws_ecr_authorization_token.token.password
    }
  }

  lifecycle {
    ignore_changes = [build["context"]]
  }
}
resource "docker_registry_image" "hasura" {
  name = local.hasura_ecr_image_name
  build {
    context    = local.hasura_root_dir
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

  lifecycle {
    ignore_changes = [build["context"]]
  }
}
resource "docker_registry_image" "websockets" {
  name = local.websockets_ecr_image_name
  build {
    context    = local.websockets_root_dir
    dockerfile = "Dockerfile"
  }

  lifecycle {
    ignore_changes = [build["context"]]
  }
}

###### Create execution role ######

resource "aws_iam_role" "execution" {
  name               = "ecs-gainy-execution-role-${var.env}"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_policy" "fargate_execution" {
  name        = "ecs-gainy-execution-policy-${var.env}"
  description = "ECS Gainy Exec Policy ${var.env}"

  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        "Resource" : "*"
      }
    ]
  })
}
resource "aws_iam_role_policy_attachment" "iam_role_policy_attachment_default" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}
resource "aws_iam_role_policy_attachment" "iam_role_policy_attachment_custom" {
  role       = aws_iam_role.execution.name
  policy_arn = aws_iam_policy.fargate_execution.arn
}

###### Create task definitions ######
resource "aws_ecs_task_definition" "default" {
  family                   = "gainy-${var.env}"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.main_cpu_credits
  memory                   = var.main_memory_credits
  tags                     = {}
  execution_role_arn       = aws_iam_role.execution.arn

  volume {
    name = "meltano-data"
  }

  container_definitions = jsonencode([
    local.hasura_task_description,
    local.meltano_airflow_ui_task_description,
    local.meltano_airflow_scheduler_description,
  ])
}

resource "aws_ecs_task_definition" "hasura" {
  family                   = "gainy-hasura-${var.env}"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.hasura_cpu_credits
  memory                   = var.hasura_memory_credits
  tags                     = {}
  execution_role_arn       = aws_iam_role.execution.arn

  container_definitions = jsonencode([
    local.hasura_replica_task_description
  ])
}

module "meltano-elb" {
  source                           = "./elb"
  name                             = "meltano-airflow"
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
  aws_ecs_task_definition_family   = aws_ecs_task_definition.default.family
  aws_ecs_task_definition_revision = aws_ecs_task_definition.default.revision
}

module "hasura-elb" {
  source                           = "./elb"
  name                             = "hasura"
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
  aws_ecs_task_definition_family   = aws_ecs_task_definition.default.family
  aws_ecs_task_definition_revision = aws_ecs_task_definition.default.revision
}

/*
 * Create ECS Service
 */
resource "aws_ecs_service" "service" {
  name                               = "gainy-${var.env}"
  cluster                            = var.ecs_cluster_name
  desired_count                      = 1
  deployment_maximum_percent         = 200
  deployment_minimum_healthy_percent = 100
  launch_type                        = "FARGATE"
  health_check_grace_period_seconds  = var.health_check_grace_period_seconds
  enable_execute_command             = true

  load_balancer {
    target_group_arn = module.meltano-elb.aws_alb_target_group.arn
    container_name   = "meltano-airflow-ui"
    container_port   = 5001
  }

  load_balancer {
    target_group_arn = module.hasura-elb.aws_alb_target_group.arn
    container_name   = "hasura"
    container_port   = 8080
  }

  network_configuration {
    subnets = var.private_subnet_ids
  }

  task_definition      = "${aws_ecs_task_definition.default.family}:${aws_ecs_task_definition.default.revision}"
  force_new_deployment = true
}

/*
 * Create Hasura autoscaling service
 */
resource "aws_ecs_service" "hasura" {
  count                              = var.env == "production" ? 1 : 0
  name                               = "gainy-hasura-${var.env}"
  cluster                            = var.ecs_cluster_name
  desired_count                      = 0
  deployment_maximum_percent         = 200
  deployment_minimum_healthy_percent = 100
  launch_type                        = "FARGATE"
  health_check_grace_period_seconds  = var.health_check_grace_period_seconds

  load_balancer {
    target_group_arn = module.hasura-elb.aws_alb_target_group.arn
    container_name   = "hasura"
    container_port   = 8080
  }

  network_configuration {
    subnets = var.private_subnet_ids
  }

  task_definition      = "${aws_ecs_task_definition.hasura.family}:${aws_ecs_task_definition.hasura.revision}"
  force_new_deployment = true

  lifecycle {
    ignore_changes = [desired_count]
  }
}
resource "aws_appautoscaling_target" "hasura" {
  count              = var.env == "production" ? 1 : 0
  max_capacity       = 4
  min_capacity       = 1
  resource_id        = "service/${var.ecs_cluster_name}/${aws_ecs_service.hasura[0].name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "ecs_policy" {
  count              = var.env == "production" ? 1 : 0
  name               = "policy-gainy-hasura-${var.env}"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.hasura[0].resource_id
  scalable_dimension = aws_appautoscaling_target.hasura[0].scalable_dimension
  service_namespace  = aws_appautoscaling_target.hasura[0].service_namespace

  target_tracking_scaling_policy_configuration {
    target_value       = 20000
    scale_in_cooldown  = 60
    scale_out_cooldown = 60

    predefined_metric_specification {
      predefined_metric_type = "ALBRequestCountPerTarget"
      resource_label         = "${module.hasura-elb.aws_alb.arn_suffix}/${module.hasura-elb.aws_alb_target_group.arn_suffix}"
    }
  }
}

output "meltano_url" {
  value = module.meltano-elb.url
}

output "hasura_url" {
  value = module.hasura-elb.url
}

output "hasura_admin_secret" {
  value     = random_password.hasura.result
  sensitive = true
}

output "name" {
  value = aws_ecs_service.service.name
}

output "public_schema_name" {
  value = local.public_schema_name
}