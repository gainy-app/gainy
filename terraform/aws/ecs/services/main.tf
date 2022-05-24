resource "time_static" "meltano_changed" {
  triggers = {
    meltano_transform_source_md5 = data.archive_file.meltano_transform_source.output_md5
    meltano_seed_source_md5      = data.archive_file.meltano_seed_source.output_md5
  }
}

locals {
  public_schema_name                = "public_${formatdate("YYMMDDhhmmss", time_static.meltano_changed.rfc3339)}"
  hasura_healthcheck_interval       = var.env == "production" ? 60 : 60
  hasura_healthcheck_retries        = var.env == "production" ? 3 : 3
  health_check_grace_period_seconds = var.env == "production" ? 60 * 10 : 60 * 20
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
data "archive_file" "meltano_transform_source" {
  type        = "zip"
  source_dir  = local.meltano_transform_root_dir
  output_path = "/tmp/meltano-transform_source.zip"
  excludes    = ["meltano/.meltano"]
}
data "archive_file" "meltano_seed_source" {
  type        = "zip"
  source_dir  = local.meltano_seed_root_dir
  output_path = "/tmp/meltano-seed_source.zip"
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
      host_name = var.docker_registry_address
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
      host_name = var.docker_registry_address
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
    build_args = local.websockets_build_args
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

resource "aws_iam_role" "task" {
  name               = "ecs-gainy-task-role-${var.env}"
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

###### Create task definitions ######

resource "aws_efs_file_system" "meltano_logs" {
  creation_token = "gainy-meltano-logs-${var.env}"

  tags = {
    Name = "gainy-meltano-logs-${var.env}"
  }
  lifecycle_policy {
    transition_to_ia = "AFTER_7_DAYS"
  }
}
resource "aws_efs_mount_target" "meltano_data_private_subnet" {
  for_each       = toset(var.private_subnet_ids)
  file_system_id = aws_efs_file_system.meltano_logs.id
  subnet_id      = each.value
}
resource "aws_ecs_task_definition" "default" {
  family                   = "gainy-${var.env}"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = local.main_cpu_credits
  memory                   = local.main_memory_credits
  tags                     = {}
  task_role_arn            = aws_iam_role.task.arn
  execution_role_arn       = aws_iam_role.execution.arn

  volume {
    name = "meltano-data"
  }
  volume {
    name = "meltano-logs"
    efs_volume_configuration {
      file_system_id = aws_efs_file_system.meltano_logs.id
    }
  }

  container_definitions = jsonencode(
    concat(
      [
        local.hasura_task_description,
        local.meltano_airflow_ui_task_description,
        local.meltano_airflow_scheduler_description,
      ],
      var.env == "production" ? [
        local.websockets_eod_task_description,
        local.websockets_polygon_task_description,
      ] : []
    )
  )
}

resource "aws_ecs_task_definition" "hasura" {
  family                   = "gainy-hasura-${var.env}"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = local.hasura_cpu_credits
  memory                   = local.hasura_memory_credits
  tags                     = {}
  task_role_arn            = aws_iam_role.task.arn
  execution_role_arn       = aws_iam_role.execution.arn

  container_definitions = jsonencode([
    local.hasura_replica_task_description
  ])
}

###### Create ALB ######

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
  cloudflare_zone_id               = var.cloudflare_zone_id
  aws_ecs_task_definition_family   = aws_ecs_task_definition.default.family
  aws_ecs_task_definition_revision = aws_ecs_task_definition.default.revision
}

/*
 * Create the main service
 */
resource "aws_ecs_service" "service" {
  name                               = "gainy-${var.env}"
  cluster                            = var.ecs_cluster_name
  desired_count                      = 1
  deployment_maximum_percent         = 200
  deployment_minimum_healthy_percent = 100
  launch_type                        = "FARGATE"
  health_check_grace_period_seconds  = local.health_check_grace_period_seconds
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
  health_check_grace_period_seconds  = local.health_check_grace_period_seconds

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

###### Output ######

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

output "additional_forwarded_log_groups" {
  value = [
    aws_cloudwatch_log_group.hasura.name,
    aws_cloudwatch_log_group.meltano.name,
    aws_cloudwatch_log_group.websockets.name,
  ]
}
