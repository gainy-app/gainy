resource "aws_cloudwatch_log_group" "hasura" {
  name = "/aws/ecs/hasura-${var.env}"
}

locals {
  hasura_default_params = {
    hasura_enable_console           = var.hasura_enable_console
    hasura_enable_dev_mode          = var.hasura_enable_dev_mode
    hasura_admin_secret             = random_password.hasura.result
    hasura_jwt_secret               = var.hasura_jwt_secret
    aws_lambda_api_gateway_endpoint = var.aws_lambda_api_gateway_endpoint
    hasura_image                    = docker_registry_image.hasura.name
    hasura_memory_credits           = local.hasura_memory_credits
    hasura_cpu_credits              = local.hasura_cpu_credits
    hasura_healthcheck_interval     = local.hasura_healthcheck_interval
    hasura_healthcheck_retries      = local.hasura_healthcheck_retries

    pg_host             = var.pg_host
    pg_password         = var.pg_password
    pg_port             = var.pg_port
    pg_username         = var.pg_username
    pg_dbname           = var.pg_dbname
    pg_replica_uris     = var.pg_replica_uris
    pg_transform_schema = local.public_schema_name
    aws_log_group_name  = aws_cloudwatch_log_group.hasura.name
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
}
