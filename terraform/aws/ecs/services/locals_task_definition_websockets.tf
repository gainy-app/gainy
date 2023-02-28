resource "aws_cloudwatch_log_group" "websockets" {
  name = "/aws/ecs/websockets-${var.env}"
}

locals {
  websockets_default_params = {
    env                 = var.env
    pg_host             = var.pg_host
    pg_dbname           = var.pg_dbname
    pg_password         = var.pg_password
    pg_port             = var.pg_port
    pg_username         = var.pg_username
    pg_transform_schema = local.public_schema_name
    datadog_api_key     = var.datadog_api_key
    aws_log_group_name  = aws_cloudwatch_log_group.websockets.name
    aws_log_region      = var.aws_log_region
    websockets_image    = docker_registry_image.websockets.name
  }
  websockets_eod_task_description = jsondecode(templatefile(
    "${path.module}/task_definitions/websockets-eod.json",
    merge(local.websockets_default_params, {
      eodhistoricaldata_api_token   = var.eodhistoricaldata_api_token
      eod_websockets_cpu_credits    = local.eod_websockets_cpu_credits
      eod_websockets_memory_credits = local.eod_websockets_memory_credits
      symbols_limit                 = local.eod_symbols_limit
    })
  ))
}
