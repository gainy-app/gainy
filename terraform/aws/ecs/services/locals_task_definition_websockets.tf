resource "aws_cloudwatch_log_group" "this" {
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
    aws_log_group_name  = aws_cloudwatch_log_group.this.name
    aws_log_region      = var.aws_log_region
    websockets_image    = docker_registry_image.websockets.name
  }
  websockets_eod_task_description = jsondecode(templatefile(
    "${path.module}/task_definitions/websockets-eod.json",
    merge(local.websockets_default_params, {
      eodhistoricaldata_api_token   = var.eodhistoricaldata_api_token
      eod_websockets_cpu_credits    = local.eod_websockets_cpu_credits
      eod_websockets_memory_credits = local.eod_websockets_memory_credits
    })
  ))
  websockets_polygon_task_description = jsondecode(templatefile(
    "${path.module}/task_definitions/websockets-polygon.json",
    merge(local.websockets_default_params, {
      polygon_websockets_cpu_credits    = local.polygon_websockets_cpu_credits
      polygon_websockets_memory_credits = local.polygon_websockets_memory_credits
      polygon_api_token                 = var.polygon_api_token
      polygon_realtime_streaming_host   = "delayed.polygon.io" # socket.polygon.io for real-time
    })
  ))
}
