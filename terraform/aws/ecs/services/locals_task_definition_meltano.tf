resource "aws_cloudwatch_log_group" "meltano" {
  name = "/aws/ecs/meltano-${var.env}"
}

locals {
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
    eodhistoricaldata_jobs_count = local.eodhistoricaldata_jobs_count
    pg_transform_schema          = local.public_schema_name
    meltano_image                = docker_registry_image.meltano.name
    aws_log_group_name           = aws_cloudwatch_log_group.meltano.name
    aws_log_region               = var.aws_log_region
  }
  meltano_airflow_ui_task_description = jsondecode(templatefile(
    "${path.module}/task_definitions/meltano-airflow-ui.json",
    merge(local.meltano_default_params, {
      airflow_ui_cpu_credits    = local.meltano_ui_cpu_credits
      airflow_ui_memory_credits = local.meltano_ui_memory_credits
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
      airflow_scheduler_memory_credits     = local.meltano_scheduler_memory_credits
      airflow_scheduler_cpu_credits        = local.meltano_scheduler_cpu_credits
      algolia_tickers_index                = var.algolia_tickers_index
      algolia_collections_index            = var.algolia_collections_index
      algolia_app_id                       = var.algolia_app_id
      algolia_indexing_key                 = var.algolia_indexing_key
      aws_lambda_api_gateway_endpoint      = var.aws_lambda_api_gateway_endpoint

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
}
