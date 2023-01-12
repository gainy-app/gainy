resource "aws_cloudwatch_log_group" "meltano" {
  name = "/aws/ecs/meltano-${var.env}"
}
locals {
  meltano_default_params = {
    env                                 = var.env
    pg_host                             = var.pg_host
    pg_dbname                           = var.pg_dbname
    pg_password                         = var.pg_password
    pg_port                             = var.pg_port
    pg_username                         = var.pg_username
    pg_meltano_schema                   = "meltano"
    pg_airflow_schema                   = "airflow"
    airflow_password                    = random_password.airflow.result
    airflow_port                        = 5001
    upstream_pool_size                  = local.upstream_pool_size
    downstream_pool_size                = local.downstream_pool_size
    eodhistoricaldata_jobs_count        = local.eodhistoricaldata_jobs_count
    eodhistoricaldata_prices_jobs_count = local.eodhistoricaldata_prices_jobs_count
    coingecko_jobs_count                = local.coingecko_jobs_count
    polygon_jobs_count                  = local.polygon_jobs_count
    pg_transform_schema                 = local.public_schema_name
    meltano_image                       = docker_registry_image.meltano.name
    aws_log_group_name                  = aws_cloudwatch_log_group.meltano.name
    aws_log_region                      = var.aws_log_region
    aws_lambda_api_gateway_endpoint     = var.aws_lambda_api_gateway_endpoint

    drivewealth_is_uat         = var.drivewealth_is_uat
    drivewealth_app_key        = var.drivewealth_app_key
    drivewealth_wlp_id         = var.drivewealth_wlp_id
    drivewealth_parent_ibid    = var.drivewealth_parent_ibid
    drivewealth_ria_id         = var.drivewealth_ria_id
    drivewealth_ria_product_id = var.drivewealth_ria_product_id
    drivewealth_api_username   = var.drivewealth_api_username
    drivewealth_api_password   = var.drivewealth_api_password
    drivewealth_api_url        = var.drivewealth_api_url
  }
  airflow_params = merge(local.meltano_default_params, {
    airflow_ui_cpu_credits    = local.airflow_cpu_credits
    airflow_ui_memory_credits = local.airflow_memory_credits
  })
  scheduler_params = merge(local.meltano_default_params, {
    eodhistoricaldata_api_token          = var.eodhistoricaldata_api_token
    eodhistoricaldata_exchanges          = jsonencode(["NASDAQ", "NYSE", "CC", "INDX"])
    polygon_crypto_symbols               = jsonencode(["CRVUSD"])
    polygon_stock_exchanges              = jsonencode(["XNAS", "XNYS"])
    pg_load_schema                       = "raw_data"
    dbt_threads                          = 3
    pg_production_host                   = var.pg_production_host
    pg_production_port                   = var.pg_production_port
    pg_production_internal_sync_username = var.pg_production_internal_sync_username
    pg_production_internal_sync_password = var.pg_production_internal_sync_password
    algolia_tickers_index                = var.algolia_tickers_index
    algolia_collections_index            = var.algolia_collections_index
    algolia_app_id                       = var.algolia_app_id
    algolia_indexing_key                 = var.algolia_indexing_key
    onesignal_app_id                     = var.onesignal_app_id
    onesignal_api_key                    = var.onesignal_api_key
    onesignal_segments_production        = jsonencode(["Subscribed Users"])
    onesignal_segments_test              = jsonencode(["Testers"])
    gainy_history_s3_bucket              = var.gainy_history_s3_bucket
    pg_datadog_password                  = var.pg_datadog_password
    pg_internal_sync_username            = var.pg_production_internal_sync_username
    pg_internal_sync_password            = var.pg_internal_sync_password
    github_app_id                        = var.github_app_id
    github_app_installation_id           = var.github_app_installation_id
    github_app_private_key               = var.github_app_private_key

    pg_external_access_host     = var.pg_external_access_host
    pg_external_access_port     = var.pg_external_access_port
    pg_external_access_username = var.pg_external_access_username
    pg_external_access_password = var.pg_external_access_password
    pg_external_access_dbname   = var.pg_external_access_dbname
    pg_analytics_schema         = var.pg_analytics_schema
    pg_website_schema           = var.pg_website_schema

    bigquery_google_project = var.bigquery_google_project
    bigquery_target_schema  = "gainyapp_integration_${var.env}"
    bigquery_location       = "US"
    bigquery_credentials    = var.bigquery_credentials

    datadog_api_key = var.datadog_api_key
    datadog_app_key = var.datadog_app_key

    polygon_api_token = var.polygon_api_token
    coingecko_api_key = var.coingecko_api_key

    plaid_client_id          = var.plaid_client_id
    plaid_secret             = var.plaid_secret
    plaid_development_secret = var.plaid_development_secret
    plaid_sandbox_secret     = var.plaid_sandbox_secret
    plaid_env                = var.plaid_env

    # mlflow
    aws_region               = var.aws_region
    aws_access_key           = var.aws_access_key
    aws_secret_key           = var.aws_secret_key
    mlflow_artifact_location = "s3://${var.mlflow_artifact_bucket}"
    pg_mlflow_schema         = "mlflow"
  })

  airflow_task_description = jsondecode(templatefile(
    "${path.module}/task_definitions/meltano-airflow-ui.json",
    local.airflow_params
  ))
  meltano_scheduler_description = merge(
    jsondecode(templatefile("${path.module}/task_definitions/meltano-airflow-scheduler.json", local.scheduler_params)),
    {
      name       = "meltano-airflow-scheduler"
      essential  = true
      entrypoint = ["meltano"]
      command    = ["invoke", "airflow", "scheduler"]
      healthCheck = {
        "command" : ["CMD-SHELL", "nc -z localhost 8793"],
        "interval" : 10,
        "retries" : 2
      }
      dependsOn = [
        { "condition" : "SUCCESS", "containerName" : "meltano-airflow-initializer" }
      ]
    }
  )

  meltano_initializer_description = merge(
    jsondecode(templatefile("${path.module}/task_definitions/meltano-airflow-scheduler.json", local.scheduler_params)),
    {
      name        = "meltano-airflow-initializer"
      essential   = false
      entrypoint  = ["/init.sh"]
      command     = []
      healthCheck = null
      dependsOn   = []
    }
  )
}
