variable "repository_name" {}
variable "env" {}
variable "ecr_address" {}
variable "aws_log_group_name" {}
variable "aws_log_region" {}
variable "eodhistoricaldata_api_token" {}
variable "pg_host" {}
variable "pg_port" {}
variable "pg_username" {}
variable "pg_password" {}
variable "pg_dbname" {}
variable "pg_replica_uris" {}
variable "pg_production_host" {}
variable "pg_production_port" {}
variable "pg_production_internal_sync_username" {}
variable "pg_production_internal_sync_password" {}
variable "vpc_id" {}
variable "vpc_default_sg_id" {}
variable "public_https_sg_id" {}
variable "public_http_sg_id" {}
variable "public_subnet_ids" {}
variable "ecs_cluster_name" {}
variable "ecs_service_role_arn" {}
variable "cloudflare_zone_id" {}
variable "domain" {}
variable "eodhistoricaldata_jobs_count" {}
variable "ui_memory_credits" {}
variable "scheduler_memory_credits" {}
variable "scheduler_cpu_credits" {}
variable "hasura_enable_console" {}
variable "hasura_enable_dev_mode" {}
variable "hasura_jwt_secret" {}
variable "aws_lambda_api_gateway_endpoint" {}
variable "deployment_key" {}
variable "hasura_memory_credits" {}
variable "hasura_cpu_credits" {}
variable "hasura_healthcheck_interval" {}
variable "hasura_healthcheck_retries" {}
variable "base_image_registry_address" {}
variable "base_image_version" {}
variable "versioned_schema_suffix" {}
variable "algolia_tickers_index" {}
variable "algolia_collections_index" {}
variable "algolia_app_id" {}
variable "algolia_indexing_key" {}
variable "eod_websockets_memory_credits" {}
variable "polygon_websockets_memory_credits" {}
variable "datadog_api_key" {}
variable "datadog_app_key" {}
variable "polygon_api_token" {}
variable "health_check_grace_period_seconds" {}
variable "private_subnet_ids" {}
variable "pg_external_access_host" {}
variable "pg_external_access_port" {}
variable "pg_external_access_username" {}
variable "pg_external_access_password" {}
variable "pg_external_access_dbname" {}
variable "pg_analytics_schema" {}
variable "pg_website_schema" {}
