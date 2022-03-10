variable "env" {}
variable "eodhistoricaldata_api_token" {}
variable "gnews_api_token" {}
variable "domain" {
  default = "gainy-infra.net"
}
variable "cloudflare_zone_id" {}
variable "hasura_jwt_secret" {}
variable "hubspot_api_key" {}

variable "aws_region" {}
variable "aws_access_key" {}
variable "aws_secret_key" {}

variable "base_image_registry_address" {}
variable "base_image_version" {}

variable "datadog_api_key" {}
variable "datadog_app_key" {}

variable "polygon_api_token" {}

variable "pg_production_host" {}
variable "pg_production_port" {}
variable "pg_production_internal_sync_username" {}
variable "pg_production_internal_sync_password" {}
variable "pg_analytics_schema" {
  default = "gainy_analytics"
}
variable "pg_website_schema" {
  default = "website"
}

variable "plaid_client_id" {}
variable "plaid_secret" {}
variable "plaid_development_secret" {}
variable "plaid_env" {}

variable "algolia_tickers_index" {}
variable "algolia_collections_index" {}
variable "algolia_app_id" {}
variable "algolia_indexing_key" {}
variable "algolia_search_key" {}

variable "codeartifact_pipy_url" {}
variable "gainy_compute_version" {}

# https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html
locals {
  ecs_instance_type                    = var.env == "production" ? "c5.2xlarge" : "m6i.xlarge"
  meltano_eodhistoricaldata_jobs_count = var.env == "production" ? 4 : 1

  hasura_healthcheck_interval       = var.env == "production" ? 60 : 60
  hasura_healthcheck_retries        = var.env == "production" ? 3 : 3
  health_check_grace_period_seconds = var.env == "production" ? 60 * 10 : 60 * 20

  eod_websockets_memory_credits     = var.env == "production" ? 512 : 128
  polygon_websockets_memory_credits = var.env == "production" ? 768 : 128

  hasura_cpu_credits            = var.env == "production" ? 1024 : 512
  meltano_scheduler_cpu_credits = var.env == "production" ? 3072 : 1024

  hasura_memory_credits            = var.env == "production" ? 2048 : 2048
  meltano_ui_memory_credits        = var.env == "production" ? 1024 : 1024
  meltano_scheduler_memory_credits = var.env == "production" ? 3072 : 4096

  main_cpu_credits    = local.hasura_cpu_credits + local.meltano_scheduler_cpu_credits
  main_memory_credits = local.hasura_memory_credits + local.meltano_ui_memory_credits + local.meltano_scheduler_memory_credits
}