variable "env" {}
variable "eodhistoricaldata_api_token" {}
variable "gnews_api_token" {}
variable "domain" {
  default = "gainy-infra.net"
}
variable "cloudflare_zone_id" {}
variable "hasura_jwt_secret" {}
variable "hubspot_api_key" {}
variable "github_app_id" {}
variable "github_app_installation_id" {}
variable "github_app_private_key" {}
variable "google_places_api_key" {}
variable "slack_bot_token" {}
variable "amplitude_api_key" {}

variable "aws_region" {}
variable "aws_access_key" {}
variable "aws_secret_key" {}
variable "docker_registry_address" {}

variable "base_image_registry_address" {}
variable "base_image_version" {}

variable "datadog_api_key" {}
variable "datadog_app_key" {}

variable "polygon_api_token" {}
variable "polygon_api_token_websockets" {}
variable "coingecko_api_key" {}

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
variable "plaid_sandbox_secret" {}
variable "plaid_env" {}

variable "algolia_tickers_index" {}
variable "algolia_collections_index" {}
variable "algolia_app_id" {}
variable "algolia_indexing_key" {}
variable "algolia_search_key" {}

variable "codeartifact_pipy_url" {}
variable "gainy_compute_version" {}
variable "onesignal_app_id" {}
variable "onesignal_api_key" {}
variable "revenuecat_api_key" {}

variable "stripe_api_key" {}
variable "stripe_publishable_key" {}

variable "bigquery_google_project" {}
variable "bigquery_credentials" {}

variable "drivewealth_is_uat" {}
variable "drivewealth_app_key" {}
variable "drivewealth_wlp_id" {}
variable "drivewealth_parent_ibid" {}
variable "drivewealth_ria_id" {}
variable "drivewealth_ria_product_id" {}
variable "drivewealth_api_username" {}
variable "drivewealth_api_password" {}
variable "drivewealth_api_url" {}
variable "drivewealth_sqs_arn" {}

variable "verification_code_cooldown" {}
variable "verification_code_ttl" {}
variable "twilio_verification_service_id" {}
variable "twilio_messaging_service_id" {}
variable "twilio_account_sid" {}
variable "twilio_auth_token" {}
variable "sendgrid_api_key" {}

variable "source_code_branch" {}
variable "source_code_branch_name" {}
