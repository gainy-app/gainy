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
variable "plaid_env" {}

variable "algolia_tickers_index" {}
variable "algolia_collections_index" {}
variable "algolia_app_id" {}
variable "algolia_indexing_key" {}
variable "algolia_search_key" {}

variable "codeartifact_pipy_url" {}
variable "gainy_compute_version" {}
