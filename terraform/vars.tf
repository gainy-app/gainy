#################################### Application ####################################

variable "env" {
  type = string
}
variable "eodhistoricaldata_api_token" {
  type      = string
  sensitive = true
}
variable "gnews_api_token" {
  type      = string
  sensitive = true
}
variable "base_image_registry_address" {
  type = string
}
variable "base_image_version" {
  type = string
}
variable "hubspot_api_key" {
  type      = string
  sensitive = true
}
variable "revenuecat_api_key" {
  type      = string
  sensitive = true
}
variable "verification_code_cooldown" {
  type    = number
  default = 30
}
variable "verification_code_ttl" {
  type    = number
  default = 300
}

#################################### OneSignal ####################################

variable "onesignal_app_id" {
  type = string
}
variable "onesignal_api_key" {
  type      = string
  sensitive = true
}

#################################### Stripe ####################################

variable "stripe_api_key" {
  type      = string
  sensitive = true
}
variable "stripe_publishable_key" {
  type = string
}

#################################### GitHub ####################################

variable "github_app_id" {
  type = string
}
variable "github_app_installation_id" {
  type = string
}
variable "github_app_private_key" {
  type      = string
  sensitive = true
}

#################################### DriveWealth ####################################

variable "drivewealth_is_uat" {
  type    = string
  default = "true"
}
variable "drivewealth_app_key" {
  type      = string
  sensitive = true
}
variable "drivewealth_wlp_id" {
  type      = string
  sensitive = true
}
variable "drivewealth_parent_ibid" {
  type      = string
  sensitive = true
}
variable "drivewealth_ria_id" {
  type      = string
  sensitive = true
}
variable "drivewealth_ria_product_id" {
  type      = string
  sensitive = true
}
variable "drivewealth_api_username" {
  type      = string
  sensitive = true
}
variable "drivewealth_api_password" {
  type      = string
  sensitive = true
}
variable "drivewealth_api_url" {
  type      = string
  sensitive = true
}
variable "drivewealth_sqs_arn" {
  type = string
}

#################################### AWS ####################################

variable "aws_region" {
  type    = string
  default = "us-east-1"
}
variable "aws_access_key" {
  sensitive = true
}
variable "aws_secret_key" {
  sensitive = true
}
variable "codeartifact_pipy_url" {
  type = string
}
variable "gainy_compute_version" {
  type = string
}

#################################### Google Cloud & Firebase ####################################

variable "google_project_id" {
  default = "gainyapp"
}
variable "google_region" {
  default = "us-central1"
}
variable "google_credentials" {
  type      = string
  sensitive = true
}
variable "bigquery_credentials" {
  type      = string
  sensitive = true
}
variable "google_billing_id" {}
variable "google_user" {}
variable "google_organization_id" {}
variable "google_places_api_key" {
  type      = string
  sensitive = true
}

#################################### Cloudflare ####################################

variable "cloudflare_email" {
  type = string
}
variable "cloudflare_api_key" {
  type      = string
  sensitive = true
}

variable "cloudflare_zone_id" {
  type      = string
  sensitive = true
}

#################################### Datadog ####################################

variable "datadog_api_key" {
  type      = string
  sensitive = true
}
variable "datadog_app_key" {
  type      = string
  sensitive = true
}
variable "datadog_aws_external_id" {
  type      = string
  sensitive = true
}
variable "datadog_api_url" {
  type = string
}

#################################### Polygon ####################################

variable "polygon_api_token" {
  type      = string
  sensitive = true
}

#################################### CoinGecko ####################################

variable "coingecko_api_key" {
  type      = string
  sensitive = true
}

#################################### Algolia ####################################

variable "algolia_app_id" {
  type = string
}
variable "algolia_admin_api_key" {
  type      = string
  sensitive = true
}

#################################### Meltano internal data sync with production ####################################

variable "pg_production_host" {
  type      = string
  sensitive = true
}
variable "pg_production_port" {
  type      = string
  sensitive = true
}
variable "pg_production_internal_sync_username" {
  type = string
}
variable "pg_production_internal_sync_password" {
  type      = string
  sensitive = true
}

#################################### Plaid ####################################

variable "plaid_client_id" {
  type      = string
  sensitive = true
}
variable "plaid_secret" {
  type      = string
  sensitive = true
}
variable "plaid_development_secret" {
  type      = string
  sensitive = true
}
variable "plaid_sandbox_secret" {
  type      = string
  sensitive = true
}
variable "plaid_env" {
  type = string
}

#################################### Twilio ####################################

variable "twilio_verification_service_id" {
  type      = string
  sensitive = true
}
variable "twilio_account_sid" {
  type      = string
  sensitive = true
}
variable "twilio_auth_token" {
  type      = string
  sensitive = true
}
