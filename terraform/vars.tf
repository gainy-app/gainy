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
variable "google_billing_id" {}
variable "google_user" {}
variable "google_organization_id" {}

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
variable "datadog_api_url" {
  type = string
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
variable "plaid_env" {
  type = string
}

