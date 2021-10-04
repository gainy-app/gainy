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

variable "hasura_jwt_secret" {}

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

variable "base_image_prefix" {
  type = string
}
variable "base_image_version" {
  type = string
}
