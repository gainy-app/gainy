variable "env" {}
variable "eodhistoricaldata_api_token" {}
variable "gnews_api_token" {}
variable "domain" {
  default = "gainy-infra.net"
}
variable "cloudflare_zone_id" {}
variable "hasura_jwt_secret" {}

variable "base_image_prefix" {}
variable "base_image_version" {}


locals {
  ecs_instance_type                    = var.env == "production" ? "c5.2xlarge" : "t3.medium"
  meltano_eodhistoricaldata_jobs_count = var.env == "production" ? 4 : 1

  hasura_cpu_credits            = var.env == "production" ? 512 : 128
  meltano_scheduler_cpu_credits = var.env == "production" ? 3072 : 256

  hasura_memory_credits            = var.env == "production" ? 1024 : 512
  meltano_ui_memory_credits        = var.env == "production" ? 1024 : 1024
  meltano_scheduler_memory_credits = var.env == "production" ? 3072 : 1024
}