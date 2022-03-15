variable "datadog_api_key" {}
variable "datadog_api_url" {}
variable "datadog_app_key" {}
variable "datadog_aws_external_id" {}
variable "additional_forwarded_log_groups" {
  default = []
}

variable "slack_account_name" {
  default = "Gainy"
}
variable "slack_channel_name" {
  default = "alerts"
}

variable "env" {}
