# terraform init -backend-config=backend.hcl -reconfigure
terraform {
  backend "remote" {
  }
}

#################################### Providers ####################################

terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "2.15.0"
    }
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 2.0"
    }
    datadog = {
      source = "DataDog/datadog"
    }
    algolia = {
      source = "philippe-vandermoere/algolia"
    }
    stripe = {
      source = "lukasaron/stripe"
    }
  }
}

data "aws_region" "current" {}
data "aws_caller_identity" "this" {}
data "aws_ecr_authorization_token" "token" {}

locals {
  docker_registry_address = format("%v.dkr.ecr.%v.amazonaws.com", data.aws_caller_identity.this.account_id, data.aws_region.current.name)
}

provider "docker" {
  registry_auth {
    address  = local.docker_registry_address
    username = data.aws_ecr_authorization_token.token.user_name
    password = data.aws_ecr_authorization_token.token.password
  }
}

provider "cloudflare" {
  email   = var.cloudflare_email
  api_key = var.cloudflare_api_key
}

provider "google" {
  project     = var.google_project_id
  region      = var.google_region
  credentials = var.google_credentials
}

provider "aws" {
  region     = var.aws_region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
}

provider "datadog" {
  api_key = var.datadog_api_key
  api_url = var.datadog_api_url
  app_key = var.datadog_app_key
}

provider "algolia" {
  application_id = var.algolia_app_id
  api_key        = var.algolia_admin_api_key
}

provider "stripe" {
  api_key = var.stripe_api_key
}

#################################### Modules ####################################

module "algolia" {
  source                = "./algolia"
  algolia_app_id        = var.algolia_app_id
  algolia_admin_api_key = var.algolia_admin_api_key
  env                   = var.env
}

module "firebase" {
  source                 = "./firebase"
  env                    = var.env
  google_project_id      = var.google_project_id
  google_billing_id      = var.google_billing_id
  google_user            = var.google_user
  google_organization_id = var.google_organization_id
}

module "aws" {
  source                      = "./aws"
  eodhistoricaldata_api_token = var.eodhistoricaldata_api_token
  gnews_api_token             = var.gnews_api_token
  env                         = var.env
  cloudflare_zone_id          = var.cloudflare_zone_id
  hasura_jwt_secret = jsonencode({
    "type"     = "RS256",
    "jwk_url"  = "https://www.googleapis.com/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com",
    "audience" = module.firebase.google_project_id,
    "issuer"   = "https://securetoken.google.com/${module.firebase.google_project_id}"
  })
  base_image_registry_address = var.base_image_registry_address
  base_image_version          = var.base_image_version
  datadog_api_key             = var.datadog_api_key
  datadog_app_key             = var.datadog_app_key
  hubspot_app_token           = var.hubspot_app_token
  polygon_api_token           = var.polygon_api_token
  coingecko_api_key           = var.coingecko_api_key
  github_app_id               = var.github_app_id
  github_app_installation_id  = var.github_app_installation_id
  github_app_private_key      = var.github_app_private_key
  google_places_api_key       = var.google_places_api_key
  source_code_branch          = var.source_code_branch
  source_code_branch_name     = var.source_code_branch_name
  slack_bot_token             = var.slack_bot_token
  amplitude_api_key           = var.amplitude_api_key
  rebrandly_api_key           = var.rebrandly_api_key
  firebase_app_id             = var.firebase_app_id
  firebase_api_secret         = var.firebase_api_secret
  appsflyer_app_id            = var.appsflyer_app_id
  appsflyer_dev_key           = var.appsflyer_dev_key

  stripe_api_key         = var.stripe_api_key
  stripe_publishable_key = var.stripe_publishable_key

  aws_region              = var.aws_region
  aws_access_key          = var.aws_access_key
  aws_secret_key          = var.aws_secret_key
  docker_registry_address = local.docker_registry_address

  pg_production_host                   = var.pg_production_host
  pg_production_port                   = var.pg_production_port
  pg_production_internal_sync_username = var.pg_production_internal_sync_username
  pg_production_internal_sync_password = var.pg_production_internal_sync_password

  plaid_client_id          = var.plaid_client_id
  plaid_secret             = var.plaid_secret
  plaid_development_secret = var.plaid_development_secret
  plaid_sandbox_secret     = var.plaid_sandbox_secret
  plaid_env                = var.plaid_env

  algolia_tickers_index     = module.algolia.algolia_tickers_index
  algolia_collections_index = module.algolia.algolia_collections_index
  algolia_app_id            = var.algolia_app_id
  algolia_indexing_key      = module.algolia.algolia_indexing_key
  algolia_search_key        = module.algolia.algolia_search_key

  drivewealth_is_uat           = var.drivewealth_is_uat
  drivewealth_app_key          = var.drivewealth_app_key
  drivewealth_wlp_id           = var.drivewealth_wlp_id
  drivewealth_parent_ibid      = var.drivewealth_parent_ibid
  drivewealth_ria_id           = var.drivewealth_ria_id
  drivewealth_ria_product_id   = var.drivewealth_ria_product_id
  drivewealth_api_username     = var.drivewealth_api_username
  drivewealth_api_password     = var.drivewealth_api_password
  drivewealth_api_url          = var.drivewealth_api_url
  drivewealth_sqs_arn          = var.drivewealth_sqs_arn
  drivewealth_house_account_no = var.drivewealth_house_account_no

  codeartifact_pipy_url = var.codeartifact_pipy_url
  gainy_compute_version = var.gainy_compute_version
  onesignal_app_id      = var.onesignal_app_id
  onesignal_api_key     = var.onesignal_api_key
  revenuecat_api_key    = var.revenuecat_api_key

  bigquery_google_project = var.google_project_id
  bigquery_credentials    = var.bigquery_credentials

  verification_code_cooldown = var.verification_code_cooldown
  verification_code_ttl      = var.verification_code_ttl

  twilio_verification_service_id = var.twilio_verification_service_id
  twilio_messaging_service_id    = var.twilio_messaging_service_id
  twilio_account_sid             = var.twilio_account_sid
  twilio_auth_token              = var.twilio_auth_token
  sendgrid_api_key               = var.sendgrid_api_key
  sendgrid_from_email            = var.sendgrid_from_email

  billing_value_fee_multiplier      = var.billing_value_fee_multiplier
  billing_min_annual_fee            = var.billing_min_annual_fee
  billing_min_value                 = var.billing_min_value
  billing_enabled_profiles          = var.billing_enabled_profiles
  billing_autosell_enabled_profiles = var.billing_autosell_enabled_profiles
  billing_min_date                  = var.billing_min_date
}

module "datadog" {
  count                           = var.env == "production" ? 1 : 0
  source                          = "./datadog"
  datadog_api_key                 = var.datadog_api_key
  datadog_api_url                 = var.datadog_api_url
  datadog_app_key                 = var.datadog_app_key
  datadog_aws_external_id         = var.datadog_aws_external_id
  env                             = var.env
  additional_forwarded_log_groups = module.aws.additional_forwarded_log_groups
}

module "stripe" {
  source      = "./stripe"
  webhook_url = "https://${module.aws.hasura_url}/api/rest/stripe_webhook"
}

#################################### Outputs ####################################

output "aws_apigatewayv2_api_endpoint" {
  value = module.aws.aws_apigatewayv2_api_endpoint
}
output "aws_rds_db_instance" {
  value = {
    pg_host     = module.aws.aws_rds.db_instance.address
    pg_port     = module.aws.aws_rds.db_instance.port
    pg_username = module.aws.aws_rds.db_instance.username
    pg_dbname   = module.aws.aws_rds.db_instance.name
  }
}
output "vpc_bridge_instance_url" {
  value = module.aws.bridge_instance_url
}
output "meltano_url" {
  value = module.aws.meltano_url
}
output "hasura_url" {
  value = module.aws.hasura_url
}