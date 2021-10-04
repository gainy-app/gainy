# terraform init -backend-config=backend.hcl -reconfigure
terraform {
  backend "remote" {
  }
}

#################################### Providers ####################################

terraform {
  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 2.0"
    }
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


module "firebase" {
  source                 = "./firebase"
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
  hasura_jwt_secret           = var.hasura_jwt_secret
  base_image_prefix           = var.base_image_prefix
  base_image_version          = var.base_image_version
}

#################################### Variables ####################################

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
output "vpc_bridge_instance_domain" {
  value = module.aws.bridge_instance.public_ip
}
output "meltano_url" {
  value = module.aws.meltano_url
}
output "hasura_url" {
  value = module.aws.hasura_url
}