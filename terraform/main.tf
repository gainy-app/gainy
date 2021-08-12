variable "env" {
  default = "dev"
  type    = string
}

variable "eodhistoricaldata_api_token" {
  type      = string
  sensitive = true
}

provider "aws" {}

output "aws_apigatewayv2_api_endpoint" {
  value = module.aws.aws_apigatewayv2_api_endpoint
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

provider "google" {
  project     = var.google_project_id
  region      = var.google_region
  credentials = var.google_credentials
}

terraform {
  backend "remote" {
    organization = "gainy"

    workspaces {
      name = "gainy-dev"
    }
  }
}

module "networking" {
  source = "./networking"
  env    = var.env
}

resource "random_password" "hasura_secret" {
  length = 16
}

module "rds" {
  env                 = var.env
  name                = "gainy"
  source              = "./rds"
  subnets             = module.networking.vpc.database_subnets
  allowed_cidrs       = module.networking.vpc.private_subnets
  security_group      = module.networking.vpc.default_security_group_id
  vpc_id              = module.networking.vpc.vpc_id
  publicly_accessible = true
}

module "heroku-gainy-managed" {
  source = "./heroku"
  name   = "gainy-managed"
  env    = "dev"
  path   = "src/hasura"
  stack  = "container"
  config = {
    HASURA_GRAPHQL_DATABASE_URL   = "postgres://${module.rds.db.db_instance_username}:${module.rds.db.db_master_password}@${module.rds.db.db_instance_endpoint}/${module.rds.db.db_instance_name}"
    HASURA_GRAPHQL_ADMIN_SECRET   = random_password.hasura_secret.result
    HASURA_GRAPHQL_ENABLE_CONSOLE = "true"
    HASURA_GRAPHQL_DEV_MODE       = true
    ## Heroku hobby tier PG has few limitations including 20 max connections
    ## https://devcenter.heroku.com/articles/heroku-postgres-plans#hobby-tier
    HASURA_GRAPHQL_PG_CONNECTIONS    = 15
    HASURA_GRAPHQL_UNAUTHORIZED_ROLE = "anonymous"
    AWS_LAMBDA_API_GATEWAY_ENDPOINT  = module.aws.aws_apigatewayv2_api_endpoint
  }
}

module "heroku-gainy-fetch" {
  source = "./heroku"
  stack  = "container"
  name   = "gainy-fetch"
  env    = "dev"
  path   = "src/gainy-fetch"
  config = {
    TARGET_POSTGRES_HOST            = module.rds.db.db_instance_address
    TARGET_POSTGRES_PORT            = module.rds.db.db_instance_port
    TARGET_POSTGRES_USER            = module.rds.db.db_instance_username
    TARGET_POSTGRES_PASSWORD        = module.rds.db.db_master_password
    TARGET_POSTGRES_DBNAME          = module.rds.db.db_instance_name
    TARGET_POSTGRES_SCHEMA          = "public"
    TAP_POSTGRES_FILTER_SCHEMAS     = "public"
    TAP_EODHISTORICALDATA_API_TOKEN = var.eodhistoricaldata_api_token
    TAP_EODHISTORICALDATA_SYMBOLS   = "[\"AAPL\"]"
    MELTANO_DATABASE_URI            = "postgresql://${module.rds.db.db_instance_username}:${module.rds.db.db_master_password}@${module.rds.db.db_instance_endpoint}/${module.rds.db.db_instance_name}?options=-csearch_path%3Dmeltano"
    PG_DATABASE                     = module.rds.db.db_instance_name
    PG_ADDRESS                      = module.rds.db.db_instance_address
    PG_PASSWORD                     = module.rds.db.db_master_password
    PG_PORT                         = module.rds.db.db_instance_port
    DBT_TARGET_SCHEMA               = "public"
    PG_USERNAME                     = module.rds.db.db_instance_username
    DBT_TARGET                      = "postgres"
  }
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
  env                         = "dev"
}
