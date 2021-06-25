variable "env" {
  default = "dev"
  type = string
}

variable "eodhistoricaldata_api_token" {
  type        = string
  sensitive   = true
}

provider "aws" {}

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
  env = var.env
}

resource "random_password" "hasura_secret" {
  length = 16
}

module "rds" {
  env = var.env
  name = "gainy"
  source = "./rds"
  subnets = module.networking.vpc.database_subnets
  allowed_cidrs = module.networking.vpc.private_subnets
  security_group = module.networking.vpc.default_security_group_id
  vpc_id = module.networking.vpc.vpc_id
  publicly_accessible = true
}

module "heroku-gainy-managed" {
  source = "./heroku"
  name = "gainy-managed"
  env = "dev"
  path = "src/hasura"
  stack = "container"
  config = {
    HASURA_GRAPHQL_DATABASE_URL = "postgres://${module.rds.db.db_instance_username}:${module.rds.db.db_master_password}@${module.rds.db.db_instance_endpoint}/${module.rds.db.db_instance_name}"
    HASURA_GRAPHQL_ADMIN_SECRET = random_password.hasura_secret.result
    HASURA_GRAPHQL_ENABLE_CONSOLE = "true"
  }
}

module "heroku-gainy-fetch" {
  source = "./heroku"
  stack = "container"
  name = "gainy-fetch"
  env = "dev"
  path = "src/gainy-fetch"
  config = {
    TARGET_POSTGRES_HOST = module.rds.db.db_instance_endpoint
    TARGET_POSTGRES_PORT = module.rds.db.db_instance_port
    TARGET_POSTGRES_USER = module.rds.db.db_instance_username
    TARGET_POSTGRES_PASSWORD = module.rds.db.db_master_password
    TARGET_POSTGRES_DBNAME = module.rds.db.db_instance_name
    TARGET_POSTGRES_SCHEMA = "public"
    TAP_POSTGRES_FILTER_SCHEMAS = "public"
    TAP_EODHISTORICALDATA_API_TOKEN = var.eodhistoricaldata_api_token
    TAP_EODHISTORICALDATA_SYMBOLS = "[\"AAPL\"]"
  }
}
