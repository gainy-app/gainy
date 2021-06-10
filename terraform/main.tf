variable "env" {
  default = "dev"
}

provider "aws" {}

module "networking" {
  source = "./networking"
  env = var.env
}

resource "random_password" "hasura_secret" {
  length = 16
}

module "lambdas" {
  source = "./lambdas"
  env = var.env
  security_group_id = module.networking.vpc.default_security_group_id
  subnets = module.networking.vpc.private_subnets
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

module "heroku" {
  source = "./heroku"
  name = "gainy-managed"
  env = "dev"
  path = "../src/hasura/"
  config = {
    HASURA_GRAPHQL_DATABASE_URL = "postgres://${module.rds.db.db_instance_username}:${module.rds.db.db_master_password}@${module.rds.db.db_instance_endpoint}/${module.rds.db.db_instance_name}"
    HASURA_GRAPHQL_ADMIN_SECRET = random_password.hasura_secret.result
    HASURA_GRAPHQL_ENABLE_CONSOLE = "true"
  }
}