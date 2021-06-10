variable "env" {
}

module "vpc" {
  source = "terraform-aws-modules/vpc/aws"

  name = var.env
  cidr = "10.0.0.0/16"
  single_nat_gateway = true

  azs = [
    "us-east-1a",
    "us-east-1b",
    "us-east-1c"]
  database_subnets = [
    "10.0.21.0/24",
    "10.0.22.0/24",
    "10.0.23.0/24"]
  public_subnets = [
    "10.0.101.0/24",
    "10.0.102.0/24",
    "10.0.103.0/24"]
  private_subnets = [
    "10.0.1.0/24",
    "10.0.2.0/24",
    "10.0.3.0/24"]
  intra_subnets = [
    "10.0.51.0/24",
    "10.0.52.0/24",
    "10.0.53.0/24"]

  enable_nat_gateway = true
  enable_vpn_gateway = true

  create_database_subnet_group = true
  create_database_subnet_route_table = true
  create_database_internet_gateway_route = true

  enable_dns_hostnames = true
  enable_dns_support = true

  tags = {
    Terraform = "true"
    Environment = var.env
  }
}

output "vpc" {
 value = module.vpc
}