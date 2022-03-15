variable "env" {}
variable "aws_zones" {
  default = ["us-east-1a", "us-east-1b"]
}
variable "vpc_index" {}
variable "db_external_access_port" {}
variable "mlflow_artifact_bucket" {}

/*
 * Create ECS cluster
 */
resource "aws_kms_key" "this" {
  description             = "gainy-kms-key-${var.env}"
  deletion_window_in_days = 7
  tags                    = {}
}

resource "aws_cloudwatch_log_group" "this" {
  name = "gainy-${var.env}"
}
resource "aws_ecs_cluster" "ecs_cluster" {
  name = "gainy-cluster-${var.env}"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  configuration {
    execute_command_configuration {
      kms_key_id = aws_kms_key.this.arn
      logging    = "OVERRIDE"

      log_configuration {
        cloud_watch_encryption_enabled = true
        cloud_watch_log_group_name     = aws_cloudwatch_log_group.this.name
      }
    }
  }
}

/*
 * Create VPC
 */
resource "aws_vpc" "vpc" {
  cidr_block = "10.${var.vpc_index}.0.0/16"
  tags = {
    Name = "gainy-${var.env}"
  }
  enable_dns_hostnames = true
  enable_dns_support   = true
}

/*
 * Create VPC Peering Connection to production
 */
data "aws_vpc" "production" {
  tags = {
    Name = "gainy-production"
  }
}
resource "aws_vpc_peering_connection" "to_prod" {
  count       = var.env == "production" ? 0 : 1
  peer_vpc_id = data.aws_vpc.production.id
  vpc_id      = aws_vpc.vpc.id
}

/*
 * Get default security group for reference later
 */
data "aws_security_group" "vpc_default_sg" {
  name   = "default"
  vpc_id = aws_vpc.vpc.id
}
resource "aws_security_group_rule" "bridge-rds" {
  type              = "ingress"
  from_port         = 5432
  to_port           = 5432
  protocol          = "tcp"
  security_group_id = data.aws_security_group.vpc_default_sg.id
  cidr_blocks       = ["10.0.0.0/8"]
}
resource "aws_security_group_rule" "bridge-elasticache" {
  type              = "ingress"
  from_port         = 6379
  to_port           = 6379
  protocol          = "tcp"
  security_group_id = data.aws_security_group.vpc_default_sg.id
  cidr_blocks       = ["10.0.0.0/8"]
}
resource "aws_security_group_rule" "rds-external-access" {
  type              = "ingress"
  from_port         = var.db_external_access_port
  to_port           = var.db_external_access_port
  protocol          = "tcp"
  security_group_id = data.aws_security_group.vpc_default_sg.id
  cidr_blocks       = ["0.0.0.0/0"]
}

/*
 * Create public and private subnets for each availability zone
 */
resource "aws_subnet" "public_subnet" {
  count             = length(var.aws_zones)
  vpc_id            = aws_vpc.vpc.id
  availability_zone = element(var.aws_zones, count.index)
  cidr_block        = "10.${var.vpc_index}.${(count.index + 1) * 10}.0/24"
  tags = {
    Name = "public-${element(var.aws_zones, count.index)}"
  }
}
resource "aws_subnet" "private_subnet" {
  count             = length(var.aws_zones)
  vpc_id            = aws_vpc.vpc.id
  availability_zone = element(var.aws_zones, count.index)
  cidr_block        = "10.${var.vpc_index}.${(count.index + 1) * 11}.0/24"
  tags = {
    Name = "private-${element(var.aws_zones, count.index)}"
  }
}

/*
 * Create internet gateway for VPC
 */
resource "aws_internet_gateway" "internet_gateway" {
  vpc_id = aws_vpc.vpc.id
}
/*
 * Create NAT gateway and allocate Elastic IP for it
 */
resource "aws_eip" "gateway_eip" {}
resource "aws_nat_gateway" "nat_gateway" {
  allocation_id = aws_eip.gateway_eip.id
  subnet_id     = aws_subnet.public_subnet.0.id
  depends_on    = [aws_internet_gateway.internet_gateway]
}
/*
 * Routes for private subnets to use NAT gateway
 */
resource "aws_route_table" "nat_route_table" {
  vpc_id = aws_vpc.vpc.id

  tags = {
    Name = "gainy-${var.env}-private"
  }
}
resource "aws_route" "nat_route" {
  route_table_id         = aws_route_table.nat_route_table.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.nat_gateway.id
}
resource "aws_route" "private_prod_vpc_route" {
  count                     = var.env == "production" ? 0 : 1
  route_table_id            = aws_route_table.nat_route_table.id
  destination_cidr_block    = "10.0.0.0/16"
  vpc_peering_connection_id = aws_vpc_peering_connection.to_prod[0].id
}
resource "aws_route_table_association" "private_route" {
  count          = length(var.aws_zones)
  subnet_id      = element(aws_subnet.private_subnet.*.id, count.index)
  route_table_id = aws_route_table.nat_route_table.id
}
/*
 * Routes for public subnets to use internet gateway
 */
resource "aws_route_table" "igw_route_table" {
  vpc_id = aws_vpc.vpc.id

  tags = {
    Name = "gainy-${var.env}-public"
  }
}
resource "aws_route" "igw_route" {
  route_table_id         = aws_route_table.igw_route_table.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.internet_gateway.id
}
resource "aws_route" "public_prod_vpc_route" {
  count                     = var.env == "production" ? 0 : 1
  route_table_id            = aws_route_table.igw_route_table.id
  destination_cidr_block    = "10.0.0.0/16"
  vpc_peering_connection_id = aws_vpc_peering_connection.to_prod[0].id
}
resource "aws_route_table_association" "public_route" {
  count          = length(var.aws_zones)
  subnet_id      = element(aws_subnet.public_subnet.*.id, count.index)
  route_table_id = aws_route_table.igw_route_table.id
}
/*
 * Create DB Subnet Group for private subnets
 */
resource "aws_db_subnet_group" "db_subnet_group" {
  name       = "db-subnet-${var.env}"
  subnet_ids = aws_subnet.private_subnet.*.id
}
resource "aws_db_subnet_group" "public_subnet_group_name" {
  name       = "public-subnet-${var.env}"
  subnet_ids = aws_subnet.public_subnet.*.id
}

/*
 * Create Security Groups
 */
resource "aws_security_group" "public_https" {
  name        = "public-https"
  description = "Allow HTTPS traffic from public"
  vpc_id      = aws_vpc.vpc.id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
resource "aws_security_group" "public_http" {
  name        = "public-http"
  description = "Allow HTTP traffic from public"
  vpc_id      = aws_vpc.vpc.id

  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

/*
 * Outputs
 */

output "aws_zones" {
  value = [var.aws_zones]
}
output "private_subnet_group_name" {
  value = aws_db_subnet_group.db_subnet_group.name
}
output "public_subnet_group_name" {
  value = aws_db_subnet_group.public_subnet_group_name.name
}
output "ecs_cluster" {
  value = aws_ecs_cluster.ecs_cluster
}
output "private_subnet_ids" {
  value = aws_subnet.private_subnet.*.id
}
output "public_subnet_ids" {
  value = aws_subnet.public_subnet.*.id
}
output "vpc_default_sg_id" {
  value = data.aws_security_group.vpc_default_sg.id
}
output "vpc_id" {
  value = aws_vpc.vpc.id
}
output "public_http_sg_id" {
  value = aws_security_group.public_http.id
}
output "public_https_sg_id" {
  value = aws_security_group.public_https.id
}