variable "name" {}
variable "env" {}
variable "db_subnet_group_name" {}
variable "vpc_default_sg_id" {}

resource "random_password" "rds" {
  length  = 16
  special = false
}

resource "aws_db_parameter_group" "default" {
  family = "postgres12"

  parameter {
    name  = "log_statement"
    value = "ddl"
  }
  parameter {
    name  = "log_min_duration_statement"
    value = 1000
  }
}

resource "aws_db_instance" "db_instance" {
  identifier              = "${var.name}-${var.env}"
  engine                  = "postgres"
  engine_version          = "12"
  instance_class          = var.env == "production" ? "db.m6g.large" : "db.t4g.small"
  allocated_storage       = var.env == "production" ? 100 : 30
  max_allocated_storage   = var.env == "production" ? 200 : 60
  backup_retention_period = var.env == "production" ? 7 : 0
  storage_type            = var.env == "production" ? "io1" : "gp2"
  iops                    = var.env == "production" ? 1000 : null
  deletion_protection     = var.env == "production" ? true : false
  parameter_group_name    = aws_db_parameter_group.default.name

  publicly_accessible = false

  name     = var.name
  username = var.name
  port     = "5432"

  password = random_password.rds.result

  db_subnet_group_name   = var.db_subnet_group_name
  vpc_security_group_ids = [var.vpc_default_sg_id]
  skip_final_snapshot    = true # TODO revert to var.env == "production" ? false : true

  storage_encrypted = true
  apply_immediately = true

  enabled_cloudwatch_logs_exports = ["postgresql"]

  tags = {
    Environment = var.env
    Terraform   = "true"
  }
}

output "db_instance" {
  value = aws_db_instance.db_instance
}
