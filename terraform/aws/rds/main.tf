variable "name" {}
variable "env" {}
variable "db_subnet_group_name" {}
variable "vpc_default_sg_id" {}

resource "random_password" "rds" {
  length  = 16
  special = false
}

resource "aws_db_instance" "db_instance" {
  identifier              = "${var.name}-${var.env}"
  engine                  = "postgres"
  engine_version          = "12"
  instance_class          = var.env == "production" ? "db.m6g.large" : "db.t3.micro"
  allocated_storage       = var.env == "production" ? 100 : 20
  max_allocated_storage   = var.env == "production" ? 200 : 21
  backup_retention_period = var.env == "production" ? 7 : 0
  #  TODO use this resource only for production and specify iops
  #  storage_type            = var.env == "production" ? "io1" : "gp2"

  publicly_accessible = false

  name     = var.name
  username = var.name
  port     = "5432"

  password = random_password.rds.result

  db_subnet_group_name   = var.db_subnet_group_name
  vpc_security_group_ids = [var.vpc_default_sg_id]
  skip_final_snapshot    = true # TODO var.env == "production" ? false : true

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
