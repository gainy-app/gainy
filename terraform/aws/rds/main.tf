variable "name" {}
variable "env" {}
variable "db_subnet_group_name" {}
variable "vpc_default_sg_id" {}

resource "random_password" "rds" {
  length           = 16
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

resource "aws_db_instance" "db_instance" {
  identifier              = "${var.name}-${var.env}"
  engine                  = "postgres"
  engine_version          = "12"
  instance_class          = "db.t3.large"
  allocated_storage       = 50
  max_allocated_storage   = 100
  backup_retention_period = var.env == "production" ? 7 : 0

  publicly_accessible = false

  name     = var.name
  username = var.name
  port     = "5432"

  password = random_password.rds.result

  db_subnet_group_name   = var.db_subnet_group_name
  vpc_security_group_ids = [var.vpc_default_sg_id]
  skip_final_snapshot    = var.env == "production" ? false : true

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
