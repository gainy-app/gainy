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
  instance_class          = var.env == "production" ? "db.m6g.2xlarge" : "db.m6g.large"
  allocated_storage       = var.env == "production" ? 100 : 100
  max_allocated_storage   = var.env == "production" ? 400 : 400
  backup_retention_period = var.env == "production" ? 7 : 0
  storage_type            = var.env == "production" ? "io1" : "io1"
  iops                    = var.env == "production" ? 1999 : 1000
  deletion_protection     = var.env == "production" ? true : false
  parameter_group_name    = aws_db_parameter_group.default.name

  publicly_accessible = false

  name     = var.name
  username = var.name
  port     = "5432"

  password = random_password.rds.result

  db_subnet_group_name      = var.db_subnet_group_name
  vpc_security_group_ids    = [var.vpc_default_sg_id]
  skip_final_snapshot       = var.env == "production" ? false : true
  final_snapshot_identifier = "${var.name}-${var.env}-final"

  storage_encrypted = true
  apply_immediately = true

  performance_insights_enabled    = true
  enabled_cloudwatch_logs_exports = ["postgresql"]

  tags = {
    Environment = var.env
  }
}

resource "aws_db_instance" "db_replica" {
  count                = var.env == "production" ? 0 : 0 # todo add replicas after figuring out hot to use them in hasura
  identifier           = "${var.name}-${var.env}-replica${count.index}"
  replicate_source_db  = aws_db_instance.db_instance.identifier
  instance_class       = var.env == "production" ? "db.m6g.2xlarge" : "db.m6g.large"
  storage_type         = var.env == "production" ? "io1" : "io1"
  iops                 = var.env == "production" ? 1999 : 1000
  parameter_group_name = aws_db_parameter_group.default.name
  storage_encrypted    = true

  publicly_accessible    = false
  vpc_security_group_ids = [var.vpc_default_sg_id]
  apply_immediately      = true

  performance_insights_enabled    = true
  enabled_cloudwatch_logs_exports = ["postgresql"]

  tags = {
    Environment = var.env
    Replica     = "true"
  }
}

output "db_instance" {
  value = aws_db_instance.db_instance
}
output "db_replica" {
  value = aws_db_instance.db_replica
}
