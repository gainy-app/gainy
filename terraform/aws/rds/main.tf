variable "name" {}
variable "env" {}
variable "private_subnet_group_name" {}
variable "public_subnet_group_name" {}
variable "vpc_default_sg_id" {}
variable "db_external_access_port" {}

resource "random_password" "rds" {
  length  = 16
  special = false
}

resource "random_password" "rds_external_access" {
  length  = 32
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
  instance_class          = var.env == "production" ? "db.m6g.4xlarge" : "db.m6g.2xlarge"
  allocated_storage       = var.env == "production" ? 100 : 100
  max_allocated_storage   = var.env == "production" ? 1500 : 1000
  backup_retention_period = var.env == "production" ? 7 : 0
  backup_window           = "10:00-11:00"
  storage_type            = var.env == "production" ? "io1" : "io1"
  iops                    = var.env == "production" ? 1999 : 2000
  deletion_protection     = var.env == "production" ? true : false
  parameter_group_name    = aws_db_parameter_group.default.name

  publicly_accessible = false

  db_name  = var.name
  username = var.name
  port     = "5432"

  password = random_password.rds.result

  db_subnet_group_name      = var.private_subnet_group_name
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
  instance_class       = var.env == "production" ? "db.m6g.4xlarge" : "db.m6g.large"
  storage_type         = var.env == "production" ? "io1" : "io1"
  iops                 = var.env == "production" ? 1999 : 1000
  parameter_group_name = aws_db_parameter_group.default.name
  storage_encrypted    = true
  deletion_protection  = false
  skip_final_snapshot  = true

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

resource "aws_db_instance" "db_external_access" {
  count                   = var.env == "production" ? 1 : 0
  identifier              = "${var.name}-external-access-${var.env}"
  engine                  = "postgres"
  engine_version          = "12"
  instance_class          = "db.t4g.small"
  allocated_storage       = 100
  max_allocated_storage   = 1000
  backup_retention_period = 0
  storage_type            = "gp2"
  deletion_protection     = true
  parameter_group_name    = aws_db_parameter_group.default.name

  publicly_accessible = true

  db_name  = "${var.name}_external_access"
  username = var.name
  port     = var.db_external_access_port

  password = random_password.rds_external_access.result

  db_subnet_group_name      = var.public_subnet_group_name
  vpc_security_group_ids    = [var.vpc_default_sg_id]
  skip_final_snapshot       = true
  final_snapshot_identifier = "${var.name}-${var.env}-final"

  storage_encrypted = true
  apply_immediately = true

  performance_insights_enabled    = true
  enabled_cloudwatch_logs_exports = ["postgresql"]

  tags = {
    Environment = var.env
  }
}

output "db_instance" {
  value = aws_db_instance.db_instance
}
output "db_replica" {
  value = aws_db_instance.db_replica
}
output "db_external_access" {
  value = aws_db_instance.db_external_access
}
