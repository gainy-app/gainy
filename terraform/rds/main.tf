variable "name" {}

variable "env" {}

variable "subnets" {}

variable "vpc_id" {}

variable "allowed_cidrs" {
  type = list(string)
}

variable "security_group" {}

resource random_password "password" {
  length = 16
  special          = true
  override_special = "\"/@ "
}

variable "publicly_accessible" {
  type = bool
}

module "db" {
  source = "terraform-aws-modules/rds/aws"

  identifier = "${var.name}-${var.env}"
  engine = "postgres"
  major_engine_version = "12"
  instance_class = "db.t3.medium"
  allocated_storage = 5

  publicly_accessible = var.publicly_accessible

  family = "postgres12"

  name= var.name
  username = var.name

  password = random_password.password.result

  port = "5432"

  subnet_ids = var.subnets

  vpc_security_group_ids=[var.security_group]

  storage_encrypted = true
  apply_immediately = true

  enabled_cloudwatch_logs_exports = [
    "postgresql"]

  tags = {
    Environment = var.env
    Terraform = "true"
  }
}

output "db" {
  value = module.db
}
