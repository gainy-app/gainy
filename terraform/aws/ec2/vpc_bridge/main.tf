#################################### VPC bridge EC2 instance ####################################

variable "env" {}
variable "vpc_id" {}
variable "vpc_default_sg_id" {}
variable "public_subnet_id" {}
variable "cloudflare_zone_id" {}
variable "datadog_api_key" {}

variable "pg_host" {}
variable "pg_port" {}
variable "pg_username" {}
variable "pg_password" {}
variable "pg_dbname" {}
variable "pg_production_internal_sync_username" {}
variable "public_schema_name" {}

locals {
  provision_script_content = templatefile(
    "${path.module}/templates/provision.sh",
    {
      PG_HOST                   = var.pg_host
      PG_PASSWORD               = var.pg_password
      PG_PORT                   = var.pg_port
      PG_USERNAME               = var.pg_username
      PG_DBNAME                 = var.pg_dbname
      PUBLIC_SCHEMA_NAME        = var.public_schema_name
      PG_DATADOG_PASSWORD       = random_password.datadog_postgres.result
      PG_INTERNAL_SYNC_USERNAME = var.pg_production_internal_sync_username
      PG_INTERNAL_SYNC_PASSWORD = random_password.internal_sync_postgres.result
      DATADOG_API_KEY           = var.datadog_api_key
    }
  )

  datadog_config_content = templatefile(
    "${path.module}/templates/datadog.postgres.yaml",
    {
      PG_HOST             = var.pg_host
      PG_PORT             = var.pg_port
      PG_DBNAME           = var.pg_dbname
      PUBLIC_SCHEMA_NAME  = var.public_schema_name
      PG_DATADOG_PASSWORD = random_password.datadog_postgres.result
      ENV                 = var.env
    }
  )
}

data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["099720109477"] # Canonical
}

resource "tls_private_key" "bridge" {
  algorithm = "RSA"
}
module "key_pair" {
  source = "terraform-aws-modules/key-pair/aws"

  key_name   = "gainy-bridge-${var.env}"
  public_key = tls_private_key.bridge.public_key_openssh
}
resource "aws_security_group" "bridge" {
  name        = "bridge"
  description = "Allow SSH"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "all"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "random_password" "datadog_postgres" {
  length  = 16
  special = false
}

resource "random_password" "internal_sync_postgres" {
  length  = 16
  special = false
}

resource "aws_instance" "bridge" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = "t3.micro"
  key_name                    = module.key_pair.key_pair_key_name
  subnet_id                   = var.public_subnet_id
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.bridge.id]
  tags = {
    Name = "gainy-bridge-${var.env}"
  }

}

resource "null_resource" "bridge" {
  # Changes to any instance of the cluster requires re-provisioning
  triggers = {
    provision_script_content = local.provision_script_content
    datadog_config_content   = local.datadog_config_content
    bridge_instance_id       = aws_instance.bridge.id
  }

  provisioner "file" {
    destination = "/tmp/provision.sh"
    content     = local.provision_script_content

    connection {
      type        = "ssh"
      user        = "ubuntu"
      host        = aws_instance.bridge.public_ip
      private_key = tls_private_key.bridge.private_key_pem
    }
  }

  provisioner "file" {
    destination = "/tmp/datadog.postgres.yaml"
    content     = local.datadog_config_content

    connection {
      type        = "ssh"
      user        = "ubuntu"
      host        = aws_instance.bridge.public_ip
      private_key = tls_private_key.bridge.private_key_pem
    }
  }

  provisioner "remote-exec" {
    inline = [
      "chmod +x /tmp/provision.sh",
      "sudo /tmp/provision.sh",
    ]

    connection {
      type        = "ssh"
      user        = "ubuntu"
      host        = aws_instance.bridge.public_ip
      private_key = tls_private_key.bridge.private_key_pem
    }
  }
}

/*
 * Create Cloudflare DNS record
 */
resource "cloudflare_record" "service" {
  name    = "gainy-bridge-${var.env}"
  value   = aws_instance.bridge.public_ip
  type    = "A"
  proxied = false
  zone_id = var.cloudflare_zone_id
}

output "bridge_instance_url" {
  value = cloudflare_record.service.hostname
}