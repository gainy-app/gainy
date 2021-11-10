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

  provisioner "file" {
    destination = "/tmp/provision.sh"
    content = templatefile(
      "${path.module}/templates/provision.sh",
      {
        pg_host                   = var.pg_host
        pg_password               = var.pg_password
        pg_port                   = var.pg_port
        pg_username               = var.pg_username
        pg_dbname                 = var.pg_dbname
        pg_datadog_password       = random_password.datadog_postgres.result
        pg_internal_sync_username = var.pg_production_internal_sync_username
        pg_internal_sync_password = random_password.internal_sync_postgres.result
        datadog_api_key           = var.datadog_api_key
      }
    )

    connection {
      type        = "ssh"
      user        = "ubuntu"
      host        = aws_instance.bridge.public_ip
      private_key = tls_private_key.bridge.private_key_pem
    }
  }

  provisioner "file" {
    destination = "/tmp/datadog.postgres.yaml"
    content = templatefile(
      "${path.module}/templates/datadog.postgres.yaml",
      {
        pg_host             = var.pg_host
        pg_port             = var.pg_port
        pg_dbname           = var.pg_dbname
        pg_datadog_password = random_password.datadog_postgres.result
        env                 = var.env
      }
    )

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