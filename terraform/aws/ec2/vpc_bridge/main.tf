#################################### VPC bridge EC2 instance ####################################

variable "env" {}
variable "vpc_id" {}
variable "vpc_default_sg_id" {}
variable "public_subnet_id" {}
variable "cloudflare_zone_id" {}

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
resource "aws_security_group_rule" "bridge-rds" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = var.vpc_default_sg_id
  source_security_group_id = aws_security_group.bridge.id
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

/*
 * Create Cloudflare DNS record
 */
resource "cloudflare_record" "service" {
  name    = "gainy-bridge-${var.env}"
  value   = aws_instance.bridge.public_dns
  type    = "CNAME"
  proxied = false
  zone_id = var.cloudflare_zone_id
}

output "bridge_instance_url" {
  value = cloudflare_record.service.hostname
}