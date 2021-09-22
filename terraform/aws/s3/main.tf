variable "env" {}

resource "aws_s3_bucket" "collections" {
  bucket = "gainy-collections-${var.env}"
  acl    = "private"

  tags = {
    Name = "Gainy collections"
  }
}

resource "aws_s3_bucket" "categories" {
  bucket = "gainy-categories-${var.env}"
  acl    = "private"

  tags = {
    Name = "Gainy categories"
  }
}

resource "aws_s3_bucket" "interests" {
  bucket = "gainy-interests-${var.env}"
  acl    = "private"

  tags = {
    Name = "Gainy interests"
  }
}