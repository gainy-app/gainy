variable "env" {}

resource "aws_s3_bucket" "collections" {
  bucket = "gainy-collections-${var.env}"

  tags = {
    Name = "Gainy collections"
  }
}
#resource "aws_s3_bucket_acl" "collections" {
#  bucket = aws_s3_bucket.collections.id
#  acl    = "private"
#}

resource "aws_s3_bucket" "categories" {
  bucket = "gainy-categories-${var.env}"

  tags = {
    Name = "Gainy categories"
  }
}
#resource "aws_s3_bucket_acl" "categories" {
#  bucket = aws_s3_bucket.categories.id
#  acl    = "private"
#}

resource "aws_s3_bucket" "interests" {
  bucket = "gainy-interests-${var.env}"

  tags = {
    Name = "Gainy interests"
  }
}
#resource "aws_s3_bucket_acl" "interests" {
#  bucket = aws_s3_bucket.interests.id
#  acl    = "private"
#}