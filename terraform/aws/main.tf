resource "aws_s3_bucket" "collections" {
  bucket = "gainy-collections"
  acl    = "private"

  tags = {
    Name = "Gainy collections"
  }
}