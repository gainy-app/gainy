resource "aws_s3_bucket" "avatars" {
  bucket = "gainy-avatars"
  acl    = "private"

  tags = {
    Name = "Gainy avatars"
  }
}

module "lambda-generateAvatarUrl" {
  source = "./lambda"
  function_name = "generateAvatarUrl"
}