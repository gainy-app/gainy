variable "env" {
}

variable "subnets" {
  type = list(any)
}

variable "security_group_id" {
  type = string
}
module "lambda_function" {
  source = "terraform-aws-modules/lambda/aws"

  function_name = "gainy-fetch"
  description   = "gainy fetch regularly grabs tickers and persists them into s3"
  handler       = "index.handler"
  runtime       = "python3.8"

  source_path = "../src/gainy-fetch"

  vpc_subnet_ids         = var.subnets
  vpc_security_group_ids = [var.security_group_id]
  attach_network_policy  = true

  tags = {
    Name        = "gainy-fetch"
    Environment = var.env
  }
}
