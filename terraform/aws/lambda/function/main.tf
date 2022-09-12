variable "exec_role_arn" {}
variable "env" {}
variable "function_name" {}
variable "handler" {}
variable "env_vars" {
  default = {}
}
variable "timeout" {
  default = 3
}
variable "image_uri" {}
variable "vpc_security_group_ids" {}
variable "vpc_subnet_ids" {}
variable "memory_size" {
  default = 128
}

resource "aws_lambda_function" "lambda" {
  function_name = "${var.function_name}_${var.env}"

  package_type = "Image"

  image_uri = var.image_uri
  image_config {
    command = [var.handler]
  }

  timeout = var.timeout
  publish = true

  memory_size = var.memory_size

  role = var.exec_role_arn

  vpc_config {
    security_group_ids = var.vpc_security_group_ids
    subnet_ids         = var.vpc_subnet_ids
  }

  environment {
    variables = var.env_vars
  }
}

output "function_name" {
  value = aws_lambda_function.lambda.function_name
}

output "arn" {
  value = aws_lambda_function.lambda.arn
}

output "version" {
  value = aws_lambda_function.lambda.version
}
