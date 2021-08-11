variable "eodhistoricaldata_api_token" {}

resource "aws_s3_bucket" "collections" {
  bucket = "gainy-collections"
  acl    = "private"

  tags = {
    Name = "Gainy collections"
  }
}

module "lambda-fetchChartData" {
  source = "./lambda"
  function_name = "fetchChartData"
  route = "POST /fetchChartData"
  env_vars = {
    eodhistoricaldata_api_token = var.eodhistoricaldata_api_token
  }
}