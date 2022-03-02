variable "env" {}

resource "aws_s3_bucket" "collections" {
  bucket = "gainy-collections-${var.env}"

  tags = {
    Name = "Gainy collections"
  }
}

resource "aws_s3_bucket" "categories" {
  bucket = "gainy-categories-${var.env}"

  tags = {
    Name = "Gainy categories"
  }
}

resource "aws_s3_bucket" "interests" {
  bucket = "gainy-interests-${var.env}"

  tags = {
    Name = "Gainy interests"
  }
}

resource "aws_s3_bucket" "mlflow" {
  bucket = "gainy-mlflow-${var.env}"

  tags = {
    Name = "Gainy MLflow"
  }
}

output "mlflow_artifact_bucket" {
  value = aws_s3_bucket.mlflow.bucket
}