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

resource "aws_s3_bucket" "gainy_history" {
  bucket = "gainy-history-${var.env}"

  tags = {
    Name = "Gainy History"
  }
}

resource "aws_s3_bucket" "uploads_kyc" {
  bucket = "uploads-kyc-${var.env}"

  tags = {
    Name = "Uploads KYC"
  }
}
resource "aws_s3_bucket_acl" "uploads_kyc" {
  acl    = "private"
  bucket = aws_s3_bucket.uploads_kyc.bucket
}
resource "aws_s3_bucket_server_side_encryption_configuration" "uploads_kyc" {
  bucket = aws_s3_bucket.uploads_kyc.bucket
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

output "mlflow_artifact_bucket" {
  value = aws_s3_bucket.mlflow.bucket
}

output "gainy_history_bucket" {
  value = aws_s3_bucket.gainy_history.bucket
}

output "uploads_kyc_bucket" {
  value = aws_s3_bucket.uploads_kyc.bucket
}
