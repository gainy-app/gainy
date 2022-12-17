data "aws_region" "current" {}
data "aws_caller_identity" "this" {}

locals {
  artifact_s3_bucket_name      = "gainy-cw-syn-results-${var.env}"
  artifact_s3_hasura_directory = "hasura"
  hasura_canary_name           = "hasura-${var.env}"
  canary_source_directory      = "${path.module}/canary_scripts"
  canary_source_filepath       = "${local.canary_source_directory}/hasura.py"
}
data "archive_file" "canary_scripts" {
  type        = "zip"
  output_path = "/tmp/canary_scripts_${filemd5(local.canary_source_filepath)}.zip"

  source {
    content = templatefile(
      local.canary_source_filepath,
      {
        hasura_url          = "https://${var.hasura_url}"
        hasura_admin_secret = var.hasura_admin_secret
      }
    )
    filename = "python/hasura.py"
  }

  source {
    content  = file("${local.canary_source_directory}/queries/GetPlaidHoldings.graphql")
    filename = "python/queries/GetPlaidHoldings.graphql"
  }

  source {
    content  = file("${local.canary_source_directory}/queries/GetHomeTabData.graphql")
    filename = "python/queries/GetHomeTabData.graphql"
  }
}

resource "aws_s3_bucket" "artifacts" {
  bucket = local.artifact_s3_bucket_name

  tags = {
    Name = "Gainy CloudWatch Synthetics Canary Artifacts"
  }
}

resource "aws_iam_role" "canary_exec" {
  name               = "synthetics_canary_${var.env}"
  assume_role_policy = <<-EOF
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Principal": {
            "Service": "lambda.amazonaws.com"
          },
          "Action": "sts:AssumeRole"
        }
      ]
    }
  EOF
}
resource "aws_iam_policy" "canary_exec" {
  name        = "synthetics_canary_${var.env}"
  description = "Canary Exec Policy ${var.env}"

  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:PutObject",
          "s3:GetObject"
        ],
        "Resource" : [
          "arn:aws:s3:::${local.artifact_s3_bucket_name}/${local.artifact_s3_hasura_directory}/*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:GetBucketLocation"
        ],
        "Resource" : [
          "arn:aws:s3:::${local.artifact_s3_bucket_name}"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:CreateLogGroup"
        ],
        "Resource" : [
          "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.this.account_id}:log-group:/aws/lambda/cwsyn-${local.hasura_canary_name}-*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:ListAllMyBuckets",
          "xray:PutTraceSegments"
        ],
        "Resource" : [
          "*"
        ]
      },
      {
        "Effect" : "Allow",
        "Resource" : "*",
        "Action" : "cloudwatch:PutMetricData",
        "Condition" : {
          "StringEquals" : {
            "cloudwatch:namespace" : "CloudWatchSynthetics"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attachment" {
  role       = aws_iam_role.canary_exec.name
  policy_arn = aws_iam_policy.canary_exec.arn
}

resource "aws_synthetics_canary" "hasura" {
  name                 = local.hasura_canary_name
  artifact_s3_location = "s3://${local.artifact_s3_bucket_name}/${local.artifact_s3_hasura_directory}"
  execution_role_arn   = aws_iam_role.canary_exec.arn
  handler              = "hasura.handler"
  zip_file             = data.archive_file.canary_scripts.output_path
  runtime_version      = "syn-python-selenium-1.3"
  start_canary         = true

  schedule {
    expression = "rate(1 minute)"
  }
}