resource "datadog_integration_slack_channel" "alerts_channel" {
  account_name = var.slack_account_name
  channel_name = "#${var.slack_channel_name}"

  display {
    message  = true
    notified = true
    snapshot = true
    tags     = true
  }
}

# Cloud Formation
# Datadog Forwarder to ship logs from S3 and CloudWatch, as well as observability data from Lambda functions to Datadog.
# https://github.com/DataDog/datadog-serverless-functions/tree/master/aws/logs_monitoring
resource "aws_secretsmanager_secret" "dd_api_key" {
  name        = "dd_api_key"
  description = "Encrypted Datadog API Key"
}
resource "aws_secretsmanager_secret_version" "dd_api_key" {
  secret_id     = aws_secretsmanager_secret.dd_api_key.id
  secret_string = var.datadog_api_key
}
resource "aws_cloudformation_stack" "datadog" {
  name         = "datadog"
  capabilities = ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND"]
  parameters = {
    DdApiKeySecretArn = aws_secretsmanager_secret_version.dd_api_key.arn
    DdSite            = "datadoghq.com"
    ExternalId        = var.datadog_aws_external_id
  }
  template_url = "https://datadog-cloudformation-template.s3.amazonaws.com/aws/main.yaml"
}
data "aws_caller_identity" "this" {}
resource "datadog_integration_aws_lambda_arn" "main_collector" {
  account_id = data.aws_caller_identity.this.account_id
  lambda_arn = aws_cloudformation_stack.datadog.outputs["DatadogForwarderArn"]
}
resource "aws_cloudwatch_log_subscription_filter" "datadog_log_subscription_filter" {
  for_each        = toset(var.additional_forwarded_log_groups)
  name            = "datadog_log_subscription_filter"
  log_group_name  = each.value
  destination_arn = aws_cloudformation_stack.datadog.outputs["DatadogForwarderArn"]
  filter_pattern  = ""
}

#################################### Billing ####################################

resource "datadog_monitor" "billing_spend" {
  name    = "Billing Spend"
  type    = "query alert"
  message = "Billing Spend Monitor triggered. Notify: @slack-${var.slack_channel_name} <!channel>"
  #  escalation_message = "Escalation message @pagerduty"

  query = "sum(last_1d):aws.billing.forecasted_spend{*}.rollup(avg, 1800) / day_before(aws.billing.forecasted_spend{*}.rollup(avg, 1800)) - 1 > 0.05"

  monitor_thresholds {
    critical          = "0.05"
    critical_recovery = "0"
  }

  require_full_window = false
  notify_no_data      = false
  renotify_interval   = 1440

  tags = ["billing"]
}

#################################### ALB ####################################

resource "datadog_monitor" "hasura_alb_5xx" {
  name    = "Hasura 5xx Errors"
  type    = "query alert"
  message = "Hasura 5xx Errors Monitor triggered. Notify: @slack-${var.slack_channel_name} <!channel>"
  #  escalation_message = "Escalation message @pagerduty"

  query = "sum(last_1h):default_zero(sum:aws.applicationelb.httpcode_elb_5xx{name:*-production} by {name}) > 0.01"

  monitor_thresholds {
    critical          = "0.01"
    critical_recovery = "0"
  }

  require_full_window = false
  notify_no_data      = false
  renotify_interval   = 60

  tags = ["hasura"]
}

#################################### ECS ####################################

resource "datadog_monitor" "healthy_hosts" {
  name    = "ECS Healthy Hosts"
  type    = "metric alert"
  message = "ECS Healthy Hosts Monitor triggered. Notify: @slack-${var.slack_channel_name} <!channel>"
  #  escalation_message = "Escalation message @pagerduty"

  query = "avg(last_15m):sum:aws.applicationelb.healthy_host_count{name:*-production} by {name} < 1"

  monitor_thresholds {
    critical = 1
  }

  require_full_window = false
  notify_no_data      = true
  renotify_interval   = 15

  tags = ["ecs"]
}

#################################### Lambda ####################################

resource "datadog_monitor" "lambda_duration" {
  name    = "Lambda Duration"
  type    = "query alert"
  message = "Lambda Duration Monitor triggered. Notify: @slack-${var.slack_channel_name} <!channel>"
  #  escalation_message = "Escalation message @pagerduty"

  query = "avg(last_7d):ewma_10(aws.lambda.duration{functionname:*_production} by {functionname}) > 3"

  monitor_thresholds {
    critical = "3"
  }

  require_full_window = true
  notify_no_data      = false
  no_data_timeframe   = 30
  renotify_interval   = 720
  evaluation_delay    = 900

  tags = ["lambda"]
}

resource "datadog_monitor" "lambda_errors" {
  name    = "Lambda Errors"
  type    = "query alert"
  message = "Lambda Errors Monitor triggered. Notify: @slack-${var.slack_channel_name} <!channel>"

  query = "sum(last_1d):sum:aws.lambda.errors{functionname:*_production} by {functionname}.rollup(sum, 3600) > 0.01"

  monitor_thresholds {
    critical          = "0.01"
    critical_recovery = "0"
  }

  no_data_timeframe   = 120
  require_full_window = true
  notify_no_data      = false
  renotify_interval   = 1440

  tags = ["lambda"]
}

#################################### RDS ####################################

resource "datadog_monitor" "rds_free_space" {
  name    = "RDS Free Space"
  type    = "metric alert"
  message = "RDS Free Space Monitor triggered. Notify: @slack-${var.slack_channel_name} <!channel>"
  #  escalation_message = "Escalation message @pagerduty"

  query = "avg(last_1h):avg:aws.rds.free_storage_space{dbinstanceidentifier:*-production} < 10"

  monitor_thresholds {
    warning_recovery  = 25
    warning           = 20
    critical_recovery = 15
    critical          = 10
  }

  require_full_window = false
  notify_no_data      = true
  renotify_interval   = 15

  tags = ["rds"]
}

resource "datadog_monitor" "rds_memory" {
  name    = "RDS Free Memory"
  type    = "metric alert"
  message = "RDS Free Memory Monitor triggered. Notify: @slack-${var.slack_channel_name} <!channel>"
  #  escalation_message = "Escalation message @pagerduty"

  query = "avg(last_1h):avg:aws.rds.freeable_memory{dbinstanceidentifier:*-production} < 1"

  monitor_thresholds {
    warning_recovery  = 2.5
    warning           = 2
    critical_recovery = 1.5
    critical          = 1
  }

  require_full_window = false
  notify_no_data      = true
  renotify_interval   = 15

  tags = ["rds"]
}

resource "datadog_monitor" "rds_cpu" {
  name    = "RDS CPU"
  type    = "query alert"
  message = "RDS CPU Monitor triggered. Notify: @slack-${var.slack_channel_name} <!channel>"

  query = "avg(last_14d):avg:aws.rds.cpuutilization{dbinstanceidentifier:*-production} by {dbinstanceidentifier}.rollup(avg, 3600) > 80"

  monitor_thresholds {
    critical          = "80"
    critical_recovery = "70"
    warning           = "50"
    warning_recovery  = "40"
  }

  require_full_window = false
  notify_no_data      = true
  renotify_interval   = 60
  no_data_timeframe   = 120

  tags = ["rds"]
}

#################################### Meltano ####################################

resource "datadog_monitor" "meltano_dag_run_date" {
  name    = "Airflow Meltano Dag Run Date"
  type    = "metric alert"
  message = "Airflow Meltano Dag Run Date triggered. Notify: @slack-${var.slack_channel_name} <!channel>"

  query = "max(last_5m):min:app.seconds_from_next_dag_run{postgres_env:production} by {dag_id} >= 1"

  monitor_thresholds {
    critical = 1
  }

  require_full_window = false
  notify_no_data      = true
  renotify_interval   = 1440

  tags = ["meltano"]
}

resource "datadog_monitor" "meltano_dag_run_duration" {
  name    = "Airflow Meltano Dag Run Duration"
  type    = "query alert"
  message = "Airflow Meltano Dag Run Duration triggered. Notify: @slack-${var.slack_channel_name} <!channel>"

  query = "avg(last_1h):avg:app.latest_dag_run_duration_minutes{*} by {dag_id}.rollup(avg, 3600) / day_before(avg:app.latest_dag_run_duration_minutes{*} by {dag_id}.rollup(avg, 3600)) > 5"

  monitor_thresholds {
    critical = 5
  }

  require_full_window = false
  notify_no_data      = true
  no_data_timeframe   = 120
  renotify_interval   = 720

  tags = ["meltano"]
}

resource "datadog_monitor" "meltano_failed_tasks" {
  name    = "Airflow Meltano Failed Tasks"
  type    = "query alert"
  message = "Airflow Meltano Failed Tasks triggered. Notify: @slack-${var.slack_channel_name} <!channel>"

  query = "max(last_5m):app.failed_tasks{postgres_env:production} by {dag_id} > 0.2"

  monitor_thresholds {
    critical          = "0.2"
    critical_recovery = "0.15"
  }

  require_full_window = false
  notify_no_data      = true
  renotify_interval   = 1440
  no_data_timeframe   = 120

  tags = ["meltano"]
}

#################################### App ####################################

resource "datadog_monitor" "data_errors_count" {
  name    = "Data errors"
  type    = "query alert"
  message = "Data errors triggered. Notify: @slack-${var.slack_channel_name} <!channel>"

  query = "sum(last_1d):avg:app.data_errors_count{postgres_env:production} by {code}.rollup(avg, 3600) / day_before(avg:app.data_errors_count{postgres_env:production} by {code}.rollup(avg, 3600)) - 1 > 1"

  monitor_thresholds {
    critical = "1"
  }

  require_full_window = false
  notify_no_data      = false
  renotify_interval   = 1440

  tags = ["meltano"]
}

#################################### CloudWatch ####################################

resource "datadog_monitor" "cloudwatch_synthetics_success_percent" {
  name    = "CloudWatch Synthetics Success Percent"
  type    = "metric alert"
  message = "CloudWatch Synthetics Success Percent. Notify: @slack-${var.slack_channel_name} <!channel>"

  query = "avg(last_15m):min:cloudwatchsynthetics.SuccessPercent{canaryname:*-production} by {canaryname} < 80"

  monitor_thresholds {
    warning_recovery  = 100
    warning           = 90
    critical_recovery = 85
    critical          = 80
  }

  require_full_window = false
  notify_no_data      = true
  renotify_interval   = 60

  tags = ["canaries"]
}

resource "datadog_monitor" "cloudwatch_synthetics_duration" {
  name    = "CloudWatch Synthetics Duration"
  type    = "query alert"
  message = "CloudWatch Synthetics Duration triggered. Notify: @slack-${var.slack_channel_name} <!channel>"

  query = "avg(last_10d):cloudwatchsynthetics.Duration{canaryname:*-production} by {canaryname}.rollup(avg, 1800) / hour_before(cloudwatchsynthetics.Duration{canaryname:*-production} by {canaryname}.rollup(avg, 1800)) - 1 > 1"

  monitor_thresholds {
    critical          = "1"
    critical_recovery = "0.5"
  }

  require_full_window = false
  notify_no_data      = true
  renotify_interval   = 240
  no_data_timeframe   = 60

  tags = ["canaries"]
}

#################################### Logs ####################################

resource "datadog_monitor" "logs" {
  name    = "Error Logs"
  type    = "log alert"
  message = <<-EOT
  Error Log. Notify: @slack-${var.slack_channel_name} <!channel>
  {{log.message}}
  {{log.service}}
  {{log.status}}
  {{log.link}}
EOT

  query = "logs(\"status:error host:*production -@type:http-log\").index(\"*\").rollup(\"count\").by(\"host\").last(\"5m\") > 0"

  monitor_thresholds {
    critical = 0
  }

  require_full_window = false
  renotify_interval   = 15

  tags = ["logs"]
}

resource "datadog_monitor" "logs_count" {
  name    = "Logs count"
  type    = "query alert"
  message = "Logs count triggered. Notify: @slack-${var.slack_channel_name} <!channel>"

  query = "sum(last_1d):clamp_min(sum:aws.logs.forwarded_log_events{!loggroupname:/aws/lambda/sqs*,!loggroupname:*_test} by {loggroupname}.rollup(sum, 86400), 1000) / clamp_min(day_before(sum:aws.logs.forwarded_log_events{!loggroupname:/aws/lambda/sqs*,!loggroupname:*_test} by {loggroupname}.rollup(sum, 86400)), 1000) - 1 > 1"

  monitor_thresholds {
    critical = 1
  }

  require_full_window = false
  renotify_interval   = 360
  evaluation_delay    = 900

  tags = ["logs"]
}
