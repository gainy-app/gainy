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

#################################### Billing ####################################

resource "datadog_monitor" "billing_spend" {
  name    = "Billing Spend"
  type    = "query alert"
  message = "Billing Spend Monitor triggered. Notify: @slack-${var.slack_channel_name} <!channel>"
  #  escalation_message = "Escalation message @pagerduty"

  query = "avg(last_1d):anomalies(sum:aws.billing.forecasted_spend{*}, 'basic', 2, direction='above', alert_window='last_12h', interval=300, count_default_zero='true') > 0.1"

  monitor_threshold_windows {
    recovery_window = "last_12h"
    trigger_window  = "last_12h"
  }

  monitor_thresholds {
    critical          = "0.1"
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

  query = "avg(last_1h):anomalies(sum:aws.applicationelb.httpcode_target_5xx{name:*-production}.as_count(), 'basic', 2, direction='above', alert_window='last_15m', interval=300, count_default_zero='true') > 0.01"

  monitor_threshold_windows {
    recovery_window = "last_15m"
    trigger_window  = "last_15m"
  }

  monitor_thresholds {
    critical          = "0.01"
    critical_recovery = "0"
  }

  require_full_window = false
  notify_no_data      = false
  renotify_interval   = 60

  tags = ["hasura"]
}

resource "datadog_monitor" "hasura_alb_active_connections" {
  name    = "Hasura Active Connections"
  type    = "query alert"
  message = "Hasura Active Connections Monitor triggered. Notify: @slack-${var.slack_channel_name} <!channel>"
  #  escalation_message = "Escalation message @pagerduty"

  query = "avg(last_7d):anomalies(sum:aws.applicationelb.active_connection_count{name:*-production}.as_count(), 'basic', 2, direction='both', alert_window='last_1d', interval=300, count_default_zero='true') > 0.8"

  monitor_threshold_windows {
    recovery_window = "last_1d"
    trigger_window  = "last_1d"
  }

  monitor_thresholds {
    critical          = "0.8"
    critical_recovery = "0.7"
    warning           = "0.5"
    warning_recovery  = "0.4"
  }

  require_full_window = true
  notify_no_data      = false
  renotify_interval   = 240

  tags = ["hasura"]
}

#################################### ECS ####################################

resource "datadog_monitor" "ecs_cpu" {
  name    = "ECS CPU Utilization"
  type    = "metric alert"
  message = "ECS CPU Utilization Monitor triggered. Notify: @slack-${var.slack_channel_name} <!channel>"
  #  escalation_message = "Escalation message @pagerduty"

  query = "avg(last_1h):avg:aws.ecs.service.cpuutilization{servicename:gainy-production} > 40"

  monitor_thresholds {
    warning_recovery  = 10
    warning           = 20
    critical_recovery = 30
    critical          = 40
  }

  require_full_window = false
  notify_no_data      = true
  renotify_interval   = 15

  tags = ["hasura"]
}

resource "datadog_monitor" "hasura_memory" {
  name    = "ECS Memory Utilization"
  type    = "metric alert"
  message = "ECS Memory Utilization Monitor triggered. Notify: @slack-${var.slack_channel_name} <!channel>"
  #  escalation_message = "Escalation message @pagerduty"

  query = "avg(last_1h):avg:aws.ecs.service.memory_utilization{servicename:gainy-production} > 50"

  monitor_thresholds {
    warning_recovery  = 35
    warning           = 40
    critical_recovery = 45
    critical          = 50
  }

  require_full_window = false
  notify_no_data      = true
  renotify_interval   = 15

  tags = ["hasura"]
}

resource "datadog_monitor" "healthy_hosts" {
  name    = "ECS Healthy Hosts"
  type    = "metric alert"
  message = "ECS Healthy Hosts Monitor triggered. Notify: @slack-${var.slack_channel_name} <!channel>"
  #  escalation_message = "Escalation message @pagerduty"

  query = "avg(last_1h):min:aws.applicationelb.healthy_host_count{name:*-production} by {name} < 1"

  monitor_thresholds {
    critical = 1
  }

  require_full_window = false
  notify_no_data      = true
  renotify_interval   = 15

  tags = ["hasura"]
}

#################################### Lambda ####################################

resource "datadog_monitor" "lambda_invocations" {
  name    = "Lambda Invocations"
  type    = "query alert"
  message = "Lambda Invocations Monitor triggered. Notify: @slack-${var.slack_channel_name} <!channel>"
  #  escalation_message = "Escalation message @pagerduty"

  query = "avg(last_7d):anomalies(sum:aws.lambda.invocations{resource:*_production} by {functionname}.as_count(), 'basic', 2, direction='above', alert_window='last_1d', interval=300, count_default_zero='true') > 1"

  monitor_threshold_windows {
    recovery_window = "last_1d"
    trigger_window  = "last_1d"
  }

  monitor_thresholds {
    critical          = "1"
    critical_recovery = "0.9"
    warning           = "0.5"
    warning_recovery  = "0.45"
  }

  require_full_window = false
  notify_no_data      = false
  renotify_interval   = 60

  tags = ["lambda"]
}

resource "datadog_monitor" "lambda_duration" {
  name    = "Lambda Duration"
  type    = "query alert"
  message = "Lambda Duration Monitor triggered. Notify: @slack-${var.slack_channel_name} <!channel>"
  #  escalation_message = "Escalation message @pagerduty"

  query = "avg(last_7d):anomalies(sum:aws.lambda.duration{resource:*_production} by {functionname}.as_count(), 'basic', 2, direction='above', alert_window='last_2d', interval=300, count_default_zero='true') > 0.7"

  monitor_threshold_windows {
    recovery_window = "last_2d"
    trigger_window  = "last_2d"
  }

  monitor_thresholds {
    critical          = "0.7"
    critical_recovery = "0.6"
    warning           = "0.5"
    warning_recovery  = "0.4"
  }

  require_full_window = true
  notify_no_data      = false
  renotify_interval   = 720

  tags = ["lambda"]
}

resource "datadog_monitor" "lambda_errors" {
  name    = "Lambda Errors"
  type    = "query alert"
  message = "Lambda Errors Monitor triggered. Notify: @slack-${var.slack_channel_name} <!channel>"
  #  escalation_message = "Escalation message @pagerduty"

  query             = "avg(last_7d):anomalies(sum:aws.lambda.errors{resource:*_production} by {functionname}.as_count(), 'basic', 2, direction='above', alert_window='last_1h', interval=300, count_default_zero='true') >= 0.01"
  no_data_timeframe = 120

  monitor_threshold_windows {
    recovery_window = "last_1h"
    trigger_window  = "last_1h"
  }

  monitor_thresholds {
    critical          = "0.01"
    critical_recovery = "0"
  }

  require_full_window = true
  notify_no_data      = false
  renotify_interval   = 15

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
  #  escalation_message = "Escalation message @pagerduty"

  query = "avg(last_14d):anomalies(sum:aws.rds.cpuutilization{dbinstanceidentifier:*-production}, 'basic', 2, direction='above', alert_window='last_4d', interval=300, count_default_zero='true') > 0.8"

  monitor_threshold_windows {
    recovery_window = "last_4d"
    trigger_window  = "last_4d"
  }

  monitor_thresholds {
    critical          = "0.8"
    critical_recovery = "0.7"
    warning           = "0.5"
    warning_recovery  = "0.4"
  }

  require_full_window = false
  notify_no_data      = true
  renotify_interval   = 15

  tags = ["rds"]
}

#################################### Meltano ####################################

resource "datadog_monitor" "meltano_dag_run_date" {
  name    = "Airflow Meltano Dag Run Date"
  type    = "metric alert"
  message = "Airflow Meltano Dag Run Date triggered. Notify: @slack-${var.slack_channel_name} <!channel>"

  query = "max(last_5m):min:app.days_from_latest_dag_run{postgres_env:production} by {dag_id} >= 1"

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

  query = "avg(last_1d):anomalies(sum:app.latest_dag_run_duration_minutes{postgres_env:production} by {dag_id}.as_count(), 'basic', 2, direction='above', alert_window='last_3h', interval=300, count_default_zero='true') > 0.35"

  monitor_threshold_windows {
    recovery_window = "last_3h"
    trigger_window  = "last_3h"
  }

  monitor_thresholds {
    critical          = "0.35"
    critical_recovery = "0.3"
  }

  require_full_window = false
  notify_no_data      = true
  renotify_interval   = 720

  tags = ["meltano"]
}

resource "datadog_monitor" "meltano_failed_dag_runs" {
  name    = "Airflow Meltano Failed Dag Runs"
  type    = "query alert"
  message = "Airflow Meltano Failed Dag Runs triggered. Notify: @slack-${var.slack_channel_name} <!channel>"

  query = "avg(last_1d):anomalies(sum:app.failed_dag_runs{postgres_env:production} by {dag_id}.as_count(), 'basic', 2, direction='above', alert_window='last_1h', interval=300, count_default_zero='true') > 0.2"

  monitor_threshold_windows {
    recovery_window = "last_1h"
    trigger_window  = "last_1h"
  }

  monitor_thresholds {
    critical          = "0.2"
    critical_recovery = "0.15"
  }

  require_full_window = false
  notify_no_data      = true
  renotify_interval   = 240

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
    warning           = 95
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

  query = "avg(last_10d):anomalies(avg:cloudwatchsynthetics.Duration{canaryname:*-production} by {canaryname}.as_count(), 'basic', 2, direction='above', alert_window='last_1d', interval=300, count_default_zero='true') > 0.5"

  monitor_threshold_windows {
    recovery_window = "last_1d"
    trigger_window  = "last_1d"
  }

  monitor_thresholds {
    critical          = "0.5"
    critical_recovery = "0.4"
  }

  require_full_window = false
  notify_no_data      = true
  renotify_interval   = 240

  tags = ["canaries"]
}
