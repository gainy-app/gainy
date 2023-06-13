# https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html
locals {
  eod_websockets_cpu_credits    = var.env == "production" ? 128 : 128
  hasura_cpu_credits            = var.env == "production" ? 512 : 256
  airflow_cpu_credits           = var.env == "production" ? 256 : 256
  meltano_scheduler_cpu_credits = var.env == "production" ? 8192 : 4096

  upstream_pool_size                  = var.env == "production" ? 3 : 1
  downstream_pool_size                = var.env == "production" ? 2 : 1
  polygon_pool_size                   = var.env == "production" ? 10 : 5
  eodhistoricaldata_jobs_count        = 3
  eodhistoricaldata_prices_jobs_count = 4
  coingecko_jobs_count                = 1
  polygon_jobs_count                  = 6
  polygon_intraday_jobs_count         = 10

  eod_symbols_limit                = var.env == "production" ? 900 : 20
  eod_websockets_memory_credits    = var.env == "production" ? 512 : 256
  hasura_memory_credits            = var.env == "production" ? 1024 : 1024
  airflow_memory_credits           = var.env == "production" ? 1024 : 1024
  meltano_scheduler_memory_credits = var.env == "production" ? 16384 : 8192
}
