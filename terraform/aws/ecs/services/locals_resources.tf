# https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html
locals {
  eod_websockets_cpu_credits     = var.env == "production" ? 128 : 128
  polygon_websockets_cpu_credits = var.env == "production" ? 128 : 0
  hasura_cpu_credits             = var.env == "production" ? 512 : 256
  airflow_cpu_credits            = var.env == "production" ? 256 : 128
  meltano_scheduler_cpu_credits  = var.env == "production" ? 3072 : 2048

  upstream_pool_size                  = var.env == "production" ? 3 : 1
  downstream_pool_size                = var.env == "production" ? 2 : 1
  eodhistoricaldata_jobs_count        = 3
  eodhistoricaldata_prices_jobs_count = 4
  coingecko_jobs_count                = 1
  polygon_jobs_count                  = 6

  eod_symbols_limit                 = var.env == "production" ? 14000 : 20
  eod_websockets_memory_credits     = var.env == "production" ? 512 : 256
  polygon_websockets_memory_credits = var.env == "production" ? 1024 : 0
  hasura_memory_credits             = var.env == "production" ? 1024 : 1024
  airflow_memory_credits            = var.env == "production" ? 1024 : 1024
  meltano_scheduler_memory_credits  = var.env == "production" ? 6144 : 4096
}
