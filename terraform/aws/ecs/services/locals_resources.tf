# https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html
locals {
  eod_websockets_cpu_credits     = var.env == "production" ? 128 : 0
  polygon_websockets_cpu_credits = var.env == "production" ? 128 : 0
  hasura_cpu_credits             = var.env == "production" ? 1024 : 256
  meltano_ui_cpu_credits         = var.env == "production" ? 256 : 128
  meltano_scheduler_cpu_credits  = var.env == "production" ? 2048 : 512
  main_cpu_credits = ceil(sum([
    local.eod_websockets_cpu_credits,
    local.polygon_websockets_cpu_credits,
    local.hasura_cpu_credits,
    local.meltano_ui_cpu_credits,
    local.meltano_scheduler_cpu_credits,
  ]) / 1024) * 1024

  upstream_pool_size                  = 4
  eodhistoricaldata_jobs_count        = 3
  eodhistoricaldata_prices_jobs_count = 1
  coingecko_jobs_count                = 1
  polygon_jobs_count                  = 6

  eod_symbols_limit                 = var.env == "production" ? 14000 : 20
  eod_websockets_memory_credits     = var.env == "production" ? 512 : 256
  polygon_websockets_memory_credits = var.env == "production" ? 1024 : 0
  hasura_memory_credits             = var.env == "production" ? 2048 : 1024
  meltano_ui_memory_credits         = var.env == "production" ? 1024 : 1024
  meltano_scheduler_memory_credits  = var.env == "production" ? 3584 : 2816
  main_memory_credits = ceil(sum([
    local.hasura_memory_credits,
    local.meltano_ui_memory_credits,
    local.meltano_scheduler_memory_credits,
    local.eod_websockets_memory_credits,
    local.polygon_websockets_memory_credits,
  ]) / 1024) * 1024
}
