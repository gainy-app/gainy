# https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html
locals {
  eod_websockets_cpu_credits     = var.env == "production" ? 128 : 0
  polygon_websockets_cpu_credits = var.env == "production" ? 128 : 0
  hasura_cpu_credits             = var.env == "production" ? 1024 : 256
  meltano_ui_cpu_credits         = var.env == "production" ? 256 : 128
  meltano_scheduler_cpu_credits  = var.env == "production" ? 2048 : 256
  main_cpu_credits = ceil(sum([
    local.eod_websockets_cpu_credits,
    local.polygon_websockets_cpu_credits,
    local.hasura_cpu_credits,
    local.meltano_ui_cpu_credits,
    local.meltano_scheduler_cpu_credits,
  ]) / 1024) * 1024

  downstream_pool_size         = var.env == "production" ? 4 : 3
  eodhistoricaldata_jobs_count = 4
  coingecko_jobs_count         = 2

  eod_websockets_memory_credits     = var.env == "production" ? 512 : 0
  polygon_websockets_memory_credits = var.env == "production" ? 768 : 0
  hasura_memory_credits             = var.env == "production" ? 2048 : 1024
  meltano_ui_memory_credits         = var.env == "production" ? 1024 : 1024
  meltano_scheduler_memory_credits  = var.env == "production" ? 3072 : 3072
  main_memory_credits = ceil(sum([
    local.hasura_memory_credits,
    local.meltano_ui_memory_credits,
    local.meltano_scheduler_memory_credits,
    local.eod_websockets_memory_credits,
    local.polygon_websockets_memory_credits,
  ]) / 1024) * 1024
}
