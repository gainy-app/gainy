variable "private_subnet_ids" {}
variable "vpc_default_sg_id" {}
variable "env" {}

resource "aws_elasticache_subnet_group" "elasticache_subnet_group" {
  name       = "elasticache-subnet-group-${var.env}"
  subnet_ids = var.private_subnet_ids
}

resource "aws_elasticache_cluster" "redis_cache" {
  cluster_id           = "redis-cache-${var.env}"
  engine               = "redis"
  parameter_group_name = "default.redis6.x"
  node_type            = var.env == "production" ? "cache.t4g.medium" : "cache.t4g.micro"
  num_cache_nodes      = 1
  port                 = 6379
  subnet_group_name    = aws_elasticache_subnet_group.elasticache_subnet_group.name
  security_group_ids   = [var.vpc_default_sg_id]
  maintenance_window   = "sun:05:00-sun:06:00"
}

output "redis_cache_host" {
  value = aws_elasticache_cluster.redis_cache.cache_nodes[0].address
}

output "redis_cache_port" {
  value = aws_elasticache_cluster.redis_cache.cache_nodes[0].port
}