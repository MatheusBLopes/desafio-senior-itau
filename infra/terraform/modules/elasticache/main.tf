# ElastiCache for Valkey: ~20% mais barato que Redis OSS, compatível com Lua, Streams e Pub/Sub.
# MVP com 1 nó. Para HA: node_count = 2 (réplica + failover automático Multi-AZ). Durabilidade
# de lance confirmado exige MemoryDB for Valkey, não AOF (indisponível aqui).
resource "aws_elasticache_subnet_group" "this" {
  name       = var.name
  subnet_ids = var.subnet_ids
}

resource "aws_elasticache_parameter_group" "this" {
  name   = var.name
  family = "valkey8"

  # Estado de leilão nunca pode ser despejado por LRU.
  parameter {
    name  = "maxmemory-policy"
    value = "noeviction"
  }
}

resource "aws_elasticache_replication_group" "this" {
  replication_group_id       = var.name
  description                = "Motor atomico de lances (Lua)"
  engine                     = "valkey"
  engine_version             = "8.1"
  node_type                  = var.node_type
  num_cache_clusters         = var.node_count
  automatic_failover_enabled = var.node_count > 1
  multi_az_enabled           = var.node_count > 1
  parameter_group_name       = aws_elasticache_parameter_group.this.name
  subnet_group_name          = aws_elasticache_subnet_group.this.name
  security_group_ids         = [var.security_group_id]
  port                       = 6379
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  snapshot_retention_limit   = var.snapshot_retention_days
  apply_immediately          = true
}

variable "name" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "security_group_id" {
  type = string
}

variable "node_type" {
  type    = string
  default = "cache.t4g.micro"
}

variable "node_count" {
  type    = number
  default = 1
}

variable "snapshot_retention_days" {
  type    = number
  default = 0
}

output "primary_endpoint" {
  value = aws_elasticache_replication_group.this.primary_endpoint_address
}

output "replication_group_id" {
  value = aws_elasticache_replication_group.this.id
}
