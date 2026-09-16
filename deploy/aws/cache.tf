# ElastiCache for Valkey: Multi-AZ, TLS in transit, encryption at rest, RBAC.
# Two users exercise both supported authentication modes:
#   - archiver-iam: IAM authentication (stream collector)
#   - archiver-pw:  password authentication (event log collector)

resource "random_password" "cache_default" {
  length  = 32
  special = false
}

resource "random_password" "cache_archiver" {
  length  = 32
  special = false
}

resource "aws_elasticache_subnet_group" "this" {
  name       = var.name
  subnet_ids = aws_subnet.private[*].id
}

# Every user group must contain a user named "default"; it is locked down.
resource "aws_elasticache_user" "default" {
  user_id       = "${var.name}-default"
  user_name     = "default"
  engine        = "valkey"
  access_string = "off -@all"
  authentication_mode {
    type      = "password"
    passwords = [random_password.cache_default.result]
  }
}

# Stream collector: replay checkpoints and leases only. It runs Lua scripts, so
# it needs @scripting, but not the dangerous commands (FLUSHALL, CONFIG, ...)
# that could wipe another collector's state.
resource "aws_elasticache_user" "iam" {
  # IAM authentication requires user_id == user_name.
  user_id       = "${var.name}-archiver-iam"
  user_name     = "${var.name}-archiver-iam"
  engine        = "valkey"
  access_string = "on ~${var.cache_key_prefix}sfarch:* resetchannels +@read +@write +@scripting +@connection -@dangerous"
  authentication_mode {
    type = "iam"
  }
}

# Event log collector: watermarks, tokens and de-duplication markers, all
# namespaced by instance name. It cannot touch the stream checkpoints.
resource "aws_elasticache_user" "password" {
  user_id       = "${var.name}-archiver-pw"
  user_name     = "archiver"
  engine        = "valkey"
  access_string = "on ~${var.cache_key_prefix}${local.instance}_* resetchannels +@read +@write +@connection -@dangerous"
  authentication_mode {
    type      = "password"
    passwords = [random_password.cache_archiver.result]
  }
}

resource "aws_elasticache_user_group" "this" {
  user_group_id = var.name
  engine        = "valkey"
  user_ids = [
    aws_elasticache_user.default.user_id,
    aws_elasticache_user.iam.user_id,
    aws_elasticache_user.password.user_id,
  ]
}

resource "aws_elasticache_replication_group" "this" {
  replication_group_id       = var.name
  description                = "${var.name} checkpoints and watermarks"
  engine                     = "valkey"
  engine_version             = "8.0"
  node_type                  = var.cache_node_type
  num_cache_clusters         = 2
  automatic_failover_enabled = true
  multi_az_enabled           = true
  transit_encryption_enabled = true
  transit_encryption_mode    = "required"
  at_rest_encryption_enabled = true
  user_group_ids             = [aws_elasticache_user_group.this.user_group_id]
  subnet_group_name          = aws_elasticache_subnet_group.this.name
  security_group_ids         = [aws_security_group.cache.id]
  port                       = 6379
  parameter_group_name       = aws_elasticache_parameter_group.this.name
  apply_immediately          = true
  snapshot_retention_limit   = var.cache_snapshot_retention_days
}

resource "aws_elasticache_parameter_group" "this" {
  name   = var.name
  family = "valkey8"

  # Checkpoints and watermarks must never be evicted.
  parameter {
    name  = "maxmemory-policy"
    value = "noeviction"
  }
}

resource "aws_ssm_parameter" "cache_password" {
  name  = "/${var.name}/cache/archiver-password"
  type  = "SecureString"
  value = random_password.cache_archiver.result
}
