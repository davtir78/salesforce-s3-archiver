output "region" {
  value = var.region
}

output "cluster" {
  value = aws_ecs_cluster.this.name
}

output "ecr_repository_url" {
  value = aws_ecr_repository.this.repository_url
}

output "bucket" {
  value = aws_s3_bucket.archive.bucket
}

output "replication_group_id" {
  value = aws_elasticache_replication_group.this.id
}

output "subnets" {
  value = aws_subnet.public[*].id
}

output "task_security_group" {
  value = aws_security_group.tasks.id
}

output "verify_task_definition" {
  value = aws_ecs_task_definition.verify.arn
}

output "log_group" {
  value = aws_cloudwatch_log_group.this.name
}
