data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# Execution role: pull images, write logs, read secrets.
resource "aws_iam_role" "execution" {
  name               = "${var.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "execution_secrets" {
  statement {
    actions   = ["ssm:GetParameters"]
    resources = [aws_ssm_parameter.cache_password.arn, aws_ssm_parameter.sf_client_secret.arn]
  }
}

resource "aws_iam_role_policy" "execution_secrets" {
  name   = "secrets"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution_secrets.json
}

# Collector task role: write the archive, connect to ElastiCache with IAM.
resource "aws_iam_role" "collector" {
  name               = "${var.name}-collector"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

# Writes are limited to this run's prefix, so a collector cannot overwrite
# another run's archive (or its manifests).
data "aws_iam_policy_document" "collector" {
  statement {
    sid       = "ArchiveWrite"
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
    resources = ["${aws_s3_bucket.archive.arn}/${local.prefix}/*"]
  }
  statement {
    sid       = "ArchiveKms"
    actions   = ["kms:GenerateDataKey", "kms:Decrypt"]
    resources = [aws_kms_key.archive.arn]
  }
}

resource "aws_iam_role_policy" "collector" {
  name   = "collector"
  role   = aws_iam_role.collector.id
  policy = data.aws_iam_policy_document.collector.json
}

# Only the stream collector authenticates to ElastiCache with IAM; the event
# log collector uses a password and a different Valkey user.
resource "aws_iam_role" "stream" {
  name               = "${var.name}-stream"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

data "aws_iam_policy_document" "stream" {
  source_policy_documents = [data.aws_iam_policy_document.collector.json]
  statement {
    sid     = "ElastiCacheIamAuth"
    actions = ["elasticache:Connect"]
    resources = [
      aws_elasticache_replication_group.this.arn,
      aws_elasticache_user.iam.arn,
    ]
  }
}

resource "aws_iam_role_policy" "stream" {
  name   = "stream-collector"
  role   = aws_iam_role.stream.id
  policy = data.aws_iam_policy_document.stream.json
}

# Verification task role: read-only access to the archive.
resource "aws_iam_role" "verify" {
  name               = "${var.name}-verify"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

data "aws_iam_policy_document" "verify" {
  statement {
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.archive.arn, "${aws_s3_bucket.archive.arn}/*"]
  }
  statement {
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.archive.arn]
  }
}

resource "aws_iam_role_policy" "verify" {
  name   = "verify"
  role   = aws_iam_role.verify.id
  policy = data.aws_iam_policy_document.verify.json
}
