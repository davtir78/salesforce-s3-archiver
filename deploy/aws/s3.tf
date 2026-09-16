resource "aws_kms_key" "archive" {
  description             = "${var.name} archive encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true
}

resource "aws_kms_alias" "archive" {
  name          = "alias/${var.name}-archive"
  target_key_id = aws_kms_key.archive.key_id
}

resource "aws_s3_bucket" "archive" {
  bucket        = "${var.name}-archive-${data.aws_caller_identity.current.account_id}"
  force_destroy = var.force_destroy
  # Object Lock can only be enabled when the bucket is created; changing it
  # later replaces the bucket.
  object_lock_enabled = var.object_lock_mode != ""
}

# Write-once retention for archived evidence. GOVERNANCE can be overridden by a
# user with the bypass permission; COMPLIANCE cannot be overridden by anyone,
# including the root account, until the retention period expires.
resource "aws_s3_bucket_object_lock_configuration" "archive" {
  count  = var.object_lock_mode == "" ? 0 : 1
  bucket = aws_s3_bucket.archive.id
  rule {
    default_retention {
      mode = var.object_lock_mode
      days = var.object_lock_days
    }
  }
  depends_on = [aws_s3_bucket_versioning.archive]
}

resource "aws_s3_bucket_ownership_controls" "archive" {
  bucket = aws_s3_bucket.archive.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "archive" {
  bucket                  = aws_s3_bucket.archive.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "archive" {
  bucket = aws_s3_bucket.archive.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.archive.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id
  rule {
    id     = "noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }
}

data "aws_iam_policy_document" "archive_bucket" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.archive.arn,
      "${aws_s3_bucket.archive.arn}/*",
    ]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "archive" {
  bucket     = aws_s3_bucket.archive.id
  policy     = data.aws_iam_policy_document.archive_bucket.json
  depends_on = [aws_s3_bucket_public_access_block.archive]
}
