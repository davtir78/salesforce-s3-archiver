variable "region" {
  description = "AWS region."
  type        = string
  default     = "ap-southeast-2"
}

variable "name" {
  description = "Name prefix for all resources."
  type        = string
  default     = "sfarchive-test"
}

variable "image_tag" {
  description = "Tag of the collector image in the ECR repository. Set by scripts/aws-deploy.sh to the commit SHA; there is no default because ECR tags are immutable and \"latest\" never exists."
  type        = string
}

variable "admin_cidr" {
  description = "CIDR allowed to reach the mock Salesforce admin API (e.g. your IP /32). Empty disables public access."
  type        = string
  default     = ""
}

variable "run_id" {
  description = "Test run identifier. Changing it gives collectors a fresh instance name (checkpoint and watermark keys) and S3 prefix, e.g. after the mock restarts with a new ledger."
  type        = string
  default     = "1"
}

variable "use_mock_salesforce" {
  description = "Deploy the mock Salesforce service and point collectors at it."
  type        = bool
  default     = true
}

variable "salesforce_token_url" {
  description = "Salesforce My Domain URL when not using the mock (e.g. https://example.my.salesforce.com)."
  type        = string
  default     = ""
  validation {
    condition     = var.use_mock_salesforce || (var.salesforce_token_url != "" && var.salesforce_client_id != "" && var.salesforce_client_secret != "")
    error_message = "With use_mock_salesforce = false, salesforce_token_url, salesforce_client_id and salesforce_client_secret are all required."
  }
}

variable "salesforce_client_id" {
  description = "Connected app client ID when not using the mock."
  type        = string
  default     = ""
}

variable "salesforce_client_secret" {
  description = "Connected app client secret when not using the mock."
  type        = string
  default     = ""
  sensitive   = true
}

variable "stream_topics" {
  description = "Pub/Sub topics archived by the stream collector."
  type        = list(string)
  default     = ["/event/LoginEventStream", "/event/ApiEventStream"]
}

variable "cache_node_type" {
  description = "ElastiCache node type."
  type        = string
  default     = "cache.t4g.micro"
}

variable "force_destroy" {
  description = "Allow terraform destroy to delete a non-empty archive bucket. Leave false for anything holding real data; scripts/aws-destroy.sh sets it only with FORCE_DESTROY_BUCKET=true."
  type        = bool
  default     = false
}

variable "cache_snapshot_retention_days" {
  description = "Automatic ElastiCache snapshot retention. Test stacks use 0 because automatic snapshots can outlive a destroyed replication group."
  type        = number
  default     = 0
}

variable "object_lock_mode" {
  description = "S3 Object Lock mode for archived data: \"\" (off), GOVERNANCE or COMPLIANCE. Can only be set when the bucket is created."
  type        = string
  default     = ""
  validation {
    condition     = contains(["", "GOVERNANCE", "COMPLIANCE"], var.object_lock_mode)
    error_message = "object_lock_mode must be empty, GOVERNANCE or COMPLIANCE."
  }
}

variable "object_lock_days" {
  description = "Retention period in days when object_lock_mode is set."
  type        = number
  default     = 365
}

variable "noncurrent_version_days" {
  description = "How long overwritten object versions are kept. Overwrites should not happen, so this is a recovery window."
  type        = number
  default     = 365
}

variable "stream_initial_replay" {
  description = "Where stream topics start when no checkpoint exists: EARLIEST, LATEST, or empty to refuse to start (the safe steady-state setting). Ignored when use_mock_salesforce is true, which always bootstraps from EARLIEST."
  type        = string
  default     = ""
  validation {
    condition     = contains(["", "EARLIEST", "LATEST"], var.stream_initial_replay)
    error_message = "stream_initial_replay must be empty, EARLIEST or LATEST."
  }
}

variable "alarm_email" {
  description = "Address subscribed to the alarm topic. Empty creates the topic without a subscription."
  type        = string
  default     = ""
}
