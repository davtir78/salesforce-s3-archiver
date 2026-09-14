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
  description = "Tag of the collector image in the ECR repository."
  type        = string
  default     = "latest"
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
