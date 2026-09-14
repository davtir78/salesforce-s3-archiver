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
  description = "Allow terraform destroy to delete a non-empty bucket (test stacks only)."
  type        = bool
  default     = true
}
