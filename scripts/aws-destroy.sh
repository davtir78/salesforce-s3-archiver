#!/usr/bin/env bash
# Destroys the AWS test stack and lists anything still tagged with the project.
set -euo pipefail
cd "$(dirname "$0")/../deploy/aws"

REGION="$(terraform output -raw region 2>/dev/null || echo ap-southeast-2)"
terraform destroy -input=false -auto-approve -var admin_cidr=127.0.0.1/32

echo "==> resources still tagged Project=salesforce-s3-archiver (KMS keys pending deletion are expected):"
aws resourcegroupstaggingapi get-resources --region "$REGION" \
  --tag-filters Key=Project,Values=salesforce-s3-archiver \
  --query 'ResourceTagMappingList[].ResourceARN' --output text
