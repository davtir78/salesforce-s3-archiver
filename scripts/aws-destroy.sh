#!/usr/bin/env bash
# Destroys the AWS stack and lists anything still tagged with the project.
#
#   CONFIRM_DESTROY=sfarchive-test bash scripts/aws-destroy.sh
#   CONFIRM_DESTROY=sfarchive-test FORCE_DESTROY_BUCKET=true bash scripts/aws-destroy.sh   # also delete archived data
#
# Without FORCE_DESTROY_BUCKET=true a non-empty archive bucket makes the destroy
# fail, so archived data is never deleted by accident.
set -euo pipefail
cd "$(dirname "$0")/../deploy/aws"
export MSYS_NO_PATHCONV=1

STACK="$(terraform output -raw cluster 2>/dev/null || true)"
if [ -z "$STACK" ]; then
  echo "No stack found in Terraform state; nothing to destroy." >&2
  exit 1
fi
if [ "${CONFIRM_DESTROY:-}" != "$STACK" ]; then
  echo "Refusing to destroy stack '$STACK'. Re-run with CONFIRM_DESTROY=$STACK" >&2
  exit 2
fi

REGION="$(terraform output -raw region)"
BUCKET="$(terraform output -raw bucket)"
# Required variables need a value even to destroy; these do not affect it.
VARS=(-var admin_cidr=127.0.0.1/32 -var image_tag=destroy)

if [ "${FORCE_DESTROY_BUCKET:-false}" = "true" ]; then
  echo "==> FORCE_DESTROY_BUCKET=true: s3://$BUCKET and ALL archived objects will be deleted"
  VARS+=(-var force_destroy=true)
  # force_destroy must be recorded in state before destroy can use it.
  terraform apply -no-color -input=false -auto-approve -target=aws_s3_bucket.archive "${VARS[@]}"
else
  counts="$(aws s3api list-object-versions --bucket "$BUCKET" --max-items 1 --query '[length(Versions || `[]`), length(DeleteMarkers || `[]`)]' --output text)"
  if [ "$(echo "$counts" | tr -d '[:space:]')" != "00" ]; then
    echo "s3://$BUCKET is not empty. Destroying would fail; set FORCE_DESTROY_BUCKET=true to delete archived data, or empty/retain the bucket first." >&2
    exit 3
  fi
fi

terraform destroy -no-color -input=false -auto-approve "${VARS[@]}"

echo "==> resources still tagged Project=salesforce-s3-archiver (KMS keys pending deletion are expected):"
aws resourcegroupstaggingapi get-resources --region "$REGION" \
  --tag-filters Key=Project,Values=salesforce-s3-archiver \
  --query 'ResourceTagMappingList[].ResourceARN' --output text
