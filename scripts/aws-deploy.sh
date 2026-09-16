#!/usr/bin/env bash
# Deploys the test stack to AWS: creates the ECR repository, builds and pushes
# the image, then applies the rest of the Terraform configuration.
#
#   bash scripts/aws-deploy.sh            # mock Salesforce (default)
#   TF_VARS="-var use_mock_salesforce=false -var salesforce_token_url=..." bash scripts/aws-deploy.sh
set -euo pipefail
cd "$(dirname "$0")/../deploy/aws"

# Real deployments must keep their settings in deploy/aws/terraform.tfvars
# (gitignored) or TF_VAR_* environment variables. Without this check, running
# the script with no arguments would silently switch a real deployment back to
# the mock org, overwrite its client secret and mix mock data into the archive.
if [ ! -f terraform.tfvars ] && [ -z "${TF_VAR_use_mock_salesforce:-}" ] && [ -z "${MOCK:-}" ]; then
  cat >&2 <<'USAGE'
Refusing to deploy without an explicit mode.

  MOCK=1 bash scripts/aws-deploy.sh          # mock Salesforce (test stack)

or create deploy/aws/terraform.tfvars:

  use_mock_salesforce      = false
  salesforce_token_url     = "https://example.my.salesforce.com"
  salesforce_client_id     = "..."
  stream_initial_replay    = ""   # EARLIEST once, to bootstrap

and put the client secret in TF_VAR_salesforce_client_secret.
USAGE
  exit 2
fi

if [ -n "${MOCK:-}" ]; then
  TF_VARS="${TF_VARS:-} -var use_mock_salesforce=true"
fi

if ! ADMIN_IP="${ADMIN_CIDR:-$(curl -sf https://checkip.amazonaws.com | tr -d '[:space:]')/32}"; then
  echo "Could not determine your public IP; set ADMIN_CIDR=x.x.x.x/32" >&2
  exit 1
fi
TF_VARS="${TF_VARS:-} -var admin_cidr=${ADMIN_IP}"
TAG="$(git rev-parse --short HEAD)"
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "Working tree has uncommitted changes; commit first so the image tag ($TAG) matches its contents." >&2
  exit 2
fi

terraform init -input=false >/dev/null
echo "==> creating ECR repository"
# image_tag is required by the configuration, even for a targeted apply.
terraform apply -no-color -input=false -auto-approve -target=aws_ecr_repository.this $TF_VARS -var image_tag="$TAG"

REPO="$(terraform output -raw ecr_repository_url)"
# <account>.dkr.ecr.<region>.amazonaws.com/<name>
REGISTRY="${REPO%%/*}"
REGION="$(echo "$REGISTRY" | cut -d. -f4)"
if aws ecr describe-images --region "$REGION" --repository-name "${REPO#*/}" --image-ids imageTag="$TAG" >/dev/null 2>&1; then
  echo "==> image ${REPO}:${TAG} already exists (tags are immutable); skipping build"
else
  echo "==> building and pushing ${REPO}:${TAG}"
  aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REGISTRY"
  docker build --platform linux/amd64 --build-arg VERSION="$TAG" -t "${REPO}:${TAG}" ../..
  docker push "${REPO}:${TAG}"
fi

echo "==> applying stack"
terraform apply -no-color -input=false -auto-approve $TF_VARS -var image_tag="$TAG"
terraform output
