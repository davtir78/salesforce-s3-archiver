#!/usr/bin/env bash
# Deploys the test stack to AWS: creates the ECR repository, builds and pushes
# the image, then applies the rest of the Terraform configuration.
#
#   bash scripts/aws-deploy.sh            # mock Salesforce (default)
#   TF_VARS="-var use_mock_salesforce=false -var salesforce_token_url=..." bash scripts/aws-deploy.sh
set -euo pipefail
cd "$(dirname "$0")/../deploy/aws"

ADMIN_CIDR="${ADMIN_CIDR:-$(curl -sf https://checkip.amazonaws.com | tr -d '[:space:]')/32}"
TF_VARS="${TF_VARS:-} -var admin_cidr=${ADMIN_CIDR}"
TAG="$(git rev-parse --short HEAD)"
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "Working tree has uncommitted changes; commit first so the image tag ($TAG) matches its contents." >&2
  exit 2
fi

terraform init -input=false >/dev/null
echo "==> creating ECR repository"
terraform apply -no-color -input=false -auto-approve -target=aws_ecr_repository.this $TF_VARS

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
