#!/usr/bin/env bash
# Runs a one-off collector command inside the VPC, for the recovery procedures
# in docs/operations.md (the cache is not reachable from outside).
#
#   bash scripts/aws-run-task.sh stream -reset-checkpoints all -confirm
#   bash scripts/aws-run-task.sh eventlog -once
#   bash scripts/aws-run-task.sh verify
#
# Waits for the task and prints its log output.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
TF="terraform -chdir=deploy/aws"

WHICH="${1:-}"
shift || true
case "$WHICH" in
  stream)   BIN=sf-archive-stream ;;
  eventlog) BIN=sf-archive-eventlog ;;
  verify)   BIN=sf-archive-verify ;;
  *) echo "usage: $0 stream|eventlog|verify [args...]" >&2; exit 2 ;;
esac

REGION=$($TF output -raw region)
CLUSTER=$($TF output -raw cluster)
LOG_GROUP=$($TF output -raw log_group)
SUBNETS=$($TF output -json subnets | tr -d '[]" \n')
SG=$($TF output -raw task_security_group)
export AWS_REGION=$REGION

# json_string quotes a value for embedding in a JSON document.
json_string() {
  printf '%s' "$1" | awk 'BEGIN{ORS=""} {gsub(/\/,"\\\\"); gsub(/"/,"\\\""); print}'
}

OVERRIDES=/tmp/sfarchive-overrides.$$.json
trap 'rm -f "$OVERRIDES"' EXIT

if [ "$WHICH" = "verify" ]; then
  FAMILY=$($TF output -raw verify_task_definition)
  CONTAINER=verify
  ARGS=""
  for arg in "$@"; do ARGS="$ARGS,\"$(json_string "$arg")\""; done
  printf '{"containerOverrides":[{"name":"%s","command":["%s"%s]}]}' "$CONTAINER" "$BIN" "$ARGS" > "$OVERRIDES"
else
  FAMILY="$CLUSTER-$WHICH"
  CONTAINER="$WHICH-collector"
  # The config lives in the task definition's CONFIG_YAML; write it out, then
  # run the requested command against it.
  CMD="printf '%s' \"\$CONFIG_YAML\" > /tmp/config.yml && exec $BIN -config /tmp/config.yml"
  for arg in "$@"; do CMD="$CMD $arg"; done
  printf '{"containerOverrides":[{"name":"%s","command":["/bin/sh","-c","%s"]}]}' "$CONTAINER" "$(json_string "$CMD")" > "$OVERRIDES"
fi

echo "==> running $BIN $* on $CLUSTER"
ARN=$(aws ecs run-task --cluster "$CLUSTER" --launch-type FARGATE --task-definition "$FAMILY" \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SG],assignPublicIp=ENABLED}" \
  --overrides "file://$OVERRIDES" --query 'tasks[0].taskArn' --output text)
ID=${ARN##*/}
aws ecs wait tasks-stopped --cluster "$CLUSTER" --tasks "$ARN"
CODE=$(aws ecs describe-tasks --cluster "$CLUSTER" --tasks "$ARN" --query 'tasks[0].containers[0].exitCode' --output text)
echo "==> exit code $CODE; logs:"
aws logs get-log-events --log-group-name "$LOG_GROUP" --log-stream-name "ecs/$CONTAINER/$ID" \
  --query 'events[].message' --output text | tr '\t' '\n'
[ "$CODE" = "0" ]
