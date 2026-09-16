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

# json_string prints its argument as a JSON string literal. Arguments are flags
# and topic names, so control characters are rejected rather than escaped.
json_string() {
  case "$1" in
    *[[:cntrl:]]*) echo "argument contains a control character: $1" >&2; return 1 ;;
  esac
  # Quoted patterns and replacements are literal on every bash version (5.2
  # changed how unquoted backslashes in a replacement are processed).
  local bs='\' dq='"' v
  v=${1//"$bs"/"$bs$bs"}
  v=${v//"$dq"/"$bs$dq"}
  printf '"%s"' "$v"
}

# json_args prints the arguments as comma-separated JSON strings.
json_args() {
  local out="" arg q
  for arg in "$@"; do
    q=$(json_string "$arg") || return 1
    out="${out:+$out,}$q"
  done
  printf '%s' "$out"
}

OVERRIDES=$(mktemp)
trap 'rm -f "$OVERRIDES"' EXIT
# On Windows (Git Bash) the AWS CLI cannot open /tmp paths; give it a native one.
OVERRIDES_PATH=$OVERRIDES
if command -v cygpath >/dev/null 2>&1; then
  OVERRIDES_PATH=$(cygpath -m "$OVERRIDES")
fi
OVERRIDE_ARGS=(--overrides "file://$OVERRIDES_PATH")

if [ "$WHICH" = "verify" ]; then
  FAMILY=$($TF output -raw verify_task_definition)
  CONTAINER=verify
  if [ $# -eq 0 ]; then
    # Run the task definition's command unchanged.
    OVERRIDE_ARGS=()
  else
    # Extra flags are appended to the task definition's command, which carries
    # -bucket, -prefix, -region and (for the mock) -ledger. Replacing it would
    # drop them.
    BASE=$(aws ecs describe-task-definition --task-definition "$FAMILY" \
      --query 'taskDefinition.containerDefinitions[?name==`verify`].command | [0]' --output json | tr -d '\n\r')
    case "$BASE" in
      \[*\]) ;;
      *) echo "could not read the verify task definition's command: $BASE" >&2; exit 1 ;;
    esac
    EXTRA=$(json_args "$@")
    printf '{"containerOverrides":[{"name":"%s","command":%s,%s]}]}' "$CONTAINER" "${BASE%]}" "$EXTRA" > "$OVERRIDES"
  fi
else
  FAMILY="$CLUSTER-$WHICH"
  CONTAINER="$WHICH-collector"
  # with-config (in the image) writes the task definition's CONFIG_YAML to a
  # file and runs the binary against it, so the override is a plain argument
  # list with no shell quoting.
  COMMAND=$(json_args with-config "$BIN" "$@")
  printf '{"containerOverrides":[{"name":"%s","command":[%s]}]}' "$CONTAINER" "$COMMAND" > "$OVERRIDES"
fi

echo "==> running $BIN $* on $CLUSTER"
ARN=$(aws ecs run-task --cluster "$CLUSTER" --launch-type FARGATE --task-definition "$FAMILY" \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SG],assignPublicIp=ENABLED}" \
  ${OVERRIDE_ARGS[@]+"${OVERRIDE_ARGS[@]}"} --query 'tasks[0].taskArn' --output text)
ID=${ARN##*/}
aws ecs wait tasks-stopped --cluster "$CLUSTER" --tasks "$ARN"
CODE=$(aws ecs describe-tasks --cluster "$CLUSTER" --tasks "$ARN" --query 'tasks[0].containers[0].exitCode' --output text)
echo "==> exit code $CODE; logs:"
aws logs get-log-events --log-group-name "$LOG_GROUP" --log-stream-name "ecs/$CONTAINER/$ID" \
  --query 'events[].message' --output text | tr '\t' '\n'
[ "$CODE" = "0" ]
