#!/usr/bin/env bash
# Chaos and reconciliation test for the AWS test stack (mock Salesforce).
#
# While the mock generates data: stops collector tasks (ECS restarts them),
# fails over ElastiCache, temporarily denies S3 writes with a bucket policy,
# and injects Salesforce faults. Then drains and runs the verify task.
#
#   DURATION=1200 bash scripts/chaos-aws.sh
set -uo pipefail
cd "$(dirname "$0")/.."
# Git Bash on Windows rewrites arguments that look like paths (e.g. /ecs/...).
export MSYS_NO_PATHCONV=1
TF="terraform -chdir=deploy/aws"

DURATION=${DURATION:-1200}
# Space-separated subset of: stop-stream stop-eventlog failover s3-deny stream-drop rest-faults
ACTIONS=(${CHAOS_ACTIONS:-stop-stream stop-stream stop-eventlog failover s3-deny stream-drop rest-faults})
EPS=${EPS:-50}
OUT=${OUT:-dev/chaos-aws}
mkdir -p "$OUT"
LOG="$OUT/chaos.log"
: > "$LOG"
log() { echo "$(date -u +%H:%M:%S) $*" | tee -a "$LOG"; }

REGION=$($TF output -raw region)
CLUSTER=$($TF output -raw cluster)
BUCKET=$($TF output -raw bucket)
RG=$($TF output -raw replication_group_id)
LOG_GROUP=$($TF output -raw log_group)
VERIFY_TD=$($TF output -raw verify_task_definition)
SUBNETS=$($TF output -json subnets | tr -d '[]" \n')
SG=$($TF output -raw task_security_group)
export AWS_REGION=$REGION

task_of() { aws ecs list-tasks --cluster "$CLUSTER" --service-name "$1" --desired-status RUNNING --query 'taskArns[0]' --output text; }
public_ip() {
  local eni
  eni=$(aws ecs describe-tasks --cluster "$CLUSTER" --tasks "$1" \
    --query "tasks[0].attachments[0].details[?name=='networkInterfaceId'].value" --output text)
  aws ec2 describe-network-interfaces --network-interface-ids "$eni" --query 'NetworkInterfaces[0].Association.PublicIp' --output text
}

log "waiting for services to be stable"
aws ecs wait services-stable --cluster "$CLUSTER" --services mock-salesforce stream-collector eventlog-collector
MOCK_TASK=$(task_of mock-salesforce)
MOCK="http://$(public_ip "$MOCK_TASK"):8080"
log "mock admin API at $MOCK"
post() { curl -sf --max-time 10 -XPOST "$MOCK$1" -H 'Content-Type: application/json' -d "$2" >/dev/null || log "WARN: POST $1 failed"; }
until curl -sf --max-time 5 "$MOCK/healthz" >/dev/null; do sleep 3; done

ORIGINAL_POLICY=$(aws s3api get-bucket-policy --bucket "$BUCKET" --query Policy --output text)
restore_policy() { aws s3api put-bucket-policy --bucket "$BUCKET" --policy "$ORIGINAL_POLICY"; }
trap 'restore_policy; post /admin/faults "{}"' EXIT

log "starting generator: ${EPS} events/s per topic, EventLogFiles every 60s, audit rows every 20s"
post /admin/generator "{\"eventsPerSecond\":${EPS},\"eventLogFileEverySeconds\":60,\"customRecordEverySeconds\":20}"

failover_done=0
end=$((SECONDS + DURATION))
while [ $SECONDS -lt $end ]; do
  sleep $((RANDOM % 40 + 30))
  case "${ACTIONS[$((RANDOM % ${#ACTIONS[@]}))]}" in
    stop-stream)
      t=$(task_of stream-collector)
      [ "$t" = "None" ] && { log "stream-collector has no running task (replacement starting); skipping"; continue; }
      log "stop stream-collector task ${t##*/} (SIGTERM, ECS replaces it)"
      aws ecs stop-task --cluster "$CLUSTER" --task "$t" --reason chaos >/dev/null ;;
    stop-eventlog)
      t=$(task_of eventlog-collector)
      [ "$t" = "None" ] && { log "eventlog-collector has no running task; skipping"; continue; }
      log "stop eventlog-collector task ${t##*/}"
      aws ecs stop-task --cluster "$CLUSTER" --task "$t" --reason chaos >/dev/null ;;
    failover)
      if [ $failover_done -eq 0 ]; then
        log "ElastiCache test-failover on $RG"
        aws elasticache test-failover --replication-group-id "$RG" --node-group-id 0001 >/dev/null \
          && failover_done=1 || log "WARN: test-failover not accepted"
      else
        log "expire all mock access tokens"
        post /admin/expire-tokens '{}'
      fi ;;
    s3-deny)
      log "deny S3 PutObject for 90s"
      DENY=$(echo "$ORIGINAL_POLICY" | sed "s#\"Statement\":\[#\"Statement\":[{\"Sid\":\"ChaosDenyWrites\",\"Effect\":\"Deny\",\"Principal\":\"*\",\"Action\":\"s3:PutObject\",\"Resource\":\"arn:aws:s3:::${BUCKET}/*\"},#")
      aws s3api put-bucket-policy --bucket "$BUCKET" --policy "$DENY"
      sleep 90
      restore_policy
      log "S3 writes allowed again" ;;
    stream-drop)
      log "pub/sub streams drop after 300 events for 60s"
      post /admin/faults '{"dropStreamAfterEvents":300}'
      sleep 60
      post /admin/faults '{}' ;;
    rest-faults)
      log "fail next 5 REST requests + truncate next download"
      post /admin/faults '{"failRestRequests":5,"truncateNextDownload":100}' ;;
  esac
done

restore_policy
post /admin/faults '{}'
post /admin/generator '{}'
log "chaos finished; generator stopped"
curl -sf "$MOCK/admin/stats" | tee -a "$LOG"; echo | tee -a "$LOG"

run_verify() {
  local arn id
  arn=$(aws ecs run-task --cluster "$CLUSTER" --launch-type FARGATE --task-definition "$VERIFY_TD" \
    --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SG],assignPublicIp=ENABLED}" \
    --query 'tasks[0].taskArn' --output text)
  id=${arn##*/}
  aws ecs wait tasks-stopped --cluster "$CLUSTER" --tasks "$arn"
  code=$(aws ecs describe-tasks --cluster "$CLUSTER" --tasks "$arn" --query 'tasks[0].containers[0].exitCode' --output text)
  aws logs get-log-events --log-group-name "$LOG_GROUP" --log-stream-name "ecs/verify/$id" \
    --query 'events[].message' --output text 2>/dev/null | tr '\t' '\n' > "$OUT/verify.json"
  echo "$code"
}

# Allow in-flight batches (maxAge 10s, poll 30s, lease 30s after restarts) to drain.
log "waiting 120s for collectors to drain"
sleep 120
for attempt in 1 2 3 4 5; do
  code=$(run_verify)
  log "verify attempt $attempt exit code: $code"
  if [ "$code" = "0" ]; then
    log "RECONCILIATION PASSED"
    grep -E '"(objects|expected|archivedUnique|missing|duplicates|ok)"' "$OUT/verify.json" | tee -a "$LOG"
    break
  fi
  sleep 60
done

aws logs filter-log-events --log-group-name "$LOG_GROUP" --start-time $(( ($(date +%s) - DURATION - 900) * 1000 )) \
  --filter-pattern '{ $.level = "ERROR" }' --query 'events[].message' --output text | tr '\t' '\n' | grep -v '^None$' > "$OUT/errors.log"
log "collector ERROR log lines: $(wc -l < "$OUT/errors.log")"
[ "$code" = "0" ]
