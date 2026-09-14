#!/usr/bin/env bash
# Chaos test against the local docker compose stack.
#
# Generates Salesforce data in the mock while repeatedly killing collectors,
# restarting Valkey, pausing MinIO and injecting Salesforce faults. Then stops
# generation, waits for the collectors to drain, and reconciles the archive
# against the mock ledger. Exit code 0 means nothing was lost.
#
#   DURATION=300 bash scripts/chaos-local.sh
set -uo pipefail
cd "$(dirname "$0")/.."

COMPOSE="docker compose -f deploy/local/docker-compose.yml"
PROJECT=sf-archive
MOCK=http://localhost:18080
DURATION=${DURATION:-300}
EPS=${EPS:-100}
OUT=${OUT:-dev/chaos-local}
mkdir -p "$OUT"
LOG="$OUT/chaos.log"
: > "$LOG"

log() { echo "$(date -u +%H:%M:%S) $*" | tee -a "$LOG"; }
container() { echo "${PROJECT}-$1-1"; }
post() { curl -sf -XPOST "$MOCK$1" -H 'Content-Type: application/json' -d "$2" >/dev/null; }

log "starting generator: ${EPS} events/s per topic, EventLogFiles every 20s, audit rows every 10s"
post /admin/generator "{\"eventsPerSecond\":${EPS},\"eventLogFileEverySeconds\":20,\"customRecordEverySeconds\":10}"

end=$((SECONDS + DURATION))
actions=0
while [ $SECONDS -lt $end ]; do
  sleep $((RANDOM % 15 + 5))
  actions=$((actions + 1))
  case $((RANDOM % 9)) in
    0|1)
      log "kill -9 stream-collector"
      docker kill -s KILL "$(container stream-collector)" >/dev/null
      sleep $((RANDOM % 3))
      docker start "$(container stream-collector)" >/dev/null ;;
    2)
      log "kill -9 eventlog-collector"
      docker kill -s KILL "$(container eventlog-collector)" >/dev/null
      sleep $((RANDOM % 3))
      docker start "$(container eventlog-collector)" >/dev/null ;;
    3)
      log "restart valkey"
      docker restart -t 1 "$(container valkey)" >/dev/null ;;
    4)
      d=$((RANDOM % 20 + 5))
      log "pause minio for ${d}s"
      docker pause "$(container minio)" >/dev/null
      sleep $d
      docker unpause "$(container minio)" >/dev/null ;;
    5)
      log "pub/sub streams drop after 150 events for 20s"
      post /admin/faults '{"dropStreamAfterEvents":150}'
      sleep 20
      post /admin/faults '{}' ;;
    6)
      log "expire all access tokens"
      post /admin/expire-tokens '{}' ;;
    7)
      log "fail next 5 REST requests"
      post /admin/faults '{"failRestRequests":5}' ;;
    8)
      log "truncate next EventLogFile download"
      post /admin/faults '{"truncateNextDownload":100}' ;;
  esac
done

post /admin/faults '{}'
post /admin/generator '{}'
docker unpause "$(container minio)" >/dev/null 2>&1
log "chaos finished after ${actions} actions; generator stopped; waiting for collectors to drain"
curl -sf "$MOCK/admin/stats" | tee -a "$LOG"; echo

verify() {
  docker run --rm --network "${PROJECT}_default" \
    -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin -e AWS_REGION=us-east-1 \
    salesforce-s3-archiver:local sf-archive-verify -bucket archive -prefix local \
    -endpoint http://minio:9000 -region us-east-1 -ledger http://mock-salesforce:8080/admin/ledger
}

deadline=$((SECONDS + 300))
until verify > "$OUT/verify.json" 2> "$OUT/verify.err"; do
  if [ $SECONDS -ge $deadline ]; then
    log "RECONCILIATION FAILED after waiting 300s"
    cat "$OUT/verify.err" | tee -a "$LOG"
    grep -E '"(expected|archivedUnique|missing|duplicates)"' "$OUT/verify.json" | tee -a "$LOG"
    $COMPOSE logs --no-color --tail 200 > "$OUT/compose.log" 2>&1
    exit 1
  fi
  sleep 10
done
log "RECONCILIATION PASSED: no missing records, all manifests match"
grep -E '"(objects|expected|archivedUnique|missing|duplicates)"' "$OUT/verify.json" | tee -a "$LOG"
$COMPOSE logs --no-color --tail 500 > "$OUT/compose.log" 2>&1
exit 0
