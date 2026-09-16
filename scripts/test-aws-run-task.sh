#!/usr/bin/env bash
# Tests scripts/aws-run-task.sh against stub terraform and aws commands: the ECS
# overrides it builds must be valid JSON carrying the intended command. Needs jq.
set -euo pipefail
cd "$(dirname "$0")/.."

STUBS=$(mktemp -d)
trap 'rm -rf "$STUBS"' EXIT
export STUB_OVERRIDES="$STUBS/overrides.json" STUB_RUN_ARGS="$STUBS/run-args"

cat > "$STUBS/terraform" <<'STUB'
#!/usr/bin/env bash
case "$*" in
  *"output -raw region"*) printf 'ap-southeast-2' ;;
  *"output -raw cluster"*) printf 'sfarch' ;;
  *"output -raw log_group"*) printf '/ecs/sfarch' ;;
  *"output -json subnets"*) printf '["subnet-a","subnet-b"]' ;;
  *"output -raw task_security_group"*) printf 'sg-1' ;;
  *"output -raw verify_task_definition"*) printf 'sfarch-verify' ;;
  *) echo "unexpected: terraform $*" >&2; exit 1 ;;
esac
STUB
cat > "$STUBS/aws" <<'STUB'
#!/usr/bin/env bash
case "$1 $2" in
  "ecs describe-task-definition")
    printf '[\n  "sf-archive-verify",\n  "-bucket",\n  "b",\n  "-prefix",\n  "p"\n]\n' ;;
  "ecs run-task")
    printf '%s\n' "$*" > "$STUB_RUN_ARGS"
    for a in "$@"; do case "$a" in file://*) cp "${a#file://}" "$STUB_OVERRIDES" ;; esac; done
    echo "arn:aws:ecs:ap-southeast-2:1:task/sfarch/abc" ;;
  "ecs wait") ;;
  "ecs describe-tasks") echo 0 ;;
  "logs get-log-events") ;;
  *) echo "unexpected: aws $*" >&2; exit 1 ;;
esac
STUB
chmod +x "$STUBS/terraform" "$STUBS/aws"
export PATH="$STUBS:$PATH"

failures=0
# expect_command ARGS... -- EXPECTED_JSON_ARRAY
expect_command() {
  local args=() expected
  while [ "$1" != "--" ]; do args+=("$1"); shift; done
  expected=$2
  rm -f "$STUB_OVERRIDES" "$STUB_RUN_ARGS"
  if ! bash scripts/aws-run-task.sh "${args[@]}" >/dev/null 2>"$STUBS/err"; then
    echo "FAIL: aws-run-task.sh ${args[*]} exited non-zero: $(cat "$STUBS/err")"; failures=$((failures + 1)); return
  fi
  local got
  if [ "$expected" = "none" ]; then
    if grep -q -- "--overrides" "$STUB_RUN_ARGS"; then
      echo "FAIL: ${args[*]}: expected no overrides"; failures=$((failures + 1))
    else
      echo "ok: ${args[*]} (task definition command)"
    fi
    return
  fi
  if ! got=$(jq -ec '.containerOverrides[0].command' "$STUB_OVERRIDES"); then
    echo "FAIL: ${args[*]}: overrides are not valid JSON: $(cat "$STUB_OVERRIDES")"; failures=$((failures + 1)); return
  fi
  if [ "$got" != "$(jq -c . <<<"$expected")" ]; then
    echo "FAIL: ${args[*]}: command $got, want $expected"; failures=$((failures + 1))
  else
    echo "ok: ${args[*]}"
  fi
}

expect_command stream -reset-checkpoints '/event/Login"Event\Stream' -confirm -- \
  '["with-config","sf-archive-stream","-reset-checkpoints","/event/Login\"Event\\Stream","-confirm"]'
expect_command eventlog -once -- '["with-config","sf-archive-eventlog","-once"]'
expect_command verify -- none
expect_command verify -strict -- '["sf-archive-verify","-bucket","b","-prefix","p","-strict"]'

if bash scripts/aws-run-task.sh stream "$(printf 'bad\tvalue')" >/dev/null 2>&1; then
  echo "FAIL: an argument with a control character must be rejected"; failures=$((failures + 1))
else
  echo "ok: control characters rejected"
fi

[ "$failures" -eq 0 ]
