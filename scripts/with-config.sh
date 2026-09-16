#!/bin/sh
# Writes the CONFIG_YAML environment variable to a file and runs the given
# command against it:
#
#   with-config sf-archive-stream -reset-checkpoints all -confirm
#
# Keeping this in the image means container commands and ECS task overrides are
# plain argument lists, with no embedded shell quoting.
set -eu
: "${CONFIG_YAML:?CONFIG_YAML is not set}"
CONFIG_PATH="${CONFIG_PATH:-/tmp/config.yml}"
printf '%s' "$CONFIG_YAML" > "$CONFIG_PATH"
BIN="$1"
shift
exec "$BIN" -config "$CONFIG_PATH" "$@"
