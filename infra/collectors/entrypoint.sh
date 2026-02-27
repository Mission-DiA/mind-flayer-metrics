#!/bin/sh
# Allowlist entrypoint — only the four named collector scripts may be executed.
# Blocks arbitrary python arguments (-c, -m, -, other filenames) passed via
# Cloud Run Job --args overrides.
set -eu

ALLOWED="gcp_collector.py aws_collector.py snowflake_collector.py mongodb_collector.py"

if [ $# -eq 0 ]; then
    echo "ERROR: No script specified. Permitted scripts: ${ALLOWED}" >&2
    exit 1
fi

SCRIPT="$1"
shift  # remaining $@ are passed through (e.g. --date, --backfill)

case "${SCRIPT}" in
    gcp_collector.py|aws_collector.py|snowflake_collector.py|mongodb_collector.py)
        ;;
    *)
        echo "ERROR: '${SCRIPT}' is not permitted. Allowed: ${ALLOWED}" >&2
        exit 1
        ;;
esac

exec python "/app/${SCRIPT}" "$@"
