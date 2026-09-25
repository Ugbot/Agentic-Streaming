#!/usr/bin/env bash
# Block until every host:port argument accepts a TCP connection, or fail after a deadline.
#
# CI service containers publish their ports before the process inside is ready to serve, and
# service containers without a health check (Kafka, Qdrant) are started but not awaited by the
# runner. The service-backed tests treat a refused connection as "service absent" and skip, so
# a race here would surface as an unexpected skip in tools/ci/skip_audit.py instead of a test
# result. Waiting up front turns a slow service into a clear failure on this step.
#
#   tools/ci/wait-for-ports.sh localhost:5434 localhost:9092
#   WAIT_FOR_PORTS_TIMEOUT=120 tools/ci/wait-for-ports.sh localhost:6333
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: $0 host:port [host:port ...]" >&2
  exit 64
fi

deadline=$(( $(date +%s) + ${WAIT_FOR_PORTS_TIMEOUT:-90} ))

for target in "$@"; do
  host="${target%:*}"
  port="${target##*:}"
  if [ -z "$host" ] || [ -z "$port" ] || [ "$host" = "$target" ]; then
    echo "wait-for-ports: '$target' is not host:port" >&2
    exit 64
  fi
  until (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "wait-for-ports: $target did not accept a connection within ${WAIT_FOR_PORTS_TIMEOUT:-90}s" >&2
      exit 1
    fi
    sleep 1
  done
  echo "wait-for-ports: $target is accepting connections"
done
