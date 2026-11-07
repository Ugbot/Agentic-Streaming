#!/usr/bin/env bash
# Sourced by the scripts here so they run on Docker or Podman, whichever is actually up. A
# machine can have both installed and one stopped, so it asks each engine rather than the PATH.
#
#   source tools/container-engine.sh
#   use_container_engine          # sets ENGINE; CONTAINER_ENGINE=docker|podman forces a choice
#   compose -f file.yml up -d
#   ensure_network agentic-flink-network
#   "$ENGINE" exec some-container ...

use_container_engine() {
  if [ -n "${CONTAINER_ENGINE:-}" ]; then
    if ! "$CONTAINER_ENGINE" info >/dev/null 2>&1; then
      echo "CONTAINER_ENGINE=$CONTAINER_ENGINE is not running" >&2
      exit 1
    fi
    ENGINE="$CONTAINER_ENGINE"
    return
  fi
  for candidate in docker podman; do
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" info >/dev/null 2>&1; then
      ENGINE="$candidate"
      return
    fi
  done
  echo "no running container engine: tried docker and podman" >&2
  exit 1
}

compose() {
  "$ENGINE" compose "$@"
}

ensure_network() {
  "$ENGINE" network inspect "$1" >/dev/null 2>&1 || "$ENGINE" network create "$1" >/dev/null
}
