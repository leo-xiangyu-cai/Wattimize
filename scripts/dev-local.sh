#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/nas-common.sh"

if local_container_running; then
  echo "Local docker compose service is already running; skipping database pull and refreshing local containers only."
  if remote_container_running; then
    stop_remote_container
  fi
else
  remote_was_running=0
  if remote_container_running; then
    remote_was_running=1
    stop_remote_container
  fi

  if [[ "${remote_was_running}" -eq 1 ]] || remote_db_exists; then
    bash "${SCRIPT_DIR}/pull-nas-db.sh"
  else
    echo "No remote runtime or remote database found; skipping database pull."
  fi
fi

docker compose up -d --build
