#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/nas-common.sh"

DEV_HEALTHCHECK_URL="${DEV_HEALTHCHECK_URL:-http://127.0.0.1:${HOST_PORT}/api/collector/status}"
DEV_HEALTHCHECK_RETRIES="${DEV_HEALTHCHECK_RETRIES:-60}"
DEV_HEALTHCHECK_SLEEP_SECONDS="${DEV_HEALTHCHECK_SLEEP_SECONDS:-2}"

wait_for_local_healthcheck() {
  local attempt=1
  echo "Waiting for local API health check at ${DEV_HEALTHCHECK_URL}..."
  while [[ "${attempt}" -le "${DEV_HEALTHCHECK_RETRIES}" ]]; do
    if curl --fail --silent --show-error "${DEV_HEALTHCHECK_URL}" >/dev/null; then
      echo "Local API is healthy."
      return 0
    fi
    echo "Health check attempt ${attempt}/${DEV_HEALTHCHECK_RETRIES} failed; retrying in ${DEV_HEALTHCHECK_SLEEP_SECONDS}s..."
    sleep "${DEV_HEALTHCHECK_SLEEP_SECONDS}"
    attempt=$((attempt + 1))
  done
  echo "Local API did not become healthy after ${DEV_HEALTHCHECK_RETRIES} attempts."
  echo "Inspect logs with: docker logs -f ${LOCAL_COMPOSE_SERVICE}"
  return 1
}

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
wait_for_local_healthcheck
