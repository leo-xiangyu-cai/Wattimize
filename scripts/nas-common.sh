#!/usr/bin/env bash
set -euo pipefail

NAS_HOST="${NAS_HOST:-tnas.local}"
NAS_PORT="${NAS_PORT:-9222}"
NAS_USER="${NAS_USER:-leo-cai}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/tnas_ed25519}"
CONTAINER_NAME="${CONTAINER_NAME:-wattimize}"
IMAGE_NAME="${IMAGE_NAME:-wattimize:amd64-latest}"
IMAGE_REPOSITORY="${IMAGE_REPOSITORY:-wattimize}"
NAS_DATA_DIR="${NAS_DATA_DIR:-/Volume1/public/wattimize-data}"
REMOTE_TAR="${REMOTE_TAR:-${NAS_DATA_DIR}/.deploy-image.tar}"
HOST_PORT="${HOST_PORT:-18000}"
CONTAINER_PORT="${CONTAINER_PORT:-8000}"
TZ_VALUE="${TZ_VALUE:-Asia/Shanghai}"
DOCKER_BIN="${DOCKER_BIN:-/Volume1/@apps/DockerEngine/dockerd/bin/docker}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
LOCAL_DB_PATH="${LOCAL_DB_PATH:-data/energy_samples.sqlite3}"
LOCAL_COMPOSE_SERVICE="${LOCAL_COMPOSE_SERVICE:-wattimize-api}"

SSH_TARGET="${NAS_USER}@${NAS_HOST}"
SSH_OPTS=(-i "${SSH_KEY}" -p "${NAS_PORT}")
DB_BASENAME="$(basename "${LOCAL_DB_PATH}")"
LOCAL_DB_DIR="$(cd "$(dirname "${LOCAL_DB_PATH}")" && pwd)"
LOCAL_DB_FILE="${LOCAL_DB_DIR}/${DB_BASENAME}"
REMOTE_DB_FILE="${NAS_DATA_DIR}/${DB_BASENAME}"

remote_exec() {
  ssh "${SSH_OPTS[@]}" "${SSH_TARGET}" "$@"
}

remote_docker_check() {
  remote_exec "
    set -euo pipefail
    if [ ! -x '${DOCKER_BIN}' ]; then
      echo 'docker command not found at configured path: ${DOCKER_BIN}' >&2
      exit 127
    fi
  "
}

local_container_running() {
  docker compose ps --status running --services 2>/dev/null | grep -Fx "${LOCAL_COMPOSE_SERVICE}" >/dev/null
}

remote_container_running() {
  remote_exec "
    set -euo pipefail
    if [ ! -x '${DOCKER_BIN}' ]; then
      echo 'docker command not found at configured path: ${DOCKER_BIN}' >&2
      exit 127
    fi
    '${DOCKER_BIN}' ps --filter name='${CONTAINER_NAME}' --filter status=running --format '{{.Names}}' | grep -Fx '${CONTAINER_NAME}' >/dev/null
  " >/dev/null 2>&1
}

remote_db_exists() {
  remote_exec "test -f '${REMOTE_DB_FILE}'" >/dev/null 2>&1
}

stop_local_compose() {
  docker compose stop >/dev/null 2>&1 || true
}

stop_remote_container() {
  echo "Stopping NAS container ${CONTAINER_NAME} on ${SSH_TARGET}..."
  remote_exec "
    set -euo pipefail
    if [ ! -x '${DOCKER_BIN}' ]; then
      echo 'docker command not found at configured path: ${DOCKER_BIN}' >&2
      exit 127
    fi
    '${DOCKER_BIN}' rm -f '${CONTAINER_NAME}' 2>/dev/null || true
  "
}

format_bytes() {
  local bytes="${1:-0}"
  awk -v bytes="${bytes}" '
    function human(x) {
      split("B KB MB GB TB PB", units, " ")
      for (i = 1; x >= 1024 && i < 6; i++) x /= 1024
      return sprintf(i == 1 ? "%.0f %s" : "%.1f %s", x, units[i])
    }
    BEGIN { print human(bytes + 0) }
  '
}
