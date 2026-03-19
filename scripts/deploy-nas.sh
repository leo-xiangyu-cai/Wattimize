#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/nas-common.sh"

HEALTHCHECK_URL="${HEALTHCHECK_URL:-http://${NAS_HOST}:${HOST_PORT}/api/health}"
HEALTHCHECK_ATTEMPTS="${HEALTHCHECK_ATTEMPTS:-30}"
HEALTHCHECK_SLEEP_SECONDS="${HEALTHCHECK_SLEEP_SECONDS:-2}"

cleanup_remote_temp() {
  remote_exec "
    set -euo pipefail
    rm -f '${REMOTE_TAR}'
    find '${NAS_DATA_DIR}' -maxdepth 1 -mindepth 1 -type d -name '.tmp-*' -exec rm -rf {} + 2>/dev/null || true
  " >/dev/null 2>&1 || true
}

trap cleanup_remote_temp EXIT

echo "Stopping local docker compose services before deployment..."
stop_local_compose

if remote_container_running; then
  echo "NAS container is already running; skipping database push and treating this as a code-only refresh."
else
  bash "${SCRIPT_DIR}/push-db-to-nas.sh"
fi

echo "Building image TAR..."
make docker

LATEST_TAR="$(ls -1t dist/docker-images/wattimize_amd64_*.tar | head -n 1)"
if [[ -z "${LATEST_TAR}" ]]; then
  echo "No TAR found in dist/docker-images after build."
  exit 1
fi

SSH_TARGET="${NAS_USER}@${NAS_HOST}"
SSH_OPTS=(-i "${SSH_KEY}" -p "${NAS_PORT}")

echo "Uploading ${LATEST_TAR} to ${SSH_TARGET}:${REMOTE_TAR}..."
cat "${LATEST_TAR}" | ssh "${SSH_OPTS[@]}" "${SSH_TARGET}" "cat > '${REMOTE_TAR}'"

echo "Loading image and restarting container on NAS..."
remote_exec "
  set -euo pipefail
  if [ ! -x '${DOCKER_BIN}' ]; then
    echo 'docker command not found at configured path: ${DOCKER_BIN}' >&2
    exit 127
  fi
  mkdir -p '${NAS_DATA_DIR}'
  '${DOCKER_BIN}' load -i '${REMOTE_TAR}'
  '${DOCKER_BIN}' rm -f '${CONTAINER_NAME}' 2>/dev/null || true
  '${DOCKER_BIN}' run -d \
    --name '${CONTAINER_NAME}' \
    --restart unless-stopped \
    -p '${HOST_PORT}:${CONTAINER_PORT}' \
    -v '${NAS_DATA_DIR}:/app/data' \
    -e 'TZ=${TZ_VALUE}' \
    '${IMAGE_NAME}'
  rm -f '${REMOTE_TAR}'
  cutoff_epoch=\$((\$(date +%s) - ${RETENTION_DAYS} * 86400))
  while IFS='|' read -r image_ref created_at; do
    [ -n \"\${image_ref}\" ] || continue
    [ \"\${image_ref}\" != '${IMAGE_NAME}' ] || continue
    created_epoch=\$(date -d \"\${created_at}\" +%s 2>/dev/null || echo '')
    [ -n \"\${created_epoch}\" ] || continue
    if [ \"\${created_epoch}\" -lt \"\${cutoff_epoch}\" ]; then
      '${DOCKER_BIN}' image rm -f \"\${image_ref}\" >/dev/null 2>&1 || true
    fi
  done <<EOF
\$('${DOCKER_BIN}' image ls --format '{{.Repository}}:{{.Tag}}|{{.CreatedAt}}' | grep '^${IMAGE_REPOSITORY}:amd64-')
EOF
  '${DOCKER_BIN}' ps --filter name='${CONTAINER_NAME}'
"

echo "Waiting for health check: ${HEALTHCHECK_URL}"
health_ok=0
for attempt in $(seq 1 "${HEALTHCHECK_ATTEMPTS}"); do
  if curl --fail --silent --show-error --max-time 5 "${HEALTHCHECK_URL}" >/dev/null; then
    health_ok=1
    break
  fi
  sleep "${HEALTHCHECK_SLEEP_SECONDS}"
done

if [[ "${health_ok}" -ne 1 ]]; then
  echo "Health check failed: ${HEALTHCHECK_URL}" >&2
  exit 1
fi

echo "Health check passed. Cleaning remote temporary files..."
cleanup_remote_temp

cutoff_file_args=("-mtime" "+${RETENTION_DAYS}")
find dist/docker-images -type f \( -name 'wattimize_amd64_*.tar' -o -name 'wattimize_amd64_*.tar.sha256' \) "${cutoff_file_args[@]}" -delete

echo "Deployment complete."
