#!/usr/bin/env bash
set -euo pipefail

NAS_HOST="${NAS_HOST:-tnas.local}"
NAS_PORT="${NAS_PORT:-9222}"
NAS_USER="${NAS_USER:-leo-cai}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/tnas_ed25519}"
REMOTE_TAR="${REMOTE_TAR:-/tmp/wattimize.tar}"
CONTAINER_NAME="${CONTAINER_NAME:-wattimize}"
IMAGE_NAME="${IMAGE_NAME:-wattimize:amd64-latest}"
IMAGE_REPOSITORY="${IMAGE_REPOSITORY:-wattimize}"
NAS_DATA_DIR="${NAS_DATA_DIR:-/Volume1/public/wattimize-data}"
HOST_PORT="${HOST_PORT:-18000}"
CONTAINER_PORT="${CONTAINER_PORT:-8000}"
TZ_VALUE="${TZ_VALUE:-Asia/Shanghai}"
DOCKER_BIN="${DOCKER_BIN:-/Volume1/@apps/DockerEngine/dockerd/bin/docker}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"

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
ssh "${SSH_OPTS[@]}" "${SSH_TARGET}" "
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

cutoff_file_args=("-mtime" "+${RETENTION_DAYS}")
find dist/docker-images -type f \( -name 'wattimize_amd64_*.tar' -o -name 'wattimize_amd64_*.tar.sha256' \) "${cutoff_file_args[@]}" -delete

echo "Deployment complete."
