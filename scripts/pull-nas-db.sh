#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/nas-common.sh"

if ! remote_db_exists; then
  echo "No remote database found at ${REMOTE_DB_FILE}; skipping pull."
  exit 0
fi

tmpdir="$(mktemp -d)"
cleanup() {
  rm -rf "${tmpdir}"
}
trap cleanup EXIT

echo "Pulling database from ${SSH_TARGET}:${REMOTE_DB_FILE}..."
db_size_bytes="$(remote_exec "wc -c < '${REMOTE_DB_FILE}'" | tr -d '[:space:]')"
echo "Downloading ${DB_BASENAME} ($(format_bytes "${db_size_bytes}"))..."
remote_exec "cat '${REMOTE_DB_FILE}'" | dd of="${tmpdir}/${DB_BASENAME}" bs=4m status=progress

for suffix in -wal -shm; do
  remote_sidecar="${REMOTE_DB_FILE}${suffix}"
  local_sidecar="${tmpdir}/${DB_BASENAME}${suffix}"
  if remote_exec "test -f '${remote_sidecar}'" >/dev/null 2>&1; then
    sidecar_size_bytes="$(remote_exec "wc -c < '${remote_sidecar}'" | tr -d '[:space:]')"
    echo "Downloading ${DB_BASENAME}${suffix} ($(format_bytes "${sidecar_size_bytes}"))..."
    remote_exec "cat '${remote_sidecar}'" | dd of="${local_sidecar}" bs=1m status=progress
  fi
done

mkdir -p "${LOCAL_DB_DIR}"
rm -f "${LOCAL_DB_FILE}" "${LOCAL_DB_FILE}-wal" "${LOCAL_DB_FILE}-shm"
mv "${tmpdir}/${DB_BASENAME}" "${LOCAL_DB_FILE}"
for suffix in -wal -shm; do
  if [[ -f "${tmpdir}/${DB_BASENAME}${suffix}" ]]; then
    mv "${tmpdir}/${DB_BASENAME}${suffix}" "${LOCAL_DB_FILE}${suffix}"
  fi
done

echo "Local database updated from NAS."
