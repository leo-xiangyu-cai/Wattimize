#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/nas-common.sh"

if [[ ! -f "${LOCAL_DB_FILE}" ]]; then
  echo "No local database found at ${LOCAL_DB_FILE}; skipping push."
  exit 0
fi

timestamp="$(date +%Y%m%d_%H%M%S)"
remote_backup_dir="${NAS_DATA_DIR}/backup"
remote_bills_backup_dir="${NAS_DATA_DIR}/backup/home-plan"
remote_tmpdir="${NAS_DATA_DIR}/.tmp-${timestamp}"

echo "Pushing database to ${SSH_TARGET}:${REMOTE_DB_FILE}..."
remote_exec "mkdir -p '${NAS_DATA_DIR}' '${remote_backup_dir}' '${remote_bills_backup_dir}' '${remote_tmpdir}'"

db_size_bytes="$(wc -c < "${LOCAL_DB_FILE}" | tr -d ' ')"
echo "Uploading ${DB_BASENAME} ($(format_bytes "${db_size_bytes}"))..."
dd if="${LOCAL_DB_FILE}" bs=4m status=progress | remote_exec "cat > '${remote_tmpdir}/${DB_BASENAME}'"
for suffix in -wal -shm; do
  local_sidecar="${LOCAL_DB_FILE}${suffix}"
  if [[ -f "${local_sidecar}" ]]; then
    sidecar_size_bytes="$(wc -c < "${local_sidecar}" | tr -d ' ')"
    echo "Uploading ${DB_BASENAME}${suffix} ($(format_bytes "${sidecar_size_bytes}"))..."
    dd if="${local_sidecar}" bs=1m status=progress | remote_exec "cat > '${remote_tmpdir}/${DB_BASENAME}${suffix}'"
  fi
done

if [[ -d "${LOCAL_BILLS_DIR}" ]]; then
  bills_size_bytes="$(du -sk "${LOCAL_BILLS_DIR}" | awk '{print $1 * 1024}')"
  echo "Uploading bill artifacts ${LOCAL_BILLS_PATH} ($(format_bytes "${bills_size_bytes}"))..."
  COPYFILE_DISABLE=1 tar --no-xattrs -C "${LOCAL_BILLS_PARENT_DIR}" -cf - "${LOCAL_BILLS_BASENAME}" | remote_exec "
    set -euo pipefail
    mkdir -p '${remote_tmpdir}/home-plan'
    tar -C '${remote_tmpdir}/home-plan' -xf -
  "
else
  echo "No local bill artifacts found at ${LOCAL_BILLS_PATH}; preserving NAS bill artifacts."
fi

remote_exec "
  set -euo pipefail
  if [ -f '${REMOTE_DB_FILE}' ]; then
    cp '${REMOTE_DB_FILE}' '${remote_backup_dir}/${DB_BASENAME}.${timestamp}.bak'
  fi
  if [ -f '${REMOTE_DB_FILE}-wal' ]; then
    cp '${REMOTE_DB_FILE}-wal' '${remote_backup_dir}/${DB_BASENAME}-wal.${timestamp}.bak'
  fi
  if [ -f '${REMOTE_DB_FILE}-shm' ]; then
    cp '${REMOTE_DB_FILE}-shm' '${remote_backup_dir}/${DB_BASENAME}-shm.${timestamp}.bak'
  fi
  rm -f '${REMOTE_DB_FILE}' '${REMOTE_DB_FILE}-wal' '${REMOTE_DB_FILE}-shm'
  mv '${remote_tmpdir}/${DB_BASENAME}' '${REMOTE_DB_FILE}'
  if [ -f '${remote_tmpdir}/${DB_BASENAME}-wal' ]; then
    mv '${remote_tmpdir}/${DB_BASENAME}-wal' '${REMOTE_DB_FILE}-wal'
  fi
  if [ -f '${remote_tmpdir}/${DB_BASENAME}-shm' ]; then
    mv '${remote_tmpdir}/${DB_BASENAME}-shm' '${REMOTE_DB_FILE}-shm'
  fi
  if [ -d '${remote_tmpdir}/home-plan/${LOCAL_BILLS_BASENAME}' ]; then
    mkdir -p '$(dirname "${REMOTE_BILLS_DIR}")' '${remote_bills_backup_dir}'
    if [ -d '${REMOTE_BILLS_DIR}' ]; then
      rm -rf '${remote_bills_backup_dir}/bills.${timestamp}.bak'
      mv '${REMOTE_BILLS_DIR}' '${remote_bills_backup_dir}/bills.${timestamp}.bak'
    fi
    mv '${remote_tmpdir}/home-plan/${LOCAL_BILLS_BASENAME}' '${REMOTE_BILLS_DIR}'
  fi
  rm -rf '${remote_tmpdir}'
"

echo "NAS database and bill artifacts updated from local copy."
