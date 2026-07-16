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

timestamp="$(date +%Y%m%d_%H%M%S)"

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

remote_bills_present=0
if remote_exec "test -d '${REMOTE_BILLS_DIR}'" >/dev/null 2>&1; then
  remote_bills_present=1
  remote_bills_size_bytes="$(remote_exec "du -sk '${REMOTE_BILLS_DIR}' | awk '{print \$1 * 1024}'" | tr -d '[:space:]')"
  echo "Downloading bill artifacts ${REMOTE_BILLS_DIR} ($(format_bytes "${remote_bills_size_bytes}"))..."
  remote_exec "tar -C '$(dirname "${REMOTE_BILLS_DIR}")' -cf - '$(basename "${REMOTE_BILLS_DIR}")'" | tar -C "${tmpdir}" -xf -
else
  echo "No NAS bill artifacts found at ${REMOTE_BILLS_DIR}; local bill artifacts will be cleared after database sync."
fi

if command -v sqlite3 >/dev/null 2>&1; then
  echo "Validating downloaded database..."
  quick_check_output="$(sqlite3 "${tmpdir}/${DB_BASENAME}" "PRAGMA quick_check;" 2>&1)" || {
    echo "Downloaded database failed SQLite validation; preserving existing local database." >&2
    echo "${quick_check_output}" >&2
    exit 1
  }
  if [[ "${quick_check_output}" != "ok" ]]; then
    echo "Downloaded database failed SQLite quick_check; preserving existing local database." >&2
    echo "${quick_check_output}" >&2
    exit 1
  fi
else
  echo "sqlite3 command not found; skipping downloaded database validation." >&2
fi

mkdir -p "${LOCAL_DB_DIR}"
rm -f "${LOCAL_DB_FILE}" "${LOCAL_DB_FILE}-wal" "${LOCAL_DB_FILE}-shm"
mv "${tmpdir}/${DB_BASENAME}" "${LOCAL_DB_FILE}"
for suffix in -wal -shm; do
  if [[ -f "${tmpdir}/${DB_BASENAME}${suffix}" ]]; then
    mv "${tmpdir}/${DB_BASENAME}${suffix}" "${LOCAL_DB_FILE}${suffix}"
  fi
done

local_bills_backup_dir="${LOCAL_DB_DIR}/backup/home-plan"
mkdir -p "${LOCAL_BILLS_PARENT_DIR}" "${local_bills_backup_dir}"
if [[ -d "${LOCAL_BILLS_DIR}" ]]; then
  rm -rf "${local_bills_backup_dir}/bills.${timestamp}.bak"
  mv "${LOCAL_BILLS_DIR}" "${local_bills_backup_dir}/bills.${timestamp}.bak"
fi
if [[ "${remote_bills_present}" -eq 1 ]]; then
  mv "${tmpdir}/$(basename "${REMOTE_BILLS_DIR}")" "${LOCAL_BILLS_DIR}"
  echo "Local bill artifacts updated from NAS."
else
  echo "Local bill artifacts cleared to match NAS."
fi

echo "Local database and bill artifacts updated from NAS."
