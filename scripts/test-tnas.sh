#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/nas-common.sh"

TNAS_TEST_SIZE_MB="${TNAS_TEST_SIZE_MB:-64}"
TNAS_PING_COUNT="${TNAS_PING_COUNT:-10}"

if ! [[ "${TNAS_TEST_SIZE_MB}" =~ ^[1-9][0-9]*$ ]]; then
  echo "TEST_SIZE_MB must be a positive integer." >&2
  exit 2
fi

if ! [[ "${TNAS_PING_COUNT}" =~ ^[1-9][0-9]*$ ]]; then
  echo "PING_COUNT must be a positive integer." >&2
  exit 2
fi

for command_name in ssh ping dd awk grep sed tail tee tr wc; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Required command not found: ${command_name}" >&2
    exit 127
  fi
done

if [[ -t 1 ]]; then
  BOLD=$'\033[1m'
  DIM=$'\033[2m'
  GREEN=$'\033[32m'
  CYAN=$'\033[36m'
  RESET=$'\033[0m'
else
  BOLD=""
  DIM=""
  GREEN=""
  CYAN=""
  RESET=""
fi

section() {
  printf '\n%s[%s/5] %s%s\n' "${BOLD}${CYAN}" "$1" "$2" "${RESET}"
}

success() {
  printf '%s[OK]%s %s\n' "${GREEN}" "${RESET}" "$1"
}

now_seconds() {
  if command -v python3 >/dev/null 2>&1; then
    python3 -c 'import time; print(time.time())'
  elif command -v perl >/dev/null 2>&1; then
    perl -MTime::HiRes=time -e 'print time'
  else
    date +%s
  fi
}

format_transfer_result() {
  local bytes="$1"
  local elapsed="$2"

  awk -v bytes="${bytes}" -v elapsed="${elapsed}" '
    BEGIN {
      if (elapsed <= 0) elapsed = 0.001
      printf "%.2f MiB/s (%.2f Mbit/s)", bytes / 1048576 / elapsed, bytes * 8 / 1000000 / elapsed
    }
  '
}

local_tmpdir="$(mktemp -d)"
upload_file="${local_tmpdir}/upload.bin"
download_file="${local_tmpdir}/download.bin"
ping_log="${local_tmpdir}/ping.log"
remote_file="${NAS_DATA_DIR}/.tnas-speedtest-$(date +%Y%m%d_%H%M%S)-$$.bin"
remote_cleanup_needed=0

cleanup() {
  rm -rf "${local_tmpdir}"
  if [[ "${remote_cleanup_needed}" -eq 1 ]]; then
    remote_exec "rm -f '${remote_file}'" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

printf '%s\n' "${BOLD}================================================================${RESET}"
printf '%s\n' "${BOLD} TNAS DEPLOYMENT PATH SPEED TEST${RESET}"
printf '%s\n' "${BOLD}================================================================${RESET}"
printf ' Target       : %s\n' "${SSH_TARGET}:${NAS_PORT}"
printf ' Remote path  : %s\n' "${NAS_DATA_DIR}"
printf ' Payload      : %s MiB random data\n' "${TNAS_TEST_SIZE_MB}"
printf ' Ping samples : %s\n' "${TNAS_PING_COUNT}"
printf ' Test scope   : Wi-Fi/LAN + SSH encryption + NAS storage\n'
printf '%s\n' "${DIM} Temporary test data is removed automatically.${RESET}"

section 1 "Latency and packet loss"
printf 'Sending %s ICMP packets to %s; replies appear below in real time.\n\n' "${TNAS_PING_COUNT}" "${NAS_HOST}"
set +e
ping -c "${TNAS_PING_COUNT}" "${NAS_HOST}" 2>&1 | tee "${ping_log}"
ping_exit_status="${PIPESTATUS[0]}"
set -e

packet_summary="$(grep 'packet loss' "${ping_log}" | tail -n 1 || true)"
latency_summary="$(grep -E '^(round-trip|rtt)' "${ping_log}" | tail -n 1 || true)"
resolved_ip="$(sed -nE '1s/^PING [^(]+ \(([^)]+)\).*/\1/p' "${ping_log}")"
packet_loss="$(printf '%s\n' "${packet_summary}" | sed -nE 's/.* ([0-9]+([.][0-9]+)?)% packet loss.*/\1/p')"
latency_values="${latency_summary#*= }"
latency_values="${latency_values% ms}"
latency_min=""
latency_avg=""
latency_max=""
latency_jitter=""
if [[ -n "${latency_summary}" ]]; then
  IFS='/' read -r latency_min latency_avg latency_max latency_jitter <<< "${latency_values}"
fi

if [[ -n "${resolved_ip}" ]]; then
  success "Resolved ${NAS_HOST} to ${resolved_ip}"
fi
if [[ "${ping_exit_status}" -eq 0 ]]; then
  success "Ping test completed"
else
  printf 'Ping exited with status %s; SSH testing will still be attempted.\n' "${ping_exit_status}"
fi

section 2 "Prepare local payload and SSH connection"
printf 'Generating %s MiB of incompressible random data on this Mac...\n' "${TNAS_TEST_SIZE_MB}"
printf '%s\n' "${DIM}The dd rate shown in this step is local-only and is not a LAN speed result.${RESET}"
dd if=/dev/urandom of="${upload_file}" bs=1048576 count="${TNAS_TEST_SIZE_MB}" status=progress
test_size_bytes="$(wc -c < "${upload_file}" | tr -d '[:space:]')"
success "Local payload ready ($(format_bytes "${test_size_bytes}")); network measurement starts in step 3"
printf 'Connecting to %s and preparing the remote test path...\n' "${SSH_TARGET}:${NAS_PORT}"
remote_exec "mkdir -p '${NAS_DATA_DIR}'; rm -f '${remote_file}'"
remote_cleanup_needed=1
success "SSH connection ready"

section 3 "Upload through the deployment path"
printf 'Streaming local random data to NAS storage. Live dd progress follows.\n'
upload_started="$(now_seconds)"
dd if="${upload_file}" bs=4194304 status=progress | remote_exec "cat > '${remote_file}'"
upload_finished="$(now_seconds)"
upload_elapsed="$(awk -v start="${upload_started}" -v end="${upload_finished}" 'BEGIN { printf "%.6f", end - start }')"
printf 'Verifying the remote file size...\n'
remote_size_bytes="$(remote_exec "wc -c < '${remote_file}'" | tr -d '[:space:]')"
if [[ "${remote_size_bytes}" != "${test_size_bytes}" ]]; then
  echo "Upload size mismatch: sent ${test_size_bytes} bytes, NAS has ${remote_size_bytes} bytes." >&2
  exit 1
fi
upload_result="$(format_transfer_result "${test_size_bytes}" "${upload_elapsed}")"
success "Upload complete: ${upload_result} in ${upload_elapsed}s"

section 4 "Download through the deployment path"
printf 'Streaming the NAS test file back to this machine. Live dd progress follows.\n'
download_started="$(now_seconds)"
remote_exec "cat '${remote_file}'" | dd of="${download_file}" bs=4194304 status=progress
download_finished="$(now_seconds)"
download_elapsed="$(awk -v start="${download_started}" -v end="${download_finished}" 'BEGIN { printf "%.6f", end - start }')"
download_size_bytes="$(wc -c < "${download_file}" | tr -d '[:space:]')"
if [[ "${download_size_bytes}" != "${test_size_bytes}" ]]; then
  echo "Download size mismatch: expected ${test_size_bytes} bytes, received ${download_size_bytes} bytes." >&2
  exit 1
fi

download_result="$(format_transfer_result "${test_size_bytes}" "${download_elapsed}")"
success "Download complete: ${download_result} in ${download_elapsed}s"

section 5 "Integrity verification and cleanup"
printf 'Calculating SHA-256 checksums for both local copies...\n'
if command -v shasum >/dev/null 2>&1; then
  upload_checksum="$(shasum -a 256 "${upload_file}" | awk '{print $1}')"
  download_checksum="$(shasum -a 256 "${download_file}" | awk '{print $1}')"
elif command -v sha256sum >/dev/null 2>&1; then
  upload_checksum="$(sha256sum "${upload_file}" | awk '{print $1}')"
  download_checksum="$(sha256sum "${download_file}" | awk '{print $1}')"
else
  echo "A SHA-256 tool (shasum or sha256sum) is required." >&2
  exit 127
fi

if [[ "${upload_checksum}" != "${download_checksum}" ]]; then
  echo "Checksum mismatch after the upload/download round trip." >&2
  exit 1
fi

success "SHA-256 round-trip integrity verified"
printf 'Removing the remote test file...\n'
remote_exec "rm -f '${remote_file}'"
remote_cleanup_needed=0
success "Temporary files cleaned up"

packet_loss_display="unavailable"
if [[ -n "${packet_loss}" ]]; then
  packet_loss_display="${packet_loss}%"
fi
latency_display="unavailable"
if [[ -n "${latency_avg}" ]]; then
  latency_display="${latency_avg} ms avg (${latency_min}/${latency_avg}/${latency_max} min/avg/max)"
fi
jitter_display="unavailable"
if [[ -n "${latency_jitter}" ]]; then
  jitter_display="${latency_jitter} ms"
fi

printf '\n%s\n' "${BOLD}================================================================${RESET}"
printf '%s\n' "${BOLD} RESULT SUMMARY${RESET}"
printf '%s\n' "${BOLD}================================================================${RESET}"
printf ' %-14s %s\n' "Target" "${NAS_HOST}${resolved_ip:+ (${resolved_ip})}"
printf ' %-14s %s\n' "Packet loss" "${packet_loss_display}"
printf ' %-14s %s\n' "Latency" "${latency_display}"
printf ' %-14s %s\n' "Jitter" "${jitter_display}"
printf ' %-14s %s\n' "Upload" "${upload_result}"
printf ' %-14s %s\n' "Download" "${download_result}"
printf ' %-14s %s\n' "Data checked" "$(format_bytes "${test_size_bytes}")"
printf ' %-14s %s\n' "Integrity" "SHA-256 verified"
printf '%s\n' "${BOLD}================================================================${RESET}"
printf '%s\n' "TNAS speed test complete."
