#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/nas-common.sh"

if [[ -t 1 ]]; then
  COLOR_RESET=$'\033[0m'
  COLOR_BOLD=$'\033[1m'
  COLOR_GREEN=$'\033[32m'
  COLOR_YELLOW=$'\033[33m'
  COLOR_BLUE=$'\033[34m'
  COLOR_RED=$'\033[31m'
  COLOR_DIM=$'\033[2m'
else
  COLOR_RESET=""
  COLOR_BOLD=""
  COLOR_GREEN=""
  COLOR_YELLOW=""
  COLOR_BLUE=""
  COLOR_RED=""
  COLOR_DIM=""
fi

if local_container_running; then
  local_runtime="running"
else
  local_runtime="stopped"
fi

if [[ -f "${LOCAL_DB_FILE}" ]]; then
  local_db="present"
else
  local_db="missing"
fi

if remote_container_running; then
  remote_runtime="running"
else
  remote_runtime="stopped"
fi

if remote_db_exists; then
  remote_db="present"
else
  remote_db="missing"
fi

format_state() {
  local value="$1"
  case "${value}" in
    running|present)
      printf '%s%s%s' "${COLOR_GREEN}" "${value}" "${COLOR_RESET}"
      ;;
    stopped)
      printf '%s%s%s' "${COLOR_YELLOW}" "${value}" "${COLOR_RESET}"
      ;;
    missing)
      printf '%s%s%s' "${COLOR_RED}" "${value}" "${COLOR_RESET}"
      ;;
    *)
      printf '%s' "${value}"
      ;;
  esac
}

print_section() {
  local title="$1"
  printf '%s%s%s\n' "${COLOR_BOLD}${COLOR_BLUE}" "${title}" "${COLOR_RESET}"
}

print_item() {
  local label="$1"
  local value="$2"
  local detail="$3"
  printf '  %-12s %s' "${label}" "${value}"
  if [[ -n "${detail}" ]]; then
    printf '  %s%s%s' "${COLOR_DIM}" "${detail}" "${COLOR_RESET}"
  fi
  printf '\n'
}

print_section "Local"
print_item "runtime" "$(format_state "${local_runtime}")" ""
print_item "database" "$(format_state "${local_db}")" "${LOCAL_DB_FILE}"
printf '\n'
print_section "NAS"
print_item "runtime" "$(format_state "${remote_runtime}")" "${SSH_TARGET}"
print_item "database" "$(format_state "${remote_db}")" "${REMOTE_DB_FILE}"
