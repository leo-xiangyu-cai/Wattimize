#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
DEFAULT_URL="http://127.0.0.1:18000/"
TARGET_URL="${1:-$DEFAULT_URL}"
SCREENSHOT_PATH="${2:-/tmp/wattimize-render-check.png}"
PLAYWRIGHT_DIR="${PLAYWRIGHT_BOOTSTRAP_DIR:-/tmp/wattimize-playwright}"
SERVER_LOG="${SERVER_LOG_PATH:-/tmp/wattimize-render-server.log}"
SERVER_PID=""

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_cmd curl
require_cmd node
require_cmd npm

if [[ ! -x "${ROOT_DIR}/.venv/bin/uvicorn" ]]; then
  echo "Expected uvicorn at ${ROOT_DIR}/.venv/bin/uvicorn" >&2
  exit 1
fi

if ! curl -fsS --max-time 2 "http://127.0.0.1:18000/api/health" >/dev/null 2>&1; then
  (
    cd "${ROOT_DIR}"
    "${ROOT_DIR}/.venv/bin/uvicorn" app.main:app --host 127.0.0.1 --port 18000
  ) >"${SERVER_LOG}" 2>&1 &
  SERVER_PID="$!"
fi

for _ in $(seq 1 60); do
  if curl -fsS --max-time 2 "http://127.0.0.1:18000/api/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! curl -fsS --max-time 2 "http://127.0.0.1:18000/api/health" >/dev/null 2>&1; then
  echo "Frontend server did not become healthy. See ${SERVER_LOG}" >&2
  exit 1
fi

mkdir -p "${PLAYWRIGHT_DIR}"
if [[ ! -d "${PLAYWRIGHT_DIR}/node_modules/playwright" ]]; then
  (
    cd "${PLAYWRIGHT_DIR}"
    npm init -y >/dev/null 2>&1
    npm install playwright >/dev/null 2>&1
  )
fi

if [[ ! -d "${PLAYWRIGHT_DIR}/node_modules/playwright-core/.local-browsers" ]] && [[ ! -d "${PLAYWRIGHT_DIR}/node_modules/playwright/.local-browsers" ]]; then
  (
    cd "${PLAYWRIGHT_DIR}"
    npx playwright install chromium >/dev/null 2>&1
  )
fi

PLAYWRIGHT_MODULE="${PLAYWRIGHT_DIR}/node_modules/playwright"
export TARGET_URL SCREENSHOT_PATH PLAYWRIGHT_MODULE

node <<'EOF'
const { chromium } = require(process.env.PLAYWRIGHT_MODULE);

(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({
    viewport: { width: 1440, height: 2200 },
    deviceScaleFactor: 1,
  });

  const consoleErrors = [];
  const pageErrors = [];
  const failedRequests = [];

  page.on("console", (msg) => {
    if (msg.type() === "error") consoleErrors.push(msg.text());
  });
  page.on("pageerror", (err) => {
    pageErrors.push(String(err));
  });
  page.on("requestfailed", (req) => {
    const failure = req.failure();
    failedRequests.push(`${req.method()} ${req.url()} :: ${failure ? failure.errorText : "unknown"}`);
  });

  await page.goto(process.env.TARGET_URL, { waitUntil: "networkidle", timeout: 30000 });
  await page.screenshot({ path: process.env.SCREENSHOT_PATH, fullPage: true });

  const payload = {
    url: page.url(),
    title: await page.title(),
    screenshot: process.env.SCREENSHOT_PATH,
    consoleErrors,
    pageErrors,
    failedRequests,
  };
  console.log(JSON.stringify(payload, null, 2));
  await browser.close();
})().catch((error) => {
  console.error(String(error));
  process.exit(1);
});
EOF
