# Wattimize API (MVP)

API inventory (for keep/remove decisions): `docs/API_INVENTORY.md`

## 1) Setup

```bash
cd /Users/caixy/Leo/Wattimize
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2) Configure

This project does not use `.docenv`.

Use the web config page:

- Start the service first, then open `http://<wattimize-host>:18000/`.
- If config is missing, the page shows a setup dialog.
- Required fields: `HA_URL`, `HA_TOKEN`.
- Optional field: `SOLPLANET_DONGLE_HOST`.
- The config dialog can auto-discover `SOLPLANET_INVERTER_SN` and `SOLPLANET_BATTERY_SN` from the dongle CGI API.
- Adjustable sampling fields: `SAJ_SAMPLE_INTERVAL_SECONDS`, `SOLPLANET_SAMPLE_INTERVAL_SECONDS` (choices: `5/10/30/60/300`).
- Detailed history retention: `1/2/3/6/12` months, default `2`. Older high-frequency source snapshots are removed to keep SQLite compact, while generated daily/monthly reports and their saved report details are preserved.
- The Monthly tab provides month-to-date estimates, comparison baselines, export-window analysis, anomalies, and load-shifting recommendations. Its year picker shows whether each month is saved, has no data, is ready to generate, or is live; completed months are persisted automatically and can also be generated through the API.
- Saved daily and monthly reports expose a `report_status` of `saved` or `no_data`. Persisted monthly snapshots remain available after detailed-history cleanup.

Config storage:
- Stored in SQLite table `app_config`
- Local default DB: `data/wattimize.sqlite3`
- Docker default DB path is controlled by `WATTIMIZE_DB_PATH`
- Existing legacy `config.json` is auto-migrated on first load

Persistence config (optional):
- `WATTIMIZE_DB_PATH` (default: `data/wattimize.sqlite3`)
- `WATTIMIZE_SAMPLE_INTERVAL_SECONDS` (default: `5`, choices: `5/10/30/60/300`)
- `WATTIMIZE_SOLPLANET_SAMPLE_INTERVAL_SECONDS` (default: `60`, choices: `5/10/30/60/300`)
- `WATTIMIZE_DETAIL_RETENTION_MONTHS` (default: `2`, choices: `1/2/3/6/12`)

## 3) Run

```bash
uvicorn app.main:app --host 0.0.0.0 --port 18000 --reload
```

Open frontend:

```bash
open http://<wattimize-host>:18000/
```

## 4) Test

```bash
curl -s http://<wattimize-host>:18000/api/health
curl -s http://<wattimize-host>:18000/api/config/status | jq
curl -s http://<wattimize-host>:18000/api/ha/ping | jq
curl -s http://<wattimize-host>:18000/api/energy-flow/saj | jq
curl -s http://<wattimize-host>:18000/api/energy-flow/solplanet | jq
curl -s http://<wattimize-host>:18000/api/time-window-rules | jq
curl -s -X PUT http://<wattimize-host>:18000/api/saj/manual-discharge-power -H 'Content-Type: application/json' -d '{"power_w":2000}' | jq
curl -s -X PUT http://<wattimize-host>:18000/api/saj/self-consumption | jq
curl -s http://<wattimize-host>:18000/api/solplanet/control/state | jq
curl -s -X POST http://<wattimize-host>:18000/api/solplanet/control/restart-api | jq
curl -s -X PUT http://<wattimize-host>:18000/api/solplanet/control/limits -H 'Content-Type: application/json' -d '{"pin":10000,"pout":10000}' | jq
curl -s -X PUT http://<wattimize-host>:18000/api/solplanet/control/day-schedule/Mon -H 'Content-Type: application/json' -d '{"slots":[184595458,302020611,0,0,0,0]}' | jq
curl -s -X PUT http://<wattimize-host>:18000/api/solplanet/control/day-schedule/Mon/slots/1 -H 'Content-Type: application/json' -d '{"enabled":true,"hour":11,"minute":0,"power":180,"mode":2}' | jq
curl -s http://<wattimize-host>:18000/api/solplanet/cgi/getdev-device-2 | jq
curl -s http://<wattimize-host>:18000/api/solplanet/cgi/getdevdata-device-2 | jq
curl -s http://<wattimize-host>:18000/api/solplanet/cgi/getdevdata-device-3 | jq
curl -s http://<wattimize-host>:18000/api/solplanet/cgi/getdevdata-device-4 | jq
curl -s http://<wattimize-host>:18000/api/solplanet/cgi/getdefine | jq
curl -s http://<wattimize-host>:18000/api/catalog/domains | jq
curl -s http://<wattimize-host>:18000/api/catalog/brands | jq
curl -s "http://<wattimize-host>:18000/api/entities?brand=saj&domain=sensor&page=1&page_size=20" | jq
curl -s http://<wattimize-host>:18000/api/storage/status | jq
curl -s "http://<wattimize-host>:18000/api/storage/daily-usage?system=saj" | jq
curl -s "http://<wattimize-host>:18000/api/storage/samples?system=saj&start_utc=2026-03-03T00:00:00Z&end_utc=2026-03-04T00:00:00Z&page=1&page_size=20" | jq
curl -s "http://<wattimize-host>:18000/api/storage/series?system=saj&start_utc=2026-03-03T00:00:00Z&end_utc=2026-03-04T00:00:00Z&max_points=500" | jq
curl -s "http://<wattimize-host>:18000/api/storage/usage-range?system=saj&start_utc=2026-03-03T00:00:00Z&end_utc=2026-03-04T00:00:00Z" | jq
curl -s "http://<wattimize-host>:18000/api/storage/zero-hero-window-history?system=combined&days=1&day_local=2026-03-03" | jq
curl -s "http://<wattimize-host>:18000/api/reports/daily?day_local=2026-03-03&page=1&page_size=100" | jq
curl -s "http://<wattimize-host>:18000/api/reports/daily/index" | jq
curl -s -X POST "http://<wattimize-host>:18000/api/reports/daily/generate?day_local=2026-03-03" | jq
curl -s "http://<wattimize-host>:18000/api/reports/monthly/index?year=2026" | jq
curl -s "http://<wattimize-host>:18000/api/reports/monthly?month=2026-03" | jq
curl -s -X POST "http://<wattimize-host>:18000/api/reports/monthly/generate?month=2026-03" | jq
curl -s http://<wattimize-host>:18000/api/home-plan/electricity-plans | jq
curl -s http://<wattimize-host>:18000/api/bills | jq
curl -s -X POST http://<wattimize-host>:18000/api/bills/upload -F "file=@globird-bill.pdf" | jq
curl -s -X POST http://<wattimize-host>:18000/api/tesla/control/charging -H 'Content-Type: application/json' -d '{"enabled":true}' | jq
curl -s -X POST http://<wattimize-host>:18000/api/tesla/control/current -H 'Content-Type: application/json' -d '{"amps":16}' | jq
curl -s -X PUT http://<wattimize-host>:18000/api/tesla/auto-control -H 'Content-Type: application/json' -d '{"enabled":false}' | jq
curl -s -X PUT http://<wattimize-host>:18000/api/tesla/force-max-charge-threshold -H 'Content-Type: application/json' -d '{"threshold_percent":45}' | jq
curl -s "http://<wattimize-host>:18000/api/operations/timeline?limit=50" | jq
curl -s http://<wattimize-host>:18000/api/devices/pool-pump | jq
curl -s -X POST http://<wattimize-host>:18000/api/devices/pool-pump/toggle -H 'Content-Type: application/json' -d '{"enabled":true}' | jq
curl -s -X PUT http://<wattimize-host>:18000/api/devices/pool-pump/auto-control -H 'Content-Type: application/json' -d '{"enabled":false}' | jq
curl -s -X PUT http://<wattimize-host>:18000/api/devices/pool-pump/force-min-runtime-hours -H 'Content-Type: application/json' -d '{"threshold_hours":1.5}' | jq
curl -s http://<wattimize-host>:18000/api/database/backups | jq
curl -s -X POST http://<wattimize-host>:18000/api/database/backups/run | jq
curl -s http://<wattimize-host>:18000/api/database/export.sqlite3 -o wattimize.sqlite3
curl -s -X POST http://<wattimize-host>:18000/api/database/import.sqlite3 -F "file=@wattimize.sqlite3" | jq
curl -s -X POST http://<wattimize-host>:18000/api/config/solplanet/discover -H 'Content-Type: application/json' -d '{"solplanet_dongle_host":"192.168.1.10"}' | jq
```

`/api/energy-flow/solplanet` behavior:
- If `SOLPLANET_DONGLE_HOST` is set, backend reads Solplanet dongle CGI directly (`getdev*.cgi`).
- If not set, backend falls back to Home Assistant entity mapping.
- CGI cache/timeout values are backend constants, not env vars.

## 5) Run with Docker

```bash
cd /Users/caixy/Leo/Wattimize
make status
docker compose up -d --build
make dev-stop
# or: make dev-down
curl -s http://<wattimize-host>:18000/api/saj/entities/core | jq
```

Use `make status` any time to check whether local or NAS is currently running and whether each side has a SQLite database file.

`make dev` now switches execution to local: if local is not already running, it stops the NAS container, pulls the NAS SQLite database and uploaded bill artifacts into `./data/`, and then starts local `docker compose`. If local is already running, it treats the command as a local code refresh and skips data sync. When `sqlite3` is available, the downloaded database must pass `PRAGMA quick_check` before it replaces the existing local database. After the containers start, the script waits for the local collector status endpoint to become healthy before returning.

To start local dev without pulling the NAS database or bill artifacts, use:

```bash
make dev SKIP_DB_PULL=1
```

`NO_DB_PULL=1` is also accepted as an alias.

To stop only the NAS container without starting local services:

```bash
make nas-stop
```

Build and deploy to TerraMaster NAS over SSH:

```bash
make nas-deploy
```

`make nas-deploy` now stops local `docker compose` services first. If the NAS container is not already running, it pushes the local SQLite database and uploaded bill artifacts to the NAS before deploying. If the NAS container is already running, it treats the command as a code-only refresh and skips data sync.
Every NAS deploy also uploads `data/home-plan/electricity-plans.json` to `${NAS_DATA_DIR}/home-plan/electricity-plans.json`, which is mounted in the container as `/app/data/home-plan/electricity-plans.json` for the Plan tab.
After the NAS health check passes, deploys prune old manual SQLite backups under `${NAS_DATA_DIR}/backup`, keeping backups from the last `NAS_DB_BACKUP_RETENTION_DAYS` days.

To deploy code to the NAS without pushing the local database or bill artifacts, use:

```bash
make nas-deploy SKIP_DB_PUSH=1
```

`NO_DB_PUSH=1` is also accepted as an alias.

For the first sync from local to NAS while the NAS is still running, use:

```bash
make nas-stop
make nas-deploy
```

Supported overrides for `make nas-deploy`:
- `NAS_HOST`, `NAS_PORT`, `NAS_USER`, `SSH_KEY`
- `CONTAINER_NAME`, `IMAGE_NAME`, `IMAGE_REPOSITORY`
- `NAS_DATA_DIR`, `HOST_PORT`, `CONTAINER_PORT`, `TZ_VALUE`
- `DOCKER_BIN`, `RETENTION_DAYS`, `NAS_DB_BACKUP_RETENTION_DAYS`
- `LOCAL_PLAN_PATH`

Test the actual SSH and NAS storage path used by deployments:

```bash
make test-tnas
```

The test displays live ping replies and transfer progress, then prints a summary
of packet loss, latency, jitter, upload speed, and download speed. It uploads and
downloads a random 64 MiB temporary file through the same SSH and NAS storage path
used by deployment. The round trip is SHA-256 verified, and both local and NAS
temporary files are removed automatically. Increase the sample size or ping count
when investigating intermittent performance:

```bash
make test-tnas TEST_SIZE_MB=256 PING_COUNT=30
```

The same `NAS_HOST`, `NAS_PORT`, `NAS_USER`, `SSH_KEY`, and `NAS_DATA_DIR`
overrides supported by NAS deployment are also supported by this test.

First-run configuration:
- Open `http://NAS_IP:18000/`.
- If config is missing, the page shows a config dialog.
- Config stores: `HA_URL`, `HA_TOKEN`, `SOLPLANET_DONGLE_HOST`, `SOLPLANET_INVERTER_SN`, `SOLPLANET_BATTERY_SN`, `SAJ_SAMPLE_INTERVAL_SECONDS`, `SOLPLANET_SAMPLE_INTERVAL_SECONDS`.
- Other values are backend constants (entity ids, port/scheme/ssl/cache/timeout), except the two sampling interval fields.
- Saved config is written into SQLite table `app_config`.
- The database path can be overridden with `WATTIMIZE_DB_PATH`.
- The checked-in `docker-compose.yml` points `WATTIMIZE_DB_PATH` at `/app/data/wattimize.sqlite3`.

## 6) Built-in SQLite Sampling

- Backend starts a background collector at startup.
- Default SAJ sampling frequency is every 5 seconds (`WATTIMIZE_SAMPLE_INTERVAL_SECONDS`).
- Default Solplanet sampling frequency is every 60 seconds (`WATTIMIZE_SOLPLANET_SAMPLE_INTERVAL_SECONDS`).
- Every sample stores current `pv_w/grid_w/battery_w/load_w/soc/inverter_status/balance` and raw flow payload.
- SQLite writes and worker log persistence now retry on transient database failures before surfacing an error in collector status.
- When SQLite corruption is detected during runtime recovery flows, the backend attempts an automatic recovery into the recovery directory and reinitializes the runtime connection pool.
- The backend creates a consistent SQLite backup once per local day under `data/backups/`, validates it with `PRAGMA quick_check`, and keeps only the latest 3 backup files.
- Daily usage endpoint integrates power snapshots into kWh (UTC day).
- `system=combined` is available on range usage and trend series endpoints for the frontend overall view.
- The combined dashboard selects `grid_w` from the SAJ or Solplanet grid reading with the lower power-balance residual and exposes both source values in the grid tooltip, marking the selected source with a check.
- The `History` tab combines range selection with an energy breakdown card, a Tesla free-window gross SOC charge metric, an estimated free-grid-electricity split for Tesla, home load, and battery charging normalized to the same free import kWh total, a power timeline chart, a combined SAJ/Solplanet SOC chart, a total usable remaining-energy chart, a Tesla SOC chart, a pool-pump on/off timeline, a live 5-minute export-window observation card, and a selected-day `16:00-23:00` matrix of five-minute export windows, color-coded for net import, mixed import/net export, and pure export.
- The `Report` tab reuses the History presentation but reads a persisted daily snapshot. The backend generates the completed local day at midnight, immediately backfills yesterday after an upgrade when needed, and stores precomputed usage, daily electricity cost, chart series, Zero Hero windows, and paginable sample rows for fast reads.
- Daily report cost uses the bill-derived GloBird ZEROHERO rates in `data/home-plan/electricity-plans.json`, including fixed charges, import time windows, feed-in credits, Super Export top-up, and the estimated ZEROHERO day credit. The Report tab keeps this as an estimate and no longer allocates uploaded bill totals back onto individual days.
- Daily reports also compute conservative SOC-delta energy floors for SAJ, Solplanet, and Tesla so persisted report totals can recover obvious charge/discharge undercounts across raw-sample gaps.
- Electricity bill uploads are accepted through the Report tab, the Bills tab, or `POST /api/bills/upload`. Uploaded PDF/text artifacts are stored under `data/home-plan/bills/` with parsed billing-period date filenames and indexed in `bills-index.json`. The Bills tab presents one uploaded bill period at a time, including billing days, pay/credit wording, parsed line items, source documents, and the plan/rate periods that apply across that bill.
- To audit or backfill persisted daily report cost fields without starting the app, run `PYTHONPYCACHEPREFIX=/tmp/wattimize-pycache python3.12 scripts/audit-daily-report-costs.py` for a read-only comparison, or add `--write` to update missing/mismatched `_daily_cost` payloads. Stop any Wattimize service that uses `data/wattimize.sqlite3` before running a write-mode DB audit/backfill.
- `GET /api/storage/samples?system=combined` now includes `tesla_charge_power_w` and `pool_pump_power_w` so frontend charts can separate home load from Tesla/pool-pump overlays.
- The frontend can export, import, view backup status, and manually trigger a full SQLite backup from the Database tab.

## 7) Time Window Rules And Tesla Control

- The `Time Window` tab shows rule switches for each automation window, covering both notifications and automated operations.
- Rule state is stored in SQLite and exposed through `GET /api/time-window-rules` and `PUT /api/time-window-rules/{rule_code}`.
- `GET /api/time-window-rules` now also returns a `priority` snapshot for Tesla and pool pump, including the active winner, Tesla auto-control state, Tesla forced-max-charge threshold, pool-pump auto-control state, and the pool pump's rolling 24-hour runtime / minimum-runtime requirement.
- The dashboard lets you temporarily override the winning device through `PUT /api/time-window-priority-override` with `auto`, `tesla`, or `pool_pump`.
- Tesla auto control is now managed only from the `Priority Scores` card through `PUT /api/tesla/auto-control`; when disabled, the worker stops issuing automatic Tesla charging actions and Tesla remains manual-only. Automatic Tesla charging intervention is also home-only: the worker must see the Tesla connected and the Tesla location state as `home`, otherwise it observes without issuing start, stop, restart, or current-change actions.
- Tesla also exposes `PUT /api/tesla/force-max-charge-threshold`, which sets the SOC threshold below which overnight automation forces Tesla to charge at its maximum allowed current before sharing power with the pool pump.
- Pool-pump automation is now managed only from the `Priority Scores` card through `PUT /api/devices/pool-pump/auto-control`; when disabled, the worker stops issuing automatic pool-pump actions and the pump remains manual-only.
- Pool pump runtime protection is configurable through `PUT /api/devices/pool-pump/force-min-runtime-hours`, which sets the minimum runtime that should be reached within the rolling 24-hour lookback used by overnight automation.
- Tesla time-window operation rows remain visible in the dashboard, but they are now read-only snapshots that reflect the unified Tesla auto-control state instead of per-window switches.
- SAJ profile automation still respects per-window rule switches before issuing control actions. During the free-energy window (`11:00-15:00`), it first forces/holds SAJ in self-consumption; only if SAJ is already targeted at self-consumption and `sensor.saj_battery_power` still shows active discharge does it apply the slow-charge hold (`charging_control=on`, `discharging_control=off`, charge/discharge time masks `1/0`, and `400W` charge limits).
- Outside the free-energy window, SAJ profile automation can apply a one-time all-day TOU discharge target for the day, and the dashboard profile popover exposes TOU sell/buy power controls backed by the narrow SAJ manual charge/discharge APIs.
- The overnight shoulder window (`23:00-11:00`) now computes a shared overnight battery budget after reserving projected home load, then allocates the remaining power between Tesla and the pool pump according to the live priority winner, the Tesla forced-charge threshold, and the pool pump's 24-hour minimum-runtime requirement.
- The FOUR4FREE export window (`16:00-23:00`) can now temporarily pull forward Tesla charging and pool-pump runtime when either device is below its minimum support target, but only while auto control is enabled for that device and the live zero-import guard still has enough headroom.
- During that export window support mode, a dedicated `5s` fast guard now watches the live Solplanet Home Assistant signal and immediately sheds the interruptible auto-controlled loads when grid import starts, the export signal disappears, or Solplanet discharge rises above the `10kW` hard limit; worker status still records the current 5-minute Zero Hero observation for review in the History tab.
- Once the export window ends at `23:00`, Tesla export-window support is paused and the overnight shoulder rule takes over; the pool-pump daytime off-period guard remains exempt only during the active export window itself.
- Tesla manual start/stop now returns UI feedback states so the dashboard can show pending, success, and failure confirmation after `/api/tesla/control/charging`.
- Tesla manual current control is available through `POST /api/tesla/control/current`, and the combined dashboard shows pending current-change state until Home Assistant confirms it.
- The combined Tesla card now shows battery energy, live voltage, charge ETA, `home` / non-home location state, and an inline current-control trigger instead of only the raw charging power number.
- Manual inverter and backend actions now register operation definitions plus run history in SQLite so the UI can reconcile pending and completed control actions.
- The dashboard now includes an `Operations` tab backed by `GET /api/operations/timeline`, which merges automatic worker actions and manual operations into a single chronological timeline and expands automatic reasons into readable `Logic / Decision / Why / Math` blocks.
- The combined dashboard home view now goes straight from the weather card into the integrated flow card and no longer renders a separate overnight battery reference panel on the page.
- The combined dashboard now restores pool-pump state from the cached combined-flow snapshot for fast first paint, while manual toggles still use `POST /api/devices/pool-pump/toggle` and `GET /api/devices/pool-pump` remains available for direct live checks.
- `GET /api/energy-flow/combined` now includes the calculated usable battery total in both `display.items.usable_battery_total` and `energy_reference.usable_battery_total_kwh`; the dashboard still falls back to calculating it from SAJ/Solplanet SOC for old cached payloads.
- The overnight shoulder automation matrix now includes `pool_pump_overnight_control`, which can keep the pool pump inside the configured `23:00-05:00` battery budget and reports its runtime budget in worker status.
- That runtime budget now divides usable remaining battery by the current overnight runtime load, defined as current base home load plus pool-pump power, and also includes Tesla runtime power when Tesla is expected to stay active, so the dashboard formula matches the actual control calculation.
- The free-energy window (`11:00-15:00`) now also includes `pool_pump_free_energy_control` and Tesla-first shared-capacity handling: Tesla is evaluated before the pool pump during daytime priority hours, the pool pump is stopped when it blocks a viable Tesla charge target, and both can continue only when capacity remains within the grid budget (`15.0kW` cap).
- Worker check payloads now include explicit `execution_order`, `resource_reservation`, and `final_switch_states` (`tesla_enabled`, `pool_pump_enabled`) so the UI can show the final on/off result of each round directly.
- The Time Window dashboard now includes a device-priority card with fixed schedule scoring: daytime (`11:00-23:00`) gives Tesla `100` and pool pump `0`, overnight (`23:00-11:00`) gives Tesla `0` and pool pump `100`, while manual override can still force either device. The card also shows the pool pump's rolling 24-hour runtime and inline controls for both the Tesla forced-charge threshold and the pool-pump minimum-runtime target.
- Dashboard auto-refresh defaults to 30 seconds, keeps the last summary cached locally for instant first paint, reads the cached combined-flow and collector-status APIs without live Home Assistant recomputation, loads weather asynchronously, makes the manual `Refresh` button trigger a real reload, and only highlights the combined snapshot as stale when it is older than 5 minutes. Automatic refresh skips the `Time Window` tab so expanded panels and in-progress edits are preserved; opening the tab or using `Refresh` still reloads it.
- The combined dashboard lets you click the Solplanet inverter node to read and update the live Solplanet `Pin` charging limit.
- On phone-sized layouts, the dashboard now hides the weather panel and switches the combined flow card to a simplified mobile-first energy map with a central hub view that keeps load, remaining battery, live flow, pool pump, and Tesla state in view.
