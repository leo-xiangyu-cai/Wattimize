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

Config storage:
- Stored in SQLite table `app_config`
- Local default DB: `data/wattimize.sqlite3`
- Docker default DB path is controlled by `WATTIMIZE_DB_PATH`
- Existing legacy `config.json` is auto-migrated on first load

Persistence config (optional):
- `WATTIMIZE_DB_PATH` (default: `data/wattimize.sqlite3`)
- `WATTIMIZE_SAMPLE_INTERVAL_SECONDS` (default: `5`, choices: `5/10/30/60/300`)
- `WATTIMIZE_SOLPLANET_SAMPLE_INTERVAL_SECONDS` (default: `60`, choices: `5/10/30/60/300`)

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
curl -s http://<wattimize-host>:18000/api/saj/control/state | jq
curl -s http://<wattimize-host>:18000/api/saj/control/profile | jq
curl -s http://<wattimize-host>:18000/api/saj/control/capabilities | jq
curl -s http://<wattimize-host>:18000/api/time-window-rules | jq
curl -s -X PUT http://<wattimize-host>:18000/api/saj/control/profile -H 'Content-Type: application/json' -d '{"profile_id":"time_of_use"}' | jq
curl -s -X PUT http://<wattimize-host>:18000/api/saj/control/working-mode -H 'Content-Type: application/json' -d '{"mode_code":1}' | jq
curl -s -X PUT http://<wattimize-host>:18000/api/saj/control/toggles -H 'Content-Type: application/json' -d '{"charging_control":true,"discharging_control":false,"charge_time_enable_mask":127,"discharge_time_enable_mask":127}' | jq
curl -s -X PUT http://<wattimize-host>:18000/api/saj/control/limits -H 'Content-Type: application/json' -d '{"battery_charge_power_limit":1100,"battery_discharge_power_limit":1100,"grid_max_charge_power":1100,"grid_max_discharge_power":1100}' | jq
curl -s -X PUT http://<wattimize-host>:18000/api/saj/control/charge-slots/1 -H 'Content-Type: application/json' -d '{"start_time":"01:00","end_time":"02:00","power_percent":25}' | jq
curl -s -X PUT http://<wattimize-host>:18000/api/saj/control/discharge-slots/1 -H 'Content-Type: application/json' -d '{"start_time":"18:00","end_time":"20:00","power_percent":30}' | jq
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
curl -s -X POST http://<wattimize-host>:18000/api/tesla/control/charging -H 'Content-Type: application/json' -d '{"enabled":true}' | jq
curl -s -X POST http://<wattimize-host>:18000/api/tesla/control/current -H 'Content-Type: application/json' -d '{"amps":16}' | jq
curl -s http://<wattimize-host>:18000/api/devices/pool-pump | jq
curl -s -X POST http://<wattimize-host>:18000/api/devices/pool-pump/toggle -H 'Content-Type: application/json' -d '{"enabled":true}' | jq
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

`make dev` now switches execution to local: if local is not already running, it stops the NAS container, pulls the NAS SQLite database into `./data/`, and then starts local `docker compose`. If local is already running, it treats the command as a local code refresh and skips database sync. After the containers start, the script waits for the local collector status endpoint to become healthy before returning.

To stop only the NAS container without starting local services:

```bash
make nas-stop
```

Build and deploy to TerraMaster NAS over SSH:

```bash
make nas-deploy
```

`make nas-deploy` now stops local `docker compose` services first. If the NAS container is not already running, it pushes the local SQLite database to the NAS before deploying. If the NAS container is already running, it treats the command as a code-only refresh and skips database sync.

For the first sync from local to NAS while the NAS is still running, use:

```bash
make nas-stop
make nas-deploy
```

Supported overrides for `make nas-deploy`:
- `NAS_HOST`, `NAS_PORT`, `NAS_USER`, `SSH_KEY`
- `CONTAINER_NAME`, `IMAGE_NAME`, `IMAGE_REPOSITORY`
- `NAS_DATA_DIR`, `HOST_PORT`, `CONTAINER_PORT`, `TZ_VALUE`
- `DOCKER_BIN`, `RETENTION_DAYS`

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
- Daily usage endpoint integrates power snapshots into kWh (UTC day).
- `system=combined` is available on range usage and trend series endpoints for the frontend overall view.
- The `History` tab combines range selection with an energy breakdown card, a power timeline chart, and compact date/week/month context for the selected window.
- The frontend can export and import the full SQLite database from the Database tab.

## 7) Time Window Rules And Tesla Control

- The `Time Window` tab shows rule switches for each automation window, covering both notifications and automated operations.
- Rule state is stored in SQLite and exposed through `GET /api/time-window-rules` and `PUT /api/time-window-rules/{rule_code}`.
- SAJ profile automation and Tesla charge automation respect these rule switches before issuing control actions.
- The overnight shoulder window (`23:00-11:00`) now starts Tesla charging only when the projected usable battery minus home load to `08:00` and remaining pool-pump load to `05:00` stays above the configured `1.0kWh` reserve, and stops once that budget is exhausted.
- Tesla manual start/stop now returns UI feedback states so the dashboard can show pending, success, and failure confirmation after `/api/tesla/control/charging`.
- Tesla manual current control is available through `POST /api/tesla/control/current`, and the combined dashboard shows pending current-change state until Home Assistant confirms it.
- The combined Tesla card now shows battery energy, live voltage, charge ETA, and an inline current-control trigger instead of only the raw charging power number.
- Manual inverter and backend actions now register operation definitions plus run history in SQLite so the UI can reconcile pending and completed control actions.
- The combined dashboard now includes an overnight battery reference panel that projects remaining usable battery at the next `08:00` and `11:00`, based on recent combined base home-load history with Tesla charging and pool-pump load excluded.
- The combined dashboard now exposes pool-pump live state and manual toggle controls through `GET /api/devices/pool-pump` and `POST /api/devices/pool-pump/toggle`.
- The overnight shoulder automation matrix now includes `pool_pump_overnight_control`, which can keep the pool pump inside the configured `23:00-05:00` battery budget and reports its runtime budget in worker status.
- That runtime budget now divides usable remaining battery by the current overnight runtime load, defined as current home load excluding Tesla charging plus the pool-pump power, so the dashboard formula matches the actual control calculation.
- The free-energy window (`11:00-14:00`) now also includes `pool_pump_free_energy_control`, which only starts the pool pump after Tesla priority is already satisfied and only when the predicted combined grid draw with the pump stays within the configured `15.0kW` cap.
- Dashboard auto-refresh defaults to 30 seconds, keeps the last summary cached locally, and shows background refresh state while new data is loading.
- The combined dashboard lets you click the Solplanet inverter node to read and update the live Solplanet `Pin` charging limit.
- On phone-sized layouts, the dashboard now hides the weather/notification/reference panels and switches the combined flow card to a simplified mobile-first energy map with a central hub view that keeps load, remaining battery, live flow, pool pump, and Tesla state in view.
