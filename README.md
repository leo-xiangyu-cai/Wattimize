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
- Adjustable sampling fields: `SAJ_SAMPLE_INTERVAL_SECONDS`, `SOLPLANET_SAMPLE_INTERVAL_SECONDS` (choices: `5/10/30/60/300`).

Config file path:
- Local default: `data/config.json`
- Docker default: `/app/data/config.json`
- Override with `WATTIMIZE_CONFIG_PATH`

Persistence config (optional):
- `WATTIMIZE_DB_PATH` (default: `data/energy_samples.sqlite3`)
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
curl -s "http://<wattimize-host>:18000/api/storage/export.csv" -o energy_samples.csv
curl -s -X POST "http://<wattimize-host>:18000/api/storage/import.csv?replace_existing=true" -F "file=@energy_samples.csv" | jq
```

`/api/energy-flow/solplanet` behavior:
- If `SOLPLANET_DONGLE_HOST` is set, backend reads Solplanet dongle CGI directly (`getdev*.cgi`).
- If not set, backend falls back to Home Assistant entity mapping.
- CGI cache/timeout values are backend constants, not env vars.

## 5) Run with Docker

```bash
cd /Users/caixy/Leo/Wattimize
docker compose up -d --build
curl -s http://<wattimize-host>:18000/api/saj/entities/core | jq
```

First-run configuration:
- Open `http://NAS_IP:18000/`.
- If `config.json` is missing, the page shows a config dialog.
- Config stores: `HA_URL`, `HA_TOKEN`, `SOLPLANET_DONGLE_HOST`, `SAJ_SAMPLE_INTERVAL_SECONDS`, `SOLPLANET_SAMPLE_INTERVAL_SECONDS`.
- Other values are backend constants (entity ids, port/scheme/ssl/cache/timeout), except the two sampling interval fields.
- Saved config is written to `/app/data/config.json` in the container by default.
- You can override config file path with `WATTIMIZE_CONFIG_PATH`.

## 6) Built-in SQLite Sampling

- Backend starts a background collector at startup.
- Default SAJ sampling frequency is every 5 seconds (`WATTIMIZE_SAMPLE_INTERVAL_SECONDS`).
- Default Solplanet sampling frequency is every 60 seconds (`WATTIMIZE_SOLPLANET_SAMPLE_INTERVAL_SECONDS`).
- Every sample stores current `pv_w/grid_w/battery_w/load_w/soc/inverter_status/balance` and raw flow payload.
- Daily usage endpoint integrates power snapshots into kWh (UTC day).
