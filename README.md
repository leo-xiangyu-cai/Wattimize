# Wattimize API (MVP)

## 1) Setup

```bash
cd /Users/caixy/Leo/Wattimize
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2) Configure

Fill your token in `.env`:

```env
HA_URL=http://192.168.68.61:8123
HA_TOKEN=YOUR_HOME_ASSISTANT_LONG_LIVED_TOKEN
SAJ_CORE_ENTITY_IDS=sensor.saj_pv_power,sensor.saj_battery_power,sensor.saj_ct_grid_power_total,sensor.saj_total_grid_power,sensor.saj_total_load_power,sensor.saj_battery_energy_percent,sensor.saj_inverter_status
SOLPLANET_CORE_ENTITY_IDS=sensor.solplanet_pv_power,sensor.solplanet_battery_power,sensor.solplanet_ct_grid_power_total,sensor.solplanet_total_grid_power,sensor.solplanet_total_load_power,sensor.solplanet_battery_energy_percent,sensor.solplanet_inverter_status
SOLPLANET_DONGLE_HOST=192.168.68.70
SOLPLANET_DONGLE_PORT=443
SOLPLANET_DONGLE_SCHEME=https
SOLPLANET_VERIFY_SSL=false
SOLPLANET_CACHE_SECONDS=3
SOLPLANET_REQUEST_TIMEOUT_SECONDS=12
```

## 3) Run

```bash
set -a; source .env; set +a
uvicorn app.main:app --host 0.0.0.0 --port 18000 --reload
```

Open frontend:

```bash
open http://127.0.0.1:18000/
```

## 4) Test

```bash
curl -s http://127.0.0.1:18000/api/health
curl -s http://127.0.0.1:18000/api/ha/ping | jq
curl -s http://127.0.0.1:18000/api/entities/core | jq
curl -s http://127.0.0.1:18000/api/entities/core/saj | jq
curl -s http://127.0.0.1:18000/api/entities/core/solplanet | jq
curl -s http://127.0.0.1:18000/api/energy-flow/saj | jq
curl -s http://127.0.0.1:18000/api/energy-flow/solplanet | jq
curl -s http://127.0.0.1:18000/api/solplanet/cgi/getdev-device-2 | jq
curl -s http://127.0.0.1:18000/api/solplanet/cgi/getdevdata-device-2 | jq
curl -s http://127.0.0.1:18000/api/solplanet/cgi/getdevdata-device-3 | jq
curl -s http://127.0.0.1:18000/api/solplanet/cgi/getdevdata-device-4 | jq
curl -s http://127.0.0.1:18000/api/solplanet/cgi/getdefine | jq
curl -s http://127.0.0.1:18000/api/catalog/domains | jq
curl -s http://127.0.0.1:18000/api/catalog/brands | jq
curl -s "http://127.0.0.1:18000/api/entities?brand=saj&domain=sensor&page=1&page_size=20" | jq
```

`/api/energy-flow/solplanet` behavior:
- If `SOLPLANET_DONGLE_HOST` is set, backend reads Solplanet dongle CGI directly (`getdev*.cgi`).
- If not set, backend falls back to Home Assistant entity mapping.
- `SOLPLANET_CACHE_SECONDS` controls short-lived API cache for CGI responses (recommended 2-5s).
- `SOLPLANET_REQUEST_TIMEOUT_SECONDS` caps each CGI request to avoid long blocking when dongle is slow.

## 5) Run with Docker

```bash
cd /Users/caixy/Leo/Wattimize
docker compose up -d --build
curl -s http://127.0.0.1:18000/api/entities/core | jq
```
