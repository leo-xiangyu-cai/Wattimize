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
curl -s http://127.0.0.1:18000/api/saj/entities/core | jq
curl -s http://127.0.0.1:18000/api/soulplanet/entities/core | jq
curl -s http://127.0.0.1:18000/api/saj/energy-flow | jq
curl -s http://127.0.0.1:18000/api/soulplanet/energy-flow | jq
curl -s http://127.0.0.1:18000/api/soulplanet/cgi/getdev-device-2 | jq
curl -s http://127.0.0.1:18000/api/soulplanet/cgi/getdevdata-device-2 | jq
curl -s http://127.0.0.1:18000/api/soulplanet/cgi/getdevdata-device-3 | jq
curl -s http://127.0.0.1:18000/api/soulplanet/cgi/getdevdata-device-4 | jq
curl -s http://127.0.0.1:18000/api/soulplanet/cgi/getdefine | jq
curl -s http://127.0.0.1:18000/api/catalog/domains | jq
curl -s http://127.0.0.1:18000/api/catalog/brands | jq
curl -s "http://127.0.0.1:18000/api/entities?brand=saj&domain=sensor&page=1&page_size=20" | jq
```

`/api/soulplanet/energy-flow` behavior:
- If `SOLPLANET_DONGLE_HOST` is set, backend reads Solplanet dongle CGI directly (`getdev*.cgi`).
- If not set, backend falls back to Home Assistant entity mapping.
- `SOLPLANET_CACHE_SECONDS` controls short-lived API cache for CGI responses (recommended 2-5s).
- `SOLPLANET_REQUEST_TIMEOUT_SECONDS` caps each CGI request to avoid long blocking when dongle is slow.

## 5) Run with Docker

```bash
cd /Users/caixy/Leo/Wattimize
docker compose up -d --build
curl -s http://127.0.0.1:18000/api/saj/entities/core | jq
```

First-run configuration:
- Open `http://NAS_IP:18000/`.
- If `config.json` is missing, the page shows a config dialog.
- Only three fields are stored in config: `HA_URL`, `HA_TOKEN`, `SOLPLANET_DONGLE_HOST`.
- Other values are hardcoded constants in backend (entity ids, port/scheme/ssl/cache/timeout).
- Saved config is written to `/app/data/config.json` in the container by default.
- You can override config file path with `WATTIMIZE_CONFIG_PATH`.
