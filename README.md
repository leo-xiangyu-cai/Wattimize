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
CORE_ENTITY_IDS=sensor.saj_pv_power,sensor.saj_battery_power,sensor.saj_total_grid_power,sensor.saj_total_load_power,sensor.saj_battery_energy_percent,sensor.saj_inverter_status
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
curl -s http://127.0.0.1:18000/api/catalog/domains | jq
curl -s http://127.0.0.1:18000/api/catalog/brands | jq
curl -s "http://127.0.0.1:18000/api/entities?brand=saj&domain=sensor&page=1&page_size=20" | jq
```

## 5) Run with Docker

```bash
cd /Users/caixy/Leo/Wattimize
docker compose up -d --build
curl -s http://127.0.0.1:18000/api/entities/core | jq
```
