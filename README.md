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

Config file path:
- Local default: `data/config.json`
- Docker default: `/app/data/config.json`
- Override with `WATTIMIZE_CONFIG_PATH`

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
curl -s http://<wattimize-host>:18000/api/solplanet/cgi/getdev-device-2 | jq
curl -s http://<wattimize-host>:18000/api/solplanet/cgi/getdevdata-device-2 | jq
curl -s http://<wattimize-host>:18000/api/solplanet/cgi/getdevdata-device-3 | jq
curl -s http://<wattimize-host>:18000/api/solplanet/cgi/getdevdata-device-4 | jq
curl -s http://<wattimize-host>:18000/api/solplanet/cgi/getdefine | jq
curl -s http://<wattimize-host>:18000/api/catalog/domains | jq
curl -s http://<wattimize-host>:18000/api/catalog/brands | jq
curl -s "http://<wattimize-host>:18000/api/entities?brand=saj&domain=sensor&page=1&page_size=20" | jq
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
- Only three fields are stored in config: `HA_URL`, `HA_TOKEN`, `SOLPLANET_DONGLE_HOST`.
- Other values are backend constants (entity ids, port/scheme/ssl/cache/timeout).
- Saved config is written to `/app/data/config.json` in the container by default.
- You can override config file path with `WATTIMIZE_CONFIG_PATH`.
