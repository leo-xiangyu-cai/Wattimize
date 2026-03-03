# Wattimize Agent Guide

This file explains the current project state so future AI agents can continue work safely and consistently.

## 1) Project Goal

Wattimize is a self-managed local energy platform running on Docker/NAS.
Current phase focuses on Home Assistant (HA) integration for SAJ inverter data.
Long-term goal is multi-brand support (SAJ, Soulplanet, Tesla, others) with unified data and control APIs.

## 2) Current Scope (Implemented)

- Backend: FastAPI
- Data source: Home Assistant REST API using Long-Lived Access Token
- Static frontend: served by FastAPI at `/`
- Current focus: read SAJ entities and explore entities via filters + pagination

Not implemented yet:
- Write/control commands to HA services
- Integration-level catalog (real HA config entries)

## 3) Directory Layout

- `app/main.py`: FastAPI routes + static mount
- `app/home_assistant.py`: HA API client
- `app/config.py`: env config loading
- `app/static/index.html`: dashboard UI
- `app/static/app.js`: frontend data loading and pagination
- `app/static/styles.css`: frontend styles
- `requirements.txt`: Python deps
- `docker-compose.yml`: container run config
- `Dockerfile`: image build
- `.env`: real local secrets (ignored)

## 4) Runtime Config

Required:
- `ha_url` (example: `http://192.168.68.61:8123`)
- `ha_token` (HA long-lived token)

Optional:
- `solplanet_dongle_host`

Config source:
- `data/config.json` locally
- `/app/data/config.json` in Docker
- override path with `WATTIMIZE_CONFIG_PATH`

Security:
- Never commit secrets
- Keep token in config file or runtime env only

## 5) API Endpoints

Health and connectivity:
- `GET /api/health`
- `GET /api/ha/ping`
- `GET /api/storage/status`
- `GET /api/storage/daily-usage?system=saj|solplanet&day_utc=YYYY-MM-DD`
- `GET /api/storage/samples?system=saj|solplanet&page=1&page_size=100`

Core SAJ metrics:
- `GET /api/entities/core`

Catalogs:
- `GET /api/catalog/domains`
- `GET /api/catalog/brands` (brand is guessed from `entity_id` prefix, not official HA classification)

Entity explorer:
- `GET /api/entities?domain=&brand=&q=&page=1&page_size=80`
- Response includes: `count`, `total`, `page`, `page_size`, `has_next`, `has_prev`, `items`

Frontend:
- `GET /` serves static dashboard
- `GET /static/*` serves JS/CSS
- Added `Sampling` tab for SQLite sample table + daily usage/status summary

## 6) Important Data Semantics

- HA `/api/states` is a unified entity pool across integrations.
- "SAJ" filter currently relies on `entity_id` naming convention (e.g. `sensor.saj_*`).
- This is practical but not authoritative integration metadata.

## 7) Run Instructions

Local dev mode (recommended while iterating):

```bash
cd /Users/caixy/Leo/Wattimize
source .venv/bin/activate
uvicorn app.main:app --host 0.0.0.0 --port 18000 --reload
```

Open UI:
- `http://127.0.0.1:18000/`

Docker mode (after local verification):

```bash
docker compose up -d --build
```

## 8) Frontend Behavior

- Summary cards: system/HA/core status
- Core metrics section: values from `/api/entities/core`
- Entity explorer:
  - default brand filter is `saj`
  - supports `domain`, `brand`, `q`
  - fixed page size 80 in frontend
  - prev/next pagination
  - displays total entity count

## 9) Known Constraints

- No caching yet: every query fetches HA states live
- Persistence currently uses local SQLite (single-node design)
- No auth on FastAPI endpoints yet (LAN-only assumption)
- Error handling is basic but functional

## 10) Next Suggested Steps

1. Add write-control APIs with safety guardrails:
   - command whitelist
   - value range limits
   - cooldown/debounce
   - audit logging
2. Add storage (InfluxDB or TimescaleDB)
3. Add periodic collector service
4. Add Soulplanet adapter and unified metric mapping
5. Add real HA integration catalog endpoint (via HA websocket/config entries)

## 11) Agent Working Rules

- Prefer local dev mode first; use Docker after APIs are stable.
- Do not expose or log `HA_TOKEN`.
- Keep API changes reflected in frontend and README.
- Preserve backward compatibility for existing endpoints when possible.
- All code comments in submitted changes must be written in English.
- Any user-facing UI text must support multilingual/i18n; backend/internal-only text can remain English.
