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
- SAJ read APIs + SAJ control APIs (working mode, slot schedule, toggles, limits)
- Frontend includes `SAJ Control` tab for configuration and state inspection

Not implemented yet:
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
- `GET /api/storage/samples?system=saj|solplanet&start_utc=<ISO>&end_utc=<ISO>&page=1&page_size=100`
- `GET /api/storage/series?system=saj|solplanet&start_utc=<ISO>&end_utc=<ISO>&max_points=500`
- `GET /api/storage/usage-range?system=saj|solplanet&start_utc=<ISO>&end_utc=<ISO>`

Core SAJ metrics:
- `GET /api/entities/core`

SAJ control APIs:
- `GET /api/saj/control/state`
- `GET /api/saj/control/capabilities`
- `PUT /api/saj/control/working-mode`
- `PUT /api/saj/control/charge-slots/{slot}`
- `PUT /api/saj/control/discharge-slots/{slot}`
- `PUT /api/saj/control/toggles`
- `PUT /api/saj/control/limits`

Catalogs:
- `GET /api/catalog/domains`
- `GET /api/catalog/brands` (brand is guessed from `entity_id` prefix, not official HA classification)

Entity explorer:
- `GET /api/entities?domain=&brand=&q=&page=1&page_size=80`
- Response includes: `count`, `total`, `page`, `page_size`, `has_next`, `has_prev`, `items`

Frontend:
- `GET /` serves static dashboard
- `GET /static/*` serves JS/CSS
- Added `Sampling` tab for SQLite sample table + daily usage/status summary + line chart

## 6) Important Data Semantics

- HA `/api/states` is a unified entity pool across integrations.
- "SAJ" filter currently relies on `entity_id` naming convention (e.g. `sensor.saj_*`).
- This is practical but not authoritative integration metadata.

SAJ control semantics (important):
- `input_*` fields are writable targets (HA input entities).
- `actual_*` fields are readback states from sensor entities (effective device state).
- `day_mask` means weekdays for a slot (Mon..Sun bitmask).
  - `127` = all week, `0` = disabled for all weekdays.
- `charge_time_enable` / `discharge_time_enable` are slot-enable bitmasks (separate from weekday mask).
- Whether schedule is truly active can depend on both:
  - App mode
  - enable bitmask
  - charging/discharging switch state

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
- SAJ mode labels can differ by firmware/app version; numeric mode code alone is stable.
- Community docs and implementation notes can disagree on bit layout details across versions.

## 10) SAJ Mode & Doc References

Primary external references (community integration):
- Main repo: `https://github.com/stanus74/home-assistant-saj-h2-modbus`
- Charging guide: `https://github.com/stanus74/home-assistant-saj-h2-modbus/blob/main/wiki-en/charging.md`
- FAQ: `https://github.com/stanus74/home-assistant-saj-h2-modbus/blob/main/wiki-en/faq.md`
- Troubleshooting: `https://github.com/stanus74/home-assistant-saj-h2-modbus/blob/main/wiki-en/troubleshooting.md`
- Changelog (bitmask behavior changes): `https://github.com/stanus74/home-assistant-saj-h2-modbus/blob/main/CHANGELOG.md`

Important mapping note:
- In this Wattimize deployment, user-confirmed app labels are:
  - `0 = Self-Consumption`
  - `1 = Time of Use`
  - `2 = Backup`
  - `3 = Unknown/needs naming confirmation`
- In upstream integration code, AppMode control logic also uses:
  - `0 = self consumption`
  - `1 = force charge/discharge (active schedule)`
  - `3 = passive mode`
- Treat `mode_code -> label` as deployment-specific; always verify against the target SAJ app UI.

Dashboard mapping rule:
- When the user refers to a metric/value shown on the dashboard, treat the dashboard's current displayed mapping as the source of truth unless the user explicitly reports the dashboard is wrong.
- Do not remap user terms by guessing from raw entity ids, backend variable names, or brand assumptions if that would conflict with dashboard semantics.
- For combined flow semantics, align control logic with the dashboard labels. Current dashboard mapping is:
  - `battery1_*` / `inverter1_*` = SAJ side
  - `battery2_*` / `inverter2_*` = Solplanet/SoulPlanet side
- If a future dashboard change alters these labels, update backend control matching to follow the dashboard after confirmation.

## 11) Next Suggested Steps

1. Add write-control APIs with safety guardrails:
   - command whitelist
   - value range limits
   - cooldown/debounce
   - audit logging
2. Add storage (InfluxDB or TimescaleDB)
3. Add periodic collector service
4. Add Soulplanet adapter and unified metric mapping
5. Add real HA integration catalog endpoint (via HA websocket/config entries)

## 12) Agent Working Rules

- Prefer local dev mode first; use Docker after APIs are stable.
- Do not expose or log `HA_TOKEN`.
- Keep API changes reflected in frontend and README.
- Preserve backward compatibility for existing endpoints when possible.
- All code comments in submitted changes must be written in English.
- Any user-facing UI text must support multilingual/i18n; backend/internal-only text can remain English.

## 13) Local Skills

- `skills/frontend-render-check`: Run the Wattimize UI locally, capture a full-page screenshot, and inspect browser console/request failures when validating rendered frontend results.
