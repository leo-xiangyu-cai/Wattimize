---
name: wattimize-current-system-debug
description: Debug the currently running Wattimize system on the NAS without changing code or starting local services. Use when the user asks to debug the current system, inspect current state, check what is happening now, compare live behavior with local source code, or describes a production/NAS Wattimize issue and wants read-only diagnosis.
---

# Wattimize Current System Debug

Use this skill for read-only diagnosis of the currently running Wattimize deployment.

The expected workflow is: first inspect live state from the NAS service, then use the local repository only for static code reading to explain or narrow the issue the user describes.

## Hard Rules

- Do not edit files, unless the user explicitly changes scope and asks for an implementation.
- Do not start a local Wattimize service.
- Do not run `make dev`, `docker compose up`, `uvicorn`, or any script that may auto-start the app.
- Do not use mutating API calls while debugging current state:
  - no `POST`
  - no `PUT`
  - no `PATCH`
  - no `DELETE`
- Do not import, export, repair, mutate, or directly write the SQLite database.
- Prefer HTTP GET requests to the NAS backend over SSH or direct database inspection.
- If a command fails because sandbox DNS/network blocks `tnas.local`, rerun the same read-only request with `sandbox_permissions: "require_escalated"`.

## NAS Endpoint

Default live service:

```text
http://tnas.local:18000
```

If the user provides another NAS IP or host, use that host with port `18000` unless they specify otherwise.

Quote URLs that contain `?` in shell commands:

```bash
curl -fsS --max-time 8 'http://tnas.local:18000/api/operations/timeline?limit=20'
```

## Initial Read-Only Snapshot

Start with a small status snapshot unless the user asks for a specific subsystem.

Recommended GET endpoints:

```bash
curl -fsS --max-time 5 http://tnas.local:18000/api/health
curl -fsS --max-time 8 http://tnas.local:18000/api/config/status
curl -fsS --max-time 8 http://tnas.local:18000/api/collector/status
curl -fsS --max-time 8 http://tnas.local:18000/api/storage/status
curl -fsS --max-time 8 http://tnas.local:18000/api/energy-flow/combined
curl -fsS --max-time 8 'http://tnas.local:18000/api/operations/timeline?limit=20'
```

Summarize the snapshot in human terms:

- whether the service is reachable
- whether config is complete
- last sample timestamps and stale/failure signals
- current energy flow values relevant to the user issue
- recent automatic operations and their reasons
- obvious contradictions between intended control state and measured readback

## Targeted Read-Only Endpoints

Use these only when relevant to the issue.

Energy and collection:

```bash
curl -fsS --max-time 8 http://tnas.local:18000/api/energy-flow/saj
curl -fsS --max-time 8 http://tnas.local:18000/api/energy-flow/solplanet
curl -fsS --max-time 8 http://tnas.local:18000/api/storage/daily-usage?system=combined
```

Control state readback:

```bash
curl -fsS --max-time 8 http://tnas.local:18000/api/saj/entities/core
curl -fsS --max-time 8 http://tnas.local:18000/api/saj/raw/core-entities
curl -fsS --max-time 8 http://tnas.local:18000/api/solplanet/control/state
curl -fsS --max-time 8 http://tnas.local:18000/api/devices/pool-pump
curl -fsS --max-time 8 http://tnas.local:18000/api/tesla/auto-control
curl -fsS --max-time 8 http://tnas.local:18000/api/time-window-rules
```

`/api/saj/entities/core` exposes the configured SAJ core energy sensors, but not all SAJ control/readback entities.

For SAJ mode, charging, or discharging questions, direct Home Assistant GET reads may be necessary. Read `.env` for `HA_URL` and `HA_TOKEN`, then query only `/api/states/{entity_id}`. Do not call `/api/services/...` unless the user explicitly changes scope from diagnosis to control.

Useful SAJ HA entities for read-only comparison:

```text
sensor.saj_app_mode
number.saj_app_mode_input
sensor.saj_inverter_working_mode
switch.saj_charging_control
switch.saj_passive_charge_control
sensor.saj_charge_time_enable_bitmask
number.saj_charge_time_enable_input
sensor.saj_grid_charge_power_limit
sensor.saj_battery_charge_power_limit
switch.saj_discharging_control
switch.saj_passive_discharge_control
sensor.saj_discharge_time_enable_bitmask
number.saj_discharge_time_enable_input
sensor.saj_grid_discharge_power_limit
sensor.saj_battery_discharge_power_limit
sensor.saj_battery_power
sensor.saj_ct_grid_power_total
sensor.saj_total_grid_power
sensor.saj_total_load_power
sensor.saj_pv_power
sensor.saj_battery_energy_percent
```

Reports and plans:

```bash
curl -fsS --max-time 8 'http://tnas.local:18000/api/reports/daily/index'
curl -fsS --max-time 8 'http://tnas.local:18000/api/reports/monthly/index?year=2026'
curl -fsS --max-time 8 http://tnas.local:18000/api/home-plan/electricity-plans
curl -fsS --max-time 8 http://tnas.local:18000/api/bills
```

Before using an endpoint, confirm from local source or README that it is a GET endpoint if there is any doubt.

## Local Code Inspection

After collecting live state and the user describes the symptom, inspect local code statically.

Use `rg` first:

```bash
rg -n "route_or_field_name|decision_reason|operation_name" app tests docs README.md
```

Good files to inspect by subsystem:

- FastAPI routes and workers: `app/main.py`
- Persistence and reports: `app/persistence.py`, `app/reports.py`
- Home Assistant reads: `app/home_assistant.py`
- Solplanet CGI/control: `app/solplanet.py`, `app/solplanet_control.py`
- Frontend state rendering: `app/static/app.js`, `app/static/index.html`, `app/static/styles.css`
- Deployment/runtime assumptions: `README.md`, `scripts/nas-common.sh`, `scripts/status.sh`

Do not run local render scripts for this skill. `skills/frontend-render-check/scripts/check_render.sh` can start local uvicorn and must be avoided unless the user explicitly changes the rule.

## Diagnosis Style

When reporting back:

- Separate observed live facts from source-code inferences.
- Use exact timestamps with timezone context when relevant.
- Point to the endpoint or local file that supports each claim.
- State uncertainty explicitly when live state is missing or stale.
- If a fix looks likely, describe the likely code area and risk, but do not edit until the user asks.

Useful structure:

```text
Live state:
- ...

Code path:
- ...

Likely explanation:
- ...

Next read-only check:
- ...
```
