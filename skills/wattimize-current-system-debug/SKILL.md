---
name: wattimize-current-system-debug
description: Debug the currently running Wattimize system on the NAS over LAN or the remote NAS web interface, without changing code or starting local services. Use when the user asks to debug the current system, inspect current state, compare live behavior with local source code, or diagnose a production/NAS Wattimize issue.
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
- If a request is blocked by execution permissions, follow the current tool permission policy; do not assume escalation is available. Distinguish a tool restriction from an unreachable LAN.

## Select LAN or Remote Access First

If the user says they are outside the LAN, go directly to the remote workflow below. Otherwise probe the LAN service once with a short timeout:

```bash
curl -fsS --connect-timeout 3 --max-time 5 http://tnas.local:18000/api/health
```

If it returns healthy, use the LAN endpoint. If name resolution or connection fails, switch to the remote workflow instead of repeatedly retrying LAN SSH or asking the user for the known remote address. A failed probe only means this LAN route is unavailable; it does not prove the user is outside the LAN or the NAS is down. An HTTP error from the service is a live diagnostic signal, not evidence of network location.

### Remote workflow

Known entry points, verified on 2026-10-09:

- Wattimize: `https://solar.leocai.me`
- NAS administration: `https://nas.leocai.me/tos/#/desktop`

1. Use the public Wattimize endpoint for the same read-only GET snapshot listed below, replacing the LAN base URL. Verify the actual response; a login page or redirect is not an API result. Continue accessible public checks while authentication is pending.
2. When container state, logs, or LAN-only device reads are needed, open the NAS administration page in the browser and use its existing Cloudflare Access/TOS login. Do not ask for a remote SSH address or VPN when this web route is available.
3. If authentication expires, reuse existing user authorization. When the user has explicitly authorized reading the connected mailbox for this NAS login, use the mailbox connector to identify the account and retrieve only the relevant recent login code, then complete the login. Do not print or persist the code, credentials, cookies, or session URLs. Otherwise let the user complete authentication; this skill does not itself authorize mailbox access. Never change Access policies to get past a login failure.
4. Open TOS **Terminal** for read-only commands on the NAS. These examples run in the NAS web terminal, never in the Mac shell:

   ```bash
   curl -fsS --max-time 8 http://127.0.0.1:18000/api/health
   curl -fsS --max-time 8 http://127.0.0.1:18000/api/collector/status
   /Volume1/@apps/DockerEngine/dockerd/bin/docker ps --filter name=wattimize
   /Volume1/@apps/DockerEngine/dockerd/bin/docker logs --since 10m --tail 100 wattimize
   ```

   The Docker path and container name are known defaults; verify them if the command fails. Inspect only relevant configuration fields and avoid dumping environment variables or secrets. For LAN-only Home Assistant reads, prefer Wattimize's readback endpoints; if insufficient, perform narrowly scoped GET requests from the NAS using its existing configuration without copying credentials to chat or local files.
5. Verify the user-visible issue on the public Wattimize page. Report live timestamps with the effective site timezone (`/api/config/status`: `local_timezone`, `now_local`), current control window, and freshness. A running container alone does not prove the application or control loop is healthy.

If both public API and NAS web access fail, report which layer failed (DNS, Access, Tunnel, or application) and what remains unverified. Do not start a local runtime, expose SSH, alter routing, restart containers, or upload code as a debugging fallback. If the user asks for a fix, switch to the implementation/deployment workflow and preserve the NAS database; TOS File Manager plus Terminal is the remote code-deployment route already used successfully.

For additional NAS access troubleshooting, consult `/Users/caixy/Leo/Tnas/.agents/skills/tnas-remote-access/SKILL.md` if present. Current explicit user authorization takes precedence over that skill's default manual-login instructions.

## NAS Endpoint

Default live service:

```text
http://tnas.local:18000
```

If the user provides another LAN NAS IP or host, use that host with port `18000` unless they specify otherwise. Remote HTTPS entry points use their normal HTTPS port, not `18000`.

Quote URLs that contain `?` in shell commands:

```bash
curl -fsS --max-time 8 'http://tnas.local:18000/api/operations/timeline?limit=20'
```

## Initial Read-Only Snapshot

Start with a small status snapshot unless the user asks for a specific subsystem. Use the base URL selected above; the examples below show the LAN form.

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
