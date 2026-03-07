# API Inventory

Last updated: 2026-03-07

This file tracks all backend APIs and their current usage status.

Status values:
- `used`: called by current frontend (`app/static/app.js`)
- `debug`: not used by current frontend, but useful for debugging/ops
- `candidate`: likely removable after migration and confirmation

## 1) Frontend-used APIs (`used`)

### Dashboard
- `GET /api/health`
- `GET /api/ha/ping`
- `GET /api/energy-flow/saj`
- `GET /api/energy-flow/solplanet`

### Entities tab
- `GET /api/entities`

### Solplanet Raw tab
- `GET /api/solplanet/cgi/getdev-device-2`
- `GET /api/solplanet/cgi/getdevdata-device-2`
- `GET /api/solplanet/cgi/getdevdata-device-3`
- `GET /api/solplanet/cgi/getdevdata-device-4`
- `GET /api/solplanet/cgi/getdefine`
- `GET /api/solplanet/cgi/explore` (`debug` — probes getdev + getdevdata for device IDs 1-7)

## 2) Kept for brand-split and compatibility

### SAJ brand paths
- `GET /api/saj/entities/core` (`candidate` for frontend migration)
- `GET /api/saj/energy-flow` (`candidate` for frontend migration)
- `GET /api/saj/control/state` (`used`)
- `GET /api/saj/control/capabilities` (`used`)
- `PUT /api/saj/control/working-mode` (`used`)
- `PUT /api/saj/control/charge-slots/{slot}` (`used`)
- `PUT /api/saj/control/discharge-slots/{slot}` (`used`)
- `PUT /api/saj/control/toggles` (`used`)
- `PUT /api/saj/control/limits` (`used`)

### Soulplanet brand paths
- `GET /api/soulplanet/entities/core` (`candidate` for frontend migration)
- `GET /api/soulplanet/energy-flow` (`candidate` for frontend migration)
- `GET /api/soulplanet/cgi-dump` (`debug`)
- `GET /api/soulplanet/cgi/getdev-device-2` (`candidate` for frontend migration)
- `GET /api/soulplanet/cgi/getdevdata-device-2` (`candidate` for frontend migration)
- `GET /api/soulplanet/cgi/getdevdata-device-3` (`candidate` for frontend migration)
- `GET /api/soulplanet/cgi/getdevdata-device-4` (`candidate` for frontend migration)
- `GET /api/soulplanet/cgi/getdefine` (`candidate` for frontend migration)

### Solplanet alias paths (legacy spelling compatibility)
- `GET /api/solplanet/entities/core` (`candidate`)
- `GET /api/solplanet/energy-flow` (`used`)
- `GET /api/solplanet/control/state` (`used`)
- `PUT /api/solplanet/control/limits` (`used`)
- `PUT /api/solplanet/control/day-schedule/{day}` (`used`)
- `PUT /api/solplanet/control/day-schedule/{day}/slots/{slot}` (`used`)
- `PUT /api/solplanet/control/raw-setting` (`used`)
- `POST /api/solplanet/control/restart-api` (`used`)
- `GET /api/solplanet/cgi-dump` (`debug`)
- `GET /api/solplanet/cgi/getdev-device-2` (`used`)
- `GET /api/solplanet/cgi/getdevdata-device-2` (`used`)
- `GET /api/solplanet/cgi/getdevdata-device-3` (`used`)
- `GET /api/solplanet/cgi/getdevdata-device-4` (`used`)
- `GET /api/solplanet/cgi/getdefine` (`used`)

### Soulplanet alias paths
- `POST /api/soulplanet/control/restart-api` (`debug`, compatibility alias)

## 3) Generic/system APIs

- `GET /api/entities/core` (`candidate`, marked deprecated in response)
- `GET /api/entities/core/{system}` (`candidate`)
- `GET /api/energy-flow/{system}` (`used`)
- `GET /api/collector/status` (`debug`)
- `POST /api/collector/restart` (`debug`)
- `GET /api/catalog/domains` (`debug`)
- `GET /api/catalog/brands` (`debug`)

## 4) Cleanup plan (when you are ready)

1. Migrate frontend URLs to `/api/saj/...` and `/api/soulplanet/...`.
2. Keep aliases for one release cycle.
3. Remove generic and alias paths that are no longer used.
4. Keep `health`, `ha/ping`, and `entities` as base operational APIs.
