---
name: saj-power-mode-control
description: Operate and reason about this Wattimize deployment's SAJ H2 mode and power controls through direct Home Assistant REST calls. Use when adjusting SAJ app mode, passive discharge, TOU discharge slots, grid/battery charge or discharge limits, or when converting observed HA behavior into Wattimize API logic.
---

# SAJ Power Mode Control

Use this skill for this Wattimize project's SAJ mode/power work. It captures behavior verified on 2026-08-01 from Home Assistant readback, not just upstream labels.

## Ground Rules

- Prefer direct Home Assistant REST calls while validating control behavior.
- Do not assume a write target succeeded because an `input` entity changed. Always compare with `sensor.*` or `switch.*` readback after a 15-30 second delay.
- Treat `sensor.saj_app_mode` as actual mode. `number.saj_app_mode_input` is only the write target and can lag or disagree.
- Treat `sensor.saj_battery_power` as the final outcome:
  - positive = battery discharging into the house/system
  - negative = battery charging
  - near zero = idle
- For grid state, read both `sensor.saj_ct_grid_power_total` and `sensor.saj_total_grid_power`; they can disagree. Do not build new API behavior from one grid sensor without checking actual battery power and load balance.
- Use `HA_URL` and `HA_TOKEN` from `.env` for direct HA calls.

## Direct HA Command Pattern

Read:

```bash
set -a; source .env; set +a
curl -fsS --max-time 12 "$HA_URL/api/states/sensor.saj_app_mode" \
  -H "Authorization: Bearer $HA_TOKEN"
```

Set number:

```bash
set -a; source .env; set +a
curl -fsS --max-time 12 -X POST "$HA_URL/api/services/number/set_value" \
  -H "Authorization: Bearer $HA_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"entity_id":"number.saj_app_mode_input","value":3}'
```

Set switch:

```bash
set -a; source .env; set +a
curl -fsS --max-time 12 -X POST "$HA_URL/api/services/switch/turn_on" \
  -H "Authorization: Bearer $HA_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"entity_id":"switch.saj_passive_discharge_control"}'
```

## Verified Mode Behavior

Local behavior is firmware/integration-specific. Trust live readback over generic labels.

- `mode=0`: expected self-consumption target, but during testing it allowed unwanted charging when the actual mode/control state had drifted.
- `mode=1`: active/TOU path. It accepted discharge slot config and toggles but did not reliably produce discharge in the tested state.
- `mode=2`: accepted by HA and produced real battery discharge during testing. This is not a power value; it is app mode.
- `mode=3`: upstream integration calls this passive mode, but on this deployment the inverter did not hold it during fixed-output tests; `number.saj_app_mode_input` reverted to `0` and `sensor.saj_app_mode` stayed `0`.
- `mode=5`: rejected by HA because `number.saj_app_mode_input` has range `0..3`.

## Entities

Mode:

- Write target: `number.saj_app_mode_input`
- Actual readback: `sensor.saj_app_mode`
- Related readback: `sensor.saj_inverter_working_mode`

Primary outcome sensors:

- `sensor.saj_battery_power`
- `sensor.saj_battery_energy_percent`
- `sensor.saj_pv_power`
- `sensor.saj_total_load_power`
- `sensor.saj_ct_grid_power_total`
- `sensor.saj_total_grid_power`
- `sensor.saj_inverter_power`

Charging controls:

- `switch.saj_charging_control`
- `switch.saj_passive_charge_control`
- `number.saj_charge_time_enable_input`
- `sensor.saj_charge_time_enable_bitmask`
- `number.saj_grid_max_charge_power_input`
- `sensor.saj_grid_charge_power_limit`
- `number.saj_battery_charge_power_limit_input`
- `sensor.saj_battery_charge_power_limit`

Verified force/import charging path:

- During the active buy/charge window (`11:00-14:00`, day mask `127`, charge power `100%`), baseline state had `sensor.saj_charge_time_enable_bitmask=0`, `switch.saj_charging_control=off`, and both charge limit readbacks at `0%`. The battery was not charging from grid.
- Setting `number.saj_battery_charge_power_limit_input=500`, `number.saj_grid_max_charge_power_input=500`, `number.saj_charge_time_enable_input=1`, `number.saj_app_mode_input=1`, and turning on `switch.saj_charging_control` succeeded after readback delay.
- At about 70 seconds after applying those settings, `sensor.saj_app_mode=1`, `sensor.saj_charge_time_enable_bitmask=1`, `switch.saj_charging_control=on`, both charge limit sensors read `50%`, and `sensor.saj_battery_power=-2361 W`. This proves forced/import charging is controllable through the exposed HA entities.
- For this deployment, leave `number.saj_grid_max_charge_power_input=0` only when intentionally preventing buy-grid charging. To force import charging, it must be above `0` and read back through `sensor.saj_grid_charge_power_limit`.

Discharging controls:

- `switch.saj_discharging_control`
- `switch.saj_passive_discharge_control`
- `number.saj_discharge_time_enable_input`
- `sensor.saj_discharge_time_enable_bitmask`
- `number.saj_grid_max_discharge_power_input`
- `sensor.saj_grid_discharge_power_limit`
- `number.saj_battery_discharge_power_limit_input`
- `sensor.saj_battery_discharge_power_limit`

TOU discharge slot 3:

- `text.saj_discharge3_start_time_time`
- `text.saj_discharge3_end_time_time`
- `number.saj_discharge3_power_percent_input`
- `number.saj_discharge3_day_mask_input`
- Readbacks: `sensor.saj_discharge_3_start_time`, `sensor.saj_discharge_3_end_time`, `sensor.saj_discharge_3_power_percent`, `sensor.saj_discharge_3_day_mask`

Passive power:

- `number.saj_passive_battery_discharge_power_input`
- `number.saj_passive_grid_discharge_power_input`
- Readbacks: `sensor.saj_passive_battery_discharge_power`, `sensor.saj_passive_grid_discharge_power`

## Normal Overnight Target

For 23:00-11:00, the expected behavior is: battery supports house load when load is high and SOC is sufficient. The practical verified target is self-consumption (`mode=0`) with conservative discharge limits, not a guaranteed passive fixed-output mode.

Minimum safe validation loop:

1. Read current state: mode, SOC, battery power, PV, load, both grid sensors, passive/TOU toggles.
2. If SOC is above the local reserve and grid import or load is high, keep self-consumption mode (`number.saj_app_mode_input=0`) unless a later test proves another mode is safer.
3. Turn charging paths off unless deliberately charging:
   - `switch.saj_charging_control=off`
   - `switch.saj_passive_charge_control=off`
   - `number.saj_charge_time_enable_input=0`
4. Bound discharge conservatively:
   - write `number.saj_battery_discharge_power_limit_input`
   - write `number.saj_grid_max_discharge_power_input`
   - wait long enough for `sensor.saj_battery_discharge_power_limit` and `sensor.saj_grid_discharge_power_limit` readbacks to update; this can take longer than 25 seconds.
5. Wait 15-30 seconds.
6. Confirm actual success with `sensor.saj_battery_power > 0`. If it is still near zero, report partial success and inspect mode/readback instead of claiming discharge works.
7. If export appears or load falls, reduce/disable passive discharge.

## API Implementation Guidance

When converting this to Wattimize APIs:

- Build APIs around outcomes, not just HA service responses.
- Return `succeeded`, `partial_success`, or `failed`.
- Include readback fields in responses: requested mode, actual mode, battery power, grid sensors, SOC, toggles, and stale timestamps.
- Do not rely only on `number.*_input` values for UI state.
- Add dynamic safety:
  - do not discharge below SOC reserve
  - stop or reduce discharge when house load falls
  - stop or reduce discharge if export exceeds a small threshold for more than one sample
  - turn off charge controls when preventing grid charging
  - restore configured normal mode after a temporary test window if requested

## Fixed Output Findings

As of 2026-08-01 testing, the exposed HA controls did not prove a true "fixed battery output W" mode.

- In `mode=3` with passive discharge on, the inverter behaved like zero-import/self-consumption support:
  - high house load produced battery output around 3.4 kW
  - when PV roughly covered load, battery output fell near idle
- Lowering `number.saj_passive_battery_discharge_power_input` and `number.saj_passive_grid_discharge_power_input` to `400` read back correctly but did not by itself force a fixed 2 kW battery output.
- In active/TOU mode, changing `number.saj_discharge3_power_percent_input` from `40` to `10` read back correctly, but did not change actual output while the inverter was following load.
- `number.saj_battery_discharge_power_limit_input=400` read back as `sensor.saj_battery_discharge_power_limit=40%`, but output remained load-following in the high-load test.
- `number.saj_battery_discharge_power_limit_input=100` caused battery power to drop near idle; it did not produce a stable low fixed output.
- Setting `number.saj_export_limit_input=400` was accepted, but `sensor.saj_anti_reflux_power_limit` still read `1000` during the test and did not make the battery hold 2 kW when PV covered load.
- A corrected passive test wrote `number.saj_passive_charge_enable_input=1`, `number.saj_passive_battery_discharge_power_input=200`, `number.saj_passive_grid_discharge_power_input=200`, and `number.saj_app_mode_input=3`. The device reverted `number.saj_app_mode_input` to `0`, `sensor.saj_app_mode` stayed `0`, and battery power did not move toward a fixed 2 kW target.
- `number.saj_battery_discharge_power_limit_input=200` plus `number.saj_grid_max_discharge_power_input=200` eventually read back as `20%` on both limit sensors. This is the verified way to cap discharge near 2 kW on a 10 kW inverter, but it is a maximum output limit, not a forced fixed output.
- SolPlanet coupling can mask house-load interpretation because excess SAJ output may be absorbed by the other system. Judge SAJ commands by before/after changes in `sensor.saj_battery_power`, `sensor.saj_inverter_power`, and SAJ limit readbacks, not only by total house load or grid import.
- A short TOU forced-discharge test during an active slot (`10:00-11:00`, `100%`, day mask `127`) set `number.saj_discharge_time_enable_input=1`, `number.saj_app_mode_input=1`, and turned on `switch.saj_discharging_control`. For about 90 seconds, `sensor.saj_app_mode=1` and the switch read `on`, but `sensor.saj_battery_power` stayed near idle (`13-21 W`). The inverter then reverted to `sensor.saj_app_mode=0` and `switch.saj_discharging_control=off`. This path did not prove battery export/selling.
- `mode=2` was retested briefly after the TOU test and was not accepted in that state; `number.saj_app_mode_input` and `sensor.saj_app_mode` returned to `0`, with battery power still near idle.

Until a lower-level fixed-output register is found, Wattimize should model SAJ control as:

- self-consumption / zero-import support,
- bounded discharge support,
- not a guaranteed fixed-Watt dispatch target.

## Known Traps

- `number.saj_app_mode_input=5` is invalid in this deployment.
- `number.saj_grid_max_charge_power_input=0` and passive charge off stopped the charging control path but did not by itself prove all charge behavior was gone.
- `number.saj_battery_charge_power_limit_input=0` changed the input entity, but readback stayed at `sensor.saj_battery_charge_power_limit=100%` during testing; do not depend on it until verified again.
- In active/TOU mode, slot bitmasks and toggles can read correctly while no real discharge happens; only `sensor.saj_battery_power` proves the outcome.
- Do not promise fixed 2 kW output from the currently known HA entities. Verify actual fixed-output behavior with `sensor.saj_battery_power` before adding such an API.
- `number.*_input` values can update immediately while corresponding `sensor.*` readbacks lag by 30+ seconds. Wait and re-read before concluding a limit write failed.
