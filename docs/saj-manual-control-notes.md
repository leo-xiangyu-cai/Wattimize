# SAJ manual control notes

Date: 2026-08-02 Australia/Sydney.

## Manual TOU discharge test

Goal: manually set SAJ to Time of Use and allow an all-day discharge window without using Wattimize backend SAJ APIs.

Home Assistant service calls used:

- `number.set_value`: `number.saj_app_mode_input = 1`
- `switch.turn_on`: `switch.saj_discharging_control`
- `number.set_value`: `number.saj_discharge_time_enable_input = 1`
- `text.set_value`: `text.saj_discharge1_start_time_time = 00:00`
- `text.set_value`: `text.saj_discharge1_end_time_time = 23:59`
- `number.set_value`: `number.saj_discharge1_power_percent_input = 40`
- `number.set_value`: `number.saj_discharge1_day_mask_input = 127`

Observed readback:

- `sensor.saj_app_mode` changed to `1` (Time of Use).
- `switch.saj_discharging_control` changed to `on`.
- `sensor.saj_discharge_start_time` / `sensor.saj_discharge_end_time` read back `00:00` / `23:59`.
- `sensor.saj_discharge_day_mask` read back `127`.
- `number.saj_discharge1_power_percent_input` accepted `40`, but `sensor.saj_discharge_power_percent` stayed at `50`.
- At the time of the test, PV exceeded SAJ-side load, so `sensor.saj_battery_power` stayed negative (charging) rather than forced discharging.

Current inference:

- The TOU mode and discharge window are being applied.
- The write target (`number.saj_discharge1_power_percent_input`) can diverge from the effective readback (`sensor.saj_discharge_power_percent`).
- In TOU, the all-day discharge window appears to permit discharge when needed; it did not force export while PV already covered load during this test.

## Follow-up: 100% discharge power input

Manual change:

- `number.set_value`: `number.saj_discharge1_power_percent_input = 100`
- Kept `sensor.saj_app_mode = 1`, `switch.saj_discharging_control = on`, and slot 1 at `00:00-23:59`, day mask `127`.

Observed readback after roughly 20 seconds:

- `number.saj_discharge1_power_percent_input` read `100`.
- `sensor.saj_discharge_power_percent` read `40`.
- `sensor.saj_battery_power` read about `1558 W`, so the battery was discharging.
- `sensor.saj_pv_power` read about `1348 W`.
- `sensor.saj_total_load_power` read about `2.906 kW`.
- `sensor.saj_ct_grid_power_total` read about `3 W`.

Current inference:

- Setting the input to `100` did not make the effective readback `100`.
- The effective discharge behavior looked closer to the actual readback (`40%`) than to the input value (`100%`).
- The all-day TOU discharge setup can cover load and hold SAJ CT grid near zero in this observed state.

## Cross-system control conflict

The house has two independent battery systems, SAJ and Solplanet. They do not coordinate intent or identify each other as batteries.

Observed/expected conflict:

- Solplanet can see export or surplus energy and decide to charge its own battery.
- SAJ sees Solplanet charging as ordinary household load, not as another battery.
- In self-consumption mode, SAJ may therefore discharge to cover that load.
- The combined result can become `SAJ battery discharging -> Solplanet battery charging`.

This is not necessarily a device fault. Each inverter is acting reasonably from its own sensor viewpoint, but the whole site can become inefficient because one battery charges another through AC conversion losses.

Control implication: do not use non-zero SAJ battery discharge power alone as proof that self-consumption mode failed. For self-consumption switching, judge success from `number.saj_app_mode_input`, `sensor.saj_app_mode`, and `switch.saj_discharging_control`; treat battery power as diagnostic context.

## Free-energy self-consumption guard

During the free-energy window (`11:00-15:00`), Wattimize first targets SAJ self-consumption and observes. If self-consumption is already the written target and `sensor.saj_battery_power` still shows active discharge (`>= 30W`), the worker applies a slow-charge hold that mirrors the locally verified app state:

- `number.saj_app_mode_input = 0`
- `switch.saj_discharging_control = off`
- `number.saj_discharge_time_enable_input = 0`
- `switch.saj_charging_control = on`
- `number.saj_charge_time_enable_input = 1`
- `number.saj_battery_charge_power_limit_input = 400`
- `number.saj_grid_max_charge_power_input = 400`

If self-consumption is not discharging, including when SAJ is already charging, the guard keeps self-consumption and does not apply the slow-charge hold.

## Morning grid-import guard

The morning protection rule is intentionally simple and does not depend on Solplanet state. Its goal is to avoid buying grid power when SAJ still has usable energy.

Rule code: `saj_morning_grid_import_guard`

Window:

- `05:00-09:00`

Start forced SAJ discharge when all are true:

- `combined.metrics.grid_w > 200 W` for 2 consecutive worker checks.
- `combined.metrics.battery1_soc_percent > 25%`.
- `combined.metrics.battery1_w` is not already discharging.

Start action:

- `PUT /api/saj/manual-discharge-power`
- payload equivalent: `{"power_w":2000}`

Keep forced discharge while:

- the guard is active,
- current time is still inside `05:00-09:00`,
- SAJ SOC stays above `20%`,
- and grid import has not been below `100 W` for 5 minutes.

Stop forced discharge when any are true:

- current time leaves `05:00-09:00`,
- `combined.metrics.battery1_soc_percent <= 20%`,
- `combined.metrics.grid_w < 100 W` for 5 minutes.

Stop action:

- `PUT /api/saj/self-consumption`

This guard treats Solplanet charging as context, not a trigger condition. The trigger is simply morning grid import while SAJ has enough charge.

## Local API wrapper

Only narrow local SAJ write APIs are currently exposed. The dashboard TOU controls and worker automation use these same wrappers.

Force all-day TOU discharge:

```bash
curl -s -X PUT http://<wattimize-host>:18000/api/saj/manual-discharge-power \
  -H 'Content-Type: application/json' \
  -d '{"power_w":2000}' | jq
```

The discharge-power request body accepts exactly one of:

- `power_w`: `0..5000`, converted to percent using the local `SAJ_RATED_POWER_W = 5000`.
- `power_percent`: `0..100`, written directly to `number.saj_discharge1_power_percent_input`.

The API always writes the same narrow manual setup:

- `number.saj_app_mode_input = 1`
- `number.saj_discharge_time_enable_input = 1`
- `text.saj_discharge1_start_time_time = 00:00`
- `text.saj_discharge1_end_time_time = 23:59`
- `number.saj_discharge1_power_percent_input = resolved power_percent`
- `number.saj_discharge1_day_mask_input = 127`
- `switch.saj_discharging_control = on`

Force all-day TOU charge:

```bash
curl -s -X PUT http://<wattimize-host>:18000/api/saj/manual-charge-power \
  -H 'Content-Type: application/json' \
  -d '{"power_w":2000}' | jq
```

The charge-power request body accepts exactly one of:

- `power_w`: `0..5000`, converted to percent using the local `SAJ_RATED_POWER_W = 5000`.
- `power_percent`: `0..100`, written directly to `number.saj_charge1_power_percent_input`.

The API writes the matching all-day TOU charge setup:

- `number.saj_app_mode_input = 1`
- `switch.saj_discharging_control = off`
- `switch.saj_charging_control = on`
- `number.saj_charge_time_enable_input = 1`
- `number.saj_discharge_time_enable_input = 0`
- `text.saj_charge1_start_time_time = 00:00`
- `text.saj_charge1_end_time_time = 23:59`
- `number.saj_charge1_power_percent_input = resolved power_percent`
- `number.saj_charge1_day_mask_input = 127`

The response includes both the requested/resolved power and a fresh SAJ control-state readback. The readback must be used to compare input values against effective `sensor.saj_*` values.

Switch back to self-consumption:

```bash
curl -s -X PUT http://<wattimize-host>:18000/api/saj/self-consumption | jq
```

The self-consumption API writes:

- `number.saj_app_mode_input = 0`
- `switch.saj_discharging_control = off`

The success criteria are mode/control readback, not zero battery power, because the cross-system conflict can still make SAJ serve Solplanet charging as household load.
