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

## Paid-energy grid-import guard

The priority is to minimize paid grid imports whenever SAJ has usable energy.
The legacy rule code `saj_morning_grid_import_guard` remains for saved settings
and operation-history compatibility, but is no longer restricted to mornings.

- Active every day from 15:00 until 11:00 the next day (Australia/Sydney).
- The four-hour 11:00–15:00 free-energy window is exempt.
- Start after two consecutive samples above 200 W import and SAJ SOC above 21%.
- Stop at the existing SAJ reserve floor of 20%, on rule disable, or during free energy.
- Missing grid, SAJ SOC or battery-power readings cannot trigger a new command.
- Samples older than three minutes are rejected; repeated cached samples do not
  count as consecutive observations or trigger retries.

The target is measured SAJ discharge plus measured grid import minus a 50 W
margin, clamped to 0–5000 W and quantized to 50 W. Signed battery power is used:
charging is subtracted so the controller does not mistake battery charging for
household demand. The same narrow `PUT /api/saj/manual-discharge-power` wrapper
clears TOU charging before setting the requested discharge target.

An active guard retargets after at least 120 seconds when the target changes by
200 W or more, or persistent import shows SAJ has not delivered the requested
power. An API acknowledgement is recorded as `await_next_power_sample`, not
as proof of actual discharge. Persistent imports remain visible in verification.
The guard can increase support even when SAJ is already discharging.

Near-zero import while supplying the home is success; it does not release the
guard. When the measured load no longer needs discharge for five minutes, the
guard returns to self-consumption. Export-window cleanup and the one-time
post-free discharge command defer to an active paid-energy guard, preventing
those rules from cancelling its target.

Read current decisions at `GET /api/collector/status`, under
`systems.combined.last_saj_morning_grid_import_guard_check`. Rule metadata is
available from `GET /api/time-window-rules`. Reserve limits, inverter capacity,
telemetry delay and device response still constrain achievable import reduction.

## Evening dispatch in two phases

**17:00–20:00:** prioritize SAJ discharge, retaining its 20% SOC floor and
5 kW power limit. The shared import/export controller owns the SAJ target and
retry timer, so paid-import protection cannot accidentally cancel evening support.
Solplanet runs in self-consumption mode (`mod_r = 2`) and supplies the actual
household deficit. It is not assigned a forced selling target or a zero discharge
limit. Mode writes are checked against device readback.

SAJ's target is constrained by combined usable energy above the configured
23:00 reserve plus its existing 0.5 kWh buffer, spread across the remaining time
until 23:00 and capped at 5 kW. Forced selling additionally requires enough
energy for the projected household load to 23:00. Missing/old battery or load
data and the SAJ floor prevent forced export.

**20:00–23:00:** end the special SAJ-first phase and resume the original combined
reserve tracking. The Solplanet monitor uses its original total recommendation
without subtracting SAJ output. When selling is warranted, it restores custom
mode and the discharge schedule; otherwise it retains household support. The
configured end-of-window reserve remains the target (currently 18 kWh on NAS,
plus the existing 0.5 kWh control buffer). The phase transition bypasses the
normal cooldown to reduce a previous SAJ export target to household support.

After 23:00, paid-energy import protection continues. Spending SAJ before then
is best effort, subject to reserve requirements and actual device output.
Read `saj_evening_plan` in the guard decision at `/api/collector/status` and
`home_support_only` in the Solplanet control result to inspect the active phase.

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
