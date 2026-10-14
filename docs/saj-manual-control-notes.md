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

## Free-energy full-power charging

During 11:00–15:00 Sydney time the worker enforces SAJ TOU mode, disables
scheduled discharge, enables only charge slot 1 at 100% for 11:00–15:00 every
day, and restores both battery/grid charge limits to 1000 (permille = 100%).
It verifies effective mode, masks, slot and charge-limit sensors; PV charging
alone does not prove the configuration is correct. The bounded slot prevents
forced charging beyond the free window if the worker becomes unavailable.
SAJ writes have a five-minute cooldown: repeated optimistic HA switch updates
can reset its regular polling timer and starve effective readback.
The previous self-consumption/40% slow-charge policy is superseded.

Solplanet checks Custom mode, the 11:00–15:00 charge slot and 10 kW Pin even
when already charging from PV. Its 95% target and five-minute write cooldown
remain. BMS and inverter limits still determine actual power.

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

## Evening SAJ-first dispatch (2026-09-16)

From 17:00 to 23:00 Australia/Sydney, SAJ is requested to discharge at up to
5 kW while its SOC exceeds 20%. The combined overnight reserve calculation
no longer vetoes this SAJ-first target. At 20:00 the priority continues;
Solplanet's normal export allocation resumes only once SAJ reaches its 20%
floor, subject to the existing reserve calculation. Unknown SAJ SOC prevents
forced SAJ discharge and does not authorize Solplanet selling.

During SAJ priority, Solplanet uses custom mode and an active discharge slot.
Its Pout is limited to the measured residual (`battery2_w + grid_w`, clamped to
0–10 kW), not an export target. When SAJ covers the load, that target falls to
zero; when SAJ fails to deliver or home load exceeds its capacity, Solplanet
may cover the actual deficit to avoid grid imports. Missing residual telemetry
falls back to self-consumption until readings recover. Both the schedule and
work mode are read back before reporting configuration confirmation.

The SAJ controller checks actual battery power every sample and retries
undelivered evening targets after 60 seconds, even without grid import.
`last_saj_morning_grid_import_guard_check.saj_delivery` distinguishes measured
output from a requested target. SAJ commands explicitly reassert TOU mode after
HA switch handlers, which can queue their own AppMode changes. The manual API
also exposes effective slot/mask verification separately from command receipt.

This is a control-policy and command-order fix, not proof that the inverter
has delivered 5 kW. Evening power, SOC and grid readings must confirm that.
Solar surplus, inverter limits and device response can prevent requested output.
The morning Tesla protection remains in force independently.

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

## 2026-09-16: morning Tesla import protection

Update 2026-09-21: the rules below now apply before 08:00. The 08:00–11:00
window uses [pre-free-energy dispatch](pre-free-energy.md): shared Tesla/pump
power budgeting, five-minute persisted import cooldown and clean-sample retry
replace the whole-morning latch. Import still sheds flexible loads first.

The NAS showed about 4.7 kW solar, SAJ charging at 2 kW, Tesla charging at
3 kW, and 1.7–1.9 kW grid import. The existing SAJ guard targeted 0 W
but actual battery power remained negative. Stopping Tesla removed import.

Before 11:00 Australia/Sydney, fresh positive grid power while Tesla is
charging/requested at home now stops Tesla on the first sample and persists
`tesla_stop_until_11_date` in the import-guard state. Start and restart commands
cannot bypass this date latch. The stop is independent of the SAJ rule toggle,
SOC floor, and inverter retry cooldown; failed stops retry while latched.
The latch expires at 11:00 and does not carry into the next day.

Morning automatic allocation is additionally capped by measured Tesla power
minus signed grid power minus 200 W headroom. Positive import, missing metrics,
or stale samples provide no charging budget. Battery energy estimates and
low-Tesla-SOC priority cannot override this cap. This deliberately prefers
leaving Tesla stopped when SAJ cannot deliver power. The guard reacts at collector
sample cadence; vehicle/HA command and telemetry latency still apply.

### 17:12 live follow-up: effective discharge limits were 50%

On 2026-09-16 the evening priority held Solplanet near 0 W, but SAJ delivered
only about 1.8–1.9 kW with roughly 1 kW PV and inverter output around 2.5 kW.
Slot 1 and TOU mode had taken effect. Both effective discharge limit sensors
were 50%, although the HA input entities displayed their stale/default 1100.

Nonzero manual/worker discharge commands now inspect
`sensor.saj_battery_discharge_power_limit` and
`sensor.saj_grid_discharge_power_limit`. When below 100% or unavailable, they
write 1000 (100% in permille, not 110%) to the corresponding input entities.
Slot targets remain bounded by the existing 5 kW inverter rating. Effective
limits are exposed in `/api/saj/control/state` and included in configuration
verification; input values alone are never proof of applied capacity.

### 20:12 follow-up: latch the exhausted evening state

SAJ supplied about 5 kW after 18:00 and reached 20% at 19:36. Its reported SOC
then rebounded to 21% without usable discharge, which incorrectly restarted
SAJ priority and put Solplanet back into slow residual-power tracking.
The worker now persists `evening_depleted_date` once a fresh evening sample
hits 20%, or shows at most 21% with no meaningful discharge. That evening's
handover survives SOC rebound and process restarts. A zero-power TOU discharge
hold ends the SAJ discharge request. The initial implementation also stopped
Solplanet selling; that was incorrect and was removed on 2026-09-17. SAJ priority
means order of discharge, not a prohibition on subsequent Solplanet export.
After handover, Solplanet resumes the normal selling target toward the configured
23:00 reserve (including its existing buffer and household-load projection).
Self-consumption is used only when the reserve budget permits no sale.
Cross-charging is addressed by the SAJ charge limits below, not by disabling sales.
The latch applies only to its calendar-date evening; next day's normal charging
and SAJ priority remain available. Missing/stale samples cannot set a new latch.

Further live verification found SAJ still charging near 2 kW after the
Solplanet mode change. The exhausted-state hold therefore also saves the two
effective SAJ charge limits and writes zero to both charge-limit inputs. The
saved values persist across restarts and are restored at the next free-energy
window (11:00), or if the guard is disabled. Do not clear the saved limits
without restoring them. This prevents overnight battery-to-battery charging
without permanently disabling next day's charging.
