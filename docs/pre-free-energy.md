# 08:00–11:00 pre-free-energy dispatch

All times use Australia/Sydney. This window takes precedence over the older
overnight Tesla allocation and morning pump-off schedule. Device auto-control
switches still apply. The existing workers and operation logs carry a
`pre_free_energy` plan, including the reserve, charging-space target, reclaimed
power, output limit, and chosen loads.

## Objective and energy reserve

Use surplus energy before 11:00 so the batteries have room for free charging,
without deliberately importing paid energy. Tesla is preferred below 80% SOC;
the configured low-SOC threshold grants urgency, not permission to buy power.
The pump may use the remainder or substitute when Tesla cannot charge, with a
9-hour rolling 24-hour runtime ceiling. No load is created merely to waste power.

For the configured 50 kWh Solplanet battery, the 95% target and 10% discharge
floor leave 42.5 kWh usable. The four-hour 10 kW free charge is budgeted at 90%
efficiency (36 kWh stored), giving a desired 11:00 usable balance of 6.5 kWh.
This is an estimate, not a guarantee of 40 kWh delivered or stored. Higher house
reserves take precedence: reserve the greater of historical house demand to
11:00 and current house demand sustained to 11:00, plus 2 kWh. Future sunlight
does not reduce that reserve. Low-SOC Tesla replenishment can use energy above
the home reserve without keeping the additional charging-space target balance.

## Power limits and import protection

The power budget includes actual charging power that can be redirected from the
home batteries, actual grid export, and a bounded Solplanet discharge allowance
funded by energy above the reserve. It subtracts existing discharge that exceeds
that allowance. SAJ SOC never provides an assumed discharge capability.
Solplanet's live Pout limit bounds extra discharge (also capped at 3 kW extra
and 8 kW inverter output); unavailable/zero Pout permits no extra discharge.
Unknown/stale required telemetry stops flexible loads. There is a 300 W power
margin, Tesla starts at 6 A and rises at most 2 A each collection cycle, and new
Tesla/pump starts cannot share the same pre-command power reading. A car command
must be reflected in a later meter sample before starting the pump.
When stopped, Tesla must report the requested current on a later collector
round before the start command; accepting a current-write request is not enough.

Fresh positive grid power sheds both automatic flexible loads before the SAJ
power guard runs. Each stop is attempted even if the other fails. The cooldown
is persisted before a stop, lasts five minutes, and retries failed stops. Three
distinct clean meter samples are required before starting again; observations
of the same sample do not satisfy that gate. Background import with both loads
already off does not continually extend the cooldown. The old pre-08:00 date
latch converts to this cooldown on entering the new window. At 11:00 normal
free-energy rules take over. Device/HA latency and the collector interval mean
the controller cannot guarantee zero instantaneous grid import.

The policy does not change the evening sale schedule or promise daily bill
neutrality. Export revenue depends on tariff windows, actual net exports,
house/vehicle consumption, solar production and charging losses.

## Verification

`tests/test_pre_free_energy.py` covers battery-charging surplus without grid
export, insufficient reserves, depleted Solplanet with high SAJ SOC, live output
limits, 80% Tesla cutoff, low-SOC priority, shared power allocation, ramping,
window boundaries, operation-worker routing, pump-off conflicts, repeated
samples, stale telemetry and independent stop attempts after a device failure.

Read `systems.combined.last_saj_battery_watch_check.pre_free_energy` and
`systems.combined.last_pool_pump_overnight_check` from `/api/collector/status`
during the window, or `details.pre_free_energy` in operation history.
