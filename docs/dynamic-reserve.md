# Dynamic overnight export reserve

Dashboard has one **Use automatic forecast** checkbox and two values: the saved manual reserve (editable only in manual mode), and a read-only forecast. Default is manual; producing predictions never enables automatic control. Checkbox changes persist immediately, and manual edits use Save (or the existing mobile change handler).

All times use the configured site timezone. At startup a preview is calculated, then every 30 minutes from 15:00 through 22:30. The target is usable energy remaining at 23:00, covering the night until next-day 11:00. Existing export pacing and SAJ priority remain in charge of inverter power; this feature supplies their effective reserve. Forecast work runs in a separate task so weather/network/history reads cannot block collection. Downward same-night revisions are limited to 1 kWh per update; upward revisions apply immediately.

## Deterministic model v1

No AI, prompts, or model service. Use up to 28 completed local nights, excluding missing data and DST-transition training nights. Require at least 7 valid nights, >=97% temporal coverage and >=90% coverage in each half-hour. Integration bridges at most 150 seconds; absent samples are not treated as zero load.

Replay each night's household load shape (including water heating, breakfast and measured pump operation), subtract historical measured Tesla load, and add a front-loaded urgent Tesla allowance based on current SOC and the configured force-charge threshold (75 kWh vehicle, 90% charging efficiency). Unknown SOC disables automatic recommendations. This is a conservative energy allowance, not a change to the Tesla charge controller. Historical discretionary pump patterns are retained, not independently identified/predicted.

PV uses the per-half-hour 90th percentile of observed generation as an empirical site envelope. Tomorrow's hourly shortwave radiation attenuates that envelope against a simple solar-position clear-sky reference, then a 0.65 uncertainty multiplier is applied. Pre-06:00 and post-23:00 generation are zero. This is a conservative heuristic, not a calibrated probabilistic PV forecast. Open-Meteo radiation timestamps represent the preceding hour. The existing configured weather coordinates are preserved and stored with each forecast; site-specific coordinates can improve regional weather accuracy.

Compute each scenario's largest cumulative deficit, using 90% charge/discharge efficiency. Retain the larger of the 28-night 85th percentile and latest-7-night 90th percentile, add 2 kWh plus up to 4 kWh for elevated last-hour household load, and round upward to 0.5 kWh. These percentiles are empirical settings, NOT claims of 85/90/95% reliability. Keeping older high-load nights helps avoid learning a week away as permanent low demand, but an unobserved return-home load increase remains a risk.

## Fallback and limits

Only a ready forecast for the current local date, generated within 90 minutes, can supply automatic control. Otherwise use the greater of manual reserve and any same-night recommendation. Recommendations above the combined 57 kWh usable capacity are marked infeasible, not clipped down. Live low Tesla SOC adds any urgent allowance not already budgeted. Existing device floors, power limits, SAJ priority, and Tesla/pump safety rules are retained. Manual setting is never overwritten by forecasts.

## Persistence and validation

`reserve_forecasts` stores the mode, every immutable prediction version (including the actual forecast available then, model version, inputs and training dates), and completed-night observations. Persistence shares the existing NAS database and survives restarts. No database replacement or historical backfilled prediction is used.

`GET /api/reserve-forecast` returns current mode/values, daily results and statistics. `PUT /api/reserve-forecast` accepts only `{automatic, manual_kwh}`; it updates the reserve without replacing device clients. The checkbox defaults off.

At the next 11:00 boundary, replay measured total load and PV to estimate the actual required reserve. Daily accuracy uses one final ready prediction issued before 23:00, so repeated revisions cannot inflate the denominator. Report mean absolute error of the median need prediction and the fraction of valid nights whose recommendation covered the observed requirement, plus reserve margin. Incomplete nights are excluded. Observed-load replay includes the actual controller's discretionary loads: it is not an exact counterfactual simulation of a different reserve, a backup-power guarantee, or verified net savings.

First outcomes appear after the first predicted night completes. Start in manual observation mode and compare at least the away and occupied periods. No promise of one-week statistical reliability is made. Tariff/free-window allowance ambiguity is not resolved by this feature; it does not report hypothetical export as realized profit.
