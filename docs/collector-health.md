# Collector health and automatic recovery

`GET /api/collector/status` includes `health_summary` and `watchdog` alongside
per-system sampling state. Health issues include missing metrics, stale samples,
request failures and auxiliary control failures. Localized English and Chinese
messages are shown in the desktop flow status and mobile freshness indicator.

The watchdog checks once per minute. A finished collector task can trigger
recovery; Solplanet or combined sampling stale for at least ten minutes can
trigger recovery when its failure reason is one of the recognized recoverable
source, timeout, incomplete-flow or backoff conditions.

Recovery recreates the Solplanet client and restarts collection under the runtime
lock, then closes the replaced client after releasing that lock. Recovery uses a
15-minute cooldown and at most three successful recoveries per rolling hour.
The in-memory watchdog state exposes last check, recovery, skip reason and recent
history. Automatic recovery is recorded as
`backend.auto.collector_watchdog_recover` in operation history.

A successful restart means collection was restarted, not that the next sample
has succeeded. Verify per-system `last_success_at` and `last_round_result` on the
NAS after recovery. The manual collector restart endpoint remains available.
