# Collector health and automatic recovery

`GET /api/collector/status` includes `health_summary` and `watchdog` alongside
per-system sampling state. Health issues include missing metrics, stale samples,
request failures and auxiliary control failures. Localized English and Chinese
details remain available in the status API. The desktop flow status and mobile
freshness indicator show only affected service names (SAJ, Solplanet, Tesla, or
pool pump), deduplicated. Combined failures caused by a source outage do not add
another label; missing combined SAJ SOC during a source outage does not label
SAJ as faulty. Unattributed failures display a short data-service error.

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

Normal Tesla unplugged/disconnected skips do not mark collection unhealthy.
Unknown/unavailable observations and actual control errors remain issues,
including nested SAJ/Solplanet free-energy control failures.

Tesla plug reminders are informational dashboard notifications, not health errors.
They require the Sydney free-energy window (11:00 inclusive to 15:00 exclusive),
confirmed home location, known SOC below 80%, and an explicitly disconnected cable.
Unknown observations do not qualify. A persisted local-date marker limits the
reminder to once per day across worker restarts and manual dismissals. The active
reminder clears when the conditions no longer hold. It is independent of automatic
charging being enabled, and does not start charging or send external messages.
