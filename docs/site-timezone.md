# Site time and daylight saving

The dashboard uses the backend's effective IANA site timezone, normally
`Australia/Sydney`, independently of the viewing browser's timezone. The header
shows the site time and zone. Calendar ranges, charts, operation timestamps,
weather timestamps, and time-window indicators all use that same zone.

`WATTIMIZE_LOCAL_TIMEZONE` sets the default; persisted `local_timezone` takes
precedence. Missing/invalid zones fall back to Sydney rather than a host offset.
Host `TZ` no longer supplies the application default. `/api/config/status`
includes `local_timezone` and `now_local` for diagnosis.

The frontend uses vendored `@date-fns/tz` 1.5.0 (MIT) through `SiteDate` in
`app/static/site-time.js`. It does not replace the browser's global Date class.
UTC ISO serialization is retained for API queries and persisted values.
Offset-free date/time inputs represent site wall time. DST gaps/overlaps use
the library's compatible calendar disambiguation; explicit-offset timestamps
retain their exact instant. Day/week/month endpoints are constructed separately
so DST days have 23 or 25 hours. Minute/hour relative ranges are elapsed time.

Backend schedules retain their local clock boundaries (including 11:00–15:00).
Elapsed overnight forecast duration, pump energy budgets and rolling 24-hour
runtime lookbacks use UTC arithmetic. Daily report boundaries already use UTC
conversion of consecutive local midnights.

Validation:

```sh
TZ=Asia/Shanghai node --test tests/test_site_time.cjs
TZ=America/Los_Angeles node --test tests/test_site_time.cjs
WATTIMIZE_DB_PATH=/tmp/wattimize-timezone-tests.sqlite3 .venv/bin/python -m pytest -q tests
```

The browser dependency can be rebuilt with esbuild 0.25.12 from the official
npm `@date-fns/tz@1.5.0` package's `date/index.js`, using
`--bundle --format=iife --global-name=WattimizeTZ --minify`; retain the adjacent
MIT license. No network dependency is needed at page load for this library.

## Remote deployment — 2026-10-09

Deployed through `nas.leocai.me` TOS File Manager and Terminal because LAN SSH
was unavailable. A code-only archive was built on NAS over the existing image
as `wattimize:timezone-20261009` (also tagged `wattimize:amd64-latest`).
The existing `/Volume1/public/wattimize-data:/app/data` mount and environment
were retained, with container `TZ=Australia/Sydney`. No database or bill upload
was performed. The stopped `wattimize-before-timezone-20261009` container remains
available for rollback; stop the new writer before restarting the old container.

Public health and collector checks passed after startup. At 11:19 AEDT the live
worker reported `Australia/Sydney`, `free_energy`, and the 11:00–15:00 window;
SAJ, Solplanet and Tesla were charging. Browser verification showed the same
site clock and active window, with no captured console errors. Backend tests:
167 passed; frontend timezone tests passed under Shanghai, Los Angeles and
Sydney viewer timezones. Future DST transitions were simulated in tests, not
by changing the live clock or device clocks.
