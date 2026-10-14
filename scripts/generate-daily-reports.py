#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import get_config_path, load_settings
from app.persistence import (
    apply_daily_report_soc_estimates,
    build_daily_report_phase_battery,
    build_daily_report_soc_estimates,
    compute_daily_cost_between,
    compute_usage_between,
    get_series_samples,
    init_db,
    list_zero_hero_window_history,
    save_daily_report,
    select_billed_electricity_plan,
)

SYSTEMS = ("saj", "solplanet", "combined")
FREE_ENERGY_WINDOW_START_HOUR = 11
FREE_ENERGY_WINDOW_END_HOUR = 15
EXPORT_WINDOW_START_HOUR = 16
EXPORT_WINDOW_END_HOUR = 23
ZERO_HERO_WINDOW_MINUTES = 5
SERIES_MAX_POINTS = 500
ELECTRICITY_PLANS_PATH = ROOT / "data" / "home-plan" / "electricity-plans.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate persisted Wattimize daily reports from raw History snapshots.")
    parser.add_argument("--db", type=Path, default=get_config_path(), help="SQLite DB path. Defaults to WATTIMIZE_DB_PATH or data/wattimize.sqlite3.")
    parser.add_argument("--start", type=date.fromisoformat, default=None, help="Local start day, YYYY-MM-DD. Defaults to earliest raw day.")
    parser.add_argument("--end", type=date.fromisoformat, default=None, help="Local end day, YYYY-MM-DD. Defaults to today, clamped to yesterday unless --include-today.")
    parser.add_argument("--include-today", action="store_true", help="Allow generating the current local day even though it is incomplete.")
    parser.add_argument("--force", action="store_true", help="Regenerate reports even when an existing report is valid.")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan without writing reports.")
    return parser.parse_args()


def sqlite_uri(db_path: Path) -> str:
    return f"{db_path.resolve().as_uri()}?mode=ro"


def resolve_local_timezone() -> tuple[object, str]:
    settings = load_settings()
    configured = str(getattr(settings, "local_timezone", "") or "").strip()
    if configured:
        try:
            return ZoneInfo(configured), configured
        except ZoneInfoNotFoundError:
            print(f"Warning: invalid configured local timezone {configured!r}; falling back to Australia/Sydney.", file=sys.stderr)
    return ZoneInfo("Australia/Sydney"), "Australia/Sydney"


def sample_interval_for_system(system: str) -> float:
    settings = load_settings()
    saj_interval = float(settings.saj_sample_interval_seconds)
    solplanet_interval = float(settings.solplanet_sample_interval_seconds)
    if system == "combined":
        return max(saj_interval, solplanet_interval)
    if system == "solplanet":
        return solplanet_interval
    return saj_interval


def load_electricity_plan_payload() -> dict[str, object]:
    return json.loads(ELECTRICITY_PLANS_PATH.read_text(encoding="utf-8"))


def local_period(day: date, local_tz: object) -> tuple[datetime, datetime]:
    start_local = datetime.combine(day, time.min, tzinfo=local_tz)
    end_local = datetime.combine(day + timedelta(days=1), time.min, tzinfo=local_tz)
    return start_local.astimezone(UTC), end_local.astimezone(UTC)


def raw_epoch_bounds(db_path: Path) -> tuple[float, float, int] | None:
    with sqlite3.connect(sqlite_uri(db_path), uri=True) as conn:
        conn.execute("PRAGMA busy_timeout=10000")
        row = conn.execute(
            "SELECT min(assembled_at_epoch), max(assembled_at_epoch), count(*) FROM assembled_flow_snapshots"
        ).fetchone()
    if not row or row[0] is None or row[1] is None or int(row[2] or 0) <= 0:
        return None
    return float(row[0]), float(row[1]), int(row[2])


def raw_count_for_period(db_path: Path, start_at_utc: datetime, end_at_utc: datetime) -> int:
    with sqlite3.connect(sqlite_uri(db_path), uri=True) as conn:
        conn.execute("PRAGMA busy_timeout=10000")
        value = conn.execute(
            """
            SELECT count(*)
            FROM assembled_flow_snapshots
            WHERE assembled_at_epoch >= ?
              AND assembled_at_epoch < ?
            """,
            (start_at_utc.timestamp(), end_at_utc.timestamp()),
        ).fetchone()[0]
    return int(value or 0)


def existing_report_state(db_path: Path, day: date) -> str:
    with sqlite3.connect(sqlite_uri(db_path), uri=True) as conn:
        conn.execute("PRAGMA busy_timeout=10000")
        row = conn.execute(
            """
            SELECT usage_json, series_json, zero_hero_json
            FROM daily_reports
            WHERE report_day_local = ?
            """,
            (day.isoformat(),),
        ).fetchone()
    if not row:
        return "missing"
    try:
        usage_json = json.loads(row[0] or "{}")
        json.loads(row[1] or "{}")
        json.loads(row[2] or "{}")
    except json.JSONDecodeError:
        return "invalid"
    if not isinstance(usage_json, dict):
        return "invalid"
    if not isinstance(usage_json.get("_phase_battery"), dict):
        return "missing_phase_battery"
    if not isinstance(usage_json.get("_daily_cost"), dict):
        return "missing_daily_cost"
    return "valid"


def generate_report(db_path: Path, day: date, local_tz: object, timezone_name: str) -> dict[str, object]:
    start_at_utc, end_at_utc = local_period(day, local_tz)
    electricity_plan = select_billed_electricity_plan(load_electricity_plan_payload(), day)
    usage_by_system = {
        system: compute_usage_between(
            db_path,
            system=system,
            start_at_utc=start_at_utc,
            end_at_utc=end_at_utc,
            sample_interval_seconds=sample_interval_for_system(system),
            local_timezone_name=timezone_name,
            grid_import_window_start_hour=FREE_ENERGY_WINDOW_START_HOUR,
            grid_import_window_end_hour=FREE_ENERGY_WINDOW_END_HOUR,
            grid_export_window_start_hour=EXPORT_WINDOW_START_HOUR,
            grid_export_window_end_hour=EXPORT_WINDOW_END_HOUR,
        )
        for system in SYSTEMS
    }
    usage_by_system["_daily_cost"] = compute_daily_cost_between(
        db_path,
        system="combined",
        report_day_local=day,
        start_at_utc=start_at_utc,
        end_at_utc=end_at_utc,
        sample_interval_seconds=sample_interval_for_system("combined"),
        local_timezone_name=timezone_name,
        electricity_plan=electricity_plan,
    )
    series_by_system = {
        system: get_series_samples(
            db_path,
            system=system,
            hours=24,
            max_points=SERIES_MAX_POINTS,
            start_at_utc=start_at_utc,
            end_at_utc=end_at_utc,
        )
        for system in SYSTEMS
    }
    zero_hero_history = list_zero_hero_window_history(
        db_path,
        system="combined",
        days=1,
        sample_interval_seconds=sample_interval_for_system("combined"),
        local_timezone_name=timezone_name,
        recent_day_anchor_local=day,
        now_utc=end_at_utc,
        window_start_hour=EXPORT_WINDOW_START_HOUR,
        window_end_hour=EXPORT_WINDOW_END_HOUR,
        window_minutes=ZERO_HERO_WINDOW_MINUTES,
    )
    phase_battery = build_daily_report_phase_battery(
        db_path,
        report_day_local=day,
        local_timezone_name=timezone_name,
    )
    soc_estimates = build_daily_report_soc_estimates(
        db_path,
        report_day_local=day,
        local_timezone_name=timezone_name,
    )
    usage_by_system = apply_daily_report_soc_estimates(usage_by_system, soc_estimates)
    return save_daily_report(
        db_path,
        report_day_local=day,
        timezone_name=timezone_name,
        period_start_utc=start_at_utc,
        period_end_utc=end_at_utc,
        generated_at_utc=datetime.now(UTC),
        usage_by_system=usage_by_system,
        series_by_system=series_by_system,
        zero_hero_history=zero_hero_history,
        phase_battery=phase_battery,
    )


def iter_days(start: date, end: date):
    cursor = start
    while cursor <= end:
        yield cursor
        cursor += timedelta(days=1)


def main() -> int:
    args = parse_args()
    db_path = args.db
    init_db(db_path)
    local_tz, timezone_name = resolve_local_timezone()
    bounds = raw_epoch_bounds(db_path)
    if not bounds:
        print(f"No raw assembled_flow_snapshots found in {db_path}")
        return 1

    min_epoch, max_epoch, raw_total = bounds
    raw_start_day = datetime.fromtimestamp(min_epoch, UTC).astimezone(local_tz).date()
    raw_end_day = datetime.fromtimestamp(max_epoch, UTC).astimezone(local_tz).date()
    today = datetime.now(local_tz).date()
    max_report_day = today if args.include_today else today - timedelta(days=1)

    start_day = args.start or raw_start_day
    end_day = args.end or today
    if end_day > max_report_day:
        print(f"End day {end_day} is not completed; clamping to {max_report_day}. Use --include-today to allow today.")
        end_day = max_report_day
    if start_day > end_day:
        print(f"Nothing to generate: start {start_day} is after end {end_day}.")
        return 0

    print(f"DB: {db_path}")
    print(f"Timezone: {timezone_name}")
    print(f"Raw range: {raw_start_day} to {raw_end_day} ({raw_total} rows)")
    print(f"Report range: {start_day} to {end_day}")
    if args.dry_run:
        print("Dry run: no reports will be written.")

    generated = 0
    skipped = 0
    failed = 0
    for day in iter_days(start_day, end_day):
        start_at_utc, end_at_utc = local_period(day, local_tz)
        raw_count = raw_count_for_period(db_path, start_at_utc, end_at_utc)
        if raw_count <= 0:
            print(f"{day}: skip, no raw rows")
            skipped += 1
            continue
        state = existing_report_state(db_path, day)
        if state == "valid" and not args.force:
            print(f"{day}: skip, existing report is valid ({raw_count} raw rows)")
            skipped += 1
            continue
        action = "regenerate" if state != "missing" else "generate"
        print(f"{day}: {action} ({state}, {raw_count} raw rows)")
        if args.dry_run:
            continue
        try:
            saved = generate_report(db_path, day, local_tz, timezone_name)
            print(f"{day}: saved report with {saved.get('sample_count')} samples")
            generated += 1
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"{day}: failed: {type(exc).__name__}: {exc}", file=sys.stderr)

    print(f"Done. Generated={generated}, skipped={skipped}, failed={failed}.")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
