from __future__ import annotations

import csv
import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.persistence import (
    DAILY_COST_ZEROHERO_IMPORT_THRESHOLD_KWH,
    DAILY_COST_ZEROHERO_WINDOW_MINUTES,
    _plan_rate_cap_kwh,
    _plan_rate_value,
    _plan_rate_window,
    _round_aud,
    _time_in_window,
    _window_duration_seconds,
    select_billed_electricity_plan,
)

METER_USAGE_INDEX_FILENAME = "usage-index.json"
RAW_USAGE_DIRNAME = "raw"


@dataclass(frozen=True)
class MeterInterval:
    start_time: time
    duration_minutes: int


def _decode_csv(raw: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _parse_meter_date(value: str) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _parse_meter_time(value: str) -> time | None:
    raw = str(value or "").strip()
    match = re.fullmatch(r"(\d{1,2}):(\d{2})", raw)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2))
    if hour > 23 or minute > 59:
        return None
    return time(hour=hour, minute=minute)


def _time_minutes(value: time) -> int:
    return value.hour * 60 + value.minute


def _build_intervals(headers: list[str]) -> list[tuple[int, MeterInterval]]:
    parsed: list[tuple[int, time]] = []
    for index, header in enumerate(headers):
        parsed_time = _parse_meter_time(header)
        if parsed_time is not None:
            parsed.append((index, parsed_time))
    intervals: list[tuple[int, MeterInterval]] = []
    for item_index, (column_index, start_time) in enumerate(parsed):
        start_minutes = _time_minutes(start_time)
        if item_index + 1 < len(parsed):
            end_minutes = _time_minutes(parsed[item_index + 1][1])
        else:
            end_minutes = 24 * 60
        if end_minutes <= start_minutes:
            end_minutes += 24 * 60
        intervals.append((column_index, MeterInterval(start_time=start_time, duration_minutes=end_minutes - start_minutes)))
    return intervals


def _stream_kind(row: list[str]) -> str | None:
    text = " ".join(str(part or "") for part in row).lower()
    codes = {str(part or "").strip().upper() for part in row}
    if "consumption" in text or "import" in text or "E1" in codes:
        return "import"
    if "solar" in text or "generation" in text or "export" in text or "B1" in codes:
        return "export"
    return None


def _stream_id(row: list[str]) -> str:
    for value in row[2:]:
        cleaned = str(value or "").strip()
        if cleaned:
            return cleaned
    return ""


def _number(value: object) -> float | None:
    try:
        number = float(str(value or "").strip())
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def parse_meter_usage_upload(raw: bytes, *, filename: str) -> dict[str, Any]:
    text = _decode_csv(raw)
    rows = list(csv.reader(text.splitlines()))
    days: dict[str, dict[str, Any]] = {}
    streams: list[dict[str, Any]] = []
    nmi = ""
    network = ""
    cursor = 0
    while cursor < len(rows):
        row = rows[cursor]
        label = str(row[0] if row else "").strip().lower()
        if label == "nmi":
            nmi = str(row[1] if len(row) > 1 else "").strip()
            network = str(row[3] if len(row) > 3 else "").strip()
            cursor += 1
            continue
        if label != "stream id":
            cursor += 1
            continue

        stream_row = row
        kind = _stream_kind(stream_row)
        stream = _stream_id(stream_row)
        unit = str(stream_row[4] if len(stream_row) > 4 else "").strip()
        source_label = str(stream_row[5] if len(stream_row) > 5 else stream).strip()
        timezone_label = ""
        if cursor + 1 < len(rows) and str(rows[cursor + 1][0] if rows[cursor + 1] else "").strip().lower() == "local time":
            timezone_label = str(rows[cursor + 1][1] if len(rows[cursor + 1]) > 1 else "").strip()
            cursor += 1
        if cursor + 1 >= len(rows) or str(rows[cursor + 1][0] if rows[cursor + 1] else "").strip().lower() != "date/time":
            cursor += 1
            continue

        header = rows[cursor + 1]
        intervals = _build_intervals(header)
        if kind in {"import", "export"}:
            streams.append({
                "stream": stream,
                "kind": kind,
                "unit": unit or "KWH",
                "label": source_label,
                "timezone": timezone_label,
                "interval_count": len(intervals),
            })
        cursor += 2
        while cursor < len(rows):
            data_row = rows[cursor]
            first = str(data_row[0] if data_row else "").strip()
            lower_first = first.lower()
            if lower_first in {"stream id", "nmi"}:
                break
            if lower_first == "total for period" or not first:
                cursor += 1
                continue
            day = _parse_meter_date(first)
            if day is None:
                cursor += 1
                continue
            day_key = day.isoformat()
            day_payload = days.setdefault(
                day_key,
                {
                    "date": day_key,
                    "quality": {},
                    "totals": {"import_kwh": 0.0, "export_kwh": 0.0},
                    "intervals": {},
                },
            )
            interval_map = day_payload["intervals"]
            measured_total = _number(data_row[-1] if data_row else None)
            quality = str(data_row[-2] if len(data_row) >= 2 else "").strip()
            if kind:
                day_payload["quality"][kind] = quality
                if measured_total is not None:
                    day_payload["totals"][f"{kind}_meter_total_kwh"] = round(measured_total, 4)
            running_total = 0.0
            for column_index, interval in intervals:
                value = _number(data_row[column_index] if column_index < len(data_row) else None)
                if value is None:
                    continue
                running_total += value
                key = interval.start_time.strftime("%H:%M")
                bucket = interval_map.setdefault(
                    key,
                    {
                        "time": key,
                        "duration_minutes": interval.duration_minutes,
                        "import_kwh": 0.0,
                        "export_kwh": 0.0,
                    },
                )
                if kind == "import":
                    bucket["import_kwh"] = round(float(bucket.get("import_kwh") or 0.0) + value, 6)
                elif kind == "export":
                    bucket["export_kwh"] = round(float(bucket.get("export_kwh") or 0.0) + value, 6)
            if kind:
                day_payload["totals"][f"{kind}_kwh"] = round(float(day_payload["totals"].get(f"{kind}_kwh") or 0.0) + running_total, 4)
            cursor += 1

    if not days:
        raise ValueError("Could not parse daily meter usage rows from the uploaded CSV")
    day_values = sorted(days.values(), key=lambda item: str(item.get("date") or ""))
    for day_payload in day_values:
        intervals = day_payload.get("intervals") if isinstance(day_payload.get("intervals"), dict) else {}
        day_payload["intervals"] = [intervals[key] for key in sorted(intervals)]
        day_payload["interval_count"] = len(day_payload["intervals"])
    return {
        "source_filename": filename,
        "format": "aemo_meter_data_wide_v1",
        "nmi": nmi,
        "network": network,
        "streams": streams,
        "period": {
            "start": str(day_values[0].get("date")),
            "end": str(day_values[-1].get("date")),
            "days": len(day_values),
        },
        "days": day_values,
    }


def _local_dt(day: date, value: time, timezone_name: str | None) -> datetime:
    try:
        tz = ZoneInfo(timezone_name or "Australia/Sydney")
    except ZoneInfoNotFoundError:
        tz = UTC
    return datetime.combine(day, value, tzinfo=tz)


def _usage_cost_variant(
    *,
    fixed_aud: float,
    import_aud: float,
    fit_aud: float,
    super_export_aud: float,
    zerohero_aud: float,
    over_cap_aud: float,
) -> dict[str, Any]:
    total_aud = fixed_aud + import_aud + fit_aud + super_export_aud + zerohero_aud
    return {
        "total_aud": _round_aud(total_aud),
        "line_items": {
            "fixed_aud": _round_aud(fixed_aud),
            "import_aud": _round_aud(import_aud),
            "solar_feed_in_aud": _round_aud(fit_aud),
            "super_export_aud": _round_aud(super_export_aud),
            "zerohero_aud": _round_aud(zerohero_aud),
            "other_aud": 0.0,
            "daily_cap_overage_aud": _round_aud(over_cap_aud),
        },
    }


def compute_meter_usage_actuals(
    usage: dict[str, Any],
    *,
    plan_payload: dict[str, Any],
    timezone_name: str | None,
    source_label: str,
) -> dict[str, dict[str, Any]]:
    actuals: dict[str, dict[str, Any]] = {}
    for day_payload in usage.get("days") if isinstance(usage.get("days"), list) else []:
        if not isinstance(day_payload, dict):
            continue
        day = _parse_meter_date(str(day_payload.get("date") or ""))
        if day is None:
            continue
        plan = select_billed_electricity_plan(plan_payload, day) or {}
        rates = plan.get("rates") if isinstance(plan.get("rates"), dict) else {}
        daily_rate = _plan_rate_value(rates, "daily_charge_aud_per_day")
        metering_rate = _plan_rate_value(rates, "metering_charge_aud_per_day")
        peak_rate = _plan_rate_value(rates, "peak_usage_aud_per_kwh")
        free_rate = _plan_rate_value(rates, "off_peak_first_50_kwh_per_day_aud_per_kwh")
        off_peak_balance_rate = _plan_rate_value(rates, "off_peak_balance_aud_per_kwh", free_rate)
        shoulder_rate = _plan_rate_value(rates, "shoulder_usage_aud_per_kwh")
        fit_evening_rate = _plan_rate_value(rates, "solar_feed_in_4pm_11pm_aud_per_kwh")
        fit_other_rate = _plan_rate_value(rates, "solar_feed_in_11pm_4pm_aud_per_kwh")
        super_export_rate = _plan_rate_value(rates, "super_export_top_up_aud_per_kwh")
        zerohero_rate = _plan_rate_value(rates, "zerohero_credit_aud_per_qualified_day")

        free_import_window = _plan_rate_window(rates, "off_peak_first_50_kwh_per_day_aud_per_kwh", (time(hour=11), time(hour=14)))
        peak_import_window = _plan_rate_window(rates, "peak_usage_aud_per_kwh", (time(hour=16), time(hour=23)))
        fit_evening_window = _plan_rate_window(rates, "solar_feed_in_4pm_11pm_aud_per_kwh", (time(hour=16), time(hour=21)))
        super_export_window = _plan_rate_window(rates, "super_export_top_up_aud_per_kwh", (time(hour=18), time(hour=20)))
        zerohero_window = _plan_rate_window(rates, "zerohero_credit_aud_per_qualified_day", super_export_window)

        free_import_kwh = 0.0
        peak_import_kwh = 0.0
        shoulder_import_kwh = 0.0
        fit_evening_export_kwh = 0.0
        fit_other_export_kwh = 0.0
        super_export_window_kwh = 0.0
        zerohero_import_kwh = 0.0
        zerohero_observed_minutes = 0.0
        zerohero_net_import_windows = 0
        zerohero_mixed_export_windows = 0
        zerohero_net_import_kwh = 0.0

        intervals = day_payload.get("intervals") if isinstance(day_payload.get("intervals"), list) else []
        for raw_interval in intervals:
            if not isinstance(raw_interval, dict):
                continue
            interval_time = _parse_meter_time(str(raw_interval.get("time") or ""))
            if interval_time is None:
                continue
            duration_minutes = int(_number(raw_interval.get("duration_minutes")) or 5)
            local_start = _local_dt(day, interval_time, timezone_name)
            import_kwh = max(0.0, float(_number(raw_interval.get("import_kwh")) or 0.0))
            export_kwh = max(0.0, float(_number(raw_interval.get("export_kwh")) or 0.0))
            if _time_in_window(local_start, free_import_window):
                free_import_kwh += import_kwh
            elif _time_in_window(local_start, peak_import_window):
                peak_import_kwh += import_kwh
            else:
                shoulder_import_kwh += import_kwh
            if _time_in_window(local_start, fit_evening_window):
                fit_evening_export_kwh += export_kwh
            else:
                fit_other_export_kwh += export_kwh
            if _time_in_window(local_start, super_export_window):
                super_export_window_kwh += export_kwh
            if _time_in_window(local_start, zerohero_window):
                zerohero_observed_minutes += duration_minutes
                zerohero_import_kwh += import_kwh
                net_import_kwh = import_kwh - export_kwh
                has_import = import_kwh > DAILY_COST_ZEROHERO_IMPORT_THRESHOLD_KWH
                if has_import and net_import_kwh > DAILY_COST_ZEROHERO_IMPORT_THRESHOLD_KWH:
                    zerohero_net_import_windows += 1
                    zerohero_net_import_kwh += net_import_kwh
                elif has_import:
                    zerohero_mixed_export_windows += 1

        free_cap = _plan_rate_cap_kwh(rates, "off_peak_first_50_kwh_per_day_aud_per_kwh")
        free_billable_kwh = min(free_import_kwh, free_cap) if free_cap is not None else free_import_kwh
        off_peak_balance_kwh = max(0.0, free_import_kwh - free_billable_kwh)
        import_aud_daily_cap = (
            free_billable_kwh * free_rate
            + off_peak_balance_kwh * off_peak_balance_rate
            + peak_import_kwh * peak_rate
            + shoulder_import_kwh * shoulder_rate
        )
        over_cap_aud = off_peak_balance_kwh * off_peak_balance_rate
        import_aud_no_daily_cap = import_aud_daily_cap - over_cap_aud
        super_cap = _plan_rate_cap_kwh(rates, "super_export_top_up_aud_per_kwh")
        super_billable_kwh = min(super_export_window_kwh, super_cap) if super_cap is not None else super_export_window_kwh
        fit_aud = -(fit_evening_export_kwh * fit_evening_rate + fit_other_export_kwh * fit_other_rate)
        super_export_aud = -(super_billable_kwh * super_export_rate)
        zerohero_expected_minutes = _window_duration_seconds(zerohero_window) / 60.0
        zerohero_coverage = zerohero_observed_minutes / zerohero_expected_minutes if zerohero_expected_minutes > 0 else None
        has_zerohero_data = zerohero_coverage is not None and zerohero_coverage >= 0.95
        zerohero_qualified = zerohero_rate > 0 and has_zerohero_data and zerohero_net_import_windows == 0
        zerohero_aud = -zerohero_rate if zerohero_qualified else 0.0
        fixed_aud = daily_rate + metering_rate
        daily_cap_variant = _usage_cost_variant(
            fixed_aud=fixed_aud,
            import_aud=import_aud_daily_cap,
            fit_aud=fit_aud,
            super_export_aud=super_export_aud,
            zerohero_aud=zerohero_aud,
            over_cap_aud=over_cap_aud,
        )
        no_daily_cap_variant = _usage_cost_variant(
            fixed_aud=fixed_aud,
            import_aud=import_aud_no_daily_cap,
            fit_aud=fit_aud,
            super_export_aud=super_export_aud,
            zerohero_aud=zerohero_aud,
            over_cap_aud=0.0,
        )
        actuals[day.isoformat()] = {
            "status": "meter_usage_calculated",
            "source": "uploaded_meter_usage",
            "invoice_number": source_label,
            "source_filename": usage.get("source_filename") or "",
            "day": day.isoformat(),
            "currency": "AUD",
            "actual_daily_cost_aud": no_daily_cap_variant["total_aud"],
            "interval_count": len(intervals),
            "estimated_daily_cost_aud": None,
            "delta_aud": None,
            "allocation_method": "meter_interval_kwh_by_plan_rate_no_daily_50_overage_charge",
            "line_items": {
                "fixed_aud": _round_aud(fixed_aud),
                "import_aud": _round_aud(import_aud_no_daily_cap),
                "solar_feed_in_aud": _round_aud(fit_aud),
                "super_export_aud": _round_aud(super_export_aud),
                "zerohero_aud": _round_aud(zerohero_aud),
                "other_aud": 0.0,
                "daily_cap_overage_aud": 0.0,
            },
            "pricing_variants": {
                "daily_cap": daily_cap_variant,
                "no_daily_cap": no_daily_cap_variant,
            },
            "usage_kwh": {
                "import_total": round(free_import_kwh + peak_import_kwh + shoulder_import_kwh, 4),
                "export_total": round(fit_evening_export_kwh + fit_other_export_kwh, 4),
                "free_import": round(free_import_kwh, 4),
                "free_import_billable": round(free_billable_kwh, 4),
                "off_peak_balance": round(off_peak_balance_kwh, 4),
                "peak_import": round(peak_import_kwh, 4),
                "shoulder_import": round(shoulder_import_kwh, 4),
                "fit_evening_export": round(fit_evening_export_kwh, 4),
                "fit_other_export": round(fit_other_export_kwh, 4),
                "super_export_window": round(super_export_window_kwh, 4),
                "super_export_billable": round(super_billable_kwh, 4),
            },
            "zerohero": {
                "qualified": zerohero_qualified,
                "coverage_ratio": round(zerohero_coverage, 4) if zerohero_coverage is not None else None,
                "import_kwh": round(zerohero_import_kwh, 5),
                "net_import_kwh": round(zerohero_net_import_kwh, 5),
                "net_import_windows": zerohero_net_import_windows,
                "mixed_export_windows": zerohero_mixed_export_windows,
                "window_minutes": DAILY_COST_ZEROHERO_WINDOW_MINUTES,
            },
            "plan": {
                "id": plan.get("id") or "",
                "display_name": plan.get("display_name") or plan.get("name") or "",
                "retailer": plan.get("retailer") or "",
                "price_basis": plan.get("price_basis") or "",
            },
        }
    return actuals


def load_meter_usage_index(usage_dir: Path) -> dict[str, Any]:
    path = usage_dir / METER_USAGE_INDEX_FILENAME
    if not path.exists():
        return {"source_note": "Uploaded meter usage CSV files parsed by Wattimize.", "uploads": [], "latest_by_day": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"source_note": "Uploaded meter usage CSV files parsed by Wattimize.", "uploads": [], "latest_by_day": {}}
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("source_note", "Uploaded meter usage CSV files parsed by Wattimize.")
    payload.setdefault("uploads", [])
    payload.setdefault("latest_by_day", {})
    return payload


def _meter_usage_stream_by_kind(usage: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    streams = usage.get("streams") if isinstance(usage.get("streams"), list) else []
    for stream in streams:
        if not isinstance(stream, dict):
            continue
        kind = str(stream.get("kind") or "").strip()
        if kind:
            result[kind] = stream
    return result


def upsert_meter_usage_upload_to_db(
    db_path: Path | str,
    *,
    usage: dict[str, Any],
    raw: bytes,
    source_filename: str,
    source_csv: str,
) -> dict[str, Any]:
    upload_id = hashlib.sha256(raw).hexdigest()[:16]
    content_sha256 = hashlib.sha256(raw).hexdigest()
    period = usage.get("period") if isinstance(usage.get("period"), dict) else {}
    days = usage.get("days") if isinstance(usage.get("days"), list) else []
    streams_by_kind = _meter_usage_stream_by_kind(usage)
    imported_at = datetime.now(UTC).isoformat()
    interval_rows: list[tuple[Any, ...]] = []
    for day_payload in days:
        if not isinstance(day_payload, dict):
            continue
        day_text = str(day_payload.get("date") or "")
        quality = day_payload.get("quality") if isinstance(day_payload.get("quality"), dict) else {}
        totals = day_payload.get("totals") if isinstance(day_payload.get("totals"), dict) else {}
        intervals = day_payload.get("intervals") if isinstance(day_payload.get("intervals"), list) else []
        for interval in intervals:
            if not isinstance(interval, dict):
                continue
            interval_time = str(interval.get("time") or "")
            duration_minutes = int(_number(interval.get("duration_minutes")) or 5)
            for kind, value_key in (("export", "export_kwh"), ("import", "import_kwh")):
                value = _number(interval.get(value_key))
                if value is None:
                    continue
                stream = streams_by_kind.get(kind, {})
                interval_rows.append(
                    (
                        upload_id,
                        source_filename,
                        str(usage.get("nmi") or ""),
                        str(usage.get("network") or ""),
                        str(stream.get("stream") or kind),
                        kind,
                        str(stream.get("unit") or "KWH"),
                        str(stream.get("label") or kind),
                        str(stream.get("timezone") or ""),
                        day_text,
                        interval_time,
                        duration_minutes,
                        float(value),
                        str(quality.get(kind) or ""),
                        _number(totals.get(f"{kind}_meter_total_kwh") or totals.get(f"{kind}_kwh")),
                        imported_at,
                    )
                )

    with sqlite3.connect(str(db_path), timeout=30.0) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            """
            INSERT OR REPLACE INTO meter_usage_uploads (
                upload_id, source_filename, source_csv, uploaded_at_utc, content_sha256,
                nmi, network, format, period_start, period_end, day_count, interval_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                upload_id,
                source_filename,
                source_csv,
                imported_at,
                content_sha256,
                str(usage.get("nmi") or ""),
                str(usage.get("network") or ""),
                str(usage.get("format") or ""),
                str(period.get("start") or ""),
                str(period.get("end") or ""),
                len(days),
                len(interval_rows),
            ),
        )
        if interval_rows:
            conn.executemany(
                """
                INSERT OR REPLACE INTO meter_usage_intervals (
                    upload_id, source_filename, nmi, network, stream, kind, unit, label, timezone,
                    local_date, interval_start_time, duration_minutes, kwh, quality, day_total_kwh,
                    imported_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                interval_rows,
            )
        conn.commit()
    return {"upload_id": upload_id, "interval_rows_upserted": len(interval_rows)}


def import_existing_meter_usage_csvs_to_db(db_path: Path | str, usage_dir: Path) -> dict[str, Any]:
    raw_dir = usage_dir / RAW_USAGE_DIRNAME
    if not raw_dir.exists():
        return {"uploads_imported": 0, "interval_rows_upserted": 0, "missing_raw_dir": True}
    index = load_meter_usage_index(usage_dir)
    source_names: dict[str, str] = {}
    for upload in index.get("uploads", []) if isinstance(index.get("uploads"), list) else []:
        if not isinstance(upload, dict):
            continue
        source_csv = str(upload.get("source_csv") or "")
        source_filename = str(upload.get("source_filename") or "")
        if source_csv and source_filename:
            source_names[source_csv] = source_filename

    imported = 0
    interval_rows = 0
    for raw_path in sorted(raw_dir.glob("*.csv")):
        try:
            raw = raw_path.read_bytes()
            relative = str(raw_path.relative_to(usage_dir))
            filename = source_names.get(relative) or raw_path.name
            usage = parse_meter_usage_upload(raw, filename=filename)
            result = upsert_meter_usage_upload_to_db(
                db_path,
                usage=usage,
                raw=raw,
                source_filename=filename,
                source_csv=relative,
            )
        except (OSError, ValueError, sqlite3.Error):
            continue
        imported += 1
        interval_rows += int(result.get("interval_rows_upserted") or 0)
    return {"uploads_imported": imported, "interval_rows_upserted": interval_rows, "missing_raw_dir": False}


def _clean_file_stem(value: object) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "").strip()).strip("-._")
    return cleaned or "meter-usage"


def save_meter_usage_artifacts(
    usage_dir: Path,
    *,
    raw: bytes,
    filename: str,
    usage: dict[str, Any],
    actuals_by_day: dict[str, dict[str, Any]],
    db_path: Path | str | None = None,
) -> dict[str, Any]:
    usage_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = usage_dir / RAW_USAGE_DIRNAME
    raw_dir.mkdir(parents=True, exist_ok=True)
    upload_hash = hashlib.sha256(raw).hexdigest()[:16]
    period = usage.get("period") if isinstance(usage.get("period"), dict) else {}
    stem = "-".join(
        part for part in (
            _clean_file_stem(period.get("start")),
            _clean_file_stem(period.get("end")),
            upload_hash,
        )
        if part
    )
    raw_path = raw_dir / f"{stem}.csv"
    raw_path.write_bytes(raw)
    db_import: dict[str, Any] | None = None
    if db_path is not None:
        db_import = upsert_meter_usage_upload_to_db(
            db_path,
            usage=usage,
            raw=raw,
            source_filename=filename,
            source_csv=str(raw_path.relative_to(usage_dir)),
        )
    days = usage.get("days") if isinstance(usage.get("days"), list) else []
    actual_values = list(actuals_by_day.values())
    total_actual = sum(float(item.get("actual_daily_cost_aud") or 0.0) for item in actual_values)
    record = {
        "upload_id": upload_hash,
        "source_filename": filename,
        "uploaded_at": datetime.now(UTC).isoformat(),
        "source_csv": str(raw_path.relative_to(usage_dir)),
        "format": usage.get("format") or "",
        "nmi": usage.get("nmi") or "",
        "network": usage.get("network") or "",
        "streams": usage.get("streams") if isinstance(usage.get("streams"), list) else [],
        "period": period,
        "day_count": len(days),
        "actual_day_count": len(actuals_by_day),
        "total_actual_cost_aud": _round_aud(total_actual),
        "db_import": db_import or {},
        "days": [
            {
                "date": str(day.get("date") or ""),
                "quality": day.get("quality") if isinstance(day.get("quality"), dict) else {},
                "totals": day.get("totals") if isinstance(day.get("totals"), dict) else {},
                "interval_count": day.get("interval_count") or 0,
                "actual_cost": actuals_by_day.get(str(day.get("date") or ""), {}),
            }
            for day in days
        ],
    }
    index = load_meter_usage_index(usage_dir)
    uploads = [item for item in index.get("uploads", []) if isinstance(item, dict) and item.get("upload_id") != upload_hash]
    uploads.append(record)
    uploads.sort(key=lambda item: str((item.get("period") or {}).get("start") or ""))
    latest_by_day = index.get("latest_by_day") if isinstance(index.get("latest_by_day"), dict) else {}
    for day_key, actual in actuals_by_day.items():
        latest_by_day[day_key] = {"upload_id": upload_hash, "actual_cost": actual}
    index["uploads"] = uploads
    index["latest_by_day"] = latest_by_day
    (usage_dir / METER_USAGE_INDEX_FILENAME).write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")
    return record
