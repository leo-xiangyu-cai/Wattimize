#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import random
import shutil
import sqlite3
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import load_settings
from app.persistence import compute_daily_report_cost, select_billed_electricity_plan

DEFAULT_DB_PATH = ROOT / "data" / "wattimize.sqlite3"
DEFAULT_PLANS_PATH = ROOT / "data" / "home-plan" / "electricity-plans.json"
DEFAULT_AUDIT_DIR = ROOT / "data" / "recovery"
FLOAT_TOLERANCE = 0.0001


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit and optionally backfill persisted daily report cost payloads."
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser.add_argument("--plans", type=Path, default=DEFAULT_PLANS_PATH, help="Electricity plans JSON path.")
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR, help="Directory for audit artifacts.")
    parser.add_argument("--start", type=date.fromisoformat, default=None, help="First local report day, YYYY-MM-DD.")
    parser.add_argument("--end", type=date.fromisoformat, default=None, help="Last local report day, YYYY-MM-DD.")
    parser.add_argument("--sample-size", type=int, default=12, help="Number of random report days to show.")
    parser.add_argument("--seed", default=None, help="Random sample seed. Omit to use current UTC time.")
    parser.add_argument("--write", action="store_true", help="Write missing or mismatched _daily_cost payloads.")
    parser.add_argument("--force", action="store_true", help="With --write, rewrite every selected report day.")
    return parser.parse_args()


def utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%d-%H%M%S")


def parse_utc(value: object) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)


def stable_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def load_json_object(value: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def normalize_cost(value: object) -> object:
    if not isinstance(value, dict):
        return value
    normalized = json.loads(stable_json(value))
    normalized.pop("source", None)
    return normalized


def equivalent(left: object, right: object, *, tolerance: float = FLOAT_TOLERANCE) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return abs(float(left) - float(right)) <= tolerance
    if isinstance(left, dict) and isinstance(right, dict):
        if set(left.keys()) != set(right.keys()):
            return False
        return all(equivalent(left[key], right[key], tolerance=tolerance) for key in left)
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return False
        return all(equivalent(a, b, tolerance=tolerance) for a, b in zip(left, right, strict=True))
    return left == right


def first_diffs(left: object, right: object, *, path: str = "$", limit: int = 12) -> list[str]:
    if limit <= 0 or equivalent(left, right):
        return []
    if isinstance(left, dict) and isinstance(right, dict):
        diffs: list[str] = []
        for key in sorted(set(left.keys()) | set(right.keys())):
            if key not in left:
                diffs.append(f"{path}.{key}: missing in stored")
            elif key not in right:
                diffs.append(f"{path}.{key}: missing in recomputed")
            else:
                diffs.extend(first_diffs(left[key], right[key], path=f"{path}.{key}", limit=limit - len(diffs)))
            if len(diffs) >= limit:
                return diffs[:limit]
        return diffs
    if isinstance(left, list) and isinstance(right, list):
        diffs = []
        for index, (a, b) in enumerate(zip(left, right, strict=False)):
            diffs.extend(first_diffs(a, b, path=f"{path}[{index}]", limit=limit - len(diffs)))
            if len(diffs) >= limit:
                return diffs[:limit]
        if len(left) != len(right):
            diffs.append(f"{path}: stored length {len(left)} != recomputed length {len(right)}")
        return diffs[:limit]
    return [f"{path}: stored={left!r} recomputed={right!r}"]


def nested_get(mapping: dict[str, Any], *keys: str) -> Any:
    cursor: Any = mapping
    for key in keys:
        if not isinstance(cursor, dict):
            return None
        cursor = cursor.get(key)
    return cursor


def cost_summary(cost: object) -> dict[str, Any]:
    if not isinstance(cost, dict):
        return {"present": False}
    return {
        "present": True,
        "status": cost.get("status"),
        "source": cost.get("source"),
        "total_daily_cost_aud": cost.get("total_daily_cost_aud"),
        "samples": cost.get("samples"),
        "coverage_ratio": cost.get("coverage_ratio"),
        "fixed_aud": nested_get(cost, "line_items", "fixed", "total_aud"),
        "import_aud": nested_get(cost, "line_items", "import", "charge_aud"),
        "export_credit_aud": nested_get(cost, "line_items", "export", "credit_aud"),
        "zerohero_status": nested_get(cost, "line_items", "zerohero", "status"),
        "zerohero_credit_aud": nested_get(cost, "line_items", "zerohero", "credit_aud"),
        "zerohero_import_kwh": nested_get(cost, "line_items", "zerohero", "import_kwh"),
        "zerohero_coverage_ratio": nested_get(cost, "line_items", "zerohero", "coverage_ratio"),
        "plan_id": nested_get(cost, "plan", "id"),
    }


def refresh_bill_estimate(payload: dict[str, Any]) -> bool:
    actual = payload.get("_bill_actual_cost")
    daily_cost = payload.get("_daily_cost")
    if not isinstance(actual, dict) or not isinstance(daily_cost, dict):
        return False
    estimated = daily_cost.get("total_daily_cost_aud")
    try:
        estimated_float = float(estimated)
        actual_float = float(actual.get("actual_daily_cost_aud"))
    except (TypeError, ValueError):
        return False
    changed = actual.get("estimated_daily_cost_aud") != estimated_float
    actual["estimated_daily_cost_aud"] = estimated_float
    actual["delta_aud"] = round(estimated_float - actual_float, 4)
    return changed


def quick_check(db_path: Path) -> str:
    with sqlite3.connect(str(db_path), timeout=30.0) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        row = conn.execute("PRAGMA quick_check").fetchone()
    return str(row[0] if row else "")


def load_report_rows(db_path: Path, start: date | None, end: date | None) -> list[sqlite3.Row]:
    clauses = []
    params: list[str] = []
    if start is not None:
        clauses.append("report_day_local >= ?")
        params.append(start.isoformat())
    if end is not None:
        clauses.append("report_day_local <= ?")
        params.append(end.isoformat())
    where_sql = " WHERE " + " AND ".join(clauses) if clauses else ""
    with sqlite3.connect(str(db_path), timeout=30.0) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn.execute(
            f"""
            SELECT report_day_local, timezone_name, period_start_utc, period_end_utc, usage_json
            FROM daily_reports
            {where_sql}
            ORDER BY report_day_local
            """,
            params,
        ).fetchall()


def backup_database(db_path: Path, audit_dir: Path, stamp: str) -> Path:
    audit_dir.mkdir(parents=True, exist_ok=True)
    backup_path = audit_dir / f"{db_path.name}.before-daily-cost-audit-{stamp}.bak"
    shutil.copy2(db_path, backup_path)
    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{db_path}{suffix}")
        if sidecar.exists():
            shutil.copy2(sidecar, audit_dir / f"{db_path.name}{suffix}.before-daily-cost-audit-{stamp}.bak")
    return backup_path


def main() -> int:
    args = parse_args()
    started_at_utc = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    stamp = utc_stamp()
    db_path = args.db.resolve()
    plans_path = args.plans.resolve()
    audit_dir = args.audit_dir.resolve()
    audit_dir.mkdir(parents=True, exist_ok=True)

    plan_payload = load_json_object(plans_path.read_text(encoding="utf-8"))
    settings = load_settings()
    sample_interval_seconds = max(
        float(settings.saj_sample_interval_seconds),
        float(settings.solplanet_sample_interval_seconds),
    )
    rows = load_report_rows(db_path, args.start, args.end)
    seed = str(args.seed or started_at_utc)
    sample_size = max(0, min(int(args.sample_size), len(rows)))
    rng = random.Random(seed)
    sample_days = {
        rows[index]["report_day_local"]
        for index in sorted(rng.sample(range(len(rows)), sample_size))
    } if sample_size else set()

    db_quick_check = quick_check(db_path)
    day_results: list[dict[str, Any]] = []
    updates: list[tuple[str, str]] = []
    missing_days: list[str] = []
    mismatch_days: list[str] = []
    refreshed_bill_estimate_days: list[str] = []

    for row in rows:
        day_text = str(row["report_day_local"])
        report_day = date.fromisoformat(day_text)
        payload = load_json_object(row["usage_json"])
        stored_cost = payload.get("_daily_cost")
        electricity_plan = select_billed_electricity_plan(plan_payload, report_day)
        recomputed_cost = compute_daily_report_cost(
            db_path,
            report_day_local=report_day,
            system="combined",
            start_at_utc=parse_utc(row["period_start_utc"]),
            end_at_utc=parse_utc(row["period_end_utc"]),
            sample_interval_seconds=sample_interval_seconds,
            local_timezone_name=str(row["timezone_name"] or ""),
            electricity_plan=electricity_plan,
        )

        stored_normalized = normalize_cost(stored_cost)
        recomputed_normalized = normalize_cost(recomputed_cost)
        stored_present = isinstance(stored_cost, dict)
        match = stored_present and equivalent(stored_normalized, recomputed_normalized)
        issue = "ok"
        if not stored_present:
            issue = "missing"
            missing_days.append(day_text)
        elif not match:
            issue = "mismatch"
            mismatch_days.append(day_text)

        should_update = args.write and (args.force or issue != "ok")
        if should_update:
            payload["_daily_cost"] = recomputed_cost
            if refresh_bill_estimate(payload):
                refreshed_bill_estimate_days.append(day_text)
            updates.append((stable_json(payload), day_text))

        day_result = {
            "day": day_text,
            "issue": issue,
            "stored_digest": sha256_bytes(stable_json(stored_normalized).encode("utf-8")) if stored_present else None,
            "recomputed_digest": sha256_bytes(stable_json(recomputed_normalized).encode("utf-8")),
            "stored": cost_summary(stored_cost),
            "recomputed": cost_summary(recomputed_cost),
            "sampled": day_text in sample_days,
        }
        if issue == "mismatch":
            day_result["diffs"] = first_diffs(stored_normalized, recomputed_normalized)
        day_results.append(day_result)

    backup_path: Path | None = None
    if updates:
        backup_path = backup_database(db_path, audit_dir, stamp)
        with sqlite3.connect(str(db_path), timeout=30.0) as conn:
            conn.execute("PRAGMA busy_timeout=30000")
            conn.executemany("UPDATE daily_reports SET usage_json = ? WHERE report_day_local = ?", updates)
            conn.commit()

    audit_payload = {
        "script": {
            "path": str(Path(__file__).resolve()),
            "sha256": sha256_file(Path(__file__).resolve()),
        },
        "started_at_utc": started_at_utc,
        "db": {
            "path": str(db_path),
            "size_bytes": db_path.stat().st_size if db_path.exists() else None,
            "quick_check_before": db_quick_check,
            "quick_check_after": quick_check(db_path) if updates else db_quick_check,
            "backup_before_write": str(backup_path) if backup_path else None,
        },
        "plans": {
            "path": str(plans_path),
            "sha256": sha256_file(plans_path),
        },
        "settings": {
            "local_timezone": str(getattr(settings, "local_timezone", "") or ""),
            "combined_sample_interval_seconds": sample_interval_seconds,
        },
        "selection": {
            "start": args.start.isoformat() if args.start else None,
            "end": args.end.isoformat() if args.end else None,
            "row_count": len(rows),
        },
        "random_sample": {
            "seed": seed,
            "sample_size": sample_size,
            "days": sorted(sample_days),
        },
        "write": {
            "enabled": bool(args.write),
            "force": bool(args.force),
            "updated_count": len(updates),
            "refreshed_bill_estimate_count": len(refreshed_bill_estimate_days),
            "refreshed_bill_estimate_days": refreshed_bill_estimate_days,
        },
        "summary": {
            "ok_count": sum(1 for item in day_results if item["issue"] == "ok"),
            "missing_count": len(missing_days),
            "mismatch_count": len(mismatch_days),
            "missing_days": missing_days,
            "mismatch_days": mismatch_days,
        },
        "sample_results": [item for item in day_results if item["sampled"]],
        "day_results": day_results,
    }
    audit_path = audit_dir / f"daily-report-cost-audit-{stamp}.json"
    audit_path.write_text(json.dumps(audit_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Audit file: {audit_path}")
    print(f"DB quick_check: {audit_payload['db']['quick_check_after']}")
    print(
        "Rows={row_count} ok={ok_count} missing={missing_count} mismatch={mismatch_count} "
        "updated={updated_count}".format(
            row_count=len(rows),
            ok_count=audit_payload["summary"]["ok_count"],
            missing_count=len(missing_days),
            mismatch_count=len(mismatch_days),
            updated_count=len(updates),
        )
    )
    print(f"Random sample seed: {seed}")
    print("Random sample days: " + ", ".join(sorted(sample_days)))
    for item in audit_payload["sample_results"]:
        stored = item["stored"]
        recomputed = item["recomputed"]
        print(
            "{day}: {issue}; stored={stored_total} recomputed={recomputed_total}; "
            "zerohero={stored_zerohero}/{recomputed_zerohero}".format(
                day=item["day"],
                issue=item["issue"],
                stored_total=stored.get("total_daily_cost_aud"),
                recomputed_total=recomputed.get("total_daily_cost_aud"),
                stored_zerohero=stored.get("zerohero_credit_aud"),
                recomputed_zerohero=recomputed.get("zerohero_credit_aud"),
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
