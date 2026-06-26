from __future__ import annotations

import io
import json
import os
import re
import shutil
import sqlite3
import statistics
import subprocess
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import Column, Float, Index, Integer, String, create_engine, delete, desc, func, insert, inspect, literal, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.worker_log_schema import (
    WORKER_LOG_STATUS_PENDING,
    WORKER_LOG_STATUS_TIMEOUT,
    status_config as worker_log_status_config,
)

NOTIFICATION_STATUS_ACTIVE = "active"
NOTIFICATION_STATUS_DISMISSED = "dismissed"
WORKER_LOG_LEGACY_STATUS_MAP: dict[str, str] = {
    "notification": "send",
    "notified": "send",
    "alarmed": "send",
    "no_notification": "noop",
    "operation": "applied",
    "nop": "noop",
}


DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "wattimize.sqlite3"
DEFAULT_SAMPLE_INTERVAL_SECONDS = 5.0
SQLITE_BUSY_TIMEOUT_SECONDS = 10.0
SQLITE_INTEGRITY_CHECK_TIMEOUT_SECONDS = 600.0
SQLITE_VACUUM_MIN_RECLAIM_BYTES = 16 * 1024 * 1024
CSV_HEADERS: tuple[str, ...] = (
    "system",
    "sampled_at_utc",
    "sampled_at_epoch",
    "source",
    "pv_w",
    "grid_w",
    "battery_w",
    "load_w",
    "battery_soc_percent",
    "inverter_status",
    "balance_w",
    "payload_json",
)
SOLPLANET_ENDPOINT_TABLES: dict[str, str] = {
    "getdev_device_0": "solplanet_getdev_device_0",
    "getdev_device_2": "solplanet_getdev_device_2",
    "getdev_device_3": "solplanet_getdev_device_3",
    "getdev_device_4": "solplanet_getdev_device_4",
    "getdevdata_device_2": "solplanet_getdevdata_device_2",
    "getdevdata_device_3": "solplanet_getdevdata_device_3",
    "getdevdata_device_4": "solplanet_getdevdata_device_4",
    "getdevdata_device_5": "solplanet_getdevdata_device_5",
    "getdefine": "solplanet_getdefine",
}
WORKER_API_LOG_RESULT_MAX_CHARS = 20000
WORKER_API_LOG_PAYLOAD_MAX_CHARS = 200000
OPERATION_RUN_STATUS_PENDING = "pending"
OPERATION_RUN_STATUS_SUCCEEDED = "succeeded"
OPERATION_RUN_STATUS_FAILED = "failed"
OPERATION_RUN_STATUS_TIMEOUT = "timeout"
POOL_PUMP_POWER_STALE_SECONDS = 30 * 60
DAILY_REPORT_BATTERY_CAPACITY_KWH = {
    "saj": 15.0,
    "solplanet": 50.0,
}
DAILY_REPORT_BATTERY_MIN_DISCHARGE_SOC_PERCENT = {
    "saj": 20.0,
    "solplanet": 10.0,
}
DAILY_REPORT_PHASE_BATTERY_STAGES: tuple[dict[str, object], ...] = (
    {"id": "overnight_shoulder", "titleKey": "notificationMatrixWindowOvernightTitle", "endHour": 11},
    {"id": "free_energy", "titleKey": "notificationMatrixWindowFreeEnergyTitle", "endHour": 14},
    {"id": "after_free_shoulder", "titleKey": "notificationMatrixWindowAfterFreeShoulderTitle", "endHour": 18},
    {"id": "export_window", "titleKey": "notificationMatrixWindowExportTitle", "endHour": 20},
    {"id": "post_export_peak", "titleKey": "notificationMatrixWindowPostExportPeakTitle", "endHour": 23},
)
DAILY_REPORT_PHASE_BATTERY_SAMPLE_TOLERANCE_SECONDS = 15 * 60
DAILY_REPORT_STATUS_SAVED = "saved"
DAILY_REPORT_STATUS_NO_DATA = "no_data"
MONTHLY_REPORT_STATUS_SAVED = "saved"
MONTHLY_REPORT_STATUS_NO_DATA = "no_data"


@dataclass(frozen=True)
class AssembledFlowSnapshot:
    round_id: str
    system: str
    assembled_at_utc: datetime
    source: str
    pv_w: float | None
    grid_w: float | None
    battery_w: float | None
    load_w: float | None
    battery_soc_percent: float | None
    inverter_status: str | None
    balance_w: float | None
    flow: dict[str, object]


EnergySample = AssembledFlowSnapshot
DatabaseTarget = Path | str


class Base(DeclarativeBase):
    pass


class AssembledFlowSnapshotRow(Base):
    __tablename__ = "assembled_flow_snapshots"
    __table_args__ = (Index("ix_assembled_flow_snapshots_system_epoch", "system", "assembled_at_epoch"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    round_id = Column(String, nullable=False)
    system = Column(String, nullable=False, index=True)
    assembled_at_utc = Column(String, nullable=False)
    assembled_at_epoch = Column(Float, nullable=False, index=True)
    source = Column(String, nullable=False)
    pv_w = Column(Float)
    grid_w = Column(Float)
    battery_w = Column(Float)
    load_w = Column(Float)
    battery_soc_percent = Column(Float)
    inverter_status = Column(String)
    balance_w = Column(Float)
    flow_json = Column(String, nullable=False)


class DailyReportRow(Base):
    __tablename__ = "daily_reports"

    report_day_local = Column(String, primary_key=True)
    timezone_name = Column(String, nullable=False)
    period_start_utc = Column(String, nullable=False)
    period_start_epoch = Column(Float, nullable=False, index=True)
    period_end_utc = Column(String, nullable=False)
    period_end_epoch = Column(Float, nullable=False, index=True)
    generated_at_utc = Column(String, nullable=False)
    generated_at_epoch = Column(Float, nullable=False, index=True)
    sample_count = Column(Integer, nullable=False, default=0)
    usage_json = Column(String, nullable=False)
    series_json = Column(String, nullable=False)
    zero_hero_json = Column(String, nullable=False)


def _daily_report_status(sample_count: int) -> str:
    """Return the display status for a persisted daily report."""
    return DAILY_REPORT_STATUS_SAVED if sample_count > 0 else DAILY_REPORT_STATUS_NO_DATA


def _monthly_report_data_day_count(payload: object) -> int:
    """Count days with usable samples in a persisted monthly report payload."""
    if not isinstance(payload, dict):
        return 0
    daily = payload.get("daily")
    items = daily.get("items") if isinstance(daily, dict) else None
    if not isinstance(items, list):
        return 0
    return sum(1 for item in items if isinstance(item, dict) and bool(item.get("has_data")))


def _monthly_report_status(payload: object) -> str:
    return MONTHLY_REPORT_STATUS_SAVED if _monthly_report_data_day_count(payload) > 0 else MONTHLY_REPORT_STATUS_NO_DATA


class MonthlyReportRow(Base):
    __tablename__ = "monthly_reports"

    report_month_local = Column(String, primary_key=True)
    timezone_name = Column(String, nullable=False)
    period_start_utc = Column(String, nullable=False)
    period_start_epoch = Column(Float, nullable=False, index=True)
    period_end_utc = Column(String, nullable=False)
    period_end_epoch = Column(Float, nullable=False, index=True)
    generated_at_utc = Column(String, nullable=False)
    generated_at_epoch = Column(Float, nullable=False, index=True)
    observed_day_count = Column(Integer, nullable=False, default=0)
    payload_json = Column(String, nullable=False)


class DailyReportSampleRow(Base):
    __tablename__ = "daily_report_samples"
    __table_args__ = (Index("ix_daily_report_samples_day_epoch", "report_day_local", "sampled_at_epoch"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_day_local = Column(String, nullable=False)
    source_snapshot_id = Column(Integer, nullable=False)
    system = Column(String, nullable=False, index=True)
    sampled_at_utc = Column(String, nullable=False)
    sampled_at_epoch = Column(Float, nullable=False)
    source = Column(String, nullable=False)
    pv_w = Column(Float)
    grid_w = Column(Float)
    battery_w = Column(Float)
    load_w = Column(Float)
    battery_soc_percent = Column(Float)
    inverter_status = Column(String)
    balance_w = Column(Float)


class RealtimeKvRow(Base):
    __tablename__ = "realtime_kv"

    attribute = Column(String, primary_key=True)
    value = Column(String, nullable=False)
    source = Column(String, nullable=False)


class WorkerApiLogRow(Base):
    __tablename__ = "worker_api_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    request_token = Column(String, index=True)
    round_id = Column(String)
    worker = Column(String, nullable=False)
    system = Column(String)
    service = Column(String, nullable=False)
    method = Column(String, nullable=False)
    api_link = Column(String, nullable=False)
    requested_at_utc = Column(String, nullable=False)
    requested_at_epoch = Column(Float, nullable=False, index=True)
    ok = Column(Integer, nullable=False)
    status = Column(String)
    status_code = Column(Integer)
    duration_ms = Column(Float)
    result_text = Column(String)
    error_text = Column(String)
    payload_json = Column(String)


class NotificationDismissalRow(Base):
    __tablename__ = "notification_dismissals"

    notification_key = Column(String, primary_key=True)
    dismissed_at_utc = Column(String, nullable=False)
    dismissed_at_epoch = Column(Float, nullable=False, index=True)


class NotificationEntryRow(Base):
    __tablename__ = "notifications"

    notification_key = Column(String, primary_key=True)
    code = Column(String, nullable=False, index=True)
    target = Column(String, nullable=False, index=True)
    level = Column(String, nullable=False)
    title = Column(String)
    message = Column(String, nullable=False)
    trigger_text = Column(String)
    window = Column(String)
    source_system = Column(String)
    source_service = Column(String)
    status = Column(String, nullable=False, index=True)
    first_seen_at_utc = Column(String, nullable=False)
    first_seen_at_epoch = Column(Float, nullable=False, index=True)
    last_seen_at_utc = Column(String, nullable=False)
    last_seen_at_epoch = Column(Float, nullable=False, index=True)
    first_log_id = Column(Integer)
    last_log_id = Column(Integer)
    dismissed_at_utc = Column(String)
    dismissed_at_epoch = Column(Float)
    payload_json = Column(String)


class TimeWindowRuleStateRow(Base):
    __tablename__ = "time_window_rule_states"

    rule_code = Column(String, primary_key=True)
    enabled = Column(Integer, nullable=False)
    updated_at_utc = Column(String, nullable=False)
    updated_at_epoch = Column(Float, nullable=False, index=True)


class OperationDefinitionRow(Base):
    __tablename__ = "operation_definitions"

    operation_code = Column(String, primary_key=True)
    system = Column(String, nullable=False, index=True)
    operation_type = Column(String, nullable=False, index=True)
    action = Column(String, nullable=False, index=True)
    title = Column(String, nullable=False)
    input_schema_json = Column(String)
    output_schema_json = Column(String)
    config_json = Column(String)
    active = Column(Integer, nullable=False, default=1, index=True)
    created_at_utc = Column(String, nullable=False)
    created_at_epoch = Column(Float, nullable=False, index=True)
    updated_at_utc = Column(String, nullable=False)
    updated_at_epoch = Column(Float, nullable=False, index=True)


class OperationRunRow(Base):
    __tablename__ = "operation_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    operation_code = Column(String, nullable=False, index=True)
    trigger_source = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False, index=True)
    started_at_utc = Column(String, nullable=False)
    started_at_epoch = Column(Float, nullable=False, index=True)
    updated_at_utc = Column(String, nullable=False)
    updated_at_epoch = Column(Float, nullable=False, index=True)
    completed_at_utc = Column(String)
    completed_at_epoch = Column(Float, index=True)
    request_payload_json = Column(String)
    previous_state_json = Column(String)
    latest_state_json = Column(String)
    response_payload_json = Column(String)
    error_text = Column(String)


_ENGINE_CACHE: dict[str, Any] = {}
_SESSION_FACTORY_CACHE: dict[str, sessionmaker[Session]] = {}


def _normalize_target(db_path: DatabaseTarget) -> str:
    if isinstance(db_path, Path):
        return str(db_path)
    return str(db_path)


def _sqlite_file_path(db_path: DatabaseTarget) -> Path | None:
    if isinstance(db_path, Path):
        return db_path
    raw = str(db_path)
    if raw.startswith("sqlite:///"):
        return Path(raw.removeprefix("sqlite:///"))
    return None


def _database_url(db_path: DatabaseTarget) -> str:
    if isinstance(db_path, Path):
        return f"sqlite:///{db_path}"
    raw = str(db_path).strip()
    if "://" in raw:
        return raw
    return f"sqlite:///{raw}"


def _sqlite_sidecar_paths(sqlite_path: Path) -> tuple[Path, Path]:
    return (
        sqlite_path.with_name(f"{sqlite_path.name}-wal"),
        sqlite_path.with_name(f"{sqlite_path.name}-shm"),
    )


def _is_sqlite(db_path: DatabaseTarget) -> bool:
    return _database_url(db_path).startswith("sqlite:///")


def _db_exists(db_path: DatabaseTarget) -> bool:
    sqlite_path = _sqlite_file_path(db_path)
    if sqlite_path is None:
        return True
    return sqlite_path.exists()


def _create_engine_for_target(db_path: DatabaseTarget):
    key = _normalize_target(db_path)
    engine = _ENGINE_CACHE.get(key)
    if engine is not None:
        return engine

    url = _database_url(db_path)
    connect_args: dict[str, object] = {}
    if url.startswith("sqlite:///"):
        connect_args = {
            "check_same_thread": False,
            "timeout": SQLITE_BUSY_TIMEOUT_SECONDS,
        }
    engine = create_engine(url, future=True, connect_args=connect_args)
    _ENGINE_CACHE[key] = engine
    return engine


def _session_factory(db_path: DatabaseTarget) -> sessionmaker[Session]:
    key = _normalize_target(db_path)
    factory = _SESSION_FACTORY_CACHE.get(key)
    if factory is not None:
        return factory
    factory = sessionmaker(bind=_create_engine_for_target(db_path), autoflush=False, expire_on_commit=False)
    _SESSION_FACTORY_CACHE[key] = factory
    return factory


def dispose_db_connections(db_path: DatabaseTarget) -> None:
    key = _normalize_target(db_path)
    _SESSION_FACTORY_CACHE.pop(key, None)
    engine = _ENGINE_CACHE.pop(key, None)
    if engine is not None:
        engine.dispose()


@contextmanager
def _session_scope(db_path: DatabaseTarget) -> Any:
    session = _session_factory(db_path)()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _parse_iso_to_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _json_loads(value: str | None, *, default: object) -> object:
    if not isinstance(value, str) or not value.strip():
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def _extract_pool_pump_power_last_updated(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    raw = payload.get("raw")
    raw_map = raw if isinstance(raw, dict) else {}
    pool_pump = raw_map.get("pool_pump")
    pool_pump_map = pool_pump if isinstance(pool_pump, dict) else {}
    metrics = pool_pump_map.get("metrics")
    metrics_map = metrics if isinstance(metrics, dict) else {}
    power_entity = metrics_map.get("power_entity")
    power_entity_map = power_entity if isinstance(power_entity, dict) else {}
    value = power_entity_map.get("last_updated") or power_entity_map.get("last_changed")
    text = str(value or "").strip()
    return text or None


def _compute_pool_pump_power_stale(
    *,
    sampled_at_utc: object,
    power_last_updated: object,
) -> bool | None:
    sampled_text = str(sampled_at_utc or "").strip()
    power_text = str(power_last_updated or "").strip()
    if not sampled_text or not power_text:
        return None
    try:
        sampled_at = _parse_iso_to_utc(sampled_text)
        power_updated_at = _parse_iso_to_utc(power_text)
    except ValueError:
        return None
    age_seconds = max((sampled_at - power_updated_at).total_seconds(), 0.0)
    return age_seconds > POOL_PUMP_POWER_STALE_SECONDS


def _truncate_result_text(text_value: str | None) -> str:
    raw = str(text_value or "")
    if len(raw) <= WORKER_API_LOG_RESULT_MAX_CHARS:
        return raw
    tail = f"\n...[truncated {len(raw) - WORKER_API_LOG_RESULT_MAX_CHARS} chars]"
    keep = max(0, WORKER_API_LOG_RESULT_MAX_CHARS - len(tail))
    return f"{raw[:keep]}{tail}"


def _truncate_payload_text(text_value: str | None) -> str:
    raw = str(text_value or "")
    if len(raw) <= WORKER_API_LOG_PAYLOAD_MAX_CHARS:
        return raw
    tail = f"\n...[truncated {len(raw) - WORKER_API_LOG_PAYLOAD_MAX_CHARS} chars]"
    keep = max(0, WORKER_API_LOG_PAYLOAD_MAX_CHARS - len(tail))
    return f"{raw[:keep]}{tail}"


def _json_dumps(value: object | None) -> str | None:
    if value is None:
        return None
    return _truncate_payload_text(json.dumps(value, ensure_ascii=False, default=str))


def _json_dumps_full(value: object | None) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


def _normalize_worker_log_status(status: str | None) -> str | None:
    normalized = str(status or "").strip().lower()
    if not normalized:
        return None
    if worker_log_status_config(normalized):
        return normalized
    return normalized


def _normalize_notification_key(notification_key: str | None, *, code: str | None = None, target: str | None = None) -> str:
    normalized_key = str(notification_key or "").strip()
    if normalized_key:
        return normalized_key
    normalized_code = str(code or "unknown").strip() or "unknown"
    normalized_target = str(target or "").strip()
    return f"{normalized_code}::{normalized_target}"


def _paginate(page: int, page_size: int) -> tuple[int, int]:
    safe_page = max(1, page)
    safe_page_size = min(500, max(1, page_size))
    return safe_page, safe_page_size


def _empty_page(page: int, page_size: int) -> dict[str, object]:
    return {
        "count": 0,
        "total": 0,
        "page": page,
        "page_size": page_size,
        "has_next": False,
        "has_prev": False,
        "items": [],
    }


def _quote_sqlite_identifier(identifier: str) -> str:
    escaped = str(identifier).replace('"', '""')
    return f'"{escaped}"'


def _json_safe_db_value(value: object) -> object:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    return str(value)


def list_database_tables(db_path: DatabaseTarget) -> dict[str, object]:
    engine = _create_engine_for_target(db_path)
    inspector = inspect(engine)
    table_names = sorted(inspector.get_table_names())
    return {
        "tables": [
            {
                "name": table_name,
            }
            for table_name in table_names
        ],
    }


def list_database_table_rows(
    db_path: DatabaseTarget,
    *,
    table: str,
    page: int,
    page_size: int,
) -> dict[str, object]:
    safe_page, safe_page_size = _paginate(page, page_size)
    engine = _create_engine_for_target(db_path)
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    normalized_table = str(table or "").strip()
    if not normalized_table:
        raise ValueError("table is required")
    if normalized_table not in table_names:
        raise ValueError(f"Unknown table: {normalized_table}")

    columns = inspector.get_columns(normalized_table)
    quoted_table = _quote_sqlite_identifier(normalized_table)
    offset = (safe_page - 1) * safe_page_size
    order_clause = " ORDER BY assembled_at_utc DESC" if normalized_table == "assembled_flow_snapshots" else ""
    with engine.connect() as conn:
        total = int(conn.execute(text(f"SELECT COUNT(*) FROM {quoted_table}")).scalar_one())
        rows = conn.execute(
            text(f"SELECT * FROM {quoted_table}{order_clause} LIMIT :limit OFFSET :offset"),
            {"limit": safe_page_size, "offset": offset},
        ).mappings().all()
    items = [{key: _json_safe_db_value(value) for key, value in row.items()} for row in rows]
    count = len(items)
    return {
        "table": normalized_table,
        "columns": [
            {
                "name": str(column.get("name") or ""),
                "type": str(column.get("type") or ""),
                "nullable": bool(column.get("nullable", True)),
            }
            for column in columns
        ],
        "count": count,
        "total": total,
        "page": safe_page,
        "page_size": safe_page_size,
        "has_next": offset + count < total,
        "has_prev": safe_page > 1,
        "items": items,
    }


def init_db(db_path: DatabaseTarget) -> None:
    sqlite_path = _sqlite_file_path(db_path)
    if sqlite_path is not None:
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    engine = _create_engine_for_target(db_path)
    Base.metadata.create_all(engine)
    if _is_sqlite(db_path):
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS energy_samples"))
            conn.execute(text("DROP TABLE IF EXISTS raw_request_results"))
            conn.execute(text("PRAGMA journal_mode=WAL;"))
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_assembled_flow_snapshots_system_epoch "
                    "ON assembled_flow_snapshots (system, assembled_at_epoch)"
                )
            )


def export_database_bytes(db_path: DatabaseTarget) -> bytes:
    sqlite_path = _sqlite_file_path(db_path)
    if sqlite_path is None:
        raise ValueError("Database export only supports local SQLite files")
    if not sqlite_path.exists():
        init_db(db_path)
    return sqlite_path.read_bytes()


def inspect_sqlite_database(db_path: DatabaseTarget) -> dict[str, object]:
    sqlite_path = _sqlite_file_path(db_path)
    if sqlite_path is None:
        raise ValueError("Database inspection only supports local SQLite files")
    if not sqlite_path.exists():
        return {
            "db_path": str(sqlite_path),
            "exists": False,
            "ok": True,
            "journal_mode": None,
            "integrity_check": [],
            "quick_check": [],
        }

    db_size_bytes = sqlite_path.stat().st_size
    with tempfile.TemporaryDirectory(prefix="wattimize-db-check-") as temp_dir_raw:
        temp_dir = Path(temp_dir_raw)
        probe_path = temp_dir / sqlite_path.name
        shutil.copy2(sqlite_path, probe_path)
        for sidecar in _sqlite_sidecar_paths(sqlite_path):
            if sidecar.exists():
                shutil.copy2(sidecar, temp_dir / sidecar.name)

        try:
            result = subprocess.run(
                [
                    "sqlite3",
                    str(probe_path),
                    "PRAGMA journal_mode; PRAGMA integrity_check; PRAGMA quick_check;",
                ],
                capture_output=True,
                text=True,
                timeout=SQLITE_INTEGRITY_CHECK_TIMEOUT_SECONDS,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            return {
                "db_path": str(sqlite_path),
                "exists": True,
                "ok": False,
                "timed_out": True,
                "db_size_bytes": db_size_bytes,
                "timeout_seconds": SQLITE_INTEGRITY_CHECK_TIMEOUT_SECONDS,
                "journal_mode": None,
                "integrity_check": [],
                "quick_check": [],
                "stderr": str(exc),
            }
    lines = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
    journal_mode = lines[0] if lines else None
    checks = lines[1:] if lines else []
    ok = result.returncode == 0 and checks and all(line == "ok" for line in checks)
    return {
        "db_path": str(sqlite_path),
        "exists": True,
        "ok": ok,
        "timed_out": False,
        "db_size_bytes": db_size_bytes,
        "journal_mode": journal_mode,
        "integrity_check": checks[:-1] if len(checks) > 1 else checks,
        "quick_check": [checks[-1]] if checks else [],
        "returncode": result.returncode,
        "stderr": (result.stderr or "").strip(),
    }


def recover_sqlite_database(db_path: DatabaseTarget, *, recovery_dir: Path | None = None) -> dict[str, object]:
    sqlite_path = _sqlite_file_path(db_path)
    if sqlite_path is None:
        raise ValueError("Database recovery only supports local SQLite files")
    if not sqlite_path.exists():
        raise FileNotFoundError(f"Database not found: {sqlite_path}")

    dispose_db_connections(db_path)
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    target_recovery_dir = recovery_dir or sqlite_path.parent / "recovery"
    target_recovery_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="wattimize-db-recover-") as temp_dir_raw:
        temp_dir = Path(temp_dir_raw)
        source_copy = temp_dir / sqlite_path.name
        shutil.copy2(sqlite_path, source_copy)
        for sidecar in _sqlite_sidecar_paths(sqlite_path):
            if sidecar.exists():
                shutil.copy2(sidecar, temp_dir / sidecar.name)

        sql_dump_path = target_recovery_dir / f"{sqlite_path.stem}.recover.sql"
        recovered_path = target_recovery_dir / f"{sqlite_path.stem}.recovered.sqlite3"
        if recovered_path.exists():
            recovered_path.unlink()
        for sidecar in _sqlite_sidecar_paths(recovered_path):
            if sidecar.exists():
                sidecar.unlink()

        with sql_dump_path.open("w", encoding="utf-8") as sql_dump:
            recover_dump = subprocess.run(
                ["sqlite3", str(source_copy), ".recover"],
                stdout=sql_dump,
                stderr=subprocess.PIPE,
                text=True,
                timeout=SQLITE_INTEGRITY_CHECK_TIMEOUT_SECONDS,
                check=False,
            )
        if recover_dump.returncode != 0:
            raise RuntimeError((recover_dump.stderr or "sqlite3 .recover failed").strip())

        rebuild = subprocess.Popen(
            ["sqlite3", str(recovered_path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert rebuild.stdin is not None
        try:
            with sql_dump_path.open("rb") as sql_input:
                for line in sql_input:
                    if line.startswith(b"CREATE TABLE sqlite_sequence"):
                        continue
                    rebuild.stdin.write(line)
            rebuild.stdin.close()
            rebuild.stdin = None
            stdout_bytes, stderr_bytes = rebuild.communicate(timeout=SQLITE_INTEGRITY_CHECK_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired as exc:
            rebuild.kill()
            stdout_bytes, stderr_bytes = rebuild.communicate()
            stdout_text = stdout_bytes.decode("utf-8", errors="replace") if stdout_bytes else ""
            stderr_text = stderr_bytes.decode("utf-8", errors="replace") if stderr_bytes else ""
            raise RuntimeError(f"sqlite3 rebuild timed out: {stderr_text or stdout_text or exc}") from exc
        except BrokenPipeError as exc:
            if rebuild.stdin is not None:
                try:
                    rebuild.stdin.close()
                except OSError:
                    pass
                rebuild.stdin = None
            stdout_bytes, stderr_bytes = rebuild.communicate()
            stdout_text = stdout_bytes.decode("utf-8", errors="replace") if stdout_bytes else ""
            stderr_text = stderr_bytes.decode("utf-8", errors="replace") if stderr_bytes else ""
            raise RuntimeError((stderr_text or stdout_text or "sqlite3 rebuild failed").strip()) from exc
        stdout_text = stdout_bytes.decode("utf-8", errors="replace") if stdout_bytes else ""
        stderr_text = stderr_bytes.decode("utf-8", errors="replace") if stderr_bytes else ""
        if rebuild.returncode != 0:
            raise RuntimeError((stderr_text or stdout_text or "sqlite3 rebuild failed").strip())

        recovered_status = inspect_sqlite_database(recovered_path)
        if not bool(recovered_status.get("ok")):
            raise RuntimeError(
                f"Recovered database integrity check failed: {recovered_status.get('integrity_check') or recovered_status.get('stderr')}"
            )

    backup_paths: list[str] = []
    backup_db_path = target_recovery_dir / f"{sqlite_path.name}.{timestamp}.bak"
    sqlite_path.replace(backup_db_path)
    backup_paths.append(str(backup_db_path))
    for sidecar in _sqlite_sidecar_paths(sqlite_path):
        if sidecar.exists():
            backup_sidecar_path = target_recovery_dir / f"{sidecar.name}.{timestamp}.bak"
            sidecar.replace(backup_sidecar_path)
            backup_paths.append(str(backup_sidecar_path))

    shutil.copy2(recovered_path, sqlite_path)
    for sidecar in _sqlite_sidecar_paths(sqlite_path):
        if sidecar.exists():
            sidecar.unlink()
    dispose_db_connections(db_path)

    return {
        "db_path": str(sqlite_path),
        "recovered_path": str(recovered_path),
        "sql_dump_path": str(sql_dump_path),
        "backup_paths": backup_paths,
        "db_size_bytes": sqlite_path.stat().st_size,
        "recovered_at_utc": datetime.now(UTC).isoformat(),
    }


def import_database_bytes(db_path: DatabaseTarget, payload: bytes) -> dict[str, object]:
    sqlite_path = _sqlite_file_path(db_path)
    if sqlite_path is None:
        raise ValueError("Database import only supports local SQLite files")
    if not payload:
        raise ValueError("Uploaded database file is empty")
    if not payload.startswith(b"SQLite format 3\x00"):
        raise ValueError("Uploaded file is not a valid SQLite database")

    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = sqlite_path.with_name(f"{sqlite_path.name}.importing")
    dispose_db_connections(db_path)
    temp_path.write_bytes(payload)
    temp_path.replace(sqlite_path)
    for sidecar in _sqlite_sidecar_paths(sqlite_path):
        if sidecar.exists():
            sidecar.unlink()
    dispose_db_connections(db_path)
    return {
        "db_path": str(sqlite_path),
        "db_size_bytes": sqlite_path.stat().st_size,
    }


def migrate_worker_log_legacy_statuses(db_path: DatabaseTarget) -> int:
    if not _db_exists(db_path):
        return 0
    updated = 0
    try:
        with _session_scope(db_path) as session:
            rows = session.scalars(
                select(WorkerApiLogRow).where(WorkerApiLogRow.status.in_(tuple(WORKER_LOG_LEGACY_STATUS_MAP.keys())))
            ).all()
            for row in rows:
                normalized = WORKER_LOG_LEGACY_STATUS_MAP.get(str(row.status or "").strip().lower())
                if not normalized:
                    continue
                if str(row.status or "").strip().lower() == normalized:
                    continue
                row.status = normalized
                updated += 1
        return updated
    except SQLAlchemyError:
        return 0


def insert_worker_api_log(
    db_path: DatabaseTarget,
    *,
    request_token: str | None = None,
    round_id: str | None = None,
    worker: str,
    system: str | None,
    service: str,
    method: str,
    api_link: str,
    requested_at_utc: str,
    ok: bool,
    status: str | None = None,
    status_code: int | None,
    duration_ms: float | None,
    result_text: str | None,
    error_text: str | None,
    payload_json: str | None = None,
) -> int:
    requested = _parse_iso_to_utc(requested_at_utc)
    normalized_status = _normalize_worker_log_status(status)
    with _session_scope(db_path) as session:
        row = WorkerApiLogRow(
            request_token=str(request_token or "") or None,
            round_id=str(round_id or "") or None,
            worker=str(worker or "worker"),
            system=str(system or "") or None,
            service=str(service or "unknown"),
            method=str(method or "GET").upper(),
            api_link=str(api_link or ""),
            requested_at_utc=requested.isoformat(),
            requested_at_epoch=requested.timestamp(),
            ok=1 if ok else 0,
            status=normalized_status,
            status_code=status_code,
            duration_ms=duration_ms,
            result_text=_truncate_result_text(result_text),
            error_text=str(error_text or "") or None,
            payload_json=_truncate_payload_text(payload_json) if payload_json is not None else None,
        )
        session.add(row)
        session.flush()
        return int(row.id)


def update_worker_api_log(
    db_path: DatabaseTarget,
    *,
    request_token: str,
    ok: bool,
    status: str | None,
    status_code: int | None,
    duration_ms: float | None,
    result_text: str | None,
    error_text: str | None,
    payload_json: str | None = None,
 ) -> int | None:
    if not request_token:
        return None
    normalized_status = _normalize_worker_log_status(status)
    with _session_scope(db_path) as session:
        row = session.scalar(
            select(WorkerApiLogRow)
            .where(WorkerApiLogRow.request_token == request_token)
            .order_by(desc(WorkerApiLogRow.id))
            .limit(1)
        )
        if row is None:
            return None
        row.ok = 1 if ok else 0
        row.status = normalized_status
        row.status_code = status_code
        row.duration_ms = duration_ms
        row.result_text = _truncate_result_text(result_text)
        row.error_text = str(error_text or "") or None
        row.payload_json = _truncate_payload_text(payload_json) if payload_json is not None else row.payload_json
        session.flush()
        return int(row.id)


def expire_pending_worker_api_logs(
    db_path: DatabaseTarget,
    *,
    older_than_epoch: float,
    status: str = WORKER_LOG_STATUS_TIMEOUT,
    error_text: str = "worker_log_timeout",
) -> int:
    try:
        with _session_scope(db_path) as session:
            rows = session.scalars(
                select(WorkerApiLogRow).where(
                    WorkerApiLogRow.status == WORKER_LOG_STATUS_PENDING,
                    WorkerApiLogRow.requested_at_epoch < float(older_than_epoch),
                )
            ).all()
            for row in rows:
                row.ok = 0
                row.status = _normalize_worker_log_status(status) or WORKER_LOG_STATUS_TIMEOUT
                row.error_text = row.error_text or str(error_text or "worker_log_timeout")
            return len(rows)
    except SQLAlchemyError:
        return 0


def finalize_pending_worker_api_logs_for_round(
    db_path: DatabaseTarget,
    *,
    round_id: str,
    services: tuple[str, ...] | None = None,
    status: str = WORKER_LOG_STATUS_TIMEOUT,
    error_text: str = "worker_round_incomplete",
) -> int:
    normalized_round_id = str(round_id or "").strip()
    if not normalized_round_id:
        return 0
    normalized_services = tuple(
        str(service or "").strip() for service in (services or ()) if str(service or "").strip()
    )
    try:
        with _session_scope(db_path) as session:
            stmt = select(WorkerApiLogRow).where(
                WorkerApiLogRow.round_id == normalized_round_id,
                WorkerApiLogRow.status == WORKER_LOG_STATUS_PENDING,
            )
            if normalized_services:
                stmt = stmt.where(WorkerApiLogRow.service.in_(normalized_services))
            rows = session.scalars(stmt).all()
            for row in rows:
                row.ok = 0
                row.status = _normalize_worker_log_status(status) or WORKER_LOG_STATUS_TIMEOUT
                row.error_text = row.error_text or str(error_text or "worker_round_incomplete")
            return len(rows)
    except SQLAlchemyError:
        return 0


def count_pending_worker_api_logs(
    db_path: DatabaseTarget,
    *,
    round_id: str,
    services: tuple[str, ...] | None = None,
) -> int:
    normalized_round_id = str(round_id or "").strip()
    if not normalized_round_id:
        return 0
    normalized_services = tuple(
        str(service or "").strip() for service in (services or ()) if str(service or "").strip()
    )
    try:
        with _session_scope(db_path) as session:
            stmt = select(func.count()).select_from(WorkerApiLogRow).where(
                WorkerApiLogRow.round_id == normalized_round_id,
                WorkerApiLogRow.status == "pending",
            )
            if normalized_services:
                stmt = stmt.where(WorkerApiLogRow.service.in_(normalized_services))
            value = session.scalar(stmt)
            return int(value or 0)
    except SQLAlchemyError:
        return 0


def delete_worker_api_logs_older_than(
    db_path: DatabaseTarget,
    *,
    older_than_epoch: float,
) -> int:
    try:
        with _session_scope(db_path) as session:
            result = session.execute(
                text("DELETE FROM worker_api_logs WHERE requested_at_epoch < :older_than_epoch"),
                {"older_than_epoch": float(older_than_epoch)},
            )
            deleted = int(result.rowcount or 0)
            return deleted
    except SQLAlchemyError:
        return 0


def upsert_operation_definitions(
    db_path: DatabaseTarget,
    *,
    definitions: list[dict[str, object]],
) -> int:
    normalized_definitions = [item for item in definitions if isinstance(item, dict) and str(item.get("operation_code") or "").strip()]
    if not normalized_definitions:
        return 0
    updated = 0
    now = datetime.now(UTC)
    try:
        with _session_scope(db_path) as session:
            for item in normalized_definitions:
                operation_code = str(item.get("operation_code") or "").strip()
                row = session.get(OperationDefinitionRow, operation_code)
                if row is None:
                    row = OperationDefinitionRow(
                        operation_code=operation_code,
                        created_at_utc=now.isoformat(),
                        created_at_epoch=now.timestamp(),
                    )
                    session.add(row)
                row.system = str(item.get("system") or "unknown").strip() or "unknown"
                row.operation_type = str(item.get("operation_type") or "manual").strip() or "manual"
                row.action = str(item.get("action") or "unknown").strip() or "unknown"
                row.title = str(item.get("title") or operation_code).strip() or operation_code
                row.input_schema_json = _json_dumps(item.get("input_schema"))
                row.output_schema_json = _json_dumps(item.get("output_schema"))
                row.config_json = _json_dumps(item.get("config"))
                row.active = 1 if bool(item.get("active", True)) else 0
                row.updated_at_utc = now.isoformat()
                row.updated_at_epoch = now.timestamp()
                updated += 1
        return updated
    except SQLAlchemyError:
        return 0


def insert_operation_run(
    db_path: DatabaseTarget,
    *,
    operation_code: str,
    trigger_source: str,
    started_at_utc: str,
    status: str = OPERATION_RUN_STATUS_PENDING,
    request_payload: object | None = None,
    previous_state: object | None = None,
) -> int:
    started = _parse_iso_to_utc(started_at_utc)
    with _session_scope(db_path) as session:
        row = OperationRunRow(
            operation_code=str(operation_code or "").strip(),
            trigger_source=str(trigger_source or "manual_ui").strip() or "manual_ui",
            status=str(status or OPERATION_RUN_STATUS_PENDING).strip() or OPERATION_RUN_STATUS_PENDING,
            started_at_utc=started.isoformat(),
            started_at_epoch=started.timestamp(),
            updated_at_utc=started.isoformat(),
            updated_at_epoch=started.timestamp(),
            request_payload_json=_json_dumps(request_payload),
            previous_state_json=_json_dumps(previous_state),
        )
        session.add(row)
        session.flush()
        return int(row.id)


def update_operation_run(
    db_path: DatabaseTarget,
    *,
    run_id: int,
    status: str,
    updated_at_utc: str | None = None,
    latest_state: object | None = None,
    response_payload: object | None = None,
    error_text: str | None = None,
    completed: bool = False,
) -> int | None:
    if int(run_id or 0) <= 0:
        return None
    updated = _parse_iso_to_utc(updated_at_utc or datetime.now(UTC).isoformat())
    with _session_scope(db_path) as session:
        row = session.get(OperationRunRow, int(run_id))
        if row is None:
            return None
        row.status = str(status or row.status or OPERATION_RUN_STATUS_PENDING).strip() or OPERATION_RUN_STATUS_PENDING
        row.updated_at_utc = updated.isoformat()
        row.updated_at_epoch = updated.timestamp()
        row.latest_state_json = _json_dumps(latest_state) if latest_state is not None else row.latest_state_json
        row.response_payload_json = _json_dumps(response_payload) if response_payload is not None else row.response_payload_json
        row.error_text = str(error_text or "") or None
        if completed:
            row.completed_at_utc = updated.isoformat()
            row.completed_at_epoch = updated.timestamp()
        session.flush()
        return int(row.id)


def get_latest_operation_runs(
    db_path: DatabaseTarget,
    *,
    operation_codes: list[str] | tuple[str, ...],
    statuses: list[str] | tuple[str, ...] | None = None,
) -> dict[str, dict[str, object]]:
    codes = tuple(str(code or "").strip() for code in operation_codes if str(code or "").strip())
    if not codes:
        return {}
    normalized_statuses = tuple(str(status or "").strip() for status in (statuses or []) if str(status or "").strip())
    try:
        with _session_scope(db_path) as session:
            stmt = (
                select(OperationRunRow)
                .where(OperationRunRow.operation_code.in_(codes))
                .order_by(desc(OperationRunRow.started_at_epoch), desc(OperationRunRow.id))
            )
            if normalized_statuses:
                stmt = stmt.where(OperationRunRow.status.in_(normalized_statuses))
            rows = session.scalars(stmt).all()
            payload: dict[str, dict[str, object]] = {}
            for row in rows:
                operation_code = str(row.operation_code or "").strip()
                if not operation_code or operation_code in payload:
                    continue
                payload[operation_code] = {
                    "id": int(row.id),
                    "operation_code": operation_code,
                    "trigger_source": row.trigger_source,
                    "status": row.status,
                    "started_at_utc": row.started_at_utc,
                    "updated_at_utc": row.updated_at_utc,
                    "completed_at_utc": row.completed_at_utc,
                    "request_payload": _json_loads(row.request_payload_json, default=None),
                    "previous_state": _json_loads(row.previous_state_json, default=None),
                    "latest_state": _json_loads(row.latest_state_json, default=None),
                    "response_payload": _json_loads(row.response_payload_json, default=None),
                    "error_text": row.error_text,
                }
            return payload
    except SQLAlchemyError:
        return {}


def list_operation_runs(
    db_path: DatabaseTarget,
    *,
    limit: int = 200,
    statuses: list[str] | tuple[str, ...] | None = None,
) -> list[dict[str, object]]:
    safe_limit = max(1, min(int(limit or 200), 1000))
    if not _db_exists(db_path):
        return []
    normalized_statuses = tuple(
        str(item or "").strip()
        for item in (statuses or ())
        if str(item or "").strip()
    )
    try:
        with _session_scope(db_path) as session:
            stmt = (
                select(OperationRunRow, OperationDefinitionRow)
                .outerjoin(
                    OperationDefinitionRow,
                    OperationDefinitionRow.operation_code == OperationRunRow.operation_code,
                )
                .order_by(desc(OperationRunRow.started_at_epoch), desc(OperationRunRow.id))
                .limit(safe_limit)
            )
            if normalized_statuses:
                stmt = stmt.where(OperationRunRow.status.in_(normalized_statuses))
            rows = session.execute(stmt).all()
    except SQLAlchemyError:
        return []

    items: list[dict[str, object]] = []
    for run_row, definition_row in rows:
        title = (
            str((definition_row.title if definition_row is not None else "") or "").strip()
            or str(run_row.operation_code or "").strip()
            or "operation"
        )
        items.append(
            {
                "id": int(run_row.id),
                "operation_code": str(run_row.operation_code or "").strip(),
                "title": title,
                "system": str((definition_row.system if definition_row is not None else "") or "").strip(),
                "operation_type": str((definition_row.operation_type if definition_row is not None else "") or "").strip(),
                "action": str((definition_row.action if definition_row is not None else "") or "").strip(),
                "trigger_source": str(run_row.trigger_source or "").strip(),
                "status": str(run_row.status or "").strip(),
                "started_at_utc": run_row.started_at_utc,
                "started_at_epoch": run_row.started_at_epoch,
                "updated_at_utc": run_row.updated_at_utc,
                "updated_at_epoch": run_row.updated_at_epoch,
                "completed_at_utc": run_row.completed_at_utc,
                "completed_at_epoch": run_row.completed_at_epoch,
                "request_payload": _json_loads(run_row.request_payload_json, default=None),
                "response_payload": _json_loads(run_row.response_payload_json, default=None),
                "error_text": str(run_row.error_text or "").strip(),
            }
        )
    return items


def list_worker_api_logs(
    db_path: DatabaseTarget,
    *,
    page: int,
    page_size: int,
    worker: str | None = None,
    system: str | None = None,
    service: str | None = None,
    status: str | None = None,
    exclude_statuses: tuple[str, ...] | None = None,
) -> dict[str, object]:
    safe_page, safe_page_size = _paginate(page, page_size)
    if not _db_exists(db_path):
        return _empty_page(safe_page, safe_page_size)

    normalized_excluded_statuses = tuple(
        str(item or "").strip() for item in (exclude_statuses or ()) if str(item or "").strip()
    )
    try:
        with _session_scope(db_path) as session:
            stmt = select(WorkerApiLogRow)
            count_stmt = select(func.count()).select_from(WorkerApiLogRow)
            status_count_stmt = select(
                WorkerApiLogRow.status,
                func.count().label("count"),
            ).select_from(WorkerApiLogRow)
            base_filters = []
            if worker:
                base_filters.append(WorkerApiLogRow.worker == worker)
            if system:
                base_filters.append(WorkerApiLogRow.system == system)
            if service:
                base_filters.append(WorkerApiLogRow.service == service)
            if status:
                base_filters.append(WorkerApiLogRow.status == status)
            if base_filters:
                status_count_stmt = status_count_stmt.where(*base_filters)
            filters = list(base_filters)
            if normalized_excluded_statuses:
                filters.append(func.coalesce(WorkerApiLogRow.status, "").not_in(normalized_excluded_statuses))
            if filters:
                stmt = stmt.where(*filters)
                count_stmt = count_stmt.where(*filters)
            total = int(session.scalar(count_stmt) or 0)
            status_rows = session.execute(
                status_count_stmt.group_by(WorkerApiLogRow.status).order_by(WorkerApiLogRow.status.asc())
            ).all()
            rows = session.scalars(
                stmt.order_by(desc(WorkerApiLogRow.requested_at_epoch))
                .offset((safe_page - 1) * safe_page_size)
                .limit(safe_page_size)
            ).all()
    except SQLAlchemyError:
        return _empty_page(safe_page, safe_page_size)

    items = [
        {
            "id": int(row.id),
            "request_token": str(row.request_token or ""),
            "round_id": str(row.round_id or ""),
            "worker": str(row.worker),
            "system": str(row.system or ""),
            "service": str(row.service),
            "method": str(row.method),
            "api_link": str(row.api_link),
            "requested_at_utc": row.requested_at_utc,
            "requested_at_epoch": row.requested_at_epoch,
            "ok": bool(row.ok),
            "status": str(row.status or ""),
            "status_code": row.status_code,
            "duration_ms": row.duration_ms,
            "result_text": str(row.result_text or ""),
            "error_text": str(row.error_text or ""),
            "payload_json": _json_loads(row.payload_json, default=None),
        }
        for row in rows
    ]
    end = safe_page * safe_page_size
    return {
        "count": len(items),
        "total": total,
        "page": safe_page,
        "page_size": safe_page_size,
        "has_next": end < total,
        "has_prev": safe_page > 1,
        "items": items,
        "status_counts": [
            {
                "status": str(status_value or ""),
                "count": int(count_value or 0),
            }
            for status_value, count_value in status_rows
            if str(status_value or "").strip()
        ],
    }


def _worker_notification_list_from_payload(payload_obj: object) -> list[dict[str, object]]:
    payload = payload_obj if isinstance(payload_obj, dict) else {}
    notifications = payload.get("notifications")
    if isinstance(notifications, list):
        return [item for item in notifications if isinstance(item, dict)]
    notification = payload.get("notification")
    return [notification] if isinstance(notification, dict) else []


def _worker_notification_key(log_id: int, notification_index: int) -> str:
    return f"worker-log-{int(log_id)}-notification-{int(notification_index)}"


def _stable_notification_keys_from_dismissals(session: Session) -> set[str]:
    raw_keys = {
        str(row.notification_key or "").strip()
        for row in session.scalars(select(NotificationDismissalRow)).all()
    }
    stable_keys = {key for key in raw_keys if key and "::" in key}
    legacy_refs: list[tuple[int, int]] = []
    for key in raw_keys:
        match = re.fullmatch(r"worker-log-(\d+)-notification-(\d+)", key)
        if not match:
            continue
        legacy_refs.append((int(match.group(1)), int(match.group(2))))
    if not legacy_refs:
        return stable_keys
    log_ids = sorted({log_id for log_id, _ in legacy_refs})
    rows = {
        int(row.id): row
        for row in session.scalars(select(WorkerApiLogRow).where(WorkerApiLogRow.id.in_(log_ids))).all()
    }
    for log_id, notification_index in legacy_refs:
        row = rows.get(log_id)
        if row is None:
            continue
        payload = _json_loads(row.payload_json, default={})
        notification_list = _worker_notification_list_from_payload(payload)
        if 0 <= notification_index < len(notification_list):
            notification = notification_list[notification_index]
            if isinstance(notification, dict):
                stable_keys.add(
                    _normalize_notification_key(
                        notification.get("notification_key"),
                        code=notification.get("code"),
                        target=notification.get("target"),
                    )
                )
    return stable_keys


def list_active_notification_entries(
    db_path: DatabaseTarget,
    *,
    system: str,
    service: str,
    status: str | tuple[str, ...],
) -> list[dict[str, object]]:
    del system, service, status
    if not _db_exists(db_path):
        return []
    try:
        with _session_scope(db_path) as session:
            rows = session.scalars(
                select(NotificationEntryRow)
                .where(NotificationEntryRow.status == NOTIFICATION_STATUS_ACTIVE)
                .order_by(desc(NotificationEntryRow.last_seen_at_epoch))
            ).all()
    except SQLAlchemyError:
        return []

    return [
        {
            "notification_key": str(row.notification_key or ""),
            "status": str(row.status or NOTIFICATION_STATUS_ACTIVE),
            "log_id": int(row.last_log_id or 0),
            "requested_at_utc": str(row.last_seen_at_utc or ""),
            "requested_at_epoch": float(row.last_seen_at_epoch or 0.0),
            "notification": {
                "code": str(row.code or "unknown"),
                "target": str(row.target or ""),
                "level": str(row.level or "info"),
                "title": str(row.title or ""),
                "message": str(row.message or ""),
                "window": str(row.window or ""),
            },
            "payload_json": _json_loads(row.payload_json, default={}),
        }
        for row in rows
    ]


def upsert_notification_entries(
    db_path: DatabaseTarget,
    *,
    notifications: list[dict[str, object]],
    requested_at_utc: str,
    source_system: str,
    source_service: str,
    log_id: int | None = None,
) -> int:
    normalized_notifications = [item for item in notifications if isinstance(item, dict)]
    if not normalized_notifications:
        return 0
    seen_at = _parse_iso_to_utc(requested_at_utc)
    updated = 0
    try:
        with _session_scope(db_path) as session:
            for notification in normalized_notifications:
                code = str(notification.get("code") or "unknown").strip() or "unknown"
                target = str(notification.get("target") or "").strip()
                notification_key = _normalize_notification_key(
                    notification.get("notification_key"),
                    code=code,
                    target=target,
                )
                row = session.get(NotificationEntryRow, notification_key)
                level = str(notification.get("level") or "info").strip().lower() or "info"
                title = str(notification.get("title") or "").strip() or None
                message = str(notification.get("message") or code or "Notification").strip() or code
                trigger_text = str(notification.get("trigger_text") or "").strip() or None
                window = str(notification.get("window") or "").strip() or None
                payload_json = json.dumps(notification, ensure_ascii=False, default=str)
                if row is None:
                    row = NotificationEntryRow(
                        notification_key=notification_key,
                        code=code,
                        target=target,
                        level=level,
                        title=title,
                        message=message,
                        trigger_text=trigger_text,
                        window=window,
                        source_system=str(source_system or "").strip() or None,
                        source_service=str(source_service or "").strip() or None,
                        status=NOTIFICATION_STATUS_ACTIVE,
                        first_seen_at_utc=seen_at.isoformat(),
                        first_seen_at_epoch=seen_at.timestamp(),
                        last_seen_at_utc=seen_at.isoformat(),
                        last_seen_at_epoch=seen_at.timestamp(),
                        first_log_id=int(log_id) if log_id else None,
                        last_log_id=int(log_id) if log_id else None,
                        payload_json=payload_json,
                    )
                    session.add(row)
                    session.flush()
                else:
                    row.code = code
                    row.target = target
                    row.level = level
                    row.title = title
                    row.message = message
                    row.trigger_text = trigger_text
                    row.window = window
                    row.source_system = str(source_system or "").strip() or None
                    row.source_service = str(source_service or "").strip() or None
                    row.status = NOTIFICATION_STATUS_ACTIVE
                    row.last_seen_at_utc = seen_at.isoformat()
                    row.last_seen_at_epoch = seen_at.timestamp()
                    row.last_log_id = int(log_id) if log_id else row.last_log_id
                    row.dismissed_at_utc = None
                    row.dismissed_at_epoch = None
                    row.payload_json = payload_json
                updated += 1
        return updated
    except SQLAlchemyError:
        return 0


def backfill_notification_entries_from_worker_logs(
    db_path: DatabaseTarget,
    *,
    system: str,
    service: str,
    active_statuses: tuple[str, ...],
) -> int:
    if not _db_exists(db_path):
        return 0
    normalized_statuses = tuple(_normalize_worker_log_status(item) for item in active_statuses if item)
    normalized_statuses = tuple(item for item in normalized_statuses if item)
    if not normalized_statuses:
        return 0
    try:
        with _session_scope(db_path) as session:
            rows = session.scalars(
                select(WorkerApiLogRow)
                .where(
                    WorkerApiLogRow.system == system,
                    WorkerApiLogRow.service == service,
                    WorkerApiLogRow.status.in_(normalized_statuses),
                )
                .order_by(WorkerApiLogRow.requested_at_epoch.asc(), WorkerApiLogRow.id.asc())
            ).all()
            dismissed_keys = _stable_notification_keys_from_dismissals(session)
            updated = 0
            for row in rows:
                payload = _json_loads(row.payload_json, default={})
                notification_list = _worker_notification_list_from_payload(payload)
                seen_at = _parse_iso_to_utc(str(row.requested_at_utc or datetime.now(UTC).isoformat()))
                for notification in notification_list:
                    if not isinstance(notification, dict):
                        continue
                    code = str(notification.get("code") or "unknown").strip() or "unknown"
                    target = str(notification.get("target") or "").strip()
                    notification_key = _normalize_notification_key(
                        notification.get("notification_key"),
                        code=code,
                        target=target,
                    )
                    row_status = NOTIFICATION_STATUS_DISMISSED if notification_key in dismissed_keys else NOTIFICATION_STATUS_ACTIVE
                    entry = session.get(NotificationEntryRow, notification_key)
                    level = str(notification.get("level") or "info").strip().lower() or "info"
                    title = str(notification.get("title") or "").strip() or None
                    message = str(notification.get("message") or code or "Notification").strip() or code
                    trigger_text = str(notification.get("trigger_text") or "").strip() or None
                    window = str(notification.get("window") or "").strip() or None
                    payload_json = json.dumps(notification, ensure_ascii=False, default=str)
                    if entry is None:
                        entry = NotificationEntryRow(
                            notification_key=notification_key,
                            code=code,
                            target=target,
                            level=level,
                            title=title,
                            message=message,
                            trigger_text=trigger_text,
                            window=window,
                            source_system=str(system or "").strip() or None,
                            source_service=str(service or "").strip() or None,
                            status=row_status,
                            first_seen_at_utc=seen_at.isoformat(),
                            first_seen_at_epoch=seen_at.timestamp(),
                            last_seen_at_utc=seen_at.isoformat(),
                            last_seen_at_epoch=seen_at.timestamp(),
                            first_log_id=int(row.id),
                            last_log_id=int(row.id),
                            dismissed_at_utc=seen_at.isoformat() if row_status == NOTIFICATION_STATUS_DISMISSED else None,
                            dismissed_at_epoch=seen_at.timestamp() if row_status == NOTIFICATION_STATUS_DISMISSED else None,
                            payload_json=payload_json,
                        )
                        session.add(entry)
                        session.flush()
                    else:
                        if not entry.first_seen_at_utc or float(entry.first_seen_at_epoch or 0.0) > seen_at.timestamp():
                            entry.first_seen_at_utc = seen_at.isoformat()
                            entry.first_seen_at_epoch = seen_at.timestamp()
                            entry.first_log_id = int(row.id)
                        if float(entry.last_seen_at_epoch or 0.0) <= seen_at.timestamp():
                            entry.code = code
                            entry.target = target
                            entry.level = level
                            entry.title = title
                            entry.message = message
                            entry.trigger_text = trigger_text
                            entry.window = window
                            entry.source_system = str(system or "").strip() or None
                            entry.source_service = str(service or "").strip() or None
                            entry.status = row_status
                            entry.last_seen_at_utc = seen_at.isoformat()
                            entry.last_seen_at_epoch = seen_at.timestamp()
                            entry.last_log_id = int(row.id)
                            entry.dismissed_at_utc = seen_at.isoformat() if row_status == NOTIFICATION_STATUS_DISMISSED else None
                            entry.dismissed_at_epoch = seen_at.timestamp() if row_status == NOTIFICATION_STATUS_DISMISSED else None
                            entry.payload_json = payload_json
                    updated += 1
        return updated
    except SQLAlchemyError:
        return 0


def dismiss_notification_entry(
    db_path: DatabaseTarget,
    *,
    notification_key: str,
    dismissed_at_utc: str,
) -> bool:
    normalized_key = str(notification_key or "").strip()
    if not normalized_key:
        return False
    dismissed_at = _parse_iso_to_utc(dismissed_at_utc)
    try:
        with _session_scope(db_path) as session:
            notification_row = session.get(NotificationEntryRow, normalized_key)
            if notification_row is not None:
                notification_row.status = NOTIFICATION_STATUS_DISMISSED
                notification_row.dismissed_at_utc = dismissed_at.isoformat()
                notification_row.dismissed_at_epoch = dismissed_at.timestamp()
            row = session.get(NotificationDismissalRow, normalized_key)
            if row is None:
                row = NotificationDismissalRow(
                    notification_key=normalized_key,
                    dismissed_at_utc=dismissed_at.isoformat(),
                    dismissed_at_epoch=dismissed_at.timestamp(),
                )
                session.add(row)
            else:
                row.dismissed_at_utc = dismissed_at.isoformat()
                row.dismissed_at_epoch = dismissed_at.timestamp()
        return True
    except SQLAlchemyError:
        return False


def get_time_window_rule_states(
    db_path: DatabaseTarget,
    *,
    rule_codes: list[str] | tuple[str, ...],
    default_enabled: bool = True,
) -> dict[str, bool]:
    normalized_codes = [str(code or "").strip() for code in rule_codes if str(code or "").strip()]
    states = {code: bool(default_enabled) for code in normalized_codes}
    if not normalized_codes:
        return states
    if not _db_exists(db_path):
        return states
    try:
        with _session_scope(db_path) as session:
            rows = session.scalars(
                select(TimeWindowRuleStateRow).where(TimeWindowRuleStateRow.rule_code.in_(tuple(normalized_codes)))
            ).all()
    except SQLAlchemyError:
        return states
    for row in rows:
        code = str(row.rule_code or "").strip()
        if code:
            states[code] = bool(row.enabled)
    return states


def list_time_window_rule_state_rows(
    db_path: DatabaseTarget,
) -> list[dict[str, object]]:
    if not _db_exists(db_path):
        return []
    try:
        with _session_scope(db_path) as session:
            rows = session.scalars(
                select(TimeWindowRuleStateRow).order_by(desc(TimeWindowRuleStateRow.updated_at_epoch))
            ).all()
    except SQLAlchemyError:
        return []
    return [
        {
            "rule_code": str(row.rule_code or ""),
            "enabled": bool(row.enabled),
            "updated_at_utc": str(row.updated_at_utc or ""),
            "updated_at_epoch": float(row.updated_at_epoch or 0.0),
        }
        for row in rows
    ]


def upsert_time_window_rule_state(
    db_path: DatabaseTarget,
    *,
    rule_code: str,
    enabled: bool,
    updated_at_utc: str,
) -> dict[str, object] | None:
    normalized_code = str(rule_code or "").strip()
    if not normalized_code:
        return None
    updated_at = _parse_iso_to_utc(updated_at_utc)
    try:
        with _session_scope(db_path) as session:
            row = session.get(TimeWindowRuleStateRow, normalized_code)
            if row is None:
                row = TimeWindowRuleStateRow(
                    rule_code=normalized_code,
                    enabled=1 if enabled else 0,
                    updated_at_utc=updated_at.isoformat(),
                    updated_at_epoch=updated_at.timestamp(),
                )
                session.add(row)
                session.flush()
            else:
                row.enabled = 1 if enabled else 0
                row.updated_at_utc = updated_at.isoformat()
                row.updated_at_epoch = updated_at.timestamp()
        return {
            "rule_code": normalized_code,
            "enabled": bool(enabled),
            "updated_at_utc": updated_at.isoformat(),
            "updated_at_epoch": updated_at.timestamp(),
        }
    except SQLAlchemyError:
        return None


def insert_sample(db_path: DatabaseTarget, sample: EnergySample) -> None:
    assembled_at_utc = sample.assembled_at_utc.astimezone(UTC)
    with _session_scope(db_path) as session:
        session.add(
            AssembledFlowSnapshotRow(
                round_id=sample.round_id,
                system=sample.system,
                assembled_at_utc=assembled_at_utc.isoformat(),
                assembled_at_epoch=assembled_at_utc.timestamp(),
                source=sample.source,
                pv_w=sample.pv_w,
                grid_w=sample.grid_w,
                battery_w=sample.battery_w,
                load_w=sample.load_w,
                battery_soc_percent=sample.battery_soc_percent,
                inverter_status=sample.inverter_status,
                balance_w=sample.balance_w,
                flow_json=json.dumps(sample.flow, ensure_ascii=False),
            )
        )


def export_samples_csv(db_path: DatabaseTarget) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(CSV_HEADERS))
    writer.writeheader()

    if not _db_exists(db_path):
        return output.getvalue()

    try:
        with _session_scope(db_path) as session:
            rows = session.scalars(
                select(AssembledFlowSnapshotRow).order_by(AssembledFlowSnapshotRow.assembled_at_epoch.asc())
            ).all()
    except SQLAlchemyError:
        rows = []

    for row in rows:
        writer.writerow(
            {
                "system": row.system,
                "sampled_at_utc": row.assembled_at_utc,
                "sampled_at_epoch": row.assembled_at_epoch,
                "source": row.source,
                "pv_w": row.pv_w,
                "grid_w": row.grid_w,
                "battery_w": row.battery_w,
                "load_w": row.load_w,
                "battery_soc_percent": row.battery_soc_percent,
                "inverter_status": row.inverter_status,
                "balance_w": row.balance_w,
                "payload_json": row.flow_json,
            }
        )
    return output.getvalue()


def _csv_nullable_float(value: str | None) -> float | None:
    if value is None:
        return None
    text_value = value.strip()
    if not text_value:
        return None
    return float(text_value)


def import_samples_csv(
    db_path: DatabaseTarget,
    csv_text: str,
    *,
    replace_existing: bool = False,
) -> dict[str, object]:
    init_db(db_path)
    reader = csv.DictReader(io.StringIO(csv_text))
    fieldnames = tuple(reader.fieldnames or ())
    missing = [column for column in CSV_HEADERS if column not in fieldnames]
    if missing:
        raise ValueError(f"CSV missing required columns: {', '.join(missing)}")

    imported = 0
    with _session_scope(db_path) as session:
        if replace_existing:
            session.query(AssembledFlowSnapshotRow).delete()
        for row in reader:
            system = str(row.get("system") or "").strip().lower()
            if not system:
                raise ValueError("CSV row missing system")
            sampled_at_utc_text = str(row.get("sampled_at_utc") or "").strip()
            if not sampled_at_utc_text:
                raise ValueError("CSV row missing sampled_at_utc")
            sampled_at = _parse_iso_to_utc(sampled_at_utc_text)
            sampled_at_epoch = _csv_nullable_float(row.get("sampled_at_epoch"))
            if sampled_at_epoch is None:
                sampled_at_epoch = sampled_at.timestamp()
            source = str(row.get("source") or "").strip() or "imported_csv"
            payload_json = str(row.get("payload_json") or "").strip() or "{}"
            payload_obj = _json_loads(payload_json, default={})
            if not isinstance(payload_obj, dict):
                payload_json = "{}"

            session.add(
                AssembledFlowSnapshotRow(
                    round_id=f"import-{int(sampled_at_epoch)}-{system}",
                    system=system,
                    assembled_at_utc=sampled_at.isoformat(),
                    assembled_at_epoch=sampled_at_epoch,
                    source=source,
                    pv_w=_csv_nullable_float(row.get("pv_w")),
                    grid_w=_csv_nullable_float(row.get("grid_w")),
                    battery_w=_csv_nullable_float(row.get("battery_w")),
                    load_w=_csv_nullable_float(row.get("load_w")),
                    battery_soc_percent=_csv_nullable_float(row.get("battery_soc_percent")),
                    inverter_status=str(row.get("inverter_status") or "").strip() or None,
                    balance_w=_csv_nullable_float(row.get("balance_w")),
                    flow_json=payload_json,
                )
            )
            imported += 1
    return {"imported_rows": imported, "replaced": replace_existing}


def get_storage_status(db_path: DatabaseTarget, sample_interval_seconds: float) -> dict[str, object]:
    if not _db_exists(db_path):
        return {
            "db_path": str(db_path),
            "db_exists": False,
            "db_size_bytes": 0,
            "rows": 0,
            "rows_by_system": {},
            "last_sample_utc": None,
            "sample_interval_seconds": sample_interval_seconds,
            "samples_per_hour": round(3600.0 / sample_interval_seconds, 3),
            "samples_per_day_per_system": round(86400.0 / sample_interval_seconds, 3),
            "estimated_bytes_per_day_per_system": 0,
            "estimated_mb_per_day_per_system": 0.0,
            "estimated_bytes_per_day_total": 0,
            "estimated_mb_per_day_total": 0.0,
        }

    try:
        with _session_scope(db_path) as session:
            total_rows = int(session.scalar(select(func.count()).select_from(AssembledFlowSnapshotRow)) or 0)
            rows_by_system_rows = session.execute(
                select(
                    AssembledFlowSnapshotRow.system,
                    func.count().label("row_count"),
                )
                .group_by(AssembledFlowSnapshotRow.system)
                .order_by(AssembledFlowSnapshotRow.system.asc())
            ).all()
            rows_by_system = {str(row[0]): int(row[1]) for row in rows_by_system_rows}
            last_sample_utc = session.scalar(
                select(AssembledFlowSnapshotRow.assembled_at_utc)
                .order_by(desc(AssembledFlowSnapshotRow.assembled_at_epoch))
                .limit(1)
            )
    except SQLAlchemyError:
        total_rows = 0
        rows_by_system = {}
        last_sample_utc = None

    sqlite_path = _sqlite_file_path(db_path)
    db_size_bytes = sqlite_path.stat().st_size if sqlite_path and sqlite_path.exists() else 0
    bytes_per_row = (db_size_bytes / total_rows) if total_rows > 0 else 0
    samples_per_day_per_system = 86400.0 / sample_interval_seconds
    estimated_bytes_per_day_per_system = int(bytes_per_row * samples_per_day_per_system)
    active_systems = max(1, len(rows_by_system))
    estimated_bytes_per_day_total = estimated_bytes_per_day_per_system * active_systems
    return {
        "db_path": str(db_path),
        "db_exists": True,
        "db_size_bytes": db_size_bytes,
        "rows": total_rows,
        "rows_by_system": rows_by_system,
        "last_sample_utc": last_sample_utc,
        "sample_interval_seconds": sample_interval_seconds,
        "samples_per_hour": round(3600.0 / sample_interval_seconds, 3),
        "samples_per_day_per_system": round(samples_per_day_per_system, 3),
        "estimated_bytes_per_day_per_system": estimated_bytes_per_day_per_system,
        "estimated_mb_per_day_per_system": round(estimated_bytes_per_day_per_system / (1024 * 1024), 3),
        "estimated_bytes_per_day_total": estimated_bytes_per_day_total,
        "estimated_mb_per_day_total": round(estimated_bytes_per_day_total / (1024 * 1024), 3),
    }


def prune_detailed_data_older_than(
    db_path: DatabaseTarget,
    *,
    older_than_epoch: float,
    older_than_day_local: date | None = None,
) -> dict[str, object]:
    """Remove high-frequency history while retaining daily and monthly summaries."""
    cutoff = float(older_than_epoch)
    result = {
        "cutoff_epoch": cutoff,
        "raw_snapshot_rows_deleted": 0,
        "daily_sample_rows_deleted": 0,
        "daily_reports_compacted": 0,
        "monthly_reports_compacted": 0,
    }
    if not _db_exists(db_path):
        return result

    try:
        with _session_scope(db_path) as session:
            raw_deleted = session.execute(
                delete(AssembledFlowSnapshotRow).where(AssembledFlowSnapshotRow.assembled_at_epoch < cutoff)
            )
            daily_samples_deleted = session.execute(
                delete(DailyReportSampleRow).where(DailyReportSampleRow.sampled_at_epoch < cutoff)
            )
            result["raw_snapshot_rows_deleted"] = max(0, int(raw_deleted.rowcount or 0))
            result["daily_sample_rows_deleted"] = max(0, int(daily_samples_deleted.rowcount or 0))

            old_daily_reports = session.scalars(
                select(DailyReportRow).where(DailyReportRow.period_end_epoch <= cutoff)
            ).all()
            for report in old_daily_reports:
                if report.series_json == "{}" and report.zero_hero_json == "{}" and int(report.sample_count or 0) == 0:
                    continue
                report.series_json = "{}"
                report.zero_hero_json = "{}"
                report.sample_count = 0
                result["daily_reports_compacted"] = int(result["daily_reports_compacted"]) + 1

            monthly_reports = session.scalars(select(MonthlyReportRow)).all()
            for report in monthly_reports:
                payload = _json_loads(report.payload_json, default={})
                if not isinstance(payload, dict):
                    continue
                zero_hero = payload.get("zero_hero")
                history = zero_hero.get("history") if isinstance(zero_hero, dict) else None
                items = history.get("items") if isinstance(history, dict) else None
                if not isinstance(items, list) or not items:
                    continue
                retained_items = items
                if older_than_day_local is not None:
                    cutoff_day_text = older_than_day_local.isoformat()
                    retained_items = [
                        item
                        for item in items
                        if isinstance(item, dict) and str(item.get("date_local") or "") >= cutoff_day_text
                    ]
                elif float(report.period_end_epoch or 0.0) <= cutoff:
                    retained_items = []
                if len(retained_items) == len(items):
                    continue
                compact_history = dict(history)
                compact_history["items"] = retained_items
                compact_history["days"] = len(retained_items)
                compact_history["detail_retained"] = bool(retained_items)
                compact_history["detail_pruned_before_epoch"] = cutoff
                compact_history["detail_pruned_before_day_local"] = older_than_day_local.isoformat() if older_than_day_local else None
                compact_zero_hero = dict(zero_hero)
                compact_zero_hero["history"] = compact_history
                payload["zero_hero"] = compact_zero_hero
                report.payload_json = _json_dumps_full(payload) or "{}"
                result["monthly_reports_compacted"] = int(result["monthly_reports_compacted"]) + 1
    except SQLAlchemyError:
        return result
    return result


def vacuum_sqlite_database(db_path: DatabaseTarget) -> dict[str, object]:
    """Reclaim unused SQLite pages after a deliberate retention cleanup."""
    sqlite_path = _sqlite_file_path(db_path)
    if sqlite_path is None:
        raise ValueError("Database compaction only supports local SQLite files")
    if not sqlite_path.exists():
        return {"before_bytes": 0, "after_bytes": 0, "reclaimed_bytes": 0}

    def storage_bytes() -> int:
        paths = (sqlite_path, sqlite_path.with_name(f"{sqlite_path.name}-wal"))
        return sum(path.stat().st_size for path in paths if path.exists())

    dispose_db_connections(db_path)
    before_bytes = storage_bytes()
    with sqlite3.connect(str(sqlite_path), timeout=SQLITE_BUSY_TIMEOUT_SECONDS) as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
        free_pages = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        reclaimable_bytes_estimate = free_pages * page_size
        if reclaimable_bytes_estimate < SQLITE_VACUUM_MIN_RECLAIM_BYTES:
            after_bytes = storage_bytes()
            return {
                "before_bytes": before_bytes,
                "after_bytes": after_bytes,
                "reclaimed_bytes": max(0, before_bytes - after_bytes),
                "reclaimable_bytes_estimate": reclaimable_bytes_estimate,
                "skipped": True,
            }
        conn.execute("VACUUM")
    after_bytes = storage_bytes()
    return {
        "before_bytes": before_bytes,
        "after_bytes": after_bytes,
        "reclaimed_bytes": max(0, before_bytes - after_bytes),
        "reclaimable_bytes_estimate": reclaimable_bytes_estimate,
        "skipped": False,
    }


def _load_power_rows(
    db_path: DatabaseTarget,
    *,
    system: str,
    start_ts: float,
    end_ts: float,
) -> list[tuple[float, float | None, float | None, float | None, float | None, float | None, float | None]]:
    if not _db_exists(db_path):
        return []
    try:
        with _session_scope(db_path) as session:
            rows = session.execute(
                select(
                    AssembledFlowSnapshotRow.assembled_at_epoch,
                    AssembledFlowSnapshotRow.pv_w,
                    AssembledFlowSnapshotRow.grid_w,
                    AssembledFlowSnapshotRow.battery_w,
                    AssembledFlowSnapshotRow.load_w,
                    AssembledFlowSnapshotRow.flow_json,
                )
                .where(
                    AssembledFlowSnapshotRow.system == system,
                    AssembledFlowSnapshotRow.assembled_at_epoch >= start_ts,
                    AssembledFlowSnapshotRow.assembled_at_epoch < end_ts,
                )
                .order_by(AssembledFlowSnapshotRow.assembled_at_epoch.asc())
            ).all()
            result = []
            for row in rows:
                payload = _json_loads(row[5], default={})
                metrics = payload.get("metrics")
                metrics_map = metrics if isinstance(metrics, dict) else {}
                result.append(
                    (
                        float(row[0]),
                        row[1],
                        row[2],
                        row[3],
                        row[4],
                        metrics_map.get("tesla_charge_power_w"),
                        metrics_map.get("pool_pump_power_w"),
                    )
                )
            return result
    except SQLAlchemyError:
        return []


def compute_daily_usage(
    db_path: DatabaseTarget,
    *,
    system: str,
    target_day_utc: date,
    sample_interval_seconds: float,
) -> dict[str, object]:
    day_start = datetime.combine(target_day_utc, time.min, tzinfo=UTC)
    day_end = day_start + timedelta(days=1)
    rows = _load_power_rows(db_path, system=system, start_ts=day_start.timestamp(), end_ts=day_end.timestamp())
    expected = int(round(86400.0 / sample_interval_seconds))
    return _integrate_usage_rows(
        rows,
        system=system,
        start_label="day_utc",
        start_value=day_start.date().isoformat(),
        expected=expected,
        sample_interval_seconds=sample_interval_seconds,
    )


def _integrate_usage_rows(
    rows: list[tuple[float, float | None, float | None, float | None, float | None, float | None, float | None]],
    *,
    system: str,
    start_label: str,
    start_value: str,
    expected: int,
    sample_interval_seconds: float,
    end_label: str | None = None,
    end_value: str | None = None,
    local_timezone_name: str | None = None,
    grid_import_window_start_hour: int | None = None,
    grid_import_window_end_hour: int | None = None,
    grid_export_window_start_hour: int | None = None,
    grid_export_window_end_hour: int | None = None,
    exclude_tesla_load: bool = False,
    exclude_pool_pump_load: bool = False,
) -> dict[str, object]:
    try:
        local_tz = ZoneInfo(local_timezone_name) if local_timezone_name else datetime.now().astimezone().tzinfo
    except ZoneInfoNotFoundError:
        local_tz = datetime.now().astimezone().tzinfo
    import_window_start = grid_import_window_start_hour
    import_window_end = grid_import_window_end_hour
    export_window_start = grid_export_window_start_hour
    export_window_end = grid_export_window_end_hour

    def split_interval_by_local_window(start_ts: float, duration_seconds: float, start_hour: int | None, end_hour: int | None) -> tuple[float, float]:
        if (
            local_tz is None
            or start_hour is None
            or end_hour is None
            or duration_seconds <= 0
            or start_hour == end_hour
        ):
            return 0.0, max(duration_seconds, 0.0)
        interval_start = datetime.fromtimestamp(start_ts, UTC)
        interval_end = interval_start + timedelta(seconds=duration_seconds)
        current = interval_start
        window_seconds = 0.0
        while current < interval_end:
            local_current = current.astimezone(local_tz)
            next_midnight_local = datetime.combine(local_current.date() + timedelta(days=1), time.min, tzinfo=local_tz)
            window_start_local = datetime.combine(local_current.date(), time(hour=start_hour), tzinfo=local_tz)
            window_end_local = datetime.combine(local_current.date(), time(hour=end_hour), tzinfo=local_tz)
            segment_end = min(interval_end, next_midnight_local.astimezone(UTC))
            overlap_start = max(current, window_start_local.astimezone(UTC))
            overlap_end = min(segment_end, window_end_local.astimezone(UTC))
            if overlap_end > overlap_start:
                window_seconds += (overlap_end - overlap_start).total_seconds()
            current = segment_end
        window_seconds = max(0.0, min(duration_seconds, window_seconds))
        return window_seconds, max(0.0, duration_seconds - window_seconds)

    if len(rows) < 2:
        payload = {
            "system": system,
            start_label: start_value,
            "samples": len(rows),
            "expected_samples": expected,
            "coverage_ratio": round((len(rows) / expected), 4) if expected > 0 else None,
            "energy_kwh": {
                "home_load": 0.0,
                "other_home_load": 0.0,
                "tesla_load": 0.0,
                "pool_pump_load": 0.0,
                "solar_generation": 0.0,
                "grid_import": 0.0,
                "grid_export": 0.0,
                "battery_charge": 0.0,
                "battery_discharge": 0.0,
            },
            "breakdown_kwh": {
                "home_load": {
                    "total": 0.0,
                    "tesla": 0.0,
                    "pool_pump": 0.0,
                    "other": 0.0,
                },
                "grid_import": {
                    "window": 0.0,
                    "other": 0.0,
                },
                "grid_export": {
                    "window": 0.0,
                    "other": 0.0,
                }
            },
            "quality": {"note": "Not enough samples to integrate."},
        }
        if end_label and end_value is not None:
            payload[end_label] = end_value
        return payload

    home_load_wh = 0.0
    solar_wh = 0.0
    grid_import_wh = 0.0
    grid_import_window_wh = 0.0
    grid_import_other_wh = 0.0
    grid_export_wh = 0.0
    grid_export_window_wh = 0.0
    grid_export_other_wh = 0.0
    battery_charge_wh = 0.0
    battery_discharge_wh = 0.0
    observed_dts = [
        max(0.0, float(next_row[0]) - float(current_row[0]))
        for current_row, next_row in zip(rows, rows[1:])
        if float(next_row[0]) > float(current_row[0])
    ]
    observed_interval_seconds = statistics.median(observed_dts) if observed_dts else 0.0
    baseline_interval_seconds = max(float(sample_interval_seconds), float(observed_interval_seconds or 0.0))
    max_dt = baseline_interval_seconds * 2.5

    tesla_load_wh = 0.0
    pool_pump_load_wh = 0.0
    other_home_load_wh = 0.0

    for current_row, next_row in zip(rows, rows[1:]):
        ts, pv_w, grid_w, battery_w, load_w, tesla_charge_power_w, pool_pump_power_w = current_row
        next_ts = next_row[0]
        dt = max(0.0, float(next_ts) - float(ts))
        if dt <= 0:
            continue
        if dt > max_dt:
            dt = max_dt
        if load_w is not None and load_w > 0:
            raw_load_w = float(load_w)
            adjusted_load_w = raw_load_w
            tesla_w = max(float(tesla_charge_power_w or 0.0), 0.0)
            pool_pump_w = max(float(pool_pump_power_w or 0.0), 0.0)
            tesla_load_wh += tesla_w * dt / 3600.0
            pool_pump_load_wh += pool_pump_w * dt / 3600.0
            other_home_load_wh += max(raw_load_w - tesla_w - pool_pump_w, 0.0) * dt / 3600.0
            if exclude_tesla_load:
                adjusted_load_w -= tesla_w
            if exclude_pool_pump_load:
                adjusted_load_w -= pool_pump_w
            if adjusted_load_w < 0:
                adjusted_load_w = 0.0
            home_load_wh += adjusted_load_w * dt / 3600.0
        if pv_w is not None and pv_w > 0:
            solar_wh += float(pv_w) * dt / 3600.0
        if grid_w is not None:
            grid = float(grid_w)
            if grid > 0:
                grid_import_wh += grid * dt / 3600.0
                window_seconds, other_seconds = split_interval_by_local_window(float(ts), dt, import_window_start, import_window_end)
                grid_import_window_wh += grid * window_seconds / 3600.0
                grid_import_other_wh += grid * other_seconds / 3600.0
            elif grid < 0:
                grid_export_wh += (-grid) * dt / 3600.0
                window_seconds, other_seconds = split_interval_by_local_window(float(ts), dt, export_window_start, export_window_end)
                grid_export_window_wh += (-grid) * window_seconds / 3600.0
                grid_export_other_wh += (-grid) * other_seconds / 3600.0
        if battery_w is not None:
            battery = float(battery_w)
            if battery > 0:
                battery_discharge_wh += battery * dt / 3600.0
            elif battery < 0:
                battery_charge_wh += (-battery) * dt / 3600.0

    payload = {
        "system": system,
        start_label: start_value,
        "samples": len(rows),
        "expected_samples": expected,
        "coverage_ratio": round((len(rows) / expected), 4) if expected > 0 else None,
        "energy_kwh": {
            "home_load": round(home_load_wh / 1000.0, 4),
            "other_home_load": round(other_home_load_wh / 1000.0, 4),
            "tesla_load": round(tesla_load_wh / 1000.0, 4),
            "pool_pump_load": round(pool_pump_load_wh / 1000.0, 4),
            "solar_generation": round(solar_wh / 1000.0, 4),
            "grid_import": round(grid_import_wh / 1000.0, 4),
            "grid_export": round(grid_export_wh / 1000.0, 4),
            "battery_charge": round(battery_charge_wh / 1000.0, 4),
            "battery_discharge": round(battery_discharge_wh / 1000.0, 4),
        },
        "breakdown_kwh": {
            "home_load": {
                "total": round(home_load_wh / 1000.0, 4),
                "tesla": round(tesla_load_wh / 1000.0, 4),
                "pool_pump": round(pool_pump_load_wh / 1000.0, 4),
                "other": round(other_home_load_wh / 1000.0, 4),
            },
            "grid_import": {
                "window": round(grid_import_window_wh / 1000.0, 4),
                "other": round(grid_import_other_wh / 1000.0, 4),
            },
            "grid_export": {
                "window": round(grid_export_window_wh / 1000.0, 4),
                "other": round(grid_export_other_wh / 1000.0, 4),
            }
        },
        "quality": {
            "integration": "left-rectangle",
            "configured_interval_seconds": float(sample_interval_seconds),
            "observed_interval_seconds": round(observed_interval_seconds, 3) if observed_interval_seconds > 0 else None,
            "gap_clamp_seconds": max_dt,
        },
    }
    if end_label and end_value is not None:
        payload[end_label] = end_value
    return payload


def list_samples(
    db_path: DatabaseTarget,
    *,
    system: str | None,
    target_day_utc: date | None,
    start_at_utc: datetime | None = None,
    end_at_utc: datetime | None = None,
    page: int,
    page_size: int,
) -> dict[str, object]:
    safe_page, safe_page_size = _paginate(page, page_size)
    if not _db_exists(db_path):
        return _empty_page(safe_page, safe_page_size)

    try:
        with _session_scope(db_path) as session:
            stmt = select(AssembledFlowSnapshotRow)
            count_stmt = select(func.count()).select_from(AssembledFlowSnapshotRow)
            filters = []
            if system:
                filters.append(AssembledFlowSnapshotRow.system == system)
            if target_day_utc is not None:
                day_start = datetime.combine(target_day_utc, time.min, tzinfo=UTC)
                day_end = day_start + timedelta(days=1)
                filters.append(AssembledFlowSnapshotRow.assembled_at_epoch >= day_start.timestamp())
                filters.append(AssembledFlowSnapshotRow.assembled_at_epoch < day_end.timestamp())
            if start_at_utc is not None:
                filters.append(AssembledFlowSnapshotRow.assembled_at_epoch >= start_at_utc.astimezone(UTC).timestamp())
            if end_at_utc is not None:
                filters.append(AssembledFlowSnapshotRow.assembled_at_epoch < end_at_utc.astimezone(UTC).timestamp())
            if filters:
                stmt = stmt.where(*filters)
                count_stmt = count_stmt.where(*filters)
            total = int(session.scalar(count_stmt) or 0)
            rows = session.scalars(
                stmt.order_by(desc(AssembledFlowSnapshotRow.assembled_at_epoch))
                .offset((safe_page - 1) * safe_page_size)
                .limit(safe_page_size)
            ).all()
    except SQLAlchemyError:
        return _empty_page(safe_page, safe_page_size)

    items = [
        {
            "id": int(row.id),
            "system": str(row.system),
            "sampled_at_utc": row.assembled_at_utc,
            "source": str(row.source),
            "pv_w": row.pv_w,
            "grid_w": row.grid_w,
            "battery_w": row.battery_w,
            "load_w": row.load_w,
            "battery_soc_percent": row.battery_soc_percent,
            "inverter_status": row.inverter_status,
            "balance_w": row.balance_w,
        }
        for row in rows
    ]
    end = safe_page * safe_page_size
    return {
        "count": len(items),
        "total": total,
        "page": safe_page,
        "page_size": safe_page_size,
        "has_next": end < total,
        "has_prev": safe_page > 1,
        "items": items,
    }


def _phase_battery_local_timezone(local_timezone_name: str):
    try:
        return ZoneInfo(str(local_timezone_name or "").strip())
    except (ZoneInfoNotFoundError, ValueError):
        return UTC


def _phase_battery_usable_kwh(system: str, soc_percent: object) -> float | None:
    capacity_kwh = DAILY_REPORT_BATTERY_CAPACITY_KWH.get(system)
    if capacity_kwh is None:
        return None
    try:
        soc = float(soc_percent)
    except (TypeError, ValueError):
        return None
    min_discharge_soc = DAILY_REPORT_BATTERY_MIN_DISCHARGE_SOC_PERCENT.get(system, 0.0)
    usable_soc = max(0.0, min(100.0, soc) - min_discharge_soc)
    return round((capacity_kwh * usable_soc) / 100.0, 4)


def _nearest_phase_battery_sample(
    session: Session,
    *,
    system: str,
    target_epoch: float,
    tolerance_seconds: float,
) -> dict[str, object] | None:
    row = session.execute(
        select(
            AssembledFlowSnapshotRow.assembled_at_utc,
            AssembledFlowSnapshotRow.assembled_at_epoch,
            AssembledFlowSnapshotRow.battery_soc_percent,
        )
        .where(
            AssembledFlowSnapshotRow.system == system,
            AssembledFlowSnapshotRow.assembled_at_epoch >= target_epoch - tolerance_seconds,
            AssembledFlowSnapshotRow.assembled_at_epoch <= target_epoch + tolerance_seconds,
        )
        .order_by(func.abs(AssembledFlowSnapshotRow.assembled_at_epoch - target_epoch).asc())
        .limit(1)
    ).first()
    if row is None:
        return None
    return {
        "sampled_at_utc": row[0],
        "sampled_at_epoch": row[1],
        "battery_soc_percent": row[2],
    }


def build_daily_report_phase_battery(
    db_path: DatabaseTarget,
    *,
    report_day_local: date,
    local_timezone_name: str,
    tolerance_seconds: float = DAILY_REPORT_PHASE_BATTERY_SAMPLE_TOLERANCE_SECONDS,
) -> dict[str, object]:
    """Capture usable SAJ + Solplanet battery at each report stage end."""
    day_text = report_day_local.isoformat()
    local_tz = _phase_battery_local_timezone(local_timezone_name)
    safe_tolerance = max(0.0, float(tolerance_seconds))
    items: list[dict[str, object]] = []

    with _session_scope(db_path) as session:
        for stage in DAILY_REPORT_PHASE_BATTERY_STAGES:
            end_hour = int(stage["endHour"])
            target_local = datetime.combine(report_day_local, time(hour=end_hour), tzinfo=local_tz)
            target_utc = target_local.astimezone(UTC)
            target_epoch = target_utc.timestamp()
            saj_sample = _nearest_phase_battery_sample(
                session,
                system="saj",
                target_epoch=target_epoch,
                tolerance_seconds=safe_tolerance,
            )
            solplanet_sample = _nearest_phase_battery_sample(
                session,
                system="solplanet",
                target_epoch=target_epoch,
                tolerance_seconds=safe_tolerance,
            )
            saj_kwh = _phase_battery_usable_kwh("saj", saj_sample.get("battery_soc_percent") if saj_sample else None)
            solplanet_kwh = _phase_battery_usable_kwh(
                "solplanet",
                solplanet_sample.get("battery_soc_percent") if solplanet_sample else None,
            )
            values = [value for value in (saj_kwh, solplanet_kwh) if value is not None]
            items.append(
                {
                    "id": stage["id"],
                    "titleKey": stage["titleKey"],
                    "endHour": end_hour,
                    "endTime": f"{end_hour:02d}:00",
                    "target_at_local": target_local.isoformat(),
                    "target_at_utc": target_utc.isoformat(),
                    "total_kwh": round(sum(values), 4) if values else None,
                    "saj_kwh": saj_kwh,
                    "solplanet_kwh": solplanet_kwh,
                    "saj_soc_percent": saj_sample.get("battery_soc_percent") if saj_sample else None,
                    "solplanet_soc_percent": solplanet_sample.get("battery_soc_percent") if solplanet_sample else None,
                    "saj_sampled_at_utc": saj_sample.get("sampled_at_utc") if saj_sample else None,
                    "solplanet_sampled_at_utc": solplanet_sample.get("sampled_at_utc") if solplanet_sample else None,
                }
            )

    return {
        "day": day_text,
        "timezone_name": str(local_timezone_name or ""),
        "tolerance_seconds": safe_tolerance,
        "items": items,
    }


def save_daily_report(
    db_path: DatabaseTarget,
    *,
    report_day_local: date,
    timezone_name: str,
    period_start_utc: datetime,
    period_end_utc: datetime,
    generated_at_utc: datetime,
    usage_by_system: dict[str, object],
    series_by_system: dict[str, object],
    zero_hero_history: dict[str, object],
    phase_battery: dict[str, object] | None = None,
) -> dict[str, object]:
    """Replace one immutable daily report and its paginable sample snapshot."""
    day_text = report_day_local.isoformat()
    start_at = period_start_utc.astimezone(UTC)
    end_at = period_end_utc.astimezone(UTC)
    generated_at = generated_at_utc.astimezone(UTC)
    if start_at >= end_at:
        raise ValueError("period_start_utc must be earlier than period_end_utc")

    with _session_scope(db_path) as session:
        report = session.get(DailyReportRow, day_text)
        if report is None:
            report = DailyReportRow(report_day_local=day_text)
            session.add(report)
        report.timezone_name = str(timezone_name)
        report.period_start_utc = start_at.isoformat()
        report.period_start_epoch = start_at.timestamp()
        report.period_end_utc = end_at.isoformat()
        report.period_end_epoch = end_at.timestamp()
        report.generated_at_utc = generated_at.isoformat()
        report.generated_at_epoch = generated_at.timestamp()
        usage_payload = dict(usage_by_system or {})
        if phase_battery is not None:
            usage_payload["_phase_battery"] = phase_battery
        report.usage_json = _json_dumps_full(usage_payload) or "{}"
        report.series_json = _json_dumps_full(series_by_system) or "{}"
        report.zero_hero_json = _json_dumps_full(zero_hero_history) or "{}"

        session.execute(delete(DailyReportSampleRow).where(DailyReportSampleRow.report_day_local == day_text))
        source_rows = select(
            literal(day_text),
            AssembledFlowSnapshotRow.id,
            AssembledFlowSnapshotRow.system,
            AssembledFlowSnapshotRow.assembled_at_utc,
            AssembledFlowSnapshotRow.assembled_at_epoch,
            AssembledFlowSnapshotRow.source,
            AssembledFlowSnapshotRow.pv_w,
            AssembledFlowSnapshotRow.grid_w,
            AssembledFlowSnapshotRow.battery_w,
            AssembledFlowSnapshotRow.load_w,
            AssembledFlowSnapshotRow.battery_soc_percent,
            AssembledFlowSnapshotRow.inverter_status,
            AssembledFlowSnapshotRow.balance_w,
        ).where(
            AssembledFlowSnapshotRow.assembled_at_epoch >= start_at.timestamp(),
            AssembledFlowSnapshotRow.assembled_at_epoch < end_at.timestamp(),
        )
        session.execute(
            insert(DailyReportSampleRow).from_select(
                [
                    "report_day_local",
                    "source_snapshot_id",
                    "system",
                    "sampled_at_utc",
                    "sampled_at_epoch",
                    "source",
                    "pv_w",
                    "grid_w",
                    "battery_w",
                    "load_w",
                    "battery_soc_percent",
                    "inverter_status",
                    "balance_w",
                ],
                source_rows,
            )
        )
        report.sample_count = int(
            session.scalar(
                select(func.count()).select_from(DailyReportSampleRow).where(
                    DailyReportSampleRow.report_day_local == day_text
                )
            )
            or 0
        )
        sample_count = int(report.sample_count)

    return {
        "report_day_local": day_text,
        "generated_at_utc": generated_at.isoformat(),
        "sample_count": sample_count,
        "report_status": _daily_report_status(sample_count),
    }


def get_daily_report(
    db_path: DatabaseTarget,
    *,
    report_day_local: date,
    page: int,
    page_size: int,
) -> dict[str, object] | None:
    safe_page, safe_page_size = _paginate(page, page_size)
    if not _db_exists(db_path):
        return None
    day_text = report_day_local.isoformat()
    try:
        with _session_scope(db_path) as session:
            report = session.get(DailyReportRow, day_text)
            if report is None:
                return None
            total = int(report.sample_count or 0)
            rows = session.scalars(
                select(DailyReportSampleRow)
                .where(DailyReportSampleRow.report_day_local == day_text)
                .order_by(desc(DailyReportSampleRow.sampled_at_epoch))
                .offset((safe_page - 1) * safe_page_size)
                .limit(safe_page_size)
            ).all()
            usage_by_system = _json_loads(report.usage_json, default={})
            series_by_system = _json_loads(report.series_json, default={})
            zero_hero_history = _json_loads(report.zero_hero_json, default={})
            metadata = {
                "report_day_local": report.report_day_local,
                "timezone_name": report.timezone_name,
                "period_start_utc": report.period_start_utc,
                "period_end_utc": report.period_end_utc,
                "generated_at_utc": report.generated_at_utc,
                "report_status": _daily_report_status(total),
            }
    except SQLAlchemyError:
        return None

    phase_battery = None
    if isinstance(usage_by_system, dict):
        usage_by_system = dict(usage_by_system)
        phase_battery = usage_by_system.pop("_phase_battery", None)

    if not isinstance(series_by_system, dict) or not series_by_system:
        try:
            period_start = _parse_iso_to_utc(str(metadata.get("period_start_utc") or ""))
            period_end = _parse_iso_to_utc(str(metadata.get("period_end_utc") or ""))
        except ValueError:
            series_by_system = {}
        else:
            series_by_system = {
                system: get_series_samples(
                    db_path,
                    system=system,
                    hours=24,
                    max_points=500,
                    start_at_utc=period_start,
                    end_at_utc=period_end,
                )
                for system in ("saj", "solplanet", "combined")
            }

    items = [
        {
            "id": int(row.source_snapshot_id),
            "system": str(row.system),
            "sampled_at_utc": row.sampled_at_utc,
            "source": str(row.source),
            "pv_w": row.pv_w,
            "grid_w": row.grid_w,
            "battery_w": row.battery_w,
            "load_w": row.load_w,
            "battery_soc_percent": row.battery_soc_percent,
            "inverter_status": row.inverter_status,
            "balance_w": row.balance_w,
        }
        for row in rows
    ]
    end = safe_page * safe_page_size
    return {
        **metadata,
        "usage_by_system": usage_by_system if isinstance(usage_by_system, dict) else {},
        "series_by_system": series_by_system if isinstance(series_by_system, dict) else {},
        "zero_hero_history": zero_hero_history if isinstance(zero_hero_history, dict) else {},
        "phase_battery": phase_battery if isinstance(phase_battery, dict) else {},
        "samples": {
            "count": len(items),
            "total": total,
            "page": safe_page,
            "page_size": safe_page_size,
            "has_next": end < total,
            "has_prev": safe_page > 1,
            "items": items,
        },
    }


def list_daily_reports(
    db_path: DatabaseTarget,
    *,
    limit: int = 366,
    start_day_local: date | None = None,
    end_day_local: date | None = None,
) -> list[dict[str, object]]:
    if not _db_exists(db_path):
        return []
    safe_limit = max(1, min(3660, int(limit)))
    try:
        with _session_scope(db_path) as session:
            stmt = select(DailyReportRow)
            if start_day_local is not None:
                stmt = stmt.where(DailyReportRow.report_day_local >= start_day_local.isoformat())
            if end_day_local is not None:
                stmt = stmt.where(DailyReportRow.report_day_local < end_day_local.isoformat())
            rows = session.scalars(stmt.order_by(desc(DailyReportRow.report_day_local)).limit(safe_limit)).all()
    except SQLAlchemyError:
        return []
    return [
        {
            "report_day_local": str(row.report_day_local),
            "timezone_name": str(row.timezone_name),
            "generated_at_utc": str(row.generated_at_utc),
            "sample_count": int(row.sample_count or 0),
            "report_status": _daily_report_status(int(row.sample_count or 0)),
        }
        for row in rows
    ]


MONTHLY_REPORT_ENERGY_KEYS: tuple[str, ...] = (
    "home_load",
    "other_home_load",
    "tesla_load",
    "pool_pump_load",
    "solar_generation",
    "grid_import",
    "grid_export",
    "battery_charge",
    "battery_discharge",
)


def _add_months(month_start: date, offset: int) -> date:
    month_index = (month_start.year * 12) + (month_start.month - 1) + offset
    return date(month_index // 12, (month_index % 12) + 1, 1)


def _monthly_report_local_datetime(day: date, local_tz: ZoneInfo) -> datetime:
    return datetime.combine(day, time.min, tzinfo=local_tz)


def _monthly_report_round_kwh(value: object, digits: int = 4) -> float:
    try:
        number = float(value or 0.0)
    except (TypeError, ValueError):
        number = 0.0
    return round(number, digits)


def _monthly_report_number(value: object, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if number == number else default


def _monthly_report_empty_usage(system: str = "combined") -> dict[str, object]:
    return {
        "system": system,
        "samples": 0,
        "expected_samples": 0,
        "coverage_ratio": 0.0,
        "energy_kwh": {key: 0.0 for key in MONTHLY_REPORT_ENERGY_KEYS},
        "breakdown_kwh": {
            "home_load": {"total": 0.0, "tesla": 0.0, "pool_pump": 0.0, "other": 0.0},
            "grid_import": {"window": 0.0, "other": 0.0},
            "grid_export": {"window": 0.0, "other": 0.0},
        },
        "quality": {"note": "No samples available for this period."},
    }


def _monthly_report_sum_usages(usages: list[dict[str, object]], system: str = "combined") -> dict[str, object]:
    aggregate = _monthly_report_empty_usage(system)
    energy = aggregate["energy_kwh"]
    breakdown = aggregate["breakdown_kwh"]
    samples = 0
    expected_samples = 0

    for usage in usages:
        if not isinstance(usage, dict):
            continue
        samples += int(_monthly_report_number(usage.get("samples"), 0.0))
        expected_samples += int(_monthly_report_number(usage.get("expected_samples"), 0.0))
        usage_energy = usage.get("energy_kwh") if isinstance(usage.get("energy_kwh"), dict) else {}
        for key in MONTHLY_REPORT_ENERGY_KEYS:
            energy[key] = _monthly_report_round_kwh(_monthly_report_number(energy.get(key)) + _monthly_report_number(usage_energy.get(key)))
        usage_breakdown = usage.get("breakdown_kwh") if isinstance(usage.get("breakdown_kwh"), dict) else {}
        home_breakdown = usage_breakdown.get("home_load") if isinstance(usage_breakdown.get("home_load"), dict) else {}
        for key in ("total", "tesla", "pool_pump", "other"):
            breakdown["home_load"][key] = _monthly_report_round_kwh(
                _monthly_report_number(breakdown["home_load"].get(key)) + _monthly_report_number(home_breakdown.get(key))
            )
        import_breakdown = usage_breakdown.get("grid_import") if isinstance(usage_breakdown.get("grid_import"), dict) else {}
        export_breakdown = usage_breakdown.get("grid_export") if isinstance(usage_breakdown.get("grid_export"), dict) else {}
        for key in ("window", "other"):
            breakdown["grid_import"][key] = _monthly_report_round_kwh(
                _monthly_report_number(breakdown["grid_import"].get(key)) + _monthly_report_number(import_breakdown.get(key))
            )
            breakdown["grid_export"][key] = _monthly_report_round_kwh(
                _monthly_report_number(breakdown["grid_export"].get(key)) + _monthly_report_number(export_breakdown.get(key))
            )

    aggregate["samples"] = samples
    aggregate["expected_samples"] = expected_samples
    aggregate["coverage_ratio"] = round(samples / expected_samples, 4) if expected_samples > 0 else 0.0
    aggregate["quality"] = {
        "aggregation": "daily-sum",
        "source_days": len([usage for usage in usages if isinstance(usage, dict)]),
    }
    return aggregate


def _monthly_report_scale_usage(usage: dict[str, object], scale: float, system: str = "combined") -> dict[str, object]:
    safe_scale = max(0.0, float(scale or 0.0))
    scaled = _monthly_report_empty_usage(system)
    source_energy = usage.get("energy_kwh") if isinstance(usage.get("energy_kwh"), dict) else {}
    scaled_energy = scaled["energy_kwh"]
    for key in MONTHLY_REPORT_ENERGY_KEYS:
        scaled_energy[key] = _monthly_report_round_kwh(_monthly_report_number(source_energy.get(key)) * safe_scale)
    source_breakdown = usage.get("breakdown_kwh") if isinstance(usage.get("breakdown_kwh"), dict) else {}
    scaled_breakdown = scaled["breakdown_kwh"]
    for group, keys in (("home_load", ("total", "tesla", "pool_pump", "other")), ("grid_import", ("window", "other")), ("grid_export", ("window", "other"))):
        source_group = source_breakdown.get(group) if isinstance(source_breakdown.get(group), dict) else {}
        for key in keys:
            scaled_breakdown[group][key] = _monthly_report_round_kwh(_monthly_report_number(source_group.get(key)) * safe_scale)
    scaled["samples"] = int(round(_monthly_report_number(usage.get("samples")) * safe_scale))
    scaled["expected_samples"] = int(round(_monthly_report_number(usage.get("expected_samples")) * safe_scale))
    scaled["coverage_ratio"] = usage.get("coverage_ratio") if usage.get("coverage_ratio") is not None else 0.0
    scaled["quality"] = {"projection_scale": safe_scale}
    return scaled


def _monthly_report_energy_comparison(current: dict[str, object], baseline: dict[str, object]) -> dict[str, object]:
    current_energy = current.get("energy_kwh") if isinstance(current.get("energy_kwh"), dict) else {}
    baseline_energy = baseline.get("energy_kwh") if isinstance(baseline.get("energy_kwh"), dict) else {}
    energy_delta: dict[str, object] = {}
    for key in MONTHLY_REPORT_ENERGY_KEYS:
        cur = _monthly_report_number(current_energy.get(key))
        base = _monthly_report_number(baseline_energy.get(key))
        delta = cur - base
        energy_delta[key] = {
            "current": round(cur, 4),
            "baseline": round(base, 4),
            "delta": round(delta, 4),
            "percent_delta": round(delta / base * 100.0, 2) if abs(base) > 0.00001 else None,
        }
    return {
        "energy_kwh": energy_delta,
        "coverage_delta": round(
            _monthly_report_number(current.get("coverage_ratio")) - _monthly_report_number(baseline.get("coverage_ratio")),
            4,
        ),
    }


def _monthly_report_localized(en: str, zh: str) -> dict[str, str]:
    return {"en": en, "zh": zh}


def _monthly_report_kwh_text(value: object) -> str:
    return f"{_monthly_report_round_kwh(value, 2)} kWh"


def _monthly_report_percent_text(value: object) -> str:
    number = _monthly_report_number(value)
    return f"{round(number * 100.0, 1)}%"


def _load_monthly_daily_report_payloads(
    db_path: DatabaseTarget,
    *,
    start_day_local: date,
    end_day_local: date,
) -> dict[str, dict[str, object]]:
    if not _db_exists(db_path):
        return {}
    try:
        with _session_scope(db_path) as session:
            rows = session.scalars(
                select(DailyReportRow).where(
                    DailyReportRow.report_day_local >= start_day_local.isoformat(),
                    DailyReportRow.report_day_local < end_day_local.isoformat(),
                )
            ).all()
    except SQLAlchemyError:
        return {}

    payloads: dict[str, dict[str, object]] = {}
    for row in rows:
        usage_by_system = _json_loads(row.usage_json, default={})
        phase_battery = None
        if isinstance(usage_by_system, dict):
            usage_by_system = dict(usage_by_system)
            phase_battery = usage_by_system.pop("_phase_battery", None)
        payloads[str(row.report_day_local)] = {
            "usage_by_system": usage_by_system if isinstance(usage_by_system, dict) else {},
            "phase_battery": phase_battery if isinstance(phase_battery, dict) else {},
            "sample_count": int(row.sample_count or 0),
            "generated_at_utc": str(row.generated_at_utc),
        }
    return payloads


def _monthly_report_usage_for_period(
    db_path: DatabaseTarget,
    *,
    system: str,
    start_local: datetime,
    end_local: datetime,
    sample_intervals_by_system: dict[str, float],
    grid_import_window_start_hour: int | None,
    grid_import_window_end_hour: int | None,
    grid_export_window_start_hour: int | None,
    grid_export_window_end_hour: int | None,
    local_timezone_name: str,
) -> dict[str, object]:
    if end_local <= start_local:
        return _monthly_report_empty_usage(system)
    return compute_usage_between(
        db_path,
        system=system,
        start_at_utc=start_local.astimezone(UTC),
        end_at_utc=end_local.astimezone(UTC),
        sample_interval_seconds=float(sample_intervals_by_system.get(system) or DEFAULT_SAMPLE_INTERVAL_SECONDS),
        local_timezone_name=local_timezone_name,
        grid_import_window_start_hour=grid_import_window_start_hour,
        grid_import_window_end_hour=grid_import_window_end_hour,
        grid_export_window_start_hour=grid_export_window_start_hour,
        grid_export_window_end_hour=grid_export_window_end_hour,
    )


def _monthly_report_phase_item(day_payload: dict[str, object] | None, stage_id: str) -> dict[str, object] | None:
    phase = day_payload.get("phase_battery") if isinstance(day_payload, dict) else None
    items = phase.get("items") if isinstance(phase, dict) else None
    if not isinstance(items, list):
        return None
    for item in items:
        if isinstance(item, dict) and item.get("id") == stage_id:
            return item
    return None


def _monthly_report_build_recommendations(
    *,
    summary_usage: dict[str, object],
    daily_rows: list[dict[str, object]],
    zero_hero_totals: dict[str, object],
    days_in_month: int,
    observed_days: float,
) -> list[dict[str, object]]:
    recommendations: list[dict[str, object]] = []

    def add(
        rec_id: str,
        *,
        severity: str,
        confidence: str,
        title_en: str,
        title_zh: str,
        finding_en: str,
        finding_zh: str,
        action_en: str,
        action_zh: str,
        impact_en: str,
        impact_zh: str,
        evidence: list[dict[str, object]],
    ) -> None:
        recommendations.append(
            {
                "id": rec_id,
                "severity": severity,
                "confidence": confidence,
                "title": _monthly_report_localized(title_en, title_zh),
                "finding": _monthly_report_localized(finding_en, finding_zh),
                "action": _monthly_report_localized(action_en, action_zh),
                "impact": _monthly_report_localized(impact_en, impact_zh),
                "evidence": evidence,
            }
        )

    energy = summary_usage.get("energy_kwh") if isinstance(summary_usage.get("energy_kwh"), dict) else {}
    breakdown = summary_usage.get("breakdown_kwh") if isinstance(summary_usage.get("breakdown_kwh"), dict) else {}
    import_breakdown = breakdown.get("grid_import") if isinstance(breakdown.get("grid_import"), dict) else {}
    export_breakdown = breakdown.get("grid_export") if isinstance(breakdown.get("grid_export"), dict) else {}
    home_breakdown = breakdown.get("home_load") if isinstance(breakdown.get("home_load"), dict) else {}

    total_load = _monthly_report_number(energy.get("home_load"))
    solar = _monthly_report_number(energy.get("solar_generation"))
    grid_import = _monthly_report_number(energy.get("grid_import"))
    grid_export = _monthly_report_number(energy.get("grid_export"))
    paid_import = _monthly_report_number(import_breakdown.get("other"))
    free_import = _monthly_report_number(import_breakdown.get("window"))
    export_other = _monthly_report_number(export_breakdown.get("other"))
    tesla_load = _monthly_report_number(home_breakdown.get("tesla"))
    pool_pump_load = _monthly_report_number(home_breakdown.get("pool_pump"))
    flexible_load = tesla_load + pool_pump_load
    coverage_ratio = _monthly_report_number(summary_usage.get("coverage_ratio"))

    import_observed_windows = int(_monthly_report_number(zero_hero_totals.get("import_observed_windows")))
    net_import_windows = int(_monthly_report_number(zero_hero_totals.get("import_windows")))
    mixed_export_windows = int(_monthly_report_number(zero_hero_totals.get("mixed_export_windows")))
    hero_import_kwh = _monthly_report_number(zero_hero_totals.get("import_wh")) / 1000.0
    no_data_windows = int(_monthly_report_number(zero_hero_totals.get("no_data_windows")))

    if import_observed_windows > 0:
        add(
            "hero_window_import",
            severity="high" if net_import_windows > 0 or hero_import_kwh >= 0.5 else "medium",
            confidence="high" if coverage_ratio >= 0.8 else "medium",
            title_en="Hero Window import needs attention",
            title_zh="Hero Window 购电需要处理",
            finding_en=(
                f"This month has {import_observed_windows} Hero Window 5-minute windows with grid import, "
                f"including {net_import_windows} net-import windows."
            ),
            finding_zh=(
                f"本月 Hero Window 有 {import_observed_windows} 个 5 分钟窗口出现电网购电，"
                f"其中 {net_import_windows} 个是净购电窗口。"
            ),
            action_en="Keep more usable battery before 18:00, and avoid Tesla or pool-pump load between 17:00 and 20:00 unless export headroom is already secure.",
            action_zh="建议 18:00 前保留更多可用电池，并避免 17:00-20:00 安排 Tesla 或水泵运行，除非上网余量已经很充足。",
            impact_en=f"Estimated avoidable Hero Window import/loss: about {_monthly_report_kwh_text(hero_import_kwh)} before tariff pricing.",
            impact_zh=f"估算可减少的 Hero Window 购电/损失约 {_monthly_report_kwh_text(hero_import_kwh)}，后续接入电价后可换算金额。",
            evidence=[
                {"label": _monthly_report_localized("Import-observed windows", "出现购电窗口"), "value": import_observed_windows},
                {"label": _monthly_report_localized("Net-import windows", "净购电窗口"), "value": net_import_windows},
                {"label": _monthly_report_localized("Mixed export windows", "有购电但净上网窗口"), "value": mixed_export_windows},
                {"label": _monthly_report_localized("Hero import energy", "Hero 窗口购电量"), "value": _monthly_report_kwh_text(hero_import_kwh)},
            ],
        )

    if paid_import >= 5.0 and flexible_load >= 2.0:
        shiftable_kwh = min(paid_import, max(flexible_load * 0.35, min(flexible_load, paid_import) * 0.5))
        add(
            "move_flexible_load",
            severity="medium",
            confidence="medium",
            title_en="Move flexible load into free or solar-rich hours",
            title_zh="把可调负载搬到免费电或光伏强的时段",
            finding_en=(
                f"Paid-window grid import is {_monthly_report_kwh_text(paid_import)}, while Tesla and pool pump together used "
                f"{_monthly_report_kwh_text(flexible_load)}."
            ),
            finding_zh=(
                f"收费时段购电约 {_monthly_report_kwh_text(paid_import)}，Tesla 和水泵合计用电 "
                f"{_monthly_report_kwh_text(flexible_load)}。"
            ),
            action_en="Prefer 11:00-14:00 for pool pump and opportunistic Tesla charging, then use solar-surplus hours before drawing paid grid energy.",
            action_zh="优先把水泵和可机会充电的 Tesla 放到 11:00-14:00，其次使用光伏富余时段，减少收费时段购电。",
            impact_en=f"Potentially shift roughly {_monthly_report_kwh_text(shiftable_kwh)} away from paid import windows.",
            impact_zh=f"粗略估算可把约 {_monthly_report_kwh_text(shiftable_kwh)} 从收费购电时段迁走。",
            evidence=[
                {"label": _monthly_report_localized("Free-window import", "免费窗口购电"), "value": _monthly_report_kwh_text(free_import)},
                {"label": _monthly_report_localized("Paid-window import", "收费窗口购电"), "value": _monthly_report_kwh_text(paid_import)},
                {"label": _monthly_report_localized("Tesla + pool pump", "Tesla + 水泵"), "value": _monthly_report_kwh_text(flexible_load)},
            ],
        )

    usable_other_rows = [
        row
        for row in daily_rows
        if row.get("has_data") and _monthly_report_number(row.get("coverage_ratio")) >= 0.5
    ]
    other_values = [
        _monthly_report_number(((row.get("breakdown_kwh") or {}).get("home_load") or {}).get("other"))
        for row in usable_other_rows
    ]
    if len(other_values) >= 5:
        median_other = statistics.median(other_values)
        high_other_rows = [
            row
            for row in usable_other_rows
            if _monthly_report_number(((row.get("breakdown_kwh") or {}).get("home_load") or {}).get("other")) >= max(median_other * 1.35, median_other + 3.0)
        ]
        if high_other_rows:
            excess = sum(
                max(
                    0.0,
                    _monthly_report_number(((row.get("breakdown_kwh") or {}).get("home_load") or {}).get("other")) - median_other,
                )
                for row in high_other_rows
            )
            dates = ", ".join(str(row.get("date_local")) for row in high_other_rows[:4])
            add(
                "base_load_anomaly",
                severity="medium",
                confidence="medium",
                title_en="Base home load has abnormal high days",
                title_zh="家庭基础负载有异常偏高日期",
                finding_en=f"{len(high_other_rows)} days are well above the month's median non-Tesla/non-pump home load.",
                finding_zh=f"有 {len(high_other_rows)} 天的非 Tesla / 非水泵家庭基础用电明显高于本月中位数。",
                action_en="Check whether HVAC, hot water, dehumidifier, or always-on appliances were left running on those days.",
                action_zh="建议检查这些日期是否有空调、热水、除湿机或常开设备持续运行。",
                impact_en=f"Excess base load above the monthly median is about {_monthly_report_kwh_text(excess)}.",
                impact_zh=f"相对本月中位数的额外基础用电约 {_monthly_report_kwh_text(excess)}。",
                evidence=[
                    {"label": _monthly_report_localized("Median base load/day", "基础用电日中位数"), "value": _monthly_report_kwh_text(median_other)},
                    {"label": _monthly_report_localized("High days", "偏高日期"), "value": dates},
                    {"label": _monthly_report_localized("Estimated excess", "估算额外用电"), "value": _monthly_report_kwh_text(excess)},
                ],
            )

    weak_battery_days: list[dict[str, object]] = []
    for row in daily_rows:
        zero_summary = row.get("zero_hero_summary") if isinstance(row.get("zero_hero_summary"), dict) else {}
        phase_item = row.get("pre_hero_battery") if isinstance(row.get("pre_hero_battery"), dict) else None
        pre_hero_kwh = _monthly_report_number(phase_item.get("total_kwh"), -1.0) if phase_item else -1.0
        if pre_hero_kwh >= 0 and pre_hero_kwh < 6.0 and _monthly_report_number(zero_summary.get("import_observed_windows")) > 0:
            weak_battery_days.append(row)
    if weak_battery_days:
        weak_import_kwh = sum(
            _monthly_report_number(((row.get("zero_hero_summary") or {}).get("import_wh"))) / 1000.0
            for row in weak_battery_days
        )
        add(
            "pre_hero_battery_reserve",
            severity="high" if weak_import_kwh >= 0.5 else "medium",
            confidence="medium",
            title_en="Pre-Hero battery reserve is too low on import days",
            title_zh="出现购电的日期里，Hero 前电池余量偏低",
            finding_en=f"{len(weak_battery_days)} import-affected days had less than 6 kWh usable battery around 18:00.",
            finding_zh=f"有 {len(weak_battery_days)} 个受购电影响的日期在 18:00 左右可用电池低于 6 kWh。",
            action_en="Raise the pre-Hero reserve target or stop flexible loads earlier on days where the 18:00 usable battery forecast is below 6 kWh.",
            action_zh="建议在 18:00 预测可用电量低于 6 kWh 时，提高 Hero 前保电目标，或更早停止可调负载。",
            impact_en=f"Those low-reserve days account for about {_monthly_report_kwh_text(weak_import_kwh)} Hero import.",
            impact_zh=f"这些低余量日期对应的 Hero 购电约 {_monthly_report_kwh_text(weak_import_kwh)}。",
            evidence=[
                {"label": _monthly_report_localized("Low-reserve import days", "低余量且购电日期"), "value": len(weak_battery_days)},
                {"label": _monthly_report_localized("Reserve threshold", "余量阈值"), "value": "6 kWh"},
                {"label": _monthly_report_localized("Hero import on these days", "这些日期 Hero 购电"), "value": _monthly_report_kwh_text(weak_import_kwh)},
            ],
        )

    if solar >= 10.0 and export_other >= 10.0 and paid_import >= 5.0:
        shift_opportunity = min(export_other, paid_import) * 0.3
        self_consumption_ratio = max(0.0, min(1.0, (solar - grid_export) / solar)) if solar > 0 else 0.0
        add(
            "solar_self_consumption",
            severity="medium",
            confidence="medium",
            title_en="Solar surplus and paid import coexist",
            title_zh="光伏富余和收费购电同时存在",
            finding_en=(
                f"The month exported {_monthly_report_kwh_text(grid_export)} while still importing "
                f"{_monthly_report_kwh_text(paid_import)} outside the free window."
            ),
            finding_zh=(
                f"本月上网 {_monthly_report_kwh_text(grid_export)}，但免费窗口外仍购电 "
                f"{_monthly_report_kwh_text(paid_import)}。"
            ),
            action_en="Move discretionary load earlier into solar-rich hours before adding more battery cycling or paid import.",
            action_zh="建议先把可自由安排的负载前移到光伏强的时段，再考虑额外电池循环或收费购电。",
            impact_en=f"Practical shift opportunity is roughly {_monthly_report_kwh_text(shift_opportunity)} this month.",
            impact_zh=f"本月可实际迁移的用电机会粗估约 {_monthly_report_kwh_text(shift_opportunity)}。",
            evidence=[
                {"label": _monthly_report_localized("Solar self-consumption", "光伏自用率"), "value": _monthly_report_percent_text(self_consumption_ratio)},
                {"label": _monthly_report_localized("Export outside Hero window", "非 Hero 上网"), "value": _monthly_report_kwh_text(export_other)},
                {"label": _monthly_report_localized("Paid import", "收费购电"), "value": _monthly_report_kwh_text(paid_import)},
            ],
        )

    if coverage_ratio < 0.85 or no_data_windows >= 5:
        add(
            "data_quality",
            severity="medium" if coverage_ratio < 0.7 else "low",
            confidence="high",
            title_en="Data coverage limits recommendation accuracy",
            title_zh="数据覆盖率会影响建议准确性",
            finding_en=f"Energy coverage is {_monthly_report_percent_text(coverage_ratio)} and Hero Window no-data windows total {no_data_windows}.",
            finding_zh=f"电量统计覆盖率为 {_monthly_report_percent_text(coverage_ratio)}，Hero Window 无数据窗口共 {no_data_windows} 个。",
            action_en="Check collector uptime and inverter/API connectivity before tuning control thresholds from this report.",
            action_zh="在根据本报告调整控制阈值前，建议先检查采集器运行和逆变器/API 连接稳定性。",
            impact_en="Better coverage improves the confidence of cost and load-shifting estimates.",
            impact_zh="提高覆盖率后，电费估算和负载迁移建议会更可靠。",
            evidence=[
                {"label": _monthly_report_localized("Coverage", "覆盖率"), "value": _monthly_report_percent_text(coverage_ratio)},
                {"label": _monthly_report_localized("No-data Hero windows", "Hero 无数据窗口"), "value": no_data_windows},
            ],
        )

    if not recommendations and observed_days > 0:
        add(
            "monthly_stable",
            severity="info",
            confidence="medium",
            title_en="No major monthly issue detected",
            title_zh="本月暂未发现明显问题",
            finding_en="Hero Window import, paid import, base load anomalies, and data quality are within the current rule thresholds.",
            finding_zh="Hero Window 购电、收费购电、基础负载异常和数据质量都在当前规则阈值内。",
            action_en="Keep the current automation strategy and revisit after more data or tariff pricing is configured.",
            action_zh="可以先保持当前自动化策略，等数据更多或接入电价后再进一步优化。",
            impact_en="No immediate change is recommended.",
            impact_zh="当前没有必须立即调整的项目。",
            evidence=[
                {"label": _monthly_report_localized("Observed days", "已统计天数"), "value": round(observed_days, 1)},
                {"label": _monthly_report_localized("Month days", "本月天数"), "value": days_in_month},
            ],
        )

    severity_rank = {"high": 0, "medium": 1, "low": 2, "info": 3}
    return sorted(recommendations, key=lambda item: severity_rank.get(str(item.get("severity")), 9))


def build_monthly_report_analysis(
    db_path: DatabaseTarget,
    *,
    report_month_local: date,
    local_timezone_name: str,
    sample_intervals_by_system: dict[str, float],
    now_utc: datetime | None = None,
    grid_import_window_start_hour: int | None = None,
    grid_import_window_end_hour: int | None = None,
    grid_export_window_start_hour: int | None = None,
    grid_export_window_end_hour: int | None = None,
    zero_hero_window_minutes: int = 5,
) -> dict[str, object]:
    """Build a live monthly analysis report with comparisons, estimates, and rule-based advice."""
    try:
        local_tz = ZoneInfo(local_timezone_name) if local_timezone_name else datetime.now().astimezone().tzinfo
    except ZoneInfoNotFoundError:
        local_tz = datetime.now().astimezone().tzinfo
    if local_tz is None:
        local_tz = UTC

    month_start = date(report_month_local.year, report_month_local.month, 1)
    next_month_start = _add_months(month_start, 1)
    previous_month_start = _add_months(month_start, -1)
    month_start_local = _monthly_report_local_datetime(month_start, local_tz)
    next_month_start_local = _monthly_report_local_datetime(next_month_start, local_tz)
    previous_month_start_local = _monthly_report_local_datetime(previous_month_start, local_tz)

    effective_now_utc = now_utc.astimezone(UTC) if now_utc is not None else datetime.now(UTC)
    now_local = effective_now_utc.astimezone(local_tz)
    is_current_month = month_start <= now_local.date() < next_month_start
    is_future_month = month_start > now_local.date()
    period_end_local = month_start_local if is_future_month else min(next_month_start_local, now_local) if is_current_month else next_month_start_local
    generated_at = datetime.now(UTC)
    days_in_month = (next_month_start - month_start).days
    observed_seconds = max(0.0, (period_end_local - month_start_local).total_seconds())
    observed_days = round(observed_seconds / 86400.0, 4)

    report_payloads = _load_monthly_daily_report_payloads(
        db_path,
        start_day_local=month_start,
        end_day_local=next_month_start,
    )

    zero_days = 0
    if period_end_local > month_start_local:
        zero_anchor_day = (period_end_local - timedelta(microseconds=1)).date()
        zero_days = max(1, min(31, (zero_anchor_day - month_start).days + 1))
        zero_hero_history = list_zero_hero_window_history(
            db_path,
            system="combined",
            days=zero_days,
            sample_interval_seconds=float(sample_intervals_by_system.get("combined") or DEFAULT_SAMPLE_INTERVAL_SECONDS),
            local_timezone_name=local_timezone_name,
            recent_day_anchor_local=zero_anchor_day,
            now_utc=effective_now_utc if is_current_month else period_end_local.astimezone(UTC),
            window_start_hour=int(grid_export_window_start_hour or 18),
            window_end_hour=int(grid_export_window_end_hour or 20),
            window_minutes=zero_hero_window_minutes,
        )
    else:
        zero_hero_history = {
            "system": "combined",
            "timezone": str(local_tz),
            "days": 0,
            "window_start_hour": int(grid_export_window_start_hour or 18),
            "window_end_hour": int(grid_export_window_end_hour or 20),
            "window_minutes": zero_hero_window_minutes,
            "window_count_per_day": 0,
            "anchor_day_local": month_start.isoformat(),
            "items": [],
            "updated_at": generated_at.isoformat(),
        }
    zero_items = [item for item in zero_hero_history.get("items", []) if isinstance(item, dict)]
    zero_by_day = {str(item.get("date_local")): item for item in zero_items if item.get("date_local")}

    included_days: list[date] = []
    cursor = month_start
    while cursor < next_month_start:
        if _monthly_report_local_datetime(cursor, local_tz) >= period_end_local:
            break
        included_days.append(cursor)
        cursor += timedelta(days=1)

    daily_rows: list[dict[str, object]] = []
    daily_usages: list[dict[str, object]] = []
    for day_local in included_days:
        day_start_local = _monthly_report_local_datetime(day_local, local_tz)
        day_end_local = min(_monthly_report_local_datetime(day_local + timedelta(days=1), local_tz), period_end_local)
        day_text = day_local.isoformat()
        report_payload = report_payloads.get(day_text)
        reported_usage = None
        if day_end_local >= _monthly_report_local_datetime(day_local + timedelta(days=1), local_tz):
            usage_map = report_payload.get("usage_by_system") if isinstance(report_payload, dict) else None
            reported_usage = usage_map.get("combined") if isinstance(usage_map, dict) else None
        usage = reported_usage if isinstance(reported_usage, dict) else _monthly_report_usage_for_period(
            db_path,
            system="combined",
            start_local=day_start_local,
            end_local=day_end_local,
            sample_intervals_by_system=sample_intervals_by_system,
            grid_import_window_start_hour=grid_import_window_start_hour,
            grid_import_window_end_hour=grid_import_window_end_hour,
            grid_export_window_start_hour=grid_export_window_start_hour,
            grid_export_window_end_hour=grid_export_window_end_hour,
            local_timezone_name=local_timezone_name,
        )
        daily_usages.append(usage)
        zero_item = zero_by_day.get(day_text, {})
        zero_summary = zero_item.get("summary") if isinstance(zero_item.get("summary"), dict) else {}
        pre_hero_battery = _monthly_report_phase_item(report_payload, "after_free_shoulder")
        daily_rows.append(
            {
                "date_local": day_text,
                "is_partial": day_end_local < _monthly_report_local_datetime(day_local + timedelta(days=1), local_tz),
                "has_data": int(_monthly_report_number(usage.get("samples"))) >= 2,
                "samples": int(_monthly_report_number(usage.get("samples"))),
                "expected_samples": int(_monthly_report_number(usage.get("expected_samples"))),
                "coverage_ratio": usage.get("coverage_ratio"),
                "energy_kwh": usage.get("energy_kwh") if isinstance(usage.get("energy_kwh"), dict) else {},
                "breakdown_kwh": usage.get("breakdown_kwh") if isinstance(usage.get("breakdown_kwh"), dict) else {},
                "zero_hero_summary": zero_summary,
                "pre_hero_battery": pre_hero_battery or {},
                "source": "daily_report" if isinstance(reported_usage, dict) else "raw",
            }
        )

    summary_usage = _monthly_report_sum_usages(daily_usages, "combined")
    energy = summary_usage.get("energy_kwh") if isinstance(summary_usage.get("energy_kwh"), dict) else {}
    solar = _monthly_report_number(energy.get("solar_generation"))
    grid_import = _monthly_report_number(energy.get("grid_import"))
    grid_export = _monthly_report_number(energy.get("grid_export"))
    load = _monthly_report_number(energy.get("home_load"))
    self_consumption_ratio = max(0.0, min(1.0, (solar - grid_export) / solar)) if solar > 0 else None
    grid_dependence_ratio = max(0.0, grid_import / load) if load > 0 else None
    net_grid_kwh = grid_import - grid_export

    period_duration = period_end_local - month_start_local
    if period_duration.total_seconds() > 0:
        previous_period_end_local = min(previous_month_start_local + period_duration, month_start_local)
    else:
        previous_period_end_local = previous_month_start_local
    previous_usage = _monthly_report_usage_for_period(
        db_path,
        system="combined",
        start_local=previous_month_start_local,
        end_local=previous_period_end_local,
        sample_intervals_by_system=sample_intervals_by_system,
        grid_import_window_start_hour=grid_import_window_start_hour,
        grid_import_window_end_hour=grid_import_window_end_hour,
        grid_export_window_start_hour=grid_export_window_start_hour,
        grid_export_window_end_hour=grid_export_window_end_hour,
        local_timezone_name=local_timezone_name,
    )
    trailing_start_local = month_start_local - timedelta(days=28)
    trailing_usage = _monthly_report_usage_for_period(
        db_path,
        system="combined",
        start_local=trailing_start_local,
        end_local=month_start_local,
        sample_intervals_by_system=sample_intervals_by_system,
        grid_import_window_start_hour=grid_import_window_start_hour,
        grid_import_window_end_hour=grid_import_window_end_hour,
        grid_export_window_start_hour=grid_export_window_start_hour,
        grid_export_window_end_hour=grid_export_window_end_hour,
        local_timezone_name=local_timezone_name,
    )
    trailing_days = max(1.0, (month_start_local - trailing_start_local).total_seconds() / 86400.0)
    trailing_scaled = _monthly_report_scale_usage(trailing_usage, observed_days / trailing_days if observed_days > 0 else 0.0)

    scale_factor = (days_in_month / observed_days) if observed_days > 0 else 0.0
    projected_usage = _monthly_report_scale_usage(summary_usage, scale_factor if is_current_month else 1.0)

    zero_totals = {
        "import_windows": 0,
        "mixed_export_windows": 0,
        "import_observed_windows": 0,
        "export_windows": 0,
        "idle_windows": 0,
        "no_data_windows": 0,
        "pending_windows": 0,
        "partial_windows": 0,
        "import_wh": 0.0,
        "export_wh": 0.0,
    }
    for item in zero_items:
        summary = item.get("summary") if isinstance(item.get("summary"), dict) else {}
        for key in zero_totals:
            zero_totals[key] = _monthly_report_number(zero_totals.get(key)) + _monthly_report_number(summary.get(key))
    zero_totals = {
        key: round(value, 2) if key.endswith("_wh") else int(round(value))
        for key, value in zero_totals.items()
    }

    def top_rows(key_getter, limit: int = 5, *, positive_only: bool = True) -> list[dict[str, object]]:
        ranked = []
        for row in daily_rows:
            value = _monthly_report_number(key_getter(row))
            if positive_only and value <= 0:
                continue
            ranked.append((value, row))
        ranked.sort(key=lambda item: item[0], reverse=True)
        return [
            {
                "date_local": row.get("date_local"),
                "value": round(value, 4),
                "coverage_ratio": row.get("coverage_ratio"),
            }
            for value, row in ranked[:limit]
        ]

    solar_values = [
        _monthly_report_number((row.get("energy_kwh") or {}).get("solar_generation"))
        for row in daily_rows
        if row.get("has_data") and _monthly_report_number(row.get("coverage_ratio")) >= 0.5
    ]
    solar_median = statistics.median(solar_values) if solar_values else 0.0
    weak_solar_days = [
        {
            "date_local": row.get("date_local"),
            "value": _monthly_report_round_kwh((row.get("energy_kwh") or {}).get("solar_generation")),
            "baseline": _monthly_report_round_kwh(solar_median),
            "coverage_ratio": row.get("coverage_ratio"),
        }
        for row in daily_rows
        if solar_median > 0
        and row.get("has_data")
        and _monthly_report_number(row.get("coverage_ratio")) >= 0.5
        and _monthly_report_number((row.get("energy_kwh") or {}).get("solar_generation")) < solar_median * 0.65
    ][:5]
    hero_issue_days = [
        {
            "date_local": row.get("date_local"),
            "import_observed_windows": int(_monthly_report_number((row.get("zero_hero_summary") or {}).get("import_observed_windows"))),
            "import_wh": _monthly_report_round_kwh(_monthly_report_number((row.get("zero_hero_summary") or {}).get("import_wh")) / 1000.0),
            "pre_hero_battery_kwh": (row.get("pre_hero_battery") or {}).get("total_kwh"),
        }
        for row in daily_rows
        if _monthly_report_number((row.get("zero_hero_summary") or {}).get("import_observed_windows")) > 0
    ][:8]

    recommendations = _monthly_report_build_recommendations(
        summary_usage=summary_usage,
        daily_rows=daily_rows,
        zero_hero_totals=zero_totals,
        days_in_month=days_in_month,
        observed_days=observed_days,
    )

    return {
        "report_month_local": f"{month_start.year:04d}-{month_start.month:02d}",
        "timezone_name": local_timezone_name,
        "period_start_local": month_start_local.isoformat(),
        "period_end_local": period_end_local.isoformat(),
        "period_start_utc": month_start_local.astimezone(UTC).isoformat(),
        "period_end_utc": period_end_local.astimezone(UTC).isoformat(),
        "generated_at_utc": generated_at.isoformat(),
        "is_current_month": is_current_month,
        "is_complete_month": period_end_local >= next_month_start_local,
        "is_future_month": is_future_month,
        "days_in_month": days_in_month,
        "observed_days": observed_days,
        "observed_day_count": len(included_days),
        "summary": {
            "usage": summary_usage,
            "self_consumption_ratio": round(self_consumption_ratio, 4) if self_consumption_ratio is not None else None,
            "grid_dependence_ratio": round(grid_dependence_ratio, 4) if grid_dependence_ratio is not None else None,
            "net_grid_kwh": round(net_grid_kwh, 4),
            "source": "daily-report-plus-raw",
        },
        "projections": {
            "scale_factor": round(scale_factor if is_current_month else 1.0, 4),
            "month_end_estimate": projected_usage,
            "is_projection": is_current_month and observed_days > 0 and period_end_local < next_month_start_local,
        },
        "comparisons": {
            "previous_period": {
                "label": previous_month_start.strftime("%Y-%m"),
                "period_start_local": previous_month_start_local.isoformat(),
                "period_end_local": previous_period_end_local.isoformat(),
                "usage": previous_usage,
                "delta": _monthly_report_energy_comparison(summary_usage, previous_usage),
            },
            "trailing_4_week_average": {
                "period_start_local": trailing_start_local.isoformat(),
                "period_end_local": month_start_local.isoformat(),
                "usage": trailing_scaled,
                "delta": _monthly_report_energy_comparison(summary_usage, trailing_scaled),
            },
        },
        "zero_hero": {
            "history": zero_hero_history,
            "totals": zero_totals,
        },
        "daily": {
            "items": daily_rows,
        },
        "anomalies": {
            "top_import_days": top_rows(lambda row: (row.get("energy_kwh") or {}).get("grid_import")),
            "top_other_load_days": top_rows(lambda row: ((row.get("breakdown_kwh") or {}).get("home_load") or {}).get("other")),
            "weak_solar_days": weak_solar_days,
            "hero_issue_days": hero_issue_days,
        },
        "recommendations": recommendations,
    }


def save_monthly_report(
    db_path: DatabaseTarget,
    *,
    payload: dict[str, object],
    generated_at_utc: datetime | None = None,
) -> dict[str, object]:
    month_text = str(payload.get("report_month_local") or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}", month_text):
        raise ValueError("payload.report_month_local must be YYYY-MM")
    try:
        period_start = _parse_iso_to_utc(str(payload.get("period_start_utc") or ""))
        period_end = _parse_iso_to_utc(str(payload.get("period_end_utc") or ""))
    except ValueError as exc:
        raise ValueError("payload period_start_utc and period_end_utc are required") from exc
    if period_start >= period_end:
        raise ValueError("monthly report period_start_utc must be earlier than period_end_utc")
    generated_at = (generated_at_utc or datetime.now(UTC)).astimezone(UTC)
    stored_payload = dict(payload)
    stored_payload["generated_at_utc"] = generated_at.isoformat()
    stored_payload["saved_report"] = True
    stored_payload["source"] = "stored"
    observed_day_count = int(_monthly_report_number(stored_payload.get("observed_day_count"), 0.0))
    data_day_count = _monthly_report_data_day_count(stored_payload)
    report_status = _monthly_report_status(stored_payload)
    stored_payload["data_day_count"] = data_day_count
    stored_payload["report_status"] = report_status

    with _session_scope(db_path) as session:
        report = session.get(MonthlyReportRow, month_text)
        if report is None:
            report = MonthlyReportRow(report_month_local=month_text)
            session.add(report)
        report.timezone_name = str(stored_payload.get("timezone_name") or "")
        report.period_start_utc = period_start.isoformat()
        report.period_start_epoch = period_start.timestamp()
        report.period_end_utc = period_end.isoformat()
        report.period_end_epoch = period_end.timestamp()
        report.generated_at_utc = generated_at.isoformat()
        report.generated_at_epoch = generated_at.timestamp()
        report.observed_day_count = observed_day_count
        report.payload_json = _json_dumps_full(stored_payload) or "{}"

    return {
        "report_month_local": month_text,
        "generated_at_utc": generated_at.isoformat(),
        "observed_day_count": observed_day_count,
        "data_day_count": data_day_count,
        "report_status": report_status,
    }


def get_monthly_report(
    db_path: DatabaseTarget,
    *,
    report_month_local: date,
) -> dict[str, object] | None:
    if not _db_exists(db_path):
        return None
    month_text = f"{report_month_local.year:04d}-{report_month_local.month:02d}"
    try:
        with _session_scope(db_path) as session:
            report = session.get(MonthlyReportRow, month_text)
            if report is None:
                return None
            payload = _json_loads(report.payload_json, default={})
            metadata = {
                "report_month_local": report.report_month_local,
                "timezone_name": report.timezone_name,
                "period_start_utc": report.period_start_utc,
                "period_end_utc": report.period_end_utc,
                "generated_at_utc": report.generated_at_utc,
                "observed_day_count": int(report.observed_day_count or 0),
            }
    except SQLAlchemyError:
        return None

    if not isinstance(payload, dict):
        payload = {}
    payload = {**metadata, **payload}
    payload["data_day_count"] = _monthly_report_data_day_count(payload)
    payload["report_status"] = _monthly_report_status(payload)
    payload["saved_report"] = True
    payload["source"] = "stored"
    return payload


def list_monthly_reports(
    db_path: DatabaseTarget,
    *,
    limit: int = 120,
    start_month_local: date | None = None,
    end_month_local: date | None = None,
) -> list[dict[str, object]]:
    if not _db_exists(db_path):
        return []
    safe_limit = max(1, min(1200, int(limit)))
    try:
        with _session_scope(db_path) as session:
            stmt = select(MonthlyReportRow)
            if start_month_local is not None:
                stmt = stmt.where(MonthlyReportRow.report_month_local >= start_month_local.strftime("%Y-%m"))
            if end_month_local is not None:
                stmt = stmt.where(MonthlyReportRow.report_month_local < end_month_local.strftime("%Y-%m"))
            rows = session.scalars(stmt.order_by(desc(MonthlyReportRow.report_month_local)).limit(safe_limit)).all()
    except SQLAlchemyError:
        return []
    items: list[dict[str, object]] = []
    for row in rows:
        payload = _json_loads(row.payload_json, default={})
        items.append(
            {
                "report_month_local": str(row.report_month_local),
                "timezone_name": str(row.timezone_name),
                "generated_at_utc": str(row.generated_at_utc),
                "observed_day_count": int(row.observed_day_count or 0),
                "data_day_count": _monthly_report_data_day_count(payload),
                "report_status": _monthly_report_status(payload),
            }
        )
    return items


def get_latest_sample(db_path: DatabaseTarget, *, system: str) -> dict[str, object] | None:
    if not _db_exists(db_path):
        return None
    try:
        with _session_scope(db_path) as session:
            row = session.scalar(
                select(AssembledFlowSnapshotRow)
                .where(AssembledFlowSnapshotRow.system == system)
                .order_by(desc(AssembledFlowSnapshotRow.assembled_at_epoch))
                .limit(1)
            )
    except SQLAlchemyError:
        return None
    if row is None:
        return None

    payload = _json_loads(row.flow_json, default={})
    if not isinstance(payload, dict):
        payload = {}
    return {
        "system": system,
        "sampled_at_utc": row.assembled_at_utc,
        "source": row.source,
        "pv_w": row.pv_w,
        "grid_w": row.grid_w,
        "battery_w": row.battery_w,
        "load_w": row.load_w,
        "battery_soc_percent": row.battery_soc_percent,
        "inverter_status": row.inverter_status,
        "balance_w": row.balance_w,
        "payload": payload,
    }


def upsert_realtime_kv(db_path: DatabaseTarget, rows: list[tuple[str, str, str]]) -> None:
    if not rows:
        return
    with _session_scope(db_path) as session:
        for attribute, value_text, source in rows:
            existing = session.get(RealtimeKvRow, attribute)
            if existing is None:
                session.add(RealtimeKvRow(attribute=attribute, value=value_text, source=source))
                continue
            existing.value = value_text
            existing.source = source


def delete_realtime_kv_by_prefix(db_path: DatabaseTarget, *, prefix: str) -> int:
    if not _db_exists(db_path):
        return 0
    with _session_scope(db_path) as session:
        result = session.execute(
            text("DELETE FROM realtime_kv WHERE attribute LIKE :pattern"),
            {"pattern": f"{prefix}%"},
        )
        return int(result.rowcount or 0)


def get_realtime_kv_by_prefix(db_path: DatabaseTarget, *, prefix: str) -> dict[str, dict[str, object]]:
    if not _db_exists(db_path):
        return {}
    try:
        with _session_scope(db_path) as session:
            rows = session.scalars(
                select(RealtimeKvRow).where(RealtimeKvRow.attribute.like(f"{prefix}%"))
            ).all()
    except SQLAlchemyError:
        return {}

    data: dict[str, dict[str, object]] = {}
    for row in rows:
        parsed_value = _json_loads(row.value, default=row.value)
        data[str(row.attribute)] = {
            "value": parsed_value,
            "source": str(row.source or ""),
        }
    return data


def list_realtime_kv_rows(db_path: DatabaseTarget, *, prefix: str | None = None) -> list[dict[str, object]]:
    if not _db_exists(db_path):
        return []
    try:
        with _session_scope(db_path) as session:
            stmt = select(RealtimeKvRow).order_by(RealtimeKvRow.attribute.asc())
            if prefix:
                stmt = stmt.where(RealtimeKvRow.attribute.like(f"{prefix}%"))
            rows = session.scalars(stmt).all()
    except SQLAlchemyError:
        return []

    items: list[dict[str, object]] = []
    for row in rows:
        items.append(
            {
                "attribute": str(row.attribute),
                "value": _json_loads(row.value, default=row.value),
                "source": str(row.source or ""),
            }
        )
    return items


def get_series_samples(
    db_path: DatabaseTarget,
    *,
    system: str,
    hours: int,
    max_points: int,
    target_day_utc: date | None = None,
    start_at_utc: datetime | None = None,
    end_at_utc: datetime | None = None,
) -> dict[str, object]:
    safe_max_points = max(50, min(5000, int(max_points)))
    safe_hours = max(1, min(168, int(hours)))
    if start_at_utc is not None and end_at_utc is not None:
        start_at = start_at_utc.astimezone(UTC)
        end_at = end_at_utc.astimezone(UTC)
    elif target_day_utc is not None:
        start_at = datetime.combine(target_day_utc, time.min, tzinfo=UTC)
        end_at = start_at + timedelta(days=1)
    else:
        end_at = datetime.now(UTC)
        start_at = end_at - timedelta(hours=safe_hours)

    if not _db_exists(db_path):
        return {
            "system": system,
            "hours": safe_hours,
            "day_utc": target_day_utc.isoformat() if target_day_utc is not None else None,
            "count": 0,
            "items": [],
            "start_at_utc": start_at.isoformat(),
            "end_at_utc": end_at.isoformat(),
        }

    try:
        with _session_scope(db_path) as session:
            rows = session.execute(
                select(
                    AssembledFlowSnapshotRow.assembled_at_utc,
                    AssembledFlowSnapshotRow.assembled_at_epoch,
                    AssembledFlowSnapshotRow.pv_w,
                    AssembledFlowSnapshotRow.grid_w,
                    AssembledFlowSnapshotRow.battery_w,
                    AssembledFlowSnapshotRow.load_w,
                    AssembledFlowSnapshotRow.battery_soc_percent,
                    AssembledFlowSnapshotRow.flow_json,
                )
                .where(
                    AssembledFlowSnapshotRow.system == system,
                    AssembledFlowSnapshotRow.assembled_at_epoch >= start_at.timestamp(),
                    AssembledFlowSnapshotRow.assembled_at_epoch <= end_at.timestamp(),
                )
                .order_by(AssembledFlowSnapshotRow.assembled_at_epoch.asc())
            ).all()
    except SQLAlchemyError:
        rows = []

    if len(rows) > safe_max_points:
        step = max(1, len(rows) // safe_max_points)
        sampled_rows = list(rows[::step])
        if sampled_rows and sampled_rows[-1][1] != rows[-1][1]:
            sampled_rows.append(rows[-1])
        rows = sampled_rows

    items = []
    for row in rows:
        tesla_battery_soc_percent = None
        tesla_charge_power_w = None
        pool_pump_power_w = None
        pool_pump_power_stale = None
        pool_pump_power_last_updated = None
        pool_pump_available = None
        pool_pump_unavailable_reason = None
        if system == "combined":
            payload = _json_loads(row[7], default={})
            if isinstance(payload, dict):
                metrics = payload.get("metrics")
                if isinstance(metrics, dict):
                    tesla_battery_soc_percent = metrics.get("tesla_battery_soc_percent")
                    tesla_charge_power_w = metrics.get("tesla_charge_power_w")
                    pool_pump_power_w = metrics.get("pool_pump_power_w")
                    pool_pump_power_stale = metrics.get("pool_pump_power_stale")
                    pool_pump_available = metrics.get("pool_pump_available")
                    pool_pump_unavailable_reason = metrics.get("pool_pump_unavailable_reason")
                pool_pump_power_last_updated = _extract_pool_pump_power_last_updated(payload)
                if pool_pump_power_stale is None and pool_pump_power_last_updated:
                    pool_pump_power_stale = _compute_pool_pump_power_stale(
                        sampled_at_utc=row[0],
                        power_last_updated=pool_pump_power_last_updated,
                    )
                if tesla_battery_soc_percent is None:
                    display_items = payload.get("display", {}).get("items", {})
                    if isinstance(display_items, dict):
                        tesla_item = display_items.get("tesla_soc")
                        if isinstance(tesla_item, dict):
                            tesla_battery_soc_percent = tesla_item.get("value")
        items.append(
            {
                "sampled_at_utc": row[0],
                "sampled_at_epoch": row[1],
                "pv_w": row[2],
                "grid_w": row[3],
                "battery_w": row[4],
                "load_w": row[5],
                "battery_soc_percent": row[6],
                "tesla_battery_soc_percent": tesla_battery_soc_percent,
                "tesla_charge_power_w": tesla_charge_power_w,
                "pool_pump_power_w": pool_pump_power_w,
                "pool_pump_power_stale": pool_pump_power_stale,
                "pool_pump_power_last_updated": pool_pump_power_last_updated,
                "pool_pump_available": pool_pump_available,
                "pool_pump_unavailable_reason": pool_pump_unavailable_reason,
            }
        )
    return {
        "system": system,
        "hours": safe_hours,
        "day_utc": target_day_utc.isoformat() if target_day_utc is not None else None,
        "count": len(items),
        "items": items,
        "start_at_utc": start_at.isoformat(),
        "end_at_utc": end_at.isoformat(),
    }


def get_metric_rows(
    db_path: DatabaseTarget,
    *,
    system: str,
    start_at_utc: datetime,
    end_at_utc: datetime,
    metric_keys: list[str],
) -> dict[str, object]:
    start_at = start_at_utc.astimezone(UTC)
    end_at = end_at_utc.astimezone(UTC)
    normalized_keys = [str(key).strip() for key in metric_keys if str(key).strip()]
    if not _db_exists(db_path):
        return {
            "system": system,
            "count": 0,
            "items": [],
            "metric_keys": normalized_keys,
            "start_at_utc": start_at.isoformat(),
            "end_at_utc": end_at.isoformat(),
        }

    try:
        with _session_scope(db_path) as session:
            rows = session.execute(
                select(
                    AssembledFlowSnapshotRow.assembled_at_utc,
                    AssembledFlowSnapshotRow.assembled_at_epoch,
                    AssembledFlowSnapshotRow.flow_json,
                )
                .where(
                    AssembledFlowSnapshotRow.system == system,
                    AssembledFlowSnapshotRow.assembled_at_epoch >= start_at.timestamp(),
                    AssembledFlowSnapshotRow.assembled_at_epoch <= end_at.timestamp(),
                )
                .order_by(AssembledFlowSnapshotRow.assembled_at_epoch.asc())
            ).all()
    except SQLAlchemyError:
        rows = []

    items: list[dict[str, object]] = []
    for row in rows:
        payload = _json_loads(row[2], default={})
        metrics = payload.get("metrics")
        metrics_map = metrics if isinstance(metrics, dict) else {}
        pool_pump_power_last_updated = _extract_pool_pump_power_last_updated(payload)
        computed_pool_pump_power_stale = (
            _compute_pool_pump_power_stale(
                sampled_at_utc=row[0],
                power_last_updated=pool_pump_power_last_updated,
            )
            if pool_pump_power_last_updated
            else None
        )
        item = {
            "sampled_at_utc": row[0],
            "sampled_at_epoch": row[1],
        }
        for key in normalized_keys:
            value = metrics_map.get(key)
            if key == "pool_pump_power_stale" and value is None:
                value = computed_pool_pump_power_stale
            elif key == "pool_pump_power_last_updated" and value is None:
                value = pool_pump_power_last_updated
            item[key] = value
        items.append(item)

    return {
        "system": system,
        "count": len(items),
        "items": items,
        "metric_keys": normalized_keys,
        "start_at_utc": start_at.isoformat(),
        "end_at_utc": end_at.isoformat(),
    }


def compute_usage_between(
    db_path: DatabaseTarget,
    *,
    system: str,
    start_at_utc: datetime,
    end_at_utc: datetime,
    sample_interval_seconds: float,
    local_timezone_name: str | None = None,
    grid_import_window_start_hour: int | None = None,
    grid_import_window_end_hour: int | None = None,
    grid_export_window_start_hour: int | None = None,
    grid_export_window_end_hour: int | None = None,
    exclude_tesla_load: bool = False,
    exclude_pool_pump_load: bool = False,
) -> dict[str, object]:
    start_ts = start_at_utc.astimezone(UTC).timestamp()
    end_ts = end_at_utc.astimezone(UTC).timestamp()
    rows = _load_power_rows(db_path, system=system, start_ts=start_ts, end_ts=end_ts)
    duration_seconds = max(0.0, end_ts - start_ts)
    expected = int(round(duration_seconds / sample_interval_seconds)) if sample_interval_seconds > 0 else 0
    return _integrate_usage_rows(
        rows,
        system=system,
        start_label="start_at_utc",
        start_value=start_at_utc.isoformat(),
        end_label="end_at_utc",
        end_value=end_at_utc.isoformat(),
        expected=expected,
        sample_interval_seconds=sample_interval_seconds,
        local_timezone_name=local_timezone_name,
        grid_import_window_start_hour=grid_import_window_start_hour,
        grid_import_window_end_hour=grid_import_window_end_hour,
        grid_export_window_start_hour=grid_export_window_start_hour,
        grid_export_window_end_hour=grid_export_window_end_hour,
        exclude_tesla_load=exclude_tesla_load,
        exclude_pool_pump_load=exclude_pool_pump_load,
    )


def list_zero_hero_window_history(
    db_path: DatabaseTarget,
    *,
    system: str,
    days: int,
    sample_interval_seconds: float,
    local_timezone_name: str | None = None,
    recent_day_anchor_local: date | None = None,
    now_utc: datetime | None = None,
    window_start_hour: int = 18,
    window_end_hour: int = 20,
    window_minutes: int = 5,
) -> dict[str, object]:
    safe_days = max(1, min(31, int(days)))
    safe_window_minutes = max(1, min(60, int(window_minutes)))
    if window_start_hour == window_end_hour:
        total_window_minutes = 24 * 60
    elif window_end_hour > window_start_hour:
        total_window_minutes = (window_end_hour - window_start_hour) * 60
    else:
        total_window_minutes = ((24 - window_start_hour) + window_end_hour) * 60
    window_count_per_day = max(1, total_window_minutes // safe_window_minutes)

    try:
        local_tz = ZoneInfo(local_timezone_name) if local_timezone_name else datetime.now().astimezone().tzinfo
    except ZoneInfoNotFoundError:
        local_tz = datetime.now().astimezone().tzinfo
    if local_tz is None:
        local_tz = UTC

    effective_now_utc = now_utc.astimezone(UTC) if now_utc is not None else datetime.now(UTC)
    now_local = effective_now_utc.astimezone(local_tz)
    anchor_day = recent_day_anchor_local or now_local.date()

    def _window_start(day_local: date, index: int) -> datetime:
        start_minutes = window_start_hour * 60 + index * safe_window_minutes
        start_day = day_local + timedelta(days=start_minutes // (24 * 60))
        minute_of_day = start_minutes % (24 * 60)
        return datetime.combine(
            start_day,
            time(hour=minute_of_day // 60, minute=minute_of_day % 60),
            tzinfo=local_tz,
        )

    def _empty_window_payload(
        *,
        index: int,
        start_local: datetime,
        end_local: datetime,
        state: str,
        is_future: bool,
    ) -> dict[str, object]:
        window_seconds = max(0.0, (end_local - start_local).total_seconds())
        expected_samples = int(round(window_seconds / sample_interval_seconds)) if sample_interval_seconds > 0 else 0
        return {
            "index": index,
            "state": state,
            "is_future": is_future,
            "is_in_progress": False,
            "is_complete": False,
            "window_minutes": safe_window_minutes,
            "window_start_local": start_local.isoformat(),
            "window_end_local": end_local.isoformat(),
            "window_start_utc": start_local.astimezone(UTC).isoformat(),
            "window_end_utc": end_local.astimezone(UTC).isoformat(),
            "observed_end_local": None,
            "samples": 0,
            "expected_samples": expected_samples,
            "coverage_ratio": 0.0 if expected_samples > 0 else None,
            "grid_import_kwh": 0.0,
            "grid_import_wh": 0.0,
            "grid_export_kwh": 0.0,
            "grid_export_wh": 0.0,
            "has_import": False,
            "has_export": False,
            "breaks_zero_import": False,
            "has_partial_coverage": False,
        }

    items: list[dict[str, object]] = []
    for day_offset in range(safe_days):
        day_local = anchor_day - timedelta(days=day_offset)
        windows: list[dict[str, object]] = []
        summary = {
            "import_windows": 0,
            "mixed_export_windows": 0,
            "import_observed_windows": 0,
            "export_windows": 0,
            "idle_windows": 0,
            "no_data_windows": 0,
            "pending_windows": 0,
            "partial_windows": 0,
            "import_wh": 0.0,
            "export_wh": 0.0,
        }

        for window_index in range(window_count_per_day):
            start_local = _window_start(day_local, window_index)
            end_local = start_local + timedelta(minutes=safe_window_minutes)
            if now_local <= start_local:
                payload = _empty_window_payload(
                    index=window_index + 1,
                    start_local=start_local,
                    end_local=end_local,
                    state="pending",
                    is_future=True,
                )
                summary["pending_windows"] += 1
                windows.append(payload)
                continue

            observed_end_local = min(end_local, now_local)
            usage = compute_usage_between(
                db_path,
                system=system,
                start_at_utc=start_local.astimezone(UTC),
                end_at_utc=observed_end_local.astimezone(UTC),
                sample_interval_seconds=sample_interval_seconds,
                local_timezone_name=local_timezone_name,
            )
            energy_map = usage.get("energy_kwh") if isinstance(usage, dict) else {}
            observed_grid_import_kwh = float((energy_map or {}).get("grid_import") or 0.0)
            observed_grid_export_kwh = float((energy_map or {}).get("grid_export") or 0.0)
            samples = int(usage.get("samples") or 0) if isinstance(usage, dict) else 0
            expected_samples = int(usage.get("expected_samples") or 0) if isinstance(usage, dict) else 0
            coverage_ratio = float(usage.get("coverage_ratio") or 0.0) if isinstance(usage, dict) else 0.0
            has_import = observed_grid_import_kwh > 0.00001
            has_export = observed_grid_export_kwh > 0.00001
            net_import = observed_grid_import_kwh - observed_grid_export_kwh
            is_in_progress = observed_end_local < end_local
            is_complete = observed_end_local >= end_local
            has_partial_coverage = expected_samples > 0 and samples < expected_samples

            if samples < 2:
                state = "no_data"
                summary["no_data_windows"] += 1
            elif has_import and net_import > 0.00001:
                state = "import"
                summary["import_observed_windows"] += 1
                summary["import_windows"] += 1
            elif has_import:
                state = "mixed_export"
                summary["import_observed_windows"] += 1
                summary["mixed_export_windows"] += 1
            elif has_export:
                state = "export"
                summary["export_windows"] += 1
            else:
                state = "idle"
                summary["idle_windows"] += 1

            if has_partial_coverage:
                summary["partial_windows"] += 1
            summary["import_wh"] += observed_grid_import_kwh * 1000.0
            summary["export_wh"] += observed_grid_export_kwh * 1000.0

            windows.append(
                {
                    "index": window_index + 1,
                    "state": state,
                    "is_future": False,
                    "is_in_progress": is_in_progress,
                    "is_complete": is_complete,
                    "window_minutes": safe_window_minutes,
                    "window_start_local": start_local.isoformat(),
                    "window_end_local": end_local.isoformat(),
                    "window_start_utc": start_local.astimezone(UTC).isoformat(),
                    "window_end_utc": end_local.astimezone(UTC).isoformat(),
                    "observed_end_local": observed_end_local.isoformat(),
                    "samples": samples,
                    "expected_samples": expected_samples,
                    "coverage_ratio": round(coverage_ratio, 4) if expected_samples > 0 else None,
                    "grid_import_kwh": round(observed_grid_import_kwh, 5),
                    "grid_import_wh": round(observed_grid_import_kwh * 1000.0, 2),
                    "grid_export_kwh": round(observed_grid_export_kwh, 5),
                    "grid_export_wh": round(observed_grid_export_kwh * 1000.0, 2),
                    "has_import": has_import,
                    "has_export": has_export,
                    "breaks_zero_import": has_import,
                    "has_partial_coverage": has_partial_coverage,
                }
            )

        items.append(
            {
                "date_local": day_local.isoformat(),
                "is_today": day_local == now_local.date(),
                "window_count": window_count_per_day,
                "summary": {
                    "import_windows": int(summary["import_windows"]),
                    "mixed_export_windows": int(summary["mixed_export_windows"]),
                    "import_observed_windows": int(summary["import_observed_windows"]),
                    "export_windows": int(summary["export_windows"]),
                    "idle_windows": int(summary["idle_windows"]),
                    "no_data_windows": int(summary["no_data_windows"]),
                    "pending_windows": int(summary["pending_windows"]),
                    "partial_windows": int(summary["partial_windows"]),
                    "import_wh": round(summary["import_wh"], 2),
                    "export_wh": round(summary["export_wh"], 2),
                },
                "windows": windows,
            }
        )

    return {
        "system": system,
        "timezone": getattr(local_tz, "key", None) or str(local_tz),
        "days": safe_days,
        "window_start_hour": int(window_start_hour),
        "window_end_hour": int(window_end_hour),
        "window_minutes": safe_window_minutes,
        "window_count_per_day": window_count_per_day,
        "anchor_day_local": anchor_day.isoformat(),
        "updated_at": effective_now_utc.isoformat(),
        "items": items,
    }
