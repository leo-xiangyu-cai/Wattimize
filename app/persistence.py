from __future__ import annotations

import csv
import io
import json
import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import Column, Float, Integer, String, create_engine, desc, func, inspect, select, text
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


DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "energy_samples.sqlite3"
DEFAULT_SAMPLE_INTERVAL_SECONDS = 5.0
SQLITE_BUSY_TIMEOUT_SECONDS = 10.0
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


@dataclass(frozen=True)
class RawRequestResult:
    round_id: str
    system: str | None
    source: str
    endpoint: str
    method: str
    request_url: str
    requested_at_utc: datetime
    duration_ms: float | None
    ok: bool
    status_code: int | None
    error_text: str | None
    response_text: str | None
    response_json: object | None


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


class RawRequestResultRow(Base):
    __tablename__ = "raw_request_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    round_id = Column(String, nullable=False, index=True)
    system = Column(String)
    source = Column(String, nullable=False, index=True)
    endpoint = Column(String, nullable=False, index=True)
    method = Column(String, nullable=False)
    request_url = Column(String, nullable=False)
    requested_at_utc = Column(String, nullable=False)
    requested_at_epoch = Column(Float, nullable=False, index=True)
    duration_ms = Column(Float)
    ok = Column(Integer, nullable=False)
    status_code = Column(Integer)
    error_text = Column(String)
    response_text = Column(String)
    response_json = Column(String)


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
    with engine.connect() as conn:
        total = int(conn.execute(text(f"SELECT COUNT(*) FROM {quoted_table}")).scalar_one())
        rows = conn.execute(
            text(f"SELECT * FROM {quoted_table} LIMIT :limit OFFSET :offset"),
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

    inspector = inspect(engine)
    worker_columns = {column["name"] for column in inspector.get_columns("worker_api_logs")}
    with engine.begin() as conn:
        if _is_sqlite(db_path):
            conn.execute(text("PRAGMA journal_mode=WAL;"))
        if "request_token" not in worker_columns:
            conn.execute(text("ALTER TABLE worker_api_logs ADD COLUMN request_token VARCHAR"))
        if "round_id" not in worker_columns:
            conn.execute(text("ALTER TABLE worker_api_logs ADD COLUMN round_id VARCHAR"))
        if "status" not in worker_columns:
            conn.execute(text("ALTER TABLE worker_api_logs ADD COLUMN status VARCHAR"))
        if "payload_json" not in worker_columns:
            conn.execute(text("ALTER TABLE worker_api_logs ADD COLUMN payload_json VARCHAR"))
    notification_columns = {column["name"] for column in inspector.get_columns("notifications")}
    with engine.begin() as conn:
        if "title" not in notification_columns:
            conn.execute(text("ALTER TABLE notifications ADD COLUMN title VARCHAR"))


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


def _load_power_rows(
    db_path: DatabaseTarget,
    *,
    system: str,
    start_ts: float,
    end_ts: float,
) -> list[tuple[float, float | None, float | None, float | None, float | None]]:
    if not _db_exists(db_path):
        return []
    try:
        with _session_scope(db_path) as session:
            return [
                (float(row[0]), row[1], row[2], row[3], row[4])
                for row in session.execute(
                    select(
                        AssembledFlowSnapshotRow.assembled_at_epoch,
                        AssembledFlowSnapshotRow.pv_w,
                        AssembledFlowSnapshotRow.grid_w,
                        AssembledFlowSnapshotRow.battery_w,
                        AssembledFlowSnapshotRow.load_w,
                    )
                    .where(
                        AssembledFlowSnapshotRow.system == system,
                        AssembledFlowSnapshotRow.assembled_at_epoch >= start_ts,
                        AssembledFlowSnapshotRow.assembled_at_epoch < end_ts,
                    )
                    .order_by(AssembledFlowSnapshotRow.assembled_at_epoch.asc())
                ).all()
            ]
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
    rows: list[tuple[float, float | None, float | None, float | None, float | None]],
    *,
    system: str,
    start_label: str,
    start_value: str,
    expected: int,
    sample_interval_seconds: float,
    end_label: str | None = None,
    end_value: str | None = None,
) -> dict[str, object]:
    if len(rows) < 2:
        payload = {
            "system": system,
            start_label: start_value,
            "samples": len(rows),
            "expected_samples": expected,
            "coverage_ratio": round((len(rows) / expected), 4) if expected > 0 else None,
            "energy_kwh": {
                "home_load": 0.0,
                "solar_generation": 0.0,
                "grid_import": 0.0,
                "grid_export": 0.0,
                "battery_charge": 0.0,
                "battery_discharge": 0.0,
            },
            "quality": {"note": "Not enough samples to integrate."},
        }
        if end_label and end_value is not None:
            payload[end_label] = end_value
        return payload

    home_load_wh = 0.0
    solar_wh = 0.0
    grid_import_wh = 0.0
    grid_export_wh = 0.0
    battery_charge_wh = 0.0
    battery_discharge_wh = 0.0
    max_dt = sample_interval_seconds * 2.5

    for current_row, next_row in zip(rows, rows[1:]):
        ts, pv_w, grid_w, battery_w, load_w = current_row
        next_ts = next_row[0]
        dt = max(0.0, float(next_ts) - float(ts))
        if dt <= 0:
            continue
        if dt > max_dt:
            dt = max_dt
        if load_w is not None and load_w > 0:
            home_load_wh += float(load_w) * dt / 3600.0
        if pv_w is not None and pv_w > 0:
            solar_wh += float(pv_w) * dt / 3600.0
        if grid_w is not None:
            grid = float(grid_w)
            if grid > 0:
                grid_import_wh += grid * dt / 3600.0
            elif grid < 0:
                grid_export_wh += (-grid) * dt / 3600.0
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
            "solar_generation": round(solar_wh / 1000.0, 4),
            "grid_import": round(grid_import_wh / 1000.0, 4),
            "grid_export": round(grid_export_wh / 1000.0, 4),
            "battery_charge": round(battery_charge_wh / 1000.0, 4),
            "battery_discharge": round(battery_discharge_wh / 1000.0, 4),
        },
        "quality": {
            "integration": "left-rectangle",
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


def insert_raw_request_result(
    db_path: DatabaseTarget,
    *,
    round_id: str,
    system: str | None,
    source: str,
    endpoint: str,
    method: str,
    request_url: str,
    requested_at_utc: str,
    duration_ms: float | None,
    ok: bool,
    status_code: int | None,
    error_text: str | None,
    response_text: str | None,
    response_json: object | None,
) -> None:
    requested = _parse_iso_to_utc(requested_at_utc)
    response_json_text = None if response_json is None else json.dumps(response_json, ensure_ascii=False)
    with _session_scope(db_path) as session:
        session.add(
            RawRequestResultRow(
                round_id=round_id,
                system=str(system or "") or None,
                source=source,
                endpoint=endpoint,
                method=method,
                request_url=request_url,
                requested_at_utc=requested.isoformat(),
                requested_at_epoch=requested.timestamp(),
                duration_ms=duration_ms,
                ok=1 if ok else 0,
                status_code=status_code,
                error_text=error_text,
                response_text=_truncate_result_text(response_text),
                response_json=response_json_text,
            )
        )


def get_latest_raw_request_result(
    db_path: DatabaseTarget,
    *,
    source: str,
    endpoint: str,
) -> dict[str, object] | None:
    if not _db_exists(db_path):
        return None
    try:
        with _session_scope(db_path) as session:
            row = session.scalar(
                select(RawRequestResultRow)
                .where(
                    RawRequestResultRow.source == source,
                    RawRequestResultRow.endpoint == endpoint,
                )
                .order_by(desc(RawRequestResultRow.requested_at_epoch))
                .limit(1)
            )
    except SQLAlchemyError:
        return None
    if row is None:
        return None

    payload = _json_loads(row.response_json, default=None)
    return {
        "round_id": row.round_id,
        "system": row.system,
        "endpoint": endpoint,
        "path": row.request_url,
        "requested_at_utc": row.requested_at_utc,
        "fetch_ms": row.duration_ms,
        "ok": bool(row.ok),
        "status_code": row.status_code,
        "error": row.error_text,
        "payload": payload,
        "response_text": row.response_text,
        "last_success_at_utc": None,
    }


def list_raw_request_results(
    db_path: DatabaseTarget,
    *,
    page: int,
    page_size: int,
    round_id: str | None = None,
    system: str | None = None,
    source: str | None = None,
) -> dict[str, object]:
    safe_page, safe_page_size = _paginate(page, page_size)
    if not _db_exists(db_path):
        return _empty_page(safe_page, safe_page_size)

    try:
        with _session_scope(db_path) as session:
            stmt = select(RawRequestResultRow)
            count_stmt = select(func.count()).select_from(RawRequestResultRow)
            filters = []
            if round_id:
                filters.append(RawRequestResultRow.round_id == round_id)
            if system:
                filters.append(RawRequestResultRow.system == system)
            if source:
                filters.append(RawRequestResultRow.source == source)
            if filters:
                stmt = stmt.where(*filters)
                count_stmt = count_stmt.where(*filters)
            total = int(session.scalar(count_stmt) or 0)
            rows = session.scalars(
                stmt.order_by(desc(RawRequestResultRow.requested_at_epoch))
                .offset((safe_page - 1) * safe_page_size)
                .limit(safe_page_size)
            ).all()
    except SQLAlchemyError:
        return _empty_page(safe_page, safe_page_size)

    items: list[dict[str, object]] = []
    for row in rows:
        items.append(
            {
                "id": int(row.id),
                "round_id": str(row.round_id),
                "system": str(row.system or ""),
                "source": str(row.source),
                "endpoint": str(row.endpoint),
                "method": str(row.method),
                "request_url": str(row.request_url),
                "requested_at_utc": row.requested_at_utc,
                "duration_ms": row.duration_ms,
                "ok": bool(row.ok),
                "status_code": row.status_code,
                "error_text": str(row.error_text or ""),
                "response_json": _json_loads(row.response_json, default=None),
            }
        )
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

    items = [
        {
            "sampled_at_utc": row[0],
            "sampled_at_epoch": row[1],
            "pv_w": row[2],
            "grid_w": row[3],
            "battery_w": row[4],
            "load_w": row[5],
        }
        for row in rows
    ]
    return {
        "system": system,
        "hours": safe_hours,
        "day_utc": target_day_utc.isoformat() if target_day_utc is not None else None,
        "count": len(items),
        "items": items,
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
    )
