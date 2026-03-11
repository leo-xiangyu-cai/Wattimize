from __future__ import annotations

import csv
import io
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path


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


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS assembled_flow_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_id TEXT NOT NULL,
                system TEXT NOT NULL,
                assembled_at_utc TEXT NOT NULL,
                assembled_at_epoch REAL NOT NULL,
                source TEXT NOT NULL,
                pv_w REAL,
                grid_w REAL,
                battery_w REAL,
                load_w REAL,
                battery_soc_percent REAL,
                inverter_status TEXT,
                balance_w REAL,
                flow_json TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_assembled_flow_snapshots_system_epoch
            ON assembled_flow_snapshots(system, assembled_at_epoch);
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS realtime_kv (
                attribute TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                source TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS worker_api_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_token TEXT,
                round_id TEXT,
                worker TEXT NOT NULL,
                system TEXT,
                service TEXT NOT NULL,
                method TEXT NOT NULL,
                api_link TEXT NOT NULL,
                requested_at_utc TEXT NOT NULL,
                requested_at_epoch REAL NOT NULL,
                ok INTEGER NOT NULL,
                status TEXT,
                status_code INTEGER,
                duration_ms REAL,
                result_text TEXT,
                error_text TEXT
            );
            """
        )
        worker_api_log_columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(worker_api_logs);").fetchall()
        }
        if "request_token" not in worker_api_log_columns:
            conn.execute("ALTER TABLE worker_api_logs ADD COLUMN request_token TEXT;")
        if "round_id" not in worker_api_log_columns:
            conn.execute("ALTER TABLE worker_api_logs ADD COLUMN round_id TEXT;")
        if "status" not in worker_api_log_columns:
            conn.execute("ALTER TABLE worker_api_logs ADD COLUMN status TEXT;")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_worker_api_logs_time
            ON worker_api_logs(requested_at_epoch DESC);
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_worker_api_logs_request_token
            ON worker_api_logs(request_token);
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_request_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_id TEXT NOT NULL,
                system TEXT,
                source TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                method TEXT NOT NULL,
                request_url TEXT NOT NULL,
                requested_at_utc TEXT NOT NULL,
                requested_at_epoch REAL NOT NULL,
                duration_ms REAL,
                ok INTEGER NOT NULL,
                status_code INTEGER,
                error_text TEXT,
                response_text TEXT,
                response_json TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_raw_request_results_round
            ON raw_request_results(round_id, requested_at_epoch);
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_raw_request_results_endpoint_time
            ON raw_request_results(source, endpoint, requested_at_epoch DESC);
            """
        )
        conn.commit()


def _truncate_result_text(text: str | None) -> str:
    raw = str(text or "")
    if len(raw) <= WORKER_API_LOG_RESULT_MAX_CHARS:
        return raw
    tail = f"\n...[truncated {len(raw) - WORKER_API_LOG_RESULT_MAX_CHARS} chars]"
    keep = max(0, WORKER_API_LOG_RESULT_MAX_CHARS - len(tail))
    return f"{raw[:keep]}{tail}"


def insert_worker_api_log(
    db_path: Path,
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
) -> int:
    parsed_requested = datetime.fromisoformat(requested_at_utc.replace("Z", "+00:00"))
    if parsed_requested.tzinfo is None:
        parsed_requested = parsed_requested.replace(tzinfo=UTC)
    requested_norm = parsed_requested.astimezone(UTC).isoformat()
    requested_epoch = parsed_requested.astimezone(UTC).timestamp()
    with sqlite3.connect(db_path, timeout=SQLITE_BUSY_TIMEOUT_SECONDS) as conn:
        conn.execute(f"PRAGMA busy_timeout={int(SQLITE_BUSY_TIMEOUT_SECONDS * 1000)};")
        cursor = conn.execute(
            """
            INSERT INTO worker_api_logs (
                request_token,
                round_id,
                worker,
                system,
                service,
                method,
                api_link,
                requested_at_utc,
                requested_at_epoch,
                ok,
                status,
                status_code,
                duration_ms,
                result_text,
                error_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                str(request_token or "") or None,
                str(round_id or "") or None,
                str(worker or "worker"),
                str(system or "") or None,
                str(service or "unknown"),
                str(method or "GET").upper(),
                str(api_link or ""),
                requested_norm,
                requested_epoch,
                1 if ok else 0,
                str(status or "") or None,
                status_code,
                duration_ms,
                _truncate_result_text(result_text),
                str(error_text or "") or None,
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)


def update_worker_api_log(
    db_path: Path,
    *,
    request_token: str,
    ok: bool,
    status: str | None,
    status_code: int | None,
    duration_ms: float | None,
    result_text: str | None,
    error_text: str | None,
) -> None:
    if not request_token:
        return
    with sqlite3.connect(db_path, timeout=SQLITE_BUSY_TIMEOUT_SECONDS) as conn:
        conn.execute(f"PRAGMA busy_timeout={int(SQLITE_BUSY_TIMEOUT_SECONDS * 1000)};")
        conn.execute(
            """
            UPDATE worker_api_logs
            SET
                ok = ?,
                status = ?,
                status_code = ?,
                duration_ms = ?,
                result_text = ?,
                error_text = ?
            WHERE id = (
                SELECT id
                FROM worker_api_logs
                WHERE request_token = ?
                ORDER BY id DESC
                LIMIT 1
            );
            """,
            (
                1 if ok else 0,
                str(status or "") or None,
                status_code,
                duration_ms,
                _truncate_result_text(result_text),
                str(error_text or "") or None,
                request_token,
            ),
        )
        conn.commit()


def expire_pending_worker_api_logs(
    db_path: Path,
    *,
    older_than_epoch: float,
    status: str = "timeout",
    error_text: str = "worker_log_timeout",
) -> int:
    with sqlite3.connect(db_path, timeout=SQLITE_BUSY_TIMEOUT_SECONDS) as conn:
        conn.execute(f"PRAGMA busy_timeout={int(SQLITE_BUSY_TIMEOUT_SECONDS * 1000)};")
        cursor = conn.execute(
            """
            UPDATE worker_api_logs
            SET
                ok = 0,
                status = ?,
                error_text = COALESCE(NULLIF(error_text, ''), ?)
            WHERE status = 'pending'
              AND requested_at_epoch < ?;
            """,
            (
                str(status or "timeout"),
                str(error_text or "worker_log_timeout"),
                float(older_than_epoch),
            ),
        )
        conn.commit()
        return int(cursor.rowcount or 0)


def finalize_pending_worker_api_logs_for_round(
    db_path: Path,
    *,
    round_id: str,
    services: tuple[str, ...] | None = None,
    status: str = "timeout",
    error_text: str = "worker_round_incomplete",
) -> int:
    normalized_round_id = str(round_id or "").strip()
    if not normalized_round_id:
        return 0
    where_sql = """
            WHERE round_id = ?
              AND status = 'pending'
    """
    params: list[object] = [normalized_round_id]
    normalized_services = tuple(str(service or "").strip() for service in (services or ()) if str(service or "").strip())
    if normalized_services:
        placeholders = ", ".join("?" for _ in normalized_services)
        where_sql += f"\n              AND service IN ({placeholders})"
        params.extend(normalized_services)
    with sqlite3.connect(db_path, timeout=SQLITE_BUSY_TIMEOUT_SECONDS) as conn:
        conn.execute(f"PRAGMA busy_timeout={int(SQLITE_BUSY_TIMEOUT_SECONDS * 1000)};")
        cursor = conn.execute(
            f"""
            UPDATE worker_api_logs
            SET
                ok = 0,
                status = ?,
                error_text = COALESCE(NULLIF(error_text, ''), ?)
            {where_sql};
            """,
            [str(status or "timeout"), str(error_text or "worker_round_incomplete"), *params],
        )
        conn.commit()
        return int(cursor.rowcount or 0)


def count_pending_worker_api_logs(
    db_path: Path,
    *,
    round_id: str,
    services: tuple[str, ...] | None = None,
) -> int:
    normalized_round_id = str(round_id or "").strip()
    if not normalized_round_id:
        return 0
    where_sql = """
        WHERE round_id = ?
          AND status = 'pending'
    """
    params: list[object] = [normalized_round_id]
    normalized_services = tuple(str(service or "").strip() for service in (services or ()) if str(service or "").strip())
    if normalized_services:
        placeholders = ", ".join("?" for _ in normalized_services)
        where_sql += f"\n          AND service IN ({placeholders})"
        params.extend(normalized_services)
    with sqlite3.connect(db_path, timeout=SQLITE_BUSY_TIMEOUT_SECONDS) as conn:
        conn.execute(f"PRAGMA busy_timeout={int(SQLITE_BUSY_TIMEOUT_SECONDS * 1000)};")
        row = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM worker_api_logs
            {where_sql};
            """,
            params,
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0


def list_worker_api_logs(
    db_path: Path,
    *,
    page: int,
    page_size: int,
    worker: str | None = None,
    system: str | None = None,
    service: str | None = None,
    status: str | None = None,
    exclude_statuses: tuple[str, ...] | None = None,
) -> dict[str, object]:
    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 1
    if page_size > 500:
        page_size = 500

    where_conditions: list[str] = []
    params: list[object] = []
    if worker:
        where_conditions.append("worker = ?")
        params.append(worker)
    if system:
        where_conditions.append("system = ?")
        params.append(system)
    if service:
        where_conditions.append("service = ?")
        params.append(service)
    if status:
        where_conditions.append("status = ?")
        params.append(status)
    normalized_excluded_statuses = tuple(
        str(item or "").strip()
        for item in (exclude_statuses or ())
        if str(item or "").strip()
    )
    if normalized_excluded_statuses:
        placeholders = ", ".join("?" for _ in normalized_excluded_statuses)
        where_conditions.append(f"COALESCE(status, '') NOT IN ({placeholders})")
        params.extend(normalized_excluded_statuses)
    where_sql = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

    if not db_path.exists():
        return {
            "count": 0,
            "total": 0,
            "page": page,
            "page_size": page_size,
            "has_next": False,
            "has_prev": False,
            "items": [],
        }

    try:
        with sqlite3.connect(db_path) as conn:
            total = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM worker_api_logs {where_sql};",
                    params,
                ).fetchone()[0]
            )
            start = (page - 1) * page_size
            query_params = [*params, page_size, start]
            rows = conn.execute(
                f"""
                SELECT
                    id,
                    request_token,
                    round_id,
                    worker,
                    system,
                    service,
                    method,
                    api_link,
                    requested_at_utc,
                    ok,
                    status,
                    status_code,
                    duration_ms,
                    result_text,
                    error_text
                FROM worker_api_logs
                {where_sql}
                ORDER BY requested_at_epoch DESC
                LIMIT ? OFFSET ?;
                """,
                query_params,
            ).fetchall()
    except sqlite3.OperationalError:
        total = 0
        rows = []

    items = [
        {
            "id": int(row[0]),
            "request_token": str(row[1] or ""),
            "round_id": str(row[2] or ""),
            "worker": str(row[3]),
            "system": str(row[4] or ""),
            "service": str(row[5]),
            "method": str(row[6]),
            "api_link": str(row[7]),
            "requested_at_utc": row[8],
            "ok": bool(row[9]),
            "status": str(row[10] or ""),
            "status_code": row[11],
            "duration_ms": row[12],
            "result_text": str(row[13] or ""),
            "error_text": str(row[14] or ""),
        }
        for row in rows
    ]
    end = page * page_size
    return {
        "count": len(items),
        "total": total,
        "page": page,
        "page_size": page_size,
        "has_next": end < total,
        "has_prev": page > 1,
        "items": items,
    }


def insert_sample(db_path: Path, sample: EnergySample) -> None:
    assembled_at_utc = sample.assembled_at_utc.astimezone(UTC)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO assembled_flow_snapshots (
                round_id,
                system,
                assembled_at_utc,
                assembled_at_epoch,
                source,
                pv_w,
                grid_w,
                battery_w,
                load_w,
                battery_soc_percent,
                inverter_status,
                balance_w,
                flow_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                sample.round_id,
                sample.system,
                assembled_at_utc.isoformat(),
                assembled_at_utc.timestamp(),
                sample.source,
                sample.pv_w,
                sample.grid_w,
                sample.battery_w,
                sample.load_w,
                sample.battery_soc_percent,
                sample.inverter_status,
                sample.balance_w,
                json.dumps(sample.flow, ensure_ascii=False),
            ),
        )
        conn.commit()


def export_samples_csv(db_path: Path) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(CSV_HEADERS))
    writer.writeheader()

    if not db_path.exists():
        return output.getvalue()

    try:
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                """
                SELECT
                    system,
                    assembled_at_utc,
                    assembled_at_epoch,
                    source,
                    pv_w,
                    grid_w,
                    battery_w,
                    load_w,
                    battery_soc_percent,
                    inverter_status,
                    balance_w,
                    flow_json
                FROM assembled_flow_snapshots
                ORDER BY assembled_at_epoch ASC;
                """
            ).fetchall()
    except sqlite3.OperationalError:
        rows = []

    for row in rows:
        writer.writerow(
            {
                "system": row[0],
                "sampled_at_utc": row[1],
                "sampled_at_epoch": row[2],
                "source": row[3],
                "pv_w": row[4],
                "grid_w": row[5],
                "battery_w": row[6],
                "load_w": row[7],
                "battery_soc_percent": row[8],
                "inverter_status": row[9],
                "balance_w": row[10],
                "payload_json": row[11],
            }
        )
    return output.getvalue()


def _csv_nullable_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    return float(text)


def import_samples_csv(db_path: Path, csv_text: str, *, replace_existing: bool = False) -> dict[str, object]:
    init_db(db_path)
    reader = csv.DictReader(io.StringIO(csv_text))
    fieldnames = tuple(reader.fieldnames or ())
    missing = [column for column in CSV_HEADERS if column not in fieldnames]
    if missing:
        raise ValueError(f"CSV missing required columns: {', '.join(missing)}")

    imported = 0
    with sqlite3.connect(db_path) as conn:
        if replace_existing:
            conn.execute("DELETE FROM assembled_flow_snapshots;")
        for row in reader:
            system = str(row.get("system") or "").strip().lower()
            if not system:
                raise ValueError("CSV row missing system")
            sampled_at_utc_text = str(row.get("sampled_at_utc") or "").strip()
            if not sampled_at_utc_text:
                raise ValueError("CSV row missing sampled_at_utc")
            sampled_at = datetime.fromisoformat(sampled_at_utc_text.replace("Z", "+00:00"))
            if sampled_at.tzinfo is None:
                sampled_at = sampled_at.replace(tzinfo=UTC)
            sampled_at = sampled_at.astimezone(UTC)
            sampled_at_epoch = _csv_nullable_float(row.get("sampled_at_epoch"))
            if sampled_at_epoch is None:
                sampled_at_epoch = sampled_at.timestamp()
            source = str(row.get("source") or "").strip() or "imported_csv"
            payload_json = str(row.get("payload_json") or "").strip() or "{}"
            try:
                payload_obj = json.loads(payload_json)
                if not isinstance(payload_obj, dict):
                    payload_json = "{}"
            except json.JSONDecodeError:
                payload_json = "{}"

            conn.execute(
                """
                INSERT INTO assembled_flow_snapshots (
                    round_id,
                    system,
                    assembled_at_utc,
                    assembled_at_epoch,
                    source,
                    pv_w,
                    grid_w,
                    battery_w,
                    load_w,
                    battery_soc_percent,
                    inverter_status,
                    balance_w,
                    flow_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    f"import-{int(sampled_at_epoch)}-{system}",
                    system,
                    sampled_at.isoformat(),
                    sampled_at_epoch,
                    source,
                    _csv_nullable_float(row.get("pv_w")),
                    _csv_nullable_float(row.get("grid_w")),
                    _csv_nullable_float(row.get("battery_w")),
                    _csv_nullable_float(row.get("load_w")),
                    _csv_nullable_float(row.get("battery_soc_percent")),
                    (str(row.get("inverter_status") or "").strip() or None),
                    _csv_nullable_float(row.get("balance_w")),
                    payload_json,
                ),
            )
            imported += 1
        conn.commit()
    return {"imported_rows": imported, "replaced": replace_existing}


def get_storage_status(db_path: Path, sample_interval_seconds: float) -> dict[str, object]:
    if not db_path.exists():
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
        with sqlite3.connect(db_path) as conn:
            total_rows = int(conn.execute("SELECT COUNT(*) FROM assembled_flow_snapshots;").fetchone()[0])
            rows_by_system_rows = conn.execute(
                """
                SELECT system, COUNT(*) AS c
                FROM assembled_flow_snapshots
                GROUP BY system
                ORDER BY system ASC;
                """
            ).fetchall()
            rows_by_system = {str(row[0]): int(row[1]) for row in rows_by_system_rows}
            last_sample_utc = conn.execute(
                "SELECT assembled_at_utc FROM assembled_flow_snapshots ORDER BY assembled_at_epoch DESC LIMIT 1;"
            ).fetchone()
    except sqlite3.OperationalError:
        total_rows = 0
        rows_by_system = {}
        last_sample_utc = None

    db_size_bytes = db_path.stat().st_size
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
        "last_sample_utc": last_sample_utc[0] if last_sample_utc else None,
        "sample_interval_seconds": sample_interval_seconds,
        "samples_per_hour": round(3600.0 / sample_interval_seconds, 3),
        "samples_per_day_per_system": round(samples_per_day_per_system, 3),
        "estimated_bytes_per_day_per_system": estimated_bytes_per_day_per_system,
        "estimated_mb_per_day_per_system": round(estimated_bytes_per_day_per_system / (1024 * 1024), 3),
        "estimated_bytes_per_day_total": estimated_bytes_per_day_total,
        "estimated_mb_per_day_total": round(estimated_bytes_per_day_total / (1024 * 1024), 3),
    }


def compute_daily_usage(
    db_path: Path,
    *,
    system: str,
    target_day_utc: date,
    sample_interval_seconds: float,
) -> dict[str, object]:
    day_start = datetime.combine(target_day_utc, time.min, tzinfo=UTC)
    day_end = day_start + timedelta(days=1)
    start_ts = day_start.timestamp()
    end_ts = day_end.timestamp()

    if not db_path.exists():
        rows: list[tuple[float, float | None, float | None, float | None, float | None]] = []
    else:
        try:
            with sqlite3.connect(db_path) as conn:
                rows = conn.execute(
                    """
                    SELECT assembled_at_epoch, pv_w, grid_w, battery_w, load_w
                    FROM assembled_flow_snapshots
                    WHERE system = ? AND assembled_at_epoch >= ? AND assembled_at_epoch < ?
                    ORDER BY assembled_at_epoch ASC;
                    """,
                    (system, start_ts, end_ts),
                ).fetchall()
        except sqlite3.OperationalError:
            rows = []

    expected = int(round(86400.0 / sample_interval_seconds))
    if len(rows) < 2:
        return {
            "system": system,
            "day_utc": day_start.date().isoformat(),
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

    home_load_wh = 0.0
    solar_wh = 0.0
    grid_import_wh = 0.0
    grid_export_wh = 0.0
    battery_charge_wh = 0.0
    battery_discharge_wh = 0.0

    max_dt = sample_interval_seconds * 2.5
    for i in range(len(rows) - 1):
        ts, pv_w, grid_w, battery_w, load_w = rows[i]
        next_ts = rows[i + 1][0]
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

    return {
        "system": system,
        "day_utc": day_start.date().isoformat(),
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


def list_samples(
    db_path: Path,
    *,
    system: str | None,
    target_day_utc: date | None,
    start_at_utc: datetime | None = None,
    end_at_utc: datetime | None = None,
    page: int,
    page_size: int,
) -> dict[str, object]:
    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 1
    if page_size > 500:
        page_size = 500

    where_conditions: list[str] = []
    params: list[object] = []
    if system:
        where_conditions.append("system = ?")
        params.append(system)
    if target_day_utc is not None:
        day_start = datetime.combine(target_day_utc, time.min, tzinfo=UTC)
        day_end = day_start + timedelta(days=1)
        where_conditions.append("assembled_at_epoch >= ?")
        where_conditions.append("assembled_at_epoch < ?")
        params.extend([day_start.timestamp(), day_end.timestamp()])
    if start_at_utc is not None:
        where_conditions.append("assembled_at_epoch >= ?")
        params.append(start_at_utc.astimezone(UTC).timestamp())
    if end_at_utc is not None:
        where_conditions.append("assembled_at_epoch < ?")
        params.append(end_at_utc.astimezone(UTC).timestamp())
    where_sql = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

    if not db_path.exists():
        return {
            "count": 0,
            "total": 0,
            "page": page,
            "page_size": page_size,
            "has_next": False,
            "has_prev": False,
            "items": [],
        }

    try:
        with sqlite3.connect(db_path) as conn:
            total = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM assembled_flow_snapshots {where_sql};",
                    params,
                ).fetchone()[0]
            )
            start = (page - 1) * page_size
            query_params = [*params, page_size, start]
            rows = conn.execute(
                f"""
                SELECT
                    id,
                    system,
                    assembled_at_utc,
                    source,
                    pv_w,
                    grid_w,
                    battery_w,
                    load_w,
                    battery_soc_percent,
                    inverter_status,
                    balance_w
                FROM assembled_flow_snapshots
                {where_sql}
                ORDER BY assembled_at_epoch DESC
                LIMIT ? OFFSET ?;
                """,
                query_params,
            ).fetchall()
    except sqlite3.OperationalError:
        total = 0
        rows = []

    items = [
        {
            "id": int(row[0]),
            "system": str(row[1]),
            "sampled_at_utc": row[2],
            "source": str(row[3]),
            "pv_w": row[4],
            "grid_w": row[5],
            "battery_w": row[6],
            "load_w": row[7],
            "battery_soc_percent": row[8],
            "inverter_status": row[9],
            "balance_w": row[10],
        }
        for row in rows
    ]
    end = page * page_size
    return {
        "count": len(items),
        "total": total,
        "page": page,
        "page_size": page_size,
        "has_next": end < total,
        "has_prev": page > 1,
        "items": items,
    }


def get_latest_sample(db_path: Path, *, system: str) -> dict[str, object] | None:
    if not db_path.exists():
        return None

    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                """
                SELECT
                    assembled_at_utc,
                    source,
                    pv_w,
                    grid_w,
                    battery_w,
                    load_w,
                    battery_soc_percent,
                    inverter_status,
                    balance_w,
                    flow_json
                FROM assembled_flow_snapshots
                WHERE system = ?
                ORDER BY assembled_at_epoch DESC
                LIMIT 1;
                """,
                (system,),
            ).fetchone()
    except sqlite3.OperationalError:
        return None

    if not row:
        return None

    payload_json = row[9]
    payload: dict[str, object] = {}
    if isinstance(payload_json, str) and payload_json.strip():
        try:
            parsed = json.loads(payload_json)
            if isinstance(parsed, dict):
                payload = parsed
        except json.JSONDecodeError:
            payload = {}

    return {
        "system": system,
        "sampled_at_utc": row[0],
        "source": row[1],
        "pv_w": row[2],
        "grid_w": row[3],
        "battery_w": row[4],
        "load_w": row[5],
        "battery_soc_percent": row[6],
        "inverter_status": row[7],
        "balance_w": row[8],
        "payload": payload,
    }


def upsert_realtime_kv(db_path: Path, rows: list[tuple[str, str, str]]) -> None:
    if not rows:
        return
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO realtime_kv(attribute, value, source)
            VALUES (?, ?, ?)
            ON CONFLICT(attribute) DO UPDATE SET
                value = excluded.value,
                source = excluded.source;
            """,
            rows,
        )
        conn.commit()


def get_realtime_kv_by_prefix(db_path: Path, *, prefix: str) -> dict[str, dict[str, object]]:
    if not db_path.exists():
        return {}
    try:
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                """
                SELECT attribute, value, source
                FROM realtime_kv
                WHERE attribute LIKE ?;
                """,
                (f"{prefix}%",),
            ).fetchall()
    except sqlite3.OperationalError:
        return {}

    data: dict[str, dict[str, object]] = {}
    for attribute, value_text, source in rows:
        parsed_value: object = value_text
        if isinstance(value_text, str):
            try:
                parsed_value = json.loads(value_text)
            except json.JSONDecodeError:
                parsed_value = value_text
        data[str(attribute)] = {
            "value": parsed_value,
            "source": str(source or ""),
        }
    return data


def list_realtime_kv_rows(db_path: Path, *, prefix: str | None = None) -> list[dict[str, object]]:
    if not db_path.exists():
        return []
    where_sql = ""
    params: tuple[object, ...] = ()
    if prefix:
        where_sql = "WHERE attribute LIKE ?"
        params = (f"{prefix}%",)
    try:
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                f"""
                SELECT attribute, value, source
                FROM realtime_kv
                {where_sql}
                ORDER BY attribute ASC;
                """,
                params,
            ).fetchall()
    except sqlite3.OperationalError:
        return []

    items: list[dict[str, object]] = []
    for attribute, value_text, source in rows:
        parsed_value: object = value_text
        if isinstance(value_text, str):
            try:
                parsed_value = json.loads(value_text)
            except json.JSONDecodeError:
                parsed_value = value_text
        items.append(
            {
                "attribute": str(attribute),
                "value": parsed_value,
                "source": str(source or ""),
            }
        )
    return items


def insert_raw_request_result(
    db_path: Path,
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
    parsed_requested = datetime.fromisoformat(requested_at_utc.replace("Z", "+00:00"))
    if parsed_requested.tzinfo is None:
        parsed_requested = parsed_requested.replace(tzinfo=UTC)
    requested_norm = parsed_requested.astimezone(UTC).isoformat()
    requested_epoch = parsed_requested.astimezone(UTC).timestamp()
    response_json_text = None if response_json is None else json.dumps(response_json, ensure_ascii=False)

    with sqlite3.connect(db_path, timeout=SQLITE_BUSY_TIMEOUT_SECONDS) as conn:
        conn.execute(f"PRAGMA busy_timeout={int(SQLITE_BUSY_TIMEOUT_SECONDS * 1000)};")
        conn.execute(
            """
            INSERT INTO raw_request_results (
                round_id,
                system,
                source,
                endpoint,
                method,
                request_url,
                requested_at_utc,
                requested_at_epoch,
                duration_ms,
                ok,
                status_code,
                error_text,
                response_text,
                response_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                round_id,
                str(system or "") or None,
                source,
                endpoint,
                method,
                request_url,
                requested_norm,
                requested_epoch,
                duration_ms,
                1 if ok else 0,
                status_code,
                error_text,
                _truncate_result_text(response_text),
                response_json_text,
            ),
        )
        conn.commit()


def get_latest_raw_request_result(
    db_path: Path,
    *,
    source: str,
    endpoint: str,
) -> dict[str, object] | None:
    if not db_path.exists():
        return None
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                """
                SELECT
                    round_id,
                    system,
                    request_url,
                    requested_at_utc,
                    duration_ms,
                    ok,
                    status_code,
                    error_text,
                    response_json,
                    response_text
                FROM raw_request_results
                WHERE source = ? AND endpoint = ?
                ORDER BY requested_at_epoch DESC
                LIMIT 1;
                """
                ,
                (source, endpoint),
            ).fetchone()
    except sqlite3.OperationalError:
        return None

    if not row:
        return None

    payload_json = row[8]
    payload: object | None = None
    if isinstance(payload_json, str) and payload_json.strip():
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError:
            payload = None

    return {
        "round_id": row[0],
        "system": row[1],
        "endpoint": endpoint,
        "path": row[2],
        "requested_at_utc": row[3],
        "fetch_ms": row[4],
        "ok": bool(row[5]),
        "status_code": row[6],
        "error": row[7],
        "payload": payload,
        "response_text": row[9],
        "last_success_at_utc": None,
    }


def list_raw_request_results(
    db_path: Path,
    *,
    page: int,
    page_size: int,
    round_id: str | None = None,
    system: str | None = None,
    source: str | None = None,
) -> dict[str, object]:
    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 1
    if page_size > 500:
        page_size = 500

    where_conditions: list[str] = []
    params: list[object] = []
    if round_id:
        where_conditions.append("round_id = ?")
        params.append(round_id)
    if system:
        where_conditions.append("system = ?")
        params.append(system)
    if source:
        where_conditions.append("source = ?")
        params.append(source)
    where_sql = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

    if not db_path.exists():
        return {
            "count": 0,
            "total": 0,
            "page": page,
            "page_size": page_size,
            "has_next": False,
            "has_prev": False,
            "items": [],
        }

    try:
        with sqlite3.connect(db_path) as conn:
            total = int(conn.execute(f"SELECT COUNT(*) FROM raw_request_results {where_sql};", params).fetchone()[0])
            start = (page - 1) * page_size
            query_params = [*params, page_size, start]
            rows = conn.execute(
                f"""
                SELECT
                    id,
                    round_id,
                    system,
                    source,
                    endpoint,
                    method,
                    request_url,
                    requested_at_utc,
                    duration_ms,
                    ok,
                    status_code,
                    error_text,
                    response_json
                FROM raw_request_results
                {where_sql}
                ORDER BY requested_at_epoch DESC
                LIMIT ? OFFSET ?;
                """,
                query_params,
            ).fetchall()
    except sqlite3.OperationalError:
        total = 0
        rows = []

    items: list[dict[str, object]] = []
    for row in rows:
        parsed_json: object = None
        if isinstance(row[12], str) and row[12].strip():
            try:
                parsed_json = json.loads(row[12])
            except json.JSONDecodeError:
                parsed_json = row[12]
        items.append(
            {
                "id": int(row[0]),
                "round_id": str(row[1]),
                "system": str(row[2] or ""),
                "source": str(row[3]),
                "endpoint": str(row[4]),
                "method": str(row[5]),
                "request_url": str(row[6]),
                "requested_at_utc": row[7],
                "duration_ms": row[8],
                "ok": bool(row[9]),
                "status_code": row[10],
                "error_text": str(row[11] or ""),
                "response_json": parsed_json,
            }
        )
    end = page * page_size
    return {
        "count": len(items),
        "total": total,
        "page": page,
        "page_size": page_size,
        "has_next": end < total,
        "has_prev": page > 1,
        "items": items,
    }


def get_series_samples(
    db_path: Path,
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

    if not db_path.exists():
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
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                """
                SELECT
                    assembled_at_utc,
                    assembled_at_epoch,
                    pv_w,
                    grid_w,
                    battery_w,
                    load_w
                    FROM assembled_flow_snapshots
                    WHERE system = ? AND assembled_at_epoch >= ? AND assembled_at_epoch <= ?
                    ORDER BY assembled_at_epoch ASC;
                """,
                (system, start_at.timestamp(), end_at.timestamp()),
            ).fetchall()
    except sqlite3.OperationalError:
        rows = []

    if len(rows) > safe_max_points:
        step = max(1, len(rows) // safe_max_points)
        sampled_rows = rows[::step]
        if sampled_rows[-1][1] != rows[-1][1]:
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
    db_path: Path,
    *,
    system: str,
    start_at_utc: datetime,
    end_at_utc: datetime,
    sample_interval_seconds: float,
) -> dict[str, object]:
    start_ts = start_at_utc.astimezone(UTC).timestamp()
    end_ts = end_at_utc.astimezone(UTC).timestamp()

    if not db_path.exists():
        rows: list[tuple[float, float | None, float | None, float | None, float | None]] = []
    else:
        try:
            with sqlite3.connect(db_path) as conn:
                rows = conn.execute(
                    """
                    SELECT assembled_at_epoch, pv_w, grid_w, battery_w, load_w
                    FROM assembled_flow_snapshots
                    WHERE system = ? AND assembled_at_epoch >= ? AND assembled_at_epoch < ?
                    ORDER BY assembled_at_epoch ASC;
                    """,
                    (system, start_ts, end_ts),
                ).fetchall()
        except sqlite3.OperationalError:
            rows = []

    duration_seconds = max(0.0, end_ts - start_ts)
    expected = int(round(duration_seconds / sample_interval_seconds)) if sample_interval_seconds > 0 else 0
    if len(rows) < 2:
        return {
            "system": system,
            "start_at_utc": start_at_utc.isoformat(),
            "end_at_utc": end_at_utc.isoformat(),
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

    home_load_wh = 0.0
    solar_wh = 0.0
    grid_import_wh = 0.0
    grid_export_wh = 0.0
    battery_charge_wh = 0.0
    battery_discharge_wh = 0.0

    max_dt = sample_interval_seconds * 2.5
    for i in range(len(rows) - 1):
        ts, pv_w, grid_w, battery_w, load_w = rows[i]
        next_ts = rows[i + 1][0]
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

    return {
        "system": system,
        "start_at_utc": start_at_utc.isoformat(),
        "end_at_utc": end_at_utc.isoformat(),
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
