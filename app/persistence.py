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
class EnergySample:
    system: str
    sampled_at_utc: datetime
    source: str
    pv_w: float | None
    grid_w: float | None
    battery_w: float | None
    load_w: float | None
    battery_soc_percent: float | None
    inverter_status: str | None
    balance_w: float | None
    payload: dict[str, object]


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS energy_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                system TEXT NOT NULL,
                sampled_at_utc TEXT NOT NULL,
                sampled_at_epoch REAL NOT NULL,
                source TEXT NOT NULL,
                pv_w REAL,
                grid_w REAL,
                battery_w REAL,
                load_w REAL,
                battery_soc_percent REAL,
                inverter_status TEXT,
                balance_w REAL,
                payload_json TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_energy_samples_system_epoch
            ON energy_samples(system, sampled_at_epoch);
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
                worker TEXT NOT NULL,
                system TEXT,
                service TEXT NOT NULL,
                method TEXT NOT NULL,
                api_link TEXT NOT NULL,
                requested_at_utc TEXT NOT NULL,
                requested_at_epoch REAL NOT NULL,
                ok INTEGER NOT NULL,
                status_code INTEGER,
                duration_ms REAL,
                result_text TEXT,
                error_text TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_worker_api_logs_time
            ON worker_api_logs(requested_at_epoch DESC);
            """
        )
        for table_name in SOLPLANET_ENDPOINT_TABLES.values():
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    path TEXT NOT NULL,
                    requested_at_utc TEXT NOT NULL,
                    requested_at_epoch REAL NOT NULL,
                    ok INTEGER NOT NULL,
                    error TEXT,
                    payload_json TEXT,
                    fetch_ms REAL,
                    last_success_at_utc TEXT
                );
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
    worker: str,
    system: str | None,
    service: str,
    method: str,
    api_link: str,
    requested_at_utc: str,
    ok: bool,
    status_code: int | None,
    duration_ms: float | None,
    result_text: str | None,
    error_text: str | None,
) -> None:
    parsed_requested = datetime.fromisoformat(requested_at_utc.replace("Z", "+00:00"))
    if parsed_requested.tzinfo is None:
        parsed_requested = parsed_requested.replace(tzinfo=UTC)
    requested_norm = parsed_requested.astimezone(UTC).isoformat()
    requested_epoch = parsed_requested.astimezone(UTC).timestamp()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO worker_api_logs (
                worker,
                system,
                service,
                method,
                api_link,
                requested_at_utc,
                requested_at_epoch,
                ok,
                status_code,
                duration_ms,
                result_text,
                error_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                str(worker or "worker"),
                str(system or "") or None,
                str(service or "unknown"),
                str(method or "GET").upper(),
                str(api_link or ""),
                requested_norm,
                requested_epoch,
                1 if ok else 0,
                status_code,
                duration_ms,
                _truncate_result_text(result_text),
                str(error_text or "") or None,
            ),
        )
        conn.commit()


def list_worker_api_logs(
    db_path: Path,
    *,
    page: int,
    page_size: int,
    worker: str | None = None,
    system: str | None = None,
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
                    worker,
                    system,
                    service,
                    method,
                    api_link,
                    requested_at_utc,
                    ok,
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
            "worker": str(row[1]),
            "system": str(row[2] or ""),
            "service": str(row[3]),
            "method": str(row[4]),
            "api_link": str(row[5]),
            "requested_at_utc": row[6],
            "ok": bool(row[7]),
            "status_code": row[8],
            "duration_ms": row[9],
            "result_text": str(row[10] or ""),
            "error_text": str(row[11] or ""),
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
    sampled_at_utc = sample.sampled_at_utc.astimezone(UTC)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO energy_samples (
                system,
                sampled_at_utc,
                sampled_at_epoch,
                source,
                pv_w,
                grid_w,
                battery_w,
                load_w,
                battery_soc_percent,
                inverter_status,
                balance_w,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                sample.system,
                sampled_at_utc.isoformat(),
                sampled_at_utc.timestamp(),
                sample.source,
                sample.pv_w,
                sample.grid_w,
                sample.battery_w,
                sample.load_w,
                sample.battery_soc_percent,
                sample.inverter_status,
                sample.balance_w,
                json.dumps(sample.payload, ensure_ascii=False),
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
                    sampled_at_utc,
                    sampled_at_epoch,
                    source,
                    pv_w,
                    grid_w,
                    battery_w,
                    load_w,
                    battery_soc_percent,
                    inverter_status,
                    balance_w,
                    payload_json
                FROM energy_samples
                ORDER BY sampled_at_epoch ASC;
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
            conn.execute("DELETE FROM energy_samples;")
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
                INSERT INTO energy_samples (
                    system,
                    sampled_at_utc,
                    sampled_at_epoch,
                    source,
                    pv_w,
                    grid_w,
                    battery_w,
                    load_w,
                    battery_soc_percent,
                    inverter_status,
                    balance_w,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
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
            total_rows = int(conn.execute("SELECT COUNT(*) FROM energy_samples;").fetchone()[0])
            rows_by_system_rows = conn.execute(
                """
                SELECT system, COUNT(*) AS c
                FROM energy_samples
                GROUP BY system
                ORDER BY system ASC;
                """
            ).fetchall()
            rows_by_system = {str(row[0]): int(row[1]) for row in rows_by_system_rows}
            last_sample_utc = conn.execute(
                "SELECT sampled_at_utc FROM energy_samples ORDER BY sampled_at_epoch DESC LIMIT 1;"
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
                    SELECT sampled_at_epoch, pv_w, grid_w, battery_w, load_w
                    FROM energy_samples
                    WHERE system = ? AND sampled_at_epoch >= ? AND sampled_at_epoch < ?
                    ORDER BY sampled_at_epoch ASC;
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
        where_conditions.append("sampled_at_epoch >= ?")
        where_conditions.append("sampled_at_epoch < ?")
        params.extend([day_start.timestamp(), day_end.timestamp()])
    if start_at_utc is not None:
        where_conditions.append("sampled_at_epoch >= ?")
        params.append(start_at_utc.astimezone(UTC).timestamp())
    if end_at_utc is not None:
        where_conditions.append("sampled_at_epoch < ?")
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
                    f"SELECT COUNT(*) FROM energy_samples {where_sql};",
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
                    sampled_at_utc,
                    source,
                    pv_w,
                    grid_w,
                    battery_w,
                    load_w,
                    battery_soc_percent,
                    inverter_status,
                    balance_w
                FROM energy_samples
                {where_sql}
                ORDER BY sampled_at_epoch DESC
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
                    sampled_at_utc,
                    source,
                    pv_w,
                    grid_w,
                    battery_w,
                    load_w,
                    battery_soc_percent,
                    inverter_status,
                    balance_w,
                    payload_json
                FROM energy_samples
                WHERE system = ?
                ORDER BY sampled_at_epoch DESC
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


def upsert_solplanet_endpoint_snapshot(
    db_path: Path,
    *,
    endpoint: str,
    path: str,
    requested_at_utc: str,
    ok: bool,
    error: str | None,
    payload: dict[str, object] | None,
    fetch_ms: float | None,
) -> None:
    table_name = SOLPLANET_ENDPOINT_TABLES.get(endpoint)
    if not table_name:
        raise ValueError(f"Unsupported endpoint: {endpoint}")

    parsed_requested = datetime.fromisoformat(requested_at_utc.replace("Z", "+00:00"))
    if parsed_requested.tzinfo is None:
        parsed_requested = parsed_requested.replace(tzinfo=UTC)
    requested_norm = parsed_requested.astimezone(UTC).isoformat()
    requested_epoch = parsed_requested.astimezone(UTC).timestamp()

    payload_json = None if payload is None else json.dumps(payload, ensure_ascii=False)

    with sqlite3.connect(db_path) as conn:
        existing = conn.execute(
            f"SELECT last_success_at_utc FROM {table_name} WHERE id = 1;"
        ).fetchone()
        previous_success = existing[0] if existing else None
        last_success = requested_norm if ok else previous_success
        conn.execute(
            f"""
            INSERT INTO {table_name} (
                id,
                path,
                requested_at_utc,
                requested_at_epoch,
                ok,
                error,
                payload_json,
                fetch_ms,
                last_success_at_utc
            ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                path = excluded.path,
                requested_at_utc = excluded.requested_at_utc,
                requested_at_epoch = excluded.requested_at_epoch,
                ok = excluded.ok,
                error = excluded.error,
                payload_json = excluded.payload_json,
                fetch_ms = excluded.fetch_ms,
                last_success_at_utc = excluded.last_success_at_utc;
            """,
            (
                path,
                requested_norm,
                requested_epoch,
                1 if ok else 0,
                error,
                payload_json,
                fetch_ms,
                last_success,
            ),
        )
        conn.commit()


def get_solplanet_endpoint_snapshot(db_path: Path, *, endpoint: str) -> dict[str, object] | None:
    table_name = SOLPLANET_ENDPOINT_TABLES.get(endpoint)
    if not table_name or not db_path.exists():
        return None
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                f"""
                SELECT
                    path,
                    requested_at_utc,
                    ok,
                    error,
                    payload_json,
                    fetch_ms,
                    last_success_at_utc
                FROM {table_name}
                WHERE id = 1;
                """
            ).fetchone()
    except sqlite3.OperationalError:
        return None

    if not row:
        return None

    payload_json = row[4]
    payload: dict[str, object] | None = None
    if isinstance(payload_json, str) and payload_json.strip():
        try:
            parsed = json.loads(payload_json)
            if isinstance(parsed, dict):
                payload = parsed
        except json.JSONDecodeError:
            payload = None

    return {
        "endpoint": endpoint,
        "path": row[0],
        "requested_at_utc": row[1],
        "ok": bool(row[2]),
        "error": row[3],
        "payload": payload,
        "fetch_ms": row[5],
        "last_success_at_utc": row[6],
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
                    sampled_at_utc,
                    sampled_at_epoch,
                    pv_w,
                    grid_w,
                    battery_w,
                    load_w
                FROM energy_samples
                WHERE system = ? AND sampled_at_epoch >= ? AND sampled_at_epoch <= ?
                ORDER BY sampled_at_epoch ASC;
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
                    SELECT sampled_at_epoch, pv_w, grid_w, battery_w, load_w
                    FROM energy_samples
                    WHERE system = ? AND sampled_at_epoch >= ? AND sampled_at_epoch < ?
                    ORDER BY sampled_at_epoch ASC;
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
