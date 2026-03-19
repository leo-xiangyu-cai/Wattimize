from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
import traceback
from urllib.parse import urlparse
from collections import Counter
from contextvars import ContextVar
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from time import monotonic
from typing import Callable, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from pydantic import BaseModel, Field
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.responses import Response

from app.config import (
    ALLOWED_SAMPLE_INTERVAL_SECONDS,
    CONST_SAJ_SAMPLE_INTERVAL_SECONDS,
    CONST_SOLPLANET_SAMPLE_INTERVAL_SECONDS,
    Settings,
    get_config_path,
    get_missing_required_fields_from_payload,
    load_settings,
    normalize_sample_interval_seconds,
    save_settings,
    settings_to_dict,
)
from app.home_assistant import HomeAssistantClient
from app.persistence import (
    DEFAULT_DB_PATH,
    EnergySample,
    OPERATION_RUN_STATUS_FAILED,
    OPERATION_RUN_STATUS_PENDING,
    OPERATION_RUN_STATUS_SUCCEEDED,
    OPERATION_RUN_STATUS_TIMEOUT,
    migrate_worker_log_legacy_statuses,
    count_pending_worker_api_logs,
    backfill_notification_entries_from_worker_logs,
    compute_daily_usage,
    compute_usage_between,
    dismiss_notification_entry,
    dispose_db_connections,
    expire_pending_worker_api_logs,
    export_database_bytes,
    finalize_pending_worker_api_logs_for_round,
    get_latest_raw_request_result,
    get_latest_operation_runs,
    get_realtime_kv_by_prefix,
    get_latest_sample,
    get_series_samples,
    get_storage_status,
    inspect_sqlite_database,
    insert_raw_request_result,
    insert_worker_api_log,
    import_database_bytes,
    init_db,
    insert_sample,
    get_time_window_rule_states,
    list_active_notification_entries,
    list_database_table_rows,
    list_database_tables,
    list_time_window_rule_state_rows,
    list_raw_request_results,
    list_realtime_kv_rows,
    list_samples,
    list_worker_api_logs,
    insert_operation_run,
    recover_sqlite_database,
    upsert_time_window_rule_state,
    upsert_operation_definitions,
    upsert_notification_entries,
    update_worker_api_log,
    update_operation_run,
    upsert_realtime_kv,
)
from app.solplanet_cgi import SolplanetCgiClient
from app.worker_log_schema import (
    WORKER_LOG_CATEGORY_ALL,
    WORKER_LOG_CATEGORY_COMBINED,
    WORKER_LOG_CATEGORY_NOTIFICATION,
    WORKER_LOG_CATEGORY_OPERATION,
    WORKER_LOG_CATEGORY_SAJ,
    WORKER_LOG_CATEGORY_SOLPLANET,
    WORKER_LOG_CATEGORY_TESLA,
    WORKER_LOG_NOTIFICATION_ACTIVE_STATUSES,
    WORKER_LOG_STATUS_APPLIED,
    WORKER_LOG_STATUS_FAILED,
    WORKER_LOG_STATUS_NOOP,
    WORKER_LOG_STATUS_OK,
    WORKER_LOG_STATUS_OUTSIDE_WINDOW,
    WORKER_LOG_STATUS_PENDING,
    WORKER_LOG_STATUS_SEND,
    WORKER_LOG_STATUS_SKIPPED,
    WORKER_LOG_STATUS_TIMEOUT,
    category_config as worker_log_category_config,
    category_service_values as worker_log_category_service_values,
    category_status_values as worker_log_category_status_values,
    service_config as worker_log_service_config,
    service_status_values as worker_log_service_status_values,
)

logger = logging.getLogger(__name__)


settings = load_settings()

POWER_FLOW_ACTIVE_THRESHOLD_W = 30
POLLED_SYSTEMS = ("saj", "solplanet")
SUPPORTED_SYSTEMS = (*POLLED_SYSTEMS, "combined")
SYSTEM_PREFIXES: dict[str, tuple[str, ...]] = {
    "saj": ("saj",),
    "solplanet": ("solplanet", "soulplanet"),
}
BALANCE_TOLERANCE_W = 100.0
TESLA_ASSUMED_CHARGING_VOLTAGE_V = 240.0
TESLA_CONTROL_FEEDBACK_KV_PREFIX = "ui.tesla_control_feedback."
TESLA_CONTROL_FEEDBACK_PENDING_TIMEOUT_SECONDS = 15.0
TESLA_CONTROL_FEEDBACK_SUCCESS_TTL_SECONDS = 4.0
TESLA_CONTROL_FEEDBACK_FAILURE_TTL_SECONDS = 30.0
TESLA_OPERATION_PENDING_TIMEOUT_SECONDS = 180.0
BATTERY_CAPACITY_KWH = {
    "saj": 15.0,
    "solplanet": 40.0,
}
TESLA_BATTERY_CAPACITY_KWH = 75.0
BATTERY_MIN_DISCHARGE_SOC_PERCENT = {
    "saj": 20.0,
    "solplanet": 10.0,
}
TESLA_POST_EXPORT_FORECAST_WINDOW_START_HOUR = 20
TESLA_POST_EXPORT_FORECAST_WINDOW_END_HOUR = 11
TESLA_POST_EXPORT_FORECAST_HISTORY_DAYS = 3
TESLA_POST_EXPORT_FORECAST_MIN_COVERAGE_RATIO = 0.5
FREE_ENERGY_WINDOW_START_HOUR = 11
FREE_ENERGY_WINDOW_END_HOUR = 14
FREE_ENERGY_WINDOW_SELF_CONSUMPTION_START_MINUTE = 50
AFTER_FREE_SHOULDER_WINDOW_START_HOUR = 14
AFTER_FREE_SHOULDER_WINDOW_END_HOUR = 16
AFTER_FREE_PEAK_WINDOW_START_HOUR = 16
AFTER_FREE_PEAK_WINDOW_END_HOUR = 18
EXPORT_WINDOW_START_HOUR = 18
EXPORT_WINDOW_END_HOUR = 20
POST_EXPORT_PEAK_WINDOW_START_HOUR = 20
POST_EXPORT_PEAK_WINDOW_END_HOUR = 23
OVERNIGHT_SHOULDER_WINDOW_START_HOUR = 23
OVERNIGHT_SHOULDER_WINDOW_END_HOUR = 11
SAJ_BATTERY_WATCH_REMINDER_THRESHOLD_HIGH_SOC_PERCENT = 50.0
SAJ_BATTERY_WATCH_REMINDER_THRESHOLD_LOW_SOC_PERCENT = 20.0
POST_EXPORT_PEAK_WINDOW_TARIFF_P_PER_KWH = 53.0
TESLA_GRID_SUPPORT_TARGET_MIN_W = 14_000.0
TESLA_GRID_SUPPORT_TARGET_MAX_W = 15_000.0
TESLA_GRID_SUPPORT_HARD_MAX_W = 15_500.0
TESLA_GRID_SUPPORT_CURRENT_OPTIONS_A: tuple[int, ...] = (10, 15)
TESLA_SOLAR_SURPLUS_START_SOLPLANET_SOC_PERCENT = 95.0
TESLA_SOLAR_SURPLUS_STOP_SOLPLANET_SOC_PERCENT = 90.0
TESLA_SOLAR_SURPLUS_MIN_EXPORT_W = 150.0
SOLPLANET_LOW_BATTERY_NOTIFICATION_SOC_PERCENT = 20.0
SOLPLANET_LOW_AVAILABLE_CAPACITY_NOTIFICATION_THRESHOLD_KWH = 25.0
BATTERY_FULL_NOTIFICATION_SOC_PERCENT = 100.0
SOLAR_SURPLUS_EXPORT_ENERGY_NOTIFICATION_THRESHOLDS_WH: tuple[float, ...] = (5_000.0, 9_000.0)
TESLA_CURRENT_MISMATCH_RESTART_MIN_DELTA_A = 2.0
TESLA_CURRENT_MISMATCH_RESTART_COOLDOWN_SECONDS = 300.0
WORKER_PENDING_LOG_TIMEOUT_SECONDS = 60.0
SAMPLE_PERSIST_MAX_ATTEMPTS = 5
SAMPLE_PERSIST_ATTEMPT_TIMEOUT_SECONDS = 5.0
SAMPLE_PERSIST_RETRY_BACKOFF_SECONDS: tuple[float, ...] = (0.2, 0.5, 1.0, 2.0)
BATTERY_FULL_NOTIFICATION_STATE_PREFIX = "notification_state."
SAJ_BATTERY_FULL_ACTIVE_KEY = f"{BATTERY_FULL_NOTIFICATION_STATE_PREFIX}saj_battery_full_active"
SOLPLANET_BATTERY_FULL_ACTIVE_KEY = f"{BATTERY_FULL_NOTIFICATION_STATE_PREFIX}solplanet_battery_full_active"
TESLA_LOG_SYSTEM = "tesla"
TESLA_OBSERVATION_SERVICE = "tesla"
SAJ_COLLECTION_SERVICE = "saj_collection"
SOLPLANET_COLLECTION_SERVICE = "solplanet_collection"
NOTIFICATION_LOG_SYSTEM = "notification"
OPERATION_LOG_SYSTEM = "operation"
NOTIFICATION_SUMMARY_SERVICE = "notification"
OPERATION_SUMMARY_SERVICE = "operation"
WINDOW_CHECK_FREE_ENERGY_SERVICE = "free_energy"
WINDOW_CHECK_AFTER_FREE_SHOULDER_SERVICE = "after_free_shoulder"
WINDOW_CHECK_AFTER_FREE_PEAK_SERVICE = "after_free_peak"
WINDOW_CHECK_EXPORT_WINDOW_SERVICE = "export_window"
WINDOW_CHECK_POST_EXPORT_PEAK_SERVICE = "post_export_peak"
OVERNIGHT_SHOULDER_SERVICE = "overnight_shoulder"
WINDOW_CHECK_SERVICES: tuple[str, ...] = (
    WINDOW_CHECK_FREE_ENERGY_SERVICE,
    WINDOW_CHECK_AFTER_FREE_SHOULDER_SERVICE,
    WINDOW_CHECK_AFTER_FREE_PEAK_SERVICE,
    WINDOW_CHECK_EXPORT_WINDOW_SERVICE,
    WINDOW_CHECK_POST_EXPORT_PEAK_SERVICE,
)
WORKER_SUMMARY_SERVICES: tuple[tuple[str, str], ...] = (
    ("saj", SAJ_COLLECTION_SERVICE),
    ("solplanet", SOLPLANET_COLLECTION_SERVICE),
    ("combined", "combined_assembly"),
    (TESLA_LOG_SYSTEM, TESLA_OBSERVATION_SERVICE),
    (NOTIFICATION_LOG_SYSTEM, NOTIFICATION_SUMMARY_SERVICE),
    (OPERATION_LOG_SYSTEM, OPERATION_SUMMARY_SERVICE),
)
WORKER_LOG_LEGACY_STATUSES: tuple[str, ...] = (
    "notification",
    "no_notification",
    "notified",
    "alarmed",
    "operation",
    "nop",
)
TIME_WINDOW_RULE_DEFINITIONS: tuple[dict[str, str], ...] = (
    {"code": "saj_profile_free_energy", "window": "free_energy", "kind": "operation"},
    {"code": "tesla_free_energy_control", "window": "free_energy", "kind": "operation"},
    {"code": "saj_profile_self_consumption", "window": "after_free_shoulder", "kind": "operation"},
    {"code": "saj_profile_self_consumption", "window": "after_free_peak", "kind": "operation"},
    {"code": "saj_profile_self_consumption", "window": "export_window", "kind": "operation"},
    {"code": "saj_profile_self_consumption", "window": "post_export_peak", "kind": "operation"},
    {"code": "saj_profile_self_consumption", "window": "overnight_shoulder", "kind": "operation"},
    {"code": "tesla_after_free_shoulder_control", "window": "after_free_shoulder", "kind": "operation"},
    {"code": "tesla_after_free_peak_control", "window": "after_free_peak", "kind": "operation"},
    {"code": "solplanet_low_available_capacity", "window": "after_free_shoulder", "kind": "notification"},
    {"code": "solplanet_low_available_capacity", "window": "after_free_peak", "kind": "notification"},
    {"code": "solplanet_low_battery", "window": "export_window", "kind": "notification"},
    {"code": "grid_import_started", "window": "export_window", "kind": "notification"},
    {"code": "solar_surplus_export_energy_reached_5000", "window": "export_window", "kind": "notification"},
    {"code": "solar_surplus_export_energy_reached_9000", "window": "export_window", "kind": "notification"},
    {"code": "solplanet_low_battery_post_export_peak", "window": "post_export_peak", "kind": "notification"},
    {"code": "grid_import_started_post_export_peak", "window": "post_export_peak", "kind": "notification"},
    {"code": "saj_battery_watch_50_percent", "window": "overnight_shoulder", "kind": "notification"},
    {"code": "saj_battery_watch_20_percent", "window": "overnight_shoulder", "kind": "notification"},
    {"code": "saj_battery_full", "window": "always", "kind": "notification"},
    {"code": "solplanet_battery_full", "window": "always", "kind": "notification"},
)
TIME_WINDOW_RULE_CODES: tuple[str, ...] = tuple(dict.fromkeys(item["code"] for item in TIME_WINDOW_RULE_DEFINITIONS))
TIME_WINDOW_RULE_WINDOWS_BY_CODE: dict[str, tuple[str, ...]] = {
    code: tuple(item["window"] for item in TIME_WINDOW_RULE_DEFINITIONS if item["code"] == code)
    for code in TIME_WINDOW_RULE_CODES
}
MANUAL_OPERATION_DEFINITIONS: tuple[dict[str, object], ...] = (
    {
        "operation_code": "tesla.manual.toggle_charging",
        "system": "tesla",
        "operation_type": "charging",
        "action": "toggle",
        "title": "Tesla Charging Toggle",
        "input_schema": {"enabled": "boolean"},
        "output_schema": {"control_state": "object", "observation": "object", "feedback": "object"},
        "config": {"confirmation_mode": "ha_state_refresh"},
    },
    {
        "operation_code": "tesla.manual.set_charge_current",
        "system": "tesla",
        "operation_type": "charging",
        "action": "set_current",
        "title": "Tesla Charge Current",
        "input_schema": {"amps": "integer"},
        "output_schema": {"control_state": "object", "observation": "object"},
        "config": {"confirmation_mode": "ha_state_refresh"},
    },
    {
        "operation_code": "saj.manual.set_working_mode",
        "system": "saj",
        "operation_type": "inverter_control",
        "action": "set_working_mode",
        "title": "SAJ Working Mode",
        "input_schema": {"mode_code": "integer"},
        "output_schema": {"state": "object", "changed": "array"},
    },
    {
        "operation_code": "saj.manual.apply_profile",
        "system": "saj",
        "operation_type": "inverter_control",
        "action": "apply_profile",
        "title": "SAJ Control Profile",
        "input_schema": {"profile_id": "string"},
        "output_schema": {"state": "object"},
    },
    {
        "operation_code": "saj.manual.set_charge_slot",
        "system": "saj",
        "operation_type": "schedule_control",
        "action": "set_charge_slot",
        "title": "SAJ Charge Slot",
        "input_schema": {"slot": "integer", "payload": "object"},
        "output_schema": {"state": "object"},
    },
    {
        "operation_code": "saj.manual.set_discharge_slot",
        "system": "saj",
        "operation_type": "schedule_control",
        "action": "set_discharge_slot",
        "title": "SAJ Discharge Slot",
        "input_schema": {"slot": "integer", "payload": "object"},
        "output_schema": {"state": "object"},
    },
    {
        "operation_code": "saj.manual.set_toggles",
        "system": "saj",
        "operation_type": "inverter_control",
        "action": "set_toggles",
        "title": "SAJ Toggle Controls",
        "input_schema": {"payload": "object"},
        "output_schema": {"state": "object", "changed": "array"},
    },
    {
        "operation_code": "saj.manual.set_limits",
        "system": "saj",
        "operation_type": "power_limit_control",
        "action": "set_limits",
        "title": "SAJ Power Limits",
        "input_schema": {"payload": "object"},
        "output_schema": {"state": "object", "changed": "array"},
    },
    {
        "operation_code": "saj.manual.refresh_touch",
        "system": "saj",
        "operation_type": "maintenance",
        "action": "refresh_touch",
        "title": "SAJ Passive Control Refresh",
        "input_schema": {},
        "output_schema": {"state": "object", "entity_id": "string", "kept_state": "boolean"},
    },
    {
        "operation_code": "solplanet.manual.set_limits",
        "system": "solplanet",
        "operation_type": "power_limit_control",
        "action": "set_limits",
        "title": "Solplanet Limits",
        "input_schema": {"payload": "object"},
        "output_schema": {"state": "object", "changed": "array"},
    },
    {
        "operation_code": "solplanet.manual.set_day_schedule",
        "system": "solplanet",
        "operation_type": "schedule_control",
        "action": "set_day_schedule",
        "title": "Solplanet Day Schedule",
        "input_schema": {"day": "string", "slots": "array"},
        "output_schema": {"state": "object", "changed": "array"},
    },
    {
        "operation_code": "solplanet.manual.set_day_schedule_slot",
        "system": "solplanet",
        "operation_type": "schedule_control",
        "action": "set_day_schedule_slot",
        "title": "Solplanet Day Schedule Slot",
        "input_schema": {"day": "string", "slot": "integer", "payload": "object"},
        "output_schema": {"state": "object", "changed": "array"},
    },
    {
        "operation_code": "solplanet.manual.set_raw_setting",
        "system": "solplanet",
        "operation_type": "advanced_control",
        "action": "set_raw_setting",
        "title": "Solplanet Raw Setting",
        "input_schema": {"payload": "object"},
        "output_schema": {"state": "object", "changed": "array"},
    },
    {
        "operation_code": "backend.manual.restart_collector",
        "system": "backend",
        "operation_type": "maintenance",
        "action": "restart_collector",
        "title": "Collector Restart",
        "input_schema": {},
        "output_schema": {"running": "boolean", "restarted_at": "string"},
    },
)


def _solar_surplus_export_energy_notification_code(threshold_wh: float) -> str:
    return f"solar_surplus_export_energy_reached_{int(round(threshold_wh))}"


def _is_solar_surplus_export_energy_notification_code(code: object) -> bool:
    return str(code or "").strip().startswith("solar_surplus_export_energy_reached_")

app = FastAPI(title="Wattimize API", version="0.1.0")
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

INDEX_CACHE_PLACEHOLDER = "__WATTIMIZE_STATIC_VERSION__"
static_asset_version = "dev"

solplanet_flow_cache: dict[str, object] = {"payload": None, "at_monotonic": 0.0}
solplanet_flow_lock = asyncio.Lock()
solplanet_context_cache: dict[str, object] = {"payload": None, "at_monotonic": 0.0}
solplanet_context_lock = asyncio.Lock()
runtime_lock = asyncio.Lock()
db_recovery_lock = asyncio.Lock()
db_recovery_state: dict[str, object] = {
    "in_progress": False,
    "last_attempt_at": None,
    "last_success_at": None,
    "last_error_at": None,
    "last_error": None,
    "last_result": None,
}
collector_task: asyncio.Task[None] | None = None
collector_stop_event = asyncio.Event()
storage_db_path = Path(os.getenv("WATTIMIZE_DB_PATH", "").strip() or DEFAULT_DB_PATH)
worker_failure_log_path = Path(
    os.getenv("WATTIMIZE_WORKER_FAILURE_LOG_PATH", "").strip() or storage_db_path.with_name("worker_failures.log")
)
request_actor_ctx: ContextVar[str] = ContextVar("request_actor_ctx", default="api")
request_system_ctx: ContextVar[str | None] = ContextVar("request_system_ctx", default=None)
request_round_ctx: ContextVar[str | None] = ContextVar("request_round_ctx", default=None)
request_round_started_at_ctx: ContextVar[str | None] = ContextVar("request_round_started_at_ctx", default=None)
sample_interval_seconds = float(settings.saj_sample_interval_seconds)
solplanet_sample_interval_seconds = float(settings.solplanet_sample_interval_seconds)
COLLECTOR_ROUND_SLEEP_SECONDS = 30.0
ENDPOINT_BACKOFF_MAX_SKIP_ROUNDS = 8
SOLPLANET_ENDPOINTS: tuple[str, ...] = (
    "getdevdata_device_2",
    "getdevdata_device_3",
    "getdevdata_device_4",
)
SAJ_ENDPOINTS: tuple[str, ...] = ("home_assistant_all_states",)
collector_round_number = 0
collector_next_due_monotonic: dict[str, float] = {system: 0.0 for system in POLLED_SYSTEMS}
collector_status: dict[str, dict[str, object]] = {
    "saj": {
        "in_progress": False,
        "continuous": True,
        "interval_seconds": None,
        "round_number": 0,
        "last_started_at": None,
        "last_finished_at": None,
        "last_success_at": None,
        "last_error_at": None,
        "last_error": None,
        "last_persistence_error_at": None,
        "last_persistence_error": None,
        "last_persistence_error_operation": None,
        "last_persistence_error_attempts": 0,
        "last_persistence_error_round_id": None,
        "last_duration_ms": None,
        "success_count": 0,
        "failure_count": 0,
    },
    "solplanet": {
        "in_progress": False,
        "continuous": True,
        "interval_seconds": None,
        "round_number": 0,
        "last_started_at": None,
        "last_finished_at": None,
        "last_success_at": None,
        "last_error_at": None,
        "last_error": None,
        "last_persistence_error_at": None,
        "last_persistence_error": None,
        "last_persistence_error_operation": None,
        "last_persistence_error_attempts": 0,
        "last_persistence_error_round_id": None,
        "last_duration_ms": None,
        "success_count": 0,
        "failure_count": 0,
    },
    "combined": {
        "in_progress": False,
        "continuous": True,
        "interval_seconds": None,
        "round_number": 0,
        "last_started_at": None,
        "last_finished_at": None,
        "last_success_at": None,
        "last_error_at": None,
        "last_error": None,
        "last_persistence_error_at": None,
        "last_persistence_error": None,
        "last_persistence_error_operation": None,
        "last_persistence_error_attempts": 0,
        "last_persistence_error_round_id": None,
        "last_duration_ms": None,
        "success_count": 0,
        "failure_count": 0,
    },
}


def _compute_static_asset_version() -> str:
    assets = [static_dir / "app.js", static_dir / "styles.css"]
    digest = hashlib.sha256()
    found = False
    for asset in assets:
        if not asset.exists():
            continue
        found = True
        digest.update(asset.name.encode("utf-8"))
        digest.update(asset.read_bytes())
    if not found:
        return "dev"
    return digest.hexdigest()[:12]


def _render_index_html() -> str:
    index_file = static_dir / "index.html"
    html = index_file.read_text(encoding="utf-8")
    version = static_asset_version or "dev"
    html = html.replace("/static/styles.css", f"/static/styles.css?v={version}")
    html = html.replace("/static/app.js", f"/static/app.js?v={version}")
    return html
collector_endpoint_state: dict[str, dict[str, dict[str, object]]] = {
    "saj": {
        endpoint: {
            "consecutive_failures": 0,
            "skip_until_round": 0,
            "last_attempt_round": None,
            "last_success_round": None,
            "last_error": None,
        }
        for endpoint in SAJ_ENDPOINTS
    },
    "solplanet": {
        endpoint: {
            "consecutive_failures": 0,
            "skip_until_round": 0,
            "last_attempt_round": None,
            "last_success_round": None,
            "last_error": None,
        }
        for endpoint in SOLPLANET_ENDPOINTS
    },
}


def _append_worker_failure_log(
    system: str,
    *,
    stage: str,
    error: Exception,
    started_monotonic: float | None = None,
    extra: dict[str, object] | None = None,
) -> None:
    timestamp = datetime.now(UTC).isoformat()
    duration_ms = round((monotonic() - started_monotonic) * 1000, 1) if started_monotonic is not None else None
    traceback_text = "".join(traceback.format_exception(type(error), error, error.__traceback__)).strip()
    lines = [
        "=" * 80,
        f"time_utc: {timestamp}",
        f"system: {system}",
        f"stage: {stage}",
    ]
    if duration_ms is not None:
        lines.append(f"duration_ms: {duration_ms}")
    lines.append(f"error: {type(error).__name__}: {error}")
    if extra:
        for key, value in extra.items():
            lines.append(f"{key}: {value}")
    lines.extend(
        [
            "traceback:",
            traceback_text or f"{type(error).__name__}: {error}",
            "",
        ]
    )
    try:
        worker_failure_log_path.parent.mkdir(parents=True, exist_ok=True)
        with worker_failure_log_path.open("a", encoding="utf-8") as fh:
            fh.write("\n".join(lines))
    except Exception as log_exc:  # noqa: BLE001
        logger.warning("Failed to append worker failure log: %s", log_exc)


def _is_retryable_sqlite_error(error: BaseException) -> bool:
    if isinstance(error, sqlite3.Error):
        return True
    cause = getattr(error, "__cause__", None)
    if isinstance(cause, sqlite3.Error):
        return True
    text = str(error).strip().lower()
    return bool(text) and any(
        token in text
        for token in (
            "sqlite",
            "disk i/o error",
            "database is locked",
            "database is busy",
            "unable to open database file",
        )
    )


def _sqlite_error_indicates_corruption(error: BaseException) -> bool:
    text = str(error).strip().lower()
    return bool(text) and any(
        token in text
        for token in (
            "database disk image is malformed",
            "disk i/o error",
            "file is not a database",
            "btreeinitpage",
        )
    )


async def _attempt_storage_db_recovery(
    *,
    trigger_error: BaseException,
    trace_system: str,
    trace_round_id: str | None,
    operation_name: str,
) -> dict[str, object]:
    async with db_recovery_lock:
        db_recovery_state["in_progress"] = True
        db_recovery_state["last_attempt_at"] = datetime.now(UTC).isoformat()
        try:
            await asyncio.to_thread(dispose_db_connections, storage_db_path)
            inspection = await asyncio.to_thread(inspect_sqlite_database, storage_db_path)
            if bool(inspection.get("ok")):
                result = {
                    "status": "inspection_ok",
                    "inspection": inspection,
                }
                db_recovery_state["last_result"] = result
                db_recovery_state["last_success_at"] = datetime.now(UTC).isoformat()
                db_recovery_state["last_error"] = None
                return result

            recovery_dir = storage_db_path.parent / "recovery"
            recovery_result = await asyncio.to_thread(
                recover_sqlite_database,
                storage_db_path,
                recovery_dir=recovery_dir,
            )
            await asyncio.to_thread(init_db, storage_db_path)
            await asyncio.to_thread(migrate_worker_log_legacy_statuses, storage_db_path)
            result = {
                "status": "recovered",
                "inspection": inspection,
                "recovery": recovery_result,
            }
            db_recovery_state["last_result"] = result
            db_recovery_state["last_success_at"] = datetime.now(UTC).isoformat()
            db_recovery_state["last_error"] = None
            _append_worker_failure_log(
                trace_system,
                stage=f"{operation_name}_auto_recovery",
                error=RuntimeError("storage_db_auto_recovered"),
                extra={
                    "round_id": trace_round_id,
                    "trigger_error": f"{type(trigger_error).__name__}: {trigger_error}",
                    "recovered_path": str((result.get("recovery") or {}).get("recovered_path") or ""),
                },
            )
            return result
        except Exception as recovery_exc:  # noqa: BLE001
            db_recovery_state["last_error_at"] = datetime.now(UTC).isoformat()
            db_recovery_state["last_error"] = f"{type(recovery_exc).__name__}: {recovery_exc}"
            _append_worker_failure_log(
                trace_system,
                stage=f"{operation_name}_auto_recovery_failed",
                error=recovery_exc,
                extra={
                    "round_id": trace_round_id,
                    "trigger_error": f"{type(trigger_error).__name__}: {trigger_error}",
                },
            )
            raise
        finally:
            db_recovery_state["in_progress"] = False


async def _run_db_call_with_retry(
    func: Callable[..., object],
    *args: object,
    operation_name: str,
    trace_system: str,
    trace_round_id: str | None = None,
    max_attempts: int = SAMPLE_PERSIST_MAX_ATTEMPTS,
    attempt_timeout_seconds: float = SAMPLE_PERSIST_ATTEMPT_TIMEOUT_SECONDS,
    retry_backoff_seconds: tuple[float, ...] = SAMPLE_PERSIST_RETRY_BACKOFF_SECONDS,
    extra_failure_context: dict[str, object] | None = None,
    **kwargs: object,
) -> object:
    safe_attempts = max(1, int(max_attempts))
    last_error: BaseException | None = None
    for attempt in range(1, safe_attempts + 1):
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(func, *args, **kwargs),
                timeout=max(0.1, float(attempt_timeout_seconds)),
            )
        except Exception as exc:  # noqa: BLE001
            retryable = _is_retryable_sqlite_error(exc) or isinstance(exc, asyncio.TimeoutError)
            corruption_suspected = _sqlite_error_indicates_corruption(exc)
            failure_context = {
                "operation_name": operation_name,
                "attempt": attempt,
                "max_attempts": safe_attempts,
                "retryable": retryable,
                "corruption_suspected": corruption_suspected,
            }
            if trace_round_id:
                failure_context["round_id"] = trace_round_id
            if extra_failure_context:
                failure_context.update(extra_failure_context)
            _append_worker_failure_log(
                trace_system,
                stage=f"{operation_name}_attempt_{attempt}",
                error=exc,
                extra=failure_context,
            )
            last_error = exc
            if not retryable or attempt >= safe_attempts:
                break
            if corruption_suspected:
                try:
                    await _attempt_storage_db_recovery(
                        trigger_error=exc,
                        trace_system=trace_system,
                        trace_round_id=trace_round_id,
                        operation_name=operation_name,
                    )
                except Exception:
                    break
            if attempt == 1:
                try:
                    await asyncio.to_thread(dispose_db_connections, storage_db_path)
                except Exception as dispose_exc:  # noqa: BLE001
                    _append_worker_failure_log(
                        trace_system,
                        stage=f"{operation_name}_dispose_connections",
                        error=dispose_exc,
                        extra={"round_id": trace_round_id} if trace_round_id else None,
                    )
            backoff_index = min(attempt - 1, len(retry_backoff_seconds) - 1)
            await asyncio.sleep(retry_backoff_seconds[backoff_index])
    assert last_error is not None
    raise last_error


def _record_persistence_failure(
    system: str,
    *,
    operation_name: str,
    error: BaseException,
    round_id: str | None = None,
    attempts: int = SAMPLE_PERSIST_MAX_ATTEMPTS,
) -> None:
    status = collector_status.setdefault(system, {})
    status["last_persistence_error_at"] = datetime.now(UTC).isoformat()
    status["last_persistence_error"] = f"{type(error).__name__}: {error}"
    status["last_persistence_error_operation"] = operation_name
    status["last_persistence_error_attempts"] = attempts
    status["last_persistence_error_round_id"] = round_id


def _clear_persistence_failure(system: str) -> None:
    status = collector_status.setdefault(system, {})
    status["last_persistence_error"] = None
    status["last_persistence_error_operation"] = None
    status["last_persistence_error_attempts"] = 0
    status["last_persistence_error_round_id"] = None


def _read_worker_failure_log(*, limit: int = 100, before: int = 0) -> dict[str, object]:
    safe_limit = max(1, min(500, int(limit)))
    safe_before = max(0, int(before))
    if not worker_failure_log_path.exists():
        return {
            "path": str(worker_failure_log_path),
            "total_lines": 0,
            "count": 0,
            "limit": safe_limit,
            "before": safe_before,
            "next_before": safe_before,
            "has_more": False,
            "from_line": None,
            "to_line": None,
            "lines": [],
        }

    with worker_failure_log_path.open("r", encoding="utf-8", errors="replace") as fh:
        all_lines = fh.read().splitlines()

    total_lines = len(all_lines)
    end_index = max(0, total_lines - safe_before)
    start_index = max(0, end_index - safe_limit)
    selected_lines = all_lines[start_index:end_index]
    items = [
        {"number": start_index + index + 1, "text": text}
        for index, text in enumerate(selected_lines)
    ]
    return {
        "path": str(worker_failure_log_path),
        "total_lines": total_lines,
        "count": len(items),
        "limit": safe_limit,
        "before": safe_before,
        "next_before": safe_before + len(items),
        "has_more": start_index > 0,
        "from_line": items[0]["number"] if items else None,
        "to_line": items[-1]["number"] if items else None,
        "lines": items,
    }


def _worker_log_request_token(round_id: str | None, service: str, method: str, api_link: str) -> str:
    normalized_round_id = str(round_id or "").strip()
    if not normalized_round_id:
        return ""
    raw = f"{normalized_round_id}|{str(service or '').strip().lower()}|{str(method or 'GET').upper()}|{str(api_link or '').strip()}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _worker_round_log_plan(round_number: int, round_id: str, requested_at_utc: str) -> list[dict[str, object]]:
    plan: list[dict[str, object]] = []

    saj_url = f"{settings.ha_url}/api/states" if settings.ha_url else ""
    saj_endpoint = "home_assistant_all_states"
    saj_status = WORKER_LOG_STATUS_PENDING if _endpoint_is_eligible("saj", saj_endpoint, round_number) else WORKER_LOG_STATUS_SKIPPED
    plan.append(
        {
            "request_token": _worker_log_request_token(round_id, "home_assistant", "GET", saj_url),
            "round_id": round_id,
            "worker": "worker",
            "system": "saj",
            "service": "home_assistant",
            "method": "GET",
            "api_link": saj_url,
            "requested_at_utc": requested_at_utc,
            "ok": False,
            "status": saj_status,
            "status_code": None,
            "duration_ms": None,
            "result_text": None,
            "error_text": "endpoint_backoff" if saj_status == WORKER_LOG_STATUS_SKIPPED else None,
        }
    )

    solplanet_paths = {
        "getdevdata_device_2": f"getdevdata.cgi?device=2&sn={settings.solplanet_inverter_sn}",
        "getdevdata_device_3": "getdevdata.cgi?device=3",
        "getdevdata_device_4": f"getdevdata.cgi?device=4&sn={settings.solplanet_battery_sn}",
    }
    for endpoint in SOLPLANET_ENDPOINTS:
        path = solplanet_paths.get(endpoint, endpoint)
        api_link = (
            f"{settings.solplanet_dongle_scheme}://{settings.solplanet_dongle_host}:{settings.solplanet_dongle_port}/{path}"
            if settings.solplanet_dongle_host
            else path
        )
        endpoint_status = WORKER_LOG_STATUS_PENDING if _endpoint_is_eligible("solplanet", endpoint, round_number) else WORKER_LOG_STATUS_SKIPPED
        plan.append(
            {
                "request_token": _worker_log_request_token(round_id, "solplanet_cgi", "GET", api_link),
                "round_id": round_id,
                "worker": "worker",
                "system": "solplanet",
                "service": "solplanet_cgi",
                "method": "GET",
                "api_link": api_link,
                "requested_at_utc": requested_at_utc,
                "ok": False,
                "status": endpoint_status,
                "status_code": None,
                "duration_ms": None,
                "result_text": None,
                "error_text": "endpoint_backoff" if endpoint_status == WORKER_LOG_STATUS_SKIPPED else None,
            }
        )

    for system, service in WORKER_SUMMARY_SERVICES:
        api_link = f"worker://{system}/{service}"
        plan.append(
            {
                "request_token": _worker_log_request_token(round_id, service, "AUTO", api_link),
                "round_id": round_id,
                "worker": "worker",
                "system": system,
                "service": service,
                "method": "AUTO",
                "api_link": api_link,
                "requested_at_utc": requested_at_utc,
                "ok": False,
                "status": WORKER_LOG_STATUS_PENDING,
                "status_code": None,
                "duration_ms": None,
                "result_text": None,
                "error_text": None,
            }
        )

    return plan


async def _insert_worker_round_log_plan(round_number: int, round_id: str, requested_at_utc: str) -> None:
    plan = _worker_round_log_plan(round_number, round_id, requested_at_utc)
    for item in plan:
        item_payload = dict(item)
        item_system = str(item_payload.pop("system", "") or "combined")
        await _run_db_call_with_retry(
            insert_worker_api_log,
            storage_db_path,
            operation_name="worker_round_log_plan",
            trace_system=item_system,
            trace_round_id=round_id,
            extra_failure_context={"service": str(item_payload.get("service") or "")},
            system=item_system,
            **item_payload,
        )


def _handle_outbound_request_log(event: dict[str, object]) -> None:
    if request_actor_ctx.get() != "worker":
        return
    actual_requested_at = str(event.get("requested_at_utc") or datetime.now(UTC).isoformat())
    phase = str(event.get("phase") or "finish").strip().lower()
    worker = request_actor_ctx.get() or "worker"
    system = request_system_ctx.get()
    round_id = str(request_round_ctx.get() or "").strip()
    round_started_at = str(request_round_started_at_ctx.get() or "").strip()
    service = str(event.get("service") or "unknown")
    method = str(event.get("method") or "GET").upper()
    api_link = str(event.get("url") or "")
    request_token = _worker_log_request_token(round_id, service, method, api_link) if round_id else str(event.get("request_token") or "").strip()
    ok = bool(event.get("ok"))
    status_code_value = event.get("status_code")
    status_code = int(status_code_value) if isinstance(status_code_value, (int, float)) else None
    duration_raw = event.get("duration_ms")
    duration_ms = round(float(duration_raw), 1) if isinstance(duration_raw, (int, float)) else None
    result_text = str(event.get("result_text") or "")
    error_text = str(event.get("error_text") or "")
    response_json = event.get("response_json")
    endpoint = _endpoint_name_from_event(service=service, method=method, url=api_link)
    parsed_url = urlparse(api_link)
    worker_requested_at = round_started_at or actual_requested_at

    try:
        if phase == "start":
            return
        if round_id:
            insert_raw_request_result(
                storage_db_path,
                round_id=round_id,
                system=system,
                source=service,
                endpoint=endpoint,
                method=method,
                request_url=api_link,
                requested_at_utc=actual_requested_at,
                duration_ms=duration_ms,
                ok=ok,
                status_code=status_code,
                error_text=error_text or None,
                response_text=result_text or None,
                response_json=response_json,
            )
        if request_token:
            update_worker_api_log(
                storage_db_path,
                request_token=request_token,
                ok=ok,
                status=WORKER_LOG_STATUS_OK if ok else WORKER_LOG_STATUS_FAILED,
                status_code=status_code,
                duration_ms=duration_ms,
                result_text=result_text,
                error_text=error_text,
            )
        else:
            insert_worker_api_log(
                storage_db_path,
                request_token=request_token or None,
                round_id=round_id or None,
                worker=worker,
                system=system,
                service=service,
                method=method,
                api_link=api_link,
                requested_at_utc=worker_requested_at,
                ok=ok,
                status=WORKER_LOG_STATUS_OK if ok else WORKER_LOG_STATUS_FAILED,
                status_code=status_code,
                duration_ms=duration_ms,
                result_text=result_text,
                error_text=error_text,
            )
    except Exception as exc:
        logger.warning("Failed to persist worker_api_log for %s %s: %s", service, api_link, exc)


def _endpoint_name_from_event(*, service: str, method: str, url: str) -> str:
    normalized_service = str(service or "").strip().lower()
    normalized_method = str(method or "GET").upper()
    lowered_url = str(url or "").lower()
    if normalized_service == "home_assistant":
        if "/api/states" in lowered_url and normalized_method == "GET":
            return "home_assistant_all_states"
        if "/api/" in lowered_url:
            return f"home_assistant:{normalized_method}:{lowered_url.split('/api/', 1)[1]}"
        return f"home_assistant:{normalized_method}"
    if normalized_service == "solplanet_cgi":
        mapping = {
            "getdev.cgi?device=0": "getdev_device_0",
            "getdev.cgi?device=2": "getdev_device_2",
            "getdev.cgi?device=3": "getdev_device_3",
            "getdev.cgi?device=4": "getdev_device_4",
            "getdevdata.cgi?device=2": "getdevdata_device_2",
            "getdevdata.cgi?device=3": "getdevdata_device_3",
            "getdevdata.cgi?device=4": "getdevdata_device_4",
            "getdevdata.cgi?device=5": "getdevdata_device_5",
            "getdefine.cgi": "getdefine",
        }
        for needle, endpoint in mapping.items():
            if needle in lowered_url:
                return endpoint
        return f"solplanet_cgi:{normalized_method}"
    return f"{normalized_service}:{normalized_method}"


def _snapshot_endpoint_backoff_state(system: str) -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    for endpoint, state in (collector_endpoint_state.get(system) or {}).items():
        out[endpoint] = dict(state)
    return out


def _endpoint_is_eligible(system: str, endpoint: str, round_number: int) -> bool:
    state = (collector_endpoint_state.get(system) or {}).get(endpoint) or {}
    skip_until_round = int(state.get("skip_until_round") or 0)
    return round_number >= skip_until_round


def _mark_endpoint_attempt(system: str, endpoint: str, round_number: int) -> None:
    state = collector_endpoint_state.setdefault(system, {}).setdefault(endpoint, {})
    state["last_attempt_round"] = round_number


def _mark_endpoint_success(system: str, endpoint: str, round_number: int) -> None:
    state = collector_endpoint_state.setdefault(system, {}).setdefault(endpoint, {})
    state["consecutive_failures"] = 0
    state["skip_until_round"] = round_number
    state["last_attempt_round"] = round_number
    state["last_success_round"] = round_number
    state["last_error"] = None


def _mark_endpoint_failure(system: str, endpoint: str, round_number: int, error_text: str) -> None:
    state = collector_endpoint_state.setdefault(system, {}).setdefault(endpoint, {})
    failures = int(state.get("consecutive_failures") or 0) + 1
    skip_rounds = min(2 ** (failures - 1), ENDPOINT_BACKOFF_MAX_SKIP_ROUNDS)
    state["consecutive_failures"] = failures
    state["skip_until_round"] = round_number + skip_rounds + 1
    state["last_attempt_round"] = round_number
    state["last_error"] = error_text


def _mark_endpoint_skipped(system: str, endpoint: str, round_number: int) -> None:
    state = collector_endpoint_state.setdefault(system, {}).setdefault(endpoint, {})
    state["last_skipped_round"] = round_number


def _review_round_results(results: dict[str, dict[str, object]]) -> dict[str, object]:
    review: dict[str, object] = {}
    for system, payload in results.items():
        attempted = [str(name) for name in payload.get("attempted", []) if name]
        succeeded = [str(name) for name in payload.get("succeeded", []) if name]
        failed = [str(name) for name in payload.get("failed", []) if name]
        skipped = [str(name) for name in payload.get("skipped", []) if name]
        review[system] = {
            "attempted_count": len(attempted),
            "success_count": len(succeeded),
            "failure_count": len(failed),
            "skipped_count": len(skipped),
            "attempted": attempted,
            "succeeded": succeeded,
            "failed": failed,
            "skipped": skipped,
        }
    return review


_COMPACT_RESULT_KEY_METRIC_FIELDS: tuple[str, ...] = (
    "pv_w",
    "grid_w",
    "battery_w",
    "battery1_w",
    "battery2_w",
    "load_w",
    "inverter_power_w",
    "inverter1_w",
    "inverter2_w",
    "battery_soc_percent",
    "battery1_soc_percent",
    "battery2_soc_percent",
    "battery_energy_kwh",
    "inverter_status",
    "inverter1_status",
    "inverter2_status",
    "tesla_charge_power_w",
)


def _compact_round_result_for_status(payload: dict[str, object]) -> dict[str, object]:
    flow = payload.get("flow")
    flow_map = flow if isinstance(flow, dict) else {}
    metrics = flow_map.get("metrics")
    metrics_map = metrics if isinstance(metrics, dict) else {}
    key_metrics = {k: metrics_map[k] for k in _COMPACT_RESULT_KEY_METRIC_FIELDS if metrics_map.get(k) is not None}
    return {
        "stored_sample": bool(payload.get("stored_sample")),
        "reason": payload.get("reason"),
        "error": payload.get("error"),
        "missing_metrics": payload.get("missing_metrics"),
        "source_details": payload.get("source_details"),
        "attempted": [str(name) for name in payload.get("attempted", []) if name],
        "succeeded": [str(name) for name in payload.get("succeeded", []) if name],
        "failed": [str(name) for name in payload.get("failed", []) if name],
        "skipped": [str(name) for name in payload.get("skipped", []) if name],
        "flow_updated_at": flow_map.get("updated_at") if isinstance(flow_map, dict) else None,
        "key_metrics": key_metrics or None,
    }


def _combined_assembly_log_payload(
    *,
    round_number: int,
    round_id: str,
    saj_result: dict[str, object],
    solplanet_result: dict[str, object],
    combined_result: dict[str, object],
) -> dict[str, object]:
    logged_at = datetime.now(UTC)
    return {
        "round_number": round_number,
        "round_id": round_id,
        "combined_logged_at_utc": logged_at.isoformat(),
        "next_worker_round_due_at_utc": (logged_at + timedelta(seconds=COLLECTOR_ROUND_SLEEP_SECONDS)).isoformat(),
        "collector_round_sleep_seconds": COLLECTOR_ROUND_SLEEP_SECONDS,
        "saj": _compact_round_result_for_status(saj_result),
        "solplanet": _compact_round_result_for_status(solplanet_result),
        "combined": _compact_round_result_for_status(combined_result),
    }


async def _resolve_combined_source_flow(
    system: str,
    round_result: dict[str, object],
) -> tuple[dict[str, object] | None, dict[str, object]]:
    flow = round_result.get("flow")
    if bool(round_result.get("stored_sample")) and isinstance(flow, dict):
        return flow, {
            "system": system,
            "origin": "current_round",
            "available": True,
            "reason": "current_round_sample",
            "updated_at": flow.get("updated_at"),
            "stored_sample": True,
        }

    try:
        cached_flow = await _get_energy_flow_from_realtime_kv(system)
    except Exception as exc:  # noqa: BLE001
        return None, {
            "system": system,
            "origin": "unavailable",
            "available": False,
            "reason": str(round_result.get("reason") or round_result.get("error") or "cache_unavailable"),
            "cache_error": f"{type(exc).__name__}: {exc}",
            "stored_sample": bool(round_result.get("stored_sample")),
        }

    return cached_flow, {
        "system": system,
        "origin": "cached_latest",
        "available": isinstance(cached_flow, dict),
        "reason": str(round_result.get("reason") or "used_cached_latest"),
        "updated_at": cached_flow.get("updated_at") if isinstance(cached_flow, dict) else None,
        "stale": bool(cached_flow.get("stale")) if isinstance(cached_flow, dict) else None,
        "stale_reason": cached_flow.get("stale_reason") if isinstance(cached_flow, dict) else None,
        "stored_sample": bool(round_result.get("stored_sample")),
    }


REQUIRED_FLOW_METRICS: dict[str, tuple[str, ...]] = {
    "saj": (
        "pv_w",
        "grid_w",
        "battery_w",
        "load_w",
        "battery_soc_percent",
        "inverter_status",
        "inverter_power_w",
    ),
    "solplanet": (
        "pv_w",
        "grid_w",
        "battery_w",
        "load_w",
        "battery_soc_percent",
        "inverter_status",
        "inverter_power_w",
    ),
    "combined": (
        "pv_w",
        "grid_w",
        "battery_w",
        "load_w",
        "battery1_w",
        "battery2_w",
        "battery1_soc_percent",
        "battery2_soc_percent",
        "inverter1_w",
        "inverter2_w",
        "inverter1_status",
        "inverter2_status",
    ),
}


def _missing_required_flow_metrics(system: str, flow: dict[str, object]) -> list[str]:
    metrics = flow.get("metrics")
    metrics_map = metrics if isinstance(metrics, dict) else {}
    missing: list[str] = []
    for key in REQUIRED_FLOW_METRICS.get(system, ()):
        value = metrics_map.get(key)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing.append(key)
    return missing


ha_client = HomeAssistantClient(
    base_url=settings.ha_url,
    token=settings.ha_token,
    request_logger=_handle_outbound_request_log,
)
solplanet_client = SolplanetCgiClient(
    host=settings.solplanet_dongle_host,
    port=settings.solplanet_dongle_port,
    scheme=settings.solplanet_dongle_scheme,
    verify_ssl=settings.solplanet_verify_ssl,
    timeout_seconds=settings.solplanet_request_timeout_seconds,
    request_logger=_handle_outbound_request_log,
)


def _new_solplanet_client(current_settings: Settings) -> SolplanetCgiClient:
    return SolplanetCgiClient(
        host=current_settings.solplanet_dongle_host,
        port=current_settings.solplanet_dongle_port,
        scheme=current_settings.solplanet_dongle_scheme,
        verify_ssl=current_settings.solplanet_verify_ssl,
        timeout_seconds=current_settings.solplanet_request_timeout_seconds,
        request_logger=_handle_outbound_request_log,
    )


def _solplanet_collection_round_timeout_seconds(current_settings: Settings) -> float:
    # One Solplanet sampling round may launch up to 8-9 CGI endpoints in parallel.
    # Keep some slack above a single-endpoint timeout so a whole round cannot hang indefinitely.
    endpoint_budget = current_settings.solplanet_request_timeout_seconds * 3.0
    return max(60.0, endpoint_budget)


class ConfigPayload(BaseModel):
    ha_url: str = Field(default="")
    ha_token: str = Field(default="")
    solplanet_dongle_host: str = Field(default="")
    saj_sample_interval_seconds: int = Field(default=CONST_SAJ_SAMPLE_INTERVAL_SECONDS)
    solplanet_sample_interval_seconds: int = Field(default=CONST_SOLPLANET_SAMPLE_INTERVAL_SECONDS)
    weather_lat: float | None = Field(default=None)
    weather_lon: float | None = Field(default=None)


class SolplanetDiscoverPayload(BaseModel):
    solplanet_dongle_host: str = Field(default="")


def _missing_config_fields(payload: dict[str, object]) -> list[str]:
    missing = get_missing_required_fields_from_payload(payload)
    inverter_sn = str(payload.get("solplanet_inverter_sn") or "").strip()
    battery_sn = str(payload.get("solplanet_battery_sn") or "").strip()
    if not inverter_sn:
        missing.append("solplanet_inverter_sn")
    if not battery_sn:
        missing.append("solplanet_battery_sn")
    return missing


SAJ_SLOT_MIN = 1
SAJ_SLOT_MAX = 7
SAJ_DAY_MASK_MIN = 0
SAJ_DAY_MASK_MAX = 127
SAJ_MODE_MIN = 0
SAJ_MODE_MAX = 8
SAJ_RATED_POWER_W = 5000
SAJ_TIME_REGEX = re.compile(r"^(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9])$")
SAJ_PROFILE_IDS: tuple[str, ...] = ("self_consumption", "time_of_use", "microgrid")
SAJ_PROFILE_MODE_CODES: dict[str, int] = {
    "self_consumption": 0,
    "time_of_use": 1,
    "microgrid": 8,
}
SOLPLANET_SLOT_MIN = 1
SOLPLANET_SLOT_MAX = 6
SOLPLANET_LIMIT_MIN = 0
SOLPLANET_LIMIT_MAX = 20000
SOLPLANET_DAY_KEYS = ("Sun", "Mon", "Tus", "Wen", "Thu", "Fri", "Sat")
SOLPLANET_DAY_ALIAS = {
    "sun": "Sun",
    "mon": "Mon",
    "tue": "Tus",
    "tus": "Tus",
    "wed": "Wen",
    "wen": "Wen",
    "thu": "Thu",
    "fri": "Fri",
    "sat": "Sat",
}


class SajWorkingModePayload(BaseModel):
    mode_code: int = Field(..., ge=SAJ_MODE_MIN, le=SAJ_MODE_MAX)


class SajProfilePayload(BaseModel):
    profile_id: str = Field(...)


class SajSlotPayload(BaseModel):
    start_time: str | None = Field(default=None)
    end_time: str | None = Field(default=None)
    power_percent: int | None = Field(default=None, ge=0, le=100)
    day_mask: int | None = Field(default=None, ge=SAJ_DAY_MASK_MIN, le=SAJ_DAY_MASK_MAX)


class SajTogglePayload(BaseModel):
    charging_control: bool | None = Field(default=None)
    discharging_control: bool | None = Field(default=None)
    charge_time_enable_mask: int | None = Field(default=None, ge=SAJ_DAY_MASK_MIN, le=SAJ_DAY_MASK_MAX)
    discharge_time_enable_mask: int | None = Field(default=None, ge=SAJ_DAY_MASK_MIN, le=SAJ_DAY_MASK_MAX)


class SajLimitsPayload(BaseModel):
    battery_charge_power_limit: int | None = Field(default=None, ge=0, le=1100)
    battery_discharge_power_limit: int | None = Field(default=None, ge=0, le=1100)
    grid_max_charge_power: int | None = Field(default=None, ge=0, le=1100)
    grid_max_discharge_power: int | None = Field(default=None, ge=0, le=1100)


class SolplanetLimitsPayload(BaseModel):
    pin: int | None = Field(default=None, ge=SOLPLANET_LIMIT_MIN, le=SOLPLANET_LIMIT_MAX)
    pout: int | None = Field(default=None, ge=SOLPLANET_LIMIT_MIN, le=SOLPLANET_LIMIT_MAX)


class SolplanetSlotPayload(BaseModel):
    enabled: bool | None = Field(default=None)
    hour: int | None = Field(default=None, ge=0, le=23)
    minute: int | None = Field(default=None, ge=0, le=59)
    power: int | None = Field(default=None, ge=0, le=255)
    mode: int | None = Field(default=None, ge=0, le=255)


class SolplanetDaySchedulePayload(BaseModel):
    slots: list[int] = Field(default_factory=list)


class SolplanetRawSettingPayload(BaseModel):
    payload: dict[str, object] = Field(default_factory=dict)


class TeslaChargingTogglePayload(BaseModel):
    enabled: bool = Field(...)


class TeslaChargeCurrentPayload(BaseModel):
    amps: int = Field(..., ge=0, le=80)


class NotificationDismissRequest(BaseModel):
    notification_key: str = Field(..., min_length=1, max_length=200)


class TimeWindowRuleStatePayload(BaseModel):
    enabled: bool = Field(...)


def _split_entity_id(entity_id: str) -> tuple[str, str]:
    if "." not in entity_id:
        return "", entity_id
    domain, object_id = entity_id.split(".", 1)
    return domain, object_id


def _guess_brand_from_entity_id(entity_id: str) -> str:
    _, object_id = _split_entity_id(entity_id)
    if "_" not in object_id:
        return "unknown"
    return object_id.split("_", 1)[0].lower()


def _get_system_entity_ids(system: str) -> tuple[str, ...]:
    if system == "saj":
        return settings.saj_core_entity_ids
    if system == "solplanet":
        return settings.solplanet_core_entity_ids
    raise ValueError(f"Unsupported system: {system}")


def _compact_entity_item(state: dict[str, object]) -> dict[str, object]:
    entity_id = str(state.get("entity_id", ""))
    domain, _ = _split_entity_id(entity_id)
    return {
        "entity_id": entity_id,
        "domain": domain,
        "brand_guess": _guess_brand_from_entity_id(entity_id),
        "state": state.get("state"),
        "unit": state.get("attributes", {}).get("unit_of_measurement"),  # type: ignore[union-attr]
        "friendly_name": state.get("attributes", {}).get("friendly_name"),  # type: ignore[union-attr]
        "last_updated": state.get("last_updated"),
    }


def _normalize_system_name(system: str) -> str:
    normalized = system.lower().strip()
    if normalized not in SUPPORTED_SYSTEMS:
        raise HTTPException(
            status_code=404,
            detail={"message": "Unsupported system", "supported": list(SUPPORTED_SYSTEMS)},
        )
    return normalized


def _sample_interval_for_system(system: str) -> float:
    if system == "combined":
        return max(sample_interval_seconds, solplanet_sample_interval_seconds)
    return solplanet_sample_interval_seconds if system == "solplanet" else sample_interval_seconds


def _validate_interval_choice(field_name: str, value: int) -> None:
    if value not in ALLOWED_SAMPLE_INTERVAL_SECONDS:
        allowed_text = ", ".join(str(item) for item in ALLOWED_SAMPLE_INTERVAL_SECONDS)
        raise HTTPException(status_code=400, detail=f"{field_name} must be one of: {allowed_text}")


def _matches_prefix(entity_id: str, prefixes: tuple[str, ...]) -> bool:
    _, object_id = _split_entity_id(entity_id)
    return any(object_id.startswith(f"{prefix}_") for prefix in prefixes)


def _to_number(value: object) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number


def _to_bool(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in ("true", "1", "yes", "on"):
        return True
    if text in ("false", "0", "no", "off"):
        return False
    return None


def _data_kind_from_source(source_value: object, fallback_kind: str = "real") -> str:
    source_text = str(source_value or "").strip()
    if not source_text or source_text == "unavailable":
        return fallback_kind
    if "estimate" in source_text.lower():
        return "estimate"
    if source_text.startswith("calc:"):
        return "calculated"
    return "real"


def _display_item(
    *,
    label: str,
    value: object,
    unit: str | None = None,
    kind: str = "real",
    source: str | None = None,
) -> dict[str, object]:
    item: dict[str, object] = {"label": label, "value": value, "kind": kind}
    if unit is not None:
        item["unit"] = unit
    if source is not None:
        item["source"] = source
    return item


def _combined_balance_from_metrics(metrics: dict[str, object]) -> tuple[float | None, bool | None]:
    pv_w = _to_number(metrics.get("pv_w"))
    grid_w = _to_number(metrics.get("grid_w"))
    battery_w = _to_number(metrics.get("battery_w"))
    load_w = _to_number(metrics.get("load_w"))
    if pv_w is None or grid_w is None or battery_w is None or load_w is None:
        return None, None

    battery_discharge_w = max(battery_w, 0.0)
    battery_charge_w = max(-battery_w, 0.0)
    grid_import_w = max(grid_w, 0.0)
    grid_export_w = max(-grid_w, 0.0)
    balance_w = pv_w + battery_discharge_w + grid_import_w - load_w - battery_charge_w - grid_export_w
    return balance_w, abs(balance_w) <= BALANCE_TOLERANCE_W


def _build_combined_display(metrics: dict[str, object]) -> dict[str, object]:
    tesla_charge_power_w = _to_number(metrics.get("tesla_charge_power_w"))
    total_load_w = _to_number(metrics.get("load_w"))
    home_load_w = total_load_w
    if home_load_w is not None:
        home_load_w = max(home_load_w - (tesla_charge_power_w or 0.0), 0.0)
        if abs(home_load_w) <= BALANCE_TOLERANCE_W:
            home_load_w = 0.0

    balance_w, balanced = _combined_balance_from_metrics(metrics)
    solar_source = str(metrics.get("pv_source") or "unavailable")
    grid_source = str(metrics.get("grid_source") or "unavailable")
    total_load_source = str(metrics.get("load_source") or "unavailable")
    battery_total_source = str(metrics.get("battery_source") or "unavailable")
    battery1_source = str(metrics.get("battery1_source") or "unavailable")
    battery2_source = str(metrics.get("battery2_source") or "unavailable")
    inverter1_source = str(metrics.get("inverter1_power_source") or "unavailable")
    inverter2_source = str(metrics.get("inverter2_power_source") or "unavailable")

    items = {
        "solar": _display_item(
            label="Solar",
            value=_to_number(metrics.get("pv_w")),
            unit="W",
            kind=_data_kind_from_source(solar_source, "real"),
            source=solar_source,
        ),
        "solar_primary": _display_item(
            label="SAJ Solar",
            value=_to_number(metrics.get("solar_primary_w")),
            unit="W",
            kind=_data_kind_from_source(solar_source, "real"),
            source=solar_source,
        ),
        "solar_secondary": _display_item(
            label="Solplanet Solar",
            value=_to_number(metrics.get("solar_secondary_w")),
            unit="W",
            kind=_data_kind_from_source(solar_source, "real"),
            source=solar_source,
        ),
        "grid": _display_item(
            label="Grid",
            value=_to_number(metrics.get("grid_w")),
            unit="W",
            kind=_data_kind_from_source(grid_source, "real"),
            source=grid_source,
        ),
        "total_load": _display_item(
            label="Total Load",
            value=total_load_w,
            unit="W",
            kind=_data_kind_from_source(total_load_source, "calculated"),
            source=total_load_source,
        ),
        "home_load": _display_item(
            label="Home Load",
            value=home_load_w,
            unit="W",
            kind="calculated",
            source="calc:combined.total_load - tesla_charge_power",
        ),
        "battery_total": _display_item(
            label="Battery Total",
            value=_to_number(metrics.get("battery_w")),
            unit="W",
            kind=_data_kind_from_source(battery_total_source, "calculated"),
            source=battery_total_source,
        ),
        "battery1_power": _display_item(
            label="SAJ Battery Power",
            value=_to_number(metrics.get("battery1_w")),
            unit="W",
            kind=_data_kind_from_source(battery1_source, "real"),
            source=battery1_source,
        ),
        "battery2_power": _display_item(
            label="Solplanet Battery Power",
            value=_to_number(metrics.get("battery2_w")),
            unit="W",
            kind=_data_kind_from_source(battery2_source, "real"),
            source=battery2_source,
        ),
        "battery1_soc": _display_item(
            label="SAJ Battery SOC",
            value=_to_number(metrics.get("battery1_soc_percent")),
            unit="%",
            kind="real",
            source="saj.battery_soc_percent",
        ),
        "battery2_soc": _display_item(
            label="Solplanet Battery SOC",
            value=_to_number(metrics.get("battery2_soc_percent")),
            unit="%",
            kind="real",
            source="solplanet.battery_soc_percent",
        ),
        "battery2_energy": _display_item(
            label="Solplanet Battery Available Energy",
            value=_to_number(metrics.get("battery2_energy_kwh")),
            unit="kWh",
            kind="real",
            source="solplanet.battery_energy_kwh",
        ),
        "inverter1_power": _display_item(
            label="SAJ Inverter Power",
            value=_to_number(metrics.get("inverter1_w")),
            unit="W",
            kind=_data_kind_from_source(inverter1_source, "real"),
            source=inverter1_source,
        ),
        "inverter2_power": _display_item(
            label="Solplanet Inverter Power",
            value=_to_number(metrics.get("inverter2_w")),
            unit="W",
            kind=_data_kind_from_source(inverter2_source, "real"),
            source=inverter2_source,
        ),
        "inverter1_status": _display_item(
            label="SAJ Inverter Status",
            value=metrics.get("inverter1_status"),
            kind="real",
            source="saj.inverter_status",
        ),
        "inverter2_status": _display_item(
            label="Solplanet Inverter Status",
            value=metrics.get("inverter2_status"),
            kind="real",
            source="solplanet.inverter_status",
        ),
        "tesla_charge_power": _display_item(
            label="Tesla Charging Power",
            value=tesla_charge_power_w,
            unit="W",
            kind="real",
            source="tesla.charging.power_w",
        ),
        "tesla_charge_current": _display_item(
            label="Tesla Charging Current",
            value=_to_number(metrics.get("tesla_charge_current_amps")),
            unit="A",
            kind="real",
            source="tesla.charging.current_amps",
        ),
        "tesla_configured_current": _display_item(
            label="Tesla Configured Current",
            value=_to_number(metrics.get("tesla_configured_current_amps")),
            unit="A",
            kind="real",
            source="tesla.charging.configured_current_amps",
        ),
        "tesla_soc": _display_item(
            label="Tesla Battery SOC",
            value=_to_number(metrics.get("tesla_battery_soc_percent")),
            unit="%",
            kind="real",
            source="tesla.battery.level_percent",
        ),
        "tesla_connection_state": _display_item(
            label="Tesla Connection State",
            value=metrics.get("tesla_connection_state"),
            kind="real",
            source="tesla.charging.connection_state",
        ),
        "balance": _display_item(
            label="Balance",
            value=balance_w,
            unit="W",
            kind="calculated",
            source="calc:pv + battery_discharge + grid_import - load - battery_charge - grid_export",
        ),
        "balance_status": _display_item(
            label="Balance Status",
            value="cleared" if balanced else ("not_cleared" if balanced is False else None),
            kind="calculated",
            source=f"calc:abs(balance) <= {int(BALANCE_TOLERANCE_W)}W",
        ),
    }
    return {"version": 1, "order": list(items.keys()), "items": items}


def _build_public_combined_flow(flow: dict[str, object]) -> dict[str, object]:
    metrics_obj = flow.get("metrics")
    metrics = metrics_obj if isinstance(metrics_obj, dict) else {}
    display_obj = flow.get("display")
    display = display_obj if isinstance(display_obj, dict) else _build_combined_display(metrics)
    raw_obj = flow.get("raw")
    raw_map = raw_obj if isinstance(raw_obj, dict) else {}
    tesla_raw_obj = raw_map.get("tesla")
    tesla_raw = tesla_raw_obj if isinstance(tesla_raw_obj, dict) else {}
    tesla_observation = tesla_raw.get("observation")
    tesla_observation_map = tesla_observation if isinstance(tesla_observation, dict) else {}
    tesla_charging_obj = tesla_observation_map.get("charging")
    tesla_charging_map = tesla_charging_obj if isinstance(tesla_charging_obj, dict) else {}
    tesla_control_state = tesla_raw.get("control_state")
    tesla_control_state_map = tesla_control_state if isinstance(tesla_control_state, dict) else {}
    tesla_control_feedback = _resolve_tesla_control_feedback(tesla_control_state_map)
    tesla_charge_forecast = _build_tesla_charge_headroom_forecast(metrics)
    tesla_battery_soc_percent = _to_number(metrics.get("tesla_battery_soc_percent"))
    tesla_current_energy_kwh = (
        round((TESLA_BATTERY_CAPACITY_KWH * max(0.0, min(100.0, tesla_battery_soc_percent))) / 100.0, 4)
        if tesla_battery_soc_percent is not None
        else None
    )
    tesla_remaining_to_full_kwh = (
        round(max(TESLA_BATTERY_CAPACITY_KWH - (tesla_current_energy_kwh or 0.0), 0.0), 4)
        if tesla_current_energy_kwh is not None
        else None
    )

    return {
        "system": "combined",
        "updated_at": flow.get("updated_at"),
        "display": display,
        "tesla": {
            "charging": {
                "power_w": _to_number(metrics.get("tesla_charge_power_w")),
                "current_amps": _to_number(metrics.get("tesla_charge_current_amps")),
                "configured_current_amps": _to_number(metrics.get("tesla_configured_current_amps")),
                "min_current_amps": _to_number((tesla_charging_map.get("min_current_amps") if isinstance(tesla_charging_map, dict) else None)),
                "max_current_amps": _to_number((tesla_charging_map.get("max_current_amps") if isinstance(tesla_charging_map, dict) else None)),
                "current_step_amps": _to_number((tesla_charging_map.get("current_step_amps") if isinstance(tesla_charging_map, dict) else None)),
                "voltage_v": _to_number((tesla_charging_map.get("voltage_v") if isinstance(tesla_charging_map, dict) else None)),
                "minutes_to_full": _to_number((tesla_charging_map.get("minutes_to_full") if isinstance(tesla_charging_map, dict) else None)),
                "completion_at": tesla_charging_map.get("completion_at") if isinstance(tesla_charging_map, dict) else None,
                "connection_state": metrics.get("tesla_connection_state"),
                "cable_connected": metrics.get("tesla_cable_connected"),
                "requested_enabled": metrics.get("tesla_charge_requested_enabled"),
                "enabled": (
                    tesla_control_state_map.get("charging_enabled")
                    if tesla_control_state_map
                    else tesla_charging_map.get("enabled")
                ),
            },
            "battery": {
                "level_percent": tesla_battery_soc_percent,
                "total_capacity_kwh": TESLA_BATTERY_CAPACITY_KWH,
                "current_energy_kwh": tesla_current_energy_kwh,
                "remaining_to_full_kwh": tesla_remaining_to_full_kwh,
            },
            "control": {
                "available": tesla_control_state_map.get("available"),
                "control_mode": tesla_control_state_map.get("control_mode"),
                "charging_enabled": tesla_control_state_map.get("charging_enabled"),
                "charge_requested_enabled": tesla_control_state_map.get("charge_requested_enabled"),
                "can_start": tesla_control_state_map.get("can_start"),
                "can_stop": tesla_control_state_map.get("can_stop"),
                "feedback": tesla_control_feedback,
            },
            "charge_forecast": tesla_charge_forecast,
        },
        "notification_metrics": {
            "grid_w": _to_number(metrics.get("grid_w")),
            "saj_battery_soc_percent": _to_number(metrics.get("battery1_soc_percent")),
            "solplanet_battery_soc_percent": _to_number(metrics.get("battery2_soc_percent")),
            "solplanet_battery_energy_kwh": _to_number(metrics.get("battery2_energy_kwh")),
        },
        "meta": {
            "source_type": ((flow.get("source") if isinstance(flow.get("source"), dict) else {}).get("type")),
            "storage_backed": bool(flow.get("storage_backed")),
            "stale": bool(flow.get("stale")),
            "stale_reason": flow.get("stale_reason"),
            "sample_age_seconds": _to_number(flow.get("sample_age_seconds")),
            "kv_item_count": _to_number(flow.get("kv_item_count")),
            "item_count": len(display.get("order") or []),
        },
    }


def _parse_iso_utc_datetime(text: str, *, field_name: str) -> datetime:
    raw = text.strip()
    if not raw:
        raise HTTPException(status_code=400, detail=f"{field_name} is required")
    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}, expected ISO-8601 datetime") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


async def _start_operation_history_run(
    operation_code: str,
    *,
    request_payload: object | None,
    previous_state: object | None,
    trigger_source: str = "manual_ui",
) -> int | None:
    try:
        return await asyncio.to_thread(
            insert_operation_run,
            storage_db_path,
            operation_code=operation_code,
            trigger_source=trigger_source,
            started_at_utc=datetime.now(UTC).isoformat(),
            status=OPERATION_RUN_STATUS_PENDING,
            request_payload=request_payload,
            previous_state=previous_state,
        )
    except Exception:  # noqa: BLE001
        logger.exception("Failed to insert operation history run", extra={"operation_code": operation_code})
        return None


async def _update_operation_history_run(
    run_id: int | None,
    *,
    status: str,
    latest_state: object | None = None,
    response_payload: object | None = None,
    error_text: str | None = None,
    completed: bool = False,
) -> None:
    if int(run_id or 0) <= 0:
        return
    try:
        await asyncio.to_thread(
            update_operation_run,
            storage_db_path,
            run_id=int(run_id),
            status=status,
            updated_at_utc=datetime.now(UTC).isoformat(),
            latest_state=latest_state,
            response_payload=response_payload,
            error_text=error_text,
            completed=completed,
        )
    except Exception:  # noqa: BLE001
        logger.exception("Failed to update operation history run", extra={"run_id": run_id, "status": status})


def _tesla_charging_run_status(result: dict[str, object], *, target_enabled: bool) -> str:
    control_state = result.get("control_state")
    control_state_map = control_state if isinstance(control_state, dict) else {}
    charging_enabled = _to_bool(control_state_map.get("charging_enabled"))
    requested_enabled = _to_bool(control_state_map.get("charge_requested_enabled"))
    if charging_enabled is target_enabled or requested_enabled is target_enabled:
        return OPERATION_RUN_STATUS_SUCCEEDED
    return OPERATION_RUN_STATUS_PENDING


def _tesla_current_run_status(result: dict[str, object], *, target_amps: int) -> str:
    observation = result.get("observation")
    observation_map = observation if isinstance(observation, dict) else {}
    charging = observation_map.get("charging")
    charging_map = charging if isinstance(charging, dict) else {}
    configured_current = _to_number(charging_map.get("configured_current_amps"))
    if configured_current is not None and abs(configured_current - float(target_amps)) < 0.05:
        return OPERATION_RUN_STATUS_SUCCEEDED
    return OPERATION_RUN_STATUS_PENDING


async def _build_tesla_pending_operation_state(
    public_flow: dict[str, object],
) -> dict[str, dict[str, object]]:
    rows = await asyncio.to_thread(
        get_latest_operation_runs,
        storage_db_path,
        operation_codes=[
            "tesla.manual.set_charge_current",
            "tesla.manual.toggle_charging",
        ],
        statuses=[OPERATION_RUN_STATUS_PENDING],
    )
    if not rows:
        return {}

    tesla_map = public_flow.get("tesla")
    tesla = tesla_map if isinstance(tesla_map, dict) else {}
    charging_map = tesla.get("charging")
    charging = charging_map if isinstance(charging_map, dict) else {}
    current_configured_amps = _to_number(charging.get("configured_current_amps"))
    current_requested_enabled = _to_bool(charging.get("requested_enabled"))
    current_enabled = _to_bool(charging.get("enabled"))
    now = datetime.now(UTC)
    payload: dict[str, dict[str, object]] = {}

    current_row = rows.get("tesla.manual.set_charge_current")
    if isinstance(current_row, dict):
        request_payload = current_row.get("request_payload")
        request_map = request_payload if isinstance(request_payload, dict) else {}
        previous_state = current_row.get("previous_state")
        previous_map = previous_state if isinstance(previous_state, dict) else {}
        previous_observation = previous_map.get("observation")
        previous_observation_map = previous_observation if isinstance(previous_observation, dict) else {}
        previous_charging = previous_observation_map.get("charging")
        previous_charging_map = previous_charging if isinstance(previous_charging, dict) else {}
        target_amps = _to_number(request_map.get("amps"))
        source_amps = _to_number(previous_charging_map.get("configured_current_amps"))
        started_at_text = str(current_row.get("started_at_utc") or "").strip()
        try:
            started_at = _parse_iso_utc_datetime(started_at_text, field_name="operation.started_at") if started_at_text else None
        except HTTPException:
            started_at = None
        expired = bool(
            started_at
            and (now - started_at).total_seconds() >= TESLA_OPERATION_PENDING_TIMEOUT_SECONDS
        )
        matched = bool(
            target_amps is not None
            and current_configured_amps is not None
            and abs(current_configured_amps - target_amps) <= 0.05
        )
        if matched:
            await _update_operation_history_run(
                int(current_row.get("id") or 0),
                status=OPERATION_RUN_STATUS_SUCCEEDED,
                latest_state={"configured_current_amps": current_configured_amps},
                completed=True,
            )
        elif expired:
            await _update_operation_history_run(
                int(current_row.get("id") or 0),
                status=OPERATION_RUN_STATUS_TIMEOUT,
                latest_state={"configured_current_amps": current_configured_amps},
                error_text="Tesla current change did not confirm before timeout",
                completed=True,
            )
        elif target_amps is not None:
            payload["set_charge_current"] = {
                "run_id": int(current_row.get("id") or 0),
                "status": OPERATION_RUN_STATUS_PENDING,
                "started_at": started_at_text or None,
                "source_amps": source_amps,
                "target_amps": target_amps,
            }

    toggle_row = rows.get("tesla.manual.toggle_charging")
    if isinstance(toggle_row, dict):
        request_payload = toggle_row.get("request_payload")
        request_map = request_payload if isinstance(request_payload, dict) else {}
        previous_state = toggle_row.get("previous_state")
        previous_map = previous_state if isinstance(previous_state, dict) else {}
        previous_control = previous_map.get("control_state")
        previous_control_map = previous_control if isinstance(previous_control, dict) else {}
        target_enabled = _to_bool(request_map.get("enabled"))
        source_enabled = _to_bool(previous_control_map.get("charging_enabled"))
        started_at_text = str(toggle_row.get("started_at_utc") or "").strip()
        try:
            started_at = _parse_iso_utc_datetime(started_at_text, field_name="operation.started_at") if started_at_text else None
        except HTTPException:
            started_at = None
        expired = bool(
            started_at
            and (now - started_at).total_seconds() >= TESLA_OPERATION_PENDING_TIMEOUT_SECONDS
        )
        matched = (
            target_enabled is not None
            and (current_requested_enabled is target_enabled or current_enabled is target_enabled)
        )
        if matched:
            await _update_operation_history_run(
                int(toggle_row.get("id") or 0),
                status=OPERATION_RUN_STATUS_SUCCEEDED,
                latest_state={
                    "requested_enabled": current_requested_enabled,
                    "charging_enabled": current_enabled,
                },
                completed=True,
            )
        elif expired:
            await _update_operation_history_run(
                int(toggle_row.get("id") or 0),
                status=OPERATION_RUN_STATUS_TIMEOUT,
                latest_state={
                    "requested_enabled": current_requested_enabled,
                    "charging_enabled": current_enabled,
                },
                error_text="Tesla charging toggle did not confirm before timeout",
                completed=True,
            )
        elif target_enabled is not None:
            payload["toggle_charging"] = {
                "run_id": int(toggle_row.get("id") or 0),
                "status": OPERATION_RUN_STATUS_PENDING,
                "started_at": started_at_text or None,
                "source_enabled": source_enabled,
                "target_enabled": target_enabled,
            }

    return payload


def _power_to_watts(value: float | None, unit: str | None) -> float | None:
    if value is None:
        return None
    normalized = (unit or "W").strip().lower()
    if normalized == "kw":
        return value * 1000
    if normalized == "mw":
        return value * 1000000
    return value


def _energy_to_kwh(value: float | None, unit: str | None) -> float | None:
    if value is None:
        return None
    normalized = (unit or "kWh").strip().lower()
    if normalized in ("kwh", "kw·h", "kw h"):
        return value
    if normalized in ("wh", "w·h", "w h"):
        return value / 1000.0
    if normalized in ("mwh", "mw·h", "mw h"):
        return value * 1000.0
    return None


def _battery_usable_energy_kwh(system: str, soc_percent: object) -> float | None:
    capacity_kwh = BATTERY_CAPACITY_KWH.get(system)
    min_discharge_soc = BATTERY_MIN_DISCHARGE_SOC_PERCENT.get(system, 0.0)
    soc = _to_number(soc_percent)
    if capacity_kwh is None or soc is None:
        return None
    clamped_soc = max(0.0, min(100.0, soc))
    usable_soc = max(0.0, clamped_soc - min_discharge_soc)
    return round((capacity_kwh * usable_soc) / 100.0, 4)


def _build_tesla_charge_headroom_forecast(metrics: dict[str, object]) -> dict[str, object] | None:
    now_local, timezone_name = _tesla_grid_support_now_local()
    local_tz = now_local.tzinfo
    if local_tz is None:
        return None

    usable_battery_values = [
        _battery_usable_energy_kwh("saj", metrics.get("battery1_soc_percent")),
        _battery_usable_energy_kwh("solplanet", metrics.get("battery2_soc_percent")),
    ]
    usable_battery_total_kwh = round(sum(value for value in usable_battery_values if value is not None), 4)
    has_current_battery_state = any(value is not None for value in usable_battery_values)
    history_windows: list[dict[str, object]] = []
    anchor_date = now_local.date()
    for _ in range(TESLA_POST_EXPORT_FORECAST_HISTORY_DAYS):
        window_end_local = datetime.combine(
            anchor_date,
            time(hour=TESLA_POST_EXPORT_FORECAST_WINDOW_END_HOUR),
            tzinfo=local_tz,
        )
        if window_end_local >= now_local:
            anchor_date -= timedelta(days=1)
            window_end_local = datetime.combine(
                anchor_date,
                time(hour=TESLA_POST_EXPORT_FORECAST_WINDOW_END_HOUR),
                tzinfo=local_tz,
            )
        window_start_local = datetime.combine(
            anchor_date - timedelta(days=1),
            time(hour=TESLA_POST_EXPORT_FORECAST_WINDOW_START_HOUR),
            tzinfo=local_tz,
        )
        usage = compute_usage_between(
            storage_db_path,
            system="combined",
            start_at_utc=window_start_local.astimezone(UTC),
            end_at_utc=window_end_local.astimezone(UTC),
            sample_interval_seconds=_sample_interval_for_system("combined"),
            local_timezone_name=timezone_name,
            grid_import_window_start_hour=FREE_ENERGY_WINDOW_START_HOUR,
            grid_import_window_end_hour=FREE_ENERGY_WINDOW_END_HOUR,
            grid_export_window_start_hour=EXPORT_WINDOW_START_HOUR,
            grid_export_window_end_hour=EXPORT_WINDOW_END_HOUR,
        )
        energy_map = usage.get("energy_kwh")
        energy_usage = energy_map if isinstance(energy_map, dict) else {}
        quality_map = usage.get("quality")
        quality = quality_map if isinstance(quality_map, dict) else {}
        configured_interval_seconds = _to_number(quality.get("configured_interval_seconds"))
        observed_interval_seconds = _to_number(quality.get("observed_interval_seconds"))
        effective_coverage_ratio = _to_number(usage.get("coverage_ratio"))
        if (
            effective_coverage_ratio is not None
            and configured_interval_seconds is not None
            and configured_interval_seconds > 0
            and observed_interval_seconds is not None
            and observed_interval_seconds > configured_interval_seconds
        ):
            effective_coverage_ratio = round(
                effective_coverage_ratio * (observed_interval_seconds / configured_interval_seconds),
                4,
            )
        history_windows.append(
            {
                "start_at_local": window_start_local.isoformat(),
                "end_at_local": window_end_local.isoformat(),
                "home_load_kwh": _to_number(energy_usage.get("home_load")),
                "coverage_ratio": _to_number(usage.get("coverage_ratio")),
                "effective_coverage_ratio": effective_coverage_ratio,
                "samples": int(_to_number(usage.get("samples")) or 0),
            }
        )
        anchor_date -= timedelta(days=1)

    valid_home_loads = [
        float(item["home_load_kwh"])
        for item in history_windows
        if item.get("home_load_kwh") is not None
        and int(item.get("samples") or 0) >= 2
        and (
            item.get("effective_coverage_ratio") is None
            or float(item.get("effective_coverage_ratio") or 0.0) >= TESLA_POST_EXPORT_FORECAST_MIN_COVERAGE_RATIO
        )
    ]
    average_home_load_kwh = round(sum(valid_home_loads) / len(valid_home_loads), 4) if valid_home_loads else None
    estimated_tesla_charge_kwh = (
        round(max(usable_battery_total_kwh - average_home_load_kwh, 0.0), 4)
        if average_home_load_kwh is not None and has_current_battery_state
        else None
    )
    return {
        "window": {
            "start_hour": TESLA_POST_EXPORT_FORECAST_WINDOW_START_HOUR,
            "end_hour": TESLA_POST_EXPORT_FORECAST_WINDOW_END_HOUR,
        },
        "history_days": TESLA_POST_EXPORT_FORECAST_HISTORY_DAYS,
        "days_used": len(valid_home_loads),
        "usable_battery_total_kwh": usable_battery_total_kwh,
        "average_home_load_kwh": average_home_load_kwh,
        "estimated_tesla_charge_kwh": estimated_tesla_charge_kwh,
        "timezone": timezone_name,
        "insufficient_history": len(valid_home_loads) < TESLA_POST_EXPORT_FORECAST_HISTORY_DAYS,
        "history_windows": history_windows,
    }


def _find_state_by_entity_ids(
    states_by_entity_id: dict[str, dict[str, object]],
    entity_ids: tuple[str, ...],
) -> dict[str, object] | None:
    for entity_id in entity_ids:
        state = states_by_entity_id.get(entity_id)
        if state:
            return state
    return None


def _find_state_by_keywords(
    states: list[dict[str, object]],
    prefixes: tuple[str, ...],
    keyword_groups: tuple[tuple[str, ...], ...],
) -> dict[str, object] | None:
    for state in states:
        entity_id = str(state.get("entity_id", ""))
        if prefixes and not _matches_prefix(entity_id, prefixes):
            continue
        friendly_name = str(state.get("attributes", {}).get("friendly_name") or "")  # type: ignore[union-attr]
        haystack = f"{entity_id} {friendly_name}".lower()
        for keyword_group in keyword_groups:
            if all(kw in haystack for kw in keyword_group):
                return state
    return None


def _tesla_haystack(state: dict[str, object]) -> str:
    entity_id = str(state.get("entity_id", ""))
    friendly_name = str(state.get("attributes", {}).get("friendly_name") or "")  # type: ignore[union-attr]
    return f"{entity_id} {friendly_name}".lower()


def _pick_best_state(states: list[dict[str, object]], scorer: Callable[[dict[str, object]], int]) -> dict[str, object] | None:
    best_state: dict[str, object] | None = None
    best_score = 0
    for state in states:
        score = scorer(state)
        if score > best_score:
            best_score = score
            best_state = state
    return best_state


def _pick_tesla_charge_switch(states: list[dict[str, object]]) -> dict[str, object] | None:
    def _score(state: dict[str, object]) -> int:
        entity_id = str(state.get("entity_id", ""))
        domain, _ = _split_entity_id(entity_id)
        if domain != "switch":
            return 0
        haystack = _tesla_haystack(state)
        if "tesla" not in haystack:
            return 0
        score = 0
        if "charger" in haystack:
            score += 140
        if "charging" in haystack:
            score += 110
        if "charge" in haystack:
            score += 70
        if "vehicle" in haystack:
            score += 10
        if any(term in haystack for term in ("port", "door", "cable", "lock", "unlock")):
            score -= 220
        if str(state.get("state", "")).lower() in ("on", "off"):
            score += 25
        return max(score, 0)

    return _pick_best_state(states, _score)


def _pick_tesla_charge_button(states: list[dict[str, object]], action: Literal["start", "stop"]) -> dict[str, object] | None:
    def _score(state: dict[str, object]) -> int:
        entity_id = str(state.get("entity_id", ""))
        domain, _ = _split_entity_id(entity_id)
        if domain != "button":
            return 0
        haystack = _tesla_haystack(state)
        if "tesla" not in haystack:
            return 0
        score = 0
        if action in haystack:
            score += 120
        if "charger" in haystack:
            score += 100
        if "charging" in haystack:
            score += 90
        if "charge" in haystack:
            score += 60
        if any(term in haystack for term in ("port", "door", "cable", "lock", "unlock")):
            score -= 220
        return max(score, 0)

    return _pick_best_state(states, _score)


def _pick_tesla_charge_cable_entity(states: list[dict[str, object]]) -> dict[str, object] | None:
    def _score(state: dict[str, object]) -> int:
        entity_id = str(state.get("entity_id", ""))
        domain, _ = _split_entity_id(entity_id)
        if domain not in ("binary_sensor", "sensor"):
            return 0
        haystack = _tesla_haystack(state)
        if "tesla" not in haystack:
            return 0
        score = 0
        if "charge cable" in haystack:
            score += 220
        if "charge_cable" in haystack:
            score += 210
        if "charger cable" in haystack:
            score += 180
        if "cable" in haystack:
            score += 120
        if "charge" in haystack:
            score += 60
        state_text = str(state.get("state", "")).strip().lower()
        if state_text in ("on", "off", "connected", "disconnected"):
            score += 20
        return max(score, 0)

    return _pick_best_state(states, _score)


def _pick_tesla_charge_status_entity(states: list[dict[str, object]]) -> dict[str, object] | None:
    def _score(state: dict[str, object]) -> int:
        entity_id = str(state.get("entity_id", ""))
        domain, _ = _split_entity_id(entity_id)
        if domain not in ("binary_sensor", "sensor"):
            return 0
        haystack = _tesla_haystack(state)
        if "tesla" not in haystack:
            return 0
        if any(term in haystack for term in ("power", "current", "voltage", "energy")):
            return 0
        score = 0
        if domain == "sensor":
            score += 50
        if "charging" in haystack:
            score += 150
        if "charger" in haystack:
            score += 120
        if "charge" in haystack:
            score += 60
        if any(term in haystack for term in ("scheduled", "pending", "trip")):
            score -= 260
        state_text = str(state.get("state", "")).lower()
        if state_text in ("charging", "stopped", "complete", "disconnected", "idle", "starting", "no_power"):
            score += 80
        elif state_text in ("on", "off"):
            score += 15
        return max(score, 0)

    return _pick_best_state(states, _score)


def _pick_tesla_charge_power_entity(states: list[dict[str, object]]) -> dict[str, object] | None:
    def _score(state: dict[str, object]) -> int:
        entity_id = str(state.get("entity_id", ""))
        domain, _ = _split_entity_id(entity_id)
        if domain != "sensor":
            return 0
        haystack = _tesla_haystack(state)
        if "tesla" not in haystack:
            return 0
        score = 0
        if "charger_power" in haystack:
            score += 160
        if "charge_power" in haystack:
            score += 145
        if "charger power" in haystack:
            score += 130
        if "charging power" in haystack:
            score += 120
        unit = str(state.get("attributes", {}).get("unit_of_measurement") or "").lower()  # type: ignore[union-attr]
        if unit in ("w", "kw", "mw"):
            score += 25
        return max(score, 0)

    return _pick_best_state(states, _score)


def _pick_tesla_charge_current_number_entity(states: list[dict[str, object]]) -> dict[str, object] | None:
    def _score(state: dict[str, object]) -> int:
        entity_id = str(state.get("entity_id", ""))
        domain, _ = _split_entity_id(entity_id)
        if domain != "number":
            return 0
        haystack = _tesla_haystack(state)
        if "tesla" not in haystack:
            return 0
        score = 0
        if "charge_current" in haystack:
            score += 180
        if "charge current" in haystack:
            score += 170
        if "charger_current" in haystack:
            score += 160
        if "charger current" in haystack:
            score += 150
        if "current" in haystack:
            score += 80
        unit = str(state.get("attributes", {}).get("unit_of_measurement") or "").lower()  # type: ignore[union-attr]
        if unit == "a":
            score += 40
        if "limit" in haystack:
            score -= 80
        return max(score, 0)

    return _pick_best_state(states, _score)


def _pick_tesla_charge_current_sensor_entity(states: list[dict[str, object]]) -> dict[str, object] | None:
    def _score(state: dict[str, object]) -> int:
        entity_id = str(state.get("entity_id", ""))
        domain, _ = _split_entity_id(entity_id)
        if domain != "sensor":
            return 0
        haystack = _tesla_haystack(state)
        if "tesla" not in haystack:
            return 0
        score = 0
        if "charger_current" in haystack:
            score += 180
        if "charger current" in haystack:
            score += 170
        if "charge_current" in haystack:
            score += 160
        if "charge current" in haystack:
            score += 150
        if "current" in haystack:
            score += 80
        unit = str(state.get("attributes", {}).get("unit_of_measurement") or "").lower()  # type: ignore[union-attr]
        if unit == "a":
            score += 40
        return max(score, 0)

    return _pick_best_state(states, _score)


def _pick_tesla_charge_voltage_entity(states: list[dict[str, object]]) -> dict[str, object] | None:
    def _score(state: dict[str, object]) -> int:
        entity_id = str(state.get("entity_id", ""))
        domain, _ = _split_entity_id(entity_id)
        if domain != "sensor":
            return 0
        haystack = _tesla_haystack(state)
        if "tesla" not in haystack:
            return 0
        score = 0
        if "charger_voltage" in haystack:
            score += 180
        if "charger voltage" in haystack:
            score += 170
        if "charge_voltage" in haystack:
            score += 160
        if "charge voltage" in haystack:
            score += 150
        if "voltage" in haystack:
            score += 80
        unit = str(state.get("attributes", {}).get("unit_of_measurement") or "").lower()  # type: ignore[union-attr]
        if unit == "v":
            score += 40
        return max(score, 0)

    return _pick_best_state(states, _score)


def _pick_tesla_battery_level_entity(states: list[dict[str, object]]) -> dict[str, object] | None:
    def _score(state: dict[str, object]) -> int:
        entity_id = str(state.get("entity_id", ""))
        domain, _ = _split_entity_id(entity_id)
        if domain != "sensor":
            return 0
        haystack = _tesla_haystack(state)
        if "tesla" not in haystack:
            return 0
        if any(term in haystack for term in ("charger", "charge current", "charge_current", "power", "voltage")):
            return 0
        score = 0
        if "battery_level" in haystack:
            score += 220
        if "battery level" in haystack:
            score += 210
        if "battery" in haystack and "level" in haystack:
            score += 180
        if "usable_battery_level" in haystack:
            score += 170
        if "usable battery level" in haystack:
            score += 160
        if "soc" in haystack:
            score += 90
        unit = str(state.get("attributes", {}).get("unit_of_measurement") or "").lower()  # type: ignore[union-attr]
        if unit == "%":
            score += 50
        return max(score, 0)

    return _pick_best_state(states, _score)


def _pick_tesla_charge_completion_entity(states: list[dict[str, object]]) -> dict[str, object] | None:
    def _score(state: dict[str, object]) -> int:
        entity_id = str(state.get("entity_id", ""))
        domain, _ = _split_entity_id(entity_id)
        if domain != "sensor":
            return 0
        haystack = _tesla_haystack(state)
        if "tesla" not in haystack:
            return 0
        if any(term in haystack for term in ("scheduled", "trip", "departure")):
            return 0
        score = 0
        if "time_to_full_charge" in haystack:
            score += 260
        if "time to full charge" in haystack:
            score += 250
        if "minutes_to_full_charge" in haystack:
            score += 240
        if "minutes to full charge" in haystack:
            score += 230
        if "time_to_full" in haystack:
            score += 220
        if "time to full" in haystack:
            score += 210
        if "charge_completion" in haystack:
            score += 205
        if "charge complete" in haystack:
            score += 195
        if "full charge" in haystack:
            score += 160
        if "completion" in haystack:
            score += 120
        attrs = state.get("attributes")
        attrs_map = attrs if isinstance(attrs, dict) else {}
        unit = str(attrs_map.get("unit_of_measurement") or "").lower()
        device_class = str(attrs_map.get("device_class") or "").lower()
        if unit in ("min", "mins", "minute", "minutes", "h", "hr", "hrs", "hour", "hours"):
            score += 80
        if device_class == "timestamp":
            score += 80
        return max(score, 0)

    return _pick_best_state(states, _score)


def _parse_tesla_charge_completion(state: dict[str, object] | None) -> dict[str, object]:
    if not isinstance(state, dict):
        return {"minutes_to_full": None, "completion_at": None}
    raw_state = state.get("state")
    state_text = str(raw_state or "").strip()
    if not state_text or state_text.lower() in ("unknown", "unavailable", "none"):
        return {"minutes_to_full": None, "completion_at": None}

    attrs = state.get("attributes")
    attrs_map = attrs if isinstance(attrs, dict) else {}
    unit = str(attrs_map.get("unit_of_measurement") or "").strip().lower()
    device_class = str(attrs_map.get("device_class") or "").strip().lower()

    if device_class == "timestamp":
        try:
            completion_at = _parse_iso_utc_datetime(state_text, field_name="tesla_charge_completion")
            return {"minutes_to_full": None, "completion_at": completion_at.isoformat()}
        except HTTPException:
            return {"minutes_to_full": None, "completion_at": None}

    number = _to_number(raw_state)
    if number is None:
        try:
            completion_at = _parse_iso_utc_datetime(state_text, field_name="tesla_charge_completion")
            return {"minutes_to_full": None, "completion_at": completion_at.isoformat()}
        except HTTPException:
            return {"minutes_to_full": None, "completion_at": None}

    minutes_value: float | None = None
    if unit in ("h", "hr", "hrs", "hour", "hours"):
        minutes_value = number * 60.0
    elif unit in ("min", "mins", "minute", "minutes"):
        minutes_value = number
    else:
        minutes_value = number

    return {
        "minutes_to_full": round(max(minutes_value or 0.0, 0.0), 1),
        "completion_at": None,
    }


def _tesla_charge_enabled_from_status(state: dict[str, object] | None) -> bool | None:
    if not isinstance(state, dict):
        return None
    state_text = str(state.get("state", "")).strip().lower()
    if state_text in ("on", "charging"):
        return True
    if state_text in ("off", "stopped", "complete", "disconnected", "idle"):
        return False
    return None


def _build_tesla_control_state(states: list[dict[str, object]]) -> dict[str, object]:
    switch_state = _pick_tesla_charge_switch(states)
    start_button_state = _pick_tesla_charge_button(states, "start")
    stop_button_state = _pick_tesla_charge_button(states, "stop")
    charge_cable_state = _pick_tesla_charge_cable_entity(states)
    status_state = _pick_tesla_charge_status_entity(states)
    power_state = _pick_tesla_charge_power_entity(states)
    charge_current_number_state = _pick_tesla_charge_current_number_entity(states)
    charge_current_sensor_state = _pick_tesla_charge_current_sensor_entity(states)
    charge_voltage_state = _pick_tesla_charge_voltage_entity(states)
    power_value = _power_to_watts(
        _to_number(power_state.get("state")) if isinstance(power_state, dict) else None,
        power_state.get("attributes", {}).get("unit_of_measurement") if isinstance(power_state, dict) else None,  # type: ignore[union-attr]
    )

    requested_enabled = str(switch_state.get("state", "")).lower() == "on" if isinstance(switch_state, dict) else None
    status_enabled = _tesla_charge_enabled_from_status(status_state)
    current_value = _to_number(charge_current_sensor_state.get("state")) if isinstance(charge_current_sensor_state, dict) else None
    if current_value is None:
        current_value = _to_number(charge_current_number_state.get("state")) if isinstance(charge_current_number_state, dict) else None
    actively_charging = None
    if status_enabled is not None:
        actively_charging = status_enabled
    elif power_value is not None:
        actively_charging = power_value >= POWER_FLOW_ACTIVE_THRESHOLD_W
    elif current_value is not None:
        actively_charging = current_value >= 1.0

    current_number_attrs = (
        charge_current_number_state.get("attributes")
        if isinstance(charge_current_number_state, dict)
        else None
    )
    current_number_attrs_map = current_number_attrs if isinstance(current_number_attrs, dict) else {}

    control_mode = "unavailable"
    if isinstance(switch_state, dict):
        control_mode = "switch"
    elif isinstance(start_button_state, dict) or isinstance(stop_button_state, dict):
        control_mode = "buttons"

    return {
        "available": control_mode != "unavailable",
        "control_mode": control_mode,
        "charging_enabled": actively_charging,
        "charge_requested_enabled": requested_enabled,
        "can_start": isinstance(switch_state, dict) or isinstance(start_button_state, dict),
        "can_stop": isinstance(switch_state, dict) or isinstance(stop_button_state, dict),
        "switch_entity": _compact_raw_ha_state(switch_state),
        "start_button_entity": _compact_raw_ha_state(start_button_state),
        "stop_button_entity": _compact_raw_ha_state(stop_button_state),
        "charge_cable_entity": _compact_raw_ha_state(charge_cable_state),
        "status_entity": _compact_raw_ha_state(status_state),
        "power_entity": _compact_raw_ha_state(power_state),
        "charge_current_number_entity": _compact_raw_ha_state(charge_current_number_state),
        "charge_current_number_min": _to_number(current_number_attrs_map.get("min")),
        "charge_current_number_max": _to_number(current_number_attrs_map.get("max")),
        "charge_current_number_step": _to_number(current_number_attrs_map.get("step")),
        "charge_current_sensor_entity": _compact_raw_ha_state(charge_current_sensor_state),
        "charge_voltage_entity": _compact_raw_ha_state(charge_voltage_state),
    }


def _build_tesla_observation_payload(states: list[dict[str, object]]) -> dict[str, object]:
    control_state = _build_tesla_control_state(states)
    battery_level_state = _pick_tesla_battery_level_entity(states)
    charge_completion_state = _pick_tesla_charge_completion_entity(states)
    charge_current_sensor = control_state.get("charge_current_sensor_entity")
    charge_current_number = control_state.get("charge_current_number_entity")
    charge_voltage_entity = control_state.get("charge_voltage_entity")
    current_a = _to_number((charge_current_sensor or {}).get("state")) if isinstance(charge_current_sensor, dict) else None
    if current_a is None:
        current_a = _to_number((charge_current_number or {}).get("state")) if isinstance(charge_current_number, dict) else None
    voltage_v = _to_number((charge_voltage_entity or {}).get("state")) if isinstance(charge_voltage_entity, dict) else None
    charge_power_w = _tesla_control_state_charge_power_w(control_state)
    status_entity = control_state.get("status_entity")
    status_text = str((status_entity or {}).get("state") or "").strip().lower()
    charging_enabled = control_state.get("charging_enabled")
    requested_enabled = control_state.get("charge_requested_enabled")
    charge_cable_entity = control_state.get("charge_cable_entity")
    charge_cable_state = str((charge_cable_entity or {}).get("state") or "").strip().lower()
    cable_connected = charge_cable_state in ("on", "connected", "plugged", "true")
    battery_level_percent = _to_number((battery_level_state or {}).get("state")) if isinstance(battery_level_state, dict) else None
    charge_completion = _parse_tesla_charge_completion(charge_completion_state)

    connection_state = "unplugged"
    if cable_connected and charging_enabled:
        connection_state = "charging"
    elif cable_connected:
        connection_state = "plugged_not_charging"

    observed_entities = {
        "battery_level_entity": _compact_raw_ha_state(battery_level_state),
        "charge_cable_entity": charge_cable_entity,
        "status_entity": control_state.get("status_entity"),
        "charge_current_sensor_entity": charge_current_sensor,
        "charge_current_number_entity": charge_current_number,
        "charge_voltage_entity": charge_voltage_entity,
        "power_entity": control_state.get("power_entity"),
        "charge_completion_entity": _compact_raw_ha_state(charge_completion_state),
    }
    observed_count = sum(1 for item in observed_entities.values() if isinstance(item, dict) and item.get("entity_id"))

    return {
        "battery": {
            "level_percent": battery_level_percent,
            "entity": observed_entities["battery_level_entity"],
        },
        "charging": {
            "enabled": charging_enabled,
            "requested_enabled": requested_enabled,
            "cable_connected": cable_connected,
            "connection_state": connection_state,
            "status_text": status_text or None,
            "current_amps": current_a,
            "configured_current_amps": _to_number((charge_current_number or {}).get("state")) if isinstance(charge_current_number, dict) else None,
            "min_current_amps": _to_number(control_state.get("charge_current_number_min")),
            "max_current_amps": _to_number(control_state.get("charge_current_number_max")),
            "current_step_amps": _to_number(control_state.get("charge_current_number_step")),
            "voltage_v": voltage_v,
            "power_w": round(charge_power_w, 1),
            "minutes_to_full": _to_number(charge_completion.get("minutes_to_full")),
            "completion_at": charge_completion.get("completion_at"),
        },
        "control_mode": control_state.get("control_mode"),
        "observed_entity_count": observed_count,
        "entities": observed_entities,
    }


async def _tesla_control_states() -> list[dict[str, object]]:
    return await ha_client.all_states()


async def _tesla_set_charging(enabled: bool) -> dict[str, object]:
    states = await _tesla_control_states()
    control_state = _build_tesla_control_state(states)
    switch_entity = control_state.get("switch_entity")
    start_button_entity = control_state.get("start_button_entity")
    stop_button_entity = control_state.get("stop_button_entity")
    control_mode = str(control_state.get("control_mode") or "unavailable")

    if control_mode == "switch":
        entity_id = str((switch_entity or {}).get("entity_id") or "")
        if not entity_id:
            raise HTTPException(status_code=400, detail="Tesla charging switch entity unavailable")
        service = "turn_on" if enabled else "turn_off"
        await ha_client.call_service("switch", service, {"entity_id": entity_id})
    elif enabled:
        entity_id = str((start_button_entity or {}).get("entity_id") or "")
        if not entity_id:
            raise HTTPException(status_code=400, detail="Tesla start charging button unavailable")
        await ha_client.call_service("button", "press", {"entity_id": entity_id})
    else:
        entity_id = str((stop_button_entity or {}).get("entity_id") or "")
        if not entity_id:
            raise HTTPException(status_code=400, detail="Tesla stop charging button unavailable")
        await ha_client.call_service("button", "press", {"entity_id": entity_id})

    refreshed_states = await _tesla_control_states()
    refreshed_control_state = _build_tesla_control_state(refreshed_states)
    return {
        "ok": True,
        "requested_enabled": enabled,
        "control_state": refreshed_control_state,
    }


async def _tesla_restart_charging() -> dict[str, object]:
    states = await _tesla_control_states()
    control_state = _build_tesla_control_state(states)
    switch_entity = control_state.get("switch_entity")
    entity_id = str((switch_entity or {}).get("entity_id") or "")
    control_mode = str(control_state.get("control_mode") or "unavailable")
    if control_mode != "switch" or not entity_id:
        return await _tesla_set_charging(True)
    await ha_client.call_service("switch", "turn_off", {"entity_id": entity_id})
    await asyncio.sleep(1.0)
    await ha_client.call_service("switch", "turn_on", {"entity_id": entity_id})
    refreshed_states = await _tesla_control_states()
    refreshed_control_state = _build_tesla_control_state(refreshed_states)
    return {
        "ok": True,
        "requested_enabled": True,
        "restart": True,
        "control_state": refreshed_control_state,
    }


async def _tesla_set_charge_current(amps: int) -> dict[str, object]:
    states = await _tesla_control_states()
    control_state = _build_tesla_control_state(states)
    current_entity = control_state.get("charge_current_number_entity")
    entity_id = str((current_entity or {}).get("entity_id") or "")
    if not entity_id:
        raise HTTPException(status_code=400, detail="Tesla charge current number entity unavailable")
    min_a = _to_number(control_state.get("charge_current_number_min"))
    max_a = _to_number(control_state.get("charge_current_number_max"))
    step_a = _to_number(control_state.get("charge_current_number_step")) or 1.0
    if min_a is not None and amps < int(round(min_a)):
        raise HTTPException(status_code=400, detail=f"Tesla charge current must be >= {int(round(min_a))}A")
    if max_a is not None and amps > int(round(max_a)):
        raise HTTPException(status_code=400, detail=f"Tesla charge current must be <= {int(round(max_a))}A")
    if step_a > 0 and min_a is not None:
        offset = amps - min_a
        steps = round(offset / step_a)
        if abs(offset - (steps * step_a)) > 1e-6:
            raise HTTPException(status_code=400, detail=f"Tesla charge current must follow step {step_a:g}A")
    await ha_client.call_service("number", "set_value", {"entity_id": entity_id, "value": int(amps)})
    refreshed_states = await _tesla_control_states()
    refreshed_control_state = _build_tesla_control_state(refreshed_states)
    return {
        "ok": True,
        "requested_charge_current_amps": int(amps),
        "control_state": refreshed_control_state,
        "observation": _build_tesla_observation_payload(refreshed_states),
    }


def _tesla_control_state_charge_power_w(control_state: dict[str, object]) -> float:
    current_entity = control_state.get("charge_current_sensor_entity")
    current_a = _to_number(current_entity.get("state")) if isinstance(current_entity, dict) else None
    voltage_entity = control_state.get("charge_voltage_entity")
    voltage_v = _to_number(voltage_entity.get("state")) if isinstance(voltage_entity, dict) else None
    if current_a is None:
        current_number_entity = control_state.get("charge_current_number_entity")
        current_a = _to_number(current_number_entity.get("state")) if isinstance(current_number_entity, dict) else None
    if current_a is None or current_a <= 0:
        current_a = None
    if current_a is not None:
        if voltage_v is None or voltage_v < 100:
            voltage_v = TESLA_ASSUMED_CHARGING_VOLTAGE_V
        return max(current_a * voltage_v, 0.0)

    power_entity = control_state.get("power_entity")
    power_w = _power_to_watts(
        _to_number(power_entity.get("state")) if isinstance(power_entity, dict) else None,
        (
            power_entity.get("unit")
            if isinstance(power_entity, dict)
            else None
        )
        or (
            power_entity.get("attributes", {}).get("unit_of_measurement")
            if isinstance(power_entity, dict)
            else None
        ),  # type: ignore[union-attr]
    )
    if power_w is not None and power_w >= 0:
        return power_w

    if current_a is None:
        return 0.0
    if voltage_v is None or voltage_v < 100:
        voltage_v = TESLA_ASSUMED_CHARGING_VOLTAGE_V
    return max(current_a * voltage_v, 0.0)


def _is_hour_in_window(hour: int, start_hour: int, end_hour: int) -> bool:
    if start_hour < end_hour:
        return start_hour <= hour < end_hour
    return hour >= start_hour or hour < end_hour


def _is_within_free_energy_window(now_local: datetime) -> bool:
    hour = now_local.hour
    return _is_hour_in_window(hour, FREE_ENERGY_WINDOW_START_HOUR, FREE_ENERGY_WINDOW_END_HOUR)


def _is_within_after_free_shoulder_window(now_local: datetime) -> bool:
    hour = now_local.hour
    return _is_hour_in_window(hour, AFTER_FREE_SHOULDER_WINDOW_START_HOUR, AFTER_FREE_SHOULDER_WINDOW_END_HOUR)


def _is_within_after_free_peak_window(now_local: datetime) -> bool:
    hour = now_local.hour
    return _is_hour_in_window(hour, AFTER_FREE_PEAK_WINDOW_START_HOUR, AFTER_FREE_PEAK_WINDOW_END_HOUR)


def _is_within_export_window(now_local: datetime) -> bool:
    hour = now_local.hour
    return _is_hour_in_window(hour, EXPORT_WINDOW_START_HOUR, EXPORT_WINDOW_END_HOUR)


def _is_within_post_export_peak_window(now_local: datetime) -> bool:
    hour = now_local.hour
    return _is_hour_in_window(hour, POST_EXPORT_PEAK_WINDOW_START_HOUR, POST_EXPORT_PEAK_WINDOW_END_HOUR)


def _is_within_overnight_shoulder_window(now_local: datetime) -> bool:
    hour = now_local.hour
    return _is_hour_in_window(hour, OVERNIGHT_SHOULDER_WINDOW_START_HOUR, OVERNIGHT_SHOULDER_WINDOW_END_HOUR)


def _overnight_shoulder_window_key(now_local: datetime) -> str:
    if now_local.hour >= OVERNIGHT_SHOULDER_WINDOW_START_HOUR:
        return now_local.strftime("%Y-%m-%d")
    return (now_local - timedelta(days=1)).strftime("%Y-%m-%d")


def _tesla_midday_window_mode(now_local: datetime) -> Literal["free_energy", "after_free_shoulder", "after_free_peak", "export_window", "post_export_peak", "off"]:
    if _is_within_free_energy_window(now_local):
        return "free_energy"
    if _is_within_after_free_shoulder_window(now_local):
        return "after_free_shoulder"
    if _is_within_after_free_peak_window(now_local):
        return "after_free_peak"
    if _is_within_export_window(now_local):
        return "export_window"
    if _is_within_post_export_peak_window(now_local):
        return "post_export_peak"
    return "off"


def _window_schedule_text(window_mode: str) -> str:
    if window_mode == "free_energy":
        return (
            f"{FREE_ENERGY_WINDOW_START_HOUR:02d}:00-"
            f"{FREE_ENERGY_WINDOW_END_HOUR:02d}:00"
        )
    if window_mode == "after_free_shoulder":
        return (
            f"{AFTER_FREE_SHOULDER_WINDOW_START_HOUR:02d}:00-"
            f"{AFTER_FREE_SHOULDER_WINDOW_END_HOUR:02d}:00"
        )
    if window_mode == "after_free_peak":
        return (
            f"{AFTER_FREE_PEAK_WINDOW_START_HOUR:02d}:00-"
            f"{AFTER_FREE_PEAK_WINDOW_END_HOUR:02d}:00"
        )
    if window_mode == "export_window":
        return (
            f"{EXPORT_WINDOW_START_HOUR:02d}:00-"
            f"{EXPORT_WINDOW_END_HOUR:02d}:00"
        )
    if window_mode == "post_export_peak":
        return (
            f"{POST_EXPORT_PEAK_WINDOW_START_HOUR:02d}:00-"
            f"{POST_EXPORT_PEAK_WINDOW_END_HOUR:02d}:00"
        )
    if window_mode == "overnight_shoulder":
        return (
            f"{OVERNIGHT_SHOULDER_WINDOW_START_HOUR:02d}:00-"
            f"{OVERNIGHT_SHOULDER_WINDOW_END_HOUR:02d}:00(+1d)"
        )
    return "outside_configured_windows"


def _window_tariff_p_per_kwh(window_mode: str) -> float | None:
    if window_mode == "post_export_peak":
        return POST_EXPORT_PEAK_WINDOW_TARIFF_P_PER_KWH
    return None


def _window_check_service_name(window_mode: str) -> str | None:
    return {
        "free_energy": WINDOW_CHECK_FREE_ENERGY_SERVICE,
        "after_free_shoulder": WINDOW_CHECK_AFTER_FREE_SHOULDER_SERVICE,
        "after_free_peak": WINDOW_CHECK_AFTER_FREE_PEAK_SERVICE,
        "export_window": WINDOW_CHECK_EXPORT_WINDOW_SERVICE,
        "post_export_peak": WINDOW_CHECK_POST_EXPORT_PEAK_SERVICE,
        "overnight_shoulder": OVERNIGHT_SHOULDER_SERVICE,
    }.get(window_mode)


def _window_mode_for_service(service: str) -> str | None:
    return {
        WINDOW_CHECK_FREE_ENERGY_SERVICE: "free_energy",
        WINDOW_CHECK_AFTER_FREE_SHOULDER_SERVICE: "after_free_shoulder",
        WINDOW_CHECK_AFTER_FREE_PEAK_SERVICE: "after_free_peak",
        WINDOW_CHECK_EXPORT_WINDOW_SERVICE: "export_window",
        WINDOW_CHECK_POST_EXPORT_PEAK_SERVICE: "post_export_peak",
        OVERNIGHT_SHOULDER_SERVICE: "overnight_shoulder",
    }.get(service)


def _window_schedule_text_for_service(service: str) -> str:
    mode = _window_mode_for_service(service)
    return _window_schedule_text(mode or "off")


def _tesla_rule_code_for_window(window_mode: str) -> str | None:
    return {
        "free_energy": "tesla_free_energy_control",
        "after_free_shoulder": "tesla_after_free_shoulder_control",
        "after_free_peak": "tesla_after_free_peak_control",
    }.get(str(window_mode or "").strip())


def _saj_profile_rule_code_for_time(now_local: datetime) -> str:
    return "saj_profile_free_energy" if _is_within_free_energy_window(now_local) else "saj_profile_self_consumption"


async def _load_time_window_rule_enabled_map() -> dict[str, bool]:
    return await asyncio.to_thread(
        get_time_window_rule_states,
        storage_db_path,
        rule_codes=TIME_WINDOW_RULE_CODES,
        default_enabled=True,
    )


def _is_time_window_rule_enabled(enabled_map: dict[str, bool] | None, rule_code: str) -> bool:
    normalized_code = str(rule_code or "").strip()
    if not normalized_code:
        return True
    if not isinstance(enabled_map, dict):
        return True
    return bool(enabled_map.get(normalized_code, True))


def _saj_target_profile_for_time(now_local: datetime) -> tuple[str, str]:
    if _is_within_free_energy_window(now_local):
        window_end_threshold = now_local.replace(
            hour=FREE_ENERGY_WINDOW_END_HOUR - 1,
            minute=FREE_ENERGY_WINDOW_SELF_CONSUMPTION_START_MINUTE,
            second=0,
            microsecond=0,
        )
        if now_local < window_end_threshold:
            return "time_of_use", "free_energy_window_force_time_of_use"
        return "self_consumption", "free_energy_window_last_10_minutes_force_self_consumption"
    return "self_consumption", "outside_free_energy_window_force_self_consumption"


async def _guard_saj_profile_for_time(now_local: datetime, enabled_map: dict[str, bool] | None = None) -> dict[str, object]:
    desired_profile, desired_reason = _saj_target_profile_for_time(now_local)
    rule_code = _saj_profile_rule_code_for_time(now_local)
    if not _is_time_window_rule_enabled(enabled_map, rule_code):
        return {
            "desired_profile": desired_profile,
            "desired_reason": desired_reason,
            "rule_code": rule_code,
            "rule_enabled": False,
            "action": "noop",
            "reason": "rule_disabled",
        }
    _, states_by_id = await _saj_control_states()
    control_state = _build_saj_control_state(states_by_id)
    profile_state = _build_saj_profile_state(control_state)
    selected_profile = str(profile_state.get("selected_profile") or "").strip()
    input_profile = str(profile_state.get("input_profile") or "").strip()
    actual_profile = str(profile_state.get("actual_profile") or "").strip()
    needs_apply = (
        selected_profile != desired_profile
        or input_profile != desired_profile
        or (actual_profile != "" and actual_profile != desired_profile)
    )
    result: dict[str, object] = {
        "desired_profile": desired_profile,
        "desired_reason": desired_reason,
        "rule_code": rule_code,
        "rule_enabled": True,
        "selected_profile": selected_profile or None,
        "input_profile": input_profile or None,
        "actual_profile": actual_profile or None,
        "actual_profile_source": profile_state.get("actual_profile_source"),
        "input_profile_source": profile_state.get("input_profile_source"),
        "pending_remote_sync": bool(profile_state.get("pending_remote_sync")),
        "action": "noop",
    }
    if not needs_apply:
        return result

    apply_result = await _saj_apply_profile(desired_profile)
    updated_profile_state = apply_result.get("profile_state")
    updated_profile_map = updated_profile_state if isinstance(updated_profile_state, dict) else {}
    result.update(
        {
            "action": "apply_profile",
            "applied_profile": desired_profile,
            "selected_profile": updated_profile_map.get("selected_profile"),
            "input_profile": updated_profile_map.get("input_profile"),
            "actual_profile": updated_profile_map.get("actual_profile"),
            "actual_profile_source": updated_profile_map.get("actual_profile_source"),
            "input_profile_source": updated_profile_map.get("input_profile_source"),
            "pending_remote_sync": bool(updated_profile_map.get("pending_remote_sync")),
            "changed": apply_result.get("changed"),
        }
    )
    return result


def _append_worker_notification(payload: dict[str, object], notification: dict[str, object]) -> None:
    notifications = payload.get("notifications")
    if isinstance(notifications, list):
        notifications.append(notification)
    else:
        payload["notifications"] = [notification]
    payload["notification"] = notification


def _worker_notification_list(payload: dict[str, object]) -> list[dict[str, object]]:
    notifications = payload.get("notifications")
    notification_list = [item for item in notifications if isinstance(item, dict)] if isinstance(notifications, list) else []
    if notification_list:
        return notification_list
    notification = payload.get("notification")
    return [notification] if isinstance(notification, dict) else []


def _worker_action_map(payload: dict[str, object]) -> dict[str, object]:
    action = payload.get("action")
    return action if isinstance(action, dict) else {}


def _worker_action_list(payload: dict[str, object]) -> list[dict[str, object]]:
    actions = payload.get("actions")
    action_list = [item for item in actions if isinstance(item, dict)] if isinstance(actions, list) else []
    if action_list:
        return action_list
    action = payload.get("action")
    return [action] if isinstance(action, dict) else []


def _append_worker_action(payload: dict[str, object], action: dict[str, object]) -> None:
    actions = payload.get("actions")
    if isinstance(actions, list):
        actions.append(action)
    else:
        payload["actions"] = [action]
    payload["action"] = action


def _worker_action_steps(action_map: dict[str, object]) -> list[str]:
    steps = action_map.get("steps")
    return [str(item) for item in steps if str(item).strip()] if isinstance(steps, list) else []


def _worker_has_operation(payload: dict[str, object]) -> bool:
    for action_map in _worker_action_list(payload):
        action_type = str(action_map.get("type") or "").strip().lower()
        if action_type not in ("", "noop"):
            return True
    return False


def _worker_effective_window_mode(payload: dict[str, object]) -> str:
    window_mode = str(payload.get("window_mode") or "").strip()
    return window_mode or "off"


def _worker_window_label(payload: dict[str, object]) -> str:
    window_mode = _worker_effective_window_mode(payload)
    return str(payload.get("window_schedule") or _window_schedule_text(window_mode)).strip() or _window_schedule_text(window_mode)


def _worker_window_name(payload: dict[str, object]) -> str:
    return _worker_effective_window_mode(payload)


def _worker_notification_trigger_text(notification_map: dict[str, object]) -> str:
    code = str(notification_map.get("code") or "unknown").strip()
    if code in ("saj_battery_full", "solplanet_battery_full"):
        return (
            f"soc={_fmt_result_number(notification_map.get('current_soc_percent'), 0, '%')}, "
            f"threshold={_fmt_result_number(notification_map.get('threshold_soc_percent'), 0, '%')}"
        )
    if code in ("solplanet_low_battery", "solplanet_low_battery_post_export_peak"):
        return (
            f"solplanet_soc={_fmt_result_number(notification_map.get('current_soc_percent'), 0, '%')}, "
            f"threshold={_fmt_result_number(notification_map.get('threshold_soc_percent'), 0, '%')}"
            + (
                f", tariff={_fmt_result_number(notification_map.get('tariff_p_per_kwh'), 1, 'p/kWh')}"
                if notification_map.get("tariff_p_per_kwh") is not None
                else ""
            )
        )
    if code == "solplanet_low_available_capacity":
        return (
            f"solplanet_available_capacity={_fmt_result_number(notification_map.get('current_capacity_kwh'), 1, 'kWh')}, "
            f"threshold={_fmt_result_number(notification_map.get('threshold_kwh'), 1, 'kWh')}"
        )
    if code in ("grid_import_started", "grid_import_started_post_export_peak"):
        return (
            f"grid_import={_fmt_result_number(notification_map.get('current_grid_import_w'), 0, 'W')}"
            + (
                f", tariff={_fmt_result_number(notification_map.get('tariff_p_per_kwh'), 1, 'p/kWh')}"
                if notification_map.get("tariff_p_per_kwh") is not None
                else ""
            )
        )
    if _is_solar_surplus_export_energy_notification_code(code):
        return (
            f"window_export={_fmt_result_number(notification_map.get('current_export_total_kwh'), 3, 'kWh')}, "
            f"threshold={_fmt_result_number(notification_map.get('threshold_kwh'), 3, 'kWh')}"
        )
    if code in ("saj_battery_watch_50_percent", "saj_battery_watch_20_percent"):
        return (
            f"saj_soc={_fmt_result_number(notification_map.get('current_soc_percent'), 0, '%')}, "
            f"threshold={_fmt_result_number(notification_map.get('threshold_soc_percent'), 0, '%')}"
        )
    return str(notification_map.get("message") or code)


def _worker_primary_notification(payload: dict[str, object]) -> dict[str, object] | None:
    notifications = _worker_notification_list(payload)
    if not notifications:
        return None
    level_rank = {"alarm": 2, "warning": 1}
    return max(
        notifications,
        key=lambda item: level_rank.get(str(item.get("level") or "").strip().lower(), 0),
    )


def _worker_operation_detail_text(payload: dict[str, object]) -> str:
    details: list[str] = []
    for action_map in _worker_action_list(payload):
        action_type = str(action_map.get("type") or "noop").strip().lower()
        steps = _worker_action_steps(action_map)
        if action_type == "stop_charging":
            details.append("stop_charging")
            continue
        if steps:
            details.append(", ".join(steps))
            continue
        details.append(str(action_map.get("reason") or action_type or "noop"))
    return " | ".join(item for item in details if item) or "noop"


def _worker_check_priority(payload: dict[str, object]) -> tuple[int, int]:
    window_mode = _worker_effective_window_mode(payload)
    notifications = _worker_notification_list(payload)
    notification_count = len(notifications)
    level_rank = {"alarm": 2, "warning": 1, "info": 0}
    highest_level = max(
        (level_rank.get(str(item.get("level") or "").strip().lower(), 0) for item in notifications),
        default=0,
    )
    operation_rank = 1 if _worker_has_operation(payload) else 0
    active_rank = 1 if bool(payload.get("window_active")) and window_mode != "off" else 0
    return (highest_level, active_rank, notification_count + operation_rank)


def _select_worker_summary_source(
    *candidates_input: dict[str, object] | None,
) -> dict[str, object]:
    candidates = []
    for candidate in candidates_input:
        if isinstance(candidate, dict):
            candidates.append(candidate)
    if not candidates:
        return {
            "window_mode": "off",
            "window_schedule": _window_schedule_text("off"),
            "window_active": False,
            "log_status": WORKER_LOG_STATUS_SKIPPED,
            "log_ok": False,
            "log_error_text": "worker_check_unavailable",
        }
    return max(candidates, key=_worker_check_priority)


def _collect_worker_notifications(*sources: dict[str, object] | None) -> list[dict[str, object]]:
    notifications: list[dict[str, object]] = []
    for source in sources:
        if isinstance(source, dict):
            notifications.extend(_worker_notification_list(source))
    return notifications


def _collect_worker_actions(*sources: dict[str, object] | None) -> list[dict[str, object]]:
    actions: list[dict[str, object]] = []
    for source in sources:
        if isinstance(source, dict):
            actions.extend(_worker_action_list(source))
    return actions


def _notification_entity_from_payload(notification: dict[str, object]) -> dict[str, object]:
    code = str(notification.get("code") or "unknown").strip() or "unknown"
    target = str(notification.get("target") or "").strip()
    return {
        **notification,
        "notification_key": f"{code}::{target}",
        "code": code,
        "target": target,
        "level": str(notification.get("level") or "info").strip().lower() or "info",
        "message": str(notification.get("message") or code or "Notification").strip() or code,
        "trigger_text": _worker_notification_trigger_text(notification),
        "window": str(notification.get("window") or "").strip(),
    }


def _build_notification_summary_payload(
    source: dict[str, object],
    *,
    notifications: list[dict[str, object]] | None = None,
) -> tuple[str, bool, dict[str, object], str | None]:
    notifications = list(notifications or _worker_notification_list(source))
    status = WORKER_LOG_STATUS_SEND if notifications else WORKER_LOG_STATUS_NOOP
    payload = {
        "record_kind": "notification",
        "window_mode": _worker_window_name(source),
        "window_schedule": _worker_window_label(source),
        "window_active": bool(source.get("window_active")),
        "source_status": source.get("log_status"),
        "notifications": notifications,
    }
    error_text = str(source.get("log_error_text") or "").strip() or None
    return status, bool(source.get("log_ok", True)), payload, error_text


def _operation_check_name(source: dict[str, object]) -> str:
    target = str(source.get("target") or "").strip().lower()
    if target == "battery_full_watch":
        return "battery_full_watch"
    service_window = str(source.get("service_window") or "").strip().lower()
    if service_window:
        return service_window
    window_mode = str(source.get("window_mode") or "").strip().lower()
    if window_mode in ("free_energy", "after_free_shoulder", "after_free_peak", "export_window", "post_export_peak"):
        return "midday_window_check"
    return target or window_mode or "worker_check"


def _operation_check_summary(source: dict[str, object]) -> dict[str, object]:
    action = source.get("action")
    action_map = action if isinstance(action, dict) else {}
    decision = source.get("decision")
    decision_map = decision if isinstance(decision, dict) else {}
    check: dict[str, object] = {
        "check": _operation_check_name(source),
        "target": source.get("target"),
        "service_window": source.get("service_window"),
        "service_window_schedule": source.get("service_window_schedule"),
        "window_mode": source.get("window_mode"),
        "window_schedule": source.get("window_schedule"),
        "window_active": bool(source.get("window_active")),
        "log_status": source.get("log_status"),
        "decision_reason": source.get("decision_reason"),
        "action": action_map,
        "decision": decision_map,
    }
    if source.get("skipped") is not None:
        check["skipped"] = source.get("skipped")
    if source.get("error") is not None:
        check["error"] = source.get("error")

    details: dict[str, object] = {}
    for key in (
        "current_grid_import_w",
        "current_grid_export_w",
        "base_grid_without_tesla_w",
        "available_current_options_amps",
        "candidates",
        "tesla_rule_code",
        "tesla_rule_enabled",
        "tesla_state_before",
        "saj_profile_guard",
        "after_free_shoulder",
        "after_free_peak",
        "export_window",
        "post_export_peak",
        "battery1_soc_percent",
        "battery2_soc_percent",
        "notification_state",
        "thresholds_soc_percent",
        "threshold_soc_percent",
    ):
        value = source.get(key)
        if value is not None:
            details[key] = value
    if details:
        check["details"] = details

    why_not_applied = None
    log_status = str(source.get("log_status") or "").strip().lower()
    action_type = str(action_map.get("type") or "").strip().lower()
    action_reason = str(action_map.get("reason") or "").strip()
    if log_status == "outside_window":
        why_not_applied = "outside_window"
    elif log_status == "skipped":
        why_not_applied = str(source.get("skipped") or action_reason or "skipped")
    elif log_status == "failed":
        why_not_applied = str(source.get("error") or source.get("log_error_text") or "failed")
    elif action_type in ("", "noop"):
        why_not_applied = action_reason or str(source.get("decision_reason") or "noop")
    if why_not_applied:
        check["why_not_applied"] = why_not_applied
    return check


def _build_operation_summary_payload(
    source: dict[str, object],
    *,
    actions: list[dict[str, object]] | None = None,
    check_sources: list[dict[str, object]] | None = None,
) -> tuple[str, bool, dict[str, object], str | None]:
    action_list = list(actions or _worker_action_list(source))
    action_map = action_list[-1] if action_list else {}
    checks = [
        _operation_check_summary(item)
        for item in (check_sources or [source])
        if isinstance(item, dict)
    ]
    status = WORKER_LOG_STATUS_APPLIED if any(str(item.get("type") or "").strip().lower() not in ("", "noop") for item in action_list) else WORKER_LOG_STATUS_NOOP
    payload = {
        "record_kind": "operation",
        "window_mode": _worker_window_name(source),
        "window_schedule": _worker_window_label(source),
        "window_active": bool(source.get("window_active")),
        "source_status": source.get("log_status"),
        "decision_reason": source.get("decision_reason"),
        "decision_reasons": [
            str(item.get("decision_reason") or "").strip()
            for item in [source]
            if isinstance(item, dict) and str(item.get("decision_reason") or "").strip()
        ],
        "actions": action_list,
        "action": action_map,
        "checks": checks,
    }
    error_text = str(source.get("log_error_text") or "").strip() or None
    return status, bool(source.get("log_ok", True)), payload, error_text


def _notification_level_rank(level: object) -> int:
    return {
        "alarm": 3,
        "warning": 2,
        "info": 1,
    }.get(str(level or "").strip().lower(), 0)


def _dashboard_notification_item(entry: dict[str, object]) -> dict[str, object]:
    notification = entry.get("notification")
    notification_map = notification if isinstance(notification, dict) else {}
    payload = entry.get("payload_json")
    payload_map = payload if isinstance(payload, dict) else {}
    window_schedule = str(
        notification_map.get("window")
        or payload_map.get("window_schedule")
        or "-"
    ).strip() or "-"
    level = str(notification_map.get("level") or "info").strip().lower() or "info"
    return {
        "notification_key": str(entry.get("notification_key") or ""),
        "state": str(entry.get("status") or "active"),
        "level": level,
        "code": str(notification_map.get("code") or "unknown"),
        "target": str(notification_map.get("target") or ""),
        "message": str(notification_map.get("message") or notification_map.get("code") or "Notification"),
        "trigger_text": _worker_notification_trigger_text(notification_map),
        "window": window_schedule,
        "requested_at_utc": str(entry.get("requested_at_utc") or ""),
        "requested_at_epoch": float(entry.get("requested_at_epoch") or 0.0),
        "log_id": int(entry.get("log_id") or 0),
    }


async def _load_battery_full_active_state() -> dict[str, bool]:
    kv_map = await asyncio.to_thread(
        get_realtime_kv_by_prefix,
        storage_db_path,
        prefix=BATTERY_FULL_NOTIFICATION_STATE_PREFIX,
    )
    return {
        "saj": bool((kv_map.get(SAJ_BATTERY_FULL_ACTIVE_KEY) or {}).get("value")),
        "solplanet": bool((kv_map.get(SOLPLANET_BATTERY_FULL_ACTIVE_KEY) or {}).get("value")),
    }


async def _store_battery_full_active_state(*, saj_full_active: bool, solplanet_full_active: bool) -> None:
    await asyncio.to_thread(
        upsert_realtime_kv,
        storage_db_path,
        [
            (SAJ_BATTERY_FULL_ACTIVE_KEY, json.dumps(bool(saj_full_active)), "worker"),
            (SOLPLANET_BATTERY_FULL_ACTIVE_KEY, json.dumps(bool(solplanet_full_active)), "worker"),
        ],
    )


def _update_solar_surplus_export_energy_tracking(
    *,
    combined_status: dict[str, object],
    now_local: datetime,
    current_grid_export_w: float,
) -> dict[str, object]:
    window_key = now_local.strftime("%Y-%m-%d")
    last_window_key = str(combined_status.get("solar_surplus_export_window_key") or "").strip()
    last_tracked_at_text = str(combined_status.get("solar_surplus_export_last_tracked_at") or "").strip()
    total_export_wh = float(combined_status.get("solar_surplus_export_total_wh") or 0.0)
    threshold_notified_map = combined_status.get("solar_surplus_export_threshold_notified_map")
    if isinstance(threshold_notified_map, dict):
        normalized_threshold_notified_map = {
            str(key): bool(value) for key, value in threshold_notified_map.items()
        }
    else:
        legacy_notified = bool(combined_status.get("solar_surplus_export_threshold_notified"))
        normalized_threshold_notified_map = {
            str(int(round(threshold_wh))): legacy_notified
            for threshold_wh in SOLAR_SURPLUS_EXPORT_ENERGY_NOTIFICATION_THRESHOLDS_WH
        }
    added_export_wh = 0.0

    if last_window_key != window_key:
        total_export_wh = 0.0
        normalized_threshold_notified_map = {
            str(int(round(threshold_wh))): False
            for threshold_wh in SOLAR_SURPLUS_EXPORT_ENERGY_NOTIFICATION_THRESHOLDS_WH
        }
        last_tracked_at_text = ""

    current_utc = datetime.now(UTC)
    restored_from_storage = False
    if not last_tracked_at_text:
        window_start_local = now_local.replace(
            hour=EXPORT_WINDOW_START_HOUR,
            minute=0,
            second=0,
            microsecond=0,
        )
        restored_usage = compute_usage_between(
            storage_db_path,
            system="combined",
            start_at_utc=window_start_local.astimezone(UTC),
            end_at_utc=current_utc,
            sample_interval_seconds=_sample_interval_for_system("combined"),
        )
        energy_map = restored_usage.get("energy_kwh")
        energy_usage = energy_map if isinstance(energy_map, dict) else {}
        restored_export_kwh = _to_number(energy_usage.get("grid_export")) or 0.0
        total_export_wh = max(restored_export_kwh * 1000.0, 0.0)
        normalized_threshold_notified_map = {
            str(int(round(threshold_wh))): total_export_wh >= threshold_wh
            for threshold_wh in SOLAR_SURPLUS_EXPORT_ENERGY_NOTIFICATION_THRESHOLDS_WH
        }
        restored_from_storage = True

    if last_tracked_at_text:
        try:
            last_tracked_at = datetime.fromisoformat(last_tracked_at_text.replace("Z", "+00:00"))
            if last_tracked_at.tzinfo is None:
                last_tracked_at = last_tracked_at.replace(tzinfo=UTC)
            dt_seconds = max((current_utc - last_tracked_at.astimezone(UTC)).total_seconds(), 0.0)
        except ValueError:
            dt_seconds = 0.0
    else:
        dt_seconds = 0.0

    max_dt_seconds = COLLECTOR_ROUND_SLEEP_SECONDS * 2.5
    if dt_seconds > max_dt_seconds:
        dt_seconds = max_dt_seconds
    if current_grid_export_w > 0.0 and dt_seconds > 0.0:
        added_export_wh = current_grid_export_w * dt_seconds / 3600.0
        total_export_wh += added_export_wh

    combined_status["solar_surplus_export_window_key"] = window_key
    combined_status["solar_surplus_export_last_tracked_at"] = current_utc.isoformat()
    combined_status["solar_surplus_export_total_wh"] = round(total_export_wh, 3)
    combined_status["solar_surplus_export_threshold_notified_map"] = normalized_threshold_notified_map
    combined_status["solar_surplus_export_threshold_notified"] = any(normalized_threshold_notified_map.values())
    return {
        "window_key": window_key,
        "interval_seconds": round(dt_seconds, 1),
        "added_export_wh": round(added_export_wh, 3),
        "total_export_wh": round(total_export_wh, 3),
        "total_export_kwh": round(total_export_wh / 1000.0, 4),
        "restored_from_storage": restored_from_storage,
        "thresholds_wh": list(SOLAR_SURPLUS_EXPORT_ENERGY_NOTIFICATION_THRESHOLDS_WH),
        "thresholds_kwh": [
            round(threshold_wh / 1000.0, 3)
            for threshold_wh in SOLAR_SURPLUS_EXPORT_ENERGY_NOTIFICATION_THRESHOLDS_WH
        ],
        "threshold_notified_map": normalized_threshold_notified_map,
    }


def _tesla_grid_support_now_local() -> tuple[datetime, str]:
    configured_timezone = str(getattr(settings, "local_timezone", "") or "").strip()
    if configured_timezone:
        try:
            zone = ZoneInfo(configured_timezone)
            return datetime.now(zone), configured_timezone
        except ZoneInfoNotFoundError:
            logger.warning("Invalid local timezone configured for Tesla grid support: %s", configured_timezone)
    fallback = datetime.now().astimezone()
    tz_name = getattr(fallback.tzinfo, "key", None) or fallback.tzname() or "system_local"
    return fallback, str(tz_name)


def _choose_tesla_grid_support_target(base_grid_w: float) -> dict[str, object]:
    candidates: list[dict[str, object]] = [
        {"mode": "off", "charge_current_amps": 0, "tesla_power_w": 0.0, "predicted_grid_w": base_grid_w},
        *[
            {
                "mode": "charge",
                "charge_current_amps": amps,
                "tesla_power_w": float(amps) * TESLA_ASSUMED_CHARGING_VOLTAGE_V,
                "predicted_grid_w": base_grid_w + (float(amps) * TESLA_ASSUMED_CHARGING_VOLTAGE_V),
            }
            for amps in TESLA_GRID_SUPPORT_CURRENT_OPTIONS_A
        ],
    ]
    charge_candidates = [candidate for candidate in candidates if str(candidate.get("mode")) == "charge"]
    target_candidates = [
        candidate
        for candidate in charge_candidates
        if float(candidate["predicted_grid_w"]) <= TESLA_GRID_SUPPORT_TARGET_MAX_W
    ]
    if target_candidates:
        return max(target_candidates, key=lambda candidate: float(candidate["predicted_grid_w"]))

    hard_cap_candidates = [
        candidate
        for candidate in charge_candidates
        if float(candidate["predicted_grid_w"]) <= TESLA_GRID_SUPPORT_HARD_MAX_W
    ]
    if hard_cap_candidates:
        return max(hard_cap_candidates, key=lambda candidate: float(candidate["predicted_grid_w"]))

    return candidates[0]


def _choose_tesla_grid_support_target_from_options(
    base_grid_w: float,
    charge_current_options_a: tuple[int, ...],
) -> dict[str, object]:
    candidates: list[dict[str, object]] = [
        {"mode": "off", "charge_current_amps": 0, "tesla_power_w": 0.0, "predicted_grid_w": base_grid_w},
        *[
            {
                "mode": "charge",
                "charge_current_amps": int(amps),
                "tesla_power_w": float(amps) * TESLA_ASSUMED_CHARGING_VOLTAGE_V,
                "predicted_grid_w": base_grid_w + (float(amps) * TESLA_ASSUMED_CHARGING_VOLTAGE_V),
            }
            for amps in charge_current_options_a
        ],
    ]
    charge_candidates = [candidate for candidate in candidates if str(candidate.get("mode")) == "charge"]
    target_candidates = [
        candidate
        for candidate in charge_candidates
        if float(candidate["predicted_grid_w"]) <= TESLA_GRID_SUPPORT_TARGET_MAX_W
    ]
    if target_candidates:
        return max(target_candidates, key=lambda candidate: float(candidate["predicted_grid_w"]))

    hard_cap_candidates = [
        candidate
        for candidate in charge_candidates
        if float(candidate["predicted_grid_w"]) <= TESLA_GRID_SUPPORT_HARD_MAX_W
    ]
    if hard_cap_candidates:
        return max(hard_cap_candidates, key=lambda candidate: float(candidate["predicted_grid_w"]))

    return candidates[0]


def _choose_tesla_solar_surplus_target_from_options(
    base_grid_without_tesla_w: float,
    charge_current_options_a: tuple[int, ...],
) -> dict[str, object]:
    candidates: list[dict[str, object]] = [
        {
            "mode": "off",
            "charge_current_amps": 0,
            "tesla_power_w": 0.0,
            "predicted_grid_w": base_grid_without_tesla_w,
        },
        *[
            {
                "mode": "charge",
                "charge_current_amps": int(amps),
                "tesla_power_w": float(amps) * TESLA_ASSUMED_CHARGING_VOLTAGE_V,
                "predicted_grid_w": base_grid_without_tesla_w + (float(amps) * TESLA_ASSUMED_CHARGING_VOLTAGE_V),
            }
            for amps in charge_current_options_a
        ],
    ]
    valid_candidates = [
        candidate
        for candidate in candidates
        if float(candidate["predicted_grid_w"]) <= 0.0
    ]
    if not valid_candidates:
        return candidates[0]
    return max(valid_candidates, key=lambda candidate: float(candidate["predicted_grid_w"]))


def _tesla_observation_charge_state(observation: dict[str, object]) -> dict[str, object]:
    charging = observation.get("charging")
    charging_map = charging if isinstance(charging, dict) else {}
    return {
        "enabled": charging_map.get("enabled"),
        "requested_enabled": charging_map.get("requested_enabled"),
        "cable_connected": charging_map.get("cable_connected"),
        "connection_state": charging_map.get("connection_state"),
        "status_text": charging_map.get("status_text"),
        "current_amps": _to_number(charging_map.get("current_amps")),
        "configured_current_amps": _to_number(charging_map.get("configured_current_amps")),
        "min_current_amps": _to_number(charging_map.get("min_current_amps")),
        "max_current_amps": _to_number(charging_map.get("max_current_amps")),
        "current_step_amps": _to_number(charging_map.get("current_step_amps")),
        "voltage_v": _to_number(charging_map.get("voltage_v")),
        "power_w": _to_number(charging_map.get("power_w")) or 0.0,
    }


def _tesla_available_charge_current_options(charge_state: dict[str, object]) -> tuple[int, ...]:
    min_a = _to_number(charge_state.get("min_current_amps"))
    max_a = _to_number(charge_state.get("max_current_amps"))
    step_a = _to_number(charge_state.get("current_step_amps"))
    if min_a is None or max_a is None:
        return TESLA_GRID_SUPPORT_CURRENT_OPTIONS_A
    min_i = int(round(min_a))
    max_i = int(round(max_a))
    if min_i <= 0 or max_i < min_i:
        return TESLA_GRID_SUPPORT_CURRENT_OPTIONS_A
    step_i = int(round(step_a or 1))
    step_i = max(step_i, 1)
    supported_values = tuple(range(min_i, max_i + 1, step_i))
    if not supported_values:
        return TESLA_GRID_SUPPORT_CURRENT_OPTIONS_A
    # Use the vehicle-reported full current range so the controller can ramp
    # down below 10A instead of falling back to "off" as soon as 10A exceeds
    # the grid cap.
    return supported_values


async def _run_midday_window_check(
    combined_flow: dict[str, object] | None,
    tesla_observation_result: dict[str, object] | None,
    *,
    round_id: str | None = None,
    requested_at_utc: str | None = None,
) -> dict[str, object]:
    requested_at_utc = str(requested_at_utc or "").strip() or datetime.now(UTC).isoformat()
    started_monotonic = monotonic()
    now_local, timezone_name = _tesla_grid_support_now_local()
    window_mode = _tesla_midday_window_mode(now_local)
    result: dict[str, object] = {
        "executed_at_utc": datetime.now(UTC).isoformat(),
        "evaluated_at_local": now_local.isoformat(),
        "timezone": timezone_name,
        "window_active": window_mode != "off",
        "window_mode": window_mode,
        "window_schedule": _window_schedule_text(window_mode),
        "window_tariff_p_per_kwh": _window_tariff_p_per_kwh(window_mode),
        "free_energy_window_active": window_mode == "free_energy",
        "after_free_shoulder_window_active": window_mode == "after_free_shoulder",
        "after_free_peak_window_active": window_mode == "after_free_peak",
        "export_window_active": window_mode == "export_window",
        "post_export_peak_window_active": window_mode == "post_export_peak",
        "target_grid_min_w": TESLA_GRID_SUPPORT_TARGET_MIN_W,
        "target_grid_max_w": TESLA_GRID_SUPPORT_TARGET_MAX_W,
        "hard_grid_max_w": TESLA_GRID_SUPPORT_HARD_MAX_W,
        "assumed_voltage_v": TESLA_ASSUMED_CHARGING_VOLTAGE_V,
    }
    status = "ok"
    ok = True
    error_text: str | None = None
    try:
        enabled_map = await _load_time_window_rule_enabled_map()
        result["rule_enabled_map"] = enabled_map
        result["saj_profile_guard"] = await _guard_saj_profile_for_time(now_local, enabled_map)
        observation = (
            tesla_observation_result.get("observation")
            if isinstance(tesla_observation_result, dict)
            else None
        )
        observation_map = observation if isinstance(observation, dict) else {}
        observed_entity_count = int(observation_map.get("observed_entity_count") or 0)
        if observed_entity_count <= 0:
            result["skipped"] = "tesla_observation_unavailable"
            status = "skipped"
            return result

        charge_state = _tesla_observation_charge_state(observation_map)
        result["tesla_state_before"] = charge_state
        charging_enabled = bool(charge_state.get("enabled"))
        charge_requested_enabled = bool(charge_state.get("requested_enabled"))

        if window_mode == "off":
            combined_status = collector_status.setdefault("combined", {})
            combined_status["grid_import_active"] = False
            combined_status["peak_price_grid_import_active"] = False
            combined_status["solar_surplus_export_last_tracked_at"] = None
            result["decision"] = {
                "mode": "observe_only",
                "charge_current_amps": 0,
                "predicted_grid_w": None,
            }
            result["decision_reason"] = "outside_tesla_automation_windows"
            result["action"] = {"type": "noop", "reason": "outside_tesla_automation_windows"}
            status = "noop"
            return result

        tesla_rule_code = _tesla_rule_code_for_window(window_mode)
        tesla_rule_enabled = _is_time_window_rule_enabled(enabled_map, tesla_rule_code or "")
        result["tesla_rule_code"] = tesla_rule_code
        result["tesla_rule_enabled"] = tesla_rule_enabled

        metrics = combined_flow.get("metrics") if isinstance(combined_flow, dict) else None
        metrics_map = metrics if isinstance(metrics, dict) else {}
        grid_w = _to_number(metrics_map.get("grid_w"))
        if grid_w is None:
            result["skipped"] = "combined_grid_unavailable"
            status = "skipped"
            return result
        current_grid_import_w = max(float(grid_w), 0.0)
        result["current_grid_import_w"] = round(current_grid_import_w, 1)

        tesla_charge_power_w = max(float(charge_state.get("power_w") or 0.0), 0.0)
        available_current_options = _tesla_available_charge_current_options(charge_state)
        result["available_current_options_amps"] = list(available_current_options)
        current_configured_amps = _to_number(charge_state.get("configured_current_amps"))
        current_actual_amps = _to_number(charge_state.get("current_amps"))
        connection_state = str(charge_state.get("connection_state") or "").strip().lower()
        status_text = str(charge_state.get("status_text") or "").strip().lower()
        combined_status = collector_status.setdefault("combined", {})

        if window_mode == "export_window":
            pv_w = _to_number(metrics_map.get("pv_w"))
            load_w = _to_number(metrics_map.get("load_w"))
            saj_soc_percent = _to_number(metrics_map.get("battery1_soc_percent"))
            solplanet_soc_percent = _to_number(metrics_map.get("battery2_soc_percent"))
            current_grid_export_w = max(-float(grid_w), 0.0)
            base_grid_without_tesla_w = float(grid_w) - tesla_charge_power_w
            export_without_tesla_w = max(-base_grid_without_tesla_w, 0.0)
            solar_excess_vs_load_w = (
                max(float(pv_w) - float(load_w), 0.0)
                if pv_w is not None and load_w is not None
                else None
            )
            can_start_from_soc = (
                solplanet_soc_percent is not None
                and solplanet_soc_percent >= TESLA_SOLAR_SURPLUS_START_SOLPLANET_SOC_PERCENT
            )
            can_continue_from_soc = (
                solplanet_soc_percent is not None
                and solplanet_soc_percent >= TESLA_SOLAR_SURPLUS_STOP_SOLPLANET_SOC_PERCENT
            )
            export_signal_active = (
                current_grid_export_w >= TESLA_SOLAR_SURPLUS_MIN_EXPORT_W
                or export_without_tesla_w >= TESLA_SOLAR_SURPLUS_MIN_EXPORT_W
            )
            solar_signal_active = (
                solar_excess_vs_load_w is not None
                and solar_excess_vs_load_w >= TESLA_SOLAR_SURPLUS_MIN_EXPORT_W
            )

            result["current_grid_export_w"] = round(current_grid_export_w, 1)
            result["base_grid_without_tesla_w"] = round(base_grid_without_tesla_w, 1)
            result["base_export_without_tesla_w"] = round(export_without_tesla_w, 1)
            result["export_window"] = {
                "pv_w": pv_w,
                "load_w": load_w,
                "solar_excess_vs_load_w": round(solar_excess_vs_load_w, 1) if solar_excess_vs_load_w is not None else None,
                "saj_soc_percent": saj_soc_percent,
                "solplanet_soc_percent": solplanet_soc_percent,
                "start_soc_percent": TESLA_SOLAR_SURPLUS_START_SOLPLANET_SOC_PERCENT,
                "stop_soc_percent": TESLA_SOLAR_SURPLUS_STOP_SOLPLANET_SOC_PERCENT,
                "min_export_signal_w": TESLA_SOLAR_SURPLUS_MIN_EXPORT_W,
                "export_signal_active": export_signal_active,
                "solar_signal_active": solar_signal_active,
                "soc_start_allowed": can_start_from_soc,
                "soc_continue_allowed": can_continue_from_soc,
                "dashboard_mapping": {
                    "battery1_soc_percent": "saj_battery_soc",
                    "battery2_soc_percent": "solplanet_battery_soc",
                },
            }
            result["solar_surplus_export_tracking"] = _update_solar_surplus_export_energy_tracking(
                combined_status=combined_status,
                now_local=now_local,
                current_grid_export_w=current_grid_export_w,
            )

        if tesla_rule_code:
            if connection_state == "unplugged":
                result["skipped"] = "tesla_unplugged"
                status = "skipped"
                return result

            if status_text in ("disconnected", "unknown", "unavailable"):
                result["skipped"] = "tesla_not_ready"
                result["tesla_status"] = status_text
                status = "skipped"
                return result

        if window_mode == "post_export_peak":
            solplanet_soc_percent = _to_number(metrics_map.get("battery2_soc_percent"))
            combined_status = collector_status.setdefault("combined", {})
            result["post_export_peak"] = {
                "tariff_p_per_kwh": POST_EXPORT_PEAK_WINDOW_TARIFF_P_PER_KWH,
                "solplanet_soc_percent": solplanet_soc_percent,
                "low_battery_alarm_threshold_soc_percent": SOLPLANET_LOW_BATTERY_NOTIFICATION_SOC_PERCENT,
            }
            if (
                solplanet_soc_percent is not None
                and solplanet_soc_percent < SOLPLANET_LOW_BATTERY_NOTIFICATION_SOC_PERCENT
                and _is_time_window_rule_enabled(enabled_map, "solplanet_low_battery_post_export_peak")
            ):
                _append_worker_notification(result, {
                    "level": "warning",
                    "code": "solplanet_low_battery_post_export_peak",
                    "target": "solplanet_battery",
                    "window": _window_schedule_text("post_export_peak"),
                    "tariff_p_per_kwh": POST_EXPORT_PEAK_WINDOW_TARIFF_P_PER_KWH,
                    "threshold_soc_percent": SOLPLANET_LOW_BATTERY_NOTIFICATION_SOC_PERCENT,
                    "current_soc_percent": round(solplanet_soc_percent, 1),
                    "message": (
                        "Solplanet battery SOC is below 20% during the post-export peak window; "
                        "raise an early warning to avoid expensive grid import."
                    ),
                })
            was_importing = bool(combined_status.get("peak_price_grid_import_active"))
            is_importing = current_grid_import_w > 0.0
            if (
                is_importing
                and not was_importing
                and _is_time_window_rule_enabled(enabled_map, "grid_import_started_post_export_peak")
            ):
                _append_worker_notification(result, {
                    "level": "alarm",
                    "code": "grid_import_started_post_export_peak",
                    "target": "grid",
                    "window": _window_schedule_text("post_export_peak"),
                    "tariff_p_per_kwh": POST_EXPORT_PEAK_WINDOW_TARIFF_P_PER_KWH,
                    "current_grid_import_w": round(current_grid_import_w, 1),
                    "message": (
                        "Grid import started during the post-export peak window; raise one alarm "
                        "so this expensive period does not consume more than expected."
                    ),
                })
            combined_status["peak_price_grid_import_active"] = is_importing
            result["decision"] = {
                "mode": "observe_only",
                "charge_current_amps": int(round(current_configured_amps)) if current_configured_amps is not None else 0,
                "predicted_grid_w": round(float(grid_w), 1),
            }
            result["decision_reason"] = "post_export_peak_notifications_only"
            result["action"] = {"type": "noop", "reason": "post_export_peak_notifications_only"}
            status = "noop"
            return result

        if window_mode == "after_free_shoulder":
            solplanet_soc_percent = _to_number(metrics_map.get("battery2_soc_percent"))
            solplanet_available_capacity_kwh = _to_number(metrics_map.get("battery2_energy_kwh"))
            if solplanet_soc_percent is None:
                result["skipped"] = "combined_solplanet_soc_unavailable"
                status = "skipped"
                return result

            result["after_free_shoulder"] = {
                "solplanet_soc_percent": solplanet_soc_percent,
                "solplanet_available_capacity_kwh": solplanet_available_capacity_kwh,
                "start_soc_percent": TESLA_SOLAR_SURPLUS_START_SOLPLANET_SOC_PERCENT,
                "stop_soc_percent": TESLA_SOLAR_SURPLUS_STOP_SOLPLANET_SOC_PERCENT,
                "low_available_capacity_alarm_threshold_kwh": SOLPLANET_LOW_AVAILABLE_CAPACITY_NOTIFICATION_THRESHOLD_KWH,
                "dashboard_mapping": {
                    "battery2_soc_percent": "solplanet_battery_soc",
                    "battery2_energy_kwh": "solplanet_battery_energy",
                },
            }
            if (
                solplanet_available_capacity_kwh is not None
                and solplanet_available_capacity_kwh < SOLPLANET_LOW_AVAILABLE_CAPACITY_NOTIFICATION_THRESHOLD_KWH
                and _is_time_window_rule_enabled(enabled_map, "solplanet_low_available_capacity")
            ):
                _append_worker_notification(result, {
                    "level": "alarm",
                    "code": "solplanet_low_available_capacity",
                    "target": "solplanet_battery_capacity",
                    "window": _window_schedule_text("after_free_shoulder"),
                    "threshold_kwh": SOLPLANET_LOW_AVAILABLE_CAPACITY_NOTIFICATION_THRESHOLD_KWH,
                    "current_capacity_kwh": round(solplanet_available_capacity_kwh, 1),
                    "message": (
                        "Solplanet available battery capacity is below 25kWh during the after-free shoulder window; "
                        "keep enough energy for the export peak window."
                    ),
                })

            active_or_requested = charging_enabled or charge_requested_enabled
            should_start = solplanet_soc_percent >= TESLA_SOLAR_SURPLUS_START_SOLPLANET_SOC_PERCENT
            should_stop = solplanet_soc_percent < TESLA_SOLAR_SURPLUS_STOP_SOLPLANET_SOC_PERCENT

            if not tesla_rule_enabled:
                result["decision"] = {
                    "mode": "observe_only",
                    "charge_current_amps": int(round(current_configured_amps)) if current_configured_amps is not None else 0,
                    "predicted_grid_w": round(float(grid_w), 1),
                }
                result["decision_reason"] = "time_window_rule_disabled"
                result["action"] = {"type": "noop", "reason": "rule_disabled"}
                status = "noop"
                return result

            if should_start:
                result["decision"] = {
                    "mode": "charge",
                    "charge_current_amps": int(round(current_configured_amps))
                    if current_configured_amps is not None
                    else 0,
                    "predicted_grid_w": round(float(grid_w), 1),
                }
                result["decision_reason"] = "after_free_shoulder_solplanet_soc_at_or_above_start_threshold"
                if charging_enabled:
                    result["action"] = {"type": "noop", "reason": "already_charging"}
                    status = "noop"
                    return result
                if charge_requested_enabled:
                    await _tesla_restart_charging()
                    result["action"] = {"type": "restart_charging", "reason": "charging_requested_but_not_active"}
                else:
                    await _tesla_set_charging(True)
                    result["action"] = {"type": "start_charging", "reason": "soc_at_or_above_start_threshold"}
                status = "applied"
                return result

            if should_stop:
                result["decision"] = {
                    "mode": "off",
                    "charge_current_amps": 0,
                    "predicted_grid_w": round(float(grid_w), 1),
                }
                result["decision_reason"] = "after_free_shoulder_solplanet_soc_below_stop_threshold"
                if active_or_requested:
                    await _tesla_set_charging(False)
                    result["action"] = {"type": "stop_charging", "reason": "soc_below_stop_threshold"}
                    status = "applied"
                else:
                    result["action"] = {"type": "noop", "reason": "already_stopped"}
                    status = "noop"
                return result

            result["decision"] = {
                "mode": "hold_current_state",
                "charge_current_amps": int(round(current_configured_amps))
                if current_configured_amps is not None
                else 0,
                "predicted_grid_w": round(float(grid_w), 1),
            }
            result["decision_reason"] = "after_free_shoulder_solplanet_soc_between_thresholds"
            result["action"] = {"type": "noop", "reason": "within_soc_hysteresis_band"}
            status = "noop"
            return result

        if window_mode == "after_free_peak":
            solplanet_soc_percent = _to_number(metrics_map.get("battery2_soc_percent"))
            solplanet_available_capacity_kwh = _to_number(metrics_map.get("battery2_energy_kwh"))
            if solplanet_soc_percent is None:
                result["skipped"] = "combined_solplanet_soc_unavailable"
                status = "skipped"
                return result

            result["after_free_peak"] = {
                "solplanet_soc_percent": solplanet_soc_percent,
                "solplanet_available_capacity_kwh": solplanet_available_capacity_kwh,
                "start_soc_percent": TESLA_SOLAR_SURPLUS_START_SOLPLANET_SOC_PERCENT,
                "stop_soc_percent": TESLA_SOLAR_SURPLUS_STOP_SOLPLANET_SOC_PERCENT,
                "low_available_capacity_alarm_threshold_kwh": SOLPLANET_LOW_AVAILABLE_CAPACITY_NOTIFICATION_THRESHOLD_KWH,
                "dashboard_mapping": {
                    "battery2_soc_percent": "solplanet_battery_soc",
                    "battery2_energy_kwh": "solplanet_battery_energy",
                },
            }
            if (
                solplanet_available_capacity_kwh is not None
                and solplanet_available_capacity_kwh < SOLPLANET_LOW_AVAILABLE_CAPACITY_NOTIFICATION_THRESHOLD_KWH
                and _is_time_window_rule_enabled(enabled_map, "solplanet_low_available_capacity")
            ):
                _append_worker_notification(result, {
                    "level": "alarm",
                    "code": "solplanet_low_available_capacity",
                    "target": "solplanet_battery_capacity",
                    "window": _window_schedule_text("after_free_peak"),
                    "threshold_kwh": SOLPLANET_LOW_AVAILABLE_CAPACITY_NOTIFICATION_THRESHOLD_KWH,
                    "current_capacity_kwh": round(solplanet_available_capacity_kwh, 1),
                    "message": (
                        "Solplanet available battery capacity is below 25kWh during the after-free peak window; "
                        "keep enough energy for the export peak window."
                    ),
                })

            active_or_requested = charging_enabled or charge_requested_enabled
            should_start = solplanet_soc_percent >= TESLA_SOLAR_SURPLUS_START_SOLPLANET_SOC_PERCENT
            should_stop = solplanet_soc_percent < TESLA_SOLAR_SURPLUS_STOP_SOLPLANET_SOC_PERCENT

            if not tesla_rule_enabled:
                result["decision"] = {
                    "mode": "observe_only",
                    "charge_current_amps": int(round(current_configured_amps)) if current_configured_amps is not None else 0,
                    "predicted_grid_w": round(float(grid_w), 1),
                }
                result["decision_reason"] = "time_window_rule_disabled"
                result["action"] = {"type": "noop", "reason": "rule_disabled"}
                status = "noop"
                return result

            if should_start:
                result["decision"] = {
                    "mode": "charge",
                    "charge_current_amps": int(round(current_configured_amps))
                    if current_configured_amps is not None
                    else 0,
                    "predicted_grid_w": round(float(grid_w), 1),
                }
                result["decision_reason"] = "after_free_peak_solplanet_soc_at_or_above_start_threshold"
                if charging_enabled:
                    result["action"] = {"type": "noop", "reason": "already_charging"}
                    status = "noop"
                    return result
                if charge_requested_enabled:
                    await _tesla_restart_charging()
                    result["action"] = {"type": "restart_charging", "reason": "charging_requested_but_not_active"}
                else:
                    await _tesla_set_charging(True)
                    result["action"] = {"type": "start_charging", "reason": "soc_at_or_above_start_threshold"}
                status = "applied"
                return result

            if should_stop:
                result["decision"] = {
                    "mode": "off",
                    "charge_current_amps": 0,
                    "predicted_grid_w": round(float(grid_w), 1),
                }
                result["decision_reason"] = "after_free_peak_solplanet_soc_below_stop_threshold"
                if active_or_requested:
                    await _tesla_set_charging(False)
                    result["action"] = {"type": "stop_charging", "reason": "soc_below_stop_threshold"}
                    status = "applied"
                else:
                    result["action"] = {"type": "noop", "reason": "already_stopped"}
                    status = "noop"
                return result

            result["decision"] = {
                "mode": "hold_current_state",
                "charge_current_amps": int(round(current_configured_amps))
                if current_configured_amps is not None
                else 0,
                "predicted_grid_w": round(float(grid_w), 1),
            }
            result["decision_reason"] = "after_free_peak_solplanet_soc_between_thresholds"
            result["action"] = {"type": "noop", "reason": "within_soc_hysteresis_band"}
            status = "noop"
            return result

        if window_mode == "export_window":
            pv_w = _to_number(metrics_map.get("pv_w"))
            load_w = _to_number(metrics_map.get("load_w"))
            saj_soc_percent = _to_number(metrics_map.get("battery1_soc_percent"))
            solplanet_soc_percent = _to_number(metrics_map.get("battery2_soc_percent"))
            if solplanet_soc_percent is None:
                result["skipped"] = "combined_solplanet_soc_unavailable"
                status = "skipped"
                return result

            current_grid_export_w = max(-float(grid_w), 0.0)
            base_grid_without_tesla_w = float(grid_w) - tesla_charge_power_w
            export_without_tesla_w = max(-base_grid_without_tesla_w, 0.0)
            solar_excess_vs_load_w = (
                max(float(pv_w) - float(load_w), 0.0)
                if pv_w is not None and load_w is not None
                else None
            )
            can_start_from_soc = solplanet_soc_percent >= TESLA_SOLAR_SURPLUS_START_SOLPLANET_SOC_PERCENT
            can_continue_from_soc = solplanet_soc_percent >= TESLA_SOLAR_SURPLUS_STOP_SOLPLANET_SOC_PERCENT
            export_signal_active = (
                current_grid_export_w >= TESLA_SOLAR_SURPLUS_MIN_EXPORT_W
                or export_without_tesla_w >= TESLA_SOLAR_SURPLUS_MIN_EXPORT_W
            )
            solar_signal_active = (
                solar_excess_vs_load_w is not None
                and solar_excess_vs_load_w >= TESLA_SOLAR_SURPLUS_MIN_EXPORT_W
            )

            result["current_grid_export_w"] = round(current_grid_export_w, 1)
            result["base_grid_without_tesla_w"] = round(base_grid_without_tesla_w, 1)
            result["base_export_without_tesla_w"] = round(export_without_tesla_w, 1)
            result["export_window"] = {
                "pv_w": pv_w,
                "load_w": load_w,
                "solar_excess_vs_load_w": round(solar_excess_vs_load_w, 1) if solar_excess_vs_load_w is not None else None,
                "saj_soc_percent": saj_soc_percent,
                "solplanet_soc_percent": solplanet_soc_percent,
                "start_soc_percent": TESLA_SOLAR_SURPLUS_START_SOLPLANET_SOC_PERCENT,
                "stop_soc_percent": TESLA_SOLAR_SURPLUS_STOP_SOLPLANET_SOC_PERCENT,
                "min_export_signal_w": TESLA_SOLAR_SURPLUS_MIN_EXPORT_W,
                "export_signal_active": export_signal_active,
                "solar_signal_active": solar_signal_active,
                "soc_start_allowed": can_start_from_soc,
                "soc_continue_allowed": can_continue_from_soc,
                "dashboard_mapping": {
                    "battery1_soc_percent": "saj_battery_soc",
                    "battery2_soc_percent": "solplanet_battery_soc",
                },
            }
            if (
                solplanet_soc_percent < SOLPLANET_LOW_BATTERY_NOTIFICATION_SOC_PERCENT
                and _is_time_window_rule_enabled(enabled_map, "solplanet_low_battery")
            ):
                _append_worker_notification(result, {
                        "level": "warning",
                        "code": "solplanet_low_battery",
                        "target": "solplanet_battery",
                    "threshold_soc_percent": SOLPLANET_LOW_BATTERY_NOTIFICATION_SOC_PERCENT,
                    "current_soc_percent": round(solplanet_soc_percent, 1),
                        "message": (
                        "Solplanet battery SOC is below 20% during the export window; keep one reminder in worklog "
                        "until notification handling is implemented."
                    ),
                })
            was_importing = bool(combined_status.get("grid_import_active"))
            is_importing = current_grid_import_w > 0.0
            if is_importing and not was_importing and _is_time_window_rule_enabled(enabled_map, "grid_import_started"):
                _append_worker_notification(result, {
                    "level": "alarm",
                    "code": "grid_import_started",
                    "target": "grid",
                    "current_grid_import_w": round(current_grid_import_w, 1),
                    "message": (
                        "Grid import started during the export window; add one alarm "
                        "reminder to worklog."
                    ),
                })
            combined_status["grid_import_active"] = is_importing
            export_tracking = result.get("solar_surplus_export_tracking")
            export_tracking = export_tracking if isinstance(export_tracking, dict) else _update_solar_surplus_export_energy_tracking(
                combined_status=combined_status,
                now_local=now_local,
                current_grid_export_w=current_grid_export_w,
            )
            result["solar_surplus_export_tracking"] = export_tracking
            previous_total_export_wh = float(export_tracking["total_export_wh"]) - float(export_tracking["added_export_wh"])
            threshold_notified_map = combined_status.get("solar_surplus_export_threshold_notified_map")
            if not isinstance(threshold_notified_map, dict):
                threshold_notified_map = {}
            for threshold_wh in SOLAR_SURPLUS_EXPORT_ENERGY_NOTIFICATION_THRESHOLDS_WH:
                threshold_key = str(int(round(threshold_wh)))
                if (
                    previous_total_export_wh < threshold_wh
                    and float(export_tracking["total_export_wh"]) >= threshold_wh
                    and not bool(threshold_notified_map.get(threshold_key))
                    and _is_time_window_rule_enabled(enabled_map, _solar_surplus_export_energy_notification_code(threshold_wh))
                ):
                    _append_worker_notification(result, {
                        "level": "alarm",
                        "code": _solar_surplus_export_energy_notification_code(threshold_wh),
                        "target": f"grid_export_energy_{threshold_key}",
                        "window": _window_schedule_text("export_window"),
                        "current_export_total_wh": round(float(export_tracking["total_export_wh"]), 1),
                        "current_export_total_kwh": round(float(export_tracking["total_export_wh"]) / 1000.0, 4),
                        "threshold_wh": threshold_wh,
                        "threshold_kwh": round(threshold_wh / 1000.0, 3),
                        "message": (
                            "Exported energy during the export window reached the configured "
                            "threshold; add one alarm reminder to worklog."
                        ),
                    })
                    threshold_notified_map[threshold_key] = True
            combined_status["solar_surplus_export_threshold_notified_map"] = threshold_notified_map
            combined_status["solar_surplus_export_threshold_notified"] = any(threshold_notified_map.values())
            result["decision"] = {
                "mode": "observe_only",
                "charge_current_amps": int(round(current_configured_amps)) if current_configured_amps is not None else 0,
                "predicted_grid_w": round(float(grid_w), 1),
            }
            result["decision_reason"] = "export_window_notifications_only"
            result["action"] = {"type": "noop", "reason": "export_window_notifications_only"}
            status = "noop"
            return result

        base_grid_w = max(float(result["current_grid_import_w"]) - tesla_charge_power_w, 0.0)
        result["base_grid_without_tesla_w"] = round(base_grid_w, 1)
        desired = _choose_tesla_grid_support_target_from_options(base_grid_w, available_current_options)
        result["decision"] = desired
        desired_predicted_grid_w = float(desired.get("predicted_grid_w") or 0.0)
        result["decision_reason"] = (
            "selected_max_safe_candidate_under_target_grid_cap"
            if desired_predicted_grid_w <= TESLA_GRID_SUPPORT_TARGET_MAX_W
            else "selected_max_safe_candidate_within_hard_grid_cap_buffer"
        )

        desired_amps = int(desired["charge_current_amps"])
        current_grid_import_w = float(result["current_grid_import_w"])
        effective_current_amps = current_actual_amps or current_configured_amps or 0.0
        configured_matches_desired = (
            current_configured_amps is not None
            and int(round(current_configured_amps)) == desired_amps
        )
        actual_matches_desired = (
            current_actual_amps is not None
            and int(round(current_actual_amps)) == desired_amps
        )
        current_gap_to_desired_amps = (
            float(desired_amps) - float(current_actual_amps)
            if current_actual_amps is not None
            else None
        )
        current_mismatch_restart_needed = bool(
            desired["mode"] != "off"
            and charging_enabled
            and charge_requested_enabled
            and configured_matches_desired
            and current_gap_to_desired_amps is not None
            and current_gap_to_desired_amps >= TESLA_CURRENT_MISMATCH_RESTART_MIN_DELTA_A
        )
        current_mismatch_restart_allowed = current_mismatch_restart_needed
        current_mismatch_restart_cooldown_remaining_s: float | None = None
        if current_mismatch_restart_needed:
            combined_status = collector_status.setdefault("combined", {})
            last_restart_at_text = str(
                combined_status.get("last_tesla_current_mismatch_restart_at") or ""
            ).strip()
            if last_restart_at_text:
                try:
                    last_restart_at = datetime.fromisoformat(last_restart_at_text.replace("Z", "+00:00"))
                    if last_restart_at.tzinfo is None:
                        last_restart_at = last_restart_at.replace(tzinfo=UTC)
                    elapsed_s = max((datetime.now(UTC) - last_restart_at.astimezone(UTC)).total_seconds(), 0.0)
                    if elapsed_s < TESLA_CURRENT_MISMATCH_RESTART_COOLDOWN_SECONDS:
                        current_mismatch_restart_allowed = False
                        current_mismatch_restart_cooldown_remaining_s = round(
                            TESLA_CURRENT_MISMATCH_RESTART_COOLDOWN_SECONDS - elapsed_s,
                            1,
                        )
                except ValueError:
                    current_mismatch_restart_allowed = current_mismatch_restart_needed

        result["candidates"] = [
            {
                "mode": "off" if amps == 0 else "charge",
                "charge_current_amps": amps,
                "predicted_grid_w": round(base_grid_w + (float(amps) * TESLA_ASSUMED_CHARGING_VOLTAGE_V), 1),
                "safe": (base_grid_w + (float(amps) * TESLA_ASSUMED_CHARGING_VOLTAGE_V)) <= TESLA_GRID_SUPPORT_HARD_MAX_W,
            }
            for amps in (0, *available_current_options)
        ]

        if (
            charging_enabled
            and configured_matches_desired
            and (current_actual_amps is None or actual_matches_desired)
        ):
            result["decision"] = {
                "mode": "hold_current_state",
                "charge_current_amps": int(round(effective_current_amps)) if effective_current_amps > 0 else desired_amps,
                "predicted_grid_w": round(current_grid_import_w, 1),
            }
            result["decision_reason"] = "already_at_max_safe_current_under_grid_cap"
            result["action"] = {"type": "noop", "reason": "keep_current_state"}
            status = "noop"
            return result

        if not tesla_rule_enabled:
            result["decision"] = {
                "mode": "observe_only",
                "charge_current_amps": int(round(current_configured_amps)) if current_configured_amps is not None else desired_amps,
                "predicted_grid_w": round(current_grid_import_w, 1),
            }
            result["decision_reason"] = "time_window_rule_disabled"
            result["action"] = {"type": "noop", "reason": "rule_disabled"}
            status = "noop"
            return result

        if desired["mode"] == "off":
            if charge_requested_enabled or charging_enabled:
                await _tesla_set_charging(False)
                result["action"] = {"type": "stop_charging"}
                status = "applied"
            else:
                result["action"] = {"type": "noop", "reason": "already_stopped"}
                status = "noop"
            return result

        actions: list[str] = []
        if current_configured_amps is None or int(round(current_configured_amps)) != desired_amps:
            await _tesla_set_charge_current(desired_amps)
            actions.append(f"set_current_{desired_amps}a")
        if current_mismatch_restart_needed:
            result["current_mismatch"] = {
                "configured_current_amps": current_configured_amps,
                "actual_current_amps": current_actual_amps,
                "desired_current_amps": desired_amps,
                "gap_to_desired_amps": round(current_gap_to_desired_amps or 0.0, 1),
                "restart_allowed": current_mismatch_restart_allowed,
                "restart_cooldown_remaining_s": current_mismatch_restart_cooldown_remaining_s,
            }
        if current_mismatch_restart_allowed:
            await _tesla_restart_charging()
            collector_status.setdefault("combined", {})["last_tesla_current_mismatch_restart_at"] = datetime.now(UTC).isoformat()
            actions.append("restart_charging_for_current_mismatch")
        if not charging_enabled:
            if charge_requested_enabled:
                await _tesla_restart_charging()
                actions.append("restart_charging")
            else:
                await _tesla_set_charging(True)
                actions.append("start_charging")

        if actions:
            result["action"] = {"type": "apply", "steps": actions}
            status = "applied"
        else:
            result["action"] = {
                "type": "noop",
                "reason": (
                    "configured_target_applied_actual_current_differs"
                    if configured_matches_desired and current_actual_amps is not None and not actual_matches_desired
                    else "already_at_target"
                ),
            }
            status = "noop"
        return result
    except Exception as exc:  # noqa: BLE001
        ok = False
        status = "failed"
        error_text = f"{type(exc).__name__}: {exc}"
        result["error"] = error_text
        raise
    finally:
        result["log_ok"] = ok
        result["log_status"] = status
        result["log_error_text"] = error_text


async def _run_saj_battery_watch_check(
    combined_flow: dict[str, object] | None,
    *,
    round_id: str | None = None,
    requested_at_utc: str | None = None,
) -> dict[str, object]:
    requested_at_utc = str(requested_at_utc or "").strip() or datetime.now(UTC).isoformat()
    started_monotonic = monotonic()
    now_local, timezone_name = _tesla_grid_support_now_local()
    window_active = _is_within_overnight_shoulder_window(now_local)
    result: dict[str, object] = {
        "executed_at_utc": datetime.now(UTC).isoformat(),
        "evaluated_at_local": now_local.isoformat(),
        "timezone": timezone_name,
        "window_active": window_active,
        "window_mode": "overnight_shoulder" if window_active else "off",
        "window_schedule": _window_schedule_text("overnight_shoulder"),
        "target": "saj_battery",
        "thresholds_soc_percent": [
            SAJ_BATTERY_WATCH_REMINDER_THRESHOLD_HIGH_SOC_PERCENT,
            SAJ_BATTERY_WATCH_REMINDER_THRESHOLD_LOW_SOC_PERCENT,
        ],
    }
    status = "ok"
    ok = True
    error_text: str | None = None
    try:
        enabled_map = await _load_time_window_rule_enabled_map()
        result["rule_enabled_map"] = enabled_map
        if not window_active:
            result["decision"] = {"mode": "observe_only"}
            result["action"] = {"type": "noop", "reason": "outside_window"}
            status = "outside_window"
            return result

        metrics = combined_flow.get("metrics") if isinstance(combined_flow, dict) else None
        metrics_map = metrics if isinstance(metrics, dict) else {}
        saj_soc_percent = _to_number(metrics_map.get("battery1_soc_percent"))
        result["saj_soc_percent"] = saj_soc_percent
        result["battery1_soc_percent"] = saj_soc_percent
        result["battery2_soc_percent"] = _to_number(metrics_map.get("battery2_soc_percent"))
        if saj_soc_percent is None:
            result["skipped"] = "combined_saj_soc_unavailable"
            status = "skipped"
            return result

        combined_status = collector_status.setdefault("combined", {})
        window_key = _overnight_shoulder_window_key(now_local)
        last_window_key = str(combined_status.get("saj_battery_watch_window_key") or "").strip()
        if last_window_key != window_key:
            combined_status["saj_battery_watch_50_notified"] = False
            combined_status["saj_battery_watch_20_notified"] = False
        combined_status["saj_battery_watch_window_key"] = window_key

        notified_50 = bool(combined_status.get("saj_battery_watch_50_notified"))
        notified_20 = bool(combined_status.get("saj_battery_watch_20_notified"))
        result["window_key"] = window_key
        result["notification_state"] = {
            "reminder_50_sent": notified_50,
            "reminder_20_sent": notified_20,
        }

        if (
            saj_soc_percent <= SAJ_BATTERY_WATCH_REMINDER_THRESHOLD_HIGH_SOC_PERCENT
            and not notified_50
            and _is_time_window_rule_enabled(enabled_map, "saj_battery_watch_50_percent")
        ):
            _append_worker_notification(result, {
                "level": "warning",
                "code": "saj_battery_watch_50_percent",
                "target": "saj_battery",
                "window": _window_schedule_text("overnight_shoulder"),
                "threshold_soc_percent": SAJ_BATTERY_WATCH_REMINDER_THRESHOLD_HIGH_SOC_PERCENT,
                "current_soc_percent": round(saj_soc_percent, 1),
                "message": "SAJ battery SOC reached 50% during the overnight shoulder window.",
            })
            combined_status["saj_battery_watch_50_notified"] = True

        if (
            saj_soc_percent <= SAJ_BATTERY_WATCH_REMINDER_THRESHOLD_LOW_SOC_PERCENT
            and not notified_20
            and _is_time_window_rule_enabled(enabled_map, "saj_battery_watch_20_percent")
        ):
            _append_worker_notification(result, {
                "level": "alarm",
                "code": "saj_battery_watch_20_percent",
                "target": "saj_battery",
                "window": _window_schedule_text("overnight_shoulder"),
                "threshold_soc_percent": SAJ_BATTERY_WATCH_REMINDER_THRESHOLD_LOW_SOC_PERCENT,
                "current_soc_percent": round(saj_soc_percent, 1),
                "message": "SAJ battery SOC reached 20% during the overnight shoulder window.",
            })
            combined_status["saj_battery_watch_20_notified"] = True

        result["notification_state"] = {
            "reminder_50_sent": bool(combined_status.get("saj_battery_watch_50_notified")),
            "reminder_20_sent": bool(combined_status.get("saj_battery_watch_20_notified")),
        }
        result["decision"] = {"mode": "observe_only"}
        result["action"] = {"type": "noop", "reason": "watching_saj_battery_soc"}
        status = "noop"
        return result
    except Exception as exc:  # noqa: BLE001
        ok = False
        status = "failed"
        error_text = f"{type(exc).__name__}: {exc}"
        result["error"] = error_text
        raise
    finally:
        result["service_window"] = OVERNIGHT_SHOULDER_SERVICE
        result["service_window_schedule"] = _window_schedule_text("overnight_shoulder")
        result["service_window_active"] = window_active
        result["log_ok"] = ok
        result["log_status"] = status
        result["log_error_text"] = error_text


async def _run_battery_full_notification_check(
    combined_flow: dict[str, object] | None,
    *,
    round_id: str | None = None,
    requested_at_utc: str | None = None,
) -> dict[str, object]:
    requested_at_utc = str(requested_at_utc or "").strip() or datetime.now(UTC).isoformat()
    now_local, timezone_name = _tesla_grid_support_now_local()
    result: dict[str, object] = {
        "executed_at_utc": datetime.now(UTC).isoformat(),
        "evaluated_at_local": now_local.isoformat(),
        "timezone": timezone_name,
        "window_active": True,
        "window_mode": "always",
        "window_schedule": "always",
        "target": "battery_full_watch",
        "threshold_soc_percent": BATTERY_FULL_NOTIFICATION_SOC_PERCENT,
    }
    status = "noop"
    ok = True
    error_text: str | None = None
    try:
        enabled_map = await _load_time_window_rule_enabled_map()
        result["rule_enabled_map"] = enabled_map
        metrics = combined_flow.get("metrics") if isinstance(combined_flow, dict) else None
        metrics_map = metrics if isinstance(metrics, dict) else {}
        saj_soc_percent = _to_number(metrics_map.get("battery1_soc_percent"))
        solplanet_soc_percent = _to_number(metrics_map.get("battery2_soc_percent"))
        result["battery1_soc_percent"] = saj_soc_percent
        result["battery2_soc_percent"] = solplanet_soc_percent
        if saj_soc_percent is None and solplanet_soc_percent is None:
            result["skipped"] = "combined_battery_soc_unavailable"
            status = "skipped"
            return result

        persisted_state = await _load_battery_full_active_state()
        saj_full_now = saj_soc_percent is not None and saj_soc_percent >= BATTERY_FULL_NOTIFICATION_SOC_PERCENT
        solplanet_full_now = (
            solplanet_soc_percent is not None and solplanet_soc_percent >= BATTERY_FULL_NOTIFICATION_SOC_PERCENT
        )
        saj_full_prev = bool(persisted_state.get("saj"))
        solplanet_full_prev = bool(persisted_state.get("solplanet"))

        if saj_full_now and not saj_full_prev and _is_time_window_rule_enabled(enabled_map, "saj_battery_full"):
            _append_worker_notification(result, {
                "level": "info",
                "code": "saj_battery_full",
                "target": "saj_battery",
                "window": "always",
                "threshold_soc_percent": BATTERY_FULL_NOTIFICATION_SOC_PERCENT,
                "current_soc_percent": round(saj_soc_percent or 0.0, 1),
                "message": "SAJ battery reached 100% SOC.",
            })
        if (
            solplanet_full_now
            and not solplanet_full_prev
            and _is_time_window_rule_enabled(enabled_map, "solplanet_battery_full")
        ):
            _append_worker_notification(result, {
                "level": "info",
                "code": "solplanet_battery_full",
                "target": "solplanet_battery",
                "window": "always",
                "threshold_soc_percent": BATTERY_FULL_NOTIFICATION_SOC_PERCENT,
                "current_soc_percent": round(solplanet_soc_percent or 0.0, 1),
                "message": "Solplanet battery reached 100% SOC.",
            })

        await _store_battery_full_active_state(
            saj_full_active=saj_full_now,
            solplanet_full_active=solplanet_full_now,
        )
        result["notification_state"] = {
            "saj_full_previous": saj_full_prev,
            "saj_full": saj_full_now,
            "solplanet_full_previous": solplanet_full_prev,
            "solplanet_full": solplanet_full_now,
        }
        result["decision"] = {"mode": "observe_only"}
        result["action"] = {"type": "noop", "reason": "watching_battery_full_soc"}
        status = "noop"
        return result
    except Exception as exc:  # noqa: BLE001
        ok = False
        status = "failed"
        error_text = f"{type(exc).__name__}: {exc}"
        result["error"] = error_text
        raise
    finally:
        result["log_ok"] = ok
        result["log_status"] = status
        result["log_error_text"] = error_text


async def _run_tesla_home_assistant_collection(
    combined_flow: dict[str, object] | None = None,
    *,
    round_id: str | None = None,
    requested_at_utc: str | None = None,
) -> dict[str, object]:
    requested_at_utc = str(requested_at_utc or "").strip() or datetime.now(UTC).isoformat()
    started_monotonic = monotonic()
    now_local, timezone_name = _tesla_grid_support_now_local()
    window_mode = _tesla_midday_window_mode(now_local)
    result: dict[str, object] = {
        "executed_at_utc": datetime.now(UTC).isoformat(),
        "evaluated_at_local": now_local.isoformat(),
        "timezone": timezone_name,
        "window_active": window_mode != "off",
        "window_mode": window_mode,
        "task_mode": "observe_only",
    }
    status = "ok"
    ok = True
    error_text: str | None = None
    try:
        states = await _tesla_control_states()
        control_state = _build_tesla_control_state(states)
        observation = _build_tesla_observation_payload(states)
        result["control_state"] = control_state
        result["observation"] = observation
        if int(observation.get("observed_entity_count") or 0) <= 0:
            result["skipped"] = "tesla_entities_unavailable"
            status = "skipped"
            return result
        return result
    except Exception as exc:  # noqa: BLE001
        ok = False
        status = "failed"
        error_text = f"{type(exc).__name__}: {exc}"
        result["error"] = error_text
        raise
    finally:
        await _persist_worker_control_log(
            round_id=round_id,
            system=TESLA_LOG_SYSTEM,
            service=TESLA_OBSERVATION_SERVICE,
            requested_at_utc=requested_at_utc,
            started_monotonic=started_monotonic,
            ok=ok,
            status=status,
            payload=result,
            error_text=error_text,
        )


def _is_saj_offnet_mode(*, mode_sensor: object, inverter_status: object) -> bool:
    mode_value = _to_number(mode_sensor)
    if mode_value is not None and int(mode_value) == 8:
        return True
    status_text = str(inverter_status or "").strip().lower()
    return "offnet" in status_text or "off-grid" in status_text or "microgrid" in status_text


def _build_energy_flow_payload(system: str, states: list[dict[str, object]]) -> dict[str, object]:
    prefixes = SYSTEM_PREFIXES[system]
    configured_core_entity_ids = _get_system_entity_ids(system)
    states_by_entity_id: dict[str, dict[str, object]] = {
        str(state.get("entity_id", "")): state for state in states
    }

    def _from_config_ids(*keywords: str) -> tuple[str, ...]:
        return tuple(
            entity_id
            for entity_id in configured_core_entity_ids
            if all(keyword in entity_id.lower() for keyword in keywords)
        )

    pv = _find_state_by_entity_ids(
        states_by_entity_id,
        (
            *_from_config_ids("pv", "power"),
            *_from_config_ids("solar", "power"),
            f"sensor.{prefixes[0]}_pv_power",
            f"sensor.{prefixes[0]}_solar_power",
        ),
    ) or _find_state_by_keywords(states, prefixes, (("pv", "power"), ("solar", "power")))

    grid = _find_state_by_entity_ids(
        states_by_entity_id,
        (
            *_from_config_ids("grid", "power"),
            f"sensor.{prefixes[0]}_ct_grid_power_total",
            f"sensor.{prefixes[0]}_fast_ct_grid_power_watt",
            f"sensor.{prefixes[0]}_grid_load_power",
            f"sensor.{prefixes[0]}_fast_grid_load_power",
            f"sensor.{prefixes[0]}_total_grid_power",
        ),
    ) or _find_state_by_keywords(states, prefixes, (("grid", "power"),))

    battery = _find_state_by_entity_ids(
        states_by_entity_id,
        (*_from_config_ids("battery", "power"), f"sensor.{prefixes[0]}_battery_power"),
    ) or _find_state_by_keywords(states, prefixes, (("battery", "power"),))

    load = _find_state_by_entity_ids(
        states_by_entity_id,
        (
            *_from_config_ids("load", "power"),
            f"sensor.{prefixes[0]}_total_load_power",
            f"sensor.{prefixes[0]}_load_power",
        ),
    ) or _find_state_by_keywords(states, prefixes, (("load", "power"),))

    soc = _find_state_by_entity_ids(
        states_by_entity_id,
        (
            *_from_config_ids("battery", "percent"),
            *_from_config_ids("battery", "soc"),
            f"sensor.{prefixes[0]}_battery_energy_percent",
            f"sensor.{prefixes[0]}_battery_soc",
        ),
    ) or _find_state_by_keywords(states, prefixes, (("battery", "percent"), ("battery", "soc")))

    battery_energy = _find_state_by_entity_ids(
        states_by_entity_id,
        tuple(
            entity_id
            for entity_id in _from_config_ids("battery", "energy")
            if "percent" not in entity_id.lower() and "soc" not in entity_id.lower()
        ),
    ) or _find_state_by_keywords(states, prefixes, (("battery", "energy"), ("battery", "remaining", "energy")))

    inverter = _find_state_by_entity_ids(
        states_by_entity_id,
        (*_from_config_ids("inverter", "status"), f"sensor.{prefixes[0]}_inverter_status"),
    ) or _find_state_by_keywords(states, prefixes, (("inverter", "status"),))
    app_mode = _find_state_by_entity_ids(
        states_by_entity_id,
        (f"sensor.{prefixes[0]}_app_mode",),
    ) or _find_state_by_keywords(states, prefixes, (("app", "mode"),))
    pv1 = _find_state_by_entity_ids(
        states_by_entity_id,
        (f"sensor.{prefixes[0]}_pv1_power",),
    ) or _find_state_by_keywords(states, prefixes, (("pv1", "power"),))
    pv2 = _find_state_by_entity_ids(
        states_by_entity_id,
        (f"sensor.{prefixes[0]}_pv2_power",),
    ) or _find_state_by_keywords(states, prefixes, (("pv2", "power"),))
    inverter_power = _find_state_by_entity_ids(
        states_by_entity_id,
        (
            *_from_config_ids("inverter", "power"),
            f"sensor.{prefixes[0]}_inverter_power",
            f"sensor.{prefixes[0]}_total_inverter_power",
            f"sensor.{prefixes[0]}_inverter_output_power",
            f"sensor.{prefixes[0]}_inverter_active_power",
        ),
    ) or _find_state_by_keywords(states, prefixes, (("inverter", "power"),))

    pv_v = _to_number(pv.get("state")) if pv else None
    grid_v = _to_number(grid.get("state")) if grid else None
    battery_v = _to_number(battery.get("state")) if battery else None
    load_v = _to_number(load.get("state")) if load else None
    soc_v = _to_number(soc.get("state")) if soc else None
    battery_energy_v = _to_number(battery_energy.get("state")) if battery_energy else None
    inverter_power_v = _to_number(inverter_power.get("state")) if inverter_power else None
    app_mode_v = _to_number(app_mode.get("state")) if app_mode else None
    pv1_v = _to_number(pv1.get("state")) if pv1 else None
    pv2_v = _to_number(pv2.get("state")) if pv2 else None
    pv_w = _power_to_watts(pv_v, pv.get("attributes", {}).get("unit_of_measurement") if pv else None)  # type: ignore[union-attr]
    grid_w = _power_to_watts(grid_v, grid.get("attributes", {}).get("unit_of_measurement") if grid else None)  # type: ignore[union-attr]
    battery_w = _power_to_watts(
        battery_v,
        battery.get("attributes", {}).get("unit_of_measurement") if battery else None,  # type: ignore[union-attr]
    )
    load_w = _power_to_watts(load_v, load.get("attributes", {}).get("unit_of_measurement") if load else None)  # type: ignore[union-attr]
    inverter_power_w = _power_to_watts(
        inverter_power_v,
        inverter_power.get("attributes", {}).get("unit_of_measurement") if inverter_power else None,  # type: ignore[union-attr]
    )
    pv1_w = _power_to_watts(pv1_v, pv1.get("attributes", {}).get("unit_of_measurement") if pv1 else None)  # type: ignore[union-attr]
    pv2_w = _power_to_watts(pv2_v, pv2.get("attributes", {}).get("unit_of_measurement") if pv2 else None)  # type: ignore[union-attr]
    battery_energy_kwh = _energy_to_kwh(
        battery_energy_v,
        battery_energy.get("attributes", {}).get("unit_of_measurement") if battery_energy else None,  # type: ignore[union-attr]
    )
    inverter_status = inverter.get("state") if inverter else None
    notes: list[str] = []
    if system == "saj" and _is_saj_offnet_mode(mode_sensor=app_mode_v, inverter_status=inverter_status):
        corrected_pv_terms = [value for value in (pv1_w, pv2_w) if value is not None]
        if corrected_pv_terms:
            raw_pv_w = pv_w
            pv_w = sum(corrected_pv_terms)
            pv_source = "calc:saj_pv1_power + saj_pv2_power"
            notes.append("saj_offnet_detected")
            notes.append(f"saj_corrected_pv_w_source:{pv_source}")
            if raw_pv_w is not None:
                notes.append(f"saj_raw_pv_power_w:{round(raw_pv_w, 1)}")
        else:
            pv_source = str(pv.get("entity_id")) if pv else "unavailable"
            notes.append("saj_offnet_detected_but_pv1_pv2_unavailable")
    else:
        pv_source = str(pv.get("entity_id")) if pv else "unavailable"

    balance_w: float | None = None
    balanced: bool | None = None
    if notes and "saj_offnet_detected" in notes:
        # In offnet/microgrid mode the raw SAJ power flow no longer closes locally because
        # external power from the other inverter is folded into SAJ's aggregate PV channel.
        balance_w = None
        balanced = None
    elif pv_w is not None and grid_w is not None and battery_w is not None and load_w is not None:
        battery_discharge_w = max(battery_w, 0)
        battery_charge_w = max(-battery_w, 0)
        grid_import_w = max(grid_w, 0)
        grid_export_w = max(-grid_w, 0)
        balance_w = pv_w + battery_discharge_w + grid_import_w - load_w - battery_charge_w - grid_export_w
        balanced = abs(balance_w) <= BALANCE_TOLERANCE_W

    matched_entities = [item for item in (pv, pv1, pv2, grid, battery, load, soc, battery_energy, inverter, inverter_power, app_mode) if item]

    return {
        "system": system,
        "prefixes": list(prefixes),
        "updated_at": datetime.now(UTC).isoformat(),
        "entities": {
            "pv": _compact_entity_item(pv) if pv else None,
            "pv1": _compact_entity_item(pv1) if pv1 else None,
            "pv2": _compact_entity_item(pv2) if pv2 else None,
            "app_mode": _compact_entity_item(app_mode) if app_mode else None,
            "grid": _compact_entity_item(grid) if grid else None,
            "battery": _compact_entity_item(battery) if battery else None,
            "load": _compact_entity_item(load) if load else None,
            "soc": _compact_entity_item(soc) if soc else None,
            "battery_energy": _compact_entity_item(battery_energy) if battery_energy else None,
            "inverter": _compact_entity_item(inverter) if inverter else None,
            "inverter_power": _compact_entity_item(inverter_power) if inverter_power else None,
        },
        "metrics": {
            "pv_w": pv_w,
            "grid_w": grid_w,
            "battery_w": battery_w,
            "load_w": load_w,
            "pv_source": pv_source,
            "grid_source": str(grid.get("entity_id")) if grid else "unavailable",
            "battery_source": str(battery.get("entity_id")) if battery else "unavailable",
            "load_source": str(load.get("entity_id")) if load else "unavailable",
            "battery_soc_percent": soc_v,
            "battery_energy_kwh": battery_energy_kwh,
            "inverter_status": inverter_status,
            "inverter_power_w": inverter_power_w,
            "inverter_power_source": str(inverter_power.get("entity_id")) if inverter_power else "unavailable",
            "solar_active": bool(pv_w is not None and pv_w >= POWER_FLOW_ACTIVE_THRESHOLD_W),
            "grid_active": bool(grid_w is not None and abs(grid_w) >= POWER_FLOW_ACTIVE_THRESHOLD_W),
            "grid_import": bool(grid_w is not None and grid_w > 0),
            "battery_active": bool(battery_w is not None and abs(battery_w) >= POWER_FLOW_ACTIVE_THRESHOLD_W),
            "battery_discharging": bool(battery_w is not None and battery_w > 0),
            "load_active": bool(load_w is not None and load_w >= POWER_FLOW_ACTIVE_THRESHOLD_W),
            "balance_w": balance_w,
            "balanced": balanced,
            "matched_entities": len(matched_entities),
            "notes": notes,
        },
    }


def _solplanet_pseudo_entity(
    entity_id: str,
    state: object,
    unit: str | None,
    friendly_name: str,
    updated_at: str,
) -> dict[str, object]:
    return {
        "entity_id": entity_id,
        "domain": "sensor",
        "brand_guess": "solplanet",
        "state": state,
        "unit": unit,
        "friendly_name": friendly_name,
        "last_updated": updated_at,
    }


def _sum_array_product(a_values: object, b_values: object) -> float | None:
    if not isinstance(a_values, list) or not isinstance(b_values, list):
        return None
    if len(a_values) != len(b_values) or not a_values:
        return None
    total = 0.0
    valid_count = 0
    for a_raw, b_raw in zip(a_values, b_values, strict=False):
        a = _to_number(a_raw)
        b = _to_number(b_raw)
        if a is None or b is None:
            continue
        total += a * b
        valid_count += 1
    if valid_count == 0:
        return None
    return total


def _map_solplanet_status(status_code: object) -> str | None:
    code = int(status_code) if _to_number(status_code) is not None else None
    if code is None:
        return None
    # Known values from Solplanet integration mapping.
    if code == 1:
        return "running"
    if code == 0:
        return "standby"
    if code == 2:
        return "fault"
    if code == 4:
        return "checking"
    return str(code)


def _is_solplanet_meter_data_valid(meter_data: dict[str, object]) -> bool:
    flg = _to_number(meter_data.get("flg"))
    tim = str(meter_data.get("tim") or "").strip()
    return flg == 1 and bool(tim)


async def _save_solplanet_endpoint_snapshot(
    *,
    endpoint: str,
    path: str,
    ok: bool,
    error: str | None,
    payload: dict[str, object] | None,
    fetch_ms: float | None,
    requested_at_utc: str | None = None,
) -> None:
    round_id = str(request_round_ctx.get() or "").strip() or "manual"
    request_url = (
        f"{settings.solplanet_dongle_scheme}://"
        f"{settings.solplanet_dongle_host}:{settings.solplanet_dongle_port}/{path.lstrip('/')}"
    )
    try:
        await asyncio.to_thread(
            insert_raw_request_result,
            storage_db_path,
            round_id=round_id,
            system="solplanet",
            source="solplanet_cgi",
            endpoint=endpoint,
            requested_at_utc=requested_at_utc or datetime.now(UTC).isoformat(),
            method="GET",
            request_url=request_url,
            duration_ms=fetch_ms,
            ok=ok,
            status_code=None,
            error_text=error,
            response_text=json.dumps(payload, ensure_ascii=False) if payload is not None else None,
            response_json=payload,
        )
        if request_actor_ctx.get() == "worker":
            request_token = _worker_log_request_token(round_id, "solplanet_cgi", "GET", request_url)
            await asyncio.to_thread(
                update_worker_api_log if request_token else insert_worker_api_log,
                storage_db_path,
                **(
                    {
                        "request_token": request_token,
                        "ok": ok,
                        "status": "ok" if ok else "failed",
                        "status_code": None,
                        "duration_ms": fetch_ms,
                        "result_text": json.dumps(payload, ensure_ascii=False) if payload is not None else None,
                        "error_text": error,
                    }
                    if request_token
                    else {
                        "worker": str(request_actor_ctx.get() or "worker"),
                        "system": str(request_system_ctx.get() or "solplanet"),
                        "service": "solplanet_cgi",
                        "method": "GET",
                        "api_link": request_url,
                        "requested_at_utc": requested_at_utc or datetime.now(UTC).isoformat(),
                        "ok": ok,
                        "status_code": None,
                        "duration_ms": fetch_ms,
                        "result_text": json.dumps(payload, ensure_ascii=False) if payload is not None else None,
                        "error_text": error,
                    }
                ),
            )
    except Exception as exc:
        logger.warning("Failed to persist raw Solplanet result for %s (%s): %s", endpoint, path, exc)


def _extract_solplanet_context(inverter_info: dict[str, object]) -> dict[str, object]:
    inv_list = inverter_info.get("inv")
    inverter_item = inv_list[0] if isinstance(inv_list, list) and inv_list and isinstance(inv_list[0], dict) else {}
    inverter_sn = str(inverter_item.get("isn") or "")
    battery_sn: str | None = None
    battery_topo = inverter_item.get("battery_topo")
    if isinstance(battery_topo, list) and battery_topo and isinstance(battery_topo[0], dict):
        maybe_bat_sn = battery_topo[0].get("bat_sn")
        if maybe_bat_sn:
            battery_sn = str(maybe_bat_sn)
    return {
        "inverter_info": inverter_info,
        "inverter_item": inverter_item,
        "inverter_sn": inverter_sn or None,
        "battery_sn": battery_sn,
        "updated_at": datetime.now(UTC).isoformat(),
    }


async def _persist_solplanet_context(context: dict[str, object]) -> None:
    inverter_sn = str(context.get("inverter_sn") or "").strip()
    battery_sn = str(context.get("battery_sn") or "").strip()
    if not inverter_sn:
        return
    if inverter_sn == settings.solplanet_inverter_sn and battery_sn == settings.solplanet_battery_sn:
        return
    persisted = await asyncio.to_thread(
        save_settings,
        {
            "solplanet_inverter_sn": inverter_sn,
            "solplanet_battery_sn": battery_sn,
        },
    )
    await _replace_runtime(persisted)


def _get_solplanet_runtime_context() -> dict[str, object] | None:
    payload = solplanet_context_cache.get("payload")
    return payload if isinstance(payload, dict) else None


def _set_solplanet_runtime_context(payload: dict[str, object]) -> None:
    solplanet_context_cache["payload"] = payload
    solplanet_context_cache["at_monotonic"] = monotonic()


def _config_backed_solplanet_context() -> dict[str, object] | None:
    inverter_sn = str(settings.solplanet_inverter_sn or "").strip()
    battery_sn = str(settings.solplanet_battery_sn or "").strip()
    if not inverter_sn:
        return None
    return {
        "inverter_info": {},
        "inverter_item": {},
        "inverter_sn": inverter_sn,
        "battery_sn": battery_sn or None,
        "updated_at": datetime.now(UTC).isoformat(),
    }


async def _run_solplanet_endpoint_request(
    endpoint: str,
    path: str,
    build_coro: Callable[[], object],
    *,
    round_number: int | None = None,
) -> tuple[str, dict[str, object], str | None, str, float]:
    requested_at_utc = datetime.now(UTC).isoformat()
    fetch_started = monotonic()
    if round_number is not None:
        _mark_endpoint_attempt("solplanet", endpoint, round_number)
    try:
        result = await asyncio.wait_for(build_coro(), timeout=settings.solplanet_request_timeout_seconds)
        payload = result if isinstance(result, dict) else {}
        fetch_ms = round((monotonic() - fetch_started) * 1000, 1)
        if round_number is not None:
            _mark_endpoint_success("solplanet", endpoint, round_number)
        return endpoint, payload, None, path, fetch_ms
    except Exception as exc:  # noqa: BLE001
        error_text = f"{type(exc).__name__}: {exc}"
        fetch_ms = round((monotonic() - fetch_started) * 1000, 1)
        if round_number is not None:
            _mark_endpoint_failure("solplanet", endpoint, round_number, error_text)
        if not isinstance(exc, httpx.HTTPError):
            await _save_solplanet_endpoint_snapshot(
                endpoint=endpoint,
                path=path,
                ok=False,
                error=error_text,
                payload=None,
                fetch_ms=fetch_ms,
                requested_at_utc=requested_at_utc,
            )
        return endpoint, {}, error_text, path, fetch_ms


async def _run_solplanet_endpoint_batch(
    steps: list[tuple[str, str, Callable[[], object]]],
    *,
    round_number: int | None = None,
) -> dict[str, tuple[dict[str, object] | None, str | None, str, float]]:
    if not steps:
        return {}
    results = await asyncio.gather(
        *[
            _run_solplanet_endpoint_request(endpoint, path, build_coro, round_number=round_number)
            for endpoint, path, build_coro in steps
        ]
    )
    return {
        endpoint: (payload, error, path, fetch_ms)
        for endpoint, payload, error, path, fetch_ms in results
    }


def _build_solplanet_endpoint_steps(
    context: dict[str, object] | None,
    *,
    round_number: int | None = None,
) -> tuple[
    list[tuple[str, str, Callable[[], object]]],
    list[tuple[str, str, Callable[[], object]]],
    list[str],
]:
    inverter_sn = str((context or {}).get("inverter_sn") or "")
    battery_sn = str((context or {}).get("battery_sn") or "")

    def _include(endpoint: str) -> bool:
        return round_number is None or _endpoint_is_eligible("solplanet", endpoint, round_number)

    skipped: list[str] = []
    initial_steps: list[tuple[str, str, Callable[[], object]]] = []
    followup_steps: list[tuple[str, str, Callable[[], object]]] = []

    endpoint_defs: list[tuple[str, str, Callable[[], object]] | tuple[str, None, None]] = [
        ("getdevdata_device_3", "getdevdata.cgi?device=3", lambda: solplanet_client.get_meter_data()),
        (
            "getdevdata_device_2",
            f"getdevdata.cgi?device=2&sn={inverter_sn}" if inverter_sn else "",
            (lambda sn=inverter_sn: solplanet_client.get_inverter_data(sn)) if inverter_sn else None,
        ),
        (
            "getdevdata_device_4",
            f"getdevdata.cgi?device=4&sn={battery_sn}" if battery_sn else "",
            (lambda sn=battery_sn: solplanet_client.get_battery_data(sn)) if battery_sn else None,
        ),
    ]
    for endpoint, path, build_coro in endpoint_defs:
        if not _include(endpoint):
            skipped.append(endpoint)
            if round_number is not None:
                _mark_endpoint_skipped("solplanet", endpoint, round_number)
            continue
        if build_coro is None:
            continue
        initial_steps.append((endpoint, path, build_coro))

    if not inverter_sn and _include("getdevdata_device_2"):
        followup_steps.append(
            ("getdevdata_device_2", "", lambda: asyncio.sleep(0, result={}))
        )
    if not battery_sn and _include("getdevdata_device_4"):
        followup_steps.append(
            ("getdevdata_device_4", "", lambda: asyncio.sleep(0, result={}))
        )

    return initial_steps, followup_steps, skipped


def _build_solplanet_energy_flow_from_endpoint_map(
    endpoint_map: dict[str, tuple[dict[str, object] | None, str | None, str, float]],
    *,
    started_at: float,
    skipped_endpoints: list[str] | None = None,
) -> dict[str, object]:
    meter_data = endpoint_map.get("getdevdata_device_3", ({}, None, "", 0.0))[0] or {}
    battery_data = endpoint_map.get("getdevdata_device_4", ({}, None, "", 0.0))[0] or {}
    inverter_data = endpoint_map.get("getdevdata_device_2", ({}, None, "", 0.0))[0] or {}
    endpoint_errors = [
        f"solplanet_endpoint_error:{name}:{error}"
        for name, (_, error, _, _) in endpoint_map.items()
        if isinstance(error, str) and error
    ]
    if skipped_endpoints:
        endpoint_errors.extend(f"solplanet_endpoint_skipped:{name}" for name in skipped_endpoints)

    updated_at = datetime.now(UTC).isoformat()
    inverter_status = _map_solplanet_status(inverter_data.get("stu"))

    pv_w = _to_number(inverter_data.get("ppv"))
    pv_source = "getdevdata_device_2.ppv"
    if pv_w is None:
        pv_w = _sum_array_product(inverter_data.get("vpv"), inverter_data.get("ipv"))
        pv_source = "calc:getdevdata_device_2.vpv * getdevdata_device_2.ipv"
    if pv_w is None:
        pv_w = _to_number(battery_data.get("ppv"))
        pv_source = "getdevdata_device_4.ppv"
    if pv_w is None:
        pv_source = "unavailable"

    battery_w = _to_number(battery_data.get("pb"))
    inverter_pac_w = _to_number(inverter_data.get("pac"))
    soc = _to_number(battery_data.get("soc"))

    meter_data_valid = _is_solplanet_meter_data_valid(meter_data)
    grid_w: float | None = None
    grid_source = "unavailable"
    if meter_data_valid:
        grid_w = _to_number(meter_data.get("pac"))
        grid_source = "getdevdata_device_3.pac" if grid_w is not None else "unavailable"

    # Solplanet inverter pac sign assumption: positive means inverter AC output to loads/grid,
    # negative means inverter is drawing AC (for example grid-charging the battery).
    load_w: float | None = None
    load_source = "unavailable"
    if inverter_pac_w is not None and grid_w is not None:
        derived_load_w = inverter_pac_w + grid_w
        # Clamp tiny negative residuals caused by firmware timing skew/noise.
        if derived_load_w < 0 and abs(derived_load_w) <= BALANCE_TOLERANCE_W:
            derived_load_w = 0.0
        load_w = max(derived_load_w, 0.0)
        load_source = "calc:getdevdata_device_2.pac + getdevdata_device_3.pac"
    elif inverter_pac_w is not None and battery_w is not None and inverter_pac_w < -5 and battery_w < -5:
        # Grid-charging scene: inverter AC draw = battery charge + home load.
        load_w = max(abs(inverter_pac_w) - abs(battery_w), 0.0)
        load_source = "calc:abs(getdevdata_device_2.pac) - abs(getdevdata_device_4.pb[charge])"
    elif inverter_pac_w is not None:
        # Meter offline fallback: treat inverter AC output as an upper-bound estimate for load.
        # This overestimates load when the inverter is also exporting to the grid, but is
        # more accurate than using battery DC values which include AC conversion losses.
        load_w = abs(inverter_pac_w)
        load_source = "calc:abs(getdevdata_device_2.pac)[meter_offline_estimate]"

    if grid_w is None and load_w is not None and pv_w is not None and battery_w is not None:
        # When meter is unavailable, infer grid net power from split load and known PV/battery.
        battery_discharge_w = max(battery_w, 0)
        battery_charge_w = max(-battery_w, 0)
        grid_w = load_w + battery_charge_w - pv_w - battery_discharge_w
        grid_source = "calc:load + battery_charge - pv - battery_discharge"

    balance_w: float | None = None
    balanced: bool | None = None
    if pv_w is not None and grid_w is not None and battery_w is not None and load_w is not None:
        battery_discharge_w = max(battery_w, 0)
        battery_charge_w = max(-battery_w, 0)
        grid_import_w = max(grid_w, 0)
        grid_export_w = max(-grid_w, 0)
        balance_w = pv_w + battery_discharge_w + grid_import_w - load_w - battery_charge_w - grid_export_w
        balanced = abs(balance_w) <= BALANCE_TOLERANCE_W

    entities: dict[str, dict[str, object] | None] = {
        "pv": _solplanet_pseudo_entity(
            "cgi.solplanet_pv_power",
            pv_w,
            "W",
            "Solplanet PV Power (CGI)",
            updated_at,
        )
        if pv_w is not None
        else None,
        "grid": _solplanet_pseudo_entity(
            "cgi.solplanet_grid_power",
            grid_w,
            "W",
            "Solplanet Grid Power (CGI)",
            updated_at,
        )
        if grid_w is not None
        else None,
        "battery": _solplanet_pseudo_entity(
            "cgi.solplanet_battery_power",
            battery_w,
            "W",
            "Solplanet Battery Power (CGI)",
            updated_at,
        )
        if battery_w is not None
        else None,
        "load": _solplanet_pseudo_entity(
            "cgi.solplanet_load_power",
            load_w,
            "W",
            "Solplanet Home Load Power (CGI, Derived)",
            updated_at,
        )
        if load_w is not None
        else None,
        "soc": _solplanet_pseudo_entity(
            "cgi.solplanet_battery_soc",
            soc,
            "%",
            "Solplanet Battery SOC (CGI)",
            updated_at,
        )
        if soc is not None
        else None,
        "inverter": _solplanet_pseudo_entity(
            "cgi.solplanet_inverter_status",
            inverter_status,
            None,
            "Solplanet Inverter Status (CGI)",
            updated_at,
        )
        if inverter_status is not None
        else None,
    }
    matched_entities = sum(1 for item in entities.values() if item is not None)

    return {
        "system": "solplanet",
        "source": {
            "type": "solplanet_cgi",
            "host": settings.solplanet_dongle_host,
            "port": settings.solplanet_dongle_port,
            "scheme": settings.solplanet_dongle_scheme,
        },
        "prefixes": ["solplanet", "soulplanet"],
        "updated_at": updated_at,
        "entities": entities,
        "raw": {
            "inverter_data": inverter_data,
            "meter_data": meter_data,
            "battery_data": battery_data,
        },
        "metrics": {
            "pv_w": pv_w,
            "grid_w": grid_w,
            "battery_w": battery_w,
            "load_w": load_w,
            "pv_source": pv_source,
            "grid_source": grid_source,
            "battery_source": "getdevdata_device_4.pb" if battery_w is not None else "unavailable",
            "load_source": load_source,
            "battery_soc_percent": soc,
            "inverter_status": inverter_status,
            "inverter_power_w": inverter_pac_w,
            "inverter_power_source": "getdevdata_device_2.pac" if inverter_pac_w is not None else "unavailable",
            "solar_active": bool(pv_w is not None and pv_w >= POWER_FLOW_ACTIVE_THRESHOLD_W),
            "grid_active": bool(grid_w is not None and abs(grid_w) >= POWER_FLOW_ACTIVE_THRESHOLD_W),
            "grid_import": bool(grid_w is not None and grid_w > 0),
            "battery_active": bool(battery_w is not None and abs(battery_w) >= POWER_FLOW_ACTIVE_THRESHOLD_W),
            "battery_discharging": bool(battery_w is not None and battery_w > 0),
            "load_active": bool(load_w is not None and load_w >= POWER_FLOW_ACTIVE_THRESHOLD_W),
            "balance_w": balance_w,
            "balanced": balanced,
            "matched_entities": matched_entities,
            "notes": [
                "solplanet_pv_w_prefer_ppv",
                f"solplanet_load_w_source:{load_source}",
                "solplanet_grid_w_uses_meter_data_pac_only",
                f"solplanet_grid_w_source:{grid_source}",
                f"solplanet_meter_data_valid:{str(meter_data_valid).lower()}",
                "solplanet_battery_w_uses_battery_pb",
                *endpoint_errors,
            ],
            "fetch_ms": round((monotonic() - started_at) * 1000, 1),
        },
    }


async def _build_solplanet_energy_flow_payload_from_cgi(
    *,
    round_number: int | None = None,
) -> dict[str, object]:
    if not solplanet_client.configured:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Solplanet CGI client is not configured",
                "required_env": "SOLPLANET_DONGLE_HOST",
            },
        )

    started_at = monotonic()
    runtime_context = _get_solplanet_runtime_context() or _config_backed_solplanet_context()
    if runtime_context is not None and _get_solplanet_runtime_context() is None:
        _set_solplanet_runtime_context(runtime_context)
    initial_steps, followup_placeholders, skipped = _build_solplanet_endpoint_steps(
        runtime_context,
        round_number=round_number,
    )
    endpoint_map = await _run_solplanet_endpoint_batch(initial_steps, round_number=round_number)

    if followup_placeholders:
        for endpoint, _, _ in followup_placeholders:
            if endpoint in endpoint_map:
                continue
            path = {
                "getdevdata_device_2": "getdevdata.cgi?device=2",
                "getdevdata_device_4": "getdevdata.cgi?device=4",
            }.get(endpoint, endpoint)
            error_text = "ValueError: Missing runtime context"
            endpoint_map[endpoint] = ({}, error_text, path, 0.0)
            if round_number is not None:
                _mark_endpoint_failure("solplanet", endpoint, round_number, error_text)
            await _save_solplanet_endpoint_snapshot(
                endpoint=endpoint,
                path=path,
                ok=False,
                error=error_text,
                payload=None,
                fetch_ms=0.0,
            )

    return _build_solplanet_energy_flow_from_endpoint_map(
        endpoint_map,
        started_at=started_at,
        skipped_endpoints=skipped,
    )


def _combined_pseudo_entity(
    entity_id: str,
    state: object,
    unit: str | None,
    friendly_name: str,
    updated_at: str,
) -> dict[str, object]:
    return {
        "entity_id": entity_id,
        "domain": "sensor",
        "brand_guess": "combined",
        "state": state,
        "unit": unit,
        "friendly_name": friendly_name,
        "last_updated": updated_at,
    }


def _build_combined_flow_payload(
    saj_flow: dict[str, object],
    solplanet_flow: dict[str, object],
    tesla_observation_result: dict[str, object] | None = None,
) -> dict[str, object]:
    saj_metrics = saj_flow.get("metrics")
    saj_metrics_map = saj_metrics if isinstance(saj_metrics, dict) else {}
    solplanet_metrics = solplanet_flow.get("metrics")
    solplanet_metrics_map = solplanet_metrics if isinstance(solplanet_metrics, dict) else {}
    saj_notes = saj_metrics_map.get("notes")
    saj_notes_list = saj_notes if isinstance(saj_notes, list) else []

    solar_primary_w = _to_number(saj_metrics_map.get("pv_w"))
    solar_secondary_w = _to_number(solplanet_metrics_map.get("pv_w"))
    grid_w = _to_number(saj_metrics_map.get("grid_w"))
    battery1_w = _to_number(saj_metrics_map.get("battery_w"))
    battery2_w = _to_number(solplanet_metrics_map.get("battery_w"))
    inverter1_w = _to_number(saj_metrics_map.get("inverter_power_w"))
    inverter2_w = _to_number(solplanet_metrics_map.get("inverter_power_w"))
    battery1_soc = _to_number(saj_metrics_map.get("battery_soc_percent"))
    battery2_soc = _to_number(solplanet_metrics_map.get("battery_soc_percent"))
    battery2_energy_kwh = _to_number(solplanet_metrics_map.get("battery_energy_kwh"))
    inverter1_status = str(saj_metrics_map.get("inverter_status")) if saj_metrics_map.get("inverter_status") is not None else None
    inverter2_status = (
        str(solplanet_metrics_map.get("inverter_status"))
        if solplanet_metrics_map.get("inverter_status") is not None
        else None
    )
    tesla_observation = (
        tesla_observation_result.get("observation")
        if isinstance(tesla_observation_result, dict)
        else None
    )
    tesla_observation_map = tesla_observation if isinstance(tesla_observation, dict) else {}
    tesla_charging = tesla_observation_map.get("charging")
    tesla_charging_map = tesla_charging if isinstance(tesla_charging, dict) else {}
    tesla_battery = tesla_observation_map.get("battery")
    tesla_battery_map = tesla_battery if isinstance(tesla_battery, dict) else {}
    tesla_charge_power_w = _to_number(tesla_charging_map.get("power_w"))
    tesla_current_amps = _to_number(tesla_charging_map.get("current_amps"))
    tesla_configured_current_amps = _to_number(tesla_charging_map.get("configured_current_amps"))
    tesla_soc_percent = _to_number(tesla_battery_map.get("level_percent"))
    tesla_connection_state = str(tesla_charging_map.get("connection_state") or "").strip() or None
    tesla_requested_enabled = tesla_charging_map.get("requested_enabled")
    tesla_cable_connected = tesla_charging_map.get("cable_connected")
    saj_pv_estimate_w: float | None = None
    if inverter1_w is not None and battery1_w is not None:
        saj_pv_estimate_w = max(inverter1_w - battery1_w, 0.0)
        if saj_pv_estimate_w <= BALANCE_TOLERANCE_W:
            saj_pv_estimate_w = 0.0
    solplanet_battery_discharging = bool(battery2_w is not None and battery2_w >= POWER_FLOW_ACTIVE_THRESHOLD_W)
    saj_pv_suspect = bool(
        saj_pv_estimate_w is not None
        and solar_primary_w is not None
        and solar_primary_w > saj_pv_estimate_w + BALANCE_TOLERANCE_W
    )
    use_saj_pv_estimate = bool(
        saj_pv_estimate_w is not None
        and (
            solar_primary_w is None
            or (
                solplanet_battery_discharging
                and (
                    saj_pv_suspect
                    or "saj_offnet_detected" in saj_notes_list
                )
            )
        )
    )
    if use_saj_pv_estimate:
        solar_primary_w = saj_pv_estimate_w

    total_load_w: float | None = None
    if inverter1_w is not None and inverter2_w is not None and grid_w is not None:
        total_load_w = inverter1_w + inverter2_w + grid_w
        if abs(total_load_w) <= BALANCE_TOLERANCE_W:
            total_load_w = 0.0

    total_solar_w: float | None = None
    if solar_primary_w is not None and solar_secondary_w is not None:
        total_solar_w = solar_primary_w + solar_secondary_w

    total_battery_w: float | None = None
    if battery1_w is not None and battery2_w is not None:
        total_battery_w = battery1_w + battery2_w

    total_inverter_w: float | None = None
    if inverter1_w is not None and inverter2_w is not None:
        total_inverter_w = inverter1_w + inverter2_w

    _source_times = [t for t in [saj_flow.get("updated_at"), solplanet_flow.get("updated_at")] if t]
    updated_at = min(_source_times) if _source_times else datetime.now(UTC).isoformat()
    metrics = {
        "pv_w": total_solar_w,
        "solar_primary_w": solar_primary_w,
        "solar_secondary_w": solar_secondary_w,
        "grid_w": grid_w,
        "battery_w": total_battery_w,
        "battery1_w": battery1_w,
        "battery2_w": battery2_w,
        "load_w": total_load_w,
        "battery_soc_percent": None,
        "battery1_soc_percent": battery1_soc,
        "battery2_soc_percent": battery2_soc,
        "battery2_energy_kwh": battery2_energy_kwh,
        "tesla_charge_power_w": tesla_charge_power_w,
        "tesla_charge_current_amps": tesla_current_amps,
        "tesla_configured_current_amps": tesla_configured_current_amps,
        "tesla_battery_soc_percent": tesla_soc_percent,
        "tesla_connection_state": tesla_connection_state,
        "tesla_charge_requested_enabled": tesla_requested_enabled,
        "tesla_cable_connected": tesla_cable_connected,
        "inverter_status": "combined",
        "inverter1_status": inverter1_status,
        "inverter2_status": inverter2_status,
        "inverter_power_w": total_inverter_w,
        "inverter1_w": inverter1_w,
        "inverter2_w": inverter2_w,
        "pv_source": (
            "calc:(saj.inverter_power_w - saj.battery_w) + solplanet.pv_w"
            if use_saj_pv_estimate
            else "calc:saj.pv_w + solplanet.pv_w"
        ),
        "grid_source": str(saj_metrics_map.get("grid_source") or "unavailable"),
        "battery_source": "calc:saj.battery_w + solplanet.battery_w",
        "battery1_source": str(saj_metrics_map.get("battery_source") or "unavailable"),
        "battery2_source": str(solplanet_metrics_map.get("battery_source") or "unavailable"),
        "load_source": "calc:saj.inverter_power_w + solplanet.inverter_power_w + saj.grid_w",
        "inverter_power_source": "calc:saj.inverter_power_w + solplanet.inverter_power_w",
        "inverter1_power_source": str(saj_metrics_map.get("inverter_power_source") or "unavailable"),
        "inverter2_power_source": str(solplanet_metrics_map.get("inverter_power_source") or "unavailable"),
        "solar_active": bool(total_solar_w is not None and total_solar_w >= POWER_FLOW_ACTIVE_THRESHOLD_W),
        "grid_active": bool(grid_w is not None and abs(grid_w) >= POWER_FLOW_ACTIVE_THRESHOLD_W),
        "grid_import": bool(grid_w is not None and grid_w > 0),
        "battery_active": bool(total_battery_w is not None and abs(total_battery_w) >= POWER_FLOW_ACTIVE_THRESHOLD_W),
        "battery_discharging": bool(total_battery_w is not None and total_battery_w > 0),
        "load_active": bool(total_load_w is not None and total_load_w >= POWER_FLOW_ACTIVE_THRESHOLD_W),
        "balance_w": None,
        "balanced": None,
        "matched_entities": 12,
        "notes": [
            "combined_flow_requires_full_round_success",
            "combined_grid_uses_saj_grid_w",
            (
                "combined_solar_uses_saj_inverter_minus_battery_when_solplanet_discharge_pollutes_saj_pv"
                if use_saj_pv_estimate
                else "combined_solar_uses_saj_pv_plus_solplanet_pv"
            ),
            "combined_total_load_uses_saj_inverter_power + solplanet_inverter_power + saj_grid",
            "combined_tesla_observation_embedded",
        ],
    }
    return {
        "system": "combined",
        "updated_at": updated_at,
        "prefixes": ["combined"],
        "display": _build_combined_display(metrics),
        "entities": {
            "solar_primary": _combined_pseudo_entity(
                "combined.solar_primary_power",
                solar_primary_w,
                "W",
                "Combined Solar Primary Power",
                updated_at,
            ),
            "solar_secondary": _combined_pseudo_entity(
                "combined.solar_secondary_power",
                solar_secondary_w,
                "W",
                "Combined Solar Secondary Power",
                updated_at,
            ),
            "grid": _combined_pseudo_entity("combined.grid_power", grid_w, "W", "Combined Grid Power", updated_at),
            "battery1": _combined_pseudo_entity(
                "combined.battery1_power",
                battery1_w,
                "W",
                "Combined Battery 1 Power",
                updated_at,
            ),
            "battery2": _combined_pseudo_entity(
                "combined.battery2_power",
                battery2_w,
                "W",
                "Combined Battery 2 Power",
                updated_at,
            ),
            "load": _combined_pseudo_entity("combined.total_load_power", total_load_w, "W", "Combined Total Load", updated_at),
            "tesla_charge_power": _combined_pseudo_entity(
                "combined.tesla_charge_power",
                tesla_charge_power_w,
                "W",
                "Combined Tesla Charge Power",
                updated_at,
            ),
            "tesla_charge_current": _combined_pseudo_entity(
                "combined.tesla_charge_current",
                tesla_current_amps,
                "A",
                "Combined Tesla Charge Current",
                updated_at,
            ),
            "tesla_battery_soc": _combined_pseudo_entity(
                "combined.tesla_battery_soc",
                tesla_soc_percent,
                "%",
                "Combined Tesla Battery SOC",
                updated_at,
            ),
            "inverter1": _combined_pseudo_entity(
                "combined.inverter1_power",
                inverter1_w,
                "W",
                "Combined Inverter 1 Power",
                updated_at,
            ),
            "inverter2": _combined_pseudo_entity(
                "combined.inverter2_power",
                inverter2_w,
                "W",
                "Combined Inverter 2 Power",
                updated_at,
            ),
        },
        "source": {
            "type": "combined_worker",
            "components": {
                "saj_updated_at": saj_flow.get("updated_at"),
                "solplanet_updated_at": solplanet_flow.get("updated_at"),
                "tesla_updated_at": tesla_observation_result.get("executed_at_utc") if isinstance(tesla_observation_result, dict) else None,
            },
        },
        "raw": {
            "saj": saj_flow,
            "solplanet": solplanet_flow,
            "tesla": tesla_observation_result,
        },
        "metrics": metrics,
    }


def _get_cached_solplanet_flow() -> dict[str, object] | None:
    payload = solplanet_flow_cache.get("payload")
    at_monotonic = float(solplanet_flow_cache.get("at_monotonic") or 0.0)
    if payload is None:
        return None
    ttl = settings.solplanet_cache_seconds
    if ttl <= 0:
        return None
    if monotonic() - at_monotonic > ttl:
        return None
    return payload if isinstance(payload, dict) else None


async def _get_solplanet_flow_cached() -> dict[str, object]:
    cached = _get_cached_solplanet_flow()
    if cached is not None:
        return cached
    async with solplanet_flow_lock:
        cached = _get_cached_solplanet_flow()
        if cached is not None:
            return cached
        try:
            fresh = await _build_solplanet_energy_flow_payload_from_cgi()
            solplanet_flow_cache["payload"] = fresh
            solplanet_flow_cache["at_monotonic"] = monotonic()
            return fresh
        except (httpx.HTTPError, TimeoutError, asyncio.TimeoutError):
            stale = solplanet_flow_cache.get("payload")
            if isinstance(stale, dict):
                fallback = dict(stale)
                fallback["stale"] = True
                fallback["stale_reason"] = "solplanet_cgi_timeout_or_error"
                return fallback
            raise


async def _get_solplanet_cgi_dump() -> dict[str, object]:
    if not solplanet_client.configured:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Solplanet CGI client is not configured",
                "required_env": "SOLPLANET_DONGLE_HOST",
            },
        )

    started_at = monotonic()
    context = await _get_solplanet_context_cached()
    inverter_sn = str(context.get("inverter_sn") or "")
    battery_sn = str(context.get("battery_sn") or "")

    async def _safe_task(name: str, coro: object) -> tuple[str, dict[str, object] | None, str | None]:
        try:
            payload = await asyncio.wait_for(coro, timeout=settings.solplanet_request_timeout_seconds)
            return name, payload if isinstance(payload, dict) else {}, None
        except Exception as exc:  # noqa: BLE001
            return name, None, f"{type(exc).__name__}: {exc}"

    tasks = [
        _safe_task("getdevdata_device_2", solplanet_client.get_inverter_data(inverter_sn))
        if inverter_sn
        else _safe_task("getdevdata_device_2", asyncio.sleep(0, result={})),
        _safe_task("getdevdata_device_3", solplanet_client.get_meter_data()),
    ]
    if battery_sn:
        tasks.append(_safe_task("getdevdata_device_4", solplanet_client.get_battery_data(battery_sn)))

    task_results = await asyncio.gather(*tasks)
    endpoints: dict[str, object] = {}
    for name, payload, error in task_results:
        endpoint_path = {
            "getdevdata_device_2": f"getdevdata.cgi?device=2&sn={inverter_sn}" if inverter_sn else "getdevdata.cgi?device=2",
            "getdevdata_device_3": "getdevdata.cgi?device=3",
            "getdevdata_device_4": f"getdevdata.cgi?device=4&sn={battery_sn}" if battery_sn else "getdevdata.cgi?device=4",
        }.get(name, name)
        endpoints[name] = {
            "path": endpoint_path,
            "ok": error is None,
            "error": error,
            "payload": payload,
        }

    return {
        "system": "solplanet",
        "source": {
            "type": "solplanet_cgi",
            "host": settings.solplanet_dongle_host,
            "port": settings.solplanet_dongle_port,
            "scheme": settings.solplanet_dongle_scheme,
        },
        "updated_at": datetime.now(UTC).isoformat(),
        "fetch_ms": round((monotonic() - started_at) * 1000, 1),
        "context": {
            "inverter_sn": inverter_sn or None,
            "battery_sn": battery_sn,
        },
        "endpoints": endpoints,
    }


def _get_cached_solplanet_context() -> dict[str, object] | None:
    payload = solplanet_context_cache.get("payload")
    at_monotonic = float(solplanet_context_cache.get("at_monotonic") or 0.0)
    if not isinstance(payload, dict):
        return None
    ttl = max(2.0, settings.solplanet_cache_seconds)
    if monotonic() - at_monotonic > ttl:
        return None
    return payload


async def _get_solplanet_context_cached() -> dict[str, object]:
    cached = _get_cached_solplanet_context()
    if cached is not None:
        return cached
    config_context = _config_backed_solplanet_context()
    if config_context is not None:
        _set_solplanet_runtime_context(config_context)
        return config_context
    async with solplanet_context_lock:
        cached = _get_cached_solplanet_context()
        if cached is not None:
            return cached
        config_context = _config_backed_solplanet_context()
        if config_context is not None:
            _set_solplanet_runtime_context(config_context)
            return config_context

        inverter_info = await asyncio.wait_for(
            solplanet_client.get_inverter_info(),
            timeout=settings.solplanet_request_timeout_seconds,
        )
        inv_list = inverter_info.get("inv")
        inverter_item = inv_list[0] if isinstance(inv_list, list) and inv_list else {}
        inverter_sn = str(inverter_item.get("isn") or "")
        battery_sn: str | None = None
        battery_topo = inverter_item.get("battery_topo")
        if isinstance(battery_topo, list) and battery_topo and isinstance(battery_topo[0], dict):
            maybe_bat_sn = battery_topo[0].get("bat_sn")
            if maybe_bat_sn:
                battery_sn = str(maybe_bat_sn)

        payload = {
            "inverter_info": inverter_info,
            "inverter_item": inverter_item,
            "inverter_sn": inverter_sn or None,
            "battery_sn": battery_sn,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        _set_solplanet_runtime_context(payload)
        await _persist_solplanet_context(payload)
        return payload


def _build_solplanet_single_endpoint_response(
    *,
    name: str,
    path: str,
    payload: dict[str, object] | None,
    error: str | None,
    started_at: float,
) -> dict[str, object]:
    return {
        "system": "solplanet",
        "endpoint": name,
        "path": path,
        "ok": error is None,
        "error": error,
        "payload": payload,
        "fetch_ms": round((monotonic() - started_at) * 1000, 1),
        "updated_at": datetime.now(UTC).isoformat(),
        "source": {
            "type": "solplanet_cgi",
            "host": settings.solplanet_dongle_host,
            "port": settings.solplanet_dongle_port,
            "scheme": settings.solplanet_dongle_scheme,
        },
    }


def _parse_iso_utc(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


async def _solplanet_endpoint_response_from_snapshot(
    *,
    name: str,
    path: str,
    started_at: float,
) -> dict[str, object]:
    snapshot = await asyncio.to_thread(
        get_latest_raw_request_result,
        storage_db_path,
        source="solplanet_cgi",
        endpoint=name,
    )
    if not snapshot:
        response = _build_solplanet_single_endpoint_response(
            name=name,
            path=path,
            payload=None,
            error="No snapshot stored yet",
            started_at=started_at,
        )
        response["status"] = "missing"
        response["last_requested_at"] = None
        response["last_success_at"] = None
        response["source"] = {"type": "raw_request_results"}
        return response

    snap_ok = bool(snapshot.get("ok"))
    snap_error = str(snapshot.get("error") or "Last request failed") if not snap_ok else None
    snap_payload = snapshot.get("payload") if isinstance(snapshot.get("payload"), dict) else None
    snap_path = str(snapshot.get("path") or path)
    response = _build_solplanet_single_endpoint_response(
        name=name,
        path=snap_path,
        payload=snap_payload,
        error=snap_error,
        started_at=started_at,
    )
    requested_at = snapshot.get("requested_at_utc")
    if isinstance(requested_at, str) and requested_at:
        response["updated_at"] = requested_at
        response["last_requested_at"] = requested_at
    else:
        response["last_requested_at"] = None
    response["last_success_at"] = None
    response["status"] = "success" if snap_ok else "failed"
    response["ok"] = snap_ok
    response["error"] = snap_error
    if snapshot.get("fetch_ms") is not None:
        response["fetch_ms"] = snapshot.get("fetch_ms")
    last_success_dt = None
    last_requested_dt = _parse_iso_utc(snapshot.get("requested_at_utc"))
    collector_solplanet = dict(collector_status.get("solplanet") or {})
    collector_last_success_dt = _parse_iso_utc(collector_solplanet.get("last_success_at"))
    collector_last_error_dt = _parse_iso_utc(collector_solplanet.get("last_error_at"))
    stale_after_seconds = max(45.0, settings.solplanet_request_timeout_seconds * 3.0)
    stale = False
    stale_reason: str | None = None
    if last_requested_dt is not None:
        age_seconds = max((datetime.now(UTC) - last_requested_dt).total_seconds(), 0.0)
        response["age_seconds"] = round(age_seconds, 1)
        if age_seconds > stale_after_seconds:
            stale = True
            stale_reason = "snapshot_too_old"
    if (
        collector_last_error_dt is not None
        and (collector_last_success_dt is None or collector_last_error_dt > collector_last_success_dt)
        and (last_success_dt is None or collector_last_error_dt >= last_success_dt)
    ):
        stale = True
        stale_reason = stale_reason or "collector_currently_failing"
    if stale:
        response["stale"] = True
        response["stale_reason"] = stale_reason
        response["status"] = "stale"
    response["source"] = {
        "type": "raw_request_results",
        "host": settings.solplanet_dongle_host,
        "port": settings.solplanet_dongle_port,
        "scheme": settings.solplanet_dongle_scheme,
    }
    return response


def _build_saj_single_endpoint_response(
    name: str,
    path: str,
    payload: dict[str, object] | None,
    error: str | None,
    started_at: float,
) -> dict[str, object]:
    return {
        "system": "saj",
        "endpoint": name,
        "path": path,
        "ok": error is None,
        "error": error,
        "payload": payload,
        "fetch_ms": round((monotonic() - started_at) * 1000, 1),
        "updated_at": datetime.now(UTC).isoformat(),
        "source": {
            "type": "home_assistant",
            "ha_url": settings.ha_url,
        },
    }


def _compact_raw_ha_state(state: dict[str, object] | None) -> dict[str, object] | None:
    if not isinstance(state, dict):
        return None
    attributes = state.get("attributes")
    attrs = attributes if isinstance(attributes, dict) else {}
    return {
        "entity_id": state.get("entity_id"),
        "state": state.get("state"),
        "unit": attrs.get("unit_of_measurement"),
        "friendly_name": attrs.get("friendly_name"),
        "device_class": attrs.get("device_class"),
        "state_class": attrs.get("state_class"),
        "last_changed": state.get("last_changed"),
        "last_updated": state.get("last_updated"),
    }


def _reset_solplanet_caches() -> None:
    solplanet_flow_cache["payload"] = None
    solplanet_flow_cache["at_monotonic"] = 0.0
    solplanet_context_cache["payload"] = None
    solplanet_context_cache["at_monotonic"] = 0.0


def _missing_required_config() -> list[str]:
    return _missing_config_fields(settings_to_dict(settings))


def _ensure_ha_configured() -> None:
    missing = _missing_required_config()
    if missing:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Service is not configured yet",
                "missing_required": missing,
                "configure_path": "/api/config",
            },
        )


def _ensure_solplanet_configured() -> None:
    if not solplanet_client.configured:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Solplanet CGI client is not configured",
                "required_env": "SOLPLANET_DONGLE_HOST",
            },
        )


def _validate_saj_slot(slot: int) -> int:
    if slot < SAJ_SLOT_MIN or slot > SAJ_SLOT_MAX:
        raise HTTPException(
            status_code=400,
            detail=f"slot must be between {SAJ_SLOT_MIN} and {SAJ_SLOT_MAX}",
        )
    return slot


def _validate_solplanet_slot(slot: int) -> int:
    if slot < SOLPLANET_SLOT_MIN or slot > SOLPLANET_SLOT_MAX:
        raise HTTPException(
            status_code=400,
            detail=f"slot must be between {SOLPLANET_SLOT_MIN} and {SOLPLANET_SLOT_MAX}",
        )
    return slot


def _normalize_solplanet_day_key(raw_day: str) -> str:
    day = SOLPLANET_DAY_ALIAS.get(raw_day.strip().lower(), "")
    if not day:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "day must be one of sun,mon,tue,wed,thu,fri,sat",
                "accepted": list(SOLPLANET_DAY_KEYS),
            },
        )
    return day


def _validate_hhmm(value: str, field_name: str) -> str:
    normalized = value.strip()
    if not SAJ_TIME_REGEX.fullmatch(normalized):
        raise HTTPException(status_code=400, detail=f"{field_name} must match HH:MM (24-hour)")
    return normalized


def _entity_number_value(
    states_by_id: dict[str, dict[str, object]],
    entity_id: str,
) -> int | float | None:
    state = states_by_id.get(entity_id)
    if not state:
        return None
    value = _to_number(state.get("state"))
    if value is None:
        return None
    if float(value).is_integer():
        return int(value)
    return value


def _entity_text_value(
    states_by_id: dict[str, dict[str, object]],
    entity_id: str,
) -> str | None:
    state = states_by_id.get(entity_id)
    if not state:
        return None
    raw = state.get("state")
    if raw is None:
        return None
    return str(raw)


def _entity_switch_value(
    states_by_id: dict[str, dict[str, object]],
    entity_id: str,
) -> bool | None:
    value = _entity_text_value(states_by_id, entity_id)
    if value is None:
        return None
    if value.lower() == "on":
        return True
    if value.lower() == "off":
        return False
    return None


def _entity_metadata(
    states_by_id: dict[str, dict[str, object]],
    entity_id: str,
) -> dict[str, object]:
    state = states_by_id.get(entity_id)
    attrs = state.get("attributes") if isinstance(state, dict) else {}
    attrs_map = attrs if isinstance(attrs, dict) else {}
    return {
        "entity_id": entity_id,
        "friendly_name": attrs_map.get("friendly_name"),
        "unit": attrs_map.get("unit_of_measurement"),
        "min": attrs_map.get("min"),
        "max": attrs_map.get("max"),
        "step": attrs_map.get("step"),
        "pattern": attrs_map.get("pattern"),
    }


async def _saj_control_states() -> tuple[list[dict[str, object]], dict[str, dict[str, object]]]:
    states = await ha_client.all_states()
    states_by_id = {str(item.get("entity_id", "")): item for item in states}
    return states, states_by_id


def _build_saj_control_state(
    states_by_id: dict[str, dict[str, object]],
) -> dict[str, object]:
    charge_power_limit = _entity_number_value(states_by_id, "number.saj_battery_charge_power_limit_input")
    discharge_power_limit = _entity_number_value(states_by_id, "number.saj_battery_discharge_power_limit_input")

    def _slot_power_w_estimate(power_percent: object) -> int | None:
        percent = _to_number(power_percent)
        if percent is None:
            return None
        return int(round(SAJ_RATED_POWER_W * percent / 100.0))

    def _sensor_slot_value(slot_kind: Literal["charge", "discharge"], slot: int, field: str) -> str | int | float | None:
        suffix = "" if slot == 1 else f"_{slot}"
        entity_id = f"sensor.saj_{slot_kind}{suffix}_{field}"
        if field in {"power_percent", "day_mask"}:
            return _entity_number_value(states_by_id, entity_id)
        return _entity_text_value(states_by_id, entity_id)

    charge_slots: list[dict[str, object]] = []
    discharge_slots: list[dict[str, object]] = []
    charge_effective_slots: list[dict[str, object]] = []
    discharge_effective_slots: list[dict[str, object]] = []
    for slot in range(SAJ_SLOT_MIN, SAJ_SLOT_MAX + 1):
        charge_slots.append(
            {
                "slot": slot,
                "start_time": _entity_text_value(states_by_id, f"text.saj_charge{slot}_start_time_time"),
                "end_time": _entity_text_value(states_by_id, f"text.saj_charge{slot}_end_time_time"),
                "power_percent": _entity_number_value(states_by_id, f"number.saj_charge{slot}_power_percent_input"),
                "day_mask": _entity_number_value(states_by_id, f"number.saj_charge{slot}_day_mask_input"),
            }
        )
        discharge_slots.append(
            {
                "slot": slot,
                "start_time": _entity_text_value(states_by_id, f"text.saj_discharge{slot}_start_time_time"),
                "end_time": _entity_text_value(states_by_id, f"text.saj_discharge{slot}_end_time_time"),
                "power_percent": _entity_number_value(states_by_id, f"number.saj_discharge{slot}_power_percent_input"),
                "day_mask": _entity_number_value(states_by_id, f"number.saj_discharge{slot}_day_mask_input"),
            }
        )
        charge_effective_slots.append(
            {
                "slot": slot,
                "start_time": _sensor_slot_value("charge", slot, "start_time"),
                "end_time": _sensor_slot_value("charge", slot, "end_time"),
                "power_percent": _sensor_slot_value("charge", slot, "power_percent"),
                "power_w_estimate": _slot_power_w_estimate(_sensor_slot_value("charge", slot, "power_percent")),
                "day_mask": _sensor_slot_value("charge", slot, "day_mask"),
            }
        )
        discharge_effective_slots.append(
            {
                "slot": slot,
                "start_time": _sensor_slot_value("discharge", slot, "start_time"),
                "end_time": _sensor_slot_value("discharge", slot, "end_time"),
                "power_percent": _sensor_slot_value("discharge", slot, "power_percent"),
                "power_w_estimate": _slot_power_w_estimate(_sensor_slot_value("discharge", slot, "power_percent")),
                "day_mask": _sensor_slot_value("discharge", slot, "day_mask"),
            }
        )

    return {
        "updated_at": datetime.now(UTC).isoformat(),
        "working_mode": {
            "mode_input": _entity_number_value(states_by_id, "number.saj_app_mode_input"),
            "mode_sensor": _entity_number_value(states_by_id, "sensor.saj_app_mode"),
            "inverter_working_mode_sensor": _entity_number_value(states_by_id, "sensor.saj_inverter_working_mode"),
            "range": {"min": SAJ_MODE_MIN, "max": SAJ_MODE_MAX},
        },
        "charge": {
            "time_enable_mask": _entity_number_value(states_by_id, "number.saj_charge_time_enable_input"),
            "control_switch": _entity_switch_value(states_by_id, "switch.saj_charging_control"),
            "slots": charge_slots,
            "effective_slots": charge_effective_slots,
        },
        "discharge": {
            "time_enable_mask": _entity_number_value(states_by_id, "number.saj_discharge_time_enable_input"),
            "control_switch": _entity_switch_value(states_by_id, "switch.saj_discharging_control"),
            "slots": discharge_slots,
            "effective_slots": discharge_effective_slots,
        },
        "limits": {
            "battery_charge_power_limit": charge_power_limit,
            "battery_discharge_power_limit": discharge_power_limit,
            "grid_max_charge_power": _entity_number_value(states_by_id, "number.saj_grid_max_charge_power_input"),
            "grid_max_discharge_power": _entity_number_value(states_by_id, "number.saj_grid_max_discharge_power_input"),
        },
        "battery": {
            "soc_percent": _entity_number_value(states_by_id, "sensor.saj_battery_energy_percent"),
            "power_w": _entity_number_value(states_by_id, "sensor.saj_battery_power"),
        },
        "inverter": {
            "rated_power_w": SAJ_RATED_POWER_W,
            "status_text": _entity_text_value(states_by_id, "sensor.saj_inverter_status"),
        },
    }


def _normalize_saj_profile_id(profile_id: object) -> str:
    value = str(profile_id or "").strip().lower()
    return value if value in SAJ_PROFILE_IDS else ""


def _saj_profile_option_list() -> list[dict[str, object]]:
    return [
        {
            "id": profile_id,
            "mode_code": SAJ_PROFILE_MODE_CODES.get(profile_id),
        }
        for profile_id in SAJ_PROFILE_IDS
    ]


def _infer_saj_profile_from_mode_code(mode_code: object) -> str:
    code = _to_number(mode_code)
    if code is None:
        return ""
    normalized = int(code)
    for profile_id, profile_mode_code in SAJ_PROFILE_MODE_CODES.items():
        if normalized == profile_mode_code:
            return profile_id
    return ""


def _infer_saj_actual_profile(
    control_state: dict[str, object],
) -> tuple[str, str]:
    working_mode = control_state.get("working_mode")
    working_mode_map = working_mode if isinstance(working_mode, dict) else {}
    inverter = control_state.get("inverter")
    inverter_map = inverter if isinstance(inverter, dict) else {}
    inferred_from_mode = _infer_saj_profile_from_mode_code(working_mode_map.get("mode_sensor"))
    if inferred_from_mode:
        return inferred_from_mode, "mode_sensor"

    inverter_mode = str(working_mode_map.get("inverter_working_mode_sensor") or "").strip().lower()
    if inverter_mode:
        if any(token in inverter_mode for token in ("microgrid", "off-grid", "offnet")):
            return "microgrid", "inverter_working_mode_sensor"
        inferred_from_inverter_mode = _infer_saj_profile_from_mode_code(inverter_mode)
        if inferred_from_inverter_mode:
            return inferred_from_inverter_mode, "inverter_working_mode_sensor"

    inverter_status = str(inverter_map.get("status_text") or "").strip().lower()
    if any(token in inverter_status for token in ("microgrid", "off-grid", "offnet")):
        return "microgrid", "inverter_status"

    return "", ""


def _infer_saj_input_profile(
    control_state: dict[str, object],
) -> tuple[str, str]:
    working_mode = control_state.get("working_mode")
    working_mode_map = working_mode if isinstance(working_mode, dict) else {}
    inferred = _infer_saj_profile_from_mode_code(working_mode_map.get("mode_input"))
    return inferred, "mode_input" if inferred else ""


def _build_saj_profile_state(
    control_state: dict[str, object],
) -> dict[str, object]:
    selected_profile = _normalize_saj_profile_id(settings.saj_target_profile)
    actual_profile, actual_source = _infer_saj_actual_profile(control_state)
    input_profile, input_source = _infer_saj_input_profile(control_state)
    effective_profile = selected_profile or actual_profile or input_profile or ""
    pending_remote_sync = bool(
        selected_profile
        and input_profile == selected_profile
        and actual_profile != selected_profile
    )
    is_custom_remote_state = not pending_remote_sync and not actual_profile and bool(control_state)
    return {
        "updated_at": datetime.now(UTC).isoformat(),
        "selected_profile": selected_profile or None,
        "actual_profile": actual_profile or None,
        "actual_profile_source": actual_source or None,
        "input_profile": input_profile or None,
        "input_profile_source": input_source or None,
        "effective_profile": effective_profile or None,
        "pending_remote_sync": pending_remote_sync,
        "is_custom_remote_state": is_custom_remote_state,
        "available_profiles": _saj_profile_option_list(),
        "control_state": control_state,
    }


def _build_saj_control_capabilities(
    states_by_id: dict[str, dict[str, object]],
) -> dict[str, object]:
    return {
        "updated_at": datetime.now(UTC).isoformat(),
        "working_mode": {
            "entity": _entity_metadata(states_by_id, "number.saj_app_mode_input"),
            "range": {"min": SAJ_MODE_MIN, "max": SAJ_MODE_MAX, "step": 1},
            "accepted_values_note": "Mode labels can differ by SAJ firmware; this API allows mode_code 0..8 for testing.",
        },
        "slot": {
            "slot_range": {"min": SAJ_SLOT_MIN, "max": SAJ_SLOT_MAX},
            "time_format": "HH:MM",
            "day_mask_range": {"min": SAJ_DAY_MASK_MIN, "max": SAJ_DAY_MASK_MAX},
            "power_percent_range": {"min": 0, "max": 100},
            "charge_slot_template": {
                "start_entity_template": "text.saj_charge{slot}_start_time_time",
                "end_entity_template": "text.saj_charge{slot}_end_time_time",
                "power_entity_template": "number.saj_charge{slot}_power_percent_input",
                "day_mask_entity_template": "number.saj_charge{slot}_day_mask_input",
            },
            "discharge_slot_template": {
                "start_entity_template": "text.saj_discharge{slot}_start_time_time",
                "end_entity_template": "text.saj_discharge{slot}_end_time_time",
                "power_entity_template": "number.saj_discharge{slot}_power_percent_input",
                "day_mask_entity_template": "number.saj_discharge{slot}_day_mask_input",
            },
        },
        "switches": {
            "charging_control_entity": _entity_metadata(states_by_id, "switch.saj_charging_control"),
            "discharging_control_entity": _entity_metadata(states_by_id, "switch.saj_discharging_control"),
            "charge_time_enable_entity": _entity_metadata(states_by_id, "number.saj_charge_time_enable_input"),
            "discharge_time_enable_entity": _entity_metadata(states_by_id, "number.saj_discharge_time_enable_input"),
        },
    }


async def _saj_set_number(entity_id: str, value: int | float) -> None:
    await ha_client.call_service("number", "set_value", {"entity_id": entity_id, "value": value})


async def _saj_set_text(entity_id: str, value: str) -> None:
    await ha_client.call_service("text", "set_value", {"entity_id": entity_id, "value": value})


async def _saj_set_switch(entity_id: str, enabled: bool) -> None:
    service = "turn_on" if enabled else "turn_off"
    await ha_client.call_service("switch", service, {"entity_id": entity_id})


async def _saj_touch_switch(entity_id: str) -> bool:
    _, states_by_id = await _saj_control_states()
    current = _entity_switch_value(states_by_id, entity_id)
    if current is None:
        raise HTTPException(status_code=400, detail=f"Switch unavailable or not boolean: {entity_id}")
    # Force a readback-triggering edge while restoring original state.
    await _saj_set_switch(entity_id, not current)
    await _saj_set_switch(entity_id, current)
    return current


async def _persist_saj_target_profile(profile_id: str) -> None:
    persisted = await asyncio.to_thread(save_settings, {"saj_target_profile": profile_id})
    await _replace_runtime(persisted)


async def _saj_apply_profile(profile_id: str) -> dict[str, object]:
    normalized_profile = _normalize_saj_profile_id(profile_id)
    if not normalized_profile:
        raise HTTPException(status_code=400, detail=f"Unsupported SAJ profile: {profile_id}")

    changed: list[dict[str, object]] = []
    mode_code = SAJ_PROFILE_MODE_CODES[normalized_profile]
    await _saj_set_number("number.saj_app_mode_input", mode_code)
    changed.append({"entity_id": "number.saj_app_mode_input", "value": mode_code})

    await _persist_saj_target_profile(normalized_profile)
    _, states_by_id = await _saj_control_states()
    control_state = _build_saj_control_state(states_by_id)
    return {
        "ok": True,
        "profile_id": normalized_profile,
        "changed": changed,
        "profile_state": _build_saj_profile_state(control_state),
        "state": control_state,
    }


async def _saj_apply_slot(
    slot_kind: Literal["charge", "discharge"],
    slot: int,
    payload: SajSlotPayload,
) -> dict[str, object]:
    safe_slot = _validate_saj_slot(slot)
    changed: list[dict[str, object]] = []

    if payload.start_time is not None:
        start_time = _validate_hhmm(payload.start_time, "start_time")
        start_entity = f"text.saj_{slot_kind}{safe_slot}_start_time_time"
        await _saj_set_text(start_entity, start_time)
        changed.append({"entity_id": start_entity, "value": start_time})
    if payload.end_time is not None:
        end_time = _validate_hhmm(payload.end_time, "end_time")
        end_entity = f"text.saj_{slot_kind}{safe_slot}_end_time_time"
        await _saj_set_text(end_entity, end_time)
        changed.append({"entity_id": end_entity, "value": end_time})
    if payload.power_percent is not None:
        power_entity = f"number.saj_{slot_kind}{safe_slot}_power_percent_input"
        await _saj_set_number(power_entity, payload.power_percent)
        changed.append({"entity_id": power_entity, "value": payload.power_percent})
    if payload.day_mask is not None:
        day_mask_entity = f"number.saj_{slot_kind}{safe_slot}_day_mask_input"
        await _saj_set_number(day_mask_entity, payload.day_mask)
        changed.append({"entity_id": day_mask_entity, "value": payload.day_mask})

    if not changed:
        raise HTTPException(status_code=400, detail="No fields to update")

    _, states_by_id = await _saj_control_states()
    return {
        "ok": True,
        "slot_kind": slot_kind,
        "slot": safe_slot,
        "changed": changed,
        "state": _build_saj_control_state(states_by_id),
    }


def _solplanet_encode_slot(*, hour: int, minute: int, power: int, mode: int) -> int:
    return ((hour & 0xFF) << 24) | ((minute & 0xFF) << 16) | ((power & 0xFF) << 8) | (mode & 0xFF)


def _solplanet_decode_slot(raw: object) -> dict[str, object]:
    value = int(_to_number(raw) or 0)
    if value <= 0:
        return {
            "encoded": 0,
            "enabled": False,
            "hour": 0,
            "minute": 0,
            "power": 0,
            "mode": 0,
            "time_text": "00:00",
        }
    hour = (value >> 24) & 0xFF
    minute = (value >> 16) & 0xFF
    power = (value >> 8) & 0xFF
    mode = value & 0xFF
    return {
        "encoded": value,
        "enabled": True,
        "hour": hour,
        "minute": minute,
        "power": power,
        "mode": mode,
        "time_text": f"{hour:02d}:{minute:02d}",
    }


def _solplanet_day_slots(raw_day_values: object) -> list[int]:
    values = raw_day_values if isinstance(raw_day_values, list) else []
    out: list[int] = []
    for idx in range(SOLPLANET_SLOT_MAX):
        raw = values[idx] if idx < len(values) else 0
        out.append(int(_to_number(raw) or 0))
    return out


def _build_solplanet_control_state(schedule: dict[str, object]) -> dict[str, object]:
    days: dict[str, dict[str, object]] = {}
    for day in SOLPLANET_DAY_KEYS:
        encoded_slots = _solplanet_day_slots(schedule.get(day))
        decoded_slots: list[dict[str, object]] = []
        for idx, encoded in enumerate(encoded_slots, start=1):
            decoded = _solplanet_decode_slot(encoded)
            decoded["slot"] = idx
            decoded_slots.append(decoded)
        days[day] = {
            "encoded_slots": encoded_slots,
            "decoded_slots": decoded_slots,
        }
    return {
        "updated_at": datetime.now(UTC).isoformat(),
        "limits": {
            "pin": int(_to_number(schedule.get("Pin")) or 0),
            "pout": int(_to_number(schedule.get("Pout")) or 0),
        },
        "days": days,
        "raw_schedule": schedule,
        "encoding_note": "slot encoding uses byte layout [hour, minute, power, mode]; inferred from local schedule data.",
    }


async def _solplanet_get_schedule_live() -> dict[str, object]:
    _ensure_solplanet_configured()
    schedule = await asyncio.wait_for(
        solplanet_client.get_schedule(),
        timeout=settings.solplanet_request_timeout_seconds,
    )
    payload = schedule if isinstance(schedule, dict) else {}
    normalized: dict[str, object] = {"Pin": int(_to_number(payload.get("Pin")) or 0), "Pout": int(_to_number(payload.get("Pout")) or 0)}
    for day in SOLPLANET_DAY_KEYS:
        normalized[day] = _solplanet_day_slots(payload.get(day))
    return normalized


async def _solplanet_get_schedule_with_fallback() -> dict[str, object]:
    try:
        return await _solplanet_get_schedule_live()
    except Exception:  # noqa: BLE001
        snapshot = await asyncio.to_thread(
            get_latest_raw_request_result,
            storage_db_path,
            source="solplanet_cgi",
            endpoint="getdefine",
        )
        payload = snapshot.get("payload") if isinstance(snapshot, dict) else None
        if not isinstance(payload, dict):
            raise
        normalized: dict[str, object] = {
            "Pin": int(_to_number(payload.get("Pin")) or 0),
            "Pout": int(_to_number(payload.get("Pout")) or 0),
        }
        for day in SOLPLANET_DAY_KEYS:
            normalized[day] = _solplanet_day_slots(payload.get(day))
        return normalized


async def _solplanet_set_schedule_payload(payload: dict[str, object]) -> dict[str, object]:
    _ensure_solplanet_configured()
    response = await asyncio.wait_for(
        solplanet_client.set_value(payload),
        timeout=settings.solplanet_request_timeout_seconds,
    )
    _reset_solplanet_caches()
    return response if isinstance(response, dict) else {}


def _validate_solplanet_day_slots(slots: list[int]) -> list[int]:
    if len(slots) != SOLPLANET_SLOT_MAX:
        raise HTTPException(status_code=400, detail=f"slots must contain exactly {SOLPLANET_SLOT_MAX} integers")
    out: list[int] = []
    for item in slots:
        value = int(_to_number(item) or 0)
        if value < 0 or value > 0xFFFFFFFF:
            raise HTTPException(status_code=400, detail="each slot value must be within 0..4294967295")
        out.append(value)
    return out


async def _replace_runtime(new_settings: Settings) -> None:
    global settings, ha_client, sample_interval_seconds, solplanet_client, solplanet_sample_interval_seconds  # noqa: PLW0603
    async with runtime_lock:
        old_solplanet = solplanet_client
        settings = new_settings
        sample_interval_seconds = float(new_settings.saj_sample_interval_seconds)
        solplanet_sample_interval_seconds = float(new_settings.solplanet_sample_interval_seconds)
        ha_client = HomeAssistantClient(
            base_url=settings.ha_url,
            token=settings.ha_token,
            request_logger=_handle_outbound_request_log,
        )
        solplanet_client = _new_solplanet_client(settings)
        collector_status.setdefault("saj", {})["interval_seconds"] = None
        collector_status.setdefault("saj", {})["continuous"] = True
        collector_status.setdefault("solplanet", {})["interval_seconds"] = None
        collector_status.setdefault("solplanet", {})["continuous"] = True
        _reset_solplanet_caches()
        _weather_cache.clear()
        await old_solplanet.aclose()


async def _recreate_solplanet_client() -> None:
    global solplanet_client  # noqa: PLW0603
    async with runtime_lock:
        old_solplanet = solplanet_client
        solplanet_client = _new_solplanet_client(settings)
        _reset_solplanet_caches()
    await old_solplanet.aclose()


def _sample_from_flow(system: str, flow: dict[str, object], *, round_id: str) -> EnergySample:
    metrics = flow.get("metrics")
    metrics_map = metrics if isinstance(metrics, dict) else {}
    if system == "solplanet":
        source = "solplanet_cgi"
    elif system == "combined":
        source = "combined_worker"
    else:
        source = "home_assistant"
    return EnergySample(
        round_id=round_id,
        system=system,
        assembled_at_utc=datetime.now(UTC),
        source=source,
        pv_w=_to_number(metrics_map.get("pv_w")),
        grid_w=_to_number(metrics_map.get("grid_w")),
        battery_w=_to_number(metrics_map.get("battery_w")),
        load_w=_to_number(metrics_map.get("load_w")),
        battery_soc_percent=_to_number(metrics_map.get("battery_soc_percent")),
        inverter_status=str(metrics_map.get("inverter_status"))
        if metrics_map.get("inverter_status") is not None
        else None,
        balance_w=_to_number(metrics_map.get("balance_w")),
        flow=flow,
    )


def _safe_parse_utc(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


async def _set_tesla_control_feedback(
    *,
    phase: str,
    target_enabled: bool,
    requested_at_utc: str | None = None,
    error: str | None = None,
) -> dict[str, object]:
    now = datetime.now(UTC)
    requested_at = _safe_parse_utc(requested_at_utc) or now
    ttl_seconds = (
        TESLA_CONTROL_FEEDBACK_FAILURE_TTL_SECONDS
        if phase in {"failed", "delayed"}
        else TESLA_CONTROL_FEEDBACK_SUCCESS_TTL_SECONDS
        if phase == "success"
        else TESLA_CONTROL_FEEDBACK_PENDING_TIMEOUT_SECONDS
    )
    payload = {
        "phase": str(phase).strip() or "requesting",
        "target_enabled": bool(target_enabled),
        "requested_at_utc": requested_at.isoformat(),
        "updated_at_utc": now.isoformat(),
        "expires_at_utc": (requested_at + timedelta(seconds=ttl_seconds)).isoformat(),
        "error": str(error or "").strip() or None,
    }
    rows = [
        (f"{TESLA_CONTROL_FEEDBACK_KV_PREFIX}{key}", json.dumps(value, ensure_ascii=False), "ui")
        for key, value in payload.items()
    ]
    await asyncio.to_thread(upsert_realtime_kv, storage_db_path, rows)
    return payload


def _load_tesla_control_feedback_sync() -> dict[str, object] | None:
    kv_map = get_realtime_kv_by_prefix(storage_db_path, prefix=TESLA_CONTROL_FEEDBACK_KV_PREFIX)
    if not kv_map:
        return None
    payload: dict[str, object] = {}
    for key in ("phase", "target_enabled", "requested_at_utc", "updated_at_utc", "expires_at_utc", "error"):
        item = kv_map.get(f"{TESLA_CONTROL_FEEDBACK_KV_PREFIX}{key}")
        if item is not None:
            payload[key] = item.get("value")
    return payload or None


def _resolve_tesla_control_feedback(control_state: dict[str, object] | None) -> dict[str, object] | None:
    stored = _load_tesla_control_feedback_sync()
    if not isinstance(stored, dict) or not stored:
        return None

    requested_at = _safe_parse_utc(stored.get("requested_at_utc"))
    if requested_at is None:
        return None
    now = datetime.now(UTC)
    target_enabled = bool(stored.get("target_enabled"))
    charging_enabled = control_state.get("charging_enabled") if isinstance(control_state, dict) else None
    requested_enabled = control_state.get("charge_requested_enabled") if isinstance(control_state, dict) else None
    age_seconds = max(0.0, (now - requested_at).total_seconds())
    base_phase = str(stored.get("phase") or "requesting").strip().lower()

    resolved_phase = base_phase
    if charging_enabled is target_enabled:
        if age_seconds > TESLA_CONTROL_FEEDBACK_SUCCESS_TTL_SECONDS:
            return None
        resolved_phase = "success"
    elif age_seconds > TESLA_CONTROL_FEEDBACK_PENDING_TIMEOUT_SECONDS:
        resolved_phase = "delayed" if requested_enabled is target_enabled else "failed"
        expires_at = requested_at + timedelta(seconds=TESLA_CONTROL_FEEDBACK_FAILURE_TTL_SECONDS)
        if now > expires_at:
            return None
    elif requested_enabled is target_enabled:
        resolved_phase = "awaiting_vehicle"
    else:
        resolved_phase = "requesting"

    return {
        "phase": resolved_phase,
        "target_enabled": target_enabled,
        "requested_at_utc": requested_at.isoformat(),
        "updated_at_utc": str(stored.get("updated_at_utc") or requested_at.isoformat()),
        "age_seconds": round(age_seconds, 1),
        "error": str(stored.get("error") or "").strip() or None,
    }


def _kv_source_for_path(system: str, path: str) -> str:
    def _cgi_url(endpoint: str) -> str:
        return (
            f"{settings.solplanet_dongle_scheme}://"
            f"{settings.solplanet_dongle_host}:{settings.solplanet_dongle_port}/{endpoint}"
        )

    if system == "combined":
        return "combined_worker"
    if system != "solplanet":
        return "home_assistant_derived"
    if path.startswith("raw.inverter_info"):
        return _cgi_url("getdev.cgi?device=2")
    if path.startswith("raw.inverter_data"):
        return _cgi_url("getdevdata.cgi?device=2")
    if path.startswith("raw.meter_data"):
        return _cgi_url("getdevdata.cgi?device=3")
    if path.startswith("raw.meter_info"):
        return _cgi_url("getdev.cgi?device=3")
    if path.startswith("raw.battery_data"):
        return _cgi_url("getdevdata.cgi?device=4")
    if path.startswith("raw.schedule"):
        return _cgi_url("getdefine.cgi")
    if (
        path.startswith("metrics.pv_w")
        or path.startswith("metrics.load_w")
        or path.startswith("metrics.inverter_status")
        or path.startswith("metrics.inverter_power_w")
    ):
        return _cgi_url("getdevdata.cgi?device=2")
    if path.startswith("metrics.grid_w"):
        return _cgi_url("getdevdata.cgi?device=3")
    if path.startswith("metrics.battery_w") or path.startswith("metrics.battery_soc_percent"):
        return _cgi_url("getdevdata.cgi?device=4")
    if path.startswith("metrics"):
        return _cgi_url("getdevdata.cgi?device=2")
    return _cgi_url("getdev.cgi?device=2")


def _flatten_payload_to_kv(
    *,
    system: str,
    path: str,
    value: object,
    out_rows: list[tuple[str, str, str]],
) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            key_text = str(key)
            next_path = f"{path}.{key_text}" if path else key_text
            _flatten_payload_to_kv(system=system, path=next_path, value=item, out_rows=out_rows)
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            next_path = f"{path}.{index}" if path else str(index)
            _flatten_payload_to_kv(system=system, path=next_path, value=item, out_rows=out_rows)
        return
    if not path:
        return

    attribute = f"{system}.{path}"
    source = _kv_source_for_path(system, path)
    out_rows.append((attribute, json.dumps(value, ensure_ascii=False), source))


async def _store_realtime_kv_snapshot(system: str, flow: dict[str, object], *, round_id: str | None = None) -> None:
    rows: list[tuple[str, str, str]] = []
    _flatten_payload_to_kv(system=system, path="", value=flow, out_rows=rows)

    updated_at = str(flow.get("updated_at") or datetime.now(UTC).isoformat())
    rows.append((f"{system}.update_time", json.dumps(updated_at, ensure_ascii=False), "collector"))
    await _run_db_call_with_retry(
        upsert_realtime_kv,
        storage_db_path,
        rows,
        operation_name="store_realtime_kv_snapshot",
        trace_system=system,
        trace_round_id=round_id,
        extra_failure_context={"row_count": len(rows)},
    )


def _kv_value(
    kv_map: dict[str, dict[str, object]],
    *,
    system: str,
    key: str,
) -> object:
    item = kv_map.get(f"{system}.{key}")
    if not isinstance(item, dict):
        return None
    return item.get("value")


def _rebuild_notes_from_kv(kv_map: dict[str, dict[str, object]], *, system: str) -> list[str]:
    prefix = f"{system}.metrics.notes."
    indexed: list[tuple[int, str]] = []
    for attr, item in kv_map.items():
        if attr.startswith(prefix):
            idx_str = attr[len(prefix):]
            try:
                idx = int(idx_str)
            except ValueError:
                continue
            val = item.get("value") if isinstance(item, dict) else None
            if isinstance(val, str):
                indexed.append((idx, val))
    return [v for _, v in sorted(indexed)]


def _build_flow_from_kv(system: str, kv_map: dict[str, dict[str, object]]) -> dict[str, object]:
    updated_at_raw = _kv_value(kv_map, system=system, key="update_time")
    updated_at = str(updated_at_raw or datetime.now(UTC).isoformat())
    sampled_at = _safe_parse_utc(updated_at)
    age_seconds = (datetime.now(UTC) - sampled_at).total_seconds() if sampled_at else None
    interval = _sample_interval_for_system(system)
    stale_after_seconds = max(15.0, interval * 2.5)

    metrics = {
        "pv_w": _to_number(_kv_value(kv_map, system=system, key="metrics.pv_w")),
        "grid_w": _to_number(_kv_value(kv_map, system=system, key="metrics.grid_w")),
        "battery_w": _to_number(_kv_value(kv_map, system=system, key="metrics.battery_w")),
        "load_w": _to_number(_kv_value(kv_map, system=system, key="metrics.load_w")),
        "pv_source": _kv_value(kv_map, system=system, key="metrics.pv_source"),
        "grid_source": _kv_value(kv_map, system=system, key="metrics.grid_source"),
        "battery_source": _kv_value(kv_map, system=system, key="metrics.battery_source"),
        "load_source": _kv_value(kv_map, system=system, key="metrics.load_source"),
        "battery_soc_percent": _to_number(_kv_value(kv_map, system=system, key="metrics.battery_soc_percent")),
        "inverter_status": _kv_value(kv_map, system=system, key="metrics.inverter_status"),
        "inverter_power_w": _to_number(_kv_value(kv_map, system=system, key="metrics.inverter_power_w")),
        "inverter_power_source": _kv_value(kv_map, system=system, key="metrics.inverter_power_source"),
        "solar_active": bool(_kv_value(kv_map, system=system, key="metrics.solar_active")),
        "grid_active": bool(_kv_value(kv_map, system=system, key="metrics.grid_active")),
        "grid_import": bool(_kv_value(kv_map, system=system, key="metrics.grid_import")),
        "battery_active": bool(_kv_value(kv_map, system=system, key="metrics.battery_active")),
        "battery_discharging": bool(_kv_value(kv_map, system=system, key="metrics.battery_discharging")),
        "load_active": bool(_kv_value(kv_map, system=system, key="metrics.load_active")),
        "balance_w": _to_number(_kv_value(kv_map, system=system, key="metrics.balance_w")),
        "balanced": _kv_value(kv_map, system=system, key="metrics.balanced"),
        "matched_entities": _to_number(_kv_value(kv_map, system=system, key="metrics.matched_entities")),
        "notes": _rebuild_notes_from_kv(kv_map, system=system),
    }
    if system == "combined":
        metrics.update(
            {
                "solar_primary_w": _to_number(_kv_value(kv_map, system=system, key="metrics.solar_primary_w")),
                "solar_secondary_w": _to_number(_kv_value(kv_map, system=system, key="metrics.solar_secondary_w")),
                "battery1_w": _to_number(_kv_value(kv_map, system=system, key="metrics.battery1_w")),
                "battery2_w": _to_number(_kv_value(kv_map, system=system, key="metrics.battery2_w")),
                "battery1_soc_percent": _to_number(
                    _kv_value(kv_map, system=system, key="metrics.battery1_soc_percent")
                ),
                "battery2_soc_percent": _to_number(
                    _kv_value(kv_map, system=system, key="metrics.battery2_soc_percent")
                ),
                "inverter1_w": _to_number(_kv_value(kv_map, system=system, key="metrics.inverter1_w")),
                "inverter2_w": _to_number(_kv_value(kv_map, system=system, key="metrics.inverter2_w")),
                "inverter1_status": _kv_value(kv_map, system=system, key="metrics.inverter1_status"),
                "inverter2_status": _kv_value(kv_map, system=system, key="metrics.inverter2_status"),
                "battery1_source": _kv_value(kv_map, system=system, key="metrics.battery1_source"),
                "battery2_source": _kv_value(kv_map, system=system, key="metrics.battery2_source"),
                "inverter1_power_source": _kv_value(kv_map, system=system, key="metrics.inverter1_power_source"),
                "inverter2_power_source": _kv_value(kv_map, system=system, key="metrics.inverter2_power_source"),
                "tesla_charge_power_w": _to_number(_kv_value(kv_map, system=system, key="metrics.tesla_charge_power_w")),
                "tesla_charge_current_amps": _to_number(
                    _kv_value(kv_map, system=system, key="metrics.tesla_charge_current_amps")
                ),
                "tesla_configured_current_amps": _to_number(
                    _kv_value(kv_map, system=system, key="metrics.tesla_configured_current_amps")
                ),
                "tesla_battery_soc_percent": _to_number(
                    _kv_value(kv_map, system=system, key="metrics.tesla_battery_soc_percent")
                ),
                "tesla_connection_state": _kv_value(kv_map, system=system, key="metrics.tesla_connection_state"),
                "tesla_charge_requested_enabled": _kv_value(
                    kv_map, system=system, key="metrics.tesla_charge_requested_enabled"
                ),
                "tesla_cable_connected": _kv_value(kv_map, system=system, key="metrics.tesla_cable_connected"),
            }
        )

    flow: dict[str, object] = {
        "system": system,
        "updated_at": updated_at,
        "metrics": metrics,
        "source": {"type": "realtime_kv"},
        "storage_backed": True,
        "kv_item_count": len(kv_map),
    }
    if system == "combined":
        flow["display"] = _build_combined_display(metrics)
        tesla_updated_at = _kv_value(kv_map, system=system, key="source.components.tesla_updated_at")
        flow["source"] = {
            "type": "realtime_kv",
            "components": {
                "saj_updated_at": _kv_value(kv_map, system=system, key="source.components.saj_updated_at"),
                "solplanet_updated_at": _kv_value(kv_map, system=system, key="source.components.solplanet_updated_at"),
                "tesla_updated_at": tesla_updated_at,
            },
        }
        flow["entities"] = {
            "tesla_charge_power": {
                "entity_id": _kv_value(kv_map, system=system, key="entities.tesla_charge_power.entity_id"),
                "domain": _kv_value(kv_map, system=system, key="entities.tesla_charge_power.domain"),
                "brand_guess": _kv_value(kv_map, system=system, key="entities.tesla_charge_power.brand_guess"),
                "state": _kv_value(kv_map, system=system, key="entities.tesla_charge_power.state"),
                "unit": _kv_value(kv_map, system=system, key="entities.tesla_charge_power.unit"),
                "friendly_name": _kv_value(kv_map, system=system, key="entities.tesla_charge_power.friendly_name"),
                "last_updated": _kv_value(kv_map, system=system, key="entities.tesla_charge_power.last_updated"),
            },
            "tesla_charge_current": {
                "entity_id": _kv_value(kv_map, system=system, key="entities.tesla_charge_current.entity_id"),
                "domain": _kv_value(kv_map, system=system, key="entities.tesla_charge_current.domain"),
                "brand_guess": _kv_value(kv_map, system=system, key="entities.tesla_charge_current.brand_guess"),
                "state": _kv_value(kv_map, system=system, key="entities.tesla_charge_current.state"),
                "unit": _kv_value(kv_map, system=system, key="entities.tesla_charge_current.unit"),
                "friendly_name": _kv_value(kv_map, system=system, key="entities.tesla_charge_current.friendly_name"),
                "last_updated": _kv_value(kv_map, system=system, key="entities.tesla_charge_current.last_updated"),
            },
            "tesla_battery_soc": {
                "entity_id": _kv_value(kv_map, system=system, key="entities.tesla_battery_soc.entity_id"),
                "domain": _kv_value(kv_map, system=system, key="entities.tesla_battery_soc.domain"),
                "brand_guess": _kv_value(kv_map, system=system, key="entities.tesla_battery_soc.brand_guess"),
                "state": _kv_value(kv_map, system=system, key="entities.tesla_battery_soc.state"),
                "unit": _kv_value(kv_map, system=system, key="entities.tesla_battery_soc.unit"),
                "friendly_name": _kv_value(kv_map, system=system, key="entities.tesla_battery_soc.friendly_name"),
                "last_updated": _kv_value(kv_map, system=system, key="entities.tesla_battery_soc.last_updated"),
            },
        }
        flow["raw"] = {
            "tesla": {
                "executed_at_utc": tesla_updated_at,
                "evaluated_at_local": _kv_value(kv_map, system=system, key="raw.tesla.evaluated_at_local"),
                "timezone": _kv_value(kv_map, system=system, key="raw.tesla.timezone"),
                "window_active": _kv_value(kv_map, system=system, key="raw.tesla.window_active"),
                "task_mode": _kv_value(kv_map, system=system, key="raw.tesla.task_mode"),
                "control_state": {
                    "available": _kv_value(kv_map, system=system, key="raw.tesla.control_state.available"),
                    "control_mode": _kv_value(kv_map, system=system, key="raw.tesla.control_state.control_mode"),
                    "charging_enabled": _kv_value(kv_map, system=system, key="raw.tesla.control_state.charging_enabled"),
                    "charge_requested_enabled": _kv_value(
                        kv_map, system=system, key="raw.tesla.control_state.charge_requested_enabled"
                    ),
                    "can_start": _kv_value(kv_map, system=system, key="raw.tesla.control_state.can_start"),
                    "can_stop": _kv_value(kv_map, system=system, key="raw.tesla.control_state.can_stop"),
                    "switch_entity": {
                        "entity_id": _kv_value(kv_map, system=system, key="raw.tesla.control_state.switch_entity.entity_id"),
                        "friendly_name": _kv_value(
                            kv_map, system=system, key="raw.tesla.control_state.switch_entity.friendly_name"
                        ),
                        "state": _kv_value(kv_map, system=system, key="raw.tesla.control_state.switch_entity.state"),
                        "unit": _kv_value(kv_map, system=system, key="raw.tesla.control_state.switch_entity.unit"),
                        "last_updated": _kv_value(
                            kv_map, system=system, key="raw.tesla.control_state.switch_entity.last_updated"
                        ),
                    },
                    "start_button_entity": {
                        "entity_id": _kv_value(
                            kv_map, system=system, key="raw.tesla.control_state.start_button_entity.entity_id"
                        ),
                        "friendly_name": _kv_value(
                            kv_map, system=system, key="raw.tesla.control_state.start_button_entity.friendly_name"
                        ),
                        "state": _kv_value(
                            kv_map, system=system, key="raw.tesla.control_state.start_button_entity.state"
                        ),
                        "unit": _kv_value(kv_map, system=system, key="raw.tesla.control_state.start_button_entity.unit"),
                        "last_updated": _kv_value(
                            kv_map, system=system, key="raw.tesla.control_state.start_button_entity.last_updated"
                        ),
                    },
                    "stop_button_entity": {
                        "entity_id": _kv_value(
                            kv_map, system=system, key="raw.tesla.control_state.stop_button_entity.entity_id"
                        ),
                        "friendly_name": _kv_value(
                            kv_map, system=system, key="raw.tesla.control_state.stop_button_entity.friendly_name"
                        ),
                        "state": _kv_value(
                            kv_map, system=system, key="raw.tesla.control_state.stop_button_entity.state"
                        ),
                        "unit": _kv_value(kv_map, system=system, key="raw.tesla.control_state.stop_button_entity.unit"),
                        "last_updated": _kv_value(
                            kv_map, system=system, key="raw.tesla.control_state.stop_button_entity.last_updated"
                        ),
                    },
                },
                "observation": {
                    "battery": {
                        "level_percent": metrics.get("tesla_battery_soc_percent"),
                        "entity": {
                            "entity_id": _kv_value(
                                kv_map, system=system, key="raw.tesla.observation.battery.entity.entity_id"
                            ),
                            "friendly_name": _kv_value(
                                kv_map, system=system, key="raw.tesla.observation.battery.entity.friendly_name"
                            ),
                            "state": _kv_value(kv_map, system=system, key="raw.tesla.observation.battery.entity.state"),
                            "unit": _kv_value(kv_map, system=system, key="raw.tesla.observation.battery.entity.unit"),
                            "last_updated": _kv_value(
                                kv_map, system=system, key="raw.tesla.observation.battery.entity.last_updated"
                            ),
                        },
                    },
                    "charging": {
                        "enabled": _kv_value(kv_map, system=system, key="raw.tesla.observation.charging.enabled"),
                        "power_w": metrics.get("tesla_charge_power_w"),
                        "current_amps": metrics.get("tesla_charge_current_amps"),
                        "configured_current_amps": metrics.get("tesla_configured_current_amps"),
                        "min_current_amps": _to_number(
                            _kv_value(kv_map, system=system, key="raw.tesla.observation.charging.min_current_amps")
                        ),
                        "max_current_amps": _to_number(
                            _kv_value(kv_map, system=system, key="raw.tesla.observation.charging.max_current_amps")
                        ),
                        "current_step_amps": _to_number(
                            _kv_value(kv_map, system=system, key="raw.tesla.observation.charging.current_step_amps")
                        ),
                        "requested_enabled": metrics.get("tesla_charge_requested_enabled"),
                        "cable_connected": metrics.get("tesla_cable_connected"),
                        "connection_state": metrics.get("tesla_connection_state"),
                        "status_text": _kv_value(kv_map, system=system, key="raw.tesla.observation.charging.status_text"),
                        "voltage_v": _to_number(
                            _kv_value(kv_map, system=system, key="raw.tesla.observation.charging.voltage_v")
                        ),
                    },
                    "control_mode": _kv_value(kv_map, system=system, key="raw.tesla.observation.control_mode"),
                },
            }
        }
    if age_seconds is not None:
        flow["sample_age_seconds"] = round(age_seconds, 1)
        if age_seconds > stale_after_seconds:
            flow["stale"] = True
            flow["stale_reason"] = "realtime_kv_too_old"
    return flow


def _build_storage_backed_flow(system: str, sample: dict[str, object]) -> dict[str, object]:
    payload_obj = sample.get("payload")
    payload = dict(payload_obj) if isinstance(payload_obj, dict) else {}
    metrics_obj = payload.get("metrics")
    metrics = dict(metrics_obj) if isinstance(metrics_obj, dict) else {}

    sampled_at_utc = str(sample.get("sampled_at_utc") or datetime.now(UTC).isoformat())
    metrics.setdefault("pv_w", _to_number(sample.get("pv_w")))
    metrics.setdefault("grid_w", _to_number(sample.get("grid_w")))
    metrics.setdefault("battery_w", _to_number(sample.get("battery_w")))
    metrics.setdefault("load_w", _to_number(sample.get("load_w")))
    metrics.setdefault("battery_soc_percent", _to_number(sample.get("battery_soc_percent")))
    metrics.setdefault("pv_source", "unavailable")
    metrics.setdefault("grid_source", "unavailable")
    metrics.setdefault("battery_source", "unavailable")
    metrics.setdefault("load_source", "unavailable")
    metrics.setdefault(
        "inverter_status",
        str(sample.get("inverter_status")) if sample.get("inverter_status") is not None else None,
    )
    metrics.setdefault("inverter_power_w", _to_number(metrics.get("inverter_power_w")))
    metrics.setdefault("inverter_power_source", "unavailable")
    metrics.setdefault("balance_w", _to_number(sample.get("balance_w")))

    if metrics.get("matched_entities") is None:
        keys = ("pv_w", "grid_w", "battery_w", "load_w", "battery_soc_percent", "inverter_status", "inverter_power_w")
        metrics["matched_entities"] = sum(1 for key in keys if metrics.get(key) is not None)

    flow: dict[str, object] = dict(payload)
    flow["system"] = system
    flow["updated_at"] = sampled_at_utc
    flow["metrics"] = metrics
    if system == "combined":
        flow["display"] = _build_combined_display(metrics)
    flow["source"] = {
        "type": "storage_latest_sample",
        "sample_source": str(sample.get("source") or ""),
    }
    flow["storage_backed"] = True

    sampled_at = _safe_parse_utc(sampled_at_utc)
    now = datetime.now(UTC)
    age_seconds = (now - sampled_at).total_seconds() if sampled_at else None
    interval = _sample_interval_for_system(system)
    stale_after_seconds = max(15.0, interval * 2.5)
    if age_seconds is not None:
        flow["sample_age_seconds"] = round(age_seconds, 1)
        if age_seconds > stale_after_seconds:
            flow["stale"] = True
            flow["stale_reason"] = "storage_sample_too_old"

    return flow


async def _get_energy_flow_from_storage(system: str) -> dict[str, object]:
    sample = await asyncio.to_thread(get_latest_sample, storage_db_path, system=system)
    if not isinstance(sample, dict):
        raise HTTPException(
            status_code=503,
            detail={
                "message": "No stored sample available yet",
                "system": system,
            },
        )
    return _build_storage_backed_flow(system, sample)


async def _get_energy_flow_from_realtime_kv(system: str) -> dict[str, object]:
    prefix = f"{system}."
    kv_map = await asyncio.to_thread(get_realtime_kv_by_prefix, storage_db_path, prefix=prefix)
    if not kv_map:
        return await _get_energy_flow_from_storage(system)
    return _build_flow_from_kv(system, kv_map)


async def _get_live_energy_flow(system: str) -> dict[str, object]:
    if system == "saj":
        _ensure_ha_configured()
        states = await ha_client.all_states()
        return _build_energy_flow_payload("saj", states)
    if system == "solplanet":
        return await _get_solplanet_flow_cached()
    if system == "combined":
        _ensure_ha_configured()
        saj_flow = await _get_live_energy_flow("saj")
        solplanet_flow = await _get_live_energy_flow("solplanet")
        tesla_states = await _tesla_control_states()
        tesla_control_state = _build_tesla_control_state(tesla_states)
        tesla_observation = _build_tesla_observation_payload(tesla_states)
        tesla_result = {
            "executed_at_utc": datetime.now(UTC).isoformat(),
            "control_state": tesla_control_state,
            "observation": tesla_observation,
        }
        return _build_combined_flow_payload(saj_flow, solplanet_flow, tesla_result)
    raise HTTPException(status_code=400, detail=f"Unsupported system: {system}")


async def _store_flow_sample(system: str, flow: dict[str, object], *, round_id: str) -> None:
    sample = _sample_from_flow(system, flow, round_id=round_id)
    try:
        await _run_db_call_with_retry(
            insert_sample,
            storage_db_path,
            sample,
            operation_name="store_flow_sample",
            trace_system=system,
            trace_round_id=round_id,
            extra_failure_context={"sample_system": system},
        )
        _clear_persistence_failure(system)
    except Exception as exc:  # noqa: BLE001
        _record_persistence_failure(
            system,
            operation_name="store_flow_sample",
            error=exc,
            round_id=round_id,
        )
        raise
    await _store_realtime_kv_snapshot(system, flow, round_id=round_id)


async def _collect_for_system(system: str, *, round_number: int, round_id: str, round_started_at_utc: str) -> dict[str, object]:
    actor_token = request_actor_ctx.set("worker")
    system_token = request_system_ctx.set(system)
    round_token = request_round_ctx.set(round_id)
    round_started_token = request_round_started_at_ctx.set(round_started_at_utc)
    try:
        if system == "saj":
            if _missing_required_config():
                return {
                    "attempted": [],
                    "succeeded": [],
                    "failed": [],
                    "skipped": list(SAJ_ENDPOINTS),
                    "stored_sample": False,
                    "reason": "missing_required_config",
                }
            endpoint = "home_assistant_all_states"
            if not _endpoint_is_eligible("saj", endpoint, round_number):
                _mark_endpoint_skipped("saj", endpoint, round_number)
                return {
                    "attempted": [],
                    "succeeded": [],
                    "failed": [],
                    "skipped": [endpoint],
                    "stored_sample": False,
                    "reason": "endpoint_backoff",
                }
            _mark_endpoint_attempt("saj", endpoint, round_number)
            try:
                states = await ha_client.all_states()
                _mark_endpoint_success("saj", endpoint, round_number)
            except Exception as exc:  # noqa: BLE001
                error_text = f"{type(exc).__name__}: {exc}"
                _mark_endpoint_failure("saj", endpoint, round_number, error_text)
                raise
            flow = _build_energy_flow_payload("saj", states)
            missing_metrics = _missing_required_flow_metrics("saj", flow)
            if missing_metrics:
                return {
                    "attempted": [endpoint],
                    "succeeded": [endpoint],
                    "failed": [],
                    "skipped": [],
                    "stored_sample": False,
                    "flow": None,
                    "reason": "incomplete_flow",
                    "missing_metrics": missing_metrics,
                }
            entities_map = flow.get("entities") or {}
            core_entity_keys = ("pv", "grid", "battery", "load")
            entity_times: list[datetime] = []
            for key in core_entity_keys:
                entity = entities_map.get(key)
                if isinstance(entity, dict):
                    parsed = _safe_parse_utc(str(entity.get("last_updated") or ""))
                    if parsed:
                        entity_times.append(parsed)
            if entity_times:
                most_recent_entity_update = max(entity_times)
                ha_entity_age_seconds = (datetime.now(UTC) - most_recent_entity_update).total_seconds()
                ha_entity_stale_threshold = max(300.0, sample_interval_seconds * 10)
                if ha_entity_age_seconds > ha_entity_stale_threshold:
                    return {
                        "attempted": [endpoint],
                        "succeeded": [endpoint],
                        "failed": [],
                        "skipped": [],
                        "stored_sample": False,
                        "flow": None,
                        "reason": "ha_entities_stale",
                        "ha_entity_age_seconds": round(ha_entity_age_seconds),
                        "ha_entity_stale_threshold": ha_entity_stale_threshold,
                        "ha_entity_last_updated": most_recent_entity_update.isoformat(),
                    }
            await _store_flow_sample("saj", flow, round_id=round_id)
            return {
                "attempted": [endpoint],
                "succeeded": [endpoint],
                "failed": [],
                "skipped": [],
                "stored_sample": True,
                "flow": flow,
            }

        if system == "solplanet":
            if not solplanet_client.configured:
                raise RuntimeError("Solplanet CGI client is not configured")
            flow = await asyncio.wait_for(
                _build_solplanet_energy_flow_payload_from_cgi(round_number=round_number),
                timeout=_solplanet_collection_round_timeout_seconds(settings),
            )
            notes = flow.get("metrics", {}).get("notes") if isinstance(flow.get("metrics"), dict) else []
            failed = []
            skipped = []
            if isinstance(notes, list):
                failed = [
                    item.split(":", 2)[1]
                    for item in notes
                    if isinstance(item, str) and item.startswith("solplanet_endpoint_error:")
                ]
                skipped = [
                    item.split(":", 1)[1]
                    for item in notes
                    if isinstance(item, str) and item.startswith("solplanet_endpoint_skipped:")
                ]
            attempted = [endpoint for endpoint in SOLPLANET_ENDPOINTS if endpoint not in skipped]
            succeeded = [endpoint for endpoint in attempted if endpoint not in failed]
            missing_metrics = _missing_required_flow_metrics("solplanet", flow)
            if failed or skipped or missing_metrics:
                return {
                    "attempted": attempted,
                    "succeeded": succeeded,
                    "failed": failed,
                    "skipped": skipped,
                    "stored_sample": False,
                    "flow": None,
                    "reason": "incomplete_flow",
                    "missing_metrics": missing_metrics,
                }
            await _store_flow_sample("solplanet", flow, round_id=round_id)
            return {
                "attempted": attempted,
                "succeeded": succeeded,
                "failed": failed,
                "skipped": skipped,
                "stored_sample": True,
                "flow": flow,
            }
        raise ValueError(f"Unsupported system: {system}")
    finally:
        request_round_started_at_ctx.reset(round_started_token)
        request_round_ctx.reset(round_token)
        request_system_ctx.reset(system_token)
        request_actor_ctx.reset(actor_token)


def _collector_mark_start(system: str) -> float:
    now_iso = datetime.now(UTC).isoformat()
    status = collector_status.setdefault(system, {})
    status["in_progress"] = True
    status["last_started_at"] = now_iso
    return monotonic()


def _collector_mark_finish(
    system: str,
    started_monotonic: float,
    *,
    error: Exception | None = None,
    round_result: dict[str, object] | None = None,
) -> None:
    now_iso = datetime.now(UTC).isoformat()
    status = collector_status.setdefault(system, {})
    status["in_progress"] = False
    status["last_finished_at"] = now_iso
    status["last_duration_ms"] = round((monotonic() - started_monotonic) * 1000, 1)
    if error is None and bool((round_result or {}).get("stored_sample")):
        status["last_success_at"] = now_iso
        status["last_error"] = None
        status["success_count"] = int(status.get("success_count") or 0) + 1
        return
    status["last_error_at"] = now_iso
    if error is not None:
        status["last_error"] = f"{type(error).__name__}: {error}"
    else:
        reason = str((round_result or {}).get("reason") or (round_result or {}).get("error") or "sample_not_stored")
        status["last_error"] = reason
    status["failure_count"] = int(status.get("failure_count") or 0) + 1


def _worker_collection_log_outcome(round_result: dict[str, object]) -> tuple[str, bool, str | None]:
    if bool(round_result.get("stored_sample")):
        return WORKER_LOG_STATUS_OK, True, None
    if round_result.get("error"):
        return WORKER_LOG_STATUS_FAILED, False, str(round_result.get("error") or "")
    reason = str(round_result.get("reason") or "").strip()
    if reason in {"missing_required_config", "endpoint_backoff", "source_flow_unavailable"}:
        return WORKER_LOG_STATUS_SKIPPED, False, reason or None
    return WORKER_LOG_STATUS_FAILED, False, reason or "sample_not_stored"


def _collector_sleep_seconds(now_monotonic: float) -> float:
    return COLLECTOR_ROUND_SLEEP_SECONDS


async def _await_round_request_log_settlement(
    round_id: str,
    *,
    system: str,
    services: tuple[str, ...],
    settle_timeout_seconds: float = 2.0,
    poll_interval_seconds: float = 0.05,
    timeout_error_text: str,
) -> int:
    deadline = monotonic() + max(0.0, settle_timeout_seconds)
    while monotonic() < deadline:
        pending_count = await _run_db_call_with_retry(
            count_pending_worker_api_logs,
            storage_db_path,
            operation_name="worker_log_settlement_count",
            trace_system=system,
            trace_round_id=round_id,
            extra_failure_context={"services": ",".join(services)},
            round_id=round_id,
            services=services,
        )
        if pending_count <= 0:
            _clear_persistence_failure(system)
            return 0
        await asyncio.sleep(poll_interval_seconds)
    result = await _run_db_call_with_retry(
        finalize_pending_worker_api_logs_for_round,
        storage_db_path,
        operation_name="worker_log_settlement_finalize",
        trace_system=system,
        trace_round_id=round_id,
        extra_failure_context={"services": ",".join(services)},
        round_id=round_id,
        services=services,
        status=WORKER_LOG_STATUS_TIMEOUT,
        error_text=timeout_error_text,
    )
    _clear_persistence_failure(system)
    return int(result)


def _fmt_result_number(value: object, digits: int = 1, unit: str | None = None) -> str:
    num = _to_number(value)
    if num is None:
        return "-"
    if float(num).is_integer() and digits <= 0:
        out = str(int(num))
    else:
        out = f"{num:.{digits}f}".rstrip("0").rstrip(".")
    return f"{out}{unit or ''}"


def _worker_control_result_text(
    *,
    service: str,
    status: str,
    payload: dict[str, object],
    error_text: str | None,
) -> str:
    if service == TESLA_OBSERVATION_SERVICE:
        observation = payload.get("observation")
        observation_map = observation if isinstance(observation, dict) else {}
        battery = observation_map.get("battery")
        battery_map = battery if isinstance(battery, dict) else {}
        charging = observation_map.get("charging")
        charging_map = charging if isinstance(charging, dict) else {}
        if status == "skipped":
            return f"Tesla HA data collection skipped: {payload.get('skipped') or error_text or 'unavailable'}"
        return (
            "Tesla HA data collected: "
            f"battery {_fmt_result_number(battery_map.get('level_percent'), 0, '%')}, "
            f"tesla {charging_map.get('connection_state') or '-'} "
            f"(request {'on' if charging_map.get('requested_enabled') else 'off'}), "
            f"status {charging_map.get('status_text') or '-'}, "
            f"current {_fmt_result_number(charging_map.get('current_amps'), 0, 'A')}, "
            f"configured {_fmt_result_number(charging_map.get('configured_current_amps'), 0, 'A')}, "
            f"voltage {_fmt_result_number(charging_map.get('voltage_v'), 0, 'V')}, "
            f"power {_fmt_result_number(charging_map.get('power_w'), 1, 'W')}"
        )
    if service in {SAJ_COLLECTION_SERVICE, SOLPLANET_COLLECTION_SERVICE}:
        attempted = [str(item) for item in payload.get("attempted", []) if item]
        succeeded = [str(item) for item in payload.get("succeeded", []) if item]
        failed = [str(item) for item in payload.get("failed", []) if item]
        skipped = [str(item) for item in payload.get("skipped", []) if item]
        stored_sample = bool(payload.get("stored_sample"))
        reason = str(payload.get("reason") or error_text or "-")
        summary = (
            f"attempted={len(attempted)}, succeeded={len(succeeded)}, "
            f"failed={len(failed)}, skipped={len(skipped)}, stored_sample={stored_sample}"
        )
        if status == WORKER_LOG_STATUS_OK:
            return f"Collection stored new sample: {summary}"
        if reason == "ha_entities_stale":
            age_s = payload.get("ha_entity_age_seconds")
            last_upd = str(payload.get("ha_entity_last_updated") or "-")
            return (
                f"Collection did not store new sample: {summary}, reason={reason}"
                f"; entity_age={age_s}s, last_updated={last_upd}"
            )
        return f"Collection did not store new sample: {summary}, reason={reason}"
    if service == NOTIFICATION_SUMMARY_SERVICE and str(payload.get("record_kind") or "") == "notification":
        window_mode = _worker_window_name(payload)
        window_schedule = _worker_window_label(payload)
        notification = _worker_primary_notification(payload)
        if status == WORKER_LOG_STATUS_SEND and notification:
            code = str(notification.get("code") or "unknown").strip()
            trigger = _worker_notification_trigger_text(notification)
            return (
                f"Notification {status}: "
                f"window={window_mode}, schedule={window_schedule}, "
                f"reason={code}, trigger={trigger}"
            )
        return (
            f"Notification {status}: "
            f"window={window_mode}, schedule={window_schedule}, "
            f"reason={error_text or 'no_notification_triggered'}"
        )
    if service == OPERATION_SUMMARY_SERVICE and str(payload.get("record_kind") or "") == "operation":
        window_mode = _worker_window_name(payload)
        window_schedule = _worker_window_label(payload)
        detail = _worker_operation_detail_text(payload)
        decision_reason = str(payload.get("decision_reason") or "-")
        checks = payload.get("checks")
        check_list = [item for item in checks if isinstance(item, dict)] if isinstance(checks, list) else []
        check_parts: list[str] = []
        for check in check_list:
            check_name = str(check.get("check") or "check")
            why_not_applied = str(check.get("why_not_applied") or "").strip()
            action = check.get("action")
            action_map = action if isinstance(action, dict) else {}
            decision = check.get("decision")
            decision_map = decision if isinstance(decision, dict) else {}
            detail_parts = []
            if why_not_applied:
                detail_parts.append(f"why={why_not_applied}")
            if decision_map.get("mode") is not None:
                detail_parts.append(f"mode={decision_map.get('mode')}")
            if decision_map.get("charge_current_amps") is not None:
                detail_parts.append(f"target={_fmt_result_number(decision_map.get('charge_current_amps'), 0, 'A')}")
            details = check.get("details")
            details_map = details if isinstance(details, dict) else {}
            if details_map.get("current_grid_import_w") is not None:
                detail_parts.append(f"grid={_fmt_result_number(details_map.get('current_grid_import_w'), 0, 'W')}")
            if details_map.get("available_current_options_amps") is not None:
                options = details_map.get("available_current_options_amps")
                if isinstance(options, list) and options:
                    detail_parts.append(f"options={','.join(str(int(item)) for item in options if item is not None)}A")
            if details_map.get("battery2_soc_percent") is not None:
                detail_parts.append(f"solplanet_soc={_fmt_result_number(details_map.get('battery2_soc_percent'), 0, '%')}")
            action_reason = str(action_map.get("reason") or "").strip()
            if action_reason and action_reason != why_not_applied:
                detail_parts.append(f"action_reason={action_reason}")
            check_parts.append(f"{check_name}({', '.join(detail_parts)})" if detail_parts else check_name)
        if status == WORKER_LOG_STATUS_APPLIED:
            return (
                f"Operation {status}: "
                f"window={window_mode}, schedule={window_schedule}, "
                f"action={detail}, reason={decision_reason}"
            )
        return (
            f"Operation {status}: "
            f"window={window_mode}, schedule={window_schedule}, "
            f"reason={decision_reason if decision_reason != '-' else detail}"
            + (f"; checks={' | '.join(check_parts)}" if check_parts else "")
        )
    if service == OVERNIGHT_SHOULDER_SERVICE:
        service_window_schedule = str(payload.get("service_window_schedule") or _window_schedule_text("overnight_shoulder"))
        if status == "outside_window":
            return (
                "Overnight shoulder outside window: "
                f"service={service}, schedule={service_window_schedule}, "
                f"current_window={payload.get('window_mode') or 'off'}"
            )
        notifications = payload.get("notifications")
        notification_list = [item for item in notifications if isinstance(item, dict)] if isinstance(notifications, list) else []
        if not notification_list:
            notification = payload.get("notification")
            if isinstance(notification, dict):
                notification_list = [notification]
        extra_parts = []
        for notification_map in notification_list:
            code = str(notification_map.get("code") or "warning")
            if code in ("saj_battery_watch_50_percent", "saj_battery_watch_20_percent"):
                extra_parts.append(
                    "notification "
                    f"{code}: "
                    f"saj_soc={_fmt_result_number(notification_map.get('current_soc_percent'), 0, '%')}, "
                    f"threshold={_fmt_result_number(notification_map.get('threshold_soc_percent'), 0, '%')}"
                )
                continue
            extra_parts.append(f"notification {code}")
        return (
            f"Overnight shoulder {'skipped' if status == 'skipped' else status}: "
            f"schedule={service_window_schedule}, active={'yes' if payload.get('window_active') else 'no'}, "
            f"saj_soc={_fmt_result_number(payload.get('saj_soc_percent'), 0, '%')}, "
            f"window_key={payload.get('window_key') or '-'}"
            + (f"; {'; '.join(extra_parts)}" if extra_parts else "")
            + (
                f"; skipped={payload.get('skipped') or error_text or 'unavailable'}"
                if status == "skipped"
                else ""
            )
        )
    if service in (
        WINDOW_CHECK_FREE_ENERGY_SERVICE,
        WINDOW_CHECK_AFTER_FREE_SHOULDER_SERVICE,
        WINDOW_CHECK_AFTER_FREE_PEAK_SERVICE,
        WINDOW_CHECK_EXPORT_WINDOW_SERVICE,
        WINDOW_CHECK_POST_EXPORT_PEAK_SERVICE,
    ):
        service_window_schedule = str(payload.get("service_window_schedule") or _window_schedule_text_for_service(service))
        if status == "outside_window":
            return (
                f"Worker window check outside window: "
                f"service={service}, schedule={service_window_schedule}, "
                f"current_window={payload.get('window_mode') or 'off'}"
            )
        action = payload.get("action")
        action_map = action if isinstance(action, dict) else {}
        decision = payload.get("decision")
        decision_map = decision if isinstance(decision, dict) else {}
        tesla_before = payload.get("tesla_state_before")
        tesla_before_map = tesla_before if isinstance(tesla_before, dict) else {}
        export_window = payload.get("export_window")
        export_window_map = export_window if isinstance(export_window, dict) else {}
        after_free_shoulder = payload.get("after_free_shoulder")
        after_free_shoulder_map = after_free_shoulder if isinstance(after_free_shoulder, dict) else {}
        after_free_peak = payload.get("after_free_peak")
        after_free_peak_map = after_free_peak if isinstance(after_free_peak, dict) else {}
        post_export_peak = payload.get("post_export_peak")
        post_export_peak_map = post_export_peak if isinstance(post_export_peak, dict) else {}
        notifications = payload.get("notifications")
        notification_list = [item for item in notifications if isinstance(item, dict)] if isinstance(notifications, list) else []
        if not notification_list:
            notification = payload.get("notification")
            if isinstance(notification, dict):
                notification_list = [notification]
        window_mode = str(payload.get("window_mode") or "off")
        window_logic = {
            "free_energy": "free_energy",
            "after_free_shoulder": "after_free_shoulder",
            "after_free_peak": "after_free_peak",
            "export_window": "export_window",
            "post_export_peak": "post_export_peak",
            "off": "outside_window",
        }.get(window_mode, window_mode or "unknown")
        action_type = str(action_map.get("type") or "-")
        steps = action_map.get("steps")
        steps_text = ", ".join(str(item) for item in steps) if isinstance(steps, list) and steps else str(action_map.get("reason") or "-")
        steps_set = {str(item) for item in steps} if isinstance(steps, list) else set()
        action_label = action_type
        if "restart_charging_for_current_mismatch" in steps_set or "restart_charging" in steps_set:
            action_label = "restart"
        elif "start_charging" in steps_set:
            action_label = "start"
        elif action_type == "stop_charging":
            action_label = "stop"
        elif any(str(item).startswith("set_current_") for item in steps_set):
            action_label = "set_current"
        elif action_type == "noop":
            action_label = "noop"
        decision_mode = str(decision_map.get("mode") or "-")
        decision_amps = _fmt_result_number(decision_map.get("charge_current_amps"), 0, "A")
        tesla_before_text = str(tesla_before_map.get("connection_state") or ("charging" if tesla_before_map.get("enabled") else "unplugged"))
        tesla_before_request = "on" if tesla_before_map.get("requested_enabled") else "off"
        tesla_before_current = _fmt_result_number(tesla_before_map.get("current_amps"), 0, "A")
        tesla_before_configured = _fmt_result_number(tesla_before_map.get("configured_current_amps"), 0, "A")
        tesla_before_power = _fmt_result_number(tesla_before_map.get("power_w"), 0, "W")
        tesla_before_status = str(tesla_before_map.get("status_text") or "-")
        tesla_after_text = tesla_before_text
        tesla_after_request = tesla_before_request
        tesla_after_current = decision_amps if decision_mode != "off" else "0A"
        tesla_after_configured = decision_amps if decision_mode != "off" else tesla_before_configured
        if action_type == "stop_charging":
            tesla_after_text = "plugged_not_charging" if tesla_before_map.get("cable_connected") else "unplugged"
            tesla_after_request = "off"
        elif action_type == "apply":
            if "start_charging" in steps_set or "restart_charging" in steps_set:
                tesla_after_text = "charging"
                tesla_after_request = "on"
            if "restart_charging_for_current_mismatch" in steps_set:
                tesla_after_text = "charging"
                tesla_after_request = "on"
            if any(str(item).startswith("set_current_") for item in steps_set):
                tesla_after_configured = decision_amps
        elif action_type == "noop" and decision_mode == "hold_current_state":
            tesla_after_current = _fmt_result_number(
                tesla_before_map.get("current_amps") if tesla_before_map.get("current_amps") is not None else decision_map.get("charge_current_amps"),
                0,
                "A",
            )
            tesla_after_configured = _fmt_result_number(
                tesla_before_map.get("configured_current_amps") if tesla_before_map.get("configured_current_amps") is not None else decision_map.get("charge_current_amps"),
                0,
                "A",
            )
        time_window_text = (
            "time_window "
            f"mode={window_mode}, schedule={payload.get('window_schedule') or '-'}, "
            f"logic={window_logic}, active={'yes' if payload.get('window_active') else 'no'}, "
            f"tariff={_fmt_result_number(payload.get('window_tariff_p_per_kwh'), 1, 'p/kWh')}"
        )
        tesla_text = (
            f"tesla now state={tesla_before_text}, request={tesla_before_request}, status={tesla_before_status}, "
            f"actual_current={tesla_before_current}, configured_current={tesla_before_configured}, power={tesla_before_power}"
        )
        battery_context_map = export_window_map
        if not battery_context_map:
            battery_context_map = after_free_shoulder_map
        if not battery_context_map:
            battery_context_map = after_free_peak_map
        battery_text = (
            "batteries "
            f"saj_soc={_fmt_result_number(export_window_map.get('saj_soc_percent'), 0, '%')}, "
            f"solplanet_soc={_fmt_result_number(battery_context_map.get('solplanet_soc_percent'), 0, '%')}, "
            f"solplanet_available_capacity={_fmt_result_number(battery_context_map.get('solplanet_available_capacity_kwh'), 1, 'kWh')}, "
            f"start_threshold={_fmt_result_number(battery_context_map.get('start_soc_percent'), 0, '%')}, "
            f"stop_threshold={_fmt_result_number(battery_context_map.get('stop_soc_percent'), 0, '%')}"
        )
        if post_export_peak_map:
            battery_text = (
                "batteries "
                f"solplanet_soc={_fmt_result_number(post_export_peak_map.get('solplanet_soc_percent'), 0, '%')}, "
                f"low_battery_alarm_threshold={_fmt_result_number(post_export_peak_map.get('low_battery_alarm_threshold_soc_percent'), 0, '%')}"
            )
        elif not battery_context_map:
            battery_text = (
                "batteries "
                f"saj_soc={_fmt_result_number(payload.get('battery1_soc_percent'), 0, '%')}, "
                f"solplanet_soc={_fmt_result_number(payload.get('battery2_soc_percent'), 0, '%')}, "
                f"solplanet_available_capacity={_fmt_result_number(payload.get('battery2_energy_kwh'), 1, 'kWh')}"
            )
        decision_text = (
            f"decision mode={decision_mode}, target_current={decision_amps}, action={action_label}, "
            f"reason={payload.get('decision_reason') or '-'}, predicted_grid={_fmt_result_number(decision_map.get('predicted_grid_w'), 0, 'W')}"
        )
        extra_parts = []
        if payload.get("current_grid_import_w") is not None or payload.get("current_grid_export_w") is not None:
            extra_parts.append(
                f"grid import={_fmt_result_number(payload.get('current_grid_import_w'), 0, 'W')}, "
                f"export={_fmt_result_number(payload.get('current_grid_export_w'), 0, 'W')}, "
                f"base={_fmt_result_number(payload.get('base_grid_without_tesla_w'), 0, 'W')}"
            )
        if export_window_map:
            extra_parts.append(
                f"solar pv={_fmt_result_number(export_window_map.get('pv_w'), 0, 'W')}, "
                f"load={_fmt_result_number(export_window_map.get('load_w'), 0, 'W')}, "
                f"excess={_fmt_result_number(export_window_map.get('solar_excess_vs_load_w'), 0, 'W')}, "
                f"export_signal={'yes' if export_window_map.get('export_signal_active') else 'no'}, "
                f"solar_signal={'yes' if export_window_map.get('solar_signal_active') else 'no'}"
            )
        export_tracking = payload.get("solar_surplus_export_tracking")
        export_tracking_map = export_tracking if isinstance(export_tracking, dict) else {}
        if export_tracking_map:
            thresholds_kwh = export_tracking_map.get("thresholds_kwh")
            threshold_text = (
                ", ".join(_fmt_result_number(value, 3, "kWh") for value in thresholds_kwh)
                if isinstance(thresholds_kwh, list) and thresholds_kwh
                else _fmt_result_number(export_tracking_map.get("threshold_kwh"), 3, "kWh")
            )
            extra_parts.append(
                "window_export "
                f"total={_fmt_result_number(export_tracking_map.get('total_export_kwh'), 3, 'kWh')}, "
                f"added={_fmt_result_number(export_tracking_map.get('added_export_wh'), 0, 'Wh')}, "
                f"threshold={threshold_text}"
            )
        if steps_text and steps_text != "-":
            extra_parts.append(f"detail {action_type} ({steps_text})")
        for notification_map in notification_list:
            code = str(notification_map.get("code") or "warning")
            if code == "solplanet_low_battery":
                extra_parts.append(
                    "notification "
                    f"{code}: "
                    f"solplanet_soc={_fmt_result_number(notification_map.get('current_soc_percent'), 0, '%')}, "
                    f"threshold={_fmt_result_number(notification_map.get('threshold_soc_percent'), 0, '%')}"
                )
                continue
            if code == "solplanet_low_battery_post_export_peak":
                extra_parts.append(
                    "notification "
                    f"{code}: "
                    f"solplanet_soc={_fmt_result_number(notification_map.get('current_soc_percent'), 0, '%')}, "
                    f"threshold={_fmt_result_number(notification_map.get('threshold_soc_percent'), 0, '%')}, "
                    f"tariff={_fmt_result_number(notification_map.get('tariff_p_per_kwh'), 1, 'p/kWh')}"
                )
                continue
            if code == "solplanet_low_available_capacity":
                extra_parts.append(
                    "notification "
                    f"{code}: "
                    f"solplanet_available_capacity={_fmt_result_number(notification_map.get('current_capacity_kwh'), 1, 'kWh')}, "
                    f"threshold={_fmt_result_number(notification_map.get('threshold_kwh'), 1, 'kWh')}"
                )
                continue
            if code == "grid_import_started":
                extra_parts.append(
                    "notification "
                    f"{code}: "
                    f"grid_import={_fmt_result_number(notification_map.get('current_grid_import_w'), 0, 'W')}"
                )
                continue
            if code == "grid_import_started_post_export_peak":
                extra_parts.append(
                    "notification "
                    f"{code}: "
                    f"grid_import={_fmt_result_number(notification_map.get('current_grid_import_w'), 0, 'W')}, "
                    f"tariff={_fmt_result_number(notification_map.get('tariff_p_per_kwh'), 1, 'p/kWh')}"
                )
                continue
            if _is_solar_surplus_export_energy_notification_code(code):
                extra_parts.append(
                    "notification "
                    f"{code}: "
                    f"window_export={_fmt_result_number(notification_map.get('current_export_total_kwh'), 3, 'kWh')}, "
                    f"threshold={_fmt_result_number(notification_map.get('threshold_kwh'), 3, 'kWh')}"
                )
                continue
            extra_parts.append(f"notification {code}")
        return (
            f"Worker window check {'skipped' if status == 'skipped' else status}: "
            f"{time_window_text}; "
            f"{tesla_text}; "
            f"{battery_text}; "
            f"{decision_text}; "
            f"expected state={tesla_after_text}, request={tesla_after_request}, "
            f"actual_current={tesla_after_current}, configured_current={tesla_after_configured}"
            + (f"; {'; '.join(extra_parts)}" if extra_parts else "")
            + (
                f"; skipped={payload.get('skipped') or error_text or 'unavailable'}"
                if status == "skipped"
                else ""
            )
        )
    if service == "combined_assembly":
        combined_result = payload.get("combined")
        combined_map = combined_result if isinstance(combined_result, dict) else {}
        combined_key_metrics = combined_map.get("key_metrics")
        combined_km = combined_key_metrics if isinstance(combined_key_metrics, dict) else {}
        logged_at_utc = str(payload.get("combined_logged_at_utc") or "").strip()
        next_due_at_utc = str(payload.get("next_worker_round_due_at_utc") or "").strip()
        schedule_suffix = ""
        if logged_at_utc or next_due_at_utc:
            schedule_suffix = (
                f"; logged_at_utc={logged_at_utc or '-'}"
                f"; next_worker_round_due_at_utc={next_due_at_utc or '-'}"
            )
        if status == "failed":
            return f"Combined assembly failed: {combined_map.get('reason') or error_text or '-'}{schedule_suffix}"
        if status == "skipped":
            return f"Combined assembly skipped: {combined_map.get('reason') or error_text or '-'}{schedule_suffix}"
        return (
            "Combined assembly stored: "
            f"grid {_fmt_result_number(combined_km.get('grid_w'), 0, 'W')}, "
            f"load {_fmt_result_number(combined_km.get('load_w'), 0, 'W')}, "
            f"pv {_fmt_result_number(combined_km.get('pv_w'), 0, 'W')}, "
            f"battery {_fmt_result_number(combined_km.get('battery_w'), 0, 'W')}"
            f"{schedule_suffix}"
        )
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)


async def _persist_worker_control_log(
    *,
    round_id: str | None = None,
    system: str,
    service: str,
    requested_at_utc: str,
    started_monotonic: float,
    ok: bool,
    status: str,
    payload: dict[str, object],
    error_text: str | None = None,
) -> int | None:
    normalized_round_id = str(round_id or "").strip() or str(request_round_ctx.get() or "").strip()
    api_link = f"worker://{system}/{service}"
    request_token = _worker_log_request_token(normalized_round_id, service, "AUTO", api_link)
    result_text = _worker_control_result_text(service=service, status=status, payload=payload, error_text=error_text)
    payload_json = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    try:
        if request_token:
            log_id = await asyncio.to_thread(
                update_worker_api_log,
                storage_db_path,
                request_token=request_token,
                ok=ok,
                status=status,
                status_code=None,
                duration_ms=round((monotonic() - started_monotonic) * 1000, 1),
                result_text=result_text,
                error_text=error_text,
                payload_json=payload_json,
            )
            return log_id
        log_id = await asyncio.to_thread(
            insert_worker_api_log,
            storage_db_path,
            request_token=request_token or None,
            round_id=normalized_round_id or None,
            worker="worker",
            system=system,
            service=service,
            method="AUTO",
            api_link=api_link,
            requested_at_utc=requested_at_utc,
            ok=ok,
            status=status,
            status_code=None,
            duration_ms=round((monotonic() - started_monotonic) * 1000, 1),
            result_text=result_text,
            error_text=error_text,
            payload_json=payload_json,
        )
        return log_id
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to persist worker control log for %s/%s: %s", system, service, exc)
        return None


async def _persist_worker_summary_logs(
    *,
    round_id: str | None,
    requested_at_utc: str,
    started_monotonic: float,
    midday_result: dict[str, object] | None,
    saj_watch_result: dict[str, object] | None,
    battery_full_result: dict[str, object] | None,
) -> None:
    source = _select_worker_summary_source(midday_result, saj_watch_result, battery_full_result)
    all_notifications = _collect_worker_notifications(midday_result, saj_watch_result, battery_full_result)
    all_actions = _collect_worker_actions(midday_result, saj_watch_result, battery_full_result)
    all_decision_reasons = [
        str(item.get("decision_reason") or "").strip()
        for item in (midday_result, saj_watch_result, battery_full_result)
        if isinstance(item, dict) and str(item.get("decision_reason") or "").strip()
    ]
    notification_status, notification_ok, notification_payload, notification_error_text = _build_notification_summary_payload(
        source,
        notifications=all_notifications,
    )
    notification_log_id = await _persist_worker_control_log(
        round_id=round_id,
        system=NOTIFICATION_LOG_SYSTEM,
        service=NOTIFICATION_SUMMARY_SERVICE,
        requested_at_utc=requested_at_utc,
        started_monotonic=started_monotonic,
        ok=notification_ok,
        status=notification_status,
        payload=notification_payload,
        error_text=notification_error_text,
    )
    if notification_status == WORKER_LOG_STATUS_SEND:
        notification_entities = [
            _notification_entity_from_payload(item)
            for item in _worker_notification_list(notification_payload)
            if isinstance(item, dict)
        ]
        if notification_entities:
            await asyncio.to_thread(
                upsert_notification_entries,
                storage_db_path,
                notifications=notification_entities,
                requested_at_utc=requested_at_utc,
                source_system=NOTIFICATION_LOG_SYSTEM,
                source_service=NOTIFICATION_SUMMARY_SERVICE,
                log_id=notification_log_id,
            )
    operation_status, operation_ok, operation_payload, operation_error_text = _build_operation_summary_payload(
        source,
        actions=all_actions,
        check_sources=[
            item
            for item in (midday_result, saj_watch_result, battery_full_result)
            if isinstance(item, dict)
        ],
    )
    operation_payload["decision_reasons"] = all_decision_reasons
    if all_decision_reasons and not operation_payload.get("decision_reason"):
        operation_payload["decision_reason"] = all_decision_reasons[-1]
    await _persist_worker_control_log(
        round_id=round_id,
        system=OPERATION_LOG_SYSTEM,
        service=OPERATION_SUMMARY_SERVICE,
        requested_at_utc=requested_at_utc,
        started_monotonic=started_monotonic,
        ok=operation_ok,
        status=operation_status,
        payload=operation_payload,
        error_text=operation_error_text,
    )


async def _collector_loop() -> None:
    global collector_round_number  # noqa: PLW0603
    while not collector_stop_event.is_set():
        collector_round_number += 1
        round_number = collector_round_number
        round_started_at_utc = datetime.now(UTC).isoformat()
        round_id = f"round-{round_number}-{int(datetime.fromisoformat(round_started_at_utc).timestamp() * 1000)}"
        try:
            await _insert_worker_round_log_plan(round_number, round_id, round_started_at_utc)
            _clear_persistence_failure("combined")
        except Exception as exc:  # noqa: BLE001
            _record_persistence_failure(
                "combined",
                operation_name="worker_round_log_plan",
                error=exc,
                round_id=round_id,
            )
        round_tasks: dict[str, asyncio.Task[dict[str, object]]] = {}
        started_monotonic_by_system: dict[str, float] = {}
        for system in POLLED_SYSTEMS:
            started_monotonic = _collector_mark_start(system)
            collector_status.setdefault(system, {})["round_number"] = round_number
            collector_status.setdefault(system, {})["round_id"] = round_id
            started_monotonic_by_system[system] = started_monotonic
            round_tasks[system] = asyncio.create_task(
                _collect_for_system(
                    system,
                    round_number=round_number,
                    round_id=round_id,
                    round_started_at_utc=round_started_at_utc,
                )
            )

        round_results: dict[str, dict[str, object]] = {}
        for system in POLLED_SYSTEMS:
            started_monotonic = started_monotonic_by_system[system]
            try:
                result = await round_tasks[system]
                round_results[system] = result
                _collector_mark_finish(system, started_monotonic, round_result=result)
                summary_service = SAJ_COLLECTION_SERVICE if system == "saj" else SOLPLANET_COLLECTION_SERVICE
                summary_status, summary_ok, summary_error_text = _worker_collection_log_outcome(result)
                await _persist_worker_control_log(
                    round_id=round_id,
                    system=system,
                    service=summary_service,
                    requested_at_utc=round_started_at_utc,
                    started_monotonic=started_monotonic,
                    ok=summary_ok,
                    status=summary_status,
                    payload=result,
                    error_text=summary_error_text,
                )
            except Exception as exc:  # noqa: BLE001
                _append_worker_failure_log(
                    system,
                    stage="collector_round",
                    error=exc,
                    started_monotonic=started_monotonic,
                )
                _collector_mark_finish(system, started_monotonic, error=exc)
                round_results[system] = {
                    "attempted": [],
                    "succeeded": [],
                    "failed": [],
                    "skipped": [],
                    "stored_sample": False,
                    "error": f"{type(exc).__name__}: {exc}",
                }
                summary_service = SAJ_COLLECTION_SERVICE if system == "saj" else SOLPLANET_COLLECTION_SERVICE
                await _persist_worker_control_log(
                    round_id=round_id,
                    system=system,
                    service=summary_service,
                    requested_at_utc=round_started_at_utc,
                    started_monotonic=started_monotonic,
                    ok=False,
                    status=WORKER_LOG_STATUS_FAILED,
                    payload=round_results[system],
                    error_text=str(round_results[system].get("error") or ""),
                )
                if system == "solplanet":
                    try:
                        await _recreate_solplanet_client()
                    except Exception as recreate_exc:  # noqa: BLE001
                        _append_worker_failure_log(
                            "solplanet",
                            stage="solplanet_client_recreate",
                            error=recreate_exc,
                        )
                        status = collector_status.setdefault("solplanet", {})
                        status["client_recreate_error_at"] = datetime.now(UTC).isoformat()
                        status["client_recreate_error"] = f"{type(recreate_exc).__name__}: {recreate_exc}"

        try:
            source_pending_timeouts = await _await_round_request_log_settlement(
                round_id,
                system="combined",
                services=("home_assistant", "solplanet_cgi"),
                timeout_error_text="worker_source_request_timeout",
            )
            if source_pending_timeouts:
                collector_status.setdefault("combined", {})["last_source_pending_timeout_count"] = source_pending_timeouts
        except Exception as exc:  # noqa: BLE001
            _record_persistence_failure(
                "combined",
                operation_name="worker_log_settlement_source",
                error=exc,
                round_id=round_id,
            )

        combined_started_monotonic = _collector_mark_start("combined")
        combined_requested_at_utc = round_started_at_utc
        collector_status.setdefault("combined", {})["round_number"] = round_number
        collector_status.setdefault("combined", {})["round_id"] = round_id
        saj_result: dict[str, object] = {}
        solplanet_result: dict[str, object] = {}
        tesla_observation_result: dict[str, object] | None = None
        try:
            tesla_observation_result = await _run_tesla_home_assistant_collection(
                round_id=round_id,
                requested_at_utc=round_started_at_utc,
            )
            collector_status.setdefault("combined", {})["last_tesla_grid_support"] = tesla_observation_result
        except Exception as control_exc:  # noqa: BLE001
            _append_worker_failure_log(
                "combined",
                stage=TESLA_OBSERVATION_SERVICE,
                error=control_exc,
            )
            collector_status.setdefault("combined", {})["last_tesla_grid_support"] = {
                "error": f"{type(control_exc).__name__}: {control_exc}",
                "executed_at": datetime.now(UTC).isoformat(),
            }
        try:
            saj_result = round_results.get("saj") or {}
            solplanet_result = round_results.get("solplanet") or {}
            saj_stored = bool(saj_result.get("stored_sample"))
            solplanet_stored = bool(solplanet_result.get("stored_sample"))
            saj_flow = saj_result.get("flow") if saj_stored else None
            solplanet_flow = solplanet_result.get("flow") if solplanet_stored else None
            if saj_flow and solplanet_flow:
                combined_flow = _build_combined_flow_payload(saj_flow, solplanet_flow, tesla_observation_result)
                combined_source = combined_flow.get("source")
                combined_source_map = combined_source if isinstance(combined_source, dict) else {}
                combined_source_map["source_details"] = {
                    "saj": {"origin": "current_round", "updated_at": saj_flow.get("updated_at")},
                    "solplanet": {"origin": "current_round", "updated_at": solplanet_flow.get("updated_at")},
                }
                combined_flow["source"] = combined_source_map
                missing_metrics = _missing_required_flow_metrics("combined", combined_flow)
                if missing_metrics:
                    round_results["combined"] = {
                        "attempted": ["combined_assembly"],
                        "succeeded": [],
                        "failed": ["combined_assembly"],
                        "skipped": [],
                        "stored_sample": False,
                        "flow": None,
                        "reason": "incomplete_flow",
                        "missing_metrics": missing_metrics,
                    }
                    await _persist_worker_control_log(
                        round_id=round_id,
                        system="combined",
                        service="combined_assembly",
                        requested_at_utc=combined_requested_at_utc,
                        started_monotonic=combined_started_monotonic,
                        ok=False,
                        status="failed",
                        payload=_combined_assembly_log_payload(
                            round_number=round_number,
                            round_id=round_id,
                            saj_result=saj_result,
                            solplanet_result=solplanet_result,
                            combined_result=round_results["combined"],
                        ),
                        error_text=f"missing_metrics: {', '.join(str(item) for item in missing_metrics)}",
                    )
                else:
                    await _store_flow_sample("combined", combined_flow, round_id=round_id)
                    round_results["combined"] = {
                        "attempted": ["combined_assembly"],
                        "succeeded": ["combined_assembly"],
                        "failed": [],
                        "skipped": [],
                        "stored_sample": True,
                        "flow": combined_flow,
                    }
                    await _persist_worker_control_log(
                        round_id=round_id,
                        system="combined",
                        service="combined_assembly",
                        requested_at_utc=combined_requested_at_utc,
                        started_monotonic=combined_started_monotonic,
                        ok=True,
                        status="applied",
                        payload=_combined_assembly_log_payload(
                            round_number=round_number,
                            round_id=round_id,
                            saj_result=saj_result,
                            solplanet_result=solplanet_result,
                            combined_result=round_results["combined"],
                        ),
                    )
            else:
                combined_reason = (
                    f"saj={'ok' if saj_stored else str(saj_result.get('reason') or 'not_stored')}; "
                    f"solplanet={'ok' if solplanet_stored else str(solplanet_result.get('reason') or 'not_stored')}"
                )
                round_results["combined"] = {
                    "attempted": ["combined_assembly"],
                    "succeeded": [],
                    "failed": [],
                    "skipped": ["combined_assembly"],
                    "stored_sample": False,
                    "flow": None,
                    "reason": "source_not_stored",
                    "source_details": {
                        "saj": {"stored_sample": saj_stored, "reason": saj_result.get("reason")},
                        "solplanet": {"stored_sample": solplanet_stored, "reason": solplanet_result.get("reason")},
                    },
                }
                await _persist_worker_control_log(
                    round_id=round_id,
                    system="combined",
                    service="combined_assembly",
                    requested_at_utc=combined_requested_at_utc,
                    started_monotonic=combined_started_monotonic,
                    ok=False,
                    status="skipped",
                    payload=_combined_assembly_log_payload(
                        round_number=round_number,
                        round_id=round_id,
                        saj_result=saj_result,
                        solplanet_result=solplanet_result,
                        combined_result=round_results["combined"],
                    ),
                    error_text=combined_reason,
                )
            _collector_mark_finish("combined", combined_started_monotonic)
        except Exception as exc:  # noqa: BLE001
            _append_worker_failure_log(
                "combined",
                stage="collector_round",
                error=exc,
                started_monotonic=combined_started_monotonic,
            )
            _collector_mark_finish("combined", combined_started_monotonic, error=exc)
            round_results["combined"] = {
                "attempted": ["combined_assembly"],
                "succeeded": [],
                "failed": ["combined_assembly"],
                "skipped": [],
                "stored_sample": False,
                "flow": None,
                "error": f"{type(exc).__name__}: {exc}",
            }
            await _persist_worker_control_log(
                round_id=round_id,
                system="combined",
                service="combined_assembly",
                requested_at_utc=combined_requested_at_utc,
                started_monotonic=combined_started_monotonic,
                ok=False,
                status="failed",
                payload=_combined_assembly_log_payload(
                    round_number=round_number,
                    round_id=round_id,
                    saj_result=saj_result if isinstance(saj_result, dict) else {},
                    solplanet_result=solplanet_result if isinstance(solplanet_result, dict) else {},
                    combined_result=round_results["combined"],
                ),
                error_text=f"{type(exc).__name__}: {exc}",
            )
        try:
            midday_window_result: dict[str, object] | None = None
            saj_battery_watch_result: dict[str, object] | None = None
            battery_full_notification_result: dict[str, object] | None = None
            midday_window_result = await _run_midday_window_check(
                round_results.get("combined", {}).get("flow") if isinstance(round_results.get("combined"), dict) else None,
                tesla_observation_result,
                round_id=round_id,
                requested_at_utc=round_started_at_utc,
            )
            collector_status.setdefault("combined", {})["last_midday_window_check"] = midday_window_result
        except Exception as window_exc:  # noqa: BLE001
            _append_worker_failure_log(
                "combined",
                stage=_window_check_service_name(
                    _tesla_midday_window_mode(_tesla_grid_support_now_local()[0])
                ) or "window_check",
                error=window_exc,
            )
            collector_status.setdefault("combined", {})["last_midday_window_check"] = {
                "error": f"{type(window_exc).__name__}: {window_exc}",
                "executed_at": datetime.now(UTC).isoformat(),
            }

        try:
            saj_battery_watch_result = await _run_saj_battery_watch_check(
                round_results.get("combined", {}).get("flow") if isinstance(round_results.get("combined"), dict) else None,
                round_id=round_id,
                requested_at_utc=round_started_at_utc,
            )
            collector_status.setdefault("combined", {})["last_saj_battery_watch_check"] = saj_battery_watch_result
        except Exception as watch_exc:  # noqa: BLE001
            _append_worker_failure_log(
                "combined",
                stage=OVERNIGHT_SHOULDER_SERVICE,
                error=watch_exc,
            )
            collector_status.setdefault("combined", {})["last_saj_battery_watch_check"] = {
                "error": f"{type(watch_exc).__name__}: {watch_exc}",
                "executed_at": datetime.now(UTC).isoformat(),
            }

        try:
            battery_full_notification_result = await _run_battery_full_notification_check(
                round_results.get("combined", {}).get("flow") if isinstance(round_results.get("combined"), dict) else None,
                round_id=round_id,
                requested_at_utc=round_started_at_utc,
            )
            collector_status.setdefault("combined", {})["last_battery_full_notification_check"] = (
                battery_full_notification_result
            )
        except Exception as battery_full_exc:  # noqa: BLE001
            _append_worker_failure_log(
                "combined",
                stage="battery_full_notification",
                error=battery_full_exc,
            )
            collector_status.setdefault("combined", {})["last_battery_full_notification_check"] = {
                "error": f"{type(battery_full_exc).__name__}: {battery_full_exc}",
                "executed_at": datetime.now(UTC).isoformat(),
            }

        try:
            summary_started_monotonic = monotonic()
            await _persist_worker_summary_logs(
                round_id=round_id,
                requested_at_utc=round_started_at_utc,
                started_monotonic=summary_started_monotonic,
                midday_result=midday_window_result,
                saj_watch_result=saj_battery_watch_result,
                battery_full_result=battery_full_notification_result,
            )
        except Exception as summary_exc:  # noqa: BLE001
            _append_worker_failure_log(
                "combined",
                stage="worker_summary_logs",
                error=summary_exc,
            )

        review = _review_round_results(round_results)
        for system in SUPPORTED_SYSTEMS:
            status = collector_status.setdefault(system, {})
            if system in POLLED_SYSTEMS:
                status["endpoint_backoff"] = _snapshot_endpoint_backoff_state(system)
            system_review = review.get(system)
            if system_review is not None:
                status["last_round_review"] = system_review
            system_result = round_results.get(system)
            if isinstance(system_result, dict):
                status["last_round_result"] = _compact_round_result_for_status(system_result)

        try:
            timed_out_pending = await _await_round_request_log_settlement(
                round_id,
                system="combined",
                services=(
                    SAJ_COLLECTION_SERVICE,
                    SOLPLANET_COLLECTION_SERVICE,
                    "combined_assembly",
                    TESLA_OBSERVATION_SERVICE,
                    NOTIFICATION_SUMMARY_SERVICE,
                    OPERATION_SUMMARY_SERVICE,
                ),
                timeout_error_text="worker_round_incomplete",
            )
            if timed_out_pending:
                collector_status.setdefault("combined", {})["last_round_timeout_count"] = timed_out_pending
        except Exception as exc:  # noqa: BLE001
            _record_persistence_failure(
                "combined",
                operation_name="worker_log_settlement_round",
                error=exc,
                round_id=round_id,
            )

        try:
            await asyncio.wait_for(
                collector_stop_event.wait(),
                timeout=_collector_sleep_seconds(monotonic()),
            )
        except asyncio.TimeoutError:
            continue


async def _start_collector() -> None:
    global collector_task  # noqa: PLW0603
    await asyncio.to_thread(init_db, storage_db_path)
    if collector_task and not collector_task.done():
        return
    for system in POLLED_SYSTEMS:
        collector_next_due_monotonic[system] = 0.0
    collector_stop_event.clear()
    collector_task = asyncio.create_task(_collector_loop())


async def _stop_collector() -> None:
    global collector_task  # noqa: PLW0603
    collector_stop_event.set()
    if collector_task:
        collector_task.cancel()
        try:
            await collector_task
        except asyncio.CancelledError:
            pass
        collector_task = None
    for system in SUPPORTED_SYSTEMS:
        status = collector_status.setdefault(system, {})
        status["in_progress"] = False


async def _restart_collector() -> dict[str, object]:
    async with runtime_lock:
        was_running = bool(collector_task and not collector_task.done())
        await _stop_collector()
        await _start_collector()
    restarted_at = datetime.now(UTC).isoformat()
    return {
        "ok": True,
        "action": "collector_restart",
        "was_running": was_running,
        "running": bool(collector_task and not collector_task.done()),
        "restarted_at": restarted_at,
    }


@app.get("/api/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Weather forecast (Open-Meteo, free, no API key required)
# ---------------------------------------------------------------------------

_weather_cache: dict[str, object] = {}
_WEATHER_CACHE_TTL_SECONDS = 1800  # 30 minutes
_OPEN_METEO_BASE = "https://api.open-meteo.com/v1/forecast"


def _wmo_to_condition(code: int) -> tuple[str, str]:
    """Return (short description, icon emoji) for a WMO weather code."""
    if code == 0:
        return "Clear sky", "☀️"
    if code <= 2:
        return "Partly cloudy", "⛅"
    if code == 3:
        return "Overcast", "☁️"
    if code in (45, 48):
        return "Foggy", "🌫️"
    if 51 <= code <= 57:
        return "Drizzle", "🌦️"
    if 61 <= code <= 67:
        return "Rain", "🌧️"
    if 71 <= code <= 77:
        return "Snow", "❄️"
    if 80 <= code <= 82:
        return "Rain showers", "🌦️"
    if 95 <= code <= 99:
        return "Thunderstorm", "⛈️"
    return "Unknown", "❓"


def _solar_rating(radiation_sum_mj: float | None) -> int:
    """Convert daily shortwave radiation sum (MJ/m²) to 0-100 solar score."""
    if radiation_sum_mj is None:
        return 0
    # ~28 MJ/m² is a very sunny Sydney summer day; cap at 100
    return min(100, round(radiation_sum_mj / 28.0 * 100))


def _weather_apply_time_flags(cached: dict[str, object]) -> dict[str, object]:
    """Recompute is_now/is_past on every request so the 'current hour' is always accurate."""
    tz = str(cached.get("timezone") or "UTC")
    local_tz = ZoneInfo(tz) if tz != "UTC" else UTC
    now_local = datetime.now(local_tz)
    today_str = now_local.strftime("%Y-%m-%d")
    current_hour_str = now_local.strftime("%Y-%m-%dT%H:00")
    cap_hour = (now_local + timedelta(hours=36)).strftime("%Y-%m-%dT%H:00")
    yesterday_start = (now_local - timedelta(days=1)).strftime("%Y-%m-%dT00:00")

    raw_hours: list[dict[str, object]] = list(cached.get("hours") or [])
    hours = [
        {**h, "is_past": str(h.get("time") or "") < current_hour_str, "is_now": str(h.get("time") or "") == current_hour_str}
        for h in raw_hours
        if yesterday_start <= str(h.get("time") or "") <= cap_hour
    ]

    raw_days: list[dict[str, object]] = list(cached.get("days") or [])
    days = [{**d, "is_past": str(d.get("date") or "") < today_str} for d in raw_days]

    return {**cached, "hours": hours, "days": days}


@app.get("/api/weather/forecast")
async def weather_forecast() -> dict[str, object]:
    now_ts = monotonic()
    cached_at = float(_weather_cache.get("cached_at") or 0)
    if _weather_cache and (now_ts - cached_at) < _WEATHER_CACHE_TTL_SECONDS:
        return _weather_apply_time_flags(dict(_weather_cache))

    lat = settings.weather_lat
    lon = settings.weather_lon
    tz = settings.local_timezone or "UTC"

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,shortwave_radiation,weather_code",
        "daily": "weather_code,temperature_2m_max,temperature_2m_min,sunrise,sunset",
        "past_days": 1,
        "forecast_days": 3,
        "timezone": tz,
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(_OPEN_METEO_BASE, params=params)
            resp.raise_for_status()
            raw = resp.json()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Weather API error: {exc}") from exc

    # --- daily summary (yesterday / today / tomorrow) ---
    daily = raw.get("daily") or {}
    d_times = daily.get("time") or []
    d_codes = daily.get("weather_code") or []
    d_tmax = daily.get("temperature_2m_max") or []
    d_tmin = daily.get("temperature_2m_min") or []
    d_sunrise = daily.get("sunrise") or []
    d_sunset = daily.get("sunset") or []

    days_raw = []
    for i, d_date in enumerate(d_times):
        wmo = int(d_codes[i]) if i < len(d_codes) else 0
        condition, icon = _wmo_to_condition(wmo)
        days_raw.append({
            "date": d_date,
            "wmo_code": wmo,
            "condition": condition,
            "icon": icon,
            "temp_max": d_tmax[i] if i < len(d_tmax) else None,
            "temp_min": d_tmin[i] if i < len(d_tmin) else None,
            "sunrise": d_sunrise[i] if i < len(d_sunrise) else None,
            "sunset": d_sunset[i] if i < len(d_sunset) else None,
        })

    # --- hourly strip: raw data without time-relative flags ---
    hourly = raw.get("hourly") or {}
    h_times: list[str] = hourly.get("time") or []
    h_temp: list[float | None] = hourly.get("temperature_2m") or []
    h_solar: list[float | None] = hourly.get("shortwave_radiation") or []
    h_codes: list[int] = hourly.get("weather_code") or []

    hours_raw: list[dict[str, object]] = []
    for i, ts in enumerate(h_times):
        wmo = int(h_codes[i]) if i < len(h_codes) else 0
        _, icon = _wmo_to_condition(wmo)
        temp_val = h_temp[i] if i < len(h_temp) else None
        solar_val = h_solar[i] if i < len(h_solar) else None
        hour_label = ts[11:16] if len(ts) >= 16 else ts  # "HH:MM"
        date_label = ts[:10] if len(ts) >= 10 else ""
        hours_raw.append({
            "time": ts,
            "date": date_label,
            "hour_label": hour_label,
            "icon": icon,
            "wmo_code": wmo,
            "temp": round(temp_val, 1) if temp_val is not None else None,
            "solar_w_m2": round(solar_val) if solar_val is not None else None,
        })

    cached_result: dict[str, object] = {
        "latitude": lat,
        "longitude": lon,
        "timezone": tz,
        "days": days_raw,
        "hours": hours_raw,
        "fetched_at": datetime.now(UTC).isoformat(),
        "cached_at": now_ts,
    }
    _weather_cache.clear()
    _weather_cache.update(cached_result)
    return _weather_apply_time_flags(cached_result)


@app.get("/api/collector/status")
async def collector_runtime_status() -> dict[str, object]:
    saj_state = dict(collector_status.get("saj") or {})
    solplanet_state = dict(collector_status.get("solplanet") or {})
    combined_state = dict(collector_status.get("combined") or {})
    saj_state["interval_seconds"] = None
    saj_state["continuous"] = True
    solplanet_state["interval_seconds"] = None
    solplanet_state["continuous"] = True
    combined_state["interval_seconds"] = None
    combined_state["continuous"] = True
    return {
        "updated_at": datetime.now(UTC).isoformat(),
        "round_number": collector_round_number,
        "systems": {
            "saj": saj_state,
            "solplanet": solplanet_state,
            "combined": combined_state,
        },
    }


@app.post("/api/collector/restart")
@app.post("/api/solplanet/control/restart-api")
@app.post("/api/soulplanet/control/restart-api")
async def post_restart_backend_api() -> dict[str, object]:
    operation_run_id = await _start_operation_history_run(
        "backend.manual.restart_collector",
        request_payload={},
        previous_state=None,
    )
    try:
        result = await _restart_collector()
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_SUCCEEDED,
            latest_state=result,
            response_payload=result,
            completed=True,
        )
        return result
    except Exception as exc:  # noqa: BLE001
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"{type(exc).__name__}: {exc}",
            completed=True,
        )
        raise


@app.get("/api/storage/status")
async def storage_status() -> dict[str, object]:
    status = await asyncio.to_thread(get_storage_status, storage_db_path, sample_interval_seconds)
    status["saj_sample_interval_seconds"] = sample_interval_seconds
    status["solplanet_sample_interval_seconds"] = solplanet_sample_interval_seconds
    return status


@app.get("/api/storage/daily-usage")
async def storage_daily_usage(
    system: str = Query(default="saj", description="saj or solplanet"),
    day_utc: str | None = Query(default=None, description="UTC day in YYYY-MM-DD, default=today"),
) -> dict[str, object]:
    normalized = _normalize_system_name(system)
    target_day = datetime.now(UTC).date()
    if day_utc:
        try:
            target_day = date.fromisoformat(day_utc)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid day_utc format, expected YYYY-MM-DD") from exc
    return await asyncio.to_thread(
        compute_daily_usage,
        storage_db_path,
        system=normalized,
        target_day_utc=target_day,
        sample_interval_seconds=_sample_interval_for_system(normalized),
    )


@app.get("/api/storage/samples")
async def storage_samples(
    system: str | None = Query(default=None, description="saj or solplanet"),
    day_utc: str | None = Query(default=None, description="UTC day in YYYY-MM-DD"),
    start_utc: str | None = Query(default=None, description="UTC datetime in ISO-8601"),
    end_utc: str | None = Query(default=None, description="UTC datetime in ISO-8601"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
) -> dict[str, object]:
    normalized: str | None = None
    if system:
        normalized = _normalize_system_name(system)
    target_day: date | None = None
    start_at: datetime | None = None
    end_at: datetime | None = None
    if day_utc:
        try:
            target_day = date.fromisoformat(day_utc)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid day_utc format, expected YYYY-MM-DD") from exc
    if start_utc:
        start_at = _parse_iso_utc_datetime(start_utc, field_name="start_utc")
    if end_utc:
        end_at = _parse_iso_utc_datetime(end_utc, field_name="end_utc")
    if start_at and end_at and start_at >= end_at:
        raise HTTPException(status_code=400, detail="start_utc must be earlier than end_utc")
    return await asyncio.to_thread(
        list_samples,
        storage_db_path,
        system=normalized,
        target_day_utc=target_day,
        start_at_utc=start_at,
        end_at_utc=end_at,
        page=page,
        page_size=page_size,
    )


@app.get("/api/database/tables")
async def database_tables() -> dict[str, object]:
    payload = await asyncio.to_thread(list_database_tables, storage_db_path)
    payload["updated_at"] = datetime.now(UTC).isoformat()
    return payload


@app.get("/api/database/table")
async def database_table_rows(
    table: str = Query(description="SQLite table name"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
) -> dict[str, object]:
    try:
        payload = await asyncio.to_thread(
            list_database_table_rows,
            storage_db_path,
            table=table,
            page=page,
            page_size=page_size,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    payload["updated_at"] = datetime.now(UTC).isoformat()
    return payload


@app.get("/api/database/export.sqlite3")
async def database_export_sqlite() -> Response:
    db_bytes = await asyncio.to_thread(export_database_bytes, storage_db_path)
    exported_at = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    headers = {"Content-Disposition": f'attachment; filename="wattimize_{exported_at}.sqlite3"'}
    return Response(content=db_bytes, media_type="application/vnd.sqlite3", headers=headers)


@app.post("/api/database/import.sqlite3")
async def database_import_sqlite(file: UploadFile = File(...)) -> dict[str, object]:
    raw = await file.read()
    async with runtime_lock:
        await _stop_collector()
        try:
            result = await asyncio.to_thread(import_database_bytes, storage_db_path, raw)
            await asyncio.to_thread(dispose_db_connections, storage_db_path)
            await asyncio.to_thread(migrate_worker_log_legacy_statuses, storage_db_path)
            await asyncio.to_thread(
                backfill_notification_entries_from_worker_logs,
                storage_db_path,
                system=NOTIFICATION_LOG_SYSTEM,
                service=NOTIFICATION_SUMMARY_SERVICE,
                active_statuses=(WORKER_LOG_STATUS_SEND, "notification", "notified", "alarmed"),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except OSError as exc:
            raise HTTPException(status_code=507, detail=f"Database import failed: {exc}") from exc
        except sqlite3.Error as exc:
            raise HTTPException(status_code=500, detail=f"Database import failed: {exc}") from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Database import failed: {exc}") from exc
        finally:
            await _start_collector()

    status = await asyncio.to_thread(get_storage_status, storage_db_path, sample_interval_seconds)
    status["saj_sample_interval_seconds"] = sample_interval_seconds
    status["solplanet_sample_interval_seconds"] = solplanet_sample_interval_seconds
    return {"ok": True, **result, "storage_status": status}


@app.get("/api/worker/logs")
async def worker_logs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    system: str | None = Query(default=None, description="saj or solplanet"),
    service: str | None = Query(default=None, description="home_assistant or solplanet_cgi"),
    status: str | None = Query(
        default=None,
        description="worker log status defined in worker-log-schema.json",
    ),
    exclude_status: str | None = Query(default=None, description="comma-separated statuses to exclude"),
    category: str | None = Query(default=None, description="worker log category defined in worker-log-schema.json"),
) -> dict[str, object]:
    valid_status_values = set(worker_log_category_status_values(WORKER_LOG_CATEGORY_ALL)) | set(WORKER_LOG_LEGACY_STATUSES)
    normalized_system: str | None = None
    normalized_service = str(service or "").strip() or None
    normalized_status = str(status or "").strip().lower() or None
    excluded_statuses = tuple(
        part
        for part in (
            str(item).strip().lower()
            for item in str(exclude_status or "").split(",")
        )
        if part
    )
    normalized_category = str(category or "").strip().lower()
    if normalized_service and not worker_log_service_config(normalized_service):
        raise HTTPException(status_code=400, detail=f"unknown worker log service: {normalized_service}")
    if normalized_status and normalized_status not in valid_status_values:
        raise HTTPException(status_code=400, detail=f"unknown worker log status: {normalized_status}")
    if normalized_category in (WORKER_LOG_CATEGORY_ALL, ""):
        normalized_category = ""
    category_entry = worker_log_category_config(normalized_category) if normalized_category else None
    if category_entry:
        normalized_system = str(category_entry.get("system") or "").strip() or None
        allowed_services = worker_log_category_service_values(normalized_category)
        if normalized_service and normalized_service not in allowed_services:
            raise HTTPException(status_code=400, detail=f"service {normalized_service} is not valid for category {normalized_category}")
        if not normalized_service and len(allowed_services) == 1:
            normalized_service = allowed_services[0]
    elif system:
        normalized_system = _normalize_system_name(system)
    elif normalized_category:
        raise HTTPException(
            status_code=400,
            detail="category must be one of the values defined in worker-log-schema.json",
        )
    elif system:
        normalized_system = _normalize_system_name(system)
    allowed_statuses = (
        worker_log_service_status_values(normalized_service)
        if normalized_service
        else worker_log_category_status_values(normalized_category or WORKER_LOG_CATEGORY_ALL)
    )
    allowed_statuses = tuple(dict.fromkeys([*allowed_statuses, *WORKER_LOG_LEGACY_STATUSES]))
    if normalized_status and allowed_statuses and normalized_status not in allowed_statuses:
        raise HTTPException(status_code=400, detail=f"status {normalized_status} is not valid for the selected category/service")
    if excluded_statuses and allowed_statuses:
        invalid_excluded = [item for item in excluded_statuses if item not in allowed_statuses]
        if invalid_excluded:
            raise HTTPException(status_code=400, detail=f"invalid excluded statuses: {', '.join(invalid_excluded)}")
    await asyncio.to_thread(
        expire_pending_worker_api_logs,
        storage_db_path,
        older_than_epoch=datetime.now(UTC).timestamp() - WORKER_PENDING_LOG_TIMEOUT_SECONDS,
        status=WORKER_LOG_STATUS_TIMEOUT,
        error_text="worker_log_timeout",
    )
    payload = await asyncio.to_thread(
        list_worker_api_logs,
        storage_db_path,
        page=page,
        page_size=page_size,
        worker="worker",
        system=normalized_system,
        service=normalized_service,
        status=normalized_status,
        exclude_statuses=excluded_statuses,
    )
    payload["updated_at"] = datetime.now(UTC).isoformat()
    return payload


@app.get("/api/dashboard/notifications")
async def dashboard_notifications() -> dict[str, object]:
    entries = await asyncio.to_thread(
        list_active_notification_entries,
        storage_db_path,
        system=NOTIFICATION_LOG_SYSTEM,
        service=NOTIFICATION_SUMMARY_SERVICE,
        status=WORKER_LOG_NOTIFICATION_ACTIVE_STATUSES,
    )
    items = [_dashboard_notification_item(entry) for entry in entries]
    items.sort(
        key=lambda item: (
            _notification_level_rank(item.get("level")),
            float(item.get("requested_at_epoch") or 0.0),
        ),
        reverse=True,
    )
    return {
        "updated_at": datetime.now(UTC).isoformat(),
        "count": len(items),
        "items": items,
    }


@app.get("/api/time-window-rules")
async def get_time_window_rules() -> dict[str, object]:
    rows = await asyncio.to_thread(list_time_window_rule_state_rows, storage_db_path)
    state_by_code = {
        str(item.get("rule_code") or "").strip(): bool(item.get("enabled"))
        for item in rows
        if str(item.get("rule_code") or "").strip()
    }
    updated_at_by_code = {
        str(item.get("rule_code") or "").strip(): str(item.get("updated_at_utc") or "")
        for item in rows
        if str(item.get("rule_code") or "").strip()
    }
    items = [
        {
            "rule_code": code,
            "enabled": bool(state_by_code.get(code, True)),
            "kind": next((item["kind"] for item in TIME_WINDOW_RULE_DEFINITIONS if item["code"] == code), "notification"),
            "windows": list(TIME_WINDOW_RULE_WINDOWS_BY_CODE.get(code, ())),
            "updated_at_utc": updated_at_by_code.get(code) or None,
        }
        for code in TIME_WINDOW_RULE_CODES
    ]
    return {
        "updated_at": datetime.now(UTC).isoformat(),
        "count": len(items),
        "items": items,
    }


@app.put("/api/time-window-rules/{rule_code}")
async def put_time_window_rule(rule_code: str, payload: TimeWindowRuleStatePayload) -> dict[str, object]:
    normalized_code = str(rule_code or "").strip()
    if normalized_code not in TIME_WINDOW_RULE_CODES:
        raise HTTPException(status_code=404, detail="Unknown time window rule")
    updated = await asyncio.to_thread(
        upsert_time_window_rule_state,
        storage_db_path,
        rule_code=normalized_code,
        enabled=bool(payload.enabled),
        updated_at_utc=datetime.now(UTC).isoformat(),
    )
    if not updated:
        raise HTTPException(status_code=500, detail="Failed to persist time window rule state")
    return {
        **updated,
        "windows": list(TIME_WINDOW_RULE_WINDOWS_BY_CODE.get(normalized_code, ())),
    }


@app.post("/api/dashboard/notifications/dismiss")
async def dismiss_dashboard_notification(payload: NotificationDismissRequest) -> dict[str, object]:
    notification_key = str(payload.notification_key or "").strip()
    if not notification_key:
        raise HTTPException(status_code=400, detail="notification_key is required")
    dismissed_at_utc = datetime.now(UTC).isoformat()
    ok = await asyncio.to_thread(
        dismiss_notification_entry,
        storage_db_path,
        notification_key=notification_key,
        dismissed_at_utc=dismissed_at_utc,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to dismiss notification")
    return {
        "notification_key": notification_key,
        "state": "dismissed",
        "dismissed_at_utc": dismissed_at_utc,
    }


@app.get("/api/worker/failure-log")
async def worker_failure_log(
    limit: int = Query(default=100, ge=1, le=500),
    before: int = Query(default=0, ge=0),
) -> dict[str, object]:
    payload = await asyncio.to_thread(_read_worker_failure_log, limit=limit, before=before)
    payload["updated_at"] = datetime.now(UTC).isoformat()
    return payload


@app.get("/api/raw-requests")
async def raw_requests(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    round_id: str | None = Query(default=None),
    system: str | None = Query(default=None, description="saj or solplanet"),
    source: str | None = Query(default=None, description="home_assistant or solplanet_cgi"),
) -> dict[str, object]:
    normalized_system: str | None = None
    if system:
        normalized_system = _normalize_system_name(system) if system.lower().strip() != "combined" else "combined"
    payload = await asyncio.to_thread(
        list_raw_request_results,
        storage_db_path,
        page=page,
        page_size=page_size,
        round_id=str(round_id or "").strip() or None,
        system=normalized_system,
        source=str(source or "").strip() or None,
    )
    payload["updated_at"] = datetime.now(UTC).isoformat()
    return payload


@app.get("/api/storage/series")
async def storage_series(
    system: str = Query(default="saj", description="saj or solplanet"),
    day_utc: str | None = Query(default=None, description="UTC day in YYYY-MM-DD"),
    start_utc: str | None = Query(default=None, description="UTC datetime in ISO-8601"),
    end_utc: str | None = Query(default=None, description="UTC datetime in ISO-8601"),
    hours: int = Query(default=24, ge=1, le=168),
    max_points: int = Query(default=800, ge=50, le=5000),
) -> dict[str, object]:
    normalized = _normalize_system_name(system)
    target_day: date | None = None
    start_at: datetime | None = None
    end_at: datetime | None = None
    if day_utc:
        try:
            target_day = date.fromisoformat(day_utc)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid day_utc format, expected YYYY-MM-DD") from exc
    if start_utc:
        start_at = _parse_iso_utc_datetime(start_utc, field_name="start_utc")
    if end_utc:
        end_at = _parse_iso_utc_datetime(end_utc, field_name="end_utc")
    if start_at and end_at and start_at >= end_at:
        raise HTTPException(status_code=400, detail="start_utc must be earlier than end_utc")
    return await asyncio.to_thread(
        get_series_samples,
        storage_db_path,
        system=normalized,
        hours=hours,
        max_points=max_points,
        target_day_utc=target_day,
        start_at_utc=start_at,
        end_at_utc=end_at,
    )


@app.get("/api/storage/usage-range")
async def storage_usage_range(
    system: str = Query(default="saj", description="saj or solplanet"),
    start_utc: str = Query(..., description="UTC datetime in ISO-8601"),
    end_utc: str = Query(..., description="UTC datetime in ISO-8601"),
) -> dict[str, object]:
    normalized = _normalize_system_name(system)
    start_at = _parse_iso_utc_datetime(start_utc, field_name="start_utc")
    end_at = _parse_iso_utc_datetime(end_utc, field_name="end_utc")
    if start_at >= end_at:
        raise HTTPException(status_code=400, detail="start_utc must be earlier than end_utc")
    _, timezone_name = _tesla_grid_support_now_local()
    return await asyncio.to_thread(
        compute_usage_between,
        storage_db_path,
        system=normalized,
        start_at_utc=start_at,
        end_at_utc=end_at,
        sample_interval_seconds=_sample_interval_for_system(normalized),
        local_timezone_name=timezone_name,
        grid_import_window_start_hour=FREE_ENERGY_WINDOW_START_HOUR,
        grid_import_window_end_hour=FREE_ENERGY_WINDOW_END_HOUR,
        grid_export_window_start_hour=EXPORT_WINDOW_START_HOUR,
        grid_export_window_end_hour=EXPORT_WINDOW_END_HOUR,
    )



@app.get("/api/config/status")
async def config_status() -> dict[str, object]:
    missing = _missing_required_config()
    return {
        "configured": len(missing) == 0,
        "missing_required": missing,
        "config_path": str(get_config_path()),
        "allowed_sample_interval_seconds": list(ALLOWED_SAMPLE_INTERVAL_SECONDS),
        "saj_sample_interval_seconds": sample_interval_seconds,
        "solplanet_sample_interval_seconds": solplanet_sample_interval_seconds,
    }


@app.get("/api/config")
async def get_config() -> dict[str, object]:
    payload = settings_to_dict(settings)
    payload["configured"] = len(_missing_required_config()) == 0
    payload["config_path"] = str(get_config_path())
    return payload


@app.put("/api/config")
async def put_config(payload: ConfigPayload) -> dict[str, object]:
    saj_interval = normalize_sample_interval_seconds(
        payload.saj_sample_interval_seconds,
        CONST_SAJ_SAMPLE_INTERVAL_SECONDS,
    )
    solplanet_interval = normalize_sample_interval_seconds(
        payload.solplanet_sample_interval_seconds,
        CONST_SOLPLANET_SAMPLE_INTERVAL_SECONDS,
    )
    _validate_interval_choice("saj_sample_interval_seconds", saj_interval)
    _validate_interval_choice("solplanet_sample_interval_seconds", solplanet_interval)
    persisted = save_settings(
        {
            **payload.model_dump(),
            "saj_sample_interval_seconds": saj_interval,
            "solplanet_sample_interval_seconds": solplanet_interval,
        }
    )
    await _replace_runtime(persisted)
    response = settings_to_dict(settings)
    response["configured"] = len(_missing_required_config()) == 0
    response["config_path"] = str(get_config_path())
    return response


@app.post("/api/config/solplanet/discover")
async def discover_solplanet_config(payload: SolplanetDiscoverPayload) -> dict[str, object]:
    dongle_host = str(payload.solplanet_dongle_host or "").strip() or str(settings.solplanet_dongle_host or "").strip()
    if not dongle_host:
        raise HTTPException(status_code=400, detail="solplanet_dongle_host is required")

    client = solplanet_client if dongle_host == str(settings.solplanet_dongle_host or "").strip() else SolplanetCgiClient(
        host=dongle_host,
        port=settings.solplanet_dongle_port,
        scheme=settings.solplanet_dongle_scheme,
        verify_ssl=settings.solplanet_verify_ssl,
        timeout_seconds=settings.solplanet_request_timeout_seconds,
        request_logger=_handle_outbound_request_log,
    )
    try:
        inverter_info = await asyncio.wait_for(
            client.get_inverter_info(),
            timeout=settings.solplanet_request_timeout_seconds,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to discover Solplanet SN: {exc}") from exc
    finally:
        if client is not solplanet_client:
            await client.aclose()

    context = _extract_solplanet_context(inverter_info)
    inverter_sn = str(context.get("inverter_sn") or "").strip()
    battery_sn = str(context.get("battery_sn") or "").strip()
    if not inverter_sn or not battery_sn:
        raise HTTPException(
            status_code=502,
            detail="Solplanet API did not return both inverter and battery SN values",
        )

    _set_solplanet_runtime_context(context)
    persisted = await asyncio.to_thread(
        save_settings,
        {
            "solplanet_dongle_host": dongle_host,
            "solplanet_inverter_sn": inverter_sn,
            "solplanet_battery_sn": battery_sn,
        },
    )
    await _replace_runtime(persisted)
    _set_solplanet_runtime_context(context)

    response = settings_to_dict(settings)
    response["configured"] = len(_missing_required_config()) == 0
    response["config_path"] = str(get_config_path())
    response["discovered"] = {
        "solplanet_inverter_sn": inverter_sn,
        "solplanet_battery_sn": battery_sn,
    }
    return response


@app.get("/")
async def frontend_index() -> Response:
    index_file = static_dir / "index.html"
    if index_file.exists():
        return Response(content=_render_index_html(), media_type="text/html; charset=utf-8")
    return JSONResponse(
        status_code=503,
        content={
            "message": "Frontend files not found",
            "expected_path": str(index_file),
        },
    )


@app.get("/api/entities/core")
async def get_core_entities() -> dict[str, object]:
    payload = await get_core_entities_by_system("saj")
    payload["deprecated"] = True
    payload["preferred_path"] = "/api/saj/entities/core"
    return payload


@app.get("/api/saj/entities/core")
async def get_core_entities_saj() -> dict[str, object]:
    return await get_core_entities_by_system("saj")


@app.get("/api/soulplanet/entities/core")
@app.get("/api/solplanet/entities/core")
async def get_core_entities_soulplanet() -> dict[str, object]:
    return await get_core_entities_by_system("solplanet")


@app.get("/api/entities/core/{system}")
async def get_core_entities_by_system(system: str) -> dict[str, object]:
    normalized = _normalize_system_name(system)
    if normalized == "solplanet" and solplanet_client.configured:
        try:
            flow = await _get_solplanet_flow_cached()
            entities = [item for item in flow.get("entities", {}).values() if item is not None]
            return {"system": normalized, "count": len(entities), "items": entities, "source": "solplanet_cgi"}
        except httpx.HTTPStatusError as exc:
            detail = {
                "message": "Solplanet CGI returned an error",
                "status_code": exc.response.status_code,
                "response": exc.response.text,
            }
            raise HTTPException(status_code=502, detail=detail) from exc
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=f"Failed to reach Solplanet CGI: {exc}") from exc
        except (TimeoutError, asyncio.TimeoutError) as exc:
            raise HTTPException(status_code=504, detail=f"Solplanet CGI timed out: {exc}") from exc

    _ensure_ha_configured()
    try:
        entity_ids = _get_system_entity_ids(normalized)
        data = await ha_client.core_entities(entity_ids)
        return {"system": normalized, "count": len(data), "items": data}
    except httpx.HTTPStatusError as exc:
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.get("/api/energy-flow/{system}")
async def get_energy_flow(system: str) -> dict[str, object]:
    normalized = _normalize_system_name(system)
    try:
        flow = await _get_energy_flow_from_realtime_kv(normalized)
    except HTTPException:
        flow = await _get_live_energy_flow(normalized)
    else:
        if bool(flow.get("stale")):
            try:
                flow = await _get_live_energy_flow(normalized)
            except HTTPException:
                flow = await _get_energy_flow_from_storage(normalized)
    if normalized == "combined":
        public_flow = _build_public_combined_flow(flow)
        tesla_pending_operations = await _build_tesla_pending_operation_state(public_flow)
        tesla_map = public_flow.get("tesla")
        if isinstance(tesla_map, dict):
            tesla_map["operations"] = tesla_pending_operations
        return public_flow
    return flow


@app.get("/api/saj/energy-flow")
async def get_energy_flow_saj() -> dict[str, object]:
    return await get_energy_flow("saj")


@app.get("/api/saj/raw/dashboard-sources")
async def get_saj_raw_dashboard_sources() -> dict[str, object]:
    started_at = monotonic()
    _ensure_ha_configured()
    try:
        states = await ha_client.all_states()
        flow = _build_energy_flow_payload("saj", states)
        states_by_entity_id = {str(item.get("entity_id", "")): item for item in states}
        entities = flow.get("entities")
        entities_map = entities if isinstance(entities, dict) else {}

        def _entity_raw(key: str) -> dict[str, object] | None:
            item = entities_map.get(key)
            if not isinstance(item, dict):
                return None
            entity_id = str(item.get("entity_id") or "")
            if not entity_id:
                return None
            return _compact_raw_ha_state(states_by_entity_id.get(entity_id))

        metrics = flow.get("metrics")
        metrics_map = metrics if isinstance(metrics, dict) else {}
        payload = {
            "dashboard_values": {
                "solar_power_w": metrics_map.get("pv_w"),
                "grid_power_w": metrics_map.get("grid_w"),
                "battery_power_w": metrics_map.get("battery_w"),
                "home_load_power_w": metrics_map.get("load_w"),
                "battery_soc_percent": metrics_map.get("battery_soc_percent"),
                "inverter_status": metrics_map.get("inverter_status"),
                "balance_w": metrics_map.get("balance_w"),
            },
            "source_entities": {
                "pv": _entity_raw("pv"),
                "grid": _entity_raw("grid"),
                "battery": _entity_raw("battery"),
                "load": _entity_raw("load"),
                "soc": _entity_raw("soc"),
                "inverter": _entity_raw("inverter"),
            },
            "matched_entities": metrics_map.get("matched_entities"),
        }
        return _build_saj_single_endpoint_response(
            name="dashboard_sources",
            path="HA /api/states (derived via energy-flow matching)",
            payload=payload,
            error=None,
            started_at=started_at,
        )
    except Exception as exc:  # noqa: BLE001
        return _build_saj_single_endpoint_response(
            name="dashboard_sources",
            path="HA /api/states (derived via energy-flow matching)",
            payload=None,
            error=f"{type(exc).__name__}: {exc}",
            started_at=started_at,
        )


@app.get("/api/saj/raw/core-entities")
async def get_saj_raw_core_entities() -> dict[str, object]:
    started_at = monotonic()
    _ensure_ha_configured()
    try:
        states = await ha_client.all_states()
        states_by_entity_id = {str(item.get("entity_id", "")): item for item in states}
        configured_ids = list(settings.saj_core_entity_ids)
        payload = {
            "configured_entity_ids": configured_ids,
            "items": [
                {
                    "configured_entity_id": entity_id,
                    "state": _compact_raw_ha_state(states_by_entity_id.get(entity_id)),
                }
                for entity_id in configured_ids
            ],
        }
        return _build_saj_single_endpoint_response(
            name="core_entities",
            path="HA /api/states (configured SAJ core entity ids)",
            payload=payload,
            error=None,
            started_at=started_at,
        )
    except Exception as exc:  # noqa: BLE001
        return _build_saj_single_endpoint_response(
            name="core_entities",
            path="HA /api/states (configured SAJ core entity ids)",
            payload=None,
            error=f"{type(exc).__name__}: {exc}",
            started_at=started_at,
        )


@app.get("/api/saj/control/state")
async def get_saj_control_state() -> dict[str, object]:
    _ensure_ha_configured()
    try:
        _, states_by_id = await _saj_control_states()
        return {"system": "saj", "control_state": _build_saj_control_state(states_by_id)}
    except httpx.HTTPStatusError as exc:
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.get("/api/saj/control/profile")
async def get_saj_control_profile() -> dict[str, object]:
    _ensure_ha_configured()
    try:
        _, states_by_id = await _saj_control_states()
        control_state = _build_saj_control_state(states_by_id)
        return {"system": "saj", "profile_state": _build_saj_profile_state(control_state)}
    except httpx.HTTPStatusError as exc:
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.get("/api/tesla/control/state")
async def get_tesla_control_state() -> dict[str, object]:
    _ensure_ha_configured()
    try:
        states = await _tesla_control_states()
        control_state = _build_tesla_control_state(states)
        return {
            "system": "tesla",
            "control_state": {
                **control_state,
                "feedback": _resolve_tesla_control_feedback(control_state),
            },
            "observation": _build_tesla_observation_payload(states),
        }
    except httpx.HTTPStatusError as exc:
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.post("/api/tesla/control/charging")
async def post_tesla_control_charging(payload: TeslaChargingTogglePayload) -> dict[str, object]:
    _ensure_ha_configured()
    requested_at_utc = datetime.now(UTC).isoformat()
    operation_run_id: int | None = None
    try:
        initial_states = await _tesla_control_states()
        previous_control_state = _build_tesla_control_state(initial_states)
        previous_observation = _build_tesla_observation_payload(initial_states)
        operation_run_id = await _start_operation_history_run(
            "tesla.manual.toggle_charging",
            request_payload=payload.model_dump(mode="json"),
            previous_state={
                "control_state": previous_control_state,
                "observation": previous_observation,
            },
        )
        await _set_tesla_control_feedback(
            phase="requesting",
            target_enabled=payload.enabled,
            requested_at_utc=requested_at_utc,
        )
        result = await _tesla_set_charging(payload.enabled)
        control_state = result.get("control_state")
        control_state_map = control_state if isinstance(control_state, dict) else {}
        result["control_state"] = {
            **control_state_map,
            "feedback": await _set_tesla_control_feedback(
                phase="awaiting_vehicle",
                target_enabled=payload.enabled,
                requested_at_utc=requested_at_utc,
            ),
        }
        run_status = _tesla_charging_run_status(result, target_enabled=payload.enabled)
        await _update_operation_history_run(
            operation_run_id,
            status=run_status,
            latest_state={
                "control_state": result.get("control_state"),
                "observation": result.get("observation"),
            },
            response_payload=result,
            completed=run_status == OPERATION_RUN_STATUS_SUCCEEDED,
        )
        return result
    except httpx.HTTPStatusError as exc:
        await _set_tesla_control_feedback(
            phase="failed",
            target_enabled=payload.enabled,
            requested_at_utc=requested_at_utc,
            error=f"Home Assistant API returned {exc.response.status_code}",
        )
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Home Assistant API returned {exc.response.status_code}",
            completed=True,
        )
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        await _set_tesla_control_feedback(
            phase="failed",
            target_enabled=payload.enabled,
            requested_at_utc=requested_at_utc,
            error=f"Failed to reach Home Assistant: {exc}",
        )
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Failed to reach Home Assistant: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc
    except HTTPException as exc:
        await _set_tesla_control_feedback(
            phase="failed",
            target_enabled=payload.enabled,
            requested_at_utc=requested_at_utc,
            error=str(exc.detail),
        )
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=str(exc.detail),
            completed=True,
        )
        raise


@app.post("/api/tesla/control/current")
async def post_tesla_control_current(payload: TeslaChargeCurrentPayload) -> dict[str, object]:
    _ensure_ha_configured()
    operation_run_id: int | None = None
    try:
        initial_states = await _tesla_control_states()
        previous_control_state = _build_tesla_control_state(initial_states)
        previous_observation = _build_tesla_observation_payload(initial_states)
        operation_run_id = await _start_operation_history_run(
            "tesla.manual.set_charge_current",
            request_payload=payload.model_dump(mode="json"),
            previous_state={
                "control_state": previous_control_state,
                "observation": previous_observation,
            },
        )
        result = await _tesla_set_charge_current(int(payload.amps))
        control_state = result.get("control_state")
        control_state_map = control_state if isinstance(control_state, dict) else {}
        result["control_state"] = {
            **control_state_map,
            "feedback": _resolve_tesla_control_feedback(control_state_map),
        }
        run_status = _tesla_current_run_status(result, target_amps=int(payload.amps))
        await _update_operation_history_run(
            operation_run_id,
            status=run_status,
            latest_state={
                "control_state": result.get("control_state"),
                "observation": result.get("observation"),
            },
            response_payload=result,
            completed=run_status == OPERATION_RUN_STATUS_SUCCEEDED,
        )
        return result
    except httpx.HTTPStatusError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Home Assistant API returned {exc.response.status_code}",
            completed=True,
        )
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Failed to reach Home Assistant: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc
    except Exception as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Failed to reach Home Assistant: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.get("/api/saj/control/capabilities")
async def get_saj_control_capabilities() -> dict[str, object]:
    _ensure_ha_configured()
    try:
        _, states_by_id = await _saj_control_states()
        return {"system": "saj", "capabilities": _build_saj_control_capabilities(states_by_id)}
    except httpx.HTTPStatusError as exc:
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.put("/api/saj/control/working-mode")
async def put_saj_working_mode(payload: SajWorkingModePayload) -> dict[str, object]:
    _ensure_ha_configured()
    operation_run_id: int | None = None
    try:
        _, initial_states_by_id = await _saj_control_states()
        operation_run_id = await _start_operation_history_run(
            "saj.manual.set_working_mode",
            request_payload=payload.model_dump(mode="json"),
            previous_state={"state": _build_saj_control_state(initial_states_by_id)},
        )
        await _saj_set_number("number.saj_app_mode_input", payload.mode_code)
        _, states_by_id = await _saj_control_states()
        result = {
            "ok": True,
            "changed": [{"entity_id": "number.saj_app_mode_input", "value": payload.mode_code}],
            "state": _build_saj_control_state(states_by_id),
        }
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_SUCCEEDED,
            latest_state={"state": result.get("state")},
            response_payload=result,
            completed=True,
        )
        return result
    except httpx.HTTPStatusError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Home Assistant API returned {exc.response.status_code}",
            completed=True,
        )
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Failed to reach Home Assistant: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.put("/api/saj/control/profile")
async def put_saj_control_profile(payload: SajProfilePayload) -> dict[str, object]:
    _ensure_ha_configured()
    operation_run_id: int | None = None
    try:
        _, initial_states_by_id = await _saj_control_states()
        operation_run_id = await _start_operation_history_run(
            "saj.manual.apply_profile",
            request_payload=payload.model_dump(mode="json"),
            previous_state={"state": _build_saj_control_state(initial_states_by_id)},
        )
        result = await _saj_apply_profile(payload.profile_id)
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_SUCCEEDED,
            latest_state={"state": result.get("state")},
            response_payload=result,
            completed=True,
        )
        return result
    except httpx.HTTPStatusError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Home Assistant API returned {exc.response.status_code}",
            completed=True,
        )
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Failed to reach Home Assistant: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.put("/api/saj/control/charge-slots/{slot}")
async def put_saj_charge_slot(slot: int, payload: SajSlotPayload) -> dict[str, object]:
    _ensure_ha_configured()
    operation_run_id: int | None = None
    try:
        _, initial_states_by_id = await _saj_control_states()
        operation_run_id = await _start_operation_history_run(
            "saj.manual.set_charge_slot",
            request_payload={"slot": slot, **payload.model_dump(mode="json")},
            previous_state={"state": _build_saj_control_state(initial_states_by_id)},
        )
        result = await _saj_apply_slot("charge", slot, payload)
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_SUCCEEDED,
            latest_state={"state": result.get("state")},
            response_payload=result,
            completed=True,
        )
        return result
    except httpx.HTTPStatusError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Home Assistant API returned {exc.response.status_code}",
            completed=True,
        )
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Failed to reach Home Assistant: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.put("/api/saj/control/discharge-slots/{slot}")
async def put_saj_discharge_slot(slot: int, payload: SajSlotPayload) -> dict[str, object]:
    _ensure_ha_configured()
    operation_run_id: int | None = None
    try:
        _, initial_states_by_id = await _saj_control_states()
        operation_run_id = await _start_operation_history_run(
            "saj.manual.set_discharge_slot",
            request_payload={"slot": slot, **payload.model_dump(mode="json")},
            previous_state={"state": _build_saj_control_state(initial_states_by_id)},
        )
        result = await _saj_apply_slot("discharge", slot, payload)
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_SUCCEEDED,
            latest_state={"state": result.get("state")},
            response_payload=result,
            completed=True,
        )
        return result
    except httpx.HTTPStatusError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Home Assistant API returned {exc.response.status_code}",
            completed=True,
        )
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Failed to reach Home Assistant: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.put("/api/saj/control/toggles")
async def put_saj_control_toggles(payload: SajTogglePayload) -> dict[str, object]:
    _ensure_ha_configured()
    changed: list[dict[str, object]] = []
    operation_run_id: int | None = None
    try:
        _, initial_states_by_id = await _saj_control_states()
        operation_run_id = await _start_operation_history_run(
            "saj.manual.set_toggles",
            request_payload=payload.model_dump(mode="json"),
            previous_state={"state": _build_saj_control_state(initial_states_by_id)},
        )
        if payload.charging_control is not None:
            await _saj_set_switch("switch.saj_charging_control", payload.charging_control)
            changed.append({"entity_id": "switch.saj_charging_control", "value": payload.charging_control})
        if payload.discharging_control is not None:
            await _saj_set_switch("switch.saj_discharging_control", payload.discharging_control)
            changed.append({"entity_id": "switch.saj_discharging_control", "value": payload.discharging_control})
        if payload.charge_time_enable_mask is not None:
            await _saj_set_number("number.saj_charge_time_enable_input", payload.charge_time_enable_mask)
            changed.append(
                {"entity_id": "number.saj_charge_time_enable_input", "value": payload.charge_time_enable_mask}
            )
        if payload.discharge_time_enable_mask is not None:
            await _saj_set_number("number.saj_discharge_time_enable_input", payload.discharge_time_enable_mask)
            changed.append(
                {
                    "entity_id": "number.saj_discharge_time_enable_input",
                    "value": payload.discharge_time_enable_mask,
                }
            )
        if not changed:
            raise HTTPException(status_code=400, detail="No fields to update")

        _, states_by_id = await _saj_control_states()
        result = {"ok": True, "changed": changed, "state": _build_saj_control_state(states_by_id)}
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_SUCCEEDED,
            latest_state={"state": result.get("state")},
            response_payload=result,
            completed=True,
        )
        return result
    except httpx.HTTPStatusError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Home Assistant API returned {exc.response.status_code}",
            completed=True,
        )
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Failed to reach Home Assistant: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.put("/api/saj/control/limits")
async def put_saj_control_limits(payload: SajLimitsPayload) -> dict[str, object]:
    _ensure_ha_configured()
    changed: list[dict[str, object]] = []
    operation_run_id: int | None = None
    try:
        _, initial_states_by_id = await _saj_control_states()
        operation_run_id = await _start_operation_history_run(
            "saj.manual.set_limits",
            request_payload=payload.model_dump(mode="json"),
            previous_state={"state": _build_saj_control_state(initial_states_by_id)},
        )
        if payload.battery_charge_power_limit is not None:
            await _saj_set_number("number.saj_battery_charge_power_limit_input", payload.battery_charge_power_limit)
            changed.append(
                {"entity_id": "number.saj_battery_charge_power_limit_input", "value": payload.battery_charge_power_limit}
            )
        if payload.battery_discharge_power_limit is not None:
            await _saj_set_number(
                "number.saj_battery_discharge_power_limit_input",
                payload.battery_discharge_power_limit,
            )
            changed.append(
                {
                    "entity_id": "number.saj_battery_discharge_power_limit_input",
                    "value": payload.battery_discharge_power_limit,
                }
            )
        if payload.grid_max_charge_power is not None:
            await _saj_set_number("number.saj_grid_max_charge_power_input", payload.grid_max_charge_power)
            changed.append({"entity_id": "number.saj_grid_max_charge_power_input", "value": payload.grid_max_charge_power})
        if payload.grid_max_discharge_power is not None:
            await _saj_set_number("number.saj_grid_max_discharge_power_input", payload.grid_max_discharge_power)
            changed.append(
                {"entity_id": "number.saj_grid_max_discharge_power_input", "value": payload.grid_max_discharge_power}
            )
        if not changed:
            raise HTTPException(status_code=400, detail="No fields to update")
        _, states_by_id = await _saj_control_states()
        result = {"ok": True, "changed": changed, "state": _build_saj_control_state(states_by_id)}
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_SUCCEEDED,
            latest_state={"state": result.get("state")},
            response_payload=result,
            completed=True,
        )
        return result
    except httpx.HTTPStatusError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Home Assistant API returned {exc.response.status_code}",
            completed=True,
        )
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Failed to reach Home Assistant: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.post("/api/saj/control/refresh-touch")
async def post_saj_control_refresh_touch() -> dict[str, object]:
    _ensure_ha_configured()
    entity_id = "switch.saj_passive_charge_control"
    operation_run_id: int | None = None
    try:
        _, initial_states_by_id = await _saj_control_states()
        operation_run_id = await _start_operation_history_run(
            "saj.manual.refresh_touch",
            request_payload={},
            previous_state={"state": _build_saj_control_state(initial_states_by_id)},
        )
        kept_state = await _saj_touch_switch(entity_id)
        _, states_by_id = await _saj_control_states()
        result = {
            "ok": True,
            "entity_id": entity_id,
            "kept_state": kept_state,
            "state": _build_saj_control_state(states_by_id),
        }
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_SUCCEEDED,
            latest_state={"state": result.get("state")},
            response_payload=result,
            completed=True,
        )
        return result
    except httpx.HTTPStatusError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Home Assistant API returned {exc.response.status_code}",
            completed=True,
        )
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Failed to reach Home Assistant: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.get("/api/soulplanet/control/state")
@app.get("/api/solplanet/control/state")
async def get_solplanet_control_state() -> dict[str, object]:
    try:
        schedule = await _solplanet_get_schedule_with_fallback()
        return {"system": "solplanet", "control_state": _build_solplanet_control_state(schedule)}
    except httpx.HTTPStatusError as exc:
        detail = {
            "message": "Solplanet CGI returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to reach Solplanet CGI: {exc}") from exc
    except (TimeoutError, asyncio.TimeoutError) as exc:
        raise HTTPException(status_code=504, detail=f"Solplanet CGI timed out: {exc}") from exc


@app.put("/api/soulplanet/control/limits")
@app.put("/api/solplanet/control/limits")
async def put_solplanet_control_limits(payload: SolplanetLimitsPayload) -> dict[str, object]:
    changed: list[dict[str, object]] = []
    update_payload: dict[str, object] = {}
    operation_run_id: int | None = None
    if payload.pin is not None:
        update_payload["Pin"] = payload.pin
        changed.append({"key": "Pin", "value": payload.pin})
    if payload.pout is not None:
        update_payload["Pout"] = payload.pout
        changed.append({"key": "Pout", "value": payload.pout})
    if not update_payload:
        raise HTTPException(status_code=400, detail="No fields to update")
    try:
        previous_schedule = await _solplanet_get_schedule_with_fallback()
        operation_run_id = await _start_operation_history_run(
            "solplanet.manual.set_limits",
            request_payload=payload.model_dump(mode="json"),
            previous_state={"state": _build_solplanet_control_state(previous_schedule)},
        )
        cgi_write = await _solplanet_set_schedule_payload(update_payload)
        state_payload: dict[str, object] | None = None
        readback_error: str | None = None
        try:
            schedule = await asyncio.wait_for(
                _solplanet_get_schedule_with_fallback(),
                timeout=min(5.0, float(settings.solplanet_request_timeout_seconds)),
            )
            state_payload = _build_solplanet_control_state(schedule)
        except Exception as exc:  # noqa: BLE001
            readback_error = f"{type(exc).__name__}: {exc}"
        result = {
            "ok": True,
            "changed": changed,
            "request_payload": update_payload,
            "cgi_write": cgi_write,
            "state": state_payload,
            "readback_error": readback_error,
        }
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_SUCCEEDED,
            latest_state={"state": state_payload},
            response_payload=result,
            completed=True,
        )
        return result
    except httpx.HTTPStatusError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Solplanet CGI returned {exc.response.status_code}",
            completed=True,
        )
        detail = {
            "message": "Solplanet CGI returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Failed to reach Solplanet CGI: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=502, detail=f"Failed to reach Solplanet CGI: {exc}") from exc
    except (TimeoutError, asyncio.TimeoutError) as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Solplanet CGI timed out: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=504, detail=f"Solplanet CGI timed out: {exc}") from exc


@app.put("/api/soulplanet/control/day-schedule/{day}")
@app.put("/api/solplanet/control/day-schedule/{day}")
async def put_solplanet_day_schedule(day: str, payload: SolplanetDaySchedulePayload) -> dict[str, object]:
    day_key = _normalize_solplanet_day_key(day)
    slots = _validate_solplanet_day_slots(payload.slots)
    operation_run_id: int | None = None
    try:
        previous_schedule = await _solplanet_get_schedule_with_fallback()
        operation_run_id = await _start_operation_history_run(
            "solplanet.manual.set_day_schedule",
            request_payload={"day": day_key, **payload.model_dump(mode="json")},
            previous_state={"state": _build_solplanet_control_state(previous_schedule)},
        )
        request_payload = {day_key: slots}
        cgi_write = await _solplanet_set_schedule_payload(request_payload)
        schedule = await _solplanet_get_schedule_with_fallback()
        result = {
            "ok": True,
            "changed": [{"key": day_key, "value": slots}],
            "request_payload": request_payload,
            "cgi_write": cgi_write,
            "state": _build_solplanet_control_state(schedule),
        }
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_SUCCEEDED,
            latest_state={"state": result.get("state")},
            response_payload=result,
            completed=True,
        )
        return result
    except httpx.HTTPStatusError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Solplanet CGI returned {exc.response.status_code}",
            completed=True,
        )
        detail = {
            "message": "Solplanet CGI returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Failed to reach Solplanet CGI: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=502, detail=f"Failed to reach Solplanet CGI: {exc}") from exc
    except (TimeoutError, asyncio.TimeoutError) as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Solplanet CGI timed out: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=504, detail=f"Solplanet CGI timed out: {exc}") from exc


@app.put("/api/soulplanet/control/day-schedule/{day}/slots/{slot}")
@app.put("/api/solplanet/control/day-schedule/{day}/slots/{slot}")
async def put_solplanet_day_schedule_slot(day: str, slot: int, payload: SolplanetSlotPayload) -> dict[str, object]:
    day_key = _normalize_solplanet_day_key(day)
    safe_slot = _validate_solplanet_slot(slot)
    operation_run_id: int | None = None
    if (
        payload.enabled is None
        and payload.hour is None
        and payload.minute is None
        and payload.power is None
        and payload.mode is None
    ):
        raise HTTPException(status_code=400, detail="No fields to update")
    try:
        previous_schedule = await _solplanet_get_schedule_live()
        operation_run_id = await _start_operation_history_run(
            "solplanet.manual.set_day_schedule_slot",
            request_payload={"day": day_key, "slot": safe_slot, **payload.model_dump(mode="json")},
            previous_state={"state": _build_solplanet_control_state(previous_schedule)},
        )
        day_slots = _solplanet_day_slots(previous_schedule.get(day_key))
        index = safe_slot - 1
        current_decoded = _solplanet_decode_slot(day_slots[index])
        enabled = current_decoded["enabled"] if payload.enabled is None else payload.enabled
        if enabled:
            hour = int(current_decoded["hour"]) if payload.hour is None else payload.hour
            minute = int(current_decoded["minute"]) if payload.minute is None else payload.minute
            power = int(current_decoded["power"]) if payload.power is None else payload.power
            mode = int(current_decoded["mode"]) if payload.mode is None else payload.mode
            day_slots[index] = _solplanet_encode_slot(hour=hour, minute=minute, power=power, mode=mode)
        else:
            day_slots[index] = 0
        request_payload = {day_key: day_slots}
        cgi_write = await _solplanet_set_schedule_payload(request_payload)
        latest = await _solplanet_get_schedule_with_fallback()
        result = {
            "ok": True,
            "changed": [{"key": day_key, "slot": safe_slot, "value": day_slots[index]}],
            "request_payload": request_payload,
            "cgi_write": cgi_write,
            "state": _build_solplanet_control_state(latest),
        }
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_SUCCEEDED,
            latest_state={"state": result.get("state")},
            response_payload=result,
            completed=True,
        )
        return result
    except httpx.HTTPStatusError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Solplanet CGI returned {exc.response.status_code}",
            completed=True,
        )
        detail = {
            "message": "Solplanet CGI returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Failed to reach Solplanet CGI: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=502, detail=f"Failed to reach Solplanet CGI: {exc}") from exc
    except (TimeoutError, asyncio.TimeoutError) as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Solplanet CGI timed out: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=504, detail=f"Solplanet CGI timed out: {exc}") from exc


@app.put("/api/soulplanet/control/raw-setting")
@app.put("/api/solplanet/control/raw-setting")
async def put_solplanet_raw_setting(payload: SolplanetRawSettingPayload) -> dict[str, object]:
    if not payload.payload:
        raise HTTPException(status_code=400, detail="payload cannot be empty")
    operation_run_id: int | None = None
    try:
        previous_schedule = await _solplanet_get_schedule_with_fallback()
        operation_run_id = await _start_operation_history_run(
            "solplanet.manual.set_raw_setting",
            request_payload=payload.model_dump(mode="json"),
            previous_state={"state": _build_solplanet_control_state(previous_schedule)},
        )
        cgi_write = await _solplanet_set_schedule_payload(payload.payload)
        schedule = await _solplanet_get_schedule_with_fallback()
        result = {
            "ok": True,
            "changed": [{"key": "raw_payload", "value": payload.payload}],
            "request_payload": payload.payload,
            "cgi_write": cgi_write,
            "state": _build_solplanet_control_state(schedule),
        }
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_SUCCEEDED,
            latest_state={"state": result.get("state")},
            response_payload=result,
            completed=True,
        )
        return result
    except httpx.HTTPStatusError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Solplanet CGI returned {exc.response.status_code}",
            completed=True,
        )
        detail = {
            "message": "Solplanet CGI returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Failed to reach Solplanet CGI: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=502, detail=f"Failed to reach Solplanet CGI: {exc}") from exc
    except (TimeoutError, asyncio.TimeoutError) as exc:
        await _update_operation_history_run(
            operation_run_id,
            status=OPERATION_RUN_STATUS_FAILED,
            error_text=f"Solplanet CGI timed out: {exc}",
            completed=True,
        )
        raise HTTPException(status_code=504, detail=f"Solplanet CGI timed out: {exc}") from exc


@app.get("/api/soulplanet/energy-flow")
@app.get("/api/solplanet/energy-flow")
async def get_energy_flow_soulplanet() -> dict[str, object]:
    return await get_energy_flow("solplanet")


@app.get("/api/soulplanet/realtime-kv")
@app.get("/api/solplanet/realtime-kv")
async def get_solplanet_realtime_kv() -> dict[str, object]:
    items = await asyncio.to_thread(list_realtime_kv_rows, storage_db_path, prefix="solplanet.")
    update_map = await asyncio.to_thread(get_realtime_kv_by_prefix, storage_db_path, prefix="solplanet.update_time")
    update_item = update_map.get("solplanet.update_time")
    update_value = update_item.get("value") if isinstance(update_item, dict) else None
    updated_at = str(update_value) if update_value is not None else None
    return {
        "system": "solplanet",
        "updated_at": updated_at,
        "count": len(items),
        "items": items,
        "source": {"type": "realtime_kv"},
    }


@app.get("/api/soulplanet/cgi-dump")
@app.get("/api/solplanet/cgi-dump")
async def get_solplanet_cgi_dump() -> dict[str, object]:
    try:
        return await _get_solplanet_cgi_dump()
    except httpx.HTTPStatusError as exc:
        detail = {
            "message": "Solplanet CGI returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to reach Solplanet CGI: {exc}") from exc
    except (TimeoutError, asyncio.TimeoutError) as exc:
        raise HTTPException(status_code=504, detail=f"Solplanet CGI timed out: {exc}") from exc


@app.get("/api/soulplanet/cgi/getdev-device-2")
@app.get("/api/solplanet/cgi/getdev-device-2")
async def get_solplanet_getdev_device_2() -> dict[str, object]:
    started_at = monotonic()
    return await _solplanet_endpoint_response_from_snapshot(
        name="getdev_device_2",
        path="getdev.cgi?device=2",
        started_at=started_at,
    )


@app.get("/api/soulplanet/cgi/getdev-device-0")
@app.get("/api/solplanet/cgi/getdev-device-0")
async def get_solplanet_getdev_device_0() -> dict[str, object]:
    started_at = monotonic()
    return await _solplanet_endpoint_response_from_snapshot(
        name="getdev_device_0",
        path="getdev.cgi?device=0",
        started_at=started_at,
    )


@app.get("/api/soulplanet/cgi/getdev-device-3")
@app.get("/api/solplanet/cgi/getdev-device-3")
async def get_solplanet_getdev_device_3() -> dict[str, object]:
    started_at = monotonic()
    return await _solplanet_endpoint_response_from_snapshot(
        name="getdev_device_3",
        path="getdev.cgi?device=3",
        started_at=started_at,
    )


@app.get("/api/soulplanet/cgi/getdev-device-4")
@app.get("/api/solplanet/cgi/getdev-device-4")
async def get_solplanet_getdev_device_4() -> dict[str, object]:
    started_at = monotonic()
    return await _solplanet_endpoint_response_from_snapshot(
        name="getdev_device_4",
        path="getdev.cgi?device=4",
        started_at=started_at,
    )


@app.get("/api/soulplanet/cgi/getdevdata-device-2")
@app.get("/api/solplanet/cgi/getdevdata-device-2")
async def get_solplanet_getdevdata_device_2() -> dict[str, object]:
    started_at = monotonic()
    return await _solplanet_endpoint_response_from_snapshot(
        name="getdevdata_device_2",
        path="getdevdata.cgi?device=2",
        started_at=started_at,
    )


@app.get("/api/soulplanet/cgi/getdevdata-device-3")
@app.get("/api/solplanet/cgi/getdevdata-device-3")
async def get_solplanet_getdevdata_device_3() -> dict[str, object]:
    started_at = monotonic()
    return await _solplanet_endpoint_response_from_snapshot(
        name="getdevdata_device_3",
        path="getdevdata.cgi?device=3",
        started_at=started_at,
    )


@app.get("/api/soulplanet/cgi/getdevdata-device-4")
@app.get("/api/solplanet/cgi/getdevdata-device-4")
async def get_solplanet_getdevdata_device_4() -> dict[str, object]:
    started_at = monotonic()
    return await _solplanet_endpoint_response_from_snapshot(
        name="getdevdata_device_4",
        path="getdevdata.cgi?device=4",
        started_at=started_at,
    )


@app.get("/api/soulplanet/cgi/getdevdata-device-5")
@app.get("/api/solplanet/cgi/getdevdata-device-5")
async def get_solplanet_getdevdata_device_5() -> dict[str, object]:
    started_at = monotonic()
    return await _solplanet_endpoint_response_from_snapshot(
        name="getdevdata_device_5",
        path="getdevdata.cgi?device=5",
        started_at=started_at,
    )


@app.get("/api/soulplanet/cgi/getdefine")
@app.get("/api/solplanet/cgi/getdefine")
async def get_solplanet_getdefine() -> dict[str, object]:
    started_at = monotonic()
    return await _solplanet_endpoint_response_from_snapshot(
        name="getdefine",
        path="getdefine.cgi",
        started_at=started_at,
    )


@app.get("/api/solplanet/cgi/explore")
async def get_solplanet_cgi_explore() -> dict[str, object]:
    """Probe all getdev.cgi?device=N and getdevdata.cgi?device=N for N=1..7.

    Used to discover additional meters or devices not queried in the normal energy-flow path.
    Device IDs 2, 3, 4 are already known (inverter, meter, battery); devices 1, 5, 6, 7 may
    reveal a second smart meter or other components.
    """
    _ensure_solplanet_configured()
    started_at = monotonic()

    async def _probe(label: str, coro: object) -> tuple[str, dict[str, object] | None, str | None]:
        try:
            result = await asyncio.wait_for(coro, timeout=settings.solplanet_request_timeout_seconds)
            return label, result if isinstance(result, dict) else {}, None
        except Exception as exc:  # noqa: BLE001
            return label, None, f"{type(exc).__name__}: {exc}"

    probe_tasks: list[object] = []
    for dev_id in range(1, 8):
        probe_tasks.append(_probe(f"getdev_{dev_id}", solplanet_client.get_device_info(dev_id)))
        probe_tasks.append(_probe(f"getdevdata_{dev_id}", solplanet_client.get_device_data(dev_id)))

    results = await asyncio.gather(*probe_tasks)
    endpoints: dict[str, object] = {}
    for label, payload, error in results:
        dev_id_str = label.split("_")[-1]
        endpoint_type = "getdev" if label.startswith("getdev_") and not label.startswith("getdevdata_") else "getdevdata"
        endpoints[label] = {
            "path": f"{endpoint_type}.cgi?device={dev_id_str}",
            "ok": error is None,
            "error": error,
            "payload": payload,
        }

    return {
        "system": "solplanet",
        "source": {
            "type": "solplanet_cgi",
            "host": settings.solplanet_dongle_host,
            "port": settings.solplanet_dongle_port,
            "scheme": settings.solplanet_dongle_scheme,
        },
        "updated_at": datetime.now(UTC).isoformat(),
        "fetch_ms": round((monotonic() - started_at) * 1000, 1),
        "note": "Probes all device IDs 1-7. Known: device=2 inverter, device=3 meter, device=4 battery.",
        "endpoints": endpoints,
    }


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await _stop_collector()
    await solplanet_client.aclose()


@app.on_event("startup")
async def on_startup() -> None:
    global static_asset_version
    static_asset_version = await asyncio.to_thread(_compute_static_asset_version)
    if storage_db_path.exists():
        inspection = await asyncio.to_thread(inspect_sqlite_database, storage_db_path)
        if not bool(inspection.get("ok")):
            await _attempt_storage_db_recovery(
                trigger_error=RuntimeError(f"startup_integrity_check_failed: {inspection.get('integrity_check')}"),
                trace_system="combined",
                trace_round_id=None,
                operation_name="startup_db_health_check",
            )
    await asyncio.to_thread(init_db, storage_db_path)
    await asyncio.to_thread(upsert_operation_definitions, storage_db_path, definitions=list(MANUAL_OPERATION_DEFINITIONS))
    await asyncio.to_thread(migrate_worker_log_legacy_statuses, storage_db_path)
    await asyncio.to_thread(
        backfill_notification_entries_from_worker_logs,
        storage_db_path,
        system=NOTIFICATION_LOG_SYSTEM,
        service=NOTIFICATION_SUMMARY_SERVICE,
        active_statuses=(WORKER_LOG_STATUS_SEND, "notification", "notified", "alarmed"),
    )
    await _start_collector()


@app.get("/api/catalog/domains")
async def list_domains() -> dict[str, object]:
    _ensure_ha_configured()
    try:
        states = await ha_client.all_states()
        counter: Counter[str] = Counter()
        for state in states:
            entity_id = str(state.get("entity_id", ""))
            domain, _ = _split_entity_id(entity_id)
            if domain:
                counter[domain] += 1
        items = [{"domain": key, "count": counter[key]} for key in sorted(counter.keys())]
        return {"count": len(items), "items": items}
    except httpx.HTTPStatusError as exc:
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.get("/api/catalog/brands")
async def list_brands() -> dict[str, object]:
    _ensure_ha_configured()
    try:
        states = await ha_client.all_states()
        counter: Counter[str] = Counter()
        for state in states:
            entity_id = str(state.get("entity_id", ""))
            brand = _guess_brand_from_entity_id(entity_id)
            counter[brand] += 1
        items = [{"brand_guess": key, "count": counter[key]} for key in sorted(counter.keys())]
        return {"count": len(items), "items": items}
    except httpx.HTTPStatusError as exc:
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.get("/api/entities")
async def list_entities(
    domain: str | None = Query(default=None, description="Filter by entity domain: sensor/switch/number"),
    brand: str | None = Query(default=None, description="Filter by guessed brand prefix: saj/tesla/..."),
    q: str | None = Query(default=None, description="Substring match on entity_id or friendly_name"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=80, ge=1, le=500),
) -> dict[str, object]:
    _ensure_ha_configured()
    try:
        states = await ha_client.all_states()
        filtered: list[dict[str, object]] = []
        domain_lower = domain.lower() if domain else None
        brand_lower = brand.lower() if brand else None
        q_lower = q.lower() if q else None

        for state in states:
            item = _compact_entity_item(state)
            if domain_lower and item["domain"] != domain_lower:
                continue
            if brand_lower and item["brand_guess"] != brand_lower:
                continue
            if q_lower:
                friendly_name = str(item.get("friendly_name") or "").lower()
                entity_id = str(item["entity_id"]).lower()
                if q_lower not in entity_id and q_lower not in friendly_name:
                    continue
            filtered.append(item)

        filtered.sort(key=lambda item: str(item["entity_id"]))
        total = len(filtered)
        start = (page - 1) * page_size
        end = start + page_size
        page_items = filtered[start:end]

        return {
            "count": len(page_items),
            "total": total,
            "page": page,
            "page_size": page_size,
            "has_next": end < total,
            "has_prev": page > 1,
            "filters": {"domain": domain, "brand": brand, "q": q},
            "items": page_items,
        }
    except httpx.HTTPStatusError as exc:
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.get("/api/ha/ping")
async def ping_home_assistant() -> dict[str, object]:
    _ensure_ha_configured()
    try:
        payload = await ha_client.api_status()
        return {"ok": True, "ha": payload}
    except httpx.HTTPStatusError as exc:
        detail = {
            "ok": False,
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc
