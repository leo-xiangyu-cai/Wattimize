from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import traceback
from urllib.parse import urlparse
from collections import Counter
from contextvars import ContextVar
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import monotonic
from typing import Callable, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from pydantic import BaseModel, Field
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.responses import Response

from app.config import (
    ALLOWED_SAMPLE_INTERVAL_SECONDS,
    CONST_SAJ_SAMPLE_INTERVAL_SECONDS,
    CONST_SOLPLANET_SAMPLE_INTERVAL_SECONDS,
    Settings,
    config_file_exists,
    get_config_path,
    get_missing_required_fields_from_payload,
    load_settings,
    normalize_sample_interval_seconds,
    read_config_file,
    save_settings,
    settings_to_dict,
)
from app.home_assistant import HomeAssistantClient
from app.persistence import (
    DEFAULT_DB_PATH,
    EnergySample,
    count_pending_worker_api_logs,
    compute_daily_usage,
    compute_usage_between,
    expire_pending_worker_api_logs,
    export_samples_csv,
    finalize_pending_worker_api_logs_for_round,
    get_latest_raw_request_result,
    get_realtime_kv_by_prefix,
    get_latest_sample,
    get_series_samples,
    get_storage_status,
    insert_raw_request_result,
    insert_worker_api_log,
    import_samples_csv,
    init_db,
    insert_sample,
    list_raw_request_results,
    list_realtime_kv_rows,
    list_samples,
    list_worker_api_logs,
    update_worker_api_log,
    upsert_realtime_kv,
)
from app.solplanet_cgi import SolplanetCgiClient

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
TESLA_GRID_SUPPORT_WINDOW_START_HOUR = 11
TESLA_GRID_SUPPORT_WINDOW_END_HOUR = 14
TESLA_SOLAR_SURPLUS_WINDOW_START_HOUR = 18
TESLA_SOLAR_SURPLUS_WINDOW_END_HOUR = 20
TESLA_GRID_SUPPORT_TARGET_MIN_W = 14_000.0
TESLA_GRID_SUPPORT_TARGET_MAX_W = 15_000.0
TESLA_GRID_SUPPORT_HARD_MAX_W = 15_000.0
TESLA_GRID_SUPPORT_CURRENT_OPTIONS_A: tuple[int, ...] = (10, 15)
TESLA_SOLAR_SURPLUS_START_SOLPLANET_SOC_PERCENT = 95.0
TESLA_SOLAR_SURPLUS_STOP_SOLPLANET_SOC_PERCENT = 90.0
TESLA_SOLAR_SURPLUS_MIN_EXPORT_W = 150.0
SOLPLANET_LOW_BATTERY_NOTIFICATION_SOC_PERCENT = 20.0
SOLAR_SURPLUS_EXPORT_ENERGY_NOTIFICATION_THRESHOLD_WH = 9_000.0
TESLA_CURRENT_MISMATCH_RESTART_MIN_DELTA_A = 2.0
TESLA_CURRENT_MISMATCH_RESTART_COOLDOWN_SECONDS = 300.0
WORKER_PENDING_LOG_TIMEOUT_SECONDS = 60.0
TESLA_OBSERVATION_SERVICE = "tesla_home_assistant_collection"
MIDDAY_WINDOW_CHECK_SERVICE = "worker_midday_window_check"

app = FastAPI(title="Wattimize API", version="0.1.0")
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

solplanet_flow_cache: dict[str, object] = {"payload": None, "at_monotonic": 0.0}
solplanet_flow_lock = asyncio.Lock()
solplanet_context_cache: dict[str, object] = {"payload": None, "at_monotonic": 0.0}
solplanet_context_lock = asyncio.Lock()
runtime_lock = asyncio.Lock()
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
        "last_duration_ms": None,
        "success_count": 0,
        "failure_count": 0,
    },
}
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
    saj_status = "pending" if _endpoint_is_eligible("saj", saj_endpoint, round_number) else "skipped"
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
            "error_text": "endpoint_backoff" if saj_status == "skipped" else None,
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
        endpoint_status = "pending" if _endpoint_is_eligible("solplanet", endpoint, round_number) else "skipped"
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
                "error_text": "endpoint_backoff" if endpoint_status == "skipped" else None,
            }
        )

    for service in ("combined_assembly", TESLA_OBSERVATION_SERVICE, MIDDAY_WINDOW_CHECK_SERVICE):
        api_link = f"worker://combined/{service}"
        plan.append(
            {
                "request_token": _worker_log_request_token(round_id, service, "AUTO", api_link),
                "round_id": round_id,
                "worker": "worker",
                "system": "combined",
                "service": service,
                "method": "AUTO",
                "api_link": api_link,
                "requested_at_utc": requested_at_utc,
                "ok": False,
                "status": "pending",
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
        await asyncio.to_thread(insert_worker_api_log, storage_db_path, **item)


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
                status="ok" if ok else "failed",
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
                status="ok" if ok else "failed",
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


def _compact_round_result_for_status(payload: dict[str, object]) -> dict[str, object]:
    flow = payload.get("flow")
    flow_map = flow if isinstance(flow, dict) else {}
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

    return {
        "system": "combined",
        "updated_at": flow.get("updated_at"),
        "display": display,
        "tesla": {
            "charging": {
                "power_w": _to_number(metrics.get("tesla_charge_power_w")),
                "current_amps": _to_number(metrics.get("tesla_charge_current_amps")),
                "configured_current_amps": _to_number(metrics.get("tesla_configured_current_amps")),
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
                "level_percent": _to_number(metrics.get("tesla_battery_soc_percent")),
            },
            "control": {
                "available": tesla_control_state_map.get("available"),
                "control_mode": tesla_control_state_map.get("control_mode"),
                "charging_enabled": tesla_control_state_map.get("charging_enabled"),
                "charge_requested_enabled": tesla_control_state_map.get("charge_requested_enabled"),
                "can_start": tesla_control_state_map.get("can_start"),
                "can_stop": tesla_control_state_map.get("can_stop"),
            },
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
    await ha_client.call_service("number", "set_value", {"entity_id": entity_id, "value": int(amps)})
    refreshed_states = await _tesla_control_states()
    refreshed_control_state = _build_tesla_control_state(refreshed_states)
    return {
        "ok": True,
        "requested_charge_current_amps": int(amps),
        "control_state": refreshed_control_state,
    }


def _tesla_control_state_charge_power_w(control_state: dict[str, object]) -> float:
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

    current_entity = control_state.get("charge_current_sensor_entity")
    current_a = _to_number(current_entity.get("state")) if isinstance(current_entity, dict) else None
    voltage_entity = control_state.get("charge_voltage_entity")
    voltage_v = _to_number(voltage_entity.get("state")) if isinstance(voltage_entity, dict) else None
    if current_a is None:
        current_number_entity = control_state.get("charge_current_number_entity")
        current_a = _to_number(current_number_entity.get("state")) if isinstance(current_number_entity, dict) else None
    if current_a is None or current_a <= 0:
        return 0.0
    if voltage_v is None or voltage_v < 100:
        voltage_v = TESLA_ASSUMED_CHARGING_VOLTAGE_V
    return max(current_a * voltage_v, 0.0)


def _is_within_tesla_grid_support_window(now_local: datetime) -> bool:
    hour = now_local.hour
    return TESLA_GRID_SUPPORT_WINDOW_START_HOUR <= hour < TESLA_GRID_SUPPORT_WINDOW_END_HOUR


def _is_within_tesla_solar_surplus_window(now_local: datetime) -> bool:
    hour = now_local.hour
    return TESLA_SOLAR_SURPLUS_WINDOW_START_HOUR <= hour < TESLA_SOLAR_SURPLUS_WINDOW_END_HOUR


def _tesla_midday_window_mode(now_local: datetime) -> Literal["grid_support", "solar_surplus", "off"]:
    if _is_within_tesla_grid_support_window(now_local):
        return "grid_support"
    if _is_within_tesla_solar_surplus_window(now_local):
        return "solar_surplus"
    return "off"


def _window_schedule_text(window_mode: str) -> str:
    if window_mode == "grid_support":
        return (
            f"{TESLA_GRID_SUPPORT_WINDOW_START_HOUR:02d}:00-"
            f"{TESLA_GRID_SUPPORT_WINDOW_END_HOUR:02d}:00"
        )
    if window_mode == "solar_surplus":
        return (
            f"{TESLA_SOLAR_SURPLUS_WINDOW_START_HOUR:02d}:00-"
            f"{TESLA_SOLAR_SURPLUS_WINDOW_END_HOUR:02d}:00"
        )
    return "outside_configured_windows"


def _append_worker_notification(payload: dict[str, object], notification: dict[str, object]) -> None:
    notifications = payload.get("notifications")
    if isinstance(notifications, list):
        notifications.append(notification)
    else:
        payload["notifications"] = [notification]
    payload["notification"] = notification


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
    threshold_notified = bool(combined_status.get("solar_surplus_export_threshold_notified"))
    added_export_wh = 0.0

    if last_window_key != window_key:
        total_export_wh = 0.0
        threshold_notified = False
        last_tracked_at_text = ""

    current_utc = datetime.now(UTC)
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
    combined_status["solar_surplus_export_threshold_notified"] = threshold_notified
    return {
        "window_key": window_key,
        "interval_seconds": round(dt_seconds, 1),
        "added_export_wh": round(added_export_wh, 3),
        "total_export_wh": round(total_export_wh, 3),
        "total_export_kwh": round(total_export_wh / 1000.0, 4),
        "threshold_wh": SOLAR_SURPLUS_EXPORT_ENERGY_NOTIFICATION_THRESHOLD_WH,
        "threshold_kwh": round(SOLAR_SURPLUS_EXPORT_ENERGY_NOTIFICATION_THRESHOLD_WH / 1000.0, 3),
        "threshold_notified": threshold_notified,
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
    valid_candidates = [
        candidate
        for candidate in candidates
        if float(candidate["predicted_grid_w"]) <= TESLA_GRID_SUPPORT_TARGET_MAX_W
    ]
    if not valid_candidates:
        return candidates[0]
    return max(valid_candidates, key=lambda candidate: float(candidate["predicted_grid_w"]))


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
    valid_candidates = [
        candidate
        for candidate in candidates
        if float(candidate["predicted_grid_w"]) <= TESLA_GRID_SUPPORT_TARGET_MAX_W
    ]
    if not valid_candidates:
        return candidates[0]
    return max(valid_candidates, key=lambda candidate: float(candidate["predicted_grid_w"]))


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

    preferred_values = tuple(
        amps
        for amps in TESLA_GRID_SUPPORT_CURRENT_OPTIONS_A
        if min_i <= amps <= max_i and ((amps - min_i) % step_i == 0)
    )
    if preferred_values:
        return preferred_values

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
        "grid_support_window_active": window_mode == "grid_support",
        "solar_surplus_window_active": window_mode == "solar_surplus",
        "target_grid_min_w": TESLA_GRID_SUPPORT_TARGET_MIN_W,
        "target_grid_max_w": TESLA_GRID_SUPPORT_TARGET_MAX_W,
        "hard_grid_max_w": TESLA_GRID_SUPPORT_HARD_MAX_W,
        "assumed_voltage_v": TESLA_ASSUMED_CHARGING_VOLTAGE_V,
    }
    status = "ok"
    ok = True
    error_text: str | None = None
    try:
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
            combined_status["solar_surplus_export_last_tracked_at"] = None
            result["decision"] = {
                "mode": "off",
                "charge_current_amps": 0,
                "predicted_grid_w": None,
            }
            result["decision_reason"] = "outside_window_force_stop"
            if charge_requested_enabled or charging_enabled:
                await _tesla_set_charging(False)
                result["action"] = {"type": "stop_charging", "reason": "outside_window"}
                status = "applied"
            else:
                result["action"] = {"type": "noop", "reason": "outside_window_already_stopped"}
                status = "noop"
            return result

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

        if connection_state == "unplugged":
            result["skipped"] = "tesla_unplugged"
            status = "skipped"
            return result

        if status_text in ("disconnected", "unknown", "unavailable"):
            result["skipped"] = "tesla_not_ready"
            result["tesla_status"] = status_text
            status = "skipped"
            return result

        if window_mode == "solar_surplus":
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
            result["solar_surplus"] = {
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
            if solplanet_soc_percent < SOLPLANET_LOW_BATTERY_NOTIFICATION_SOC_PERCENT:
                _append_worker_notification(result, {
                    "level": "warning",
                    "code": "solplanet_low_battery",
                    "target": "solplanet_battery",
                    "threshold_soc_percent": SOLPLANET_LOW_BATTERY_NOTIFICATION_SOC_PERCENT,
                    "current_soc_percent": round(solplanet_soc_percent, 1),
                    "message": (
                        "Solplanet battery SOC is below 20%; keep one reminder in worklog "
                        "until notification handling is implemented."
                    ),
                })
            combined_status = collector_status.setdefault("combined", {})
            was_importing = bool(combined_status.get("grid_import_active"))
            is_importing = current_grid_import_w > 0.0
            if is_importing and not was_importing:
                _append_worker_notification(result, {
                    "level": "alarm",
                    "code": "grid_import_started",
                    "target": "grid",
                    "current_grid_import_w": round(current_grid_import_w, 1),
                    "message": (
                        "Grid import started during the third worker window; add one alarm "
                        "reminder to worklog."
                    ),
                })
            combined_status["grid_import_active"] = is_importing
            export_tracking = _update_solar_surplus_export_energy_tracking(
                combined_status=combined_status,
                now_local=now_local,
                current_grid_export_w=current_grid_export_w,
            )
            result["solar_surplus_export_tracking"] = export_tracking
            previous_total_export_wh = float(export_tracking["total_export_wh"]) - float(export_tracking["added_export_wh"])
            if (
                previous_total_export_wh < SOLAR_SURPLUS_EXPORT_ENERGY_NOTIFICATION_THRESHOLD_WH
                and float(export_tracking["total_export_wh"]) >= SOLAR_SURPLUS_EXPORT_ENERGY_NOTIFICATION_THRESHOLD_WH
                and not bool(combined_status.get("solar_surplus_export_threshold_notified"))
            ):
                _append_worker_notification(result, {
                    "level": "alarm",
                    "code": "solar_surplus_export_energy_reached",
                    "target": "grid_export_energy",
                    "window": _window_schedule_text("solar_surplus"),
                    "current_export_total_wh": round(float(export_tracking["total_export_wh"]), 1),
                    "current_export_total_kwh": round(float(export_tracking["total_export_wh"]) / 1000.0, 4),
                    "threshold_wh": SOLAR_SURPLUS_EXPORT_ENERGY_NOTIFICATION_THRESHOLD_WH,
                    "threshold_kwh": round(SOLAR_SURPLUS_EXPORT_ENERGY_NOTIFICATION_THRESHOLD_WH / 1000.0, 3),
                    "message": (
                        "Exported energy during the third worker window reached the configured "
                        "threshold; add one alarm reminder to worklog."
                    ),
                })
                combined_status["solar_surplus_export_threshold_notified"] = True

            desired = _choose_tesla_solar_surplus_target_from_options(
                base_grid_without_tesla_w,
                available_current_options,
            )
            desired_reason = "use_exported_solar_without_importing_grid_when_available"
            active_or_requested = charging_enabled or charge_requested_enabled
            should_start = can_start_from_soc
            should_continue = active_or_requested and can_continue_from_soc
            preferred_solar_surplus_amps = (
                int(max(available_current_options))
                if available_current_options
                else 0
            )
            if (should_start or should_continue) and preferred_solar_surplus_amps > 0:
                desired = {
                    "mode": "charge",
                    "charge_current_amps": preferred_solar_surplus_amps,
                    "tesla_power_w": float(preferred_solar_surplus_amps) * TESLA_ASSUMED_CHARGING_VOLTAGE_V,
                    "predicted_grid_w": round(
                        base_grid_without_tesla_w + (float(preferred_solar_surplus_amps) * TESLA_ASSUMED_CHARGING_VOLTAGE_V),
                        1,
                    ),
                }
                desired_reason = (
                    "start_from_solplanet_soc_threshold_with_maximum_current"
                    if should_start and not active_or_requested
                    else "keep_maximum_current_until_solplanet_soc_below_stop_threshold"
                )
            elif not should_continue and not should_start:
                desired = {
                    "mode": "off",
                    "charge_current_amps": 0,
                    "tesla_power_w": 0.0,
                    "predicted_grid_w": round(base_grid_without_tesla_w, 1),
                }
                desired_reason = (
                    "wait_for_solplanet_soc_start_threshold"
                    if not active_or_requested
                    else "solplanet_soc_below_stop_threshold"
                )

            result["decision"] = desired
            result["decision_reason"] = desired_reason
            result["candidates"] = [
                {
                    "mode": "off" if amps == 0 else "charge",
                    "charge_current_amps": amps,
                    "predicted_grid_w": round(
                        base_grid_without_tesla_w + (float(amps) * TESLA_ASSUMED_CHARGING_VOLTAGE_V),
                        1,
                    ),
                    "uses_only_surplus_solar": (
                        base_grid_without_tesla_w + (float(amps) * TESLA_ASSUMED_CHARGING_VOLTAGE_V)
                    ) <= 0.0,
                }
                for amps in (0, *available_current_options)
            ]

            desired_amps = int(desired["charge_current_amps"])
            configured_matches_desired = (
                current_configured_amps is not None
                and int(round(current_configured_amps)) == desired_amps
            )
            actual_matches_desired = (
                current_actual_amps is not None
                and int(round(current_actual_amps)) == desired_amps
            )
            effective_current_amps = current_actual_amps or current_configured_amps or 0.0
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

            if (
                desired["mode"] != "off"
                and charging_enabled
                and configured_matches_desired
                and (current_actual_amps is None or actual_matches_desired)
            ):
                result["decision"] = {
                    "mode": "hold_current_state",
                    "charge_current_amps": int(round(effective_current_amps)) if effective_current_amps > 0 else desired_amps,
                    "predicted_grid_w": round(float(grid_w), 1),
                }
                result["decision_reason"] = "already_running_with_allowed_current"
                result["action"] = {"type": "noop", "reason": "keep_current_state"}
                status = "noop"
                return result

            if desired["mode"] == "off":
                if active_or_requested:
                    await _tesla_set_charging(False)
                    result["action"] = {"type": "stop_charging", "reason": desired_reason}
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
                result["action"] = {"type": "noop", "reason": "already_at_target"}
                status = "noop"
            return result

        base_grid_w = max(float(result["current_grid_import_w"]) - tesla_charge_power_w, 0.0)
        result["base_grid_without_tesla_w"] = round(base_grid_w, 1)
        desired = _choose_tesla_grid_support_target_from_options(base_grid_w, available_current_options)
        result["decision"] = desired
        result["decision_reason"] = "selected_max_safe_candidate_under_grid_cap"

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
        await _persist_worker_control_log(
            round_id=round_id,
            system="combined",
            service=MIDDAY_WINDOW_CHECK_SERVICE,
            requested_at_utc=requested_at_utc,
            started_monotonic=started_monotonic,
            ok=ok,
            status=status,
            payload=result,
            error_text=error_text,
        )


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
        observation = _build_tesla_observation_payload(states)
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
            system="combined",
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

    updated_at = datetime.now(UTC).isoformat()
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
    if not config_file_exists():
        return ["ha_url", "ha_token"]
    file_payload = read_config_file()
    return get_missing_required_fields_from_payload(file_payload)


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


async def _store_realtime_kv_snapshot(system: str, flow: dict[str, object]) -> None:
    rows: list[tuple[str, str, str]] = []
    _flatten_payload_to_kv(system=system, path="", value=flow, out_rows=rows)

    updated_at = str(flow.get("updated_at") or datetime.now(UTC).isoformat())
    rows.append((f"{system}.update_time", json.dumps(updated_at, ensure_ascii=False), "collector"))
    await asyncio.to_thread(upsert_realtime_kv, storage_db_path, rows)


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


async def _store_flow_sample(system: str, flow: dict[str, object], *, round_id: str) -> None:
    sample = _sample_from_flow(system, flow, round_id=round_id)
    await asyncio.to_thread(insert_sample, storage_db_path, sample)
    await _store_realtime_kv_snapshot(system, flow)


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


def _collector_mark_finish(system: str, started_monotonic: float, *, error: Exception | None = None) -> None:
    now_iso = datetime.now(UTC).isoformat()
    status = collector_status.setdefault(system, {})
    status["in_progress"] = False
    status["last_finished_at"] = now_iso
    status["last_duration_ms"] = round((monotonic() - started_monotonic) * 1000, 1)
    if error is None:
        status["last_success_at"] = now_iso
        status["last_error"] = None
        status["success_count"] = int(status.get("success_count") or 0) + 1
        return
    status["last_error_at"] = now_iso
    status["last_error"] = f"{type(error).__name__}: {error}"
    status["failure_count"] = int(status.get("failure_count") or 0) + 1


def _collector_sleep_seconds(now_monotonic: float) -> float:
    return COLLECTOR_ROUND_SLEEP_SECONDS


async def _await_round_request_log_settlement(
    round_id: str,
    *,
    services: tuple[str, ...],
    settle_timeout_seconds: float = 2.0,
    poll_interval_seconds: float = 0.05,
    timeout_error_text: str,
) -> int:
    deadline = monotonic() + max(0.0, settle_timeout_seconds)
    while monotonic() < deadline:
        pending_count = await asyncio.to_thread(
            count_pending_worker_api_logs,
            storage_db_path,
            round_id=round_id,
            services=services,
        )
        if pending_count <= 0:
            return 0
        await asyncio.sleep(poll_interval_seconds)
    return await asyncio.to_thread(
        finalize_pending_worker_api_logs_for_round,
        storage_db_path,
        round_id=round_id,
        services=services,
        status="timeout",
        error_text=timeout_error_text,
    )


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
    if service == MIDDAY_WINDOW_CHECK_SERVICE:
        action = payload.get("action")
        action_map = action if isinstance(action, dict) else {}
        decision = payload.get("decision")
        decision_map = decision if isinstance(decision, dict) else {}
        tesla_before = payload.get("tesla_state_before")
        tesla_before_map = tesla_before if isinstance(tesla_before, dict) else {}
        solar_surplus = payload.get("solar_surplus")
        solar_surplus_map = solar_surplus if isinstance(solar_surplus, dict) else {}
        notifications = payload.get("notifications")
        notification_list = [item for item in notifications if isinstance(item, dict)] if isinstance(notifications, list) else []
        if not notification_list:
            notification = payload.get("notification")
            if isinstance(notification, dict):
                notification_list = [notification]
        window_mode = str(payload.get("window_mode") or "off")
        window_logic = {
            "grid_support": "grid_support",
            "solar_surplus": "solar_surplus",
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
            f"logic={window_logic}, active={'yes' if payload.get('window_active') else 'no'}"
        )
        tesla_text = (
            f"tesla now state={tesla_before_text}, request={tesla_before_request}, status={tesla_before_status}, "
            f"actual_current={tesla_before_current}, configured_current={tesla_before_configured}, power={tesla_before_power}"
        )
        battery_text = (
            "batteries "
            f"saj_soc={_fmt_result_number(solar_surplus_map.get('saj_soc_percent'), 0, '%')}, "
            f"solplanet_soc={_fmt_result_number(solar_surplus_map.get('solplanet_soc_percent'), 0, '%')}, "
            f"start_threshold={_fmt_result_number(solar_surplus_map.get('start_soc_percent'), 0, '%')}, "
            f"stop_threshold={_fmt_result_number(solar_surplus_map.get('stop_soc_percent'), 0, '%')}"
        )
        if not solar_surplus_map:
            battery_text = (
                "batteries "
                f"saj_soc={_fmt_result_number(payload.get('battery1_soc_percent'), 0, '%')}, "
                f"solplanet_soc={_fmt_result_number(payload.get('battery2_soc_percent'), 0, '%')}"
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
        if solar_surplus_map:
            extra_parts.append(
                f"solar pv={_fmt_result_number(solar_surplus_map.get('pv_w'), 0, 'W')}, "
                f"load={_fmt_result_number(solar_surplus_map.get('load_w'), 0, 'W')}, "
                f"excess={_fmt_result_number(solar_surplus_map.get('solar_excess_vs_load_w'), 0, 'W')}, "
                f"export_signal={'yes' if solar_surplus_map.get('export_signal_active') else 'no'}, "
                f"solar_signal={'yes' if solar_surplus_map.get('solar_signal_active') else 'no'}"
            )
        export_tracking = payload.get("solar_surplus_export_tracking")
        export_tracking_map = export_tracking if isinstance(export_tracking, dict) else {}
        if export_tracking_map:
            extra_parts.append(
                "window_export "
                f"total={_fmt_result_number(export_tracking_map.get('total_export_kwh'), 3, 'kWh')}, "
                f"added={_fmt_result_number(export_tracking_map.get('added_export_wh'), 0, 'Wh')}, "
                f"threshold={_fmt_result_number(export_tracking_map.get('threshold_kwh'), 3, 'kWh')}"
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
            if code == "grid_import_started":
                extra_parts.append(
                    "notification "
                    f"{code}: "
                    f"grid_import={_fmt_result_number(notification_map.get('current_grid_import_w'), 0, 'W')}"
                )
                continue
            if code == "solar_surplus_export_energy_reached":
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
            f"grid {_fmt_result_number(combined_map.get('grid_w'), 0, 'W')}, "
            f"load {_fmt_result_number(combined_map.get('load_w'), 0, 'W')}, "
            f"pv {_fmt_result_number(combined_map.get('pv_w'), 0, 'W')}, "
            f"battery {_fmt_result_number(combined_map.get('battery_w'), 0, 'W')}"
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
) -> None:
    normalized_round_id = str(round_id or "").strip() or str(request_round_ctx.get() or "").strip()
    api_link = f"worker://{system}/{service}"
    request_token = _worker_log_request_token(normalized_round_id, service, "AUTO", api_link)
    result_text = _worker_control_result_text(service=service, status=status, payload=payload, error_text=error_text)
    try:
        if request_token:
            await asyncio.to_thread(
                update_worker_api_log,
                storage_db_path,
                request_token=request_token,
                ok=ok,
                status=status,
                status_code=None,
                duration_ms=round((monotonic() - started_monotonic) * 1000, 1),
                result_text=result_text,
                error_text=error_text,
            )
            return
        await asyncio.to_thread(
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
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to persist worker control log for %s/%s: %s", system, service, exc)


async def _collector_loop() -> None:
    global collector_round_number  # noqa: PLW0603
    while not collector_stop_event.is_set():
        collector_round_number += 1
        round_number = collector_round_number
        round_started_at_utc = datetime.now(UTC).isoformat()
        round_id = f"round-{round_number}-{int(datetime.fromisoformat(round_started_at_utc).timestamp() * 1000)}"
        await _insert_worker_round_log_plan(round_number, round_id, round_started_at_utc)
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
                _collector_mark_finish(system, started_monotonic)
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

        source_pending_timeouts = await _await_round_request_log_settlement(
            round_id,
            services=("home_assistant", "solplanet_cgi"),
            timeout_error_text="worker_source_request_timeout",
        )
        if source_pending_timeouts:
            collector_status.setdefault("combined", {})["last_source_pending_timeout_count"] = source_pending_timeouts

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
            saj_flow, saj_source_detail = await _resolve_combined_source_flow("saj", saj_result)
            solplanet_flow, solplanet_source_detail = await _resolve_combined_source_flow("solplanet", solplanet_result)
            if saj_flow and solplanet_flow:
                combined_flow = _build_combined_flow_payload(saj_flow, solplanet_flow, tesla_observation_result)
                combined_source = combined_flow.get("source")
                combined_source_map = combined_source if isinstance(combined_source, dict) else {}
                combined_source_map["source_details"] = {
                    "saj": saj_source_detail,
                    "solplanet": solplanet_source_detail,
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
                        "source_details": {
                            "saj": saj_source_detail,
                            "solplanet": solplanet_source_detail,
                        },
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
                        "source_details": {
                            "saj": saj_source_detail,
                            "solplanet": solplanet_source_detail,
                        },
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
                    f"saj={saj_source_detail.get('origin')}:{saj_source_detail.get('reason')}; "
                    f"solplanet={solplanet_source_detail.get('origin')}:{solplanet_source_detail.get('reason')}"
                )
                round_results["combined"] = {
                    "attempted": ["combined_assembly"],
                    "succeeded": [],
                    "failed": [],
                    "skipped": ["combined_assembly"],
                    "stored_sample": False,
                    "flow": None,
                    "reason": "source_flow_unavailable",
                    "source_details": {
                        "saj": saj_source_detail,
                        "solplanet": solplanet_source_detail,
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
                stage=MIDDAY_WINDOW_CHECK_SERVICE,
                error=window_exc,
            )
            collector_status.setdefault("combined", {})["last_midday_window_check"] = {
                "error": f"{type(window_exc).__name__}: {window_exc}",
                "executed_at": datetime.now(UTC).isoformat(),
            }

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

        timed_out_pending = await _await_round_request_log_settlement(
            round_id,
            services=("combined_assembly", TESLA_OBSERVATION_SERVICE, MIDDAY_WINDOW_CHECK_SERVICE),
            timeout_error_text="worker_round_incomplete",
        )
        if timed_out_pending:
            collector_status.setdefault("combined", {})["last_round_timeout_count"] = timed_out_pending

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
    return await _restart_collector()


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


@app.get("/api/worker/logs")
async def worker_logs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    system: str | None = Query(default=None, description="saj or solplanet"),
    service: str | None = Query(default=None, description="home_assistant or solplanet_cgi"),
    status: str | None = Query(default=None, description="ok/failed/pending/skipped/applied/noop/timeout"),
    exclude_status: str | None = Query(default=None, description="comma-separated statuses to exclude"),
    category: str | None = Query(default=None, description="all/saj/solplanet/combined/tesla"),
) -> dict[str, object]:
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
    if normalized_category in ("all", ""):
        normalized_category = ""
    if normalized_category == "saj":
        normalized_system = "saj"
        normalized_service = None
    elif normalized_category == "solplanet":
        normalized_system = "solplanet"
        normalized_service = None
    elif normalized_category == "combined":
        normalized_system = "combined"
        normalized_service = "combined_assembly"
    elif normalized_category == "tesla":
        normalized_system = None
        normalized_service = TESLA_OBSERVATION_SERVICE
    elif system:
        normalized_system = _normalize_system_name(system)
    elif normalized_category:
        raise HTTPException(
            status_code=400,
            detail="category must be one of: all, saj, solplanet, combined, tesla",
        )
    elif system:
        normalized_system = _normalize_system_name(system)
    await asyncio.to_thread(
        expire_pending_worker_api_logs,
        storage_db_path,
        older_than_epoch=datetime.now(UTC).timestamp() - WORKER_PENDING_LOG_TIMEOUT_SECONDS,
        status="timeout",
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
    return await asyncio.to_thread(
        compute_usage_between,
        storage_db_path,
        system=normalized,
        start_at_utc=start_at,
        end_at_utc=end_at,
        sample_interval_seconds=_sample_interval_for_system(normalized),
    )


@app.get("/api/storage/export.csv")
async def storage_export_csv() -> Response:
    csv_text = await asyncio.to_thread(export_samples_csv, storage_db_path)
    exported_at = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    headers = {"Content-Disposition": f'attachment; filename="energy_samples_{exported_at}.csv"'}
    return Response(content=csv_text, media_type="text/csv; charset=utf-8", headers=headers)


@app.post("/api/storage/import.csv")
async def storage_import_csv(
    file: UploadFile = File(...),
    replace_existing: bool = Query(default=True, description="clear existing rows before import"),
) -> dict[str, object]:
    raw = await file.read()
    try:
        csv_text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail="CSV must be UTF-8 encoded") from exc

    try:
        result = await asyncio.to_thread(import_samples_csv, storage_db_path, csv_text, replace_existing=replace_existing)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    status = await asyncio.to_thread(get_storage_status, storage_db_path, sample_interval_seconds)
    status["saj_sample_interval_seconds"] = sample_interval_seconds
    status["solplanet_sample_interval_seconds"] = solplanet_sample_interval_seconds
    return {"ok": True, **result, "storage_status": status}


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


@app.get("/")
async def frontend_index() -> Response:
    index_file = static_dir / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
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
    flow = await _get_energy_flow_from_realtime_kv(normalized)
    if normalized == "combined":
        return _build_public_combined_flow(flow)
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
        return {
            "system": "tesla",
            "control_state": _build_tesla_control_state(states),
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
    try:
        return await _tesla_set_charging(payload.enabled)
    except httpx.HTTPStatusError as exc:
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
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
    try:
        await _saj_set_number("number.saj_app_mode_input", payload.mode_code)
        _, states_by_id = await _saj_control_states()
        return {
            "ok": True,
            "changed": [{"entity_id": "number.saj_app_mode_input", "value": payload.mode_code}],
            "state": _build_saj_control_state(states_by_id),
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


@app.put("/api/saj/control/profile")
async def put_saj_control_profile(payload: SajProfilePayload) -> dict[str, object]:
    _ensure_ha_configured()
    try:
        return await _saj_apply_profile(payload.profile_id)
    except httpx.HTTPStatusError as exc:
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.put("/api/saj/control/charge-slots/{slot}")
async def put_saj_charge_slot(slot: int, payload: SajSlotPayload) -> dict[str, object]:
    _ensure_ha_configured()
    try:
        return await _saj_apply_slot("charge", slot, payload)
    except httpx.HTTPStatusError as exc:
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.put("/api/saj/control/discharge-slots/{slot}")
async def put_saj_discharge_slot(slot: int, payload: SajSlotPayload) -> dict[str, object]:
    _ensure_ha_configured()
    try:
        return await _saj_apply_slot("discharge", slot, payload)
    except httpx.HTTPStatusError as exc:
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.put("/api/saj/control/toggles")
async def put_saj_control_toggles(payload: SajTogglePayload) -> dict[str, object]:
    _ensure_ha_configured()
    changed: list[dict[str, object]] = []
    try:
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
        return {"ok": True, "changed": changed, "state": _build_saj_control_state(states_by_id)}
    except httpx.HTTPStatusError as exc:
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.put("/api/saj/control/limits")
async def put_saj_control_limits(payload: SajLimitsPayload) -> dict[str, object]:
    _ensure_ha_configured()
    changed: list[dict[str, object]] = []
    try:
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
        return {"ok": True, "changed": changed, "state": _build_saj_control_state(states_by_id)}
    except httpx.HTTPStatusError as exc:
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


@app.post("/api/saj/control/refresh-touch")
async def post_saj_control_refresh_touch() -> dict[str, object]:
    _ensure_ha_configured()
    entity_id = "switch.saj_passive_charge_control"
    try:
        kept_state = await _saj_touch_switch(entity_id)
        _, states_by_id = await _saj_control_states()
        return {
            "ok": True,
            "entity_id": entity_id,
            "kept_state": kept_state,
            "state": _build_saj_control_state(states_by_id),
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
    if payload.pin is not None:
        update_payload["Pin"] = payload.pin
        changed.append({"key": "Pin", "value": payload.pin})
    if payload.pout is not None:
        update_payload["Pout"] = payload.pout
        changed.append({"key": "Pout", "value": payload.pout})
    if not update_payload:
        raise HTTPException(status_code=400, detail="No fields to update")
    try:
        cgi_write = await _solplanet_set_schedule_payload(update_payload)
        schedule = await _solplanet_get_schedule_with_fallback()
        return {
            "ok": True,
            "changed": changed,
            "request_payload": update_payload,
            "cgi_write": cgi_write,
            "state": _build_solplanet_control_state(schedule),
        }
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


@app.put("/api/soulplanet/control/day-schedule/{day}")
@app.put("/api/solplanet/control/day-schedule/{day}")
async def put_solplanet_day_schedule(day: str, payload: SolplanetDaySchedulePayload) -> dict[str, object]:
    day_key = _normalize_solplanet_day_key(day)
    slots = _validate_solplanet_day_slots(payload.slots)
    try:
        request_payload = {day_key: slots}
        cgi_write = await _solplanet_set_schedule_payload(request_payload)
        schedule = await _solplanet_get_schedule_with_fallback()
        return {
            "ok": True,
            "changed": [{"key": day_key, "value": slots}],
            "request_payload": request_payload,
            "cgi_write": cgi_write,
            "state": _build_solplanet_control_state(schedule),
        }
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


@app.put("/api/soulplanet/control/day-schedule/{day}/slots/{slot}")
@app.put("/api/solplanet/control/day-schedule/{day}/slots/{slot}")
async def put_solplanet_day_schedule_slot(day: str, slot: int, payload: SolplanetSlotPayload) -> dict[str, object]:
    day_key = _normalize_solplanet_day_key(day)
    safe_slot = _validate_solplanet_slot(slot)
    if (
        payload.enabled is None
        and payload.hour is None
        and payload.minute is None
        and payload.power is None
        and payload.mode is None
    ):
        raise HTTPException(status_code=400, detail="No fields to update")
    try:
        schedule = await _solplanet_get_schedule_live()
        day_slots = _solplanet_day_slots(schedule.get(day_key))
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
        return {
            "ok": True,
            "changed": [{"key": day_key, "slot": safe_slot, "value": day_slots[index]}],
            "request_payload": request_payload,
            "cgi_write": cgi_write,
            "state": _build_solplanet_control_state(latest),
        }
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


@app.put("/api/soulplanet/control/raw-setting")
@app.put("/api/solplanet/control/raw-setting")
async def put_solplanet_raw_setting(payload: SolplanetRawSettingPayload) -> dict[str, object]:
    if not payload.payload:
        raise HTTPException(status_code=400, detail="payload cannot be empty")
    try:
        cgi_write = await _solplanet_set_schedule_payload(payload.payload)
        schedule = await _solplanet_get_schedule_with_fallback()
        return {
            "ok": True,
            "changed": [{"key": "raw_payload", "value": payload.payload}],
            "request_payload": payload.payload,
            "cgi_write": cgi_write,
            "state": _build_solplanet_control_state(schedule),
        }
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
