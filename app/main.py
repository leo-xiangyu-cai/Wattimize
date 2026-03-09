from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import traceback
from collections import Counter
from contextvars import ContextVar
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import monotonic
from typing import Callable, Literal

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
    compute_daily_usage,
    compute_usage_between,
    export_samples_csv,
    get_realtime_kv_by_prefix,
    get_solplanet_endpoint_snapshot,
    get_latest_sample,
    get_series_samples,
    get_storage_status,
    insert_worker_api_log,
    import_samples_csv,
    init_db,
    insert_sample,
    list_realtime_kv_rows,
    list_samples,
    list_worker_api_logs,
    upsert_solplanet_endpoint_snapshot,
    upsert_realtime_kv,
)
from app.solplanet_cgi import SolplanetCgiClient

logger = logging.getLogger(__name__)


settings = load_settings()

POWER_FLOW_ACTIVE_THRESHOLD_W = 30
SUPPORTED_SYSTEMS = ("saj", "solplanet")
SYSTEM_PREFIXES: dict[str, tuple[str, ...]] = {
    "saj": ("saj",),
    "solplanet": ("solplanet", "soulplanet"),
}
BALANCE_TOLERANCE_W = 120.0

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
sample_interval_seconds = float(settings.saj_sample_interval_seconds)
solplanet_sample_interval_seconds = float(settings.solplanet_sample_interval_seconds)
collector_status: dict[str, dict[str, object]] = {
    "saj": {
        "in_progress": False,
        "continuous": False,
        "interval_seconds": sample_interval_seconds,
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
        "backoff_seconds": 0.0,
        "next_retry_at": None,
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


def _handle_outbound_request_log(event: dict[str, object]) -> None:
    if request_actor_ctx.get() != "worker":
        return
    requested_at = str(event.get("requested_at_utc") or datetime.now(UTC).isoformat())
    worker = request_actor_ctx.get() or "worker"
    system = request_system_ctx.get()
    service = str(event.get("service") or "unknown")
    method = str(event.get("method") or "GET").upper()
    api_link = str(event.get("url") or "")
    ok = bool(event.get("ok"))
    status_code_value = event.get("status_code")
    status_code = int(status_code_value) if isinstance(status_code_value, (int, float)) else None
    duration_raw = event.get("duration_ms")
    duration_ms = round(float(duration_raw), 1) if isinstance(duration_raw, (int, float)) else None
    result_text = str(event.get("result_text") or "")
    error_text = str(event.get("error_text") or "")

    try:
        insert_worker_api_log(
            storage_db_path,
            worker=worker,
            system=system,
            service=service,
            method=method,
            api_link=api_link,
            requested_at_utc=requested_at,
            ok=ok,
            status_code=status_code,
            duration_ms=duration_ms,
            result_text=result_text,
            error_text=error_text,
        )
    except Exception as exc:
        logger.warning("Failed to persist worker_api_log for %s %s: %s", service, api_link, exc)


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
    # One Solplanet sampling round may hit 8-9 CGI endpoints sequentially.
    # Cap the whole round so the collector cannot hang indefinitely and stop retrying.
    endpoint_budget = current_settings.solplanet_request_timeout_seconds * 10.0
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
SOLPLANET_SLOT_MIN = 1
SOLPLANET_SLOT_MAX = 6
SOLPLANET_LIMIT_MIN = 0
SOLPLANET_LIMIT_MAX = 20000
SOLPLANET_BACKOFF_INITIAL_SECONDS = 5.0
SOLPLANET_BACKOFF_MAX_SECONDS = 300.0
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


async def _build_solplanet_energy_flow_payload_from_cgi() -> dict[str, object]:
    if not solplanet_client.configured:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Solplanet CGI client is not configured",
                "required_env": "SOLPLANET_DONGLE_HOST",
            },
        )

    async def _save_endpoint_snapshot(
        *,
        endpoint: str,
        path: str,
        ok: bool,
        error: str | None,
        payload: dict[str, object] | None,
        fetch_ms: float | None,
    ) -> None:
        try:
            await asyncio.to_thread(
                upsert_solplanet_endpoint_snapshot,
                storage_db_path,
                endpoint=endpoint,
                path=path,
                requested_at_utc=datetime.now(UTC).isoformat(),
                ok=ok,
                error=error,
                payload=payload,
                fetch_ms=fetch_ms,
            )
        except Exception as exc:
            logger.warning("Failed to persist Solplanet endpoint snapshot for %s (%s): %s", endpoint, path, exc)

    started_at = monotonic()
    getdev2_started = monotonic()
    try:
        inverter_info = await asyncio.wait_for(
            solplanet_client.get_inverter_info(),
            timeout=settings.solplanet_request_timeout_seconds,
        )
        await _save_endpoint_snapshot(
            endpoint="getdev_device_2",
            path="getdev.cgi?device=2",
            ok=True,
            error=None,
            payload=inverter_info if isinstance(inverter_info, dict) else {},
            fetch_ms=round((monotonic() - getdev2_started) * 1000, 1),
        )
    except Exception as exc:  # noqa: BLE001
        await _save_endpoint_snapshot(
            endpoint="getdev_device_2",
            path="getdev.cgi?device=2",
            ok=False,
            error=f"{type(exc).__name__}: {exc}",
            payload=None,
            fetch_ms=round((monotonic() - getdev2_started) * 1000, 1),
        )
        raise

    inv_list = inverter_info.get("inv")
    if not isinstance(inv_list, list) or not inv_list:
        raise HTTPException(status_code=502, detail="Solplanet CGI returned empty inverter list")

    inverter_item = inv_list[0] if isinstance(inv_list[0], dict) else {}
    inverter_sn = str(inverter_item.get("isn") or "")
    if not inverter_sn:
        raise HTTPException(status_code=502, detail="Solplanet CGI inverter serial number is missing")

    meter_data: dict[str, object] = {}
    meter_info: dict[str, object] = {}
    battery_data: dict[str, object] = {}
    schedule_data: dict[str, object] = {}

    battery_topo = inverter_item.get("battery_topo")
    battery_sn: str | None = None
    if isinstance(battery_topo, list) and battery_topo:
        maybe_first = battery_topo[0]
        if isinstance(maybe_first, dict):
            sn = maybe_first.get("bat_sn")
            if sn:
                battery_sn = str(sn)
    steps: list[tuple[str, str, Callable[[], object]]] = [
        ("getdev_device_0", "getdev.cgi?device=0", lambda: solplanet_client.get_dongle_info()),
        (
            "getdevdata_device_2",
            f"getdevdata.cgi?device=2&sn={inverter_sn}",
            lambda: solplanet_client.get_inverter_data(inverter_sn),
        ),
        ("getdevdata_device_3", "getdevdata.cgi?device=3", lambda: solplanet_client.get_meter_data()),
        ("getdev_device_3", "getdev.cgi?device=3", lambda: solplanet_client.get_meter_info()),
        (
            "getdev_device_4",
            f"getdev.cgi?device=4&sn={inverter_sn}",
            lambda: solplanet_client.get_battery_info(inverter_sn),
        ),
        ("getdefine", "getdefine.cgi", lambda: solplanet_client.get_schedule()),
        ("getdevdata_device_5", "getdevdata.cgi?device=5", lambda: solplanet_client.get_device_data(5)),
    ]
    if battery_sn:
        steps.append(
            (
                "getdevdata_device_4",
                f"getdevdata.cgi?device=4&sn={battery_sn}",
                lambda bat_sn=battery_sn: solplanet_client.get_battery_data(bat_sn),
            )
        )
    else:
        await _save_endpoint_snapshot(
            endpoint="getdevdata_device_4",
            path="getdevdata.cgi?device=4",
            ok=False,
            error="ValueError: Missing battery_sn",
            payload=None,
            fetch_ms=0.0,
        )

    endpoint_map: dict[str, tuple[dict[str, object] | None, str | None, str, float]] = {}
    # getdevdata_device_2 is mandatory for energy-flow (inverter live data).
    # Other endpoints can degrade gracefully if they timeout/fail.
    mandatory_endpoints = {"getdevdata_device_2"}
    for endpoint, path, build_coro in steps:
        fetch_started = monotonic()
        try:
            result = await asyncio.wait_for(build_coro(), timeout=settings.solplanet_request_timeout_seconds)
            payload = result if isinstance(result, dict) else {}
            fetch_ms = round((monotonic() - fetch_started) * 1000, 1)
            await _save_endpoint_snapshot(
                endpoint=endpoint,
                path=path,
                ok=True,
                error=None,
                payload=payload,
                fetch_ms=fetch_ms,
            )
            endpoint_map[endpoint] = (payload, None, path, fetch_ms)
        except Exception as exc:  # noqa: BLE001
            fetch_ms = round((monotonic() - fetch_started) * 1000, 1)
            error_text = f"{type(exc).__name__}: {exc}"
            await _save_endpoint_snapshot(
                endpoint=endpoint,
                path=path,
                ok=False,
                error=error_text,
                payload=None,
                fetch_ms=fetch_ms,
            )
            endpoint_map[endpoint] = ({}, error_text, path, fetch_ms)
            if endpoint in mandatory_endpoints:
                raise

    inverter_data = endpoint_map.get("getdevdata_device_2", ({}, None, "", 0.0))[0] or {}
    meter_data = endpoint_map.get("getdevdata_device_3", ({}, None, "", 0.0))[0] or {}
    meter_info = endpoint_map.get("getdev_device_3", ({}, None, "", 0.0))[0] or {}
    schedule_data = endpoint_map.get("getdefine", ({}, None, "", 0.0))[0] or {}
    battery_data = endpoint_map.get("getdevdata_device_4", ({}, None, "", 0.0))[0] or {}
    endpoint_errors = [
        f"solplanet_endpoint_error:{name}:{error}"
        for name, (_, error, _, _) in endpoint_map.items()
        if isinstance(error, str) and error
    ]

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
    else:
        # getdevdata.cgi?device=3 returned flg=0 (meter offline/disconnected).
        # meter_pac from getdev.cgi?device=3 is used if it is non-zero.
        # NOTE: total_pac from getdev.cgi?device=3 is NOT used here because it mirrors
        # inverter.pac (AC output), not actual net grid power, giving severely wrong readings.
        meter_pac = _to_number(meter_info.get("meter_pac"))
        if meter_pac is not None and abs(meter_pac) >= POWER_FLOW_ACTIVE_THRESHOLD_W:
            grid_w = meter_pac
            grid_source = "getdev_device_3.meter_pac"

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
            "inverter_info": inverter_item,
            "inverter_data": inverter_data,
            "meter_data": meter_data,
            "meter_info": meter_info,
            "schedule": schedule_data,
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
                "solplanet_grid_w_prefers_meter_data_pac_then_meter_pac",
                f"solplanet_grid_w_source:{grid_source}",
                f"solplanet_meter_data_valid:{str(meter_data_valid).lower()}",
                "solplanet_battery_w_uses_battery_pb",
                "solplanet_total_pac_not_used:mirrors_inverter_pac_not_grid",
                *endpoint_errors,
            ],
            "fetch_ms": round((monotonic() - started_at) * 1000, 1),
        },
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
        _safe_task("getdev_device_3", solplanet_client.get_meter_info()),
        _safe_task("getdevdata_device_3", solplanet_client.get_meter_data()),
        _safe_task("getdefine", solplanet_client.get_schedule()),
    ]
    if battery_sn:
        tasks.append(_safe_task("getdevdata_device_4", solplanet_client.get_battery_data(battery_sn)))

    task_results = await asyncio.gather(*tasks)
    endpoints: dict[str, object] = {
        "getdev_device_2": {
            "path": "getdev.cgi?device=2",
            "ok": True,
            "error": None,
            "payload": inverter_info,
        }
    }
    for name, payload, error in task_results:
        endpoint_path = {
            "getdevdata_device_2": f"getdevdata.cgi?device=2&sn={inverter_sn}" if inverter_sn else "getdevdata.cgi?device=2",
            "getdev_device_3": "getdev.cgi?device=3",
            "getdevdata_device_3": "getdevdata.cgi?device=3",
            "getdevdata_device_4": f"getdevdata.cgi?device=4&sn={battery_sn}" if battery_sn else "getdevdata.cgi?device=4",
            "getdefine": "getdefine.cgi",
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
    async with solplanet_context_lock:
        cached = _get_cached_solplanet_context()
        if cached is not None:
            return cached

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
        solplanet_context_cache["payload"] = payload
        solplanet_context_cache["at_monotonic"] = monotonic()
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
    snapshot = await asyncio.to_thread(get_solplanet_endpoint_snapshot, storage_db_path, endpoint=name)
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
        response["source"] = {"type": "solplanet_endpoint_snapshot_table"}
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
    response["last_success_at"] = snapshot.get("last_success_at_utc")
    response["status"] = "success" if snap_ok else "failed"
    response["ok"] = snap_ok
    response["error"] = snap_error
    if snapshot.get("fetch_ms") is not None:
        response["fetch_ms"] = snapshot.get("fetch_ms")
    last_success_dt = _parse_iso_utc(snapshot.get("last_success_at_utc"))
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
        "type": "solplanet_endpoint_snapshot_table",
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
        },
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
        snapshot = await asyncio.to_thread(get_solplanet_endpoint_snapshot, storage_db_path, endpoint="getdefine")
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
        collector_status.setdefault("saj", {})["interval_seconds"] = sample_interval_seconds
        collector_status.setdefault("saj", {})["continuous"] = False
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


def _sample_from_flow(system: str, flow: dict[str, object]) -> EnergySample:
    metrics = flow.get("metrics")
    metrics_map = metrics if isinstance(metrics, dict) else {}
    source = "solplanet_cgi" if system == "solplanet" else "home_assistant"
    return EnergySample(
        system=system,
        sampled_at_utc=datetime.now(UTC),
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
        payload=flow,
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

    flow: dict[str, object] = {
        "system": system,
        "updated_at": updated_at,
        "metrics": metrics,
        "source": {"type": "realtime_kv"},
        "storage_backed": True,
        "kv_item_count": len(kv_map),
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


async def _store_flow_sample(system: str, flow: dict[str, object]) -> None:
    sample = _sample_from_flow(system, flow)
    await asyncio.to_thread(insert_sample, storage_db_path, sample)
    await _store_realtime_kv_snapshot(system, flow)


async def _collect_for_system(system: str) -> None:
    actor_token = request_actor_ctx.set("worker")
    system_token = request_system_ctx.set(system)
    try:
        if system == "saj":
            if _missing_required_config():
                return
            states = await ha_client.all_states()
            flow = _build_energy_flow_payload("saj", states)
            await _store_flow_sample("saj", flow)
            return

        if system == "solplanet":
            if not solplanet_client.configured:
                raise RuntimeError("Solplanet CGI client is not configured")
            flow = await asyncio.wait_for(
                _build_solplanet_energy_flow_payload_from_cgi(),
                timeout=_solplanet_collection_round_timeout_seconds(settings),
            )
            await _store_flow_sample("solplanet", flow)
            return
    finally:
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


async def _collector_loop() -> None:
    last_collected_at: dict[str, float] = {system: 0.0 for system in SUPPORTED_SYSTEMS}
    solplanet_backoff_seconds = SOLPLANET_BACKOFF_INITIAL_SECONDS
    solplanet_next_retry_monotonic = 0.0
    while not collector_stop_event.is_set():
        for system in SUPPORTED_SYSTEMS:
            previous = last_collected_at.get(system, 0.0)
            # Solplanet polling runs continuously: start the next round immediately
            # after one round finishes, without waiting for a fixed interval.
            if system == "solplanet":
                if monotonic() < solplanet_next_retry_monotonic:
                    continue
            else:
                interval = _sample_interval_for_system(system)
                if previous > 0.0 and (monotonic() - previous) < interval:
                    continue
            started_monotonic = _collector_mark_start(system)
            try:
                await _collect_for_system(system)
                last_collected_at[system] = monotonic()
                _collector_mark_finish(system, started_monotonic)
                if system == "solplanet":
                    solplanet_backoff_seconds = SOLPLANET_BACKOFF_INITIAL_SECONDS
                    solplanet_next_retry_monotonic = 0.0
                    status = collector_status.setdefault("solplanet", {})
                    status["backoff_seconds"] = 0.0
                    status["next_retry_at"] = None
            except Exception as exc:  # noqa: BLE001
                # Keep collector alive even if one round fails.
                _append_worker_failure_log(
                    system,
                    stage="collector_round",
                    error=exc,
                    started_monotonic=started_monotonic,
                )
                _collector_mark_finish(system, started_monotonic, error=exc)
                if system == "solplanet":
                    try:
                        await _recreate_solplanet_client()
                    except Exception as recreate_exc:  # noqa: BLE001
                        _append_worker_failure_log(
                            "solplanet",
                            stage="solplanet_client_recreate",
                            error=recreate_exc,
                            extra={
                                "retry_after_seconds": round(
                                    max(SOLPLANET_BACKOFF_INITIAL_SECONDS, solplanet_backoff_seconds),
                                    1,
                                ),
                            },
                        )
                        status = collector_status.setdefault("solplanet", {})
                        status["client_recreate_error_at"] = datetime.now(UTC).isoformat()
                        status["client_recreate_error"] = f"{type(recreate_exc).__name__}: {recreate_exc}"
                    now_mono = monotonic()
                    retry_after = max(SOLPLANET_BACKOFF_INITIAL_SECONDS, solplanet_backoff_seconds)
                    solplanet_next_retry_monotonic = now_mono + retry_after
                    status = collector_status.setdefault("solplanet", {})
                    status["backoff_seconds"] = round(retry_after, 1)
                    status["next_retry_at"] = (datetime.now(UTC) + timedelta(seconds=retry_after)).isoformat()
                    solplanet_backoff_seconds = min(retry_after * 2.0, SOLPLANET_BACKOFF_MAX_SECONDS)
                continue

        # Keep loop responsive so Solplanet can run back-to-back rounds.
        # SAJ still respects its own interval guard above.
        sleep_seconds = 0.1
        try:
            await asyncio.wait_for(collector_stop_event.wait(), timeout=sleep_seconds)
        except asyncio.TimeoutError:
            continue


async def _start_collector() -> None:
    global collector_task  # noqa: PLW0603
    await asyncio.to_thread(init_db, storage_db_path)
    if collector_task and not collector_task.done():
        return
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
    saj_state["interval_seconds"] = sample_interval_seconds
    saj_state["continuous"] = False
    solplanet_state["interval_seconds"] = None
    solplanet_state["continuous"] = True
    return {
        "updated_at": datetime.now(UTC).isoformat(),
        "systems": {
            "saj": saj_state,
            "solplanet": solplanet_state,
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
) -> dict[str, object]:
    normalized_system: str | None = None
    if system:
        normalized_system = _normalize_system_name(system)
    payload = await asyncio.to_thread(
        list_worker_api_logs,
        storage_db_path,
        page=page,
        page_size=page_size,
        worker="worker",
        system=normalized_system,
        service=str(service or "").strip() or None,
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
    return await _get_energy_flow_from_realtime_kv(normalized)


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
