from __future__ import annotations

import asyncio
import os
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from time import monotonic

import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.responses import Response

from app.config import (
    Settings,
    config_file_exists,
    get_config_path,
    get_missing_required_fields_from_payload,
    load_settings,
    read_config_file,
    save_settings,
    settings_to_dict,
)
from app.home_assistant import HomeAssistantClient
from app.persistence import (
    DEFAULT_DB_PATH,
    DEFAULT_SAMPLE_INTERVAL_SECONDS,
    EnergySample,
    compute_daily_usage,
    get_storage_status,
    init_db,
    insert_sample,
    list_samples,
)
from app.solplanet_cgi import SolplanetCgiClient


settings = load_settings()
ha_client = HomeAssistantClient(base_url=settings.ha_url, token=settings.ha_token)
solplanet_client = SolplanetCgiClient(
    host=settings.solplanet_dongle_host,
    port=settings.solplanet_dongle_port,
    scheme=settings.solplanet_dongle_scheme,
    verify_ssl=settings.solplanet_verify_ssl,
    timeout_seconds=settings.solplanet_request_timeout_seconds,
)
SUPPORTED_SYSTEMS = ("saj", "solplanet")
SYSTEM_PREFIXES: dict[str, tuple[str, ...]] = {
    "saj": ("saj",),
    "solplanet": ("solplanet", "soulplanet"),
}

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


def _read_sample_interval_seconds() -> float:
    raw = os.getenv("WATTIMIZE_SAMPLE_INTERVAL_SECONDS", "").strip()
    try:
        value = float(raw) if raw else DEFAULT_SAMPLE_INTERVAL_SECONDS
    except ValueError:
        value = DEFAULT_SAMPLE_INTERVAL_SECONDS
    return max(1.0, value)


sample_interval_seconds = _read_sample_interval_seconds()


class ConfigPayload(BaseModel):
    ha_url: str = Field(default="")
    ha_token: str = Field(default="")
    solplanet_dongle_host: str = Field(default="")


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


def _power_to_watts(value: float | None, unit: str | None) -> float | None:
    if value is None:
        return None
    normalized = (unit or "W").strip().lower()
    if normalized == "kw":
        return value * 1000
    if normalized == "mw":
        return value * 1000000
    return value


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

    inverter = _find_state_by_entity_ids(
        states_by_entity_id,
        (*_from_config_ids("inverter", "status"), f"sensor.{prefixes[0]}_inverter_status"),
    ) or _find_state_by_keywords(states, prefixes, (("inverter", "status"),))

    pv_v = _to_number(pv.get("state")) if pv else None
    grid_v = _to_number(grid.get("state")) if grid else None
    battery_v = _to_number(battery.get("state")) if battery else None
    load_v = _to_number(load.get("state")) if load else None
    soc_v = _to_number(soc.get("state")) if soc else None
    pv_w = _power_to_watts(pv_v, pv.get("attributes", {}).get("unit_of_measurement") if pv else None)  # type: ignore[union-attr]
    grid_w = _power_to_watts(grid_v, grid.get("attributes", {}).get("unit_of_measurement") if grid else None)  # type: ignore[union-attr]
    battery_w = _power_to_watts(
        battery_v,
        battery.get("attributes", {}).get("unit_of_measurement") if battery else None,  # type: ignore[union-attr]
    )
    load_w = _power_to_watts(load_v, load.get("attributes", {}).get("unit_of_measurement") if load else None)  # type: ignore[union-attr]

    balance_w: float | None = None
    balanced: bool | None = None
    if pv_w is not None and grid_w is not None and battery_w is not None and load_w is not None:
        battery_discharge_w = max(battery_w, 0)
        battery_charge_w = max(-battery_w, 0)
        grid_import_w = max(grid_w, 0)
        grid_export_w = max(-grid_w, 0)
        balance_w = pv_w + battery_discharge_w + grid_import_w - load_w - battery_charge_w - grid_export_w
        balanced = round(balance_w) == 0

    matched_entities = [item for item in (pv, grid, battery, load, soc, inverter) if item]
    inverter_status = inverter.get("state") if inverter else None

    return {
        "system": system,
        "prefixes": list(prefixes),
        "updated_at": datetime.now(UTC).isoformat(),
        "entities": {
            "pv": _compact_entity_item(pv) if pv else None,
            "grid": _compact_entity_item(grid) if grid else None,
            "battery": _compact_entity_item(battery) if battery else None,
            "load": _compact_entity_item(load) if load else None,
            "soc": _compact_entity_item(soc) if soc else None,
            "inverter": _compact_entity_item(inverter) if inverter else None,
        },
        "metrics": {
            "pv_w": pv_w,
            "grid_w": grid_w,
            "battery_w": battery_w,
            "load_w": load_w,
            "battery_soc_percent": soc_v,
            "inverter_status": inverter_status,
            "solar_active": bool(pv_w is not None and pv_w > 5),
            "grid_active": bool(grid_w is not None and abs(grid_w) > 5),
            "grid_import": bool(grid_w is not None and grid_w > 0),
            "battery_active": bool(battery_w is not None and abs(battery_w) > 5),
            "battery_discharging": bool(battery_w is not None and battery_w > 0),
            "load_active": bool(load_w is not None and load_w > 5),
            "balance_w": balance_w,
            "balanced": balanced,
            "matched_entities": len(matched_entities),
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


async def _build_solplanet_energy_flow_payload_from_cgi() -> dict[str, object]:
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
    if not isinstance(inv_list, list) or not inv_list:
        raise HTTPException(status_code=502, detail="Solplanet CGI returned empty inverter list")

    inverter_item = inv_list[0] if isinstance(inv_list[0], dict) else {}
    inverter_sn = str(inverter_item.get("isn") or "")
    if not inverter_sn:
        raise HTTPException(status_code=502, detail="Solplanet CGI inverter serial number is missing")

    meter_data: dict[str, object] = {}
    battery_data: dict[str, object] = {}

    battery_topo = inverter_item.get("battery_topo")
    battery_sn: str | None = None
    if isinstance(battery_topo, list) and battery_topo:
        maybe_first = battery_topo[0]
        if isinstance(maybe_first, dict):
            sn = maybe_first.get("bat_sn")
            if sn:
                battery_sn = str(sn)
    async def _safe_fetch(coro: object) -> dict[str, object]:
        try:
            result = await asyncio.wait_for(coro, timeout=settings.solplanet_request_timeout_seconds)
            return result if isinstance(result, dict) else {}
        except (httpx.HTTPError, TimeoutError, asyncio.TimeoutError):
            return {}

    tasks = [
        _safe_fetch(solplanet_client.get_inverter_data(inverter_sn)),
        _safe_fetch(solplanet_client.get_meter_data()),
    ]
    if battery_sn:
        tasks.append(_safe_fetch(solplanet_client.get_battery_data(battery_sn)))
    results = await asyncio.gather(*tasks)

    inverter_data = results[0] if len(results) >= 1 else {}
    meter_data = results[1] if len(results) >= 2 else {}
    battery_data = results[2] if len(results) >= 3 else {}

    updated_at = datetime.now(UTC).isoformat()
    inverter_status = _map_solplanet_status(inverter_data.get("stu"))

    pv_w = _to_number(inverter_data.get("ppv"))
    if pv_w is None:
        pv_w = _sum_array_product(inverter_data.get("vpv"), inverter_data.get("ipv"))
    if pv_w is None:
        pv_w = _to_number(battery_data.get("ppv"))

    grid_w = _to_number(meter_data.get("pac"))
    battery_w = _to_number(battery_data.get("pb"))
    load_w = _to_number(inverter_data.get("pac"))
    soc = _to_number(battery_data.get("soc"))

    # Solplanet battery power sign assumption: positive means discharging.
    balance_w: float | None = None
    balanced: bool | None = None
    if pv_w is not None and grid_w is not None and battery_w is not None and load_w is not None:
        battery_discharge_w = max(battery_w, 0)
        battery_charge_w = max(-battery_w, 0)
        grid_import_w = max(grid_w, 0)
        grid_export_w = max(-grid_w, 0)
        balance_w = pv_w + battery_discharge_w + grid_import_w - load_w - battery_charge_w - grid_export_w
        balanced = round(balance_w) == 0

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
            "Solplanet Inverter AC Power (CGI)",
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
            "battery_data": battery_data,
        },
        "metrics": {
            "pv_w": pv_w,
            "grid_w": grid_w,
            "battery_w": battery_w,
            "load_w": load_w,
            "battery_soc_percent": soc,
            "inverter_status": inverter_status,
            "solar_active": bool(pv_w is not None and pv_w > 5),
            "grid_active": bool(grid_w is not None and abs(grid_w) > 5),
            "grid_import": bool(grid_w is not None and grid_w > 0),
            "battery_active": bool(battery_w is not None and abs(battery_w) > 5),
            "battery_discharging": bool(battery_w is not None and battery_w > 0),
            "load_active": bool(load_w is not None and load_w > 5),
            "balance_w": balance_w,
            "balanced": balanced,
            "matched_entities": matched_entities,
            "notes": [
                "solplanet_pv_w_prefer_ppv",
                "solplanet_load_w_uses_inverter_pac",
                "solplanet_grid_w_uses_meter_pac",
                "solplanet_battery_w_uses_battery_pb",
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


async def _replace_runtime(new_settings: Settings) -> None:
    global settings, ha_client, solplanet_client  # noqa: PLW0603
    async with runtime_lock:
        old_solplanet = solplanet_client
        settings = new_settings
        ha_client = HomeAssistantClient(base_url=settings.ha_url, token=settings.ha_token)
        solplanet_client = SolplanetCgiClient(
            host=settings.solplanet_dongle_host,
            port=settings.solplanet_dongle_port,
            scheme=settings.solplanet_dongle_scheme,
            verify_ssl=settings.solplanet_verify_ssl,
            timeout_seconds=settings.solplanet_request_timeout_seconds,
        )
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


async def _store_flow_sample(system: str, flow: dict[str, object]) -> None:
    sample = _sample_from_flow(system, flow)
    await asyncio.to_thread(insert_sample, storage_db_path, sample)


async def _collect_for_system(system: str) -> None:
    if system == "saj":
        if _missing_required_config():
            return
        states = await ha_client.all_states()
        flow = _build_energy_flow_payload("saj", states)
        await _store_flow_sample("saj", flow)
        return

    if system == "solplanet":
        if not solplanet_client.configured:
            return
        flow = await _get_solplanet_flow_cached()
        await _store_flow_sample("solplanet", flow)
        return


async def _collector_loop() -> None:
    while not collector_stop_event.is_set():
        loop_started = monotonic()
        for system in SUPPORTED_SYSTEMS:
            try:
                await _collect_for_system(system)
            except Exception:  # noqa: BLE001
                # Keep collector alive even if one round fails.
                continue

        elapsed = monotonic() - loop_started
        sleep_seconds = max(0.1, sample_interval_seconds - elapsed)
        try:
            await asyncio.wait_for(collector_stop_event.wait(), timeout=sleep_seconds)
        except asyncio.TimeoutError:
            continue


async def _start_collector() -> None:
    global collector_task  # noqa: PLW0603
    await asyncio.to_thread(init_db, storage_db_path)
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


@app.get("/api/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/storage/status")
async def storage_status() -> dict[str, object]:
    return await asyncio.to_thread(get_storage_status, storage_db_path, sample_interval_seconds)


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
        sample_interval_seconds=sample_interval_seconds,
    )


@app.get("/api/storage/samples")
async def storage_samples(
    system: str | None = Query(default=None, description="saj or solplanet"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
) -> dict[str, object]:
    normalized: str | None = None
    if system:
        normalized = _normalize_system_name(system)
    return await asyncio.to_thread(
        list_samples,
        storage_db_path,
        system=normalized,
        page=page,
        page_size=page_size,
    )


@app.get("/api/config/status")
async def config_status() -> dict[str, object]:
    missing = _missing_required_config()
    return {
        "configured": len(missing) == 0,
        "missing_required": missing,
        "config_path": str(get_config_path()),
    }


@app.get("/api/config")
async def get_config() -> dict[str, object]:
    payload = settings_to_dict(settings)
    payload["configured"] = len(_missing_required_config()) == 0
    payload["config_path"] = str(get_config_path())
    return payload


@app.put("/api/config")
async def put_config(payload: ConfigPayload) -> dict[str, object]:
    persisted = save_settings(payload.model_dump())
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
    if normalized == "solplanet" and solplanet_client.configured:
        try:
            return await _get_solplanet_flow_cached()
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
        states = await ha_client.all_states()
        return _build_energy_flow_payload(normalized, states)
    except httpx.HTTPStatusError as exc:
        detail = {
            "message": "Home Assistant API returned an error",
            "status_code": exc.response.status_code,
            "response": exc.response.text,
        }
        raise HTTPException(status_code=502, detail=detail) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to reach Home Assistant: {exc}") from exc


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


@app.get("/api/soulplanet/energy-flow")
@app.get("/api/solplanet/energy-flow")
async def get_energy_flow_soulplanet() -> dict[str, object]:
    return await get_energy_flow("solplanet")


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
    try:
        context = await _get_solplanet_context_cached()
        return _build_solplanet_single_endpoint_response(
            name="getdev_device_2",
            path="getdev.cgi?device=2",
            payload=context.get("inverter_info") if isinstance(context.get("inverter_info"), dict) else {},
            error=None,
            started_at=started_at,
        )
    except Exception as exc:  # noqa: BLE001
        return _build_solplanet_single_endpoint_response(
            name="getdev_device_2",
            path="getdev.cgi?device=2",
            payload=None,
            error=f"{type(exc).__name__}: {exc}",
            started_at=started_at,
        )


@app.get("/api/soulplanet/cgi/getdevdata-device-2")
@app.get("/api/solplanet/cgi/getdevdata-device-2")
async def get_solplanet_getdevdata_device_2() -> dict[str, object]:
    started_at = monotonic()
    try:
        context = await _get_solplanet_context_cached()
        inverter_sn = context.get("inverter_sn")
        if not inverter_sn:
            raise ValueError("Missing inverter_sn")
        payload = await asyncio.wait_for(
            solplanet_client.get_inverter_data(str(inverter_sn)),
            timeout=settings.solplanet_request_timeout_seconds,
        )
        return _build_solplanet_single_endpoint_response(
            name="getdevdata_device_2",
            path=f"getdevdata.cgi?device=2&sn={inverter_sn}",
            payload=payload,
            error=None,
            started_at=started_at,
        )
    except Exception as exc:  # noqa: BLE001
        return _build_solplanet_single_endpoint_response(
            name="getdevdata_device_2",
            path="getdevdata.cgi?device=2",
            payload=None,
            error=f"{type(exc).__name__}: {exc}",
            started_at=started_at,
        )


@app.get("/api/soulplanet/cgi/getdevdata-device-3")
@app.get("/api/solplanet/cgi/getdevdata-device-3")
async def get_solplanet_getdevdata_device_3() -> dict[str, object]:
    started_at = monotonic()
    try:
        payload = await asyncio.wait_for(
            solplanet_client.get_meter_data(),
            timeout=settings.solplanet_request_timeout_seconds,
        )
        return _build_solplanet_single_endpoint_response(
            name="getdevdata_device_3",
            path="getdevdata.cgi?device=3",
            payload=payload,
            error=None,
            started_at=started_at,
        )
    except Exception as exc:  # noqa: BLE001
        return _build_solplanet_single_endpoint_response(
            name="getdevdata_device_3",
            path="getdevdata.cgi?device=3",
            payload=None,
            error=f"{type(exc).__name__}: {exc}",
            started_at=started_at,
        )


@app.get("/api/soulplanet/cgi/getdevdata-device-4")
@app.get("/api/solplanet/cgi/getdevdata-device-4")
async def get_solplanet_getdevdata_device_4() -> dict[str, object]:
    started_at = monotonic()
    try:
        context = await _get_solplanet_context_cached()
        battery_sn = context.get("battery_sn")
        if not battery_sn:
            raise ValueError("Missing battery_sn")
        payload = await asyncio.wait_for(
            solplanet_client.get_battery_data(str(battery_sn)),
            timeout=settings.solplanet_request_timeout_seconds,
        )
        return _build_solplanet_single_endpoint_response(
            name="getdevdata_device_4",
            path=f"getdevdata.cgi?device=4&sn={battery_sn}",
            payload=payload,
            error=None,
            started_at=started_at,
        )
    except Exception as exc:  # noqa: BLE001
        return _build_solplanet_single_endpoint_response(
            name="getdevdata_device_4",
            path="getdevdata.cgi?device=4",
            payload=None,
            error=f"{type(exc).__name__}: {exc}",
            started_at=started_at,
        )


@app.get("/api/soulplanet/cgi/getdefine")
@app.get("/api/solplanet/cgi/getdefine")
async def get_solplanet_getdefine() -> dict[str, object]:
    started_at = monotonic()
    try:
        payload = await asyncio.wait_for(
            solplanet_client.get_schedule(),
            timeout=settings.solplanet_request_timeout_seconds,
        )
        return _build_solplanet_single_endpoint_response(
            name="getdefine",
            path="getdefine.cgi",
            payload=payload,
            error=None,
            started_at=started_at,
        )
    except Exception as exc:  # noqa: BLE001
        return _build_solplanet_single_endpoint_response(
            name="getdefine",
            path="getdefine.cgi",
            payload=None,
            error=f"{type(exc).__name__}: {exc}",
            started_at=started_at,
        )


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
