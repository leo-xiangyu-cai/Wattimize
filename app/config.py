from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_SAJ_ENTITY_IDS: tuple[str, ...] = (
    "sensor.saj_pv_power",
    "sensor.saj_battery_power",
    "sensor.saj_ct_grid_power_total",
    "sensor.saj_total_grid_power",
    "sensor.saj_total_load_power",
    "sensor.saj_battery_energy_percent",
    "sensor.saj_inverter_status",
)

DEFAULT_SOLPLANET_ENTITY_IDS: tuple[str, ...] = (
    "sensor.solplanet_pv_power",
    "sensor.solplanet_battery_power",
    "sensor.solplanet_ct_grid_power_total",
    "sensor.solplanet_total_grid_power",
    "sensor.solplanet_total_load_power",
    "sensor.solplanet_battery_energy_percent",
    "sensor.solplanet_inverter_status",
)

CONST_SOLPLANET_DONGLE_PORT = 443
CONST_SOLPLANET_DONGLE_SCHEME = "https"
CONST_SOLPLANET_VERIFY_SSL = False
CONST_SOLPLANET_CACHE_SECONDS = 3.0
CONST_SOLPLANET_REQUEST_TIMEOUT_SECONDS = 30.0
ALLOWED_SAMPLE_INTERVAL_SECONDS: tuple[int, ...] = (5, 10, 30, 60, 300)
CONST_SAJ_SAMPLE_INTERVAL_SECONDS = 5
CONST_SOLPLANET_SAMPLE_INTERVAL_SECONDS = 60


@dataclass(frozen=True)
class Settings:
    ha_url: str
    ha_token: str
    local_timezone: str
    saj_target_profile: str
    saj_core_entity_ids: tuple[str, ...]
    solplanet_core_entity_ids: tuple[str, ...]
    solplanet_dongle_host: str
    solplanet_inverter_sn: str
    solplanet_battery_sn: str
    solplanet_dongle_port: int
    solplanet_dongle_scheme: str
    solplanet_verify_ssl: bool
    solplanet_cache_seconds: float
    solplanet_request_timeout_seconds: float
    saj_sample_interval_seconds: int
    solplanet_sample_interval_seconds: int


def normalize_sample_interval_seconds(value: object, default: int) -> int:
    try:
        parsed = int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default
    if parsed in ALLOWED_SAMPLE_INTERVAL_SECONDS:
        return parsed
    return default


def get_config_path() -> Path:
    configured = os.getenv("WATTIMIZE_CONFIG_PATH", "").strip()
    if configured:
        return Path(configured)
    container_default = Path("/app/data/config.json")
    if Path("/app").exists():
        return container_default
    project_default = Path(__file__).resolve().parent.parent / "data" / "config.json"
    return project_default


def read_config_file() -> dict[str, object]:
    path = get_config_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def config_file_exists() -> bool:
    return get_config_path().exists()


def _env_values() -> dict[str, object]:
    return {
        "ha_url": os.getenv("HA_URL", "").strip(),
        "ha_token": os.getenv("HA_TOKEN", "").strip(),
        "local_timezone": os.getenv("WATTIMIZE_LOCAL_TIMEZONE", os.getenv("TZ", "")).strip(),
        "solplanet_dongle_host": os.getenv("SOLPLANET_DONGLE_HOST", "").strip(),
        "solplanet_inverter_sn": os.getenv("SOLPLANET_INVERTER_SN", "").strip(),
        "solplanet_battery_sn": os.getenv("SOLPLANET_BATTERY_SN", "").strip(),
        "saj_sample_interval_seconds": os.getenv("WATTIMIZE_SAMPLE_INTERVAL_SECONDS", "").strip(),
        "solplanet_sample_interval_seconds": os.getenv("WATTIMIZE_SOLPLANET_SAMPLE_INTERVAL_SECONDS", "").strip(),
    }


def _build_settings(raw: dict[str, object]) -> Settings:
    return Settings(
        ha_url=str(raw.get("ha_url") or "").strip().rstrip("/"),
        ha_token=str(raw.get("ha_token") or "").strip(),
        local_timezone=str(raw.get("local_timezone") or "").strip(),
        saj_target_profile=str(raw.get("saj_target_profile") or "").strip(),
        saj_core_entity_ids=DEFAULT_SAJ_ENTITY_IDS,
        solplanet_core_entity_ids=DEFAULT_SOLPLANET_ENTITY_IDS,
        solplanet_dongle_host=str(raw.get("solplanet_dongle_host") or "").strip(),
        solplanet_inverter_sn=str(raw.get("solplanet_inverter_sn") or "").strip(),
        solplanet_battery_sn=str(raw.get("solplanet_battery_sn") or "").strip(),
        solplanet_dongle_port=CONST_SOLPLANET_DONGLE_PORT,
        solplanet_dongle_scheme=CONST_SOLPLANET_DONGLE_SCHEME,
        solplanet_verify_ssl=CONST_SOLPLANET_VERIFY_SSL,
        solplanet_cache_seconds=CONST_SOLPLANET_CACHE_SECONDS,
        solplanet_request_timeout_seconds=CONST_SOLPLANET_REQUEST_TIMEOUT_SECONDS,
        saj_sample_interval_seconds=normalize_sample_interval_seconds(
            raw.get("saj_sample_interval_seconds"),
            CONST_SAJ_SAMPLE_INTERVAL_SECONDS,
        ),
        solplanet_sample_interval_seconds=normalize_sample_interval_seconds(
            raw.get("solplanet_sample_interval_seconds"),
            CONST_SOLPLANET_SAMPLE_INTERVAL_SECONDS,
        ),
    )


def load_settings() -> Settings:
    merged = {**_env_values(), **read_config_file()}
    return _build_settings(merged)


def settings_to_dict(settings: Settings) -> dict[str, object]:
    return {
        "ha_url": settings.ha_url,
        "ha_token": settings.ha_token,
        "local_timezone": settings.local_timezone,
        "saj_target_profile": settings.saj_target_profile,
        "solplanet_dongle_host": settings.solplanet_dongle_host,
        "solplanet_inverter_sn": settings.solplanet_inverter_sn,
        "solplanet_battery_sn": settings.solplanet_battery_sn,
        "saj_sample_interval_seconds": settings.saj_sample_interval_seconds,
        "solplanet_sample_interval_seconds": settings.solplanet_sample_interval_seconds,
    }


def save_settings(payload: dict[str, object]) -> Settings:
    base = settings_to_dict(load_settings())
    base.update(payload)
    normalized = _build_settings(base)

    path = get_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(settings_to_dict(normalized), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return normalized


def get_missing_required_fields_from_payload(payload: dict[str, object]) -> list[str]:
    missing: list[str] = []
    ha_url = str(payload.get("ha_url") or "").strip()
    ha_token = str(payload.get("ha_token") or "").strip()
    if not ha_url:
        missing.append("ha_url")
    if not ha_token:
        missing.append("ha_token")
    return missing
