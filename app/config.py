from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    ha_url: str
    ha_token: str
    saj_core_entity_ids: tuple[str, ...]
    solplanet_core_entity_ids: tuple[str, ...]
    solplanet_dongle_host: str
    solplanet_dongle_port: int
    solplanet_dongle_scheme: str
    solplanet_verify_ssl: bool
    solplanet_cache_seconds: float
    solplanet_request_timeout_seconds: float


def _parse_entity_ids(raw: str | None, default: tuple[str, ...]) -> tuple[str, ...]:
    if not raw:
        return default
    return tuple(item.strip() for item in raw.split(",") if item.strip())


def load_settings() -> Settings:
    ha_url = os.getenv("HA_URL", "").strip()
    ha_token = os.getenv("HA_TOKEN", "").strip()
    saj_default = (
        "sensor.saj_pv_power",
        "sensor.saj_battery_power",
        "sensor.saj_ct_grid_power_total",
        "sensor.saj_total_grid_power",
        "sensor.saj_total_load_power",
        "sensor.saj_battery_energy_percent",
        "sensor.saj_inverter_status",
    )
    solplanet_default = (
        "sensor.solplanet_pv_power",
        "sensor.solplanet_battery_power",
        "sensor.solplanet_ct_grid_power_total",
        "sensor.solplanet_total_grid_power",
        "sensor.solplanet_total_load_power",
        "sensor.solplanet_battery_energy_percent",
        "sensor.solplanet_inverter_status",
    )
    saj_entity_ids = _parse_entity_ids(
        os.getenv("SAJ_CORE_ENTITY_IDS") or os.getenv("CORE_ENTITY_IDS"),
        saj_default,
    )
    solplanet_entity_ids = _parse_entity_ids(
        os.getenv("SOLPLANET_CORE_ENTITY_IDS"),
        solplanet_default,
    )
    solplanet_dongle_host = os.getenv("SOLPLANET_DONGLE_HOST", "").strip()
    solplanet_dongle_port = int(os.getenv("SOLPLANET_DONGLE_PORT", "443").strip() or "443")
    solplanet_dongle_scheme = os.getenv("SOLPLANET_DONGLE_SCHEME", "https").strip().lower()
    solplanet_verify_ssl = os.getenv("SOLPLANET_VERIFY_SSL", "false").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    solplanet_cache_seconds = float(os.getenv("SOLPLANET_CACHE_SECONDS", "3").strip() or "3")
    solplanet_request_timeout_seconds = float(
        os.getenv("SOLPLANET_REQUEST_TIMEOUT_SECONDS", "12").strip() or "12"
    )

    if not ha_url:
        raise ValueError("Missing HA_URL environment variable")
    if not ha_token:
        raise ValueError("Missing HA_TOKEN environment variable")

    return Settings(
        ha_url=ha_url.rstrip("/"),
        ha_token=ha_token,
        saj_core_entity_ids=saj_entity_ids,
        solplanet_core_entity_ids=solplanet_entity_ids,
        solplanet_dongle_host=solplanet_dongle_host,
        solplanet_dongle_port=solplanet_dongle_port,
        solplanet_dongle_scheme=solplanet_dongle_scheme if solplanet_dongle_scheme in ("http", "https") else "https",
        solplanet_verify_ssl=solplanet_verify_ssl,
        solplanet_cache_seconds=max(0.0, solplanet_cache_seconds),
        solplanet_request_timeout_seconds=max(0.5, solplanet_request_timeout_seconds),
    )
