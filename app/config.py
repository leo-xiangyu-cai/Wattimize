from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    ha_url: str
    ha_token: str
    core_entity_ids: tuple[str, ...]


def _parse_entity_ids(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return (
            "sensor.saj_pv_power",
            "sensor.saj_battery_power",
            "sensor.saj_ct_grid_power_total",
            "sensor.saj_total_grid_power",
            "sensor.saj_total_load_power",
            "sensor.saj_battery_energy_percent",
            "sensor.saj_inverter_status",
        )
    return tuple(item.strip() for item in raw.split(",") if item.strip())


def load_settings() -> Settings:
    ha_url = os.getenv("HA_URL", "").strip()
    ha_token = os.getenv("HA_TOKEN", "").strip()
    entity_ids = _parse_entity_ids(os.getenv("CORE_ENTITY_IDS"))

    if not ha_url:
        raise ValueError("Missing HA_URL environment variable")
    if not ha_token:
        raise ValueError("Missing HA_TOKEN environment variable")

    return Settings(ha_url=ha_url.rstrip("/"), ha_token=ha_token, core_entity_ids=entity_ids)
