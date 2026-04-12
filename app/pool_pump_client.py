from __future__ import annotations

import asyncio
from typing import Any

from app.home_assistant import HomeAssistantClient


class PoolPumpClient:
    def __init__(
        self,
        ha_client: HomeAssistantClient,
        *,
        switch_entity_id: str,
        power_entity_id: str,
        energy_today_entity_id: str | None = None,
        info_entity_id: str | None = None,
    ) -> None:
        self._ha_client = ha_client
        self._switch_entity_id = switch_entity_id
        self._power_entity_id = power_entity_id
        self._energy_today_entity_id = energy_today_entity_id
        self._info_entity_id = info_entity_id

    async def turn_on(self) -> list[dict[str, Any]]:
        return await self._set_enabled(True)

    async def turn_off(self) -> list[dict[str, Any]]:
        return await self._set_enabled(False)

    async def check_status(self) -> dict[str, object]:
        tasks: list[object] = [
            self._ha_client.entity_state(self._switch_entity_id),
            self._ha_client.entity_state(self._power_entity_id),
        ]
        if self._energy_today_entity_id:
            tasks.append(self._ha_client.entity_state(self._energy_today_entity_id))
        else:
            tasks.append(asyncio.sleep(0, result={}))
        switch_state, power_state, energy_today_state = await asyncio.gather(*tasks, return_exceptions=True)
        switch_state = await self._resolve_switch_state(switch_state)
        return {
            "switch_state": switch_state,
            "power_state": power_state,
            "energy_today_state": energy_today_state,
        }

    async def _resolve_switch_state(self, switch_state: object) -> object:
        """When the switch entity is unavailable, fall back to the info entity's switch.on attribute."""
        if not self._info_entity_id:
            return switch_state
        raw_state = str((switch_state or {}).get("state") or "").strip().lower() if isinstance(switch_state, dict) else ""
        if raw_state in ("on", "off"):
            return switch_state
        try:
            info_state = await self._ha_client.entity_state(self._info_entity_id)
        except Exception:  # noqa: BLE001
            return switch_state
        if not isinstance(info_state, dict):
            return switch_state
        switch_on = info_state.get("attributes", {}).get("switch.on")
        if switch_on is None:
            return switch_state
        resolved = "on" if switch_on else "off"
        return {
            **(switch_state if isinstance(switch_state, dict) else {"entity_id": self._switch_entity_id, "attributes": {}}),
            "state": resolved,
            "_state_source": "info_entity_fallback",
            "_info_entity_id": self._info_entity_id,
        }

    async def _set_enabled(self, enabled: bool) -> list[dict[str, Any]]:
        # Xiaomi Miot service is the most reliable control path for this plug.
        payload = {
            "entity_id": self._switch_entity_id,
            "field": "switch.on",
            "value": bool(enabled),
        }
        return await self._ha_client.call_service("xiaomi_miot", "set_property", payload)
