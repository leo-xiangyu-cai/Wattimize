from __future__ import annotations

from typing import Any

import httpx


class HomeAssistantClient:
    def __init__(self, base_url: str, token: str, timeout_seconds: float = 8.0) -> None:
        self._base_url = base_url
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        self._timeout = timeout_seconds

    async def api_status(self) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(f"{self._base_url}/api/", headers=self._headers)
            response.raise_for_status()
            return response.json()

    async def entity_state(self, entity_id: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(
                f"{self._base_url}/api/states/{entity_id}",
                headers=self._headers,
            )
            response.raise_for_status()
            return response.json()

    async def all_states(self) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(f"{self._base_url}/api/states", headers=self._headers)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                return []
            return payload

    async def core_entities(self, entity_ids: tuple[str, ...]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for entity_id in entity_ids:
            state = await self.entity_state(entity_id)
            results.append(
                {
                    "entity_id": state.get("entity_id", entity_id),
                    "state": state.get("state"),
                    "unit": state.get("attributes", {}).get("unit_of_measurement"),
                    "friendly_name": state.get("attributes", {}).get("friendly_name"),
                    "last_updated": state.get("last_updated"),
                }
            )
        return results
