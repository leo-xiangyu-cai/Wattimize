from __future__ import annotations

from datetime import UTC, datetime
from time import monotonic
from typing import Any, Callable

import httpx


class HomeAssistantClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        timeout_seconds: float = 8.0,
        request_logger: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self._base_url = base_url
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        self._timeout = timeout_seconds
        self._request_logger = request_logger

    def _emit_log(
        self,
        *,
        method: str,
        url: str,
        requested_at_utc: str,
        ok: bool,
        status_code: int | None,
        duration_ms: float | None,
        result_text: str | None,
        error_text: str | None,
    ) -> None:
        if not self._request_logger:
            return
        try:
            self._request_logger(
                {
                    "service": "home_assistant",
                    "method": method,
                    "url": url,
                    "requested_at_utc": requested_at_utc,
                    "ok": ok,
                    "status_code": status_code,
                    "duration_ms": duration_ms,
                    "result_text": result_text,
                    "error_text": error_text,
                }
            )
        except Exception:
            return

    async def _request_json(self, method: str, path: str, *, payload: dict[str, Any] | None = None) -> Any:
        url = f"{self._base_url}{path}"
        requested_at_utc = datetime.now(UTC).isoformat()
        started = monotonic()
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.request(method=method, url=url, headers=self._headers, json=payload)
            response.raise_for_status()
            self._emit_log(
                method=method,
                url=url,
                requested_at_utc=requested_at_utc,
                ok=True,
                status_code=response.status_code,
                duration_ms=round((monotonic() - started) * 1000, 1),
                result_text=response.text,
                error_text=None,
            )
            return response.json()
        except httpx.HTTPStatusError as exc:
            response = exc.response
            self._emit_log(
                method=method,
                url=url,
                requested_at_utc=requested_at_utc,
                ok=False,
                status_code=response.status_code if response is not None else None,
                duration_ms=round((monotonic() - started) * 1000, 1),
                result_text=response.text if response is not None else None,
                error_text=str(exc),
            )
            raise
        except httpx.HTTPError as exc:
            self._emit_log(
                method=method,
                url=url,
                requested_at_utc=requested_at_utc,
                ok=False,
                status_code=None,
                duration_ms=round((monotonic() - started) * 1000, 1),
                result_text=None,
                error_text=str(exc),
            )
            raise

    async def api_status(self) -> dict[str, Any]:
        payload = await self._request_json("GET", "/api/")
        return payload if isinstance(payload, dict) else {}

    async def entity_state(self, entity_id: str) -> dict[str, Any]:
        payload = await self._request_json("GET", f"/api/states/{entity_id}")
        return payload if isinstance(payload, dict) else {}

    async def all_states(self) -> list[dict[str, Any]]:
        payload = await self._request_json("GET", "/api/states")
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

    async def call_service(self, domain: str, service: str, data: dict[str, Any]) -> list[dict[str, Any]]:
        payload = await self._request_json("POST", f"/api/services/{domain}/{service}", payload=data)
        if not isinstance(payload, list):
            return []
        return payload
