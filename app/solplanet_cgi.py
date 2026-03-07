from __future__ import annotations

from datetime import UTC, datetime
from time import monotonic
from typing import Any, Callable

import httpx


class SolplanetCgiClient:
    def __init__(
        self,
        host: str,
        port: int = 443,
        scheme: str = "https",
        verify_ssl: bool = False,
        timeout_seconds: float = 8.0,
        request_logger: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self._host = host.strip()
        self._port = port
        self._scheme = scheme if scheme in ("http", "https") else "https"
        self._verify_ssl = verify_ssl
        self._timeout = timeout_seconds
        self._client = httpx.AsyncClient(
            timeout=self._timeout,
            verify=self._verify_ssl,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )
        self._request_logger = request_logger

    @property
    def configured(self) -> bool:
        return bool(self._host)

    def _url(self, endpoint: str) -> str:
        clean = endpoint.lstrip("/")
        return f"{self._scheme}://{self._host}:{self._port}/{clean}"

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
                    "service": "solplanet_cgi",
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

    async def _get_json(self, endpoint: str) -> dict[str, Any]:
        url = self._url(endpoint)
        requested_at_utc = datetime.now(UTC).isoformat()
        started = monotonic()
        try:
            response = await self._client.get(url)
            response.raise_for_status()
            self._emit_log(
                method="GET",
                url=url,
                requested_at_utc=requested_at_utc,
                ok=True,
                status_code=response.status_code,
                duration_ms=round((monotonic() - started) * 1000, 1),
                result_text=response.text,
                error_text=None,
            )
            payload = response.json()
            return payload if isinstance(payload, dict) else {}
        except httpx.HTTPStatusError as exc:
            response = exc.response
            self._emit_log(
                method="GET",
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
                method="GET",
                url=url,
                requested_at_utc=requested_at_utc,
                ok=False,
                status_code=None,
                duration_ms=round((monotonic() - started) * 1000, 1),
                result_text=None,
                error_text=str(exc),
            )
            raise

    async def _post_json(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        url = self._url(endpoint)
        requested_at_utc = datetime.now(UTC).isoformat()
        started = monotonic()
        try:
            response = await self._client.post(url, json=data)
            response.raise_for_status()
            self._emit_log(
                method="POST",
                url=url,
                requested_at_utc=requested_at_utc,
                ok=True,
                status_code=response.status_code,
                duration_ms=round((monotonic() - started) * 1000, 1),
                result_text=response.text,
                error_text=None,
            )
            payload = response.json()
            return payload if isinstance(payload, dict) else {}
        except httpx.HTTPStatusError as exc:
            response = exc.response
            self._emit_log(
                method="POST",
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
                method="POST",
                url=url,
                requested_at_utc=requested_at_utc,
                ok=False,
                status_code=None,
                duration_ms=round((monotonic() - started) * 1000, 1),
                result_text=None,
                error_text=str(exc),
            )
            raise

    async def get_inverter_info(self) -> dict[str, Any]:
        return await self._get_json("getdev.cgi?device=2")

    async def get_inverter_data(self, inverter_sn: str) -> dict[str, Any]:
        return await self._get_json(f"getdevdata.cgi?device=2&sn={inverter_sn}")

    async def get_meter_data(self) -> dict[str, Any]:
        return await self._get_json("getdevdata.cgi?device=3")

    async def get_meter_info(self) -> dict[str, Any]:
        return await self._get_json("getdev.cgi?device=3")

    async def get_dongle_info(self) -> dict[str, Any]:
        return await self._get_json("getdev.cgi?device=0")

    async def get_battery_info(self, inverter_sn: str) -> dict[str, Any]:
        return await self._get_json(f"getdev.cgi?device=4&sn={inverter_sn}")

    async def get_battery_data(self, battery_sn: str) -> dict[str, Any]:
        return await self._get_json(f"getdevdata.cgi?device=4&sn={battery_sn}")

    async def get_schedule(self) -> dict[str, Any]:
        return await self._get_json("getdefine.cgi")

    async def get_device_info(self, device_id: int) -> dict[str, Any]:
        return await self._get_json(f"getdev.cgi?device={device_id}")

    async def get_device_data(self, device_id: int, sn: str | None = None) -> dict[str, Any]:
        path = f"getdevdata.cgi?device={device_id}"
        if sn:
            path += f"&sn={sn}"
        return await self._get_json(path)

    async def set_value(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._post_json("setting.cgi", payload)

    async def aclose(self) -> None:
        await self._client.aclose()
