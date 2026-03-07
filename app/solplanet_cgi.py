from __future__ import annotations

from typing import Any

import httpx


class SolplanetCgiClient:
    def __init__(
        self,
        host: str,
        port: int = 443,
        scheme: str = "https",
        verify_ssl: bool = False,
        timeout_seconds: float = 8.0,
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

    @property
    def configured(self) -> bool:
        return bool(self._host)

    def _url(self, endpoint: str) -> str:
        clean = endpoint.lstrip("/")
        return f"{self._scheme}://{self._host}:{self._port}/{clean}"

    async def _get_json(self, endpoint: str) -> dict[str, Any]:
        response = await self._client.get(self._url(endpoint))
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    async def _post_json(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        response = await self._client.post(self._url(endpoint), json=data)
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

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
