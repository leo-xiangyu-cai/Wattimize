from __future__ import annotations

import json
from pathlib import Path


_SCHEMA_PATH = Path(__file__).parent / "static" / "worker-log-schema.json"
_RAW_SCHEMA = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))

WORKER_LOG_SCHEMA: dict[str, object] = _RAW_SCHEMA
WORKER_LOG_CATEGORIES: dict[str, dict[str, object]] = dict(_RAW_SCHEMA.get("categories") or {})
WORKER_LOG_SERVICES: dict[str, dict[str, object]] = dict(_RAW_SCHEMA.get("services") or {})
WORKER_LOG_STATUSES: dict[str, dict[str, object]] = dict(_RAW_SCHEMA.get("statuses") or {})


def _entry_value(mapping: dict[str, dict[str, object]], key: str) -> str:
    entry = mapping[key]
    return str(entry.get("value") or key)


def category_value(key: str) -> str:
    return _entry_value(WORKER_LOG_CATEGORIES, key)


def service_value(key: str) -> str:
    return _entry_value(WORKER_LOG_SERVICES, key)


def status_value(key: str) -> str:
    return _entry_value(WORKER_LOG_STATUSES, key)


def category_config(value: str) -> dict[str, object] | None:
    normalized = str(value or "").strip().lower()
    for entry in WORKER_LOG_CATEGORIES.values():
        if str(entry.get("value") or "").strip().lower() == normalized:
            return entry
    return None


def service_config(value: str) -> dict[str, object] | None:
    normalized = str(value or "").strip().lower()
    for entry in WORKER_LOG_SERVICES.values():
        if str(entry.get("value") or "").strip().lower() == normalized:
            return entry
    return None


def status_config(value: str) -> dict[str, object] | None:
    normalized = str(value or "").strip().lower()
    for entry in WORKER_LOG_STATUSES.values():
        if str(entry.get("value") or "").strip().lower() == normalized:
            return entry
    return None


def category_service_values(category: str) -> tuple[str, ...]:
    entry = category_config(category)
    if not entry:
        return ()
    return tuple(str(item).strip() for item in entry.get("services") or () if str(item).strip())


def category_status_values(category: str) -> tuple[str, ...]:
    entry = category_config(category)
    if not entry:
        return ()
    return tuple(str(item).strip() for item in entry.get("statuses") or () if str(item).strip())


def service_status_values(service: str) -> tuple[str, ...]:
    entry = service_config(service)
    if not entry:
        return ()
    return tuple(str(item).strip() for item in entry.get("statuses") or () if str(item).strip())


WORKER_LOG_CATEGORY_ALL = category_value("all")
WORKER_LOG_CATEGORY_SAJ = category_value("saj")
WORKER_LOG_CATEGORY_SOLPLANET = category_value("solplanet")
WORKER_LOG_CATEGORY_COMBINED = category_value("combined")
WORKER_LOG_CATEGORY_TESLA = category_value("tesla")
WORKER_LOG_CATEGORY_NOTIFICATION = category_value("notification")
WORKER_LOG_CATEGORY_OPERATION = category_value("operation")

WORKER_LOG_STATUS_PENDING = status_value("pending")
WORKER_LOG_STATUS_OK = status_value("ok")
WORKER_LOG_STATUS_FAILED = status_value("failed")
WORKER_LOG_STATUS_TIMEOUT = status_value("timeout")
WORKER_LOG_STATUS_SKIPPED = status_value("skipped")
WORKER_LOG_STATUS_OUTSIDE_WINDOW = status_value("outside_window")
WORKER_LOG_STATUS_NOOP = status_value("noop")
WORKER_LOG_STATUS_APPLIED = status_value("applied")
WORKER_LOG_STATUS_SEND = status_value("send")
WORKER_LOG_STATUS_DISMISSED = status_value("dismissed")

WORKER_LOG_NOTIFICATION_ACTIVE_STATUSES = (
    WORKER_LOG_STATUS_SEND,
)
