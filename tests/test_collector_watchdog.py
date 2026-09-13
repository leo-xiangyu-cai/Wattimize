import asyncio
from datetime import UTC, datetime, timedelta

import app.main as main


def _reset_watchdog(monkeypatch):
    now = datetime(2026, 9, 9, 0, 0, tzinfo=UTC)
    monkeypatch.setitem(main.collector_watchdog_state, "last_recovery_at", None)
    monkeypatch.setitem(main.collector_watchdog_state, "last_skipped_at", None)
    monkeypatch.setitem(main.collector_watchdog_state, "last_skipped_reason", None)
    monkeypatch.setitem(main.collector_watchdog_state, "recovery_history", [])
    monkeypatch.setattr(main, "collector_task", None)
    for system in ("saj", "solplanet", "combined"):
        monkeypatch.setitem(
            main.collector_status,
            system,
            {
                "last_success_at": now.isoformat(),
                "last_error": None,
                "last_round_result": {"stored_sample": True, "reason": None},
            },
        )


def test_collector_watchdog_triggers_for_stale_combined_solplanet_source(monkeypatch):
    _reset_watchdog(monkeypatch)
    now = datetime(2026, 9, 9, 0, 0, tzinfo=UTC)
    monkeypatch.setitem(
        main.collector_status,
        "combined",
        {
            "last_success_at": (now - timedelta(minutes=20)).isoformat(),
            "last_error": "source_not_stored",
            "last_round_result": {
                "stored_sample": False,
                "reason": "source_not_stored",
                "source_details": {
                    "saj": {"stored_sample": True, "reason": None},
                    "solplanet": {"stored_sample": False, "reason": "incomplete_flow"},
                },
            },
        },
    )

    trigger = main._collector_watchdog_recovery_trigger(now)

    assert trigger is not None
    assert trigger["system"] == "combined"
    assert trigger["reason"] == "source_not_stored"
    assert trigger["action"] == "recreate_solplanet_client_and_restart_collector"


def test_collector_watchdog_ignores_recent_success(monkeypatch):
    _reset_watchdog(monkeypatch)
    now = datetime(2026, 9, 9, 0, 0, tzinfo=UTC)
    monkeypatch.setitem(
        main.collector_status,
        "combined",
        {
            "last_success_at": (now - timedelta(minutes=3)).isoformat(),
            "last_error": "source_not_stored",
            "last_round_result": {"stored_sample": False, "reason": "source_not_stored"},
        },
    )

    assert main._collector_watchdog_recovery_trigger(now) is None


def test_collector_watchdog_respects_recovery_cooldown(monkeypatch):
    _reset_watchdog(monkeypatch)
    now = datetime(2026, 9, 9, 0, 0, tzinfo=UTC)
    monkeypatch.setitem(main.collector_watchdog_state, "last_recovery_at", (now - timedelta(minutes=5)).isoformat())
    monkeypatch.setitem(
        main.collector_status,
        "solplanet",
        {
            "last_success_at": (now - timedelta(minutes=30)).isoformat(),
            "last_error": "incomplete_flow",
            "last_round_result": {"stored_sample": False, "reason": "incomplete_flow"},
        },
    )

    assert main._collector_watchdog_recovery_trigger(now) is None
    assert main.collector_watchdog_state["last_skipped_reason"] == "recovery_cooldown"


def test_watchdog_restart_recreates_client_without_reacquiring_runtime_lock(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.closed = False

        async def aclose(self):
            self.closed = True

    async def exercise():
        old_client = FakeClient()
        new_client = FakeClient()

        async def no_op():
            return None

        monkeypatch.setattr(main, "runtime_lock", asyncio.Lock())
        monkeypatch.setattr(main, "collector_task", None)
        monkeypatch.setattr(main, "solplanet_client", old_client)
        monkeypatch.setattr(main, "_new_solplanet_client", lambda _: new_client)
        monkeypatch.setattr(main, "_reset_solplanet_caches", lambda: None)
        monkeypatch.setattr(main, "_stop_collector", no_op)
        monkeypatch.setattr(main, "_start_collector", no_op)

        result = await asyncio.wait_for(
            main._restart_collector_runtime(recreate_solplanet_client=True),
            timeout=0.2,
        )

        assert result["solplanet_client_recreated"] is True
        assert main.solplanet_client is new_client
        assert old_client.closed is True

    asyncio.run(exercise())
