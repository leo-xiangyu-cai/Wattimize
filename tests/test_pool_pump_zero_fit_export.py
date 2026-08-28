import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from app import main


def _pool_pump_state(*, enabled: bool, power_w: float | None = None) -> dict[str, object]:
    return {
        "control": {
            "enabled": enabled,
            "available": True,
        },
        "metrics": {
            "power_w": power_w,
        },
    }


def _zero_fit_export_flow() -> dict[str, object]:
    return {
        "metrics": {
            "grid_w": -1945.0,
            "pv_w": 3965.0,
            "battery_w": -1268.0,
            "load_w": 557.0,
            "pool_pump_power_w": 1100.0,
        }
    }


def test_after_free_zero_fit_export_starts_pool_pump(monkeypatch) -> None:
    actions: list[bool] = []
    now_local = datetime(2026, 8, 28, 15, 26, tzinfo=ZoneInfo("Australia/Sydney"))

    async def enabled_rules() -> dict[str, bool]:
        return {"pool_pump_free_energy_control": True}

    async def auto_enabled() -> bool:
        return True

    async def load_state() -> dict[str, object]:
        return {}

    async def store_state(state: dict[str, object]) -> None:
        assert state["auto_started"] is True

    async def set_enabled(enabled: bool) -> dict[str, object]:
        actions.append(enabled)
        return {"ok": True}

    async def priority_snapshot(*args, **kwargs) -> dict[str, object]:
        return {
            "tesla": {"score": 100.0},
            "pool_pump": {"score": 0.0},
            "winner": "tesla",
        }

    async def pool_pump_states() -> dict[str, object]:
        return {}

    async def final_switch_states() -> dict[str, object]:
        return {}

    monkeypatch.setattr(main, "_tesla_grid_support_now_local", lambda: (now_local, "Australia/Sydney"))
    monkeypatch.setattr(main, "_load_time_window_rule_enabled_map", enabled_rules)
    monkeypatch.setattr(main, "_load_pool_pump_auto_control_enabled", auto_enabled)
    monkeypatch.setattr(main, "_pool_pump_states_by_id", pool_pump_states)
    monkeypatch.setattr(main, "_build_pool_pump_state", lambda states: _pool_pump_state(enabled=False))
    monkeypatch.setattr(main, "_load_pool_pump_free_energy_state", load_state)
    monkeypatch.setattr(main, "_store_pool_pump_free_energy_state", store_state)
    monkeypatch.setattr(main, "_pool_pump_set_enabled", set_enabled)
    monkeypatch.setattr(main, "_build_device_priority_snapshot", priority_snapshot)
    monkeypatch.setattr(main, "_collect_final_switch_states", final_switch_states)

    result = asyncio.run(
        main._run_pool_pump_free_energy_control_check(
            _zero_fit_export_flow(),
            {"decision": {"mode": "off"}, "tesla_state_before": {}},
        )
    )

    assert result["window_mode"] == "after_free_shoulder"
    assert result["zero_fit_export_sink"]["active"] is True
    assert result["decision_reason"] == "zero_fit_solar_export_pool_pump_within_export_surplus"
    assert result["action"] == {"type": "start_pool_pump", "reason": "zero_fit_solar_export_surplus"}
    assert actions == [True]


def test_after_free_zero_fit_export_exempts_pool_pump_off_period(monkeypatch) -> None:
    now_local = datetime(2026, 8, 28, 15, 26, tzinfo=ZoneInfo("Australia/Sydney"))

    async def enabled_rules() -> dict[str, bool]:
        return {"pool_pump_off_period_control": True}

    async def auto_enabled() -> bool:
        return True

    async def fail_set_enabled(enabled: bool) -> dict[str, object]:
        raise AssertionError(f"unexpected pool pump control: {enabled}")

    async def pool_pump_states() -> dict[str, object]:
        return {}

    async def final_switch_states() -> dict[str, object]:
        return {}

    monkeypatch.setattr(main, "_tesla_grid_support_now_local", lambda: (now_local, "Australia/Sydney"))
    monkeypatch.setattr(main, "_load_time_window_rule_enabled_map", enabled_rules)
    monkeypatch.setattr(main, "_load_pool_pump_auto_control_enabled", auto_enabled)
    monkeypatch.setattr(main, "_pool_pump_states_by_id", pool_pump_states)
    monkeypatch.setattr(main, "_build_pool_pump_state", lambda states: _pool_pump_state(enabled=True, power_w=1100.0))
    monkeypatch.setattr(main, "_pool_pump_set_enabled", fail_set_enabled)
    monkeypatch.setattr(main, "_collect_final_switch_states", final_switch_states)

    result = asyncio.run(main._run_pool_pump_off_period_check(_zero_fit_export_flow()))

    assert result["midday_window_mode"] == "after_free_shoulder"
    assert result["zero_fit_export_sink"]["active"] is True
    assert result["decision"] == {
        "mode": "run",
        "predicted_grid_w": -1945.0,
        "pool_pump_power_w": 1100.0,
    }
    assert result["action"] == {"type": "noop", "reason": "zero_fit_solar_export_exempt"}
