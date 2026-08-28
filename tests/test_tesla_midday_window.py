import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from app import main


def test_after_free_shoulder_force_charges_tesla_below_threshold(monkeypatch) -> None:
    actions: list[tuple[str, int | bool | None]] = []
    now_local = datetime(2026, 8, 14, 15, 45, tzinfo=ZoneInfo("Australia/Sydney"))

    async def enabled_rules() -> dict[str, bool]:
        return {
            "saj_profile_self_consumption": True,
            "solplanet_free_energy_recovery": True,
            "tesla_after_free_shoulder_control": True,
        }

    async def saj_guard(*args, **kwargs) -> dict[str, object]:
        return {"action": "noop"}

    async def solplanet_recovery(*args, **kwargs) -> dict[str, object]:
        return {"action": {"type": "noop", "reason": "outside_free_energy_window"}}

    async def solplanet_export_control(*args, **kwargs) -> dict[str, object]:
        return {"action": {"type": "noop", "reason": "outside_export_window"}}

    async def tesla_auto_enabled() -> bool:
        return True

    async def force_threshold() -> int:
        return 80

    async def set_current(amps: int) -> None:
        actions.append(("set_current", amps))

    async def set_charging(enabled: bool) -> None:
        actions.append(("set_charging", enabled))

    async def restart_charging() -> None:
        actions.append(("restart_charging", None))

    monkeypatch.setattr(main, "_tesla_grid_support_now_local", lambda: (now_local, "Australia/Sydney"))
    monkeypatch.setattr(main, "_load_time_window_rule_enabled_map", enabled_rules)
    monkeypatch.setattr(main, "_guard_saj_profile_for_time", saj_guard)
    monkeypatch.setattr(main, "_apply_solplanet_free_energy_recovery", solplanet_recovery)
    monkeypatch.setattr(main, "_apply_solplanet_export_monitor_control", solplanet_export_control)
    monkeypatch.setattr(main, "_load_tesla_auto_control_enabled", tesla_auto_enabled)
    monkeypatch.setattr(main, "_load_tesla_force_max_charge_threshold_soc_percent", force_threshold)
    monkeypatch.setattr(main, "_tesla_set_charge_current", set_current)
    monkeypatch.setattr(main, "_tesla_set_charging", set_charging)
    monkeypatch.setattr(main, "_tesla_restart_charging", restart_charging)

    combined_flow = {
        "metrics": {
            "grid_w": -12.0,
            "battery2_soc_percent": 75.0,
            "tesla_battery_soc_percent": 45.0,
        }
    }
    tesla_observation_result = {
        "observation": {
            "observed_entity_count": 9,
            "charging": {
                "enabled": False,
                "requested_enabled": False,
                "connection_state": "plugged_not_charging",
                "status_text": "stopped",
                "current_amps": 0.0,
                "configured_current_amps": 12.0,
                "min_current_amps": 0.0,
                "max_current_amps": 15.0,
                "current_step_amps": 1.0,
                "power_w": 0.0,
            },
            "location": {
                "state": "home",
                "status": "home",
                "at_home": True,
                "auto_control_allowed": True,
            },
        }
    }

    result = asyncio.run(main._run_midday_window_check(combined_flow, tesla_observation_result))

    assert result["window_mode"] == "after_free_shoulder"
    assert result["after_free_shoulder_tesla_force_max_charge_active"] is True
    assert result["decision"] == {
        "mode": "charge",
        "charge_current_amps": 15,
        "predicted_grid_w": -12.0,
    }
    assert result["decision_reason"] == "after_free_shoulder_tesla_force_max_charge_below_threshold"
    assert result["action"] == {
        "type": "apply",
        "steps": ["set_current_15a", "start_charging"],
        "reason": "after_free_shoulder_tesla_force_max_charge_below_threshold",
    }
    assert actions == [("set_current", 15), ("set_charging", True)]
