import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest

from app import main, pre_free_energy as policy


def now(hour=9, minute=0):
    return datetime(2026, 9, 21, hour, minute, tzinfo=ZoneInfo("Australia/Sydney"))


def metrics(**changes):
    return dict(grid_w=-20, battery1_w=0, battery2_w=-3400,
                battery1_soc_percent=20, battery2_soc_percent=36,
                inverter2_w=-3600, tesla_charge_power_w=0, load_w=1000,
                pool_pump_power_w=0, **{}) | changes


def plan(m=None, **changes):
    args = dict(now=now(), fresh=True, home_reserve_kwh=2,
                tesla_eligible=True, tesla_soc=40, force_soc=40,
                tesla_max_amps=15, tesla_current_amps=0, pump_eligible=True,
                solplanet_discharge_limit_w=10000)
    return policy.plan(metrics() if m is None else m, **(args | changes))


def test_charging_battery_with_no_export_can_start_car():
    p = plan()
    assert p["tesla_amps"] == 6
    assert not p["pump_run"]  # no simultaneous starts using an old meter sample
    assert p["expected_free_charge_kwh"] == 36
    assert p["desired_solplanet_usable_at_11_kwh"] == 6.5


def test_uses_pump_when_car_is_full_or_unavailable():
    assert plan(tesla_soc=80)["pump_run"]
    assert plan(tesla_soc=80)["tesla_amps"] == 0
    assert plan(tesla_eligible=False)["pump_run"]


@pytest.mark.parametrize("change", [{"grid_w": 1}, {"battery2_w": None}, {"inverter2_w": float("nan")}])
def test_import_or_bad_telemetry_stops_flexible_loads(change):
    p = plan(metrics(**change))
    assert p["tesla_amps"] == 0 and not p["pump_run"]


@pytest.mark.parametrize("args", [{"fresh": False}, {"locked": True}])
def test_no_start_during_stale_data_or_cooldown(args):
    assert plan(**args)["tesla_amps"] == 0


def test_running_car_cannot_consume_protected_home_reserve():
    p = plan(metrics(battery2_soc_percent=12, battery2_w=3500, inverter2_w=4000,
                     tesla_charge_power_w=3000, load_w=4000), tesla_current_amps=13)
    assert p["tesla_amps"] == 0 and not p["pump_run"]


def test_running_car_yields_solar_to_rebuild_depleted_reserve():
    p = plan(metrics(battery2_soc_percent=10, battery2_w=-500, inverter2_w=-600,
                     tesla_charge_power_w=3000, load_w=4000), tesla_current_amps=13)
    assert p["tesla_amps"] == 0


def test_saj_soc_is_not_a_power_guarantee():
    p = plan(metrics(battery1_soc_percent=95, battery1_w=2000, battery2_soc_percent=10,
                     battery2_w=0, inverter2_w=0, load_w=2000))
    assert p["extra_solplanet_discharge_w"] == 0
    assert p["tesla_amps"] == 0 and not p["pump_run"]


def test_power_budget_shared_and_car_ramps_gradually():
    p = plan(metrics(tesla_charge_power_w=1440, load_w=2440), tesla_current_amps=6)
    assert p["tesla_amps"] == 8
    assert p["tesla_amps"] * 240 + (1100 if p["pump_run"] else 0) <= p["flexible_power_budget_w"]


def test_last_half_hour_still_consumes_surplus():
    assert plan(now=now(10, 50))["tesla_amps"] == 6


@pytest.mark.parametrize("hour,active", [(7, False), (8, True), (10, True), (11, False)])
def test_window_boundaries(hour, active):
    assert policy.window_active(now(hour)) is active
    assert (plan(now=now(hour))["reason"] != "outside_window") is active


def test_urgent_car_does_not_bypass_import_protection():
    assert plan(tesla_soc=20)["reason"] == "pre_free_urgent_tesla"
    assert plan(metrics(grid_w=1500), tesla_soc=20)["tesla_amps"] == 0


def setup_control(monkeypatch, *, running=False, soc=40, auto=True):
    state = {"pre_free_day": "2026-09-21", "pre_free_clean_samples": 3}
    monkeypatch.setattr(main, "_tesla_grid_support_now_local", lambda: (now(), "Australia/Sydney"))
    monkeypatch.setattr(main, "_load_saj_morning_grid_import_guard_state", AsyncMock(side_effect=lambda: dict(state)))
    async def save(value):
        state.clear()
        state.update(value)
    monkeypatch.setattr(main, "_store_saj_morning_grid_import_guard_state", save)
    monkeypatch.setattr(main, "_build_device_priority_snapshot", AsyncMock(return_value={
        "tesla": {"auto_control_enabled": auto, "force_max_charge_threshold_soc_percent": 40},
        "pool_pump": {"auto_control_enabled": True, "rolling_runtime_hours_24h": 3},
    }))
    monkeypatch.setattr(main, "_build_overnight_energy_reference", lambda _: {})
    monkeypatch.setattr(main, "_solplanet_get_schedule_live", AsyncMock(return_value={"Pout": 10000}))
    monkeypatch.setattr(main, "_pool_pump_states_by_id", AsyncMock(return_value={}))
    monkeypatch.setattr(main, "_build_pool_pump_state", lambda _: {"control": {"enabled": False, "available": True}})
    monkeypatch.setattr(main, "_load_pool_pump_auto_control_enabled", AsyncMock(return_value=True))
    monkeypatch.setattr(main, "_load_time_window_rule_enabled_map", AsyncMock(return_value={}))
    monkeypatch.setattr(main, "_collect_final_switch_states", AsyncMock(return_value={}))
    actions = []
    async def current(amps): actions.append(("current", amps))
    async def charging(enabled): actions.append(("car", enabled))
    async def pump(enabled): actions.append(("pump", enabled))
    monkeypatch.setattr(main, "_tesla_set_charge_current", current)
    monkeypatch.setattr(main, "_tesla_set_charging", charging)
    monkeypatch.setattr(main, "_pool_pump_set_enabled", pump)
    flow = {"updated_at": datetime.now(UTC).isoformat(), "metrics": metrics()}
    obs = {"observation": {"observed_entity_count": 9, "battery": {"level_percent": soc},
        "location": {"at_home": True}, "charging": {"enabled": running,
        "requested_enabled": running, "cable_connected": True, "status_text": "charging" if running else "stopped",
        "power_w": 0, "max_current_amps": 15, "configured_current_amps": 15, "current_amps": 0}}}
    return state, actions, flow, obs


def test_real_worker_routes_to_new_plan_and_sets_current_before_start(monkeypatch):
    _, actions, flow, obs = setup_control(monkeypatch)
    result = asyncio.run(main._run_saj_battery_watch_check(flow, obs))
    assert result["window_mode"] == "pre_free_energy"
    assert result["service_window_schedule"] == "08:00-11:00"
    assert actions == [("current", 6)]
    asyncio.run(main._run_pool_pump_overnight_control_check(flow, result))
    assert not any(a[0] == "pump" for a in actions)
    obs["observation"]["charging"]["configured_current_amps"] = 6
    result = asyncio.run(main._run_saj_battery_watch_check(flow, obs))
    assert actions == [("current", 6), ("car", True)]


def test_full_car_stops_before_pump_can_start_on_later_sample(monkeypatch):
    _, actions, flow, obs = setup_control(monkeypatch, running=True, soc=80)
    result = asyncio.run(main._run_saj_battery_watch_check(flow, obs))
    assert actions == [("car", False)]
    assert not result["pre_free_energy"]["pump_run"]


def test_pump_routes_to_new_window_and_off_worker_does_not_fight_it(monkeypatch):
    _, actions, flow, obs = setup_control(monkeypatch, soc=80)
    result = asyncio.run(main._run_saj_battery_watch_check(flow, obs))
    asyncio.run(main._run_pool_pump_overnight_control_check(flow, result))
    assert actions == [("pump", True)]
    off = asyncio.run(main._run_pool_pump_off_period_check(flow))
    assert off["log_status"] == "outside_window"


def test_disabled_tesla_not_commanded(monkeypatch):
    _, actions, flow, obs = setup_control(monkeypatch, auto=False)
    asyncio.run(main._run_saj_battery_watch_check(flow, obs))
    assert actions == []


def test_same_meter_sample_cannot_satisfy_stability_gate(monkeypatch):
    state, actions, flow, obs = setup_control(monkeypatch)
    state["pre_free_clean_samples"] = 0
    for _ in range(4):
        asyncio.run(main._run_saj_battery_watch_check(flow, obs))
    assert state["pre_free_clean_samples"] == 1
    assert actions == []


def test_import_sheds_pump_even_if_car_stop_fails_and_persists_cooldown(monkeypatch):
    state, actions, flow, obs = setup_control(monkeypatch, running=True)
    monkeypatch.setattr(main, "_tesla_control_states", AsyncMock(return_value=[]))
    monkeypatch.setattr(main, "_build_tesla_observation_payload", lambda _: obs["observation"])
    monkeypatch.setattr(main, "_build_pool_pump_state", lambda _: {"control": {"enabled": True, "available": True}})
    monkeypatch.setattr(main, "_tesla_set_charging", AsyncMock(side_effect=RuntimeError("offline")))
    flow["metrics"]["grid_w"] = 1900
    with pytest.raises(RuntimeError, match="offline"):
        asyncio.run(main._enforce_tesla_morning_import_guard(flow, dict(state), now()))
    assert actions == [("pump", False)]
    assert main._pre_free_cooldown_active(state, now())
    assert not main._pre_free_cooldown_active(state, now() + timedelta(minutes=6))
    assert state["pre_free_clean_samples"] == 0


def test_failed_car_plan_cannot_enable_pump(monkeypatch):
    _, actions, flow, _ = setup_control(monkeypatch)
    asyncio.run(main._run_pool_pump_overnight_control_check(flow, None))
    assert actions == []


def test_inverter_zero_output_limit_never_funds_extra_discharge():
    p = plan(metrics(battery2_w=0, battery2_soc_percent=90), solplanet_discharge_limit_w=0)
    assert p["extra_solplanet_discharge_w"] == 0
    assert p["tesla_amps"] == 0


def test_inverter_near_output_limit_caps_extra_power():
    p = plan(metrics(battery2_w=7500, inverter2_w=7900, battery2_soc_percent=95),
             now=now(10, 50), solplanet_discharge_limit_w=8000)
    assert p["extra_solplanet_discharge_w"] <= 100


def test_manually_running_load_power_cannot_be_reallocated():
    p = plan(metrics(battery2_w=0, tesla_charge_power_w=3000, load_w=4000),
             tesla_eligible=False, solplanet_discharge_limit_w=0)
    assert not p["pump_run"]
    p = plan(metrics(battery2_w=0, load_w=2100), pump_running_w=1100,
             pump_eligible=False, solplanet_discharge_limit_w=0)
    assert p["tesla_amps"] == 0


def test_background_import_with_loads_off_does_not_extend_cooldown(monkeypatch):
    state, _, flow, obs = setup_control(monkeypatch)
    monkeypatch.setattr(main, "_tesla_control_states", AsyncMock(return_value=[]))
    monkeypatch.setattr(main, "_build_tesla_observation_payload", lambda _: obs["observation"])
    flow["metrics"]["grid_w"] = 20
    asyncio.run(main._enforce_tesla_morning_import_guard(flow, dict(state), now()))
    assert "pre_free_retry_after" not in state
