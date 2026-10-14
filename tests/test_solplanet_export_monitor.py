from dataclasses import replace
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from app import main


def _build_monitor(
    *,
    now_local: datetime,
    usable_kwh: float,
    load_w: float = 0.0,
    saj_usable_kwh: float | None = None,
    solplanet_usable_kwh: float | None = None,
    reserve_floor_kwh: float | None = None,
) -> dict[str, object]:
    metrics_map: dict[str, object] = {
        "usable_battery_total_kwh": usable_kwh,
        "load_w": load_w,
    }
    if saj_usable_kwh is not None:
        metrics_map["battery1_usable_kwh"] = saj_usable_kwh
    if solplanet_usable_kwh is not None:
        metrics_map["battery2_usable_kwh"] = solplanet_usable_kwh
    original_settings = main.settings
    if reserve_floor_kwh is not None:
        main.settings = replace(original_settings, solplanet_export_reserve_floor_kwh=reserve_floor_kwh)
    try:
        return main._build_solplanet_export_monitor(
            metrics_map=metrics_map,
            now_local=now_local,
            export_tracking={"total_export_kwh": 0.0},
            current_grid_import_w=0.0,
            current_grid_export_w=0.0,
            # These exercise Solplanet's sale/reserve policy after the SAJ
            # priority phase. That phase has its own integration tests.
            saj_depleted=True,
        )
    finally:
        main.settings = original_settings


def test_peak_hold_sells_when_net_export_cannot_reach_target() -> None:
    monitor = _build_monitor(
        now_local=datetime(2026, 8, 22, 20, 48, tzinfo=ZoneInfo("Australia/Sydney")),
        usable_kwh=45.0,
        load_w=3000.0,
    )
    assert monitor["phase"] == "peak_hold"
    assert monitor["recommendation"] == "sell"
    assert monitor["reason"] == "export_window_full_power_runway_guard"
    assert monitor["reserve_floor_kwh"] == float(main.settings.solplanet_export_reserve_floor_kwh)
    assert monitor["full_power_export_remaining_kwh"] == 22.0
    assert monitor["net_export_remaining_kwh"] == 15.4
    assert monitor["surplus_above_floor_kwh"] == round(45.0 - float(main.settings.solplanet_export_reserve_floor_kwh), 4)
    assert monitor["early_sell_required_kwh"] == 3.1
    assert monitor["target_sell_kwh"] == 19.9
    assert monitor["recommended_pout_w"] == 10000.0


def test_peak_hold_tracks_end_reserve_when_full_power_is_not_required() -> None:
    monitor = _build_monitor(
        now_local=datetime(2026, 8, 22, 20, 0, tzinfo=ZoneInfo("Australia/Sydney")),
        usable_kwh=40.0,
        load_w=5000.0,
    )
    assert monitor["phase"] == "peak_hold"
    assert monitor["recommendation"] == "sell"
    assert monitor["reason"] == "export_window_track_end_reserve"
    assert monitor["reserve_floor_kwh"] == float(main.settings.solplanet_export_reserve_floor_kwh)
    assert monitor["full_power_export_remaining_kwh"] == 30.0
    assert monitor["net_export_remaining_kwh"] == 15.0
    assert monitor["surplus_above_floor_kwh"] == round(40.0 - float(main.settings.solplanet_export_reserve_floor_kwh), 4)
    assert monitor["early_sell_required_kwh"] == 3.5
    assert monitor["exportable_after_home_load_kwh"] == 6.5
    assert monitor["target_sell_kwh"] == 6.5
    assert monitor["recommended_pout_w"] == pytest.approx(7166.7)


def test_peak_hold_sells_surplus_even_when_net_export_can_reach_old_daily_target() -> None:
    monitor = _build_monitor(
        now_local=datetime(2026, 8, 22, 20, 0, tzinfo=ZoneInfo("Australia/Sydney")),
        usable_kwh=40.0,
    )

    assert monitor["phase"] == "peak_hold"
    assert monitor["recommendation"] == "sell"
    assert monitor["reason"] == "export_window_track_end_reserve"
    assert monitor["full_power_export_remaining_kwh"] == 30.0
    assert monitor["net_export_remaining_kwh"] == 30.0
    assert monitor["surplus_above_floor_kwh"] == round(40.0 - float(main.settings.solplanet_export_reserve_floor_kwh), 4)
    assert monitor["early_sell_required_kwh"] == 0.0
    assert monitor["target_sell_kwh"] == 21.5
    assert monitor["recommended_pout_w"] == pytest.approx(7166.7)


def test_export_monitor_sell_slots_cover_rest_of_export_window() -> None:
    slots = main._solplanet_export_monitor_discharge_slots(
        datetime(2026, 8, 22, 20, 12, tzinfo=ZoneInfo("Australia/Sydney")),
        should_sell=True,
    )

    decoded = [main._solplanet_decode_slot(slot) for slot in slots]

    assert decoded[0]["time_text"] == "16:00"
    assert decoded[0]["end_time_text"] == "20:15"
    assert decoded[1]["time_text"] == "20:15"
    assert decoded[1]["end_time_text"] == "23:00"


def test_export_monitor_uses_combined_usable_energy_for_sell_floor_when_available() -> None:
    reserve_floor_kwh = float(main.settings.solplanet_export_reserve_floor_kwh)
    monitor = _build_monitor(
        now_local=datetime(2026, 8, 22, 20, 12, tzinfo=ZoneInfo("Australia/Sydney")),
        usable_kwh=46.15,
        saj_usable_kwh=3.15,
        solplanet_usable_kwh=43.0,
        load_w=5000.0,
    )

    assert monitor["usable_battery_total_kwh"] == 46.15
    assert monitor["usable_battery_by_system_kwh"] == {"saj": 3.15, "solplanet": 43.0}
    assert monitor["control_usable_battery_kwh"] == 46.15
    assert monitor["control_usable_battery_source"] == "combined"
    assert monitor["surplus_above_floor_kwh"] == round(46.15 - reserve_floor_kwh, 4)
    assert monitor["target_sell_kwh"] == pytest.approx(13.65)
    assert monitor["projected_usable_after_target_kwh"] == pytest.approx(reserve_floor_kwh + 0.5)


def test_export_monitor_stops_when_solplanet_at_floor_even_if_combined_has_surplus() -> None:
    reserve_floor_kwh = 10.0
    monitor = _build_monitor(
        now_local=datetime(2026, 9, 6, 22, 44, tzinfo=ZoneInfo("Australia/Sydney")),
        usable_kwh=16.45,
        saj_usable_kwh=16.45,
        solplanet_usable_kwh=0.0,
        load_w=610.0,
        reserve_floor_kwh=reserve_floor_kwh,
    )

    assert monitor["phase"] == "late_target"
    assert monitor["recommendation"] == "stop"
    assert monitor["reason"] == "solplanet_reserve_floor_reached"
    assert monitor["control_usable_battery_kwh"] == 16.45
    assert monitor["control_usable_battery_source"] == "combined"
    assert monitor["surplus_above_floor_kwh"] == round(16.45 - reserve_floor_kwh, 4)
    assert monitor["target_sell_kwh"] == 0.0


def test_export_monitor_does_not_dump_today_evening_surplus_at_full_power() -> None:
    monitor = _build_monitor(
        now_local=datetime(2026, 9, 7, 20, 34, 4, tzinfo=ZoneInfo("Australia/Sydney")),
        usable_kwh=21.8,
        saj_usable_kwh=7.8,
        solplanet_usable_kwh=14.0,
        load_w=1926.0,
        reserve_floor_kwh=10.0,
    )

    assert monitor["phase"] == "peak_hold"
    assert monitor["recommendation"] == "sell"
    assert monitor["reason"] == "export_window_track_end_reserve"
    assert monitor["runway_target_kwh"] == pytest.approx(34.3222)
    assert monitor["projected_home_load_to_export_end_kwh"] == pytest.approx(4.6845)
    assert monitor["exportable_after_home_load_kwh"] == pytest.approx(6.6155)
    assert monitor["recommended_pout_w"] == pytest.approx(4646.0, abs=1.0)
