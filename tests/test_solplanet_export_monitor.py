from datetime import datetime
from zoneinfo import ZoneInfo

from app import main


def _build_monitor(*, now_local: datetime, usable_kwh: float) -> dict[str, object]:
    return main._build_solplanet_export_monitor(
        metrics_map={
            "usable_battery_total_kwh": usable_kwh,
            "load_w": 0.0,
        },
        now_local=now_local,
        export_tracking={"total_export_kwh": 0.0},
        current_grid_import_w=0.0,
        current_grid_export_w=0.0,
    )


def test_peak_hold_sells_when_full_power_cannot_clear_surplus() -> None:
    monitor = _build_monitor(
        now_local=datetime(2026, 8, 22, 20, 48, tzinfo=ZoneInfo("Australia/Sydney")),
        usable_kwh=45.0,
    )

    assert monitor["phase"] == "peak_hold"
    assert monitor["recommendation"] == "sell"
    assert monitor["reason"] == "peak_hold_full_power_cannot_clear_surplus_before_export_end"
    assert monitor["early_sell_floor_kwh"] == 20.0
    assert monitor["full_power_export_remaining_kwh"] == 22.0
    assert monitor["surplus_above_early_sell_floor_kwh"] == 25.0
    assert monitor["early_sell_required_kwh"] == 3.0
    assert monitor["recommended_pout_w"] == 10000.0


def test_peak_hold_keeps_holding_when_full_power_can_clear_surplus() -> None:
    monitor = _build_monitor(
        now_local=datetime(2026, 8, 22, 20, 0, tzinfo=ZoneInfo("Australia/Sydney")),
        usable_kwh=40.0,
    )

    assert monitor["phase"] == "peak_hold"
    assert monitor["recommendation"] == "hold_until_late_phase"
    assert monitor["reason"] == "evening_peak_reserve_period"
    assert monitor["early_sell_floor_kwh"] == 20.0
    assert monitor["full_power_export_remaining_kwh"] == 30.0
    assert monitor["surplus_above_early_sell_floor_kwh"] == 20.0
    assert monitor["early_sell_required_kwh"] == 0.0
