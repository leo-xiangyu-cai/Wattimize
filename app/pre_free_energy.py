"""Conservative, measured-power planning for the 08:00–11:00 window."""

from datetime import datetime
from math import floor, isfinite

START_HOUR = 8
END_HOUR = 11
TESLA_TARGET_SOC = 80
MIN_CHARGE_AMPS = 6
POWER_MARGIN_W = 300.0
IMPORT_COOLDOWN_SECONDS = 300


def window_active(now: datetime) -> bool:
    return START_HOUR <= now.hour < END_HOUR


def plan(
    metrics: dict, *, now: datetime, fresh: bool, home_reserve_kwh: float | None,
    tesla_eligible: bool, tesla_soc: float | None, force_soc: float,
    tesla_max_amps: float, tesla_current_amps: float,
    pump_eligible: bool, pump_power_w: float = 1100.0,
    pump_running_w: float = 0.0, locked: bool = False,
    solplanet_discharge_limit_w: float = 0.0,
) -> dict:
    """Keep a home reserve, reclaim charging PV, and bound battery discharge.

    Free charging is an estimate, not a promise: 10 kW * 4h * 90%.
    No future sunshine or unverified SAJ discharge capability funds new loads.
    """
    result = dict(window="pre_free_energy", schedule="08:00-11:00",
                  tesla_amps=0, pump_run=False, reason="pre_free_unavailable")
    if not window_active(now):
        return {**result, "reason": "outside_window"}
    required = ("grid_w", "battery1_w", "battery2_w", "battery1_soc_percent",
                "battery2_soc_percent", "inverter2_w", "tesla_charge_power_w", "load_w")
    if not fresh or any(
        not isinstance(metrics.get(k), (int, float)) or not isfinite(metrics[k]) for k in required
    ):
        return {**result, "reason": "pre_free_telemetry_unavailable"}
    if not all(0 <= metrics[k] <= 100 for k in ("battery1_soc_percent", "battery2_soc_percent")):
        return {**result, "reason": "pre_free_telemetry_unavailable"}
    if locked or metrics["grid_w"] > 0:
        return {**result, "reason": "pre_free_import_protection"}

    hours = max((now.replace(hour=11, minute=0, second=0, microsecond=0) - now).total_seconds() / 3600, 1 / 60)
    solplanet_usable = max(0, (metrics["battery2_soc_percent"] - 10) * 0.5)
    expected_free_charge = 10 * 4 * 0.90
    # 95% target minus the 10% protected floor leaves 42.5 kWh usable.
    target_usable_at_11 = max(0, 42.5 - expected_free_charge)
    house_w = max(0, metrics["load_w"] - metrics["tesla_charge_power_w"] - pump_running_w)
    # Reserve enough for the house through 11 even if sunlight disappears.
    reserve = max(house_w * hours / 1000, home_reserve_kwh or 0) + 2.0
    urgent = tesla_eligible and tesla_soc is not None and tesla_soc < force_soc
    retained = reserve if urgent else max(reserve, target_usable_at_11)
    energy_surplus = max(0, solplanet_usable - retained)
    # Additional discharge never assumes SAJ can deliver its nameplate power.
    discharge_allowance = energy_surplus / hours * 1000
    spare_inverter = max(0, min(8000, solplanet_discharge_limit_w) - max(0, metrics["inverter2_w"]))
    extra_discharge = min(3000, spare_inverter,
                          max(0, discharge_allowance - max(0, metrics["battery2_w"])))
    charging_pv = max(0, -metrics["battery1_w"]) + max(0, -metrics["battery2_w"])
    # When below the home reserve, leave charging power to replenish it first.
    refill_w = max(0, reserve - solplanet_usable) / hours * 1000
    reclaim_w = max(0, charging_pv - refill_w)
    current_ev = max(0, metrics["tesla_charge_power_w"])
    budget = max(0, current_ev + pump_running_w - metrics["grid_w"]
                 + reclaim_w + extra_discharge - POWER_MARGIN_W
                 - max(0, metrics["battery2_w"] - discharge_allowance)
                 - max(0, metrics["battery1_w"])
                 - max(0, refill_w - charging_pv)
                 - (current_ev if not tesla_eligible else 0)
                 - (pump_running_w if not pump_eligible else 0))
    voltage = 240.0  # Conservative versus the observed 210–240 V supply.
    amps = 0
    if tesla_eligible and tesla_soc is not None and tesla_soc < TESLA_TARGET_SOC:
        amps = min(floor(budget / voltage), floor(tesla_max_amps))
        if amps < MIN_CHARGE_AMPS:
            amps = 0
        else:
            # Start at 6A and increase by at most 2A per collector cycle.
            amps = min(amps, max(MIN_CHARGE_AMPS, floor(tesla_current_amps) + 2))
    remainder = max(0, budget - amps * voltage)
    pump_run = bool(pump_eligible and remainder >= pump_power_w + 200)
    # Never start two loads from the same pre-command meter reading.
    if amps > 0 and current_ev < 100 and pump_running_w < 100:
        pump_run = False
    result.update(
        reason="pre_free_urgent_tesla" if urgent else "pre_free_make_charge_room",
        tesla_amps=amps, pump_run=pump_run, hours_to_11=round(hours, 3),
        solplanet_usable_kwh=round(solplanet_usable, 3),
        expected_free_charge_kwh=expected_free_charge,
        desired_solplanet_usable_at_11_kwh=target_usable_at_11,
        home_reserve_kwh=round(reserve, 3), retained_usable_kwh=round(retained, 3),
        reclaim_charging_power_w=round(reclaim_w, 1),
        extra_solplanet_discharge_w=round(extra_discharge, 1),
        solplanet_discharge_limit_w=solplanet_discharge_limit_w,
        flexible_power_budget_w=round(budget, 1),
        tesla_target_soc_percent=TESLA_TARGET_SOC,
    )
    return result
