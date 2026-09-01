from app import main


def _state(entity_id: str, state: str, unit: str | None = None) -> dict[str, object]:
    attrs: dict[str, object] = {}
    if unit is not None:
        attrs["unit_of_measurement"] = unit
    return {
        "entity_id": entity_id,
        "state": state,
        "attributes": attrs,
    }


def test_stopped_tesla_power_prefers_power_sensor_over_current() -> None:
    observation = main._build_tesla_observation_payload(
        [
            _state("switch.leo_s_tesla_charge", "off"),
            _state("sensor.leo_s_tesla_charging", "stopped"),
            _state("binary_sensor.leo_s_tesla_charge_cable", "on"),
            _state("sensor.leo_s_tesla_charger_current", "3", "A"),
            _state("number.leo_s_tesla_charge_current", "15", "A"),
            _state("sensor.leo_s_tesla_charger_power", "0", "kW"),
            _state("sensor.leo_s_tesla_charger_voltage", "237", "V"),
        ]
    )

    charging = observation["charging"]
    assert isinstance(charging, dict)
    assert charging["enabled"] is False
    assert charging["requested_enabled"] is False
    assert charging["connection_state"] == "plugged_not_charging"
    assert charging["current_amps"] == 3.0
    assert charging["configured_current_amps"] == 15.0
    assert charging["power_w"] == 0.0
