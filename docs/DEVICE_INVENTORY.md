# Device Inventory

This file tracks Home Assistant entities that Wattimize operators may control manually or reuse for future automation.

## Xiaomi / Mijia Switches

| Label | HA Entity ID | Friendly Name | Device Type | Intended Use | Current Verification |
| --- | --- | --- | --- | --- | --- |
| Pool Pump Plug | `switch.cuco_v3_3c00_switch` | `Mijia Smart Plug 3 Switch` | Xiaomi smart plug | Pool water pump | Verified manual `turn_off` and `turn_on` via HA API on 2026-03-23 |

## Device Metrics

| Label | HA Entity ID | Friendly Name | Metric | Notes |
| --- | --- | --- | --- | --- |
| Pool Pump Plug Power | `sensor.cuco_v3_3c00_electric_power` | `Mijia Smart Plug 3 Electric Power Unit:1W` | Real-time power (W) | Read successfully via HA API on 2026-03-23 |

## Notes

- Home Assistant base URL is configured locally and manual control has been confirmed from this workspace.
- Add one row per controllable device so future automations can reference a stable entity id and intended load.
