import asyncio
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo
from unittest.mock import AsyncMock

import pytest

from app import main


@pytest.mark.parametrize('hour,minute,expected', [(0,0,True),(8,41,True),(9,0,True),(10,59,True),(11,0,False),(14,59,False),(15,0,True),(18,0,True),(23,59,True)])
def test_paid_window(hour, minute, expected):
    assert main._is_within_saj_morning_grid_import_guard_window(
        datetime(2026,9,13,hour,minute,tzinfo=ZoneInfo('Australia/Sydney'))
    ) is expected


def setup_guard(monkeypatch, *, hour=9, state=None, enabled=True):
    state = dict(state or {})
    now = datetime(2026,9,13,hour,0,tzinfo=ZoneInfo('Australia/Sydney'))
    monkeypatch.setattr(main, '_tesla_grid_support_now_local', lambda: (now, 'Australia/Sydney'))
    monkeypatch.setattr(main, '_load_time_window_rule_enabled_map', AsyncMock(return_value={'saj_morning_grid_import_guard': enabled}))
    monkeypatch.setattr(main, '_load_saj_morning_grid_import_guard_state', AsyncMock(side_effect=lambda: dict(state)))
    async def save(value):
        state.clear()
        state.update(value)
    monkeypatch.setattr(main, '_store_saj_morning_grid_import_guard_state', save)
    monkeypatch.setattr(main, '_persist_worker_control_log', AsyncMock())
    discharge = AsyncMock(return_value={'ok': True})
    release = AsyncMock(return_value={'ok': True})
    monkeypatch.setattr(main, 'put_saj_manual_discharge_power', discharge)
    monkeypatch.setattr(main, 'put_saj_self_consumption', release)
    return state, discharge, release


def run_guard(grid=2100, battery=-2000, soc=91):
    return asyncio.run(main._run_saj_morning_grid_import_guard_check({'updated_at':datetime.now(UTC).isoformat(), 'metrics': {
        'grid_w':grid, 'battery1_w':battery, 'battery1_soc_percent':soc,
    }}))


def test_morning_incident_at_nine_stops_charging_without_forcing_excess_export(monkeypatch):
    state, discharge, release = setup_guard(monkeypatch)
    run_guard()
    assert discharge.await_count == 0
    result = run_guard()
    assert discharge.call_args.args[0].power_w == 50
    assert state['active']
    assert result['verification'] == 'await_next_power_sample'
    assert discharge.call_args.kwargs['trigger_source'] == 'worker:saj_morning_grid_import_guard'
    release.assert_not_awaited()


def test_increases_discharge_even_when_already_discharging(monkeypatch):
    _, discharge, _ = setup_guard(monkeypatch, hour=23)
    run_guard(grid=3000,battery=2000)
    run_guard(grid=3000,battery=2000)
    assert discharge.call_args.args[0].power_w == 4950


def test_retries_ineffective_command_with_power_limit(monkeypatch):
    state, discharge, _ = setup_guard(monkeypatch, state={
        'active':True, 'target_power_w':5000,
        'last_action_at_utc':(datetime.now(UTC)-timedelta(minutes=3)).isoformat(),
    })
    result = run_guard(grid=7000,battery=-1000)
    assert discharge.call_args.args[0].power_w == 5000
    assert result['decision']['mode'] == 'adjust_guard'
    assert state['active']


def test_retry_cooldown(monkeypatch):
    _, discharge, _ = setup_guard(monkeypatch,state={
        'active':True,'target_power_w':5000,'last_action_at_utc':datetime.now(UTC).isoformat(),
    })
    result=run_guard(grid=7000,battery=-1000)
    discharge.assert_not_awaited()
    assert result['verification'] == 'grid_import_persists'


def test_successful_support_is_not_released_at_nine(monkeypatch):
    state, discharge, release = setup_guard(monkeypatch,state={
        'active':True,'target_power_w':2000,
        'low_import_since_utc':(datetime.now(UTC)-timedelta(minutes=10)).isoformat(),
    })
    run_guard(grid=40,battery=2000)
    release.assert_not_awaited()
    assert state['active']


@pytest.mark.parametrize('hour,soc,enabled',[(11,None,True),(15,20,True),(9,80,False)])
def test_exit_free_window_floor_or_disabled_even_without_grid(monkeypatch,hour,soc,enabled):
    state, discharge, release=setup_guard(monkeypatch,hour=hour,state={'active':True},enabled=enabled)
    run_guard(grid=None,battery=None,soc=soc)
    release.assert_awaited_once()
    discharge.assert_not_awaited()
    assert not state['active']


def test_missing_telemetry_resets_consecutive_trigger(monkeypatch):
    state, discharge, _=setup_guard(monkeypatch,state={'high_import_checks':1})
    run_guard(battery=None)
    run_guard()
    discharge.assert_not_awaited()
    assert state['high_import_checks']==1


def test_export_control_does_not_cancel_import_guard(monkeypatch):
    setup_guard(monkeypatch,hour=19,state={'active':True})
    live=AsyncMock()
    monkeypatch.setattr(main,'_saj_control_states',live)
    result=asyncio.run(main._apply_saj_export_window_discharge_control(
        solplanet_monitor=None,solplanet_control=None,
        now_local=datetime(2026,9,13,19,0,tzinfo=ZoneInfo('Australia/Sydney')),combined_status={},
    ))
    assert result['action']['reason']=='paid_grid_import_guard_has_priority'
    live.assert_not_awaited()


def test_cached_samples_cannot_trigger_or_retry_discharge(monkeypatch):
    state, discharge, _ = setup_guard(monkeypatch,state={'high_import_checks':1})
    flow = {'updated_at':(datetime.now(UTC)-timedelta(minutes=10)).isoformat(),
            'metrics':{'grid_w':3000,'battery1_w':0,'battery1_soc_percent':80}}
    result=asyncio.run(main._run_saj_morning_grid_import_guard_check(flow))
    assert result['decision_reason']=='stale_metrics'
    assert state['high_import_checks']==0
    discharge.assert_not_awaited()
    flow['updated_at']=datetime.now(UTC).isoformat()
    asyncio.run(main._run_saj_morning_grid_import_guard_check(flow))
    result=asyncio.run(main._run_saj_morning_grid_import_guard_check(flow))
    assert result['decision_reason']=='waiting_for_new_sample'
    discharge.assert_not_awaited()
