import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest
from fastapi import HTTPException
from app import main


def setup(monkeypatch, *, hour=7, state=None, running=True):
    now = datetime(2026, 9, 16, hour, 0, tzinfo=ZoneInfo('Australia/Sydney'))
    state = dict(state or {})
    monkeypatch.setattr(main, '_tesla_grid_support_now_local', lambda: (now, 'Australia/Sydney'))
    monkeypatch.setattr(main, '_load_saj_morning_grid_import_guard_state', AsyncMock(return_value=state))
    save = AsyncMock()
    monkeypatch.setattr(main, '_store_saj_morning_grid_import_guard_state', save)
    monkeypatch.setattr(main, '_tesla_control_states', AsyncMock(return_value={}))
    monkeypatch.setattr(main, '_build_tesla_observation_payload', lambda _: {})
    monkeypatch.setattr(main, '_tesla_observation_charge_state', lambda _: {'enabled': running, 'power_w': 3000 if running else 0})
    monkeypatch.setattr(main, '_tesla_auto_control_location_block_reason', lambda _: None)
    return now, state, save


def flow(grid=1900, stale=False):
    return {'updated_at': (datetime.now(UTC) - timedelta(minutes=10 if stale else 0)).isoformat(),
            'metrics': {'grid_w': grid}}


@pytest.mark.parametrize('grid', [1, 1900])
def test_first_import_stops_even_without_saj_metrics(monkeypatch, grid):
    now, state, save = setup(monkeypatch)
    async def stop(enabled):
        assert enabled is False
        assert state['tesla_stop_until_11_date'] == '2026-09-16'
        save.assert_awaited_once()
        return {'ok': True}
    monkeypatch.setattr(main, '_tesla_set_charging', stop)
    result = asyncio.run(main._enforce_tesla_morning_import_guard(flow(grid), state, now))
    assert result['locked']


@pytest.mark.parametrize('hour,grid,stale', [(11,1900,False),(10,-200,False),(10,1900,True)])
def test_no_stop_outside_window_export_or_stale_sample(monkeypatch,hour,grid,stale):
    now,state,_ = setup(monkeypatch,hour=hour)
    stop = AsyncMock()
    monkeypatch.setattr(main,'_tesla_set_charging',stop)
    asyncio.run(main._enforce_tesla_morning_import_guard(flow(grid,stale),state,now))
    stop.assert_not_awaited()


def test_failed_stop_remains_locked_and_retries_without_fresh_metrics(monkeypatch):
    now,state,_=setup(monkeypatch)
    stop=AsyncMock(side_effect=RuntimeError('vehicle unavailable'))
    monkeypatch.setattr(main,'_tesla_set_charging',stop)
    with pytest.raises(RuntimeError):
        asyncio.run(main._enforce_tesla_morning_import_guard(flow(),state,now))
    assert asyncio.run(main._tesla_morning_import_locked())
    stop.side_effect=None
    asyncio.run(main._enforce_tesla_morning_import_guard(None,state,now))
    assert stop.await_count == 2


@pytest.mark.parametrize('method', ['_tesla_set_charging','_tesla_restart_charging'])
def test_start_and_restart_cannot_bypass_persisted_lock(monkeypatch,method):
    setup(monkeypatch,state={'tesla_stop_until_11_date':'2026-09-16'})
    with pytest.raises(HTTPException) as exc:
        asyncio.run(getattr(main,method)(True) if method=='_tesla_set_charging' else getattr(main,method)())
    assert exc.value.status_code==409


@pytest.mark.parametrize('hour,date,expected', [(10,'2026-09-16',True),(11,'2026-09-16',False),(10,'2026-09-15',False)])
def test_lock_expires_at_eleven_and_does_not_leak_to_next_day(monkeypatch,hour,date,expected):
    setup(monkeypatch,hour=hour,state={'tesla_stop_until_11_date':date})
    assert asyncio.run(main._tesla_morning_import_locked()) is expected


@pytest.mark.parametrize('grid,tesla,expected', [(1900,3000,0),(-3,0,0),(-4000,0,3800),(-300,3000,3100)])
def test_actual_power_not_battery_soc_funds_charging(grid,tesla,expected):
    sample=flow(grid)
    sample['metrics'].update(tesla_charge_power_w=tesla,battery1_soc_percent=80,battery1_w=-2000)
    assert main._tesla_morning_measured_power_budget(sample)==expected


def test_stale_or_missing_grid_fails_closed():
    sample=flow(-4000,True)
    sample['metrics']['tesla_charge_power_w']=0
    assert main._tesla_morning_measured_power_budget(sample)==0
    assert main._tesla_morning_measured_power_budget(None)==0
