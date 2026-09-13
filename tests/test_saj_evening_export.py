from datetime import datetime
from dataclasses import replace
from zoneinfo import ZoneInfo

import pytest

from app import main
from test_paid_grid_import_guard import setup_guard
import asyncio
from datetime import UTC, timedelta


def metrics(**overrides):
    return {'battery1_soc_percent':100, 'battery2_soc_percent':90,
            'battery1_w':0,'grid_w':0,'load_w':1000, **overrides}


def local(hour,minute=0):
    return datetime(2026,9,14,hour,minute,tzinfo=ZoneInfo('Australia/Sydney'))


@pytest.mark.parametrize('hour,active',[(16,False),(17,True),(18,True),(19,True),(20,False),(22,False),(23,False),(0,False)])
def test_evening_window(hour,active):
    assert main._saj_evening_export_plan(metrics(),local(hour))['window_active'] is active


def test_full_saj_gets_priority_and_can_finish_before_eleven(monkeypatch):
    monkeypatch.setattr(main,'settings',replace(main.settings,solplanet_export_reserve_floor_kwh=18))
    plan=main._saj_evening_export_plan(metrics(),local(17))
    assert plan['target_power_w']==5000
    assert plan['saj_usable_kwh']==12
    assert plan['deadline_feasible_at_target']
    assert plan['target_soc_percent']==20


@pytest.mark.parametrize('overrides',[
    {'battery1_soc_percent':20}, {'battery2_soc_percent':None},
    {'battery1_soc_percent':30,'battery2_soc_percent':30,'load_w':4000},
])
def test_floor_unknown_state_or_night_reserve_prevents_sale(monkeypatch,overrides):
    monkeypatch.setattr(main,'settings',replace(main.settings,solplanet_export_reserve_floor_kwh=18))
    assert main._saj_evening_export_plan(metrics(**overrides),local(18))['target_power_w']==0


def monitor(battery_w,grid_import=0):
    return main._build_solplanet_export_monitor(metrics_map=metrics(battery1_w=battery_w),
        now_local=local(19),export_tracking={},current_grid_import_w=grid_import,current_grid_export_w=0)


def test_solplanet_does_not_force_sell_during_saj_priority():
    for power in (0, -2000, 3000):
        result = monitor(power)
        assert result['recommended_pout_w'] == 0
        assert result['recommendation'] == 'hold'
        assert result['reason'] == 'solplanet_home_support_during_saj_priority'
    assert monitor(3000, 2000)['recommended_pout_w'] == 0


def test_after_twenty_resumes_original_total_reserve_target():
    result = main._build_solplanet_export_monitor(metrics_map=metrics(battery1_w=3000),
        now_local=local(20),export_tracking={},current_grid_import_w=0,current_grid_export_w=0)
    assert result['recommendation'] in ('sell', 'sell_surplus')
    assert result['recommended_pout_w'] == result['total_recommended_pout_w']
    assert result['recommended_pout_w'] > 0
    assert not result['saj_evening_plan']['window_active']


def test_evening_sale_starts_without_import_and_uses_shared_controller(monkeypatch):
    state,discharge,release=setup_guard(monkeypatch,hour=17)
    result=asyncio.run(main._run_saj_morning_grid_import_guard_check({
        'updated_at':datetime.now(UTC).isoformat(),'metrics':metrics(),
    }))
    assert result['decision_reason']=='saj_first_before_23'
    assert state['active'] and state['evening_export_active']
    assert discharge.call_args.args[0].power_w>0
    release.assert_not_awaited()


def test_import_guard_keeps_evening_target_when_grid_balanced(monkeypatch):
    _,discharge,release=setup_guard(monkeypatch,hour=18,state={
        'active':True,'target_power_w':2000,
        'last_action_at_utc':(datetime.now(UTC)-timedelta(minutes=3)).isoformat(),
    })
    result=asyncio.run(main._run_saj_morning_grid_import_guard_check({
        'updated_at':datetime.now(UTC).isoformat(),'metrics':metrics(battery1_w=2000),
    }))
    assert result['desired_discharge_power_w']>=result['saj_evening_plan']['target_power_w']
    assert discharge.call_args.args[0].power_w>2000
    release.assert_not_awaited()


def test_twenty_removes_export_component_immediately_but_keeps_house_support(monkeypatch):
    _,discharge,_=setup_guard(monkeypatch,hour=20,state={
        'active':True,'evening_export_active':True,'target_power_w':5000,
        'last_action_at_utc':datetime.now(UTC).isoformat(),
    })
    result=asyncio.run(main._run_saj_morning_grid_import_guard_check({
        'updated_at':datetime.now(UTC).isoformat(),'metrics':metrics(battery1_w=5000,grid_w=-3000),
    }))
    assert result['saj_evening_plan']['target_power_w']==0
    assert discharge.call_args.args[0].power_w==1950


def test_priority_mode_supplies_home_without_zeroing_discharge_limit(monkeypatch):
    from unittest.mock import AsyncMock
    info=AsyncMock(return_value={'mod_r':4})
    write=AsyncMock(return_value={'readback_mode':'self_consumption'})
    schedule=AsyncMock()
    monkeypatch.setattr(main,'_solplanet_get_battery_info_live',info)
    monkeypatch.setattr(main,'_solplanet_set_work_mode_payload',write)
    monkeypatch.setattr(main,'_solplanet_get_schedule_live',schedule)
    state={}
    result=asyncio.run(main._apply_solplanet_export_monitor_control(
        monitor=monitor(0),now_local=local(18),combined_status=state))
    write.assert_awaited_once_with('self_consumption')
    schedule.assert_not_awaited()
    assert result['home_support_only']
    assert result['action']['type']=='apply'


def test_twenty_switches_solplanet_back_to_original_selling_mode(monkeypatch):
    from unittest.mock import AsyncMock
    schedule=AsyncMock(return_value={'Pout':0})
    write_mode=AsyncMock(return_value={'readback_mode':'custom'})
    write_schedule=AsyncMock(return_value={})
    monkeypatch.setattr(main,'_solplanet_get_schedule_live',schedule)
    monkeypatch.setattr(main,'_solplanet_set_work_mode_payload',write_mode)
    monkeypatch.setattr(main,'_solplanet_set_schedule_payload',write_schedule)
    state={'solplanet_export_monitor_control':{
        'home_support_only':True,'desired_pout_w':0,'checked_at_utc':datetime.now(UTC).isoformat(),
        'action':{'type':'noop','reason':'already_at_target'},
    }}
    result=asyncio.run(main._apply_solplanet_export_monitor_control(
        monitor={'recommendation':'sell','recommended_pout_w':4000,'reserve_state':'above_comfort'},
        now_local=local(20),combined_status=state))
    write_mode.assert_awaited_once_with('custom')
    assert write_schedule.call_args.args[0]['Pout']==4000
    assert result['desired_pout_w']==4000


def test_home_support_mode_readback_failure_is_not_success(monkeypatch):
    from unittest.mock import AsyncMock
    monkeypatch.setattr(main,'_solplanet_get_battery_info_live',AsyncMock(return_value={'mod_r':4}))
    monkeypatch.setattr(main,'_solplanet_set_work_mode_payload',AsyncMock(return_value={'readback_mode':'custom'}))
    result=asyncio.run(main._apply_solplanet_export_monitor_control(
        monitor=monitor(0),now_local=local(17),combined_status={}))
    assert result['action']['type']=='failed'
