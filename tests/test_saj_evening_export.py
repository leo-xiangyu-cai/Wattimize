import asyncio
from datetime import UTC, datetime, timedelta
from dataclasses import replace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest
from app import main
from test_paid_grid_import_guard import setup_guard


def metrics(**overrides):
    return {'battery1_soc_percent':100, 'battery2_soc_percent':90,
            'battery1_w':0, 'battery2_w':0, 'grid_w':0, 'load_w':1000, **overrides}


def local(hour,minute=0):
    return datetime(2026,9,14,hour,minute,tzinfo=ZoneInfo('Australia/Sydney'))


@pytest.mark.parametrize('hour,active',[(16,False),(17,True),(18,True),(19,True),(20,True),(22,True),(23,False),(0,False)])
def test_evening_window(hour,active):
    assert main._saj_evening_export_plan(metrics(),local(hour))['window_active'] is active


def test_full_saj_gets_priority_and_can_finish_before_eleven(monkeypatch):
    monkeypatch.setattr(main,'settings',replace(main.settings,solplanet_export_reserve_floor_kwh=18))
    plan=main._saj_evening_export_plan(metrics(),local(17))
    assert plan['target_power_w']==5000
    assert plan['saj_usable_kwh']==12
    assert plan['deadline_feasible_at_target']
    assert plan['target_soc_percent']==20


@pytest.mark.parametrize('overrides', [
    {'battery2_soc_percent':None},
    {'battery1_soc_percent':30,'battery2_soc_percent':30,'load_w':4000},
])
def test_combined_reserve_does_not_block_spending_saj_first(overrides):
    assert main._saj_evening_export_plan(metrics(**overrides),local(18))['target_power_w']==5000


@pytest.mark.parametrize('soc,priority',[(20,False),(None,True)])
def test_saj_floor_handover_and_unknown_soc_no_forced_discharge(soc,priority):
    plan=main._saj_evening_export_plan(metrics(battery1_soc_percent=soc),local(18))
    assert plan['target_power_w']==0
    assert plan['priority_active'] is priority


def monitor(hour=19,**overrides):
    m=metrics(**overrides)
    return main._build_solplanet_export_monitor(metrics_map=m,now_local=local(hour),
        export_tracking={},current_grid_import_w=max(m['grid_w'] or 0,0),current_grid_export_w=max(-(m['grid_w'] or 0),0))


@pytest.mark.parametrize('power', [0,-2000,3000])
def test_no_speculative_solplanet_sale_while_saj_has_energy(power):
    result=monitor(battery1_w=power)
    assert result['recommended_pout_w']==0
    assert result['recommendation']=='saj_priority'
    assert result['reason']=='preserve_solplanet_until_saj_floor'


def test_solplanet_only_covers_measured_residual_and_yields_to_saj():
    assert monitor(battery2_w=1000,grid_w=500)['recommended_pout_w']==1500
    assert monitor(battery2_w=1000,grid_w=-1500)['recommended_pout_w']==0


def test_twenty_does_not_handover_until_saj_floor():
    assert monitor(hour=20)['recommendation']=='saj_priority'
    result=monitor(hour=20,battery1_soc_percent=20)
    assert not result['saj_evening_plan']['priority_active']
    assert result['recommendation']!='saj_priority'


def test_guard_starts_at_five_without_grid_import(monkeypatch):
    state,discharge,release=setup_guard(monkeypatch,hour=17)
    result=asyncio.run(main._run_saj_morning_grid_import_guard_check({
        'updated_at':datetime.now(UTC).isoformat(),'metrics':metrics(),
    }))
    assert result['decision_reason']=='saj_first_before_23'
    assert state['active'] and state['evening_export_active']
    assert discharge.call_args.args[0].power_w==5000
    release.assert_not_awaited()


def test_failed_actual_delivery_retries_even_without_import(monkeypatch):
    _,discharge,release=setup_guard(monkeypatch,hour=20,state={
        'active':True,'target_power_w':5000,'evening_export_active':True,
        'last_action_at_utc':(datetime.now(UTC)-timedelta(seconds=65)).isoformat(),
    })
    result=asyncio.run(main._run_saj_morning_grid_import_guard_check({
        'updated_at':datetime.now(UTC).isoformat(),'metrics':metrics(battery1_w=-2000),
    }))
    assert result['saj_delivery']['confirmed'] is False
    assert discharge.call_args.args[0].power_w==5000
    release.assert_not_awaited()


def setup_solplanet(monkeypatch, *, mismatch=False, mode='custom'):
    schedule={'Pout':4000}
    async def write(payload, **kwargs):
        if not mismatch:
            schedule.update(payload)
        return {}
    getter=AsyncMock(side_effect=lambda:dict(schedule))
    writer=AsyncMock(side_effect=write)
    mode_writer=AsyncMock(return_value={'readback_mode':mode})
    monkeypatch.setattr(main,'_solplanet_get_schedule_live',getter)
    monkeypatch.setattr(main,'_solplanet_set_schedule_payload',writer)
    monkeypatch.setattr(main,'_solplanet_get_battery_info_live',AsyncMock(return_value={'mod_r':2}))
    monkeypatch.setattr(main,'_solplanet_set_work_mode_payload',mode_writer)
    return getter,writer,mode_writer


def test_priority_enforces_zero_power_active_slot_instead_of_self_consumption(monkeypatch):
    _,writer,mode_writer=setup_solplanet(monkeypatch)
    result=asyncio.run(main._apply_solplanet_export_monitor_control(
        monitor=monitor(),now_local=local(19),combined_status={}))
    mode_writer.assert_awaited_once_with('custom')
    assert writer.call_args.args[0]['Pout']==0
    slots=writer.call_args.args[0]['Mon']
    assert any(main._solplanet_decode_slot(s)['mode']==3 for s in slots)
    assert result['home_support_only']
    assert result['action']['type']=='apply'


@pytest.mark.parametrize('schedule_mismatch,mode',[(True,'custom'),(False,'self_consumption')])
def test_readback_mismatch_is_failure(monkeypatch,schedule_mismatch,mode):
    setup_solplanet(monkeypatch,mismatch=schedule_mismatch,mode=mode)
    result=asyncio.run(main._apply_solplanet_export_monitor_control(
        monitor=monitor(),now_local=local(19),combined_status={}))
    assert result['action']['type']=='failed'


def test_saj_configuration_confirmation_uses_effective_sensors():
    state={'working_mode':{'mode_sensor':1},'charge':{'actual_time_enable_mask':0},
           'discharge':{'actual_time_enable_mask':1,'effective_slots':[
               {'start_time':'00:00','end_time':'23:59','day_mask':127,'power_percent':100}]},
           'battery':{'power_w':-2000}}
    state['limits']={'actual_battery_discharge_power_limit_percent':100,'actual_grid_discharge_power_limit_percent':100}
    result=main._saj_discharge_readback(state,100)
    assert result['configuration_confirmed']
    assert result['status']=='configuration_confirmed_await_power_sample'
    state['discharge']['effective_slots'][0]['power_percent']=0
    assert not main._saj_discharge_readback(state,100)['configuration_confirmed']


def test_missing_residual_telemetry_keeps_household_fallback(monkeypatch):
    _,writer,mode_writer=setup_solplanet(monkeypatch,mode='self_consumption')
    result=asyncio.run(main._apply_solplanet_export_monitor_control(
        monitor=monitor(battery2_w=None),now_local=local(19),combined_status={}))
    mode_writer.assert_awaited_once_with('self_consumption')
    writer.assert_not_awaited()
    assert result['verification']=='missing_residual_telemetry'


def test_other_export_controller_cannot_cancel_saj_priority_before_guard_starts(monkeypatch):
    live=AsyncMock()
    monkeypatch.setattr(main,'_saj_control_states',live)
    result=asyncio.run(main._apply_saj_export_window_discharge_control(
        solplanet_monitor=monitor(),solplanet_control=None,now_local=local(17),combined_status={}))
    assert result['action']['reason']=='saj_evening_priority_has_control'
    live.assert_not_awaited()


def test_saj_floor_releases_zero_custom_hold_for_home_supply(monkeypatch):
    _,writer,mode_writer=setup_solplanet(monkeypatch,mode='self_consumption')
    result=asyncio.run(main._apply_solplanet_export_monitor_control(
        monitor=monitor(battery1_soc_percent=20,battery2_soc_percent=30),now_local=local(19),
        combined_status={'solplanet_export_monitor_control':{'home_support_only':True,'desired_pout_w':0}}))
    mode_writer.assert_awaited_once_with('self_consumption')
    assert result['action']['reason']=='saj_floor_solplanet_home_support'
    writer.assert_not_awaited()


def test_effective_masks_use_nas_sensor_entities_not_input_values():
    states={key:{'state':value} for key,value in {
        'sensor.saj_charge_time_enable_bitmask':'0',
        'sensor.saj_discharge_time_enable_bitmask':'1',
        'number.saj_discharge_time_enable_input':'127',
    }.items()}
    state=main._build_saj_control_state(states)
    assert state['charge']['actual_time_enable_mask']==0
    assert state['discharge']['actual_time_enable_mask']==1
    assert state['discharge']['time_enable_mask']==127


def test_actual_half_power_limits_override_misleading_input_defaults(monkeypatch):
    write=AsyncMock()
    monkeypatch.setattr(main,'_saj_set_number',write)
    states={k:{'state':str(v)} for k,v in {
        'number.saj_battery_discharge_power_limit_input':1100,
        'number.saj_grid_max_discharge_power_input':1100,
        'sensor.saj_battery_discharge_power_limit':50,
        'sensor.saj_grid_discharge_power_limit':50,
    }.items()}
    changes=asyncio.run(main._saj_ensure_discharge_capacity(states))
    assert len(changes)==2
    assert [c.args[1] for c in write.call_args_list]==[1000,1000]
    states['sensor.saj_battery_discharge_power_limit']['state']='100'
    states['sensor.saj_grid_discharge_power_limit']['state']='100'
    write.reset_mock()
    assert asyncio.run(main._saj_ensure_discharge_capacity(states))==[]
    write.assert_not_awaited()


def test_floor_rebound_cannot_reactivate_saj_priority(monkeypatch):
    state,discharge,release=setup_guard(monkeypatch,hour=20,state={'active':True,'target_power_w':5000})
    state['depleted_charge_limits_saved']={'test':400}
    state['depleted_charge_limits_applied']=True
    def check(soc,power):
        return asyncio.run(main._run_saj_morning_grid_import_guard_check({
            'updated_at':datetime.now(UTC).isoformat(),'metrics':metrics(battery1_soc_percent=soc,battery1_w=power)}))
    check(20,10)
    assert discharge.call_args.args[0].power_w==0
    discharge.reset_mock()
    result=check(21,-8)
    assert result['decision_reason']=='saj_exhausted_solplanet_takes_over'
    assert not result['saj_evening_plan']['priority_active']
    discharge.assert_not_awaited()
    release.assert_not_awaited()


def test_already_rebounded_but_unable_to_discharge_hands_over(monkeypatch):
    _,discharge,_=setup_guard(monkeypatch,hour=20,state={'active':True,'target_power_w':5000,'depleted_charge_limits_saved':{'test':400},'depleted_charge_limits_applied':True})
    result=asyncio.run(main._run_saj_morning_grid_import_guard_check({
        'updated_at':datetime.now(UTC).isoformat(),'metrics':metrics(battery1_soc_percent=21,battery1_w=-9)}))
    assert result['decision_reason']=='saj_exhausted_solplanet_takes_over'
    assert discharge.call_args.args[0].power_w==0


def test_persisted_handover_releases_solplanet_after_rebound():
    result=main._build_solplanet_export_monitor(metrics_map=metrics(battery1_soc_percent=21),now_local=local(20),
        export_tracking={},current_grid_import_w=0,current_grid_export_w=0,saj_depleted=True)
    assert not result['saj_evening_plan']['priority_active']
    assert result['recommendation']!='saj_priority'


@pytest.mark.parametrize('hour,soc,load', [(20,79,1500), (21,79,1312), (22,70,4320)])
def test_depleted_saj_hands_surplus_to_solplanet_for_sale(monkeypatch,hour,soc,load):
    monkeypatch.setattr(main,'settings',replace(main.settings,solplanet_export_reserve_floor_kwh=18))
    result=main._build_solplanet_export_monitor(
        metrics_map=metrics(battery1_soc_percent=21,battery2_soc_percent=soc,load_w=load),
        now_local=local(hour),export_tracking={'total_export_kwh':7.2},
        current_grid_import_w=0,current_grid_export_w=0,saj_depleted=True)
    assert not result['saj_evening_plan']['priority_active']
    assert result['recommendation'] in ('sell','sell_surplus')
    assert result['recommended_pout_w'] > load
    assert result['target_sell_kwh'] > 0
    assert result['stop_floor_kwh']==18.5
    _,writer,mode_writer=setup_solplanet(monkeypatch)
    applied=asyncio.run(main._apply_solplanet_export_monitor_control(
        monitor=result,now_local=local(hour),combined_status={}))
    mode_writer.assert_awaited_once_with('custom')
    assert writer.call_args.args[0]['Pout'] > load
    assert applied['action']['type']=='apply'


@pytest.mark.parametrize('hour,soc', [(22,46),(23,79)])
def test_depleted_handover_still_respects_reserve_and_window(monkeypatch,hour,soc):
    monkeypatch.setattr(main,'settings',replace(main.settings,solplanet_export_reserve_floor_kwh=18))
    result=main._build_solplanet_export_monitor(
        metrics_map=metrics(battery1_soc_percent=21,battery2_soc_percent=soc),
        now_local=local(hour),export_tracking={},current_grid_import_w=0,
        current_grid_export_w=0,saj_depleted=True)
    assert result['recommended_pout_w']==0
    assert result['recommendation']=='stop'


def test_exhausted_charge_limits_preserved_and_restored_at_free_window(monkeypatch):
    state,_,_=setup_guard(monkeypatch,hour=20,state={'active':True,'target_power_w':5000})
    live={k:{'state':'40'} for k in ['sensor.saj_battery_charge_power_limit','sensor.saj_grid_charge_power_limit']}
    monkeypatch.setattr(main,'_saj_control_states',AsyncMock(return_value=([],live)))
    write=AsyncMock()
    monkeypatch.setattr(main,'_saj_set_number',write)
    asyncio.run(main._run_saj_morning_grid_import_guard_check({'updated_at':datetime.now(UTC).isoformat(),
        'metrics':metrics(battery1_soc_percent=20,battery1_w=0)}))
    assert [c.args[1] for c in write.call_args_list]==[0,0]
    assert set(state['depleted_charge_limits_saved'].values())=={400}
    monkeypatch.setattr(main,'_tesla_grid_support_now_local',lambda:(local(11),'Australia/Sydney'))
    write.reset_mock()
    asyncio.run(main._run_saj_morning_grid_import_guard_check(None))
    assert [c.args[1] for c in write.call_args_list]==[400,400]
    assert 'depleted_charge_limits_saved' not in state
