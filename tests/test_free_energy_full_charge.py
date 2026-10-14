import asyncio
from datetime import datetime
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest
from app import main


def local(hour=11):
    return datetime(2026, 9, 18, hour, 15, tzinfo=ZoneInfo('Australia/Sydney'))


def saj_setup(monkeypatch, configured=False):
    monkeypatch.setitem(main.collector_status, 'combined', {})
    state = {
        'charge': {'control_switch': configured, 'actual_time_enable_mask': int(configured),
                   'effective_slots': [{'start_time':'11:00','end_time':'15:00','power_percent':100,'day_mask':127}]},
        'discharge': {'control_switch':False,'actual_time_enable_mask':0},
        'battery': {'power_w':-2000},
    }
    sensors = {key:{'state': '100' if configured else '40'} for key in
               ('sensor.saj_battery_charge_power_limit','sensor.saj_grid_charge_power_limit')}
    monkeypatch.setattr(main, '_saj_control_states', AsyncMock(return_value=([], sensors)))
    monkeypatch.setattr(main, '_build_saj_control_state', lambda _:state)
    monkeypatch.setattr(main, '_build_saj_profile_state', lambda _: {
        'input_profile':'time_of_use' if configured else 'self_consumption',
        'actual_profile':'time_of_use' if configured else 'self_consumption'})
    writer=AsyncMock(return_value={'ok':True})
    monkeypatch.setattr(main, '_put_saj_charge_power', writer)
    return writer, sensors


def test_pv_charging_does_not_skip_saj_full_power(monkeypatch):
    writer,_=saj_setup(monkeypatch)
    result=asyncio.run(main._guard_saj_profile_for_time(local()))
    assert result['action']=='applied'
    assert writer.call_args.args[0].power_percent==100
    assert writer.call_args.kwargs['charge_window']==('11:00','15:00')
    assert writer.call_args.kwargs['restore_charge_limits'] is True


def test_saj_effective_full_power_does_not_rewrite(monkeypatch):
    writer,_=saj_setup(monkeypatch,True)
    asyncio.run(main._guard_saj_profile_for_time(local()))
    writer.assert_not_awaited()


def test_saj_actual_limit_drift_is_repaired(monkeypatch):
    writer,sensors=saj_setup(monkeypatch,True)
    sensors['sensor.saj_grid_charge_power_limit']['state']='40'
    asyncio.run(main._guard_saj_profile_for_time(local()))
    writer.assert_awaited_once()


def test_disabled_saj_rule_does_not_write(monkeypatch):
    writer,_=saj_setup(monkeypatch)
    asyncio.run(main._guard_saj_profile_for_time(local(),{'saj_profile_free_energy':False}))
    writer.assert_not_awaited()


@pytest.mark.parametrize('configured', [False,True])
def test_solplanet_checks_schedule_even_when_pv_charging(monkeypatch,configured):
    slot=main._solplanet_encode_slot(hour=11,minute=0,duration_minutes=240,mode=2)
    schedule={'Pin':10000 if configured else 2000,'Pout':0,'Fri':[slot if configured else 0,0,0,0,0,0]}
    monkeypatch.setattr(main,'_solplanet_get_schedule_live',AsyncMock(return_value=schedule))
    monkeypatch.setattr(main,'_solplanet_get_battery_info_live',AsyncMock(return_value={
        'mod_r': main.SOLPLANET_WORK_MODE_REGISTER_BY_ID[main.SOLPLANET_WORK_MODE_CUSTOM] if configured else -1}))
    mode=AsyncMock(return_value={});write=AsyncMock(return_value={})
    monkeypatch.setattr(main,'_solplanet_set_work_mode_payload',mode)
    monkeypatch.setattr(main,'_solplanet_set_schedule_payload',write)
    result=asyncio.run(main._apply_solplanet_free_energy_recovery(
        combined_flow={'metrics':{'battery2_w':-2800,'battery2_soc_percent':44}},
        now_local=local(),enabled_map={},combined_status={}))
    if configured:
        write.assert_not_awaited();mode.assert_not_awaited()
    else:
        assert result['action']['type']=='apply'
        assert write.call_args.args[0]['Pin']==10000
        assert write.call_args.args[0]['Fri'][0]==slot


@pytest.mark.parametrize('hour,soc', [(10,44),(15,44),(11,95)])
def test_solplanet_respects_window_and_target(monkeypatch,hour,soc):
    read=AsyncMock();monkeypatch.setattr(main,'_solplanet_get_schedule_live',read)
    asyncio.run(main._apply_solplanet_free_energy_recovery(
        combined_flow={'metrics':{'battery2_w':-2800,'battery2_soc_percent':soc}},
        now_local=local(hour),enabled_map={},combined_status={}))
    read.assert_not_awaited()


@pytest.mark.parametrize('payload,expected', [
    ({'skipped':'tesla_unplugged'},0),
    ({'skipped':'tesla_not_ready','tesla_status':'disconnected'},0),
    ({'skipped':'tesla_not_ready','tesla_status':'unavailable'},1),
    ({'skipped':'tesla_observation_unavailable'},1),
    ({'skipped':'tesla_unplugged','error':'timeout'},1),
    ({'skipped':'tesla_unplugged','solplanet_free_energy_recovery':{'error':'timeout'}},1),
])
def test_normal_tesla_skips_keep_real_errors(payload,expected):
    assert len(main._build_combined_auxiliary_issues({'last_midday_window_check':payload}))==expected


def test_charge_writer_bounds_slot_and_restores_limits(monkeypatch):
    monkeypatch.setattr(main, '_ensure_ha_configured', lambda:None)
    monkeypatch.setattr(main, '_saj_control_states', AsyncMock(return_value=([],{})))
    monkeypatch.setattr(main, '_start_operation_history_run', AsyncMock(return_value=1))
    monkeypatch.setattr(main, '_update_operation_history_run', AsyncMock())
    numbers=AsyncMock();texts=AsyncMock();switches=AsyncMock()
    monkeypatch.setattr(main, '_saj_set_number', numbers)
    monkeypatch.setattr(main, '_saj_set_text', texts)
    monkeypatch.setattr(main, '_saj_set_switch', switches)
    asyncio.run(main._put_saj_charge_power(main.SajManualChargePowerPayload(power_percent=100,settle_seconds=0),
        charge_window=('11:00','15:00'),restore_charge_limits=True))
    values={call.args[0]:call.args[1] for call in numbers.call_args_list}
    assert values['number.saj_battery_charge_power_limit_input']==1000
    assert values['number.saj_grid_max_charge_power_input']==1000
    assert values['number.saj_charge1_power_percent_input']==100
    assert values['number.saj_discharge_time_enable_input']==0
    assert ('text.saj_charge1_start_time_time','11:00') in [c.args for c in texts.call_args_list]
    assert ('text.saj_charge1_end_time_time','15:00') in [c.args for c in texts.call_args_list]


def test_manual_charge_api_keeps_original_body_shape():
    schema=main.app.openapi()
    body=schema['paths']['/api/saj/manual-charge-power']['put']['requestBody']['content']['application/json']['schema']
    assert body['$ref'].endswith('/SajManualChargePowerPayload')


def test_saj_waits_for_real_readback_without_rewriting_every_minute(monkeypatch):
    writer,_=saj_setup(monkeypatch)
    asyncio.run(main._guard_saj_profile_for_time(local()))
    result=asyncio.run(main._guard_saj_profile_for_time(local()))
    writer.assert_awaited_once()
    assert result['reason']=='awaiting_charge_readback'
