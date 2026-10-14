import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest
from app import main
from app.persistence import init_db, list_active_notification_entries


def now(hour=11, minute=0):
    return datetime(2026,9,18,hour,minute,tzinfo=ZoneInfo('Australia/Sydney'))


def observation(soc=79, home=True, cable=False, connection='unplugged'):
    return {'observation':{'battery':{'level_percent':soc},'location':{'at_home':home},
                           'charging':{'cable_connected':cable,'connection_state':connection}}}


@pytest.fixture
def reminder_db(tmp_path, monkeypatch):
    db=tmp_path/'reminder.sqlite3'
    init_db(db)
    monkeypatch.setattr(main,'storage_db_path',db)
    monkeypatch.setattr(main,'_persist_worker_control_log',AsyncMock(return_value=1))
    return db


def active(db):
    return list_active_notification_entries(db,system='notification',service='notification',status='send')


@pytest.mark.parametrize('time,obs', [
    (now(10,59),observation()),(now(15),observation()),
    (now(),observation(soc=80)),(now(),observation(soc=100)),(now(),observation(soc=None)),
    (now(),observation(soc=-1)),(now(),observation(home=False)),(now(),observation(home=None)),
    (now(),observation(cable=True,connection='plugged_not_charging')),
    (now(),observation(cable=None)),(now(),observation(connection='unknown')),
    (now(),{'error':'offline',**observation()}),(now(),None),
])
def test_only_known_home_low_battery_unplugged_in_free_window(reminder_db,time,obs):
    result=asyncio.run(main._check_tesla_free_energy_plug_reminder(obs,now_local=time))
    assert result['eligible'] is False
    assert active(reminder_db)==[]


def test_reminds_at_eleven_once_per_day_across_restart_and_dismissal(reminder_db):
    assert asyncio.run(main._check_tesla_free_energy_plug_reminder(observation(),now_local=now()))['status']=='reminded'
    entries=active(reminder_db)
    assert len(entries)==1 and entries[0]['notification']['level']=='info'
    asyncio.run(main.dismiss_dashboard_notification(main.NotificationDismissRequest(notification_key=entries[0]['notification_key'])))
    assert asyncio.run(main._check_tesla_free_energy_plug_reminder(observation(),now_local=now(14)))['status']=='already_reminded_today'
    assert active(reminder_db)==[]
    assert asyncio.run(main._check_tesla_free_energy_plug_reminder(observation(),now_local=now()+timedelta(days=1)))['status']=='reminded'
    assert len(active(reminder_db))==1


@pytest.mark.parametrize('time,obs',[(now(15),observation()),(now(),observation(cable=True)),(now(),observation(home=False)),(now(),observation(soc=80))])
def test_reminder_clears_when_conditions_end(reminder_db,time,obs):
    asyncio.run(main._check_tesla_free_energy_plug_reminder(observation(),now_local=now()))
    asyncio.run(main._check_tesla_free_energy_plug_reminder(obs,now_local=time))
    assert active(reminder_db)==[]


def test_failed_notification_save_can_retry(reminder_db,monkeypatch):
    writer=main.upsert_notification_entries
    monkeypatch.setattr(main,'upsert_notification_entries',lambda *a,**k:0)
    with pytest.raises(RuntimeError):
        asyncio.run(main._check_tesla_free_energy_plug_reminder(observation(),now_local=now()))
    monkeypatch.setattr(main,'upsert_notification_entries',writer)
    assert asyncio.run(main._check_tesla_free_energy_plug_reminder(observation(),now_local=now()))['status']=='reminded'


def test_next_day_clears_previous_day_without_extra_reminder(reminder_db):
    asyncio.run(main._check_tesla_free_energy_plug_reminder(observation(),now_local=now()))
    first_key=active(reminder_db)[0]['notification_key']
    asyncio.run(main._check_tesla_free_energy_plug_reminder(observation(),now_local=now()+timedelta(days=1)))
    entries=active(reminder_db)
    assert len(entries)==1
    assert entries[0]['notification_key'] != first_key
