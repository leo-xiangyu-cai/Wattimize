from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dataclasses import replace
import asyncio
import pytest
from app import reserve_forecast as rf

TZ = ZoneInfo('Australia/Sydney')
NOW = datetime(2026, 10, 14, 18, tzinfo=TZ)


def histories(load=.3, pv=1.):
    return [dict(day=f'2026-10-{13-i:02}', valid=True, bins=[dict(load=load, ev=0, pv=pv if j>=14 else 0) for j in range(24)]) for i in range(10)]


def weather(radiation=500):
    return dict(latitude=-33.86, longitude=151.2, hours=[dict(time=f'2026-10-15T{h:02}:00', solar_w_m2=radiation) for h in range(7,12)])


def test_maximum_deficit_not_end_of_morning_balance():
    assert rf.required_energy([2,2,0,0], [0,0,3,3]) == pytest.approx(4/.9)
    assert rf.required_energy([2,2,0,0], [3,3,0,0]) < 4/.9


def test_clouds_and_urgent_car_increase_reserve():
    sunny=rf.predict(NOW,histories(),weather())
    dark=rf.predict(NOW,histories(),weather(0))
    urgent=rf.predict(NOW,histories(),weather(),ev_kwh=8)
    assert sunny['status']=='ready'
    assert dark['recommended_kwh'] >= sunny['recommended_kwh']
    assert urgent['recommended_kwh'] > sunny['recommended_kwh']+5
    assert rf.predict(NOW,histories(),weather(),ev_known=False)['status']=='unavailable'


def test_holiday_does_not_erase_occupied_load_tail():
    nights=histories(.1)+histories(.7)
    mixed=rf.predict(NOW,nights,weather())
    away=rf.predict(NOW,histories(.1),weather())
    assert mixed['recommended_kwh']>away['recommended_kwh']+5


def test_missing_weather_insufficient_history_and_capacity():
    assert rf.predict(NOW,histories()[:3],weather())['reason']=='insufficient_history'
    assert rf.predict(NOW,histories(),{})['reason']=='weather_unavailable'
    assert rf.predict(NOW,histories(3),weather(0))['status']=='capacity_exceeded'


def test_manual_auto_stale_and_cross_day_fallback():
    p=rf.predict(NOW,histories(),weather())
    assert rf.effective(15,False,p,NOW)==(15,'manual')
    assert rf.effective(15,True,p,NOW)==(p['recommended_kwh'],'automatic')
    p['recommended_kwh']=20
    assert rf.effective(15,True,p,NOW+timedelta(hours=2))==(20,'fallback')
    assert rf.effective(15,True,p,NOW+timedelta(days=1))==(15,'fallback')


def test_missing_intervals_do_not_count_as_zero_consumption():
    start,end=rf.period(NOW.date(),TZ)
    rows=[dict(sampled_at_epoch=start.timestamp()+i*60,load_w=1000,pv_w=0,battery_w=1000,grid_w=0) for i in range(721)]
    complete=rf.profile(rows,start,end)
    assert complete['valid']
    assert complete['load_kwh']==12
    incomplete=rf.profile(rows[:100]+rows[300:],start,end)
    assert not incomplete['valid']
    assert rf.evaluate({},incomplete)['status']=='incomplete'


def test_dst_window_uses_actual_elapsed_time():
    start,end=rf.period(datetime(2026,10,3).date(),TZ)
    assert (end.timestamp()-start.timestamp())/3600==11


def test_immutable_versions_and_one_outcome_per_night(tmp_path):
    from app.persistence import init_db
    db=tmp_path/'test.sqlite3';init_db(db)
    p=rf.predict(NOW,histories(),weather())
    rf.write_record(db,'prediction:1',p,immutable=True)
    rf.write_record(db,'prediction:1',dict(p,recommended_kwh=99),immutable=True)
    p2=dict(p,generated_at=(NOW+timedelta(hours=1)).isoformat())
    rf.write_record(db,'prediction:2',p2,immutable=True)
    actual=dict(valid=True,coverage=1.,load_kwh=7.2,pv_kwh=10,bins=histories()[0]['bins'])
    rf.write_record(db,'actual:'+p['night'],actual)
    records=rf.read_records(db)
    assert records['prediction:1']['recommended_kwh']!=99
    view=rf.history_view(records)
    assert view['stats']['evaluated_nights']==1
    assert view['version_count']==2
    assert len(view['items'])==1


def test_control_persists_mode_without_replacing_manual_with_prediction(tmp_path,monkeypatch):
    from app import main
    from app.persistence import init_db
    db=tmp_path/'test.sqlite3';init_db(db)
    monkeypatch.setattr(main,'storage_db_path',db)
    monkeypatch.setattr(main,'_reserve_records',{})
    monkeypatch.setattr(main,'_reserve_prediction',rf.predict(NOW,histories(),weather()))
    monkeypatch.setattr(main,'_tesla_grid_support_now_local',lambda:(NOW,'Australia/Sydney'))
    original=main.settings
    monkeypatch.setattr(main,'settings',original)
    monkeypatch.setattr(main,'save_settings',lambda payload:replace(original,solplanet_export_reserve_floor_kwh=payload['solplanet_export_reserve_floor_kwh']))
    auto=asyncio.run(main.put_reserve_forecast(main.ReserveControlPayload(automatic=True,manual_kwh=15)))
    assert auto['source']=='automatic' and auto['manual_kwh']==15
    manual=asyncio.run(main.put_reserve_forecast(main.ReserveControlPayload(automatic=False,manual_kwh=16)))
    assert manual['effective_kwh']==16 and manual['source']=='manual'
    assert rf.read_records(db)['settings']['automatic'] is False
