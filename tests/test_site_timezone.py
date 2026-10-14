from datetime import UTC, date, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from app import config, main


@pytest.mark.parametrize('configured', ['', 'invalid/zone'])
def test_missing_or_invalid_zone_falls_back_to_sydney(monkeypatch, configured):
    monkeypatch.setattr(main, 'settings', SimpleNamespace(local_timezone=configured))
    now, zone = main._tesla_grid_support_now_local()
    assert zone == 'Australia/Sydney'
    assert now.tzinfo.key == zone


def test_default_site_timezone_does_not_inherit_host_timezone(monkeypatch):
    monkeypatch.setenv('TZ', 'Asia/Shanghai')
    monkeypatch.delenv('WATTIMIZE_LOCAL_TIMEZONE', raising=False)
    assert config._env_values()['local_timezone'] == 'Australia/Sydney'
    assert config._build_settings({'local_timezone': ''}).local_timezone == 'Australia/Sydney'


@pytest.mark.parametrize('day,hours', [(date(2026,10,4),23), (date(2026,4,5),25), (date(2027,4,4),25)])
def test_daily_report_uses_actual_dst_day_length(monkeypatch, day, hours):
    monkeypatch.setattr(main, 'settings', SimpleNamespace(local_timezone='Australia/Sydney'))
    start, end, zone = main._daily_report_local_period(day)
    assert (end-start).total_seconds() == hours*3600
    assert zone == 'Australia/Sydney'


@pytest.mark.parametrize('day,hours', [(date(2026,10,3),5), (date(2026,4,4),7), (date(2026,7,4),6)])
def test_overnight_budget_uses_elapsed_hours(monkeypatch, day, hours):
    now = datetime(day.year,day.month,day.day,23,tzinfo=ZoneInfo('Australia/Sydney'))
    observed = []
    def usage(*args, **kwargs):
        observed.append(kwargs)
        return {'energy_kwh': {'home_load': 1}, 'samples': 2, 'coverage_ratio': 1}
    monkeypatch.setattr(main,'compute_usage_between',usage)
    ref = main._build_overnight_home_load_reference(now_local=now,timezone_name='Australia/Sydney',target_hour=5,lookback_days=2)
    assert ref['hours_from_now'] == hours
    assert all((x['end_at_utc']-x['start_at_utc']).total_seconds() == hours*3600 for x in observed)
    pump = main._build_pool_pump_night_schedule_reference(now_local=now,timezone_name='Australia/Sydney',usable_battery_total_kwh=20,has_current_battery_state=True,pool_pump_power_w=1000,lookback_days=1)
    assert pump['schedule']['duration_hours'] == hours
    assert pump['pool_pump_energy_kwh'] == hours


def test_frontend_uses_backend_site_timezone(monkeypatch):
    monkeypatch.setattr(main,'settings',SimpleNamespace(local_timezone='Australia/Sydney'))
    html = main._render_index_html()
    assert 'name="wattimize-timezone" content="Australia/Sydney"' in html
    assert html.index('/static/site-time.js') < html.index('/static/app.js')
