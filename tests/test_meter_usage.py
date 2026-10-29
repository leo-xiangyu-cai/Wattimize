import json
import sqlite3
from pathlib import Path

import pytest

from app.meter_usage import compute_meter_usage_actuals, parse_meter_usage_upload, upsert_meter_usage_upload_to_db
from app.persistence import init_db


def usage_for(day, timezone_label="AEST"):
    return {
        "streams": [{"kind": "import", "timezone": timezone_label}],
        "days": [{"date": day, "intervals": [
            {"time": "10:00", "duration_minutes": 5, "import_kwh": 1},
            {"time": "14:00", "duration_minutes": 5, "import_kwh": 2},
            {"time": "15:00", "duration_minutes": 5, "import_kwh": 3, "export_kwh": 4},
            {"time": "22:00", "duration_minutes": 5, "import_kwh": 5, "export_kwh": 6},
        ]}],
    }


def calculate(usage):
    plan = json.loads(Path("data/home-plan/electricity-plans.json").read_text())
    return compute_meter_usage_actuals(usage, plan_payload=plan, timezone_name="Australia/Sydney", source_label="test.csv")


@pytest.mark.parametrize("day,expected", [
    ("2026-10-03", (2, 5, 4, 6)),
    ("2026-10-04", (1, 3, 7, 4)),
    ("2026-10-25", (1, 3, 7, 4)),
])
def test_aest_tariff_boundaries_across_dst(day, expected):
    result = calculate(usage_for(day))[day]
    kwh = result["usage_kwh"]
    assert tuple(kwh[k] for k in ("free_import", "peak_import", "shoulder_import", "fit_evening_export")) == expected
    assert kwh["import_total"] == 11
    assert kwh["export_total"] == 10
    assert result["source_timezone"] == "AEST"
    assert result["day_basis"] == "meter_source_day"


def test_local_timestamp_export_is_not_shifted_twice():
    result = calculate(usage_for("2026-10-25", "Australia/Sydney"))["2026-10-25"]
    assert result["usage_kwh"]["free_import"] == 2
    assert result["usage_kwh"]["peak_import"] == 5


def test_unknown_timezone_rejected_before_costing():
    with pytest.raises(ValueError, match="Unsupported meter timezone"):
        calculate(usage_for("2026-10-25", "unknown"))


def test_source_day_retained_when_last_hour_is_next_local_day():
    usage = usage_for("2026-10-25")
    usage["days"][0]["intervals"] = [{"time": "23:55", "duration_minutes": 5, "import_kwh": 1}]
    result = calculate(usage)
    assert list(result) == ["2026-10-25"]
    assert result["2026-10-25"]["usage_kwh"]["shoulder_import"] == 1


def test_zero_intervals_survive_database_import(tmp_path):
    raw = b"Nmi,123,NETWORK,TEST\nStream ID,Meter,E1,E1,KWH,CONSUMPTION\nLOCAL TIME,AEST\nDate/Time,0:00,0:05,Quality,Total\n20261025,0,1,A,1\n"
    usage = parse_meter_usage_upload(raw, filename="test.csv")
    db = tmp_path / "usage.sqlite3"
    init_db(db)
    upsert_meter_usage_upload_to_db(db, usage=usage, raw=raw, source_filename="test.csv", source_csv="raw/test.csv")
    with sqlite3.connect(db) as conn:
        assert conn.execute("select kwh from meter_usage_intervals where kind='import' order by interval_start_time").fetchall() == [(0.0,), (1.0,)]
