"""Deterministic overnight reserve scenarios, persisted independently of manual settings.

All energies are usable kWh above the device SOC floors. No LLM or prompts.
Forecast versions are immutable; outcomes use observed load, not claimed savings.
"""
from __future__ import annotations

import json
import math
from datetime import UTC, date, datetime, time, timedelta
from statistics import median

from sqlalchemy import Column, String, select
from app.persistence import Base, _session_scope, get_series_samples

VERSION = 'reserve-v1'


class ReserveForecastRow(Base):
    __tablename__ = 'reserve_forecasts'
    key = Column(String, primary_key=True)
    payload = Column(String, nullable=False)


def read_records(db):
    with _session_scope(db) as session:
        return {r.key: json.loads(r.payload) for r in session.scalars(select(ReserveForecastRow))}


def write_record(db, key, payload, *, immutable=False):
    with _session_scope(db) as session:
        row = session.get(ReserveForecastRow, key)
        if row is None:
            session.add(ReserveForecastRow(key=key, payload=json.dumps(payload, allow_nan=False)))
        elif not immutable:
            row.payload = json.dumps(payload, allow_nan=False)


def quantile(values, q):
    values = sorted(values)
    if not values:
        return 0.0
    position = (len(values) - 1) * q
    lo = int(position)
    return values[lo] + (values[min(lo + 1, len(values) - 1)] - values[lo]) * (position - lo)


def period(day, tz):
    return (datetime.combine(day, time(23), tzinfo=tz),
            datetime.combine(day + timedelta(days=1), time(11), tzinfo=tz))


def finite(value):
    return isinstance(value, (float, int)) and math.isfinite(value)


def profile(rows, start, end):
    """Integrate 30 minute bins in absolute time. Reject holes, never bridge outages."""
    begin, finish = start.timestamp(), end.timestamp()
    n = math.ceil((finish - begin) / 1800)
    bins = [dict(load=0., pv=0., ev=0., battery=0., grid=0., seconds=0.) for _ in range(n)]
    for a, b in zip(rows, rows[1:]):
        left = max(begin, a['sampled_at_epoch'])
        right = min(finish, b['sampled_at_epoch'], a['sampled_at_epoch'] + 150)
        if not all(finite(a.get(k)) for k in ('load_w', 'pv_w', 'battery_w', 'grid_w')):
            continue
        while left < right:
            i = int((left - begin) // 1800)
            stop = min(right, begin + (i + 1) * 1800)
            seconds = stop - left
            target = bins[i]
            target['seconds'] += seconds
            for name, source in [('load', 'load_w'), ('pv', 'pv_w'), ('ev', 'tesla_charge_power_w'),
                                 ('battery', 'battery_w'), ('grid', 'grid_w')]:
                value = a.get(source)
                value = value if finite(value) else 0.
                if name in ('load', 'pv', 'ev'):
                    value = max(0., value)
                target[name] += value * seconds / 3600000
            left = stop
    coverage = sum(b['seconds'] for b in bins) / (finish - begin)
    # DST transition nights do not train the ordinary 12 hour shape.
    valid = coverage >= .97 and all(b['seconds'] >= 1620 for b in bins)
    return dict(bins=bins, coverage=round(coverage, 4), valid=valid,
                load_kwh=round(sum(b['load'] for b in bins), 3),
                pv_kwh=round(sum(b['pv'] for b in bins), 3))


def required_energy(load, pv):
    accumulated = peak = 0.
    for demand, solar in zip(load, pv):
        net = demand - solar
        accumulated += net / .90 if net >= 0 else net * .90
        peak = max(peak, accumulated)
    return max(0., peak)


def read_night(db, day, tz):
    start, end = period(day, tz)
    rows = get_series_samples(db, system='combined', hours=24, max_points=5000,
                              start_at_utc=start.astimezone(UTC), end_at_utc=end.astimezone(UTC))['items']
    result = profile(rows, start, end)
    result['day'] = day.isoformat()
    return result


def training_history(db, now):
    last = now.date() - timedelta(days=1 if now.hour >= 11 else 2)
    nights = [read_night(db, last - timedelta(days=i), now.tzinfo) for i in range(28)]
    return [n for n in nights if n['valid'] and len(n['bins']) == 24]


def clear_radiation(at, latitude, longitude):
    """Simple clear-sky reference, only for conservative cloud attenuation."""
    utc = at.astimezone(UTC)
    angle = 2 * math.pi * (utc.timetuple().tm_yday - 1) / 365
    eq = 229.18 * (.000075 + .001868 * math.cos(angle) - .032077 * math.sin(angle)
                  - .014615 * math.cos(2 * angle) - .040849 * math.sin(2 * angle))
    decl = .006918 - .399912 * math.cos(angle) + .070257 * math.sin(angle) - .006758 * math.cos(2 * angle) + .000907 * math.sin(2 * angle)
    solar_minutes = utc.hour * 60 + utc.minute + eq + 4 * longitude
    hour_angle = math.radians(solar_minutes / 4 - 180)
    lat = math.radians(latitude)
    elevation = math.sin(lat) * math.sin(decl) + math.cos(lat) * math.cos(decl) * math.cos(hour_angle)
    return max(0., 1000 * elevation)


def predict(now, history, weather, *, ev_kwh=0., ev_known=True, manual_kwh=15., recent_load_w=0.):
    start, end = period(now.date(), now.tzinfo)
    result = dict(model=VERSION, generated_at=now.astimezone(UTC).isoformat(), night=now.date().isoformat(),
                  start=start.isoformat(), end=end.isoformat(), training_nights=len(history),
                  manual_kwh=manual_kwh, ev_kwh=round(ev_kwh, 2), forecast=weather,
                  status='unavailable', reason='insufficient_history', recommended_kwh=None)
    if (end.timestamp() - start.timestamp()) != 43200:
        result['reason'] = 'dst_transition'
        return result
    if len(history) < 7:
        return result
    if not ev_known:
        result['reason'] = 'telemetry_unavailable'
        return result
    hours = {h['time']: h for h in weather.get('hours', [])}
    pv = []
    for i in range(24):
        at = datetime.fromtimestamp(start.timestamp() + i * 1800, now.tzinfo)
        if at.hour < 6 or at.hour >= 23:
            pv.append(0.)
            continue
        # Open-Meteo shortwave radiation is the PRECEDING hour mean.
        ending = at.replace(minute=0, second=0) + timedelta(hours=1)
        h = hours.get(ending.strftime('%Y-%m-%dT%H:%M'))
        if h is None or not finite(h.get('solar_w_m2')):
            result['reason'] = 'weather_unavailable'
            return result
        clear = clear_radiation(ending - timedelta(minutes=30), weather['latitude'], weather['longitude'])
        attenuation = min(1., max(0., h['solar_w_m2']) / max(clear, 80.))
        envelope = quantile([n['bins'][i]['pv'] for n in history], .90)
        pv.append(envelope * attenuation * .65)
    scenarios = []
    for night in history:
        # Subtract measured historical EV; add tonight's mandatory EV once.
        load = [max(0., b['load'] - b['ev']) for b in night['bins']]
        left = ev_kwh
        for i in range(24):
            extra = min(left, 1.5)  # 3 kW charging, front-loaded to avoid optimistic timing.
            load[i] += extra
            left -= extra
        if left > 0:
            load[-1] += left
        scenarios.append((required_energy(load, pv), sum(load)))
    # Keep the longer-history upper tail while also responding to recent changes.
    needed = max(quantile([s[0] for s in scenarios], .85), quantile([s[0] for s in scenarios[:7]], .90))
    # A sustained live evening load raises tomorrow's uncertainty, never lowers it.
    baseline_w = median([sum(max(0, b['load'] - b['ev']) for b in n['bins'][:14]) / 7 * 1000 for n in history])
    change_buffer = min(4., max(0., recent_load_w - baseline_w) / 1000 * 2)
    recommended = math.ceil((needed + 2. + change_buffer) * 2) / 2
    result.update(status='ready' if recommended <= 57 else 'capacity_exceeded',
                  reason='historical_scenarios', recommended_kwh=round(recommended, 1),
                  predicted_load_kwh=round(median([s[1] for s in scenarios]), 2),
                  predicted_pv_kwh=round(sum(pv), 2), expected_need_kwh=round(median([s[0] for s in scenarios]), 2),
                  cautious_need_kwh=round(needed, 2), buffer_kwh=round(2. + change_buffer, 2),
                  training_dates=[n['day'] for n in history], pv_bins_kwh=pv)
    return result


def effective(manual, enabled, prediction, now):
    if not enabled:
        return float(manual), 'manual'
    try:
        fresh = 0 <= (now.astimezone(UTC) - datetime.fromisoformat(prediction['generated_at'])).total_seconds() <= 5400
        correct_day = prediction['night'] == now.date().isoformat()
        if fresh and correct_day and prediction['status'] == 'ready' and finite(prediction.get('recommended_kwh')):
            return float(prediction['recommended_kwh']), 'automatic'
    except (KeyError, ValueError, TypeError):
        pass
    # Preserve an elevated same-night recommendation through transient forecast outages.
    previous = prediction.get('recommended_kwh') if prediction.get('night') == now.date().isoformat() else None
    return max(float(manual), float(previous) if finite(previous) else 0.), 'fallback'


def evaluate(prediction, actual):
    if not actual['valid']:
        return dict(status='incomplete', coverage=actual['coverage'])
    bins = actual['bins']
    need = required_energy([b['load'] for b in bins], [b['pv'] for b in bins])
    return dict(status='complete', coverage=actual['coverage'], actual_need_kwh=round(need, 2),
                actual_load_kwh=actual['load_kwh'], actual_pv_kwh=actual['pv_kwh'],
                error_kwh=round(prediction['expected_need_kwh'] - need, 2),
                margin_kwh=round(prediction['recommended_kwh'] - need, 2),
                covered=prediction['recommended_kwh'] >= need)


def history_view(records):
    versions = [v for k, v in records.items() if k.startswith('prediction:')]
    latest = {}
    for p in sorted(versions, key=lambda p: p['generated_at']):
        # Compare the last usable pre-23:00 prediction, once per night (not per revision).
        if p['status'] == 'ready' and datetime.fromisoformat(p['generated_at']) < datetime.fromisoformat(p['start']):
            latest[p['night']] = p
    items = []
    for night, p in sorted(latest.items(), reverse=True):
        actual = records.get('actual:' + night)
        score = evaluate(p, actual) if actual else {'status': 'pending'}
        items.append({k: v for k, v in p.items() if k not in ('forecast', 'pv_bins_kwh', 'training_dates')} | {'outcome': score})
    completed = [x['outcome'] for x in items if x['outcome']['status'] == 'complete']
    stats = dict(evaluated_nights=len(completed),
                 mae_kwh=round(sum(abs(s['error_kwh']) for s in completed) / len(completed), 2) if completed else None,
                 coverage_percent=round(100 * sum(s['covered'] for s in completed) / len(completed), 1) if completed else None)
    return dict(items=items[:30], stats=stats, version_count=len(versions))
