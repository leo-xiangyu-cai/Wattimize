# Solplanet CGI Findings (2026-03-13)

This note records direct observations from the Solplanet inverter CGI endpoints on `192.168.68.70`.

## Scope

- Goal: change the current Friday 11:00 charging configuration from `10000W` to `8000W`
- Constraint: use direct inverter CGI HTTP requests only
- No FastAPI path was used for the actual write attempts

## Device

- Host: `192.168.68.70`
- Port: `443`
- Scheme: `https`

Dongle info observed from device:

- `key = 7EM3XNGFFHFGTWLC`
- `pdk = a251hFoHEfc`
- protocol reported: `V2.2`

## Read Endpoints That Worked

### `GET /getdefine.cgi`

Returned:

```json
{
  "Pin": 10000,
  "Pout": 10000,
  "Sun": [184595458, 302020611, 0, 0, 0, 0],
  "Mon": [184595458, 302020611, 0, 0, 0, 0],
  "Tus": [184595458, 302020611, 0, 0, 0, 0],
  "Wen": [184595458, 302020611, 0, 0, 0, 0],
  "Thu": [184595458, 302020611, 0, 0, 0, 0],
  "Fri": [184595458, 302020611, 0, 0, 0, 0],
  "Sat": [184595458, 302020611, 0, 0, 0, 0]
}
```

Observations:

- `Pin = 10000`
- `Pout = 10000`
- Friday has two non-zero schedule entries

The first non-zero value:

- `184595458`
- decoded as bytes: `11, 0, 180, 2`
- interpreted as:
  - start time `11:00`
  - duration-like value `180`
  - mode-like value `2`

The second non-zero value:

- `302020611`
- decoded as bytes: `18, 0, 120, 3`
- interpreted as:
  - start time `18:00`
  - duration-like value `120`
  - mode-like value `3`

This matches the user's statement closely:

- Friday charge starts at `11:00`
- duration `3 hours`
- current charge power is around `10kW`

### `GET /getchargeplan.cgi?date=5`

Returned:

```json
{"plan":[{"g":0,"p":100,"o":10,"v":0,"s":"00:00","e":"15:00"},{"g":0,"p":100,"o":50,"v":0,"s":"15:00","e":"24:00"}],"mode":2,"date":5}
```

Observations:

- There is a higher-level charge-plan API beyond `getdefine.cgi`
- For Friday (`date=5`), plan is returned in a business-like structure
- `mode = 2`
- Both plan entries currently have `p = 100`

### `GET /getchargestatus.cgi`

Returned:

```json
{"m1":1,"t2":1,"w3":1,"t4":1,"f5":1,"s6":1,"s7":1}
```

Observations:

- Looks like weekday enable flags
- Friday (`f5`) is enabled

### Realtime battery data

Observed from `getdevdata.cgi?device=4&sn=EQ00120258023107`:

- `pb = -9837`

Interpretation:

- Battery charging power was approximately `9.8kW`
- This is consistent with `Pin = 10000`

## Write Endpoint Findings

### `POST /setting.cgi`

This endpoint exists and responds, but the correct request body format was not identified.

Tried:

1. Form POST with only `Pin=8000`
2. Form POST with full `getdefine.cgi` payload
3. Form POST with full payload plus `key` and `pdk`
4. Form POST with weekday arrays serialized as comma-separated strings
5. JSON POST with full payload
6. Multipart POST with full payload
7. Plain text POST with raw JSON string
8. Form POST carrying `date/mode/plan` inferred from `getchargeplan.cgi`

Results:

- Some requests returned:

```text
error : setting_handler
```

- Some requests hung until timeout with no useful response
- After every attempt, `GET /getdefine.cgi` still returned `Pin = 10000`

Conclusion:

- `setting.cgi` is almost certainly the write path used by the device/app family
- The body format or required companion parameters are still wrong
- No successful write to `8000W` was achieved

## Endpoint Probing Summary

Worked:

- `GET /getdefine.cgi`
- `GET /getchargeplan.cgi?date=5`
- `GET /getchargestatus.cgi`
- `GET /getdev.cgi?device=0`
- `GET /getdev.cgi?device=2`
- `GET /getdev.cgi?device=3`
- `GET /getdev.cgi?device=4&sn=AL010K01Q25C0022`
- `GET /getdevdata.cgi?device=2&sn=AL010K01Q25C0022`
- `GET /getdevdata.cgi?device=3`
- `GET /getdevdata.cgi?device=4&sn=EQ00120258023107`

Did not exist / returned 404:

- `/setting.cgi?Pin=8000` as GET
- `/setchargeplan.cgi`
- `/setchargestatus.cgi`
- `/setsimplemode.cgi`
- `/setcustommode.cgi`
- `/setplan.cgi`
- `/savechargeplan.cgi`
- `/chargeplan.cgi`
- `/chargestatus.cgi`
- `/simplemode.cgi`

## Working Hypotheses

1. `Pin` is very likely the charge power limit relevant to the current `Custom mode`.
2. The first Friday slot in `getdefine.cgi` likely corresponds to the 11:00 charge window.
3. The `180` value inside the raw encoded slot is very likely duration in minutes.
4. `getchargeplan.cgi` exposes a higher-level representation of the same configuration.
5. The correct write shape may not be a simple partial update; it may require:
   - a different body schema
   - a signed/encrypted payload
   - a cookie/session established elsewhere
   - a different serialization of `plan`

## Most Useful Next Step

The fastest way to reverse the write format is likely:

1. Change the Friday charge power from `10000W` to `8000W` using the official app.
2. Immediately re-read:
   - `GET /getdefine.cgi`
   - `GET /getchargeplan.cgi?date=5`
   - `GET /getchargestatus.cgi`
3. Compare exactly which fields changed.

That will confirm whether:

- only `Pin` changes
- `plan[].p` changes
- `mode/date/status` changes
- or multiple fields must change together

## Bottom Line

As of this investigation:

- Read path is confirmed
- Current Friday 11:00 charge setup is visible through CGI
- Current charge power is still `10000W`
- No direct CGI write attempt succeeded in changing it to `8000W`
