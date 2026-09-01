from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any


BILLS_INDEX_FILENAME = "bills-index.json"
RAW_BILLS_DIRNAME = "raw"
TEXT_BILLS_DIRNAME = "text"
GLOBIRD_PLAN_NAME = "ZEROHERO"


@dataclass(frozen=True)
class BillLine:
    description: str
    category: str
    period_start: date
    period_end: date
    quantity: float
    unit: str
    rate_aud: float
    total_aud: float


def _clean_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _parse_money(value: object) -> float:
    text = str(value or "").strip().replace(",", "").replace("−", "-")
    text = text.replace("$-", "-").replace("$", "").replace(" ", "")
    if text in {"", "-"}:
        return 0.0
    return float(text)


def _parse_number(value: object) -> float:
    text = str(value or "").strip().replace(",", "")
    return float(text) if text else 0.0


def _parse_full_date(value: str) -> date:
    return datetime.strptime(value.strip(), "%d-%b-%Y").date()


def _parse_partial_date(value: str, *, bill_start: date, bill_end: date) -> date:
    raw = value.strip()
    for year in (bill_start.year, bill_end.year, bill_start.year - 1, bill_end.year + 1):
        candidate = datetime.strptime(f"{raw}-{year}", "%d-%b-%Y").date()
        if bill_start <= candidate <= bill_end:
            return candidate
    return datetime.strptime(f"{raw}-{bill_start.year}", "%d-%b-%Y").date()


def _date_range(start: date, end: date) -> list[date]:
    if end < start:
        return []
    days: list[date] = []
    cursor = start
    while cursor <= end:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days


def _line_category(description: str) -> str:
    text = description.lower()
    if "daily charge" in text:
        return "fixed"
    if "zerohero" in text:
        return "zerohero"
    if "offpeak usage" in text:
        return "offpeak_import"
    if "peak usage" in text:
        return "peak_import"
    if "shoulder usage" in text:
        return "shoulder_import"
    if "super export" in text:
        return "super_export"
    if "solar/generation feed in (4pm-" in text:
        return "fit_evening"
    if "solar/generation feed in" in text:
        return "fit_other"
    return "other"


def extract_bill_text_from_pdf(raw_pdf: bytes) -> str:
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise RuntimeError("pypdf is required to parse uploaded bill PDFs") from exc

    reader = PdfReader(BytesIO(raw_pdf))
    pages = [page.extract_text() or "" for page in reader.pages]
    return "\n".join(pages)


def _extract_first(pattern: str, text: str, default: str = "") -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
    return match.group(1).strip() if match else default


def _parse_bill_period(text: str) -> tuple[date, date, int]:
    match = re.search(
        r"Billing Period:\s*(\d{2}-[A-Za-z]{3}-\d{4})\s+to\s+(\d{2}-[A-Za-z]{3}-\d{4})\s+\((\d+)\s+days\)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        match = re.search(
            r"Supply Period\s+(\d{2}-[A-Za-z]{3}-\d{4})\s+to\s+(\d{2}-[A-Za-z]{3}-\d{4})",
            text,
            flags=re.IGNORECASE,
        )
    if not match:
        raise ValueError("Could not find billing period in the uploaded bill")
    start = _parse_full_date(match.group(1))
    end = _parse_full_date(match.group(2))
    days = int(match.group(3)) if match.lastindex and match.lastindex >= 3 else len(_date_range(start, end))
    return start, end, days


def _charge_section(text: str) -> str:
    detailed_index = text.rfind("Electricity Charges for Period")
    if detailed_index >= 0:
        section = text[detailed_index:]
        total_match = re.search(r"\nTOTAL\s+\(including\b.*", section, flags=re.IGNORECASE)
        if total_match:
            return section[: total_match.end()]
        return section
    summary_index = text.find("Billing Period:")
    if summary_index >= 0:
        section = text[summary_index:]
        total_match = re.search(r"\nTOTAL\s+\(including\b.*", section, flags=re.IGNORECASE)
        if total_match:
            return section[: total_match.end()]
        return section
    return text


def _parse_bill_lines(text: str, *, bill_start: date, bill_end: date) -> list[BillLine]:
    section = _charge_section(text)
    money_pattern = r"-?\$\s*[0-9,]+(?:\.\d+)?|\$\s*-?[0-9,]+(?:\.\d+)?"
    row_pattern = re.compile(
        r"^(?P<description>.+?)\s+"
        r"(?:(?P<plan>[A-Z][A-Z0-9]*(?:[ -][A-Z0-9]+)*)\s+)?"
        r"(?P<period>\d{2}-[A-Za-z]{3}\s*-\s*\d{2}-[A-Za-z]{3})\s+"
        r"(?P<quantity>[0-9,]+(?:\.\d+)?)\s+"
        r"(?P<unit>Days?|Day|kWh)\s+x\s+"
        rf"(?P<rate>{money_pattern})\s+"
        rf"(?P<total>{money_pattern})"
        rf"(?:\s+(?:(?P<discount>[0-9]+(?:\.\d+)?%)\s+)?(?P<net_total>{money_pattern}))?\s*$",
    )
    lines: list[BillLine] = []
    for raw_line in section.splitlines():
        normalized = _clean_text(raw_line)
        if not normalized or normalized.lower() in {"new usage charges", "other"}:
            continue
        match = row_pattern.match(normalized)
        if not match:
            continue
        period_text = match.group("period")
        period_match = re.match(r"(\d{2}-[A-Za-z]{3})\s*-\s*(\d{2}-[A-Za-z]{3})", period_text)
        if not period_match:
            continue
        start = _parse_partial_date(period_match.group(1), bill_start=bill_start, bill_end=bill_end)
        end = _parse_partial_date(period_match.group(2), bill_start=bill_start, bill_end=bill_end)
        description = _clean_text(match.group("description"))
        if description.lower().startswith(("description ", "period ", "plan name ")):
            continue
        lines.append(
            BillLine(
                description=description,
                category=_line_category(description),
                period_start=start,
                period_end=end,
                quantity=_parse_number(match.group("quantity")),
                unit=match.group("unit"),
                rate_aud=_parse_money(match.group("rate")),
                total_aud=_parse_money(match.group("net_total") or match.group("total")),
            )
        )
    if not lines:
        raise ValueError("Could not parse bill line items from the uploaded bill")
    return lines


def parse_globird_bill_text(text: str, *, source_filename: str = "") -> dict[str, Any]:
    normalized_text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\u00a0", " ")
    bill_start, bill_end, days = _parse_bill_period(normalized_text)
    lines = _parse_bill_lines(normalized_text, bill_start=bill_start, bill_end=bill_end)
    invoice_number = _extract_first(r"Invoice number\s+(\d+)", normalized_text)
    if not invoice_number:
        digest = hashlib.sha256(normalized_text.encode("utf-8")).hexdigest()[:12]
        invoice_number = f"unknown-{digest}"
    issue_date_text = _extract_first(r"Issue Date\s+(\d{2}-[A-Za-z]{3}-\d{4})", normalized_text)
    due_date_text = _extract_first(r"Due Date(?:\s+\(Direct Debit Date\))?\s+(\d{2}-[A-Za-z]{3}-\d{4})", normalized_text)
    nmi = _extract_first(r"National Metering Identifier \(NMI\)\s+([0-9]+)", normalized_text)
    amount_due_text = _extract_first(r"Amount Due\s*\n?\s*(\$ ?[0-9,]+(?:\.\d{2})?)", normalized_text)
    bill_total_text = _extract_first(r"^TOTAL \(including \$[0-9,.]+ GST\)\s+(-?\$ ?[0-9,]+(?:\.\d{2})?)", normalized_text)
    gst_text = _extract_first(r"^TOTAL \(including \$([0-9,.]+) GST\)", normalized_text)

    line_dicts = [
        {
            "description": line.description,
            "category": line.category,
            "period_start": line.period_start.isoformat(),
            "period_end": line.period_end.isoformat(),
            "quantity": line.quantity,
            "unit": line.unit,
            "rate_aud": line.rate_aud,
            "total_aud": line.total_aud,
        }
        for line in lines
    ]
    bill_total = _parse_money(bill_total_text) if bill_total_text else round(sum(line.total_aud for line in lines), 2)
    return {
        "invoice_number": invoice_number,
        "retailer": "GloBird Energy",
        "plan_name": GLOBIRD_PLAN_NAME if GLOBIRD_PLAN_NAME in normalized_text else "",
        "source_filename": source_filename,
        "issue_date": _parse_full_date(issue_date_text).isoformat() if issue_date_text else "",
        "due_date": _parse_full_date(due_date_text).isoformat() if due_date_text else "",
        "nmi": nmi,
        "billing_period": {
            "start": bill_start.isoformat(),
            "end": bill_end.isoformat(),
            "days": days,
        },
        "amount_due_aud": _parse_money(amount_due_text) if amount_due_text else None,
        "bill_total_including_gst_aud": bill_total,
        "gst_included_line_aud": _parse_money(gst_text) if gst_text else None,
        "line_items": line_dicts,
        "zerohero_credited_days": round(
            sum(line.quantity for line in lines if line.category == "zerohero" and line.unit.lower().startswith("day")),
            4,
        ),
        "parsed_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }


def parse_bill_upload(raw: bytes, *, filename: str) -> tuple[dict[str, Any], str]:
    lower = filename.lower()
    if lower.endswith(".pdf") or raw[:4] == b"%PDF":
        text = extract_bill_text_from_pdf(raw)
    else:
        text = raw.decode("utf-8", errors="replace")
    bill = parse_globird_bill_text(text, source_filename=filename)
    return bill, text


def _clean_file_stem(value: object) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "")).strip(".-")


def _safe_file_stem(value: object) -> str:
    cleaned = _clean_file_stem(value)
    return cleaned or "bill"


def _bill_artifact_stem(bill: dict[str, Any], *, filename: str) -> str:
    period = bill.get("billing_period") if isinstance(bill.get("billing_period"), dict) else {}
    period_start = _clean_file_stem(period.get("start"))
    period_end = _clean_file_stem(period.get("end"))
    invoice = _clean_file_stem(bill.get("invoice_number"))

    if period_start and period_end:
        date_stem = period_start if period_start == period_end else f"{period_start}_to_{period_end}"
        return f"{date_stem}_invoice-{invoice}" if invoice else date_stem

    issue_date = _clean_file_stem(bill.get("issue_date"))
    if issue_date:
        return f"{issue_date}_invoice-{invoice}" if invoice else issue_date

    return _safe_file_stem(invoice or Path(filename).stem)


def load_bills_index(bills_dir: Path) -> dict[str, Any]:
    path = bills_dir / BILLS_INDEX_FILENAME
    if not path.exists():
        return {
            "recorded_at_local": date.today().isoformat(),
            "timezone": "Australia/Sydney",
            "source_note": "Uploaded electricity bills parsed by Wattimize.",
            "bills": [],
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("bills", [])
    return payload


def save_bill_artifacts(
    bills_dir: Path,
    *,
    raw: bytes,
    filename: str,
    bill: dict[str, Any],
    text: str,
) -> dict[str, Any]:
    raw_dir = bills_dir / RAW_BILLS_DIRNAME
    text_dir = bills_dir / TEXT_BILLS_DIRNAME
    raw_dir.mkdir(parents=True, exist_ok=True)
    text_dir.mkdir(parents=True, exist_ok=True)
    artifact_stem = _bill_artifact_stem(bill, filename=filename)
    extension = ".pdf" if filename.lower().endswith(".pdf") or raw[:4] == b"%PDF" else ".txt"
    raw_path = raw_dir / f"{artifact_stem}{extension}"
    text_path = text_dir / f"{artifact_stem}.txt"
    raw_path.write_bytes(raw)
    text_path.write_text(text, encoding="utf-8")

    index = load_bills_index(bills_dir)
    record = dict(bill)
    record["source_pdf"] = str(raw_path.relative_to(bills_dir))
    record["extracted_text"] = str(text_path.relative_to(bills_dir))
    record["content_sha256"] = hashlib.sha256(raw).hexdigest()
    bills = [item for item in index.get("bills", []) if str(item.get("invoice_number")) != str(record.get("invoice_number"))]
    bills.append(record)
    bills.sort(key=lambda item: str((item.get("billing_period") or {}).get("start") or ""))
    index["bills"] = bills
    index["recorded_at_local"] = date.today().isoformat()
    (bills_dir / BILLS_INDEX_FILENAME).write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")
    return record


def _daily_metric(usage_payload: dict[str, Any], category: str) -> float:
    daily_cost = usage_payload.get("_daily_cost")
    if not isinstance(daily_cost, dict):
        return 0.0
    line_items = daily_cost.get("line_items")
    if not isinstance(line_items, dict):
        return 0.0
    import_item = line_items.get("import") if isinstance(line_items.get("import"), dict) else {}
    export_item = line_items.get("export") if isinstance(line_items.get("export"), dict) else {}
    if category == "peak_import":
        return float(import_item.get("peak_kwh") or 0.0)
    if category == "shoulder_import":
        return float(import_item.get("shoulder_kwh") or 0.0)
    if category == "offpeak_import":
        return float(import_item.get("free_kwh") or 0.0) + float(import_item.get("off_peak_balance_kwh") or 0.0)
    if category == "fit_evening":
        return float(export_item.get("fit_evening_kwh") or 0.0)
    if category == "fit_other":
        return float(export_item.get("fit_other_kwh") or 0.0)
    if category == "super_export":
        return float(export_item.get("super_export_billable_kwh") or 0.0)
    return 0.0


def _estimate_total(usage_payload: dict[str, Any]) -> float | None:
    daily_cost = usage_payload.get("_daily_cost")
    if not isinstance(daily_cost, dict):
        return None
    value = daily_cost.get("total_daily_cost_aud")
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _empty_actual(day: date, bill: dict[str, Any], usage_payload: dict[str, Any]) -> dict[str, Any]:
    estimated = _estimate_total(usage_payload)
    return {
        "status": "bill_allocated",
        "source": "uploaded_bill",
        "invoice_number": bill.get("invoice_number") or "",
        "issue_date": bill.get("issue_date") or "",
        "bill_period": bill.get("billing_period") or {},
        "day": day.isoformat(),
        "currency": "AUD",
        "actual_daily_cost_aud": 0.0,
        "estimated_daily_cost_aud": estimated,
        "delta_aud": None,
        "allocation_method": "bill_line_totals_weighted_by_daily_report_kwh",
        "line_items": {
            "fixed_aud": 0.0,
            "import_aud": 0.0,
            "solar_feed_in_aud": 0.0,
            "super_export_aud": 0.0,
            "zerohero_aud": 0.0,
            "other_aud": 0.0,
        },
    }


def _actual_bucket(category: str) -> str:
    if category == "fixed":
        return "fixed_aud"
    if category in {"peak_import", "offpeak_import", "shoulder_import"}:
        return "import_aud"
    if category in {"fit_evening", "fit_other"}:
        return "solar_feed_in_aud"
    if category == "super_export":
        return "super_export_aud"
    if category == "zerohero":
        return "zerohero_aud"
    return "other_aud"


def apply_bill_actuals_to_daily_reports(db_path: Path | str, bill: dict[str, Any]) -> dict[str, Any]:
    period = bill.get("billing_period") if isinstance(bill.get("billing_period"), dict) else {}
    start = date.fromisoformat(str(period.get("start")))
    end = date.fromisoformat(str(period.get("end")))
    all_bill_days = _date_range(start, end)
    if not all_bill_days:
        return {"updated_daily_reports": 0, "covered_days": 0, "missing_report_days": []}

    with sqlite3.connect(str(db_path), timeout=30.0) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        rows = conn.execute(
            """
            SELECT report_day_local, usage_json
            FROM daily_reports
            WHERE report_day_local >= ?
              AND report_day_local <= ?
            ORDER BY report_day_local
            """,
            (start.isoformat(), end.isoformat()),
        ).fetchall()

    usage_by_day: dict[date, dict[str, Any]] = {}
    for day_text, usage_json_text in rows:
        try:
            payload = json.loads(usage_json_text or "{}")
        except json.JSONDecodeError:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        usage_by_day[date.fromisoformat(day_text)] = payload

    actuals: dict[date, dict[str, Any]] = {
        day: _empty_actual(day, bill, usage_payload)
        for day, usage_payload in usage_by_day.items()
        if start <= day <= end
    }

    line_items = bill.get("line_items") if isinstance(bill.get("line_items"), list) else []
    for raw_line in line_items:
        if not isinstance(raw_line, dict):
            continue
        category = str(raw_line.get("category") or "other")
        try:
            line_start = date.fromisoformat(str(raw_line.get("period_start")))
            line_end = date.fromisoformat(str(raw_line.get("period_end")))
            total = float(raw_line.get("total_aud") or 0.0)
        except (TypeError, ValueError):
            continue
        covered_days = [day for day in _date_range(max(line_start, start), min(line_end, end)) if day in actuals]
        if not covered_days:
            continue
        if category in {"fixed", "zerohero", "other"}:
            weights = {day: 1.0 for day in covered_days}
        else:
            weights = {day: max(_daily_metric(usage_by_day.get(day, {}), category), 0.0) for day in covered_days}
            if sum(weights.values()) <= 0:
                weights = {day: 1.0 for day in covered_days}
        weight_total = sum(weights.values()) or float(len(covered_days))
        bucket = _actual_bucket(category)
        for day in covered_days:
            share = total * (weights[day] / weight_total)
            actuals[day]["actual_daily_cost_aud"] += share
            actuals[day]["line_items"][bucket] += share

    updates: list[tuple[str, str]] = []
    for day, usage_payload in usage_by_day.items():
        actual = actuals.get(day)
        if actual is None:
            continue
        actual_total = round(float(actual["actual_daily_cost_aud"]), 4)
        actual["actual_daily_cost_aud"] = actual_total
        for key, value in list(actual["line_items"].items()):
            actual["line_items"][key] = round(float(value), 4)
        estimated = actual.get("estimated_daily_cost_aud")
        if estimated is not None:
            actual["delta_aud"] = round(float(estimated) - actual_total, 4)
        usage_payload["_bill_actual_cost"] = actual
        updates.append((json.dumps(usage_payload, ensure_ascii=False, separators=(",", ":")), day.isoformat()))

    if updates:
        with sqlite3.connect(str(db_path), timeout=30.0) as conn:
            conn.execute("PRAGMA busy_timeout=30000")
            conn.executemany("UPDATE daily_reports SET usage_json = ? WHERE report_day_local = ?", updates)
            conn.commit()

    missing_days = [day.isoformat() for day in all_bill_days if day not in usage_by_day]
    return {
        "updated_daily_reports": len(updates),
        "covered_days": len(all_bill_days),
        "missing_report_days": missing_days,
        "invoice_number": bill.get("invoice_number") or "",
        "bill_total_including_gst_aud": bill.get("bill_total_including_gst_aud"),
    }
