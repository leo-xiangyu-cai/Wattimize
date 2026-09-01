from app.billing import parse_globird_bill_text


def test_parse_bill_lines_with_discounted_net_total() -> None:
    text = """
Invoice number 11262392
Issue Date 02-Aug-2026
Amount Due
$0
National Metering Identifier (NMI) 43104479765
Billing Period: 05-Jul-2026 to 01-Aug-2026 (28 days)
Electricity Charges for Period 05-Jul-2026 to 01-Aug-2026
Description Plan Name Period Quantity & Unit Rates Total Discount Net Total
New Usage Charges
Peak Usage ZEROHERO 05-Jul - 27-Jul 9.00 kWh x  $0.52800 $4.75 $4.75
Offpeak Usage ZEROHERO 05-Jul - 27-Jul 891.05 kWh x  $0.00000 $0.00 $0.00
Shoulder Usage ZEROHERO 05-Jul - 27-Jul 121.95 kWh x  $0.41800 $50.98 $50.98
Solar/Generation Feed in (4pm-9pm) ZEROHERO 05-Jul - 27-Jul 206.64 kWh x -$0.02000 -$4.13 -$4.13
Solar/Generation Feed in (9pm-10am, 2pm-4pm) ZEROHERO 05-Jul - 27-Jul 7.72 kWh x  $0.00000 $0.00 $0.00
Solar/Generation Feed in (10am-2pm) ZEROHERO 05-Jul - 27-Jul 0.67 kWh x  $0.00000 $0.00 $0.00
Peak Usage FOUR4FREE 28-Jul - 01-Aug 1.27 kWh x  $0.59620 $0.76 3% $0.73
Offpeak Usage - Step 1 FOUR4FREE 28-Jul - 01-Aug 228.46 kWh x  $0.00000 $0.00 3% $0.00
Shoulder Usage FOUR4FREE 28-Jul - 01-Aug 20.81 kWh x  $0.38610 $8.03 3% $7.79
Solar/Generation Feed in (4pm-11pm) FOUR4FREE 28-Jul - 01-Aug 56.21 kWh x -$0.08000 -$4.50 -$4.50
Solar/Generation Feed in (11pm-4pm) FOUR4FREE 28-Jul - 01-Aug 1.93 kWh x  $0.00000 $0.00 $0.00
Super Export top up - Step 1 ZEROHERO 05-Jul - 27-Jul 204.44 kWh x -$0.08000 -$16.36 -$16.36
ZeroHero ZEROHERO 06-Jul - 27-Jul 22.00 Day x -$1.00000 -$22.00 -$22.00
Other
Daily Charge ZEROHERO 05-Jul - 27-Jul 23 Days x  $1.70500 $39.22 $39.22
Daily Charge FOUR4FREE 28-Jul - 01-Aug 5 Days x  $1.52900 $7.65 3% $7.42
SUBTOTAL (including $10.14 GST) $64.40
Total Discount $0.50
TOTAL (including $10.08 GST) $63.90
"""

    bill = parse_globird_bill_text(text, source_filename="bill.txt")

    assert bill["bill_total_including_gst_aud"] == 63.9
    assert bill["gst_included_line_aud"] == 10.08
    assert bill["amount_due_aud"] == 0.0
    assert round(sum(item["total_aud"] for item in bill["line_items"]), 2) == 63.9
    assert [item["description"] for item in bill["line_items"]] == [
        "Peak Usage",
        "Offpeak Usage",
        "Shoulder Usage",
        "Solar/Generation Feed in (4pm-9pm)",
        "Solar/Generation Feed in (9pm-10am, 2pm-4pm)",
        "Solar/Generation Feed in (10am-2pm)",
        "Peak Usage",
        "Offpeak Usage - Step 1",
        "Shoulder Usage",
        "Solar/Generation Feed in (4pm-11pm)",
        "Solar/Generation Feed in (11pm-4pm)",
        "Super Export top up - Step 1",
        "ZeroHero",
        "Daily Charge",
        "Daily Charge",
    ]
    assert bill["line_items"][7]["category"] == "offpeak_import"
    assert bill["line_items"][9]["category"] == "fit_evening"
    assert bill["line_items"][14]["total_aud"] == 7.42


def test_parse_bill_lines_without_plan_or_discount_columns() -> None:
    text = """
Invoice number 11111111
Issue Date 30-Jun-2026
Amount Due
$40.25
National Metering Identifier (NMI) 43104479765
Billing Period: 01-Jun-2026 to 30-Jun-2026 (30 days)
Description Period Quantity & Unit Rates Total
Daily Charge 01-Jun - 30-Jun 30 Days x  $1.52900 $45.87
Peak Usage 01-Jun - 30-Jun 10.00 kWh x  $0.59620 $5.96
Solar/Generation Feed in (4pm-9pm) 01-Jun - 30-Jun 30.00 kWh x -$0.08000 -$2.40
TOTAL (including $4.94 GST) $49.43
"""

    bill = parse_globird_bill_text(text, source_filename="old-bill.txt")

    assert bill["bill_total_including_gst_aud"] == 49.43
    assert [item["category"] for item in bill["line_items"]] == ["fixed", "peak_import", "fit_evening"]
