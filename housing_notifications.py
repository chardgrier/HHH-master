#!/usr/bin/env python3
"""
HHH Housing Searches — Daily digest + stale alerts.

One email per morning to the housing team containing:
  • Searches assigned to each rep, grouped by status (Still Open, Pending)
  • Stale watchlist:
      - Still Open ≥ 7 days
      - Pending ≥ 5 days past presentation date
  • New searches created in the last 24h

Runs after housing_sync.py has refreshed data/housing.json.
Skipped entirely if there are no active searches.
"""
import json
import os
from datetime import date, datetime, timedelta

from notifier import send_email

DATA_FILE = "data/housing.json"

HOUSING_TEAM = [
    "carrie@hardhathousing.com",
    "briana@hardhathousing.com",
    "carlos@hardhathousing.com",
    "may@hardhathousing.com",
    "patrice@hardhathousing.com",
    "david@hardhathousing.com",
]

STALE_OPEN_DAYS = 7
STALE_PENDING_DAYS = 5


def load_data():
    if not os.path.exists(DATA_FILE):
        return None
    with open(DATA_FILE) as f:
        return json.load(f)


def fmt_date(s):
    if not s:
        return "—"
    try:
        d = date.fromisoformat(s)
        return f"{d.month}/{d.day}/{str(d.year)[-2:]}"
    except Exception:
        return s


def section_table(title, color, records, columns):
    """Build an HTML section with a colored header and a table of records."""
    if not records:
        return ""
    th = "".join(f'<th style="text-align:left;padding:6px 10px">{c}</th>' for c, _ in columns)
    rows = ""
    for r in records:
        cells = ""
        for _, fn in columns:
            v = fn(r)
            cells += f'<td style="padding:6px 10px;border-bottom:1px solid #e2e8f0">{v}</td>'
        rows += f"<tr>{cells}</tr>"
    return f"""
<h3 style="color:{color};margin:18px 0 6px;font-size:14px">{title} ({len(records)})</h3>
<table style="border-collapse:collapse;width:100%;font-size:12px">
  <thead><tr style="background:#f7f8fc;color:#718096;text-transform:uppercase;font-size:10px">{th}</tr></thead>
  <tbody>{rows}</tbody>
</table>
"""


def linked_company(r):
    co = r.get("company") or "(unknown)"
    url = r.get("url")
    return f'<a href="{url}" style="color:#3182ce;text-decoration:none">{co}</a>' if url else co


def build_digest_html(data):
    today = date.today().isoformat()
    records = data.get("records") or []
    attn = data.get("attention") or {}
    totals = data.get("totals") or {}

    # Active = anything that isn't Closed/Converted
    active = [r for r in records if r["status"] in ("Still Open", "Pending", "Future")]
    by_rep = {}
    for r in active:
        rep = r.get("housing_rep") or "Unassigned"
        by_rep.setdefault(rep, []).append(r)

    # New in last 24h
    cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat() + "Z"
    new_today = [r for r in records if (r.get("created_at") or "") > cutoff]

    stale_open = attn.get("stale_still_open") or []
    stale_pending = attn.get("stale_pending") or []

    cols_main = [
        ("Status",     lambda r: r.get("status") or ""),
        ("Company",    lambda r: linked_company(r)),
        ("City, State", lambda r: r.get("location") or "—"),
        ("Salesperson", lambda r: r.get("salesperson") or "—"),
        ("Crew",       lambda r: r.get("crew") or ""),
        ("Budget",     lambda r: r.get("budget") or ""),
        ("Start",      lambda r: fmt_date(r.get("start_date"))),
        ("Days open",  lambda r: f'{r.get("days_open") or 0}d'),
        ("Options due", lambda r: fmt_date(r.get("due_on"))),
    ]

    cols_stale_open = [
        ("Days",       lambda r: f'<b>{r.get("days_open") or 0}d</b>'),
        ("Company",    lambda r: linked_company(r)),
        ("City, State", lambda r: r.get("location") or "—"),
        ("Rep",        lambda r: r.get("housing_rep") or "—"),
        ("Salesperson", lambda r: r.get("salesperson") or "—"),
    ]
    cols_stale_pending = [
        ("Past due",   lambda r: f'<b>+{r.get("days_past_due") or 0}d</b>'),
        ("Company",    lambda r: linked_company(r)),
        ("City, State", lambda r: r.get("location") or "—"),
        ("Rep",        lambda r: r.get("housing_rep") or "—"),
        ("Presented",  lambda r: fmt_date(r.get("due_on"))),
    ]
    cols_new = [
        ("Company",    lambda r: linked_company(r)),
        ("City, State", lambda r: r.get("location") or "—"),
        ("Salesperson", lambda r: r.get("salesperson") or "—"),
        ("Rep",        lambda r: r.get("housing_rep") or "—"),
        ("Crew",       lambda r: r.get("crew") or ""),
        ("Start",      lambda r: fmt_date(r.get("start_date"))),
    ]

    summary = f"""
<p style="font-size:14px">
  <b>{totals.get('still_open', 0)}</b> still open ·
  <b>{totals.get('pending', 0)}</b> pending presentation ·
  <b>{totals.get('future', 0)}</b> future ·
  <b style="color:#9b2c2c">{len(stale_open)}</b> stale opens ·
  <b style="color:#c05621">{len(stale_pending)}</b> stale pendings
</p>
"""

    parts = [summary]

    if new_today:
        parts.append(section_table(
            "🆕 New in the last 24 hours", "#3182ce", new_today, cols_new))

    if stale_open:
        parts.append(section_table(
            f"🚨 Still Open ≥ {STALE_OPEN_DAYS} days — please follow up",
            "#9b2c2c", stale_open, cols_stale_open))
    if stale_pending:
        parts.append(section_table(
            f"⏰ Pending ≥ {STALE_PENDING_DAYS} days past presentation",
            "#c05621", stale_pending, cols_stale_pending))

    parts.append('<h3 style="margin:20px 0 6px;font-size:14px">📋 Active by housing rep</h3>')
    if not by_rep:
        parts.append('<p style="color:#718096;font-size:12px">No active searches.</p>')
    else:
        for rep in sorted(by_rep.keys()):
            rep_recs = sorted(by_rep[rep], key=lambda r: -(r.get("days_open") or 0))
            parts.append(f'<h4 style="margin:10px 0 4px;color:#1a1a2e;font-size:13px">{rep} — {len(rep_recs)} search(es)</h4>')
            parts.append(section_table("", "#1a1a2e", rep_recs, cols_main).replace(
                '<h3 style="color:#1a1a2e;margin:18px 0 6px;font-size:14px"> (' + str(len(rep_recs)) + ')</h3>',
                ''
            ))

    html = f"""\
<html><body style="font-family:-apple-system,sans-serif;color:#2d3748;font-size:13px;max-width:900px">
<h2 style="color:#1a1a2e">🏠 HHH Housing Searches — Daily Digest ({today})</h2>
{"".join(parts)}
<p style="font-size:11px;color:#718096;margin-top:18px">
  Full dashboard: <a href="https://dashboard.hardhat-housing.com/housing.html">dashboard.hardhat-housing.com/housing.html</a><br>
  Status changes: drag the Asana card between sections in the
  <a href="https://app.asana.com/0/1209569809329318/list">2026 Housing Searches</a> project.
</p>
</body></html>"""

    return html


def main():
    print(f"=== Housing notifications @ {date.today().isoformat()} ===")
    data = load_data()
    if not data:
        print("· no data/housing.json — nothing to send")
        return

    totals = data.get("totals") or {}
    active = (totals.get("still_open") or 0) + (totals.get("pending") or 0)
    if active == 0:
        print("· no active searches — skipping digest")
        return

    html = build_digest_html(data)
    subject_bits = []
    if totals.get("still_open"):
        subject_bits.append(f"{totals['still_open']} open")
    if totals.get("pending"):
        subject_bits.append(f"{totals['pending']} pending")
    stale_count = len((data.get("attention") or {}).get("stale_still_open") or []) + \
                  len((data.get("attention") or {}).get("stale_pending")   or [])
    if stale_count:
        subject_bits.append(f"⚠ {stale_count} stale")
    subject = "Housing Searches — " + " · ".join(subject_bits) + f" ({date.today().isoformat()})"

    try:
        send_email(to=HOUSING_TEAM, subject=subject, html_body=html)
    except Exception as e:
        print(f"  ! digest send failed: {e}")
        raise

    print("✓ done")


if __name__ == "__main__":
    main()
