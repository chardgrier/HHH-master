#!/usr/bin/env python3
"""
HHH Dashboard — Daily notifications.

Three jobs, run once per morning after sync.py + qb_sync.py have refreshed data:

  1. Maintenance digest → charlie@hardhathousing.com
     All open (non-Resolved) maintenance tickets from data/maintenance.json.
     Skipped if zero open tickets.

  2. Stale invoice reminder → assigned salesperson, CC richard@
     A/R invoices with balance > 0 and days_overdue > 45.
     One email per (salesperson, batch) — sent ONCE per invoice. State tracked
     in data/notification_state.json so we never spam the same invoice twice.

  3. A/P overdue alert → arun@hardhathousing.com
     A/P bills with balance > 0 and due_date < today (any past-due bill).
     One email per bill, sent ONCE per bill_id (state-tracked).

First-run safety: if data/notification_state.json doesn't exist, the script
creates it pre-seeded with all currently-overdue items WITHOUT sending email.
This prevents a flood of historical alerts on initial deploy. Going forward,
only newly-overdue items trigger an email.
"""
import json
import os
import sys
from datetime import date

from notifier import send_email

STATE_FILE = "data/notification_state.json"
DATA_DIR = "data"

CHARLIE_EMAIL = "charlie@hardhathousing.com"
ARUN_EMAIL = "arun@hardhathousing.com"
RICHARD_EMAIL = "richard@hardhathousing.com"
DOMAIN = "@hardhathousing.com"

STALE_DAYS_THRESHOLD = 45

# Salesperson name → email override. Default is `firstname.lower()@hardhathousing.com`.
SALESPERSON_EMAIL_OVERRIDES = {
    # "Display Name": "real-email@hardhathousing.com"
}


def salesperson_email(sp):
    """Map a salesperson display name to their @hardhathousing.com email."""
    if not sp:
        return None
    if sp in SALESPERSON_EMAIL_OVERRIDES:
        return SALESPERSON_EMAIL_OVERRIDES[sp]
    first = sp.strip().split()[0].lower()
    if not first or first in ("unknown", "hhh"):
        return None
    return f"{first}{DOMAIN}"


def load_json(path, default=None):
    if not os.path.exists(path):
        return default
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        print(f"  ! failed to read {path}: {e}")
        return default


def load_state():
    s = load_json(STATE_FILE, default=None)
    if s is None:
        return {"stale_invoice_sent": [], "ap_overdue_sent": [], "_seeded": False}
    s.setdefault("stale_invoice_sent", [])
    s.setdefault("ap_overdue_sent", [])
    s.setdefault("_seeded", True)
    return s


def save_state(state):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def fmt_money(n):
    try:
        return "$" + f"{round(float(n)):,}"
    except Exception:
        return str(n)


# ──────────────────────────────────────────────────────────────────────────
# 1. Maintenance digest
# ──────────────────────────────────────────────────────────────────────────

def send_maintenance_digest():
    data = load_json("data/maintenance.json", default={})
    tickets = data.get("maintenance_tasks", []) or []
    open_tickets = [t for t in tickets if (t.get("status") or "").strip().lower() != "resolved"]

    if not open_tickets:
        print("· maintenance: no open tickets, skipping email")
        return

    # Group by severity (Emergency → Urgent → Standard → General Updates)
    sev_order = ["Emergency Maintenance", "Urgent Maintenance", "Standard Maintenance", "General Updates"]
    by_sev = {s: [] for s in sev_order}
    for t in open_tickets:
        by_sev.setdefault(t.get("severity", "General Updates"), []).append(t)

    today = date.today().isoformat()
    rows_html = ""
    for sev in sev_order:
        rows = by_sev.get(sev) or []
        if not rows:
            continue
        color = {
            "Emergency Maintenance": "#9b2c2c",
            "Urgent Maintenance": "#c05621",
            "Standard Maintenance": "#2b6cb0",
            "General Updates": "#4a5568",
        }.get(sev, "#4a5568")
        rows_html += f'<tr><td colspan="4" style="background:{color};color:#fff;font-weight:700;padding:8px 10px">{sev} ({len(rows)})</td></tr>'
        for t in rows:
            name = t.get("name") or "(untitled)"
            proj = t.get("project") or "—"
            house = t.get("house") or ""
            status = t.get("status") or "—"
            url = t.get("url") or "#"
            link = f'<a href="{url}">{name}</a>' if url and url != "#" else name
            rows_html += (
                f'<tr>'
                f'<td style="padding:6px 10px;border-bottom:1px solid #e2e8f0">{link}</td>'
                f'<td style="padding:6px 10px;border-bottom:1px solid #e2e8f0">{proj}</td>'
                f'<td style="padding:6px 10px;border-bottom:1px solid #e2e8f0;text-align:center">{house}</td>'
                f'<td style="padding:6px 10px;border-bottom:1px solid #e2e8f0">{status}</td>'
                f'</tr>'
            )

    html = f"""\
<html><body style="font-family:-apple-system,sans-serif;color:#2d3748;font-size:14px">
<h2 style="color:#1a1a2e">🔧 HHH Maintenance — Open Tickets ({today})</h2>
<p>{len(open_tickets)} open ticket(s) — anything not in <b>Resolved</b> status.</p>
<table style="border-collapse:collapse;width:100%;font-size:13px">
  <thead><tr style="background:#f7f8fc;color:#718096;text-transform:uppercase;font-size:11px">
    <th style="text-align:left;padding:6px 10px">Ticket</th>
    <th style="text-align:left;padding:6px 10px">Project</th>
    <th style="padding:6px 10px">House</th>
    <th style="text-align:left;padding:6px 10px">Status</th>
  </tr></thead>
  <tbody>{rows_html}</tbody>
</table>
<p style="font-size:12px;color:#718096;margin-top:14px">
  Full dashboard: <a href="https://dashboard.hardhat-housing.com/maintenance.html">dashboard.hardhat-housing.com/maintenance.html</a>
</p>
</body></html>"""

    send_email(
        to=CHARLIE_EMAIL,
        subject=f"HHH Maintenance — {len(open_tickets)} open ticket(s) — {today}",
        html_body=html,
    )


# ──────────────────────────────────────────────────────────────────────────
# 2. Stale invoice reminder
# ──────────────────────────────────────────────────────────────────────────

def find_stale_invoices(qb, projects):
    """Return list of (salesperson, invoice_dict, project_name) for invoices >45 days overdue."""
    sp_by_name = {p["name"]: (p.get("salesperson") or "Unknown") for p in projects}
    sp_by_num = {}
    for p in projects:
        if p.get("project_number"):
            sp_by_num[p["project_number"]] = p.get("salesperson") or "Unknown"

    today = date.today()
    out = []
    for proj_name, months in (qb.get("ar_status_by_project") or {}).items():
        for invoices in months.values():
            for inv in invoices:
                bal = float(inv.get("balance") or 0)
                if bal <= 0.001:
                    continue
                due_str = inv.get("due_date")
                if not due_str:
                    continue
                try:
                    due = date.fromisoformat(due_str)
                except Exception:
                    continue
                days_overdue = (today - due).days
                if days_overdue <= STALE_DAYS_THRESHOLD:
                    continue

                sp = sp_by_name.get(proj_name)
                if not sp:
                    pnum = inv.get("project_number")
                    if pnum:
                        sp = sp_by_num.get(str(pnum))
                if not sp:
                    sp = "Unknown"

                out.append((sp, {**inv, "days_overdue": days_overdue}, proj_name))
    return out


def send_stale_invoice_emails(state, seeding):
    """For each stale invoice not yet notified, email the assigned salesperson."""
    qb = load_json("data/qb_status.json", default={})
    projects_data = load_json("data/projects.json", default={"projects": []})
    projects = projects_data.get("projects", [])

    stale = find_stale_invoices(qb, projects)
    sent_set = set(state.get("stale_invoice_sent") or [])

    pending = [(sp, inv, proj) for sp, inv, proj in stale if str(inv.get("invoice_id")) not in sent_set]
    if not pending:
        print(f"· stale invoices: 0 new (already-notified: {len(sent_set)})")
        return

    if seeding:
        for _, inv, _ in pending:
            sent_set.add(str(inv.get("invoice_id")))
        state["stale_invoice_sent"] = sorted(sent_set)
        print(f"· stale invoices: SEEDED {len(pending)} existing (no emails sent on first run)")
        return

    # Group pending by salesperson — one email per rep summarizing all their stale invoices
    by_sp = {}
    for sp, inv, proj in pending:
        by_sp.setdefault(sp, []).append((inv, proj))

    for sp, items in by_sp.items():
        to = salesperson_email(sp)
        if not to:
            print(f"  ! no email for salesperson '{sp}' — skipping {len(items)} invoice(s)")
            continue

        items.sort(key=lambda x: -x[0]["days_overdue"])
        total = sum(float(i[0].get("balance") or 0) for i in items)

        rows_html = ""
        for inv, proj in items:
            rows_html += (
                f'<tr>'
                f'<td style="padding:6px 10px;border-bottom:1px solid #e2e8f0">{proj}</td>'
                f'<td style="padding:6px 10px;border-bottom:1px solid #e2e8f0">{inv.get("doc_number") or inv.get("invoice_id") or "—"}</td>'
                f'<td style="padding:6px 10px;border-bottom:1px solid #e2e8f0;text-align:right">{fmt_money(inv.get("balance"))}</td>'
                f'<td style="padding:6px 10px;border-bottom:1px solid #e2e8f0;text-align:center">{inv["days_overdue"]}d</td>'
                f'<td style="padding:6px 10px;border-bottom:1px solid #e2e8f0">{inv.get("due_date") or "—"}</td>'
                f'</tr>'
            )

        html = f"""\
<html><body style="font-family:-apple-system,sans-serif;color:#2d3748;font-size:14px">
<h2 style="color:#1a1a2e">💸 Stale Invoice Reminder — {sp}</h2>
<p>You have <b>{len(items)}</b> invoice(s) more than {STALE_DAYS_THRESHOLD} days past due, totaling <b>{fmt_money(total)}</b>. Please follow up with these clients.</p>
<table style="border-collapse:collapse;width:100%;font-size:13px">
  <thead><tr style="background:#f7f8fc;color:#718096;text-transform:uppercase;font-size:11px">
    <th style="text-align:left;padding:6px 10px">Project</th>
    <th style="text-align:left;padding:6px 10px">Invoice</th>
    <th style="text-align:right;padding:6px 10px">Balance</th>
    <th style="padding:6px 10px">Days Past Due</th>
    <th style="text-align:left;padding:6px 10px">Due Date</th>
  </tr></thead>
  <tbody>{rows_html}</tbody>
</table>
<p style="font-size:12px;color:#718096;margin-top:14px">
  This is a one-time alert per invoice. Once you collect, you won't hear from us again about these. Full A/R aging:
  <a href="https://dashboard.hardhat-housing.com/sales.html">dashboard.hardhat-housing.com/sales.html</a>
</p>
</body></html>"""

        send_email(
            to=to,
            cc=RICHARD_EMAIL,
            subject=f"HHH Stale Invoices — {len(items)} past due ({fmt_money(total)})",
            html_body=html,
        )
        for inv, _ in items:
            sent_set.add(str(inv.get("invoice_id")))

    state["stale_invoice_sent"] = sorted(sent_set)


# ──────────────────────────────────────────────────────────────────────────
# 3. A/P overdue alerts (one email per bill)
# ──────────────────────────────────────────────────────────────────────────

def find_overdue_bills(qb):
    today = date.today()
    out = []
    for bill in (qb.get("ap_bills") or []):
        bal = float(bill.get("balance") or 0)
        if bal <= 0.001:
            continue
        due_str = bill.get("due_date")
        if not due_str:
            continue
        try:
            due = date.fromisoformat(due_str)
        except Exception:
            continue
        days_overdue = (today - due).days
        if days_overdue <= 0:
            continue
        out.append({**bill, "days_overdue": days_overdue})
    return out


def send_ap_overdue_emails(state, seeding):
    qb = load_json("data/qb_status.json", default={})
    overdue = find_overdue_bills(qb)
    sent_set = set(state.get("ap_overdue_sent") or [])

    pending = [b for b in overdue if str(b.get("bill_id")) not in sent_set]
    if not pending:
        print(f"· A/P overdue: 0 new (already-notified: {len(sent_set)})")
        return

    if seeding:
        for b in pending:
            sent_set.add(str(b.get("bill_id")))
        state["ap_overdue_sent"] = sorted(sent_set)
        print(f"· A/P overdue: SEEDED {len(pending)} existing (no emails sent on first run)")
        return

    pending.sort(key=lambda b: -b["days_overdue"])

    for bill in pending:
        vendor = bill.get("vendor") or "(unknown vendor)"
        bal = bill.get("balance")
        due = bill.get("due_date") or "—"
        days = bill.get("days_overdue")
        doc = bill.get("doc_number") or bill.get("bill_id") or "—"
        proj = bill.get("project_number") or "—"

        html = f"""\
<html><body style="font-family:-apple-system,sans-serif;color:#2d3748;font-size:14px">
<h2 style="color:#9b2c2c">⚠️ A/P Overdue — {vendor}</h2>
<table style="border-collapse:collapse;font-size:14px;margin:10px 0">
  <tr><td style="padding:6px 12px;color:#718096">Vendor</td><td style="padding:6px 12px;font-weight:700">{vendor}</td></tr>
  <tr><td style="padding:6px 12px;color:#718096">Bill #</td><td style="padding:6px 12px">{doc}</td></tr>
  <tr><td style="padding:6px 12px;color:#718096">Project</td><td style="padding:6px 12px">{proj}</td></tr>
  <tr><td style="padding:6px 12px;color:#718096">Balance</td><td style="padding:6px 12px;font-weight:700">{fmt_money(bal)}</td></tr>
  <tr><td style="padding:6px 12px;color:#718096">Due Date</td><td style="padding:6px 12px">{due}</td></tr>
  <tr><td style="padding:6px 12px;color:#718096">Days Past Due</td><td style="padding:6px 12px;color:#9b2c2c;font-weight:700">{days}d</td></tr>
</table>
<p style="font-size:12px;color:#718096;margin-top:10px">One-time alert per bill. Pay it (or mark it paid in QuickBooks) and you won't hear from this bill again.</p>
</body></html>"""

        send_email(
            to=ARUN_EMAIL,
            subject=f"A/P OVERDUE: {vendor} · {fmt_money(bal)} · {days}d past due",
            html_body=html,
        )
        sent_set.add(str(bill.get("bill_id")))

    state["ap_overdue_sent"] = sorted(sent_set)


# ──────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────

def main():
    print(f"=== HHH Notifications @ {date.today().isoformat()} ===")
    state = load_state()
    seeding = not state.get("_seeded", False)
    if seeding:
        print("· first run — seeding state, no notification emails will be sent today")

    # Maintenance digest doesn't use state — sends every morning if any open tickets
    try:
        send_maintenance_digest()
    except Exception as e:
        print(f"  ! maintenance digest failed: {e}")

    try:
        send_stale_invoice_emails(state, seeding)
    except Exception as e:
        print(f"  ! stale invoice emails failed: {e}")

    try:
        send_ap_overdue_emails(state, seeding)
    except Exception as e:
        print(f"  ! A/P overdue emails failed: {e}")

    state["_seeded"] = True
    save_state(state)
    print("✓ done")


if __name__ == "__main__":
    main()
