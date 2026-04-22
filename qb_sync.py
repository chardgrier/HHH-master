#!/usr/bin/env python3
"""
QuickBooks Online → data/qb_status.json

Reads invoices (A/R) and bills (A/P) from your QB company and produces a
per-project-per-month status file the dashboard displays as icons.

Status values:
  • "paid"     — invoice/bill fully paid
  • "sent"     — invoice sent or bill recorded, not paid yet, not overdue
  • "overdue"  — unpaid past due date
  • "none"     — no invoice/bill on file for that project+month

Matching heuristic: we extract the construction-company name from the
Asana project name (e.g. "Miller Pipeline" from "Miller Pipeline (Artera) -
Saint Peters, MO - Paul - 2506") and fuzzy-match it to QB Customer names.
Similarly for homeowner bills → Vendor names. Anything we can't match
gets flagged in the output for manual review.
"""
import os, sys, re, json, base64
from datetime import date, datetime
from collections import defaultdict

try:
    import requests
except ImportError:
    os.system("pip3 install requests -q --user"); import requests

CLIENT_ID     = os.environ.get("QB_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("QB_CLIENT_SECRET", "")
REFRESH_TOKEN = os.environ.get("QB_REFRESH_TOKEN", "")
REALM_ID      = os.environ.get("QB_REALM_ID", "")

if not all([CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, REALM_ID]):
    print("ERROR: QB_CLIENT_ID, QB_CLIENT_SECRET, QB_REFRESH_TOKEN, QB_REALM_ID required")
    sys.exit(1)

TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
# Dev credentials connect to sandbox companies; prod credentials connect to real companies.
# Allow env var override so we can switch later if the user gets production keys.
QB_ENV = os.environ.get("QB_ENV", "sandbox").lower()
API_BASE = (
    f"https://sandbox-quickbooks.api.intuit.com/v3/company/{REALM_ID}"
    if QB_ENV == "sandbox"
    else f"https://quickbooks.api.intuit.com/v3/company/{REALM_ID}"
)

# ─── Access token refresh ─────────────────────────────────────────────────────

def get_access_token():
    basic = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    r = requests.post(TOKEN_URL,
        headers={"Authorization": f"Basic {basic}",
                 "Accept": "application/json",
                 "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type":"refresh_token","refresh_token": REFRESH_TOKEN},
        timeout=30)
    r.raise_for_status()
    t = r.json()
    # Note: Intuit issues a new refresh_token with each access_token response.
    # We don't persist it here since GitHub secrets can't be updated from within
    # the workflow. Refresh tokens remain valid as long as they're used within
    # 100 days, so the nightly run keeps ours alive.
    if t.get("refresh_token") and t["refresh_token"] != REFRESH_TOKEN:
        print(f"  (note: received a new refresh token from Intuit — first 12 chars: {t['refresh_token'][:12]}…)")
    return t["access_token"]

# ─── QB query helper ─────────────────────────────────────────────────────────

def qb_query(access_token, sql):
    """Run a QB SQL-like query and return the list of records."""
    results = []
    start = 1
    per_page = 500
    while True:
        paged = f"{sql} STARTPOSITION {start} MAXRESULTS {per_page}"
        r = requests.get(f"{API_BASE}/query",
            headers={"Authorization": f"Bearer {access_token}", "Accept":"application/json"},
            params={"query": paged}, timeout=30)
        if r.status_code >= 300:
            print(f"  ! QB error {r.status_code}: {r.text[:200]}")
            break
        d = r.json().get("QueryResponse", {})
        # QB returns data under a key named after the entity type
        batch = []
        for k, v in d.items():
            if isinstance(v, list):
                batch = v; break
        if not batch: break
        results.extend(batch)
        if len(batch) < per_page: break
        start += per_page
    return results

# ─── Name normalisation / matching ────────────────────────────────────────────

def normalize(s):
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()

def project_company(name):
    """Extract the construction company name from an Asana project name."""
    # Typical: "Miller Pipeline (Artera) - Saint Peters, MO - Paul - 2506"
    # Take everything up to the first " - " (if present)
    if " - " in name:
        company = name.split(" - ")[0]
    else:
        company = name
    # Drop parenthesised parts
    company = re.sub(r"\s*\(.*?\)\s*", " ", company)
    return normalize(company)

def status_for(invoice, today):
    """Return 'paid' | 'sent' | 'overdue' given a QB Invoice/Bill record."""
    balance = float(invoice.get("Balance", 0) or 0)
    due_str = invoice.get("DueDate") or invoice.get("TxnDate")
    due = None
    try: due = date.fromisoformat(due_str) if due_str else None
    except: pass
    if balance <= 0.001: return "paid"
    if due and due < today: return "overdue"
    return "sent"

def month_key(txn_date_str):
    """'2026-04-15' → '2026-04'"""
    if not txn_date_str: return None
    return txn_date_str[:7]

# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print(f"=== QB Sync @ {datetime.now():%Y-%m-%d %H:%M} ===")
    token = get_access_token()
    print("✓ Got access token")

    invoices = qb_query(token, "SELECT * FROM Invoice")
    bills    = qb_query(token, "SELECT * FROM Bill")
    print(f"✓ {len(invoices)} invoices · {len(bills)} bills loaded")

    today = date.today()

    # Load Asana project list (for matching)
    with open("data/projects.json") as f:
        projects = json.load(f)["projects"]

    # Build company→project map (heuristic)
    proj_by_company = defaultdict(list)
    for p in projects:
        c = project_company(p["name"])
        if c: proj_by_company[c].append(p)

    # ── A/R status per project per month ──
    ar_status = defaultdict(lambda: defaultdict(list))   # project_name → month → [invoice records]
    unmatched_invoices = []
    for inv in invoices:
        cust_name = (inv.get("CustomerRef") or {}).get("name", "")
        cn = normalize(cust_name)
        matched = None
        for company, projs in proj_by_company.items():
            if company and (company in cn or cn in company):
                if len(projs) == 1:
                    matched = projs[0]; break
                # Ambiguous — pick by matching location/number if possible
                matched = projs[0]; break
        mk = month_key(inv.get("TxnDate",""))
        if matched and mk:
            ar_status[matched["name"]][mk].append({
                "status":   status_for(inv, today),
                "amount":   float(inv.get("TotalAmt", 0) or 0),
                "balance":  float(inv.get("Balance",  0) or 0),
                "invoice_id": inv.get("Id"),
                "doc_number": inv.get("DocNumber"),
                "due_date": inv.get("DueDate"),
            })
        else:
            unmatched_invoices.append({"customer": cust_name, "date": inv.get("TxnDate"),
                                        "amount": float(inv.get("TotalAmt",0) or 0),
                                        "doc_number": inv.get("DocNumber")})

    # ── A/P status per project per month ──
    # For vendors (homeowners), we'd need an explicit mapping between QB vendor
    # names and specific project homeowner leases. For now, we just record the
    # bill by vendor and leave matching for a later enhancement.
    ap_bills = []
    for bill in bills:
        vendor = (bill.get("VendorRef") or {}).get("name", "")
        ap_bills.append({
            "vendor":    vendor,
            "amount":    float(bill.get("TotalAmt", 0) or 0),
            "balance":   float(bill.get("Balance",  0) or 0),
            "txn_date":  bill.get("TxnDate"),
            "due_date":  bill.get("DueDate"),
            "status":    status_for(bill, today),
            "bill_id":   bill.get("Id"),
            "doc_number": bill.get("DocNumber"),
        })

    out = {
        "generated_at": datetime.now().isoformat(),
        "realm_id": REALM_ID,
        "ar_status_by_project": {k: dict(v) for k, v in ar_status.items()},
        "ap_bills": ap_bills,
        "unmatched_invoices": unmatched_invoices,
        "summary": {
            "invoices_total":      len(invoices),
            "invoices_matched":    sum(len(v) for v in ar_status.values() for v in v.values() if isinstance(v, list)),
            "invoices_unmatched":  len(unmatched_invoices),
            "bills_total":         len(bills),
        },
    }
    os.makedirs("data", exist_ok=True)
    with open("data/qb_status.json","w") as f:
        json.dump(out, f, indent=2, default=str)

    print(f"\n✓ A/R matched to {len(ar_status)} projects")
    print(f"✓ {len(unmatched_invoices)} invoices couldn't be matched to a project (see qb_status.json → unmatched_invoices)")
    print(f"✓ {len(bills)} bills recorded (homeowner→project matching TBD)")
    print("  Output → data/qb_status.json")

if __name__ == "__main__":
    main()
