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

DISCOVERY_URL = "https://developer.api.intuit.com/.well-known/openid_configuration"

def _fetch_token_endpoint():
    """Pull the current token endpoint from Intuit's OpenID discovery document."""
    try:
        r = requests.get(DISCOVERY_URL, timeout=10)
        r.raise_for_status()
        return r.json().get("token_endpoint") or \
               "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    except Exception:
        return "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

TOKEN_URL = _fetch_token_endpoint()

# Dev credentials connect to sandbox companies; prod credentials connect to real companies.
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
            tid = r.headers.get("intuit_tid", "none")
            print(f"  ! QB error {r.status_code} [intuit_tid: {tid}]: {r.text[:200]}")
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
    if " - " in name:
        company = name.split(" - ")[0]
    else:
        company = name
    company = re.sub(r"\s*\(.*?\)\s*", " ", company)
    return normalize(company)

# Invoice / bill doc_number patterns:
#   "2506-01"         → project 2506, no house
#   "2506-E01"        → project 2506, house E
#   "2506-A-01"       → project 2506, house A
#   "2506 A1"         → project 2506, house A
DOC_NUM_RE = re.compile(r"^\s*(\d{4})[\s\-]*([A-Z])?", re.I)

def parse_doc_number(doc_num):
    """Extract (project_number, house_letter) from an invoice/bill doc number."""
    if not doc_num: return (None, None)
    m = DOC_NUM_RE.match(str(doc_num).strip())
    if not m: return (None, None)
    return (m.group(1), (m.group(2) or "").upper() or None)

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

# Keywords that identify a security-deposit line item (case-insensitive).
# HHH wants A/R and A/P to include rent + cleaning + fees but NOT deposits.
DEPOSIT_KEYWORDS = ("security deposit", "sec deposit", "sec dep", "deposit",
                    "refundable deposit")

def is_deposit_line(line):
    """True if this invoice/bill line item looks like a security deposit."""
    # Combine everything we might see the word "deposit" in
    desc = (line.get("Description") or "").lower()
    detail = line.get("SalesItemLineDetail") or line.get("AccountBasedExpenseLineDetail") or {}
    item_name   = ((detail.get("ItemRef")    or {}).get("name") or "").lower()
    acct_name   = ((detail.get("AccountRef") or {}).get("name") or "").lower()
    blob = " ".join([desc, item_name, acct_name])
    return any(kw in blob for kw in DEPOSIT_KEYWORDS)

def rental_total(invoice_or_bill):
    """Return the TOTAL minus any security-deposit line items."""
    total    = float(invoice_or_bill.get("TotalAmt", 0) or 0)
    lines    = invoice_or_bill.get("Line") or []
    deposits = 0.0
    for ln in lines:
        if ln.get("DetailType") == "SubTotalLineDetail": continue
        if is_deposit_line(ln):
            deposits += float(ln.get("Amount", 0) or 0)
    return round(total - deposits, 2), round(deposits, 2)

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

    # Index projects by their project number (2xxx) for DocNumber matching
    proj_by_number = defaultdict(list)
    for p in projects:
        num = p.get("project_number") or ""
        if num: proj_by_number[num].append(p)
    # Also index by company name as a fallback for invoices without a proper doc number
    proj_by_company = defaultdict(list)
    for p in projects:
        c = project_company(p["name"])
        if c: proj_by_company[c].append(p)

    def match_project(doc_number, fallback_name):
        """Prefer DocNumber (HHH convention: 'NNNN-...' or 'NNNN-<letter><NN>').
        Fall back to customer/vendor name match."""
        pnum, house = parse_doc_number(doc_number)
        if pnum and pnum in proj_by_number:
            candidates = proj_by_number[pnum]
            # If a house letter was parsed AND we have a row matching it, prefer that
            if house:
                for p in candidates:
                    if f"[House {house}]" in p["name"] or f" — Phase" in p["name"] and house in p["name"]:
                        return p, pnum, house
            # Otherwise pick the first candidate for the project number
            return candidates[0], pnum, house
        # Fall back to company/vendor name
        cn = normalize(fallback_name)
        for company, projs in proj_by_company.items():
            if company and (company in cn or cn in company):
                return projs[0], None, None
        return None, None, None

    # ── A/R status per project per month (matched by invoice DocNumber) ──
    ar_status = defaultdict(lambda: defaultdict(list))
    unmatched_invoices = []
    for inv in invoices:
        cust_name = (inv.get("CustomerRef") or {}).get("name", "")
        matched, pnum, house = match_project(inv.get("DocNumber"), cust_name)
        mk = month_key(inv.get("TxnDate",""))
        rental, deposit = rental_total(inv)
        if matched and mk:
            ar_status[matched["name"]][mk].append({
                "status":     status_for(inv, today),
                "amount":     rental,           # rent + cleaning + fees, EXCLUDING security deposits
                "deposit":    deposit,          # separate for reference
                "total_amt":  float(inv.get("TotalAmt", 0) or 0),
                "balance":    float(inv.get("Balance",  0) or 0),
                "invoice_id": inv.get("Id"),
                "doc_number": inv.get("DocNumber"),
                "due_date":   inv.get("DueDate"),
                "project_number": pnum,
                "house":      house,
            })
        else:
            unmatched_invoices.append({
                "customer": cust_name, "date": inv.get("TxnDate"),
                "amount": rental, "deposit": deposit,
                "doc_number": inv.get("DocNumber"),
            })

    # ── A/P status per project per month (matched by bill DocNumber) ──
    ap_status = defaultdict(lambda: defaultdict(list))
    ap_bills = []
    unmatched_bills = []
    for bill in bills:
        vendor = (bill.get("VendorRef") or {}).get("name", "")
        matched, pnum, house = match_project(bill.get("DocNumber"), vendor)
        mk = month_key(bill.get("TxnDate",""))
        rental, deposit = rental_total(bill)
        record = {
            "vendor":     vendor,
            "amount":     rental,           # rent + cleaning + fees, EXCLUDING security deposits
            "deposit":    deposit,          # separate for reference
            "total_amt":  float(bill.get("TotalAmt", 0) or 0),
            "balance":    float(bill.get("Balance",  0) or 0),
            "txn_date":   bill.get("TxnDate"),
            "due_date":   bill.get("DueDate"),
            "status":     status_for(bill, today),
            "bill_id":    bill.get("Id"),
            "doc_number": bill.get("DocNumber"),
            "project_number": pnum,
            "house":      house,
        }
        ap_bills.append(record)
        if matched and mk:
            ap_status[matched["name"]][mk].append(record)
        else:
            unmatched_bills.append({
                "vendor": vendor, "date": bill.get("TxnDate"),
                "amount": rental, "deposit": deposit,
                "doc_number": bill.get("DocNumber"),
            })

    out = {
        "generated_at": datetime.now().isoformat(),
        "realm_id": REALM_ID,
        "ar_status_by_project": {k: dict(v) for k, v in ar_status.items()},
        "ap_status_by_project": {k: dict(v) for k, v in ap_status.items()},
        "ap_bills": ap_bills,
        "unmatched_invoices": unmatched_invoices,
        "unmatched_bills":    unmatched_bills,
        "summary": {
            "invoices_total":     len(invoices),
            "invoices_matched":   len(invoices) - len(unmatched_invoices),
            "invoices_unmatched": len(unmatched_invoices),
            "bills_total":        len(bills),
            "bills_matched":      len(bills) - len(unmatched_bills),
            "bills_unmatched":    len(unmatched_bills),
        },
    }
    os.makedirs("data", exist_ok=True)
    with open("data/qb_status.json","w") as f:
        json.dump(out, f, indent=2, default=str)

    print(f"\n✓ A/R matched: {len(ar_status)} projects, {len(invoices)-len(unmatched_invoices)}/{len(invoices)} invoices")
    print(f"✓ A/P matched: {len(ap_status)} projects, {len(bills)-len(unmatched_bills)}/{len(bills)} bills")
    print(f"  {len(unmatched_invoices)} invoices + {len(unmatched_bills)} bills unmatched (see qb_status.json)")
    print("  Output → data/qb_status.json")

if __name__ == "__main__":
    main()
