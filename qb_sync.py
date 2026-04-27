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
REALM_ID      = os.environ.get("QB_REALM_ID", "")
ENC_KEY       = os.environ.get("QB_TOKEN_ENC_KEY", "").strip()
TOKEN_FILE    = "data/qb_refresh_token.enc"

# ─── Encrypted refresh-token store ───────────────────────────────────────────
# Intuit rotates the refresh token on each use. To keep the chain alive across
# runs without manual intervention, we encrypt the current token to a file in
# the repo using QB_TOKEN_ENC_KEY (a stable Fernet key set as a GitHub secret).
# Each successful sync decrypts → uses → encrypts the new one back. The repo
# only ever sees the ciphertext; the key never leaves GitHub secrets.

def _load_persisted_token():
    if not ENC_KEY or not os.path.exists(TOKEN_FILE):
        return None
    try:
        from cryptography.fernet import Fernet
        with open(TOKEN_FILE, "rb") as fh:
            return Fernet(ENC_KEY.encode()).decrypt(fh.read()).decode().strip()
    except Exception as e:
        print(f"  ! could not decrypt {TOKEN_FILE}: {e}")
        return None

def _save_persisted_token(token):
    if not ENC_KEY:
        return False
    try:
        from cryptography.fernet import Fernet
        os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
        with open(TOKEN_FILE, "wb") as fh:
            fh.write(Fernet(ENC_KEY.encode()).encrypt(token.encode()))
        return True
    except Exception as e:
        print(f"  ! could not write {TOKEN_FILE}: {e}")
        return False

# Persisted (rotated) token wins over the env-var bootstrap.
REFRESH_TOKEN = _load_persisted_token() or os.environ.get("QB_REFRESH_TOKEN", "")

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
    # Intuit rotates the refresh token on every successful exchange. Persist
    # the new value to data/qb_refresh_token.enc so the next run uses it.
    # Without this the chain dies after enough rotations and we get locked out.
    new_rt = t.get("refresh_token")
    if new_rt and new_rt != REFRESH_TOKEN:
        if _save_persisted_token(new_rt):
            print(f"  ✓ persisted rotated refresh token (first 12 chars: {new_rt[:12]}…)")
        else:
            print("  ! Intuit issued a new refresh token but it could NOT be persisted — "
                  "set QB_TOKEN_ENC_KEY secret to enable auto-rotation.")
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

PROJECT_NUM_RE = re.compile(r"\b(2\d{3})\b")

def extract_project_number(*name_parts):
    """Find a 4-digit HHH project number in any of the given strings."""
    for s in name_parts:
        if not s: continue
        m = PROJECT_NUM_RE.search(str(s))
        if m: return m.group(1)
    return None

def pull_qb_lookups(token):
    """Pull all Customers + Vendors from QBO and build id→project_number maps.

    QBO sub-customers (Jobs) and Projects both live under the Customer entity.
    Their FullyQualifiedName includes the parent's name (e.g. "Dean Steel - 2562:
    House A"), so a single regex over FQN catches projects regardless of whether
    the project number is on the parent, the sub, or both.

    For invoices/bills whose CustomerRef/VendorRef ID matches a row here, we get
    a project number for free — no doc-number convention or fuzzy match needed.
    """
    customer_to_project = {}
    vendor_to_project   = {}
    try:
        customers = qb_query(token, "SELECT * FROM Customer")
        for c in customers:
            cid = c.get("Id")
            fqn  = c.get("FullyQualifiedName") or ""
            disp = c.get("DisplayName") or ""
            comp = c.get("CompanyName") or ""
            pnum = extract_project_number(fqn, disp, comp)
            if cid and pnum:
                customer_to_project[cid] = pnum
    except Exception as e:
        print(f"  ! failed to pull Customers: {e}")
    try:
        vendors = qb_query(token, "SELECT * FROM Vendor")
        for v in vendors:
            vid = v.get("Id")
            disp = v.get("DisplayName") or ""
            comp = v.get("CompanyName") or ""
            pnum = extract_project_number(disp, comp)
            if vid and pnum:
                vendor_to_project[vid] = pnum
    except Exception as e:
        print(f"  ! failed to pull Vendors: {e}")
    return customer_to_project, vendor_to_project

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

    customer_to_project, vendor_to_project = pull_qb_lookups(token)
    print(f"✓ {len(customer_to_project)} customers · {len(vendor_to_project)} vendors auto-mapped to projects")

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

    # Load manual overrides (highest priority)
    overrides = {"invoice_overrides":{}, "bill_overrides":{},
                 "customer_project":{}, "vendor_project":{}}
    ignore = {"ignore_invoice_ids": set(), "ignore_bill_ids": set(),
              "ignore_customer_patterns": [], "ignore_vendor_patterns": []}
    if os.path.exists("data/qb_matches.json"):
        with open("data/qb_matches.json") as f:
            saved = json.load(f)
        for k in overrides:
            if isinstance(saved.get(k), dict):
                overrides[k] = saved[k]
        for k in ("ignore_invoice_ids", "ignore_bill_ids"):
            if isinstance(saved.get(k), list):
                ignore[k] = {str(x) for x in saved[k]}
        for k in ("ignore_customer_patterns", "ignore_vendor_patterns"):
            if isinstance(saved.get(k), list):
                ignore[k] = [str(p) for p in saved[k] if p]

    def should_ignore(record_id, party_name, kind):
        id_set     = ignore["ignore_invoice_ids"] if kind == "invoice" else ignore["ignore_bill_ids"]
        patterns   = ignore["ignore_customer_patterns"] if kind == "invoice" else ignore["ignore_vendor_patterns"]
        if record_id is not None and str(record_id) in id_set:
            return True
        nm = (party_name or "").lower()
        return any(p.lower() in nm for p in patterns)

    def match_project(doc_number, fallback_name, record_id=None, record_kind="invoice",
                      party_id=None):
        """Five-tier matching:
          1. Exact record-ID override (manual: invoice_overrides / bill_overrides)
          2. DocNumber prefix (HHH convention: 'NNNN-...' or 'NNNN-<letter><NN>')
          3. Customer/vendor name-to-project override (manual)
          4. QBO customer/vendor ID → project (auto from Customer/Vendor names)
          5. Customer/vendor name heuristic (company name substring fallback)
        """
        # (1) Manual record-ID override
        id_map_key = "invoice_overrides" if record_kind == "invoice" else "bill_overrides"
        if record_id and str(record_id) in overrides.get(id_map_key, {}):
            pnum = str(overrides[id_map_key][str(record_id)])
            if pnum in proj_by_number:
                return proj_by_number[pnum][0], pnum, None

        # (2) DocNumber
        pnum, house = parse_doc_number(doc_number)
        if pnum and pnum in proj_by_number:
            candidates = proj_by_number[pnum]
            if house:
                for p in candidates:
                    if f"[House {house}]" in p["name"] or f" — Phase" in p["name"] and house in p["name"]:
                        return p, pnum, house
            return candidates[0], pnum, house

        # (3) Manual customer/vendor name-to-project override
        name_map_key = "customer_project" if record_kind == "invoice" else "vendor_project"
        for pattern, pnum in overrides.get(name_map_key, {}).items():
            if pattern and pattern.lower() in (fallback_name or "").lower():
                if str(pnum) in proj_by_number:
                    return proj_by_number[str(pnum)][0], str(pnum), None

        # (4) QBO customer/vendor ID → project (auto)
        id_map = customer_to_project if record_kind == "invoice" else vendor_to_project
        if party_id and str(party_id) in id_map:
            pnum = id_map[str(party_id)]
            if pnum in proj_by_number:
                return proj_by_number[pnum][0], pnum, None

        # (5) Company/vendor name heuristic (fuzzy)
        cn = normalize(fallback_name)
        for company, projs in proj_by_company.items():
            if company and (company in cn or cn in company):
                return projs[0], None, None
        return None, None, None

    # ── A/R status per project per month (matched by invoice DocNumber) ──
    ar_status = defaultdict(lambda: defaultdict(list))
    unmatched_invoices = []
    ignored_invoices = 0
    for inv in invoices:
        cust_ref = inv.get("CustomerRef") or {}
        cust_name = cust_ref.get("name", "")
        cust_id   = cust_ref.get("value")
        if should_ignore(inv.get("Id"), cust_name, "invoice"):
            ignored_invoices += 1
            continue
        matched, pnum, house = match_project(
            inv.get("DocNumber"), cust_name,
            record_id=inv.get("Id"), record_kind="invoice",
            party_id=cust_id)
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
                "invoice_id": inv.get("Id"),
                "customer":   cust_name,
                "date":       inv.get("TxnDate"),
                "amount":     rental,
                "deposit":    deposit,
                "total_amt":  float(inv.get("TotalAmt", 0) or 0),
                "balance":    float(inv.get("Balance",  0) or 0),
                "status":     status_for(inv, today),
                "doc_number": inv.get("DocNumber"),
            })

    # ── A/P status per project per month (matched by bill DocNumber) ──
    ap_status = defaultdict(lambda: defaultdict(list))
    ap_bills = []
    unmatched_bills = []
    ignored_bills = 0
    for bill in bills:
        v_ref = bill.get("VendorRef") or {}
        vendor = v_ref.get("name", "")
        v_id   = v_ref.get("value")
        if should_ignore(bill.get("Id"), vendor, "bill"):
            ignored_bills += 1
            continue
        matched, pnum, house = match_project(
            bill.get("DocNumber"), vendor,
            record_id=bill.get("Id"), record_kind="bill",
            party_id=v_id)
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
                "bill_id":    bill.get("Id"),
                "vendor":     vendor,
                "date":       bill.get("TxnDate"),
                "amount":     rental,
                "deposit":    deposit,
                "total_amt":  float(bill.get("TotalAmt", 0) or 0),
                "balance":    float(bill.get("Balance",  0) or 0),
                "status":     status_for(bill, today),
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
            "invoices_ignored":   ignored_invoices,
            "invoices_matched":   len(invoices) - len(unmatched_invoices) - ignored_invoices,
            "invoices_unmatched": len(unmatched_invoices),
            "bills_total":        len(bills),
            "bills_ignored":      ignored_bills,
            "bills_matched":      len(bills) - len(unmatched_bills) - ignored_bills,
            "bills_unmatched":    len(unmatched_bills),
        },
    }
    os.makedirs("data", exist_ok=True)
    with open("data/qb_status.json","w") as f:
        json.dump(out, f, indent=2, default=str)

    matched_inv = len(invoices) - len(unmatched_invoices) - ignored_invoices
    matched_bill = len(bills) - len(unmatched_bills) - ignored_bills
    print(f"\n✓ A/R matched: {len(ar_status)} projects, {matched_inv}/{len(invoices)} invoices"
          f" ({ignored_invoices} ignored)")
    print(f"✓ A/P matched: {len(ap_status)} projects, {matched_bill}/{len(bills)} bills"
          f" ({ignored_bills} ignored)")
    print(f"  {len(unmatched_invoices)} invoices + {len(unmatched_bills)} bills unmatched (see qb_status.json)")
    print("  Output → data/qb_status.json")

if __name__ == "__main__":
    main()
