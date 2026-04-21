#!/usr/bin/env python3
"""
Populate HHH custom fields on Homeowner Lease tasks and Construction/Homeowner
Addendum tasks by parsing their note text.

For each lease task we compute Monthly A/P (or A/R) as:
    rent + cleaning fee + any listed other fees  (security deposit EXCLUDED)

For addendum tasks we extract the extension end date and the stated new rate,
which will override the parent lease's end date and rate on the dashboard.
"""
import os, re, json, sys, time
import requests

TOK = os.environ.get("ASANA_TOKEN", "")
if not TOK: print("ERROR: ASANA_TOKEN not set"); sys.exit(1)

WS  = "1203487090849714"
BASE = "https://app.asana.com/api/1.0"
H = {"Authorization": f"Bearer {TOK}"}

with open("data/custom_field_gids.json") as f: CF = json.load(f)

SALESPEOPLE = ["Paul","Zeke","Matt","Logan","David","Charlie","Peyton"]
HHH_PATTERN = re.compile(rf"\b({'|'.join(SALESPEOPLE)})\b.*?\d{{4}}", re.I)
SKIP_NAMES = ["template","general to do","xxxx","hard hat housing template"]
VOID = ("did not send","did not use","cancelled","terminated","termination",
        "backup","back up","not used","duplicate","did not","unused")

# ─── HTTP helpers ─────────────────────────────────────────────────────────────

def http(method, endpoint, params=None, data=None):
    url = f"{BASE}/{endpoint}"
    while True:
        r = requests.request(method, url, headers=H, params=params, json=data, timeout=30)
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", 5))); continue
        return r

def paginate(endpoint, params=None):
    url = f"{BASE}/{endpoint}"
    out = []
    while url:
        r = requests.get(url, headers=H, params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", 5))); continue
        r.raise_for_status()
        d = r.json()
        out.extend(d.get("data", []))
        nxt = d.get("next_page")
        url = nxt["uri"] if nxt else None
        params = None
    return out

_attached = set()
def ensure_fields_on_project(pgid):
    if pgid in _attached: return
    r = http("GET", f"projects/{pgid}/custom_field_settings",
             params={"opt_fields":"custom_field.gid"})
    have = set()
    if r.status_code == 200:
        for s in r.json().get("data", []):
            have.add((s.get("custom_field") or {}).get("gid"))
    for name, gid in CF.items():
        if gid in have: continue
        http("POST", f"projects/{pgid}/addCustomFieldSetting",
             data={"data":{"custom_field": gid}})
    _attached.add(pgid)

# ─── Note parsing ─────────────────────────────────────────────────────────────

def parse_date(s):
    if not s: return None
    s = s.strip().rstrip(".")
    m = re.search(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})", s)
    if not m: return None
    mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if y < 100: y += 2000
    try:
        from datetime import date
        return date(y, mo, d).isoformat()
    except ValueError:
        return None

def parse_dates_range(text):
    """Find a date range like MM/DD/YY - MM/DD/YY in text."""
    m = re.search(
        r"(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})\s*(?:thru|through|to|[-–]+)\s*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})",
        text, re.I)
    if not m: return None, None
    return parse_date(m.group(1)), parse_date(m.group(2))

def clean_num(s):
    """Parse '$1,200.50' / '1200.50' / '$3,595/mo' into float 1200.50."""
    if s is None: return None
    m = re.search(r"(\d[\d,]*(?:\.\d{1,2})?)", s.replace(",", ""))
    if not m: return None
    try:
        v = float(m.group(1))
        return v if 10 < v < 500_000 else None
    except ValueError:
        return None

def extract_first_amount(text, *labels, reject=("yearly","annual","daily","per year")):
    """Find the first $amount that appears after any of the given labels."""
    if not text: return None
    for label in labels:
        pat = re.compile(rf"{re.escape(label)}\s*[:\-]?\s*\$?([\d,]+(?:\.\d{{1,2}})?)", re.I)
        for m in pat.finditer(text):
            # look at the next 80 chars for yearly/daily context
            window = text[m.start():m.end()+80].lower()
            if any(r in window for r in reject):
                continue
            v = clean_num(m.group(1))
            if v: return v
    return None

def parse_lease_components(notes):
    """Return (monthly_total, components_dict) for a lease task's notes.

    monthly_total = rent + cleaning + other_fees  (deposit EXCLUDED)
    """
    if not notes: return None, {}

    # Drop parentheses that contain yearly/daily/annual figures
    cleaned = re.sub(r"\(.*?(?:yearly|daily|annual|per year).*?\)", "", notes, flags=re.I)

    rent     = extract_first_amount(cleaned, "RENT PER MONTH", "Rent per Month", "Monthly Rent", "Rate")
    cleaning = extract_first_amount(cleaned, "CLEANING FEE", "Cleaning Fee", "Monthly Cleaning")
    other    = extract_first_amount(cleaned, "OTHER FEES", "Other Fees", "Additional Fees",
                                             "Monthly Utilities", "Utilities Fee")
    deposit  = extract_first_amount(cleaned, "SECURITY DEPOSIT", "Security Deposit")

    if rent is None: return None, {}

    total = rent + (cleaning or 0) + (other or 0)
    return total, {"rent": rent, "cleaning": cleaning, "other": other, "deposit": deposit}

def parse_addendum_dates_rate(notes):
    """Return (new_end_date, new_monthly_rate) from an addendum's notes."""
    if not notes: return None, None
    new_end = None; new_rate = None

    # Look for "Extension #N: dates" or "Extended to: date"
    for m in re.finditer(
        r"(?:extension\s*#?\d*|extended(?:\s+to)?)\s*:?\s*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})\s*(?:thru|through|to|[-–]+)\s*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})",
        notes, re.I):
        d = parse_date(m.group(2))
        if d and (new_end is None or d > new_end):
            new_end = d
    if not new_end:
        # fallback: any date range in notes
        _, new_end = parse_dates_range(notes)

    # Parse a new monthly rate if stated
    total, _comps = parse_lease_components(notes)
    if total: new_rate = total

    return new_end, new_rate

# ─── Task classification ──────────────────────────────────────────────────────

def classify_task(name):
    """Return one of: 'construction_lease','homeowner_lease',
                     'construction_addendum','homeowner_addendum', or None."""
    n = name.lower()
    if any(v in n for v in VOID): return None
    # Prefixed patterns (A: Construction Lease, B: Homeowner Addendum, etc.)
    for kind, rx in [
        ("construction_addendum", r"construction\s+addendum"),
        ("homeowner_addendum",    r"homeowner\s+addendum"),
        ("construction_lease",    r"construction\s+lease"),
        ("homeowner_lease",       r"homeowner\s+lease"),
    ]:
        if re.search(rx, n):
            return kind
    return None

# ─── Set custom fields (additive, non-destructive) ───────────────────────────

def set_fields(task_gid, values):
    body = {"data": {"custom_fields": values}}
    r = http("PUT", f"tasks/{task_gid}", data=body)
    return (r.status_code < 300, r.text[:150] if r.status_code >= 300 else None)

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    ar_gid    = CF["HHH: Monthly A/R"]
    ap_gid    = CF["HHH: Monthly A/P"]
    start_gid = CF["HHH: Start Date"]
    end_gid   = CF["HHH: End Date"]

    # 1. Get all HHH-pattern projects
    projects = []
    for p in paginate("projects", {"workspace": WS, "archived":"false",
                                   "opt_fields":"name","limit":100}):
        nm = p.get("name","")
        if not HHH_PATTERN.search(nm): continue
        if any(s in nm.lower() for s in SKIP_NAMES): continue
        projects.append(p)
    print(f"Scanning {len(projects)} HHH projects…")

    totals = {"homeowner_leases": 0, "homeowner_addendums": 0,
              "construction_addendums": 0, "skipped": 0, "errors": 0}

    for proj in projects:
        pgid = proj["gid"]; pname = proj["name"]
        ensure_fields_on_project(pgid)

        tasks = paginate("tasks", {
            "project": pgid,
            "opt_fields": "name,notes,custom_fields.gid,custom_fields.number_value,"
                          "custom_fields.date_value",
            "limit": 100,
        })

        for t in tasks:
            tname = (t.get("name") or "").strip()
            notes = (t.get("notes") or "").strip()
            kind = classify_task(tname)
            if not kind: continue

            # Already populated? Skip if both money + dates already set.
            has_money_value = False
            has_start_value = False
            has_end_value   = False
            for cf in t.get("custom_fields", []):
                g = cf.get("gid")
                if g in (ar_gid, ap_gid) and cf.get("number_value") is not None:
                    has_money_value = True
                if g == start_gid and cf.get("date_value"):
                    has_start_value = True
                if g == end_gid and cf.get("date_value"):
                    has_end_value = True
            if has_money_value and has_start_value and has_end_value:
                totals["skipped"] += 1
                continue

            values = {}

            if kind in ("homeowner_lease", "construction_lease"):
                total, comps = parse_lease_components(notes)
                s, e = parse_dates_range(notes)
                money_gid = ap_gid if kind == "homeowner_lease" else ar_gid
                if total is not None and not has_money_value:
                    values[money_gid] = total
                if s and not has_start_value:
                    values[start_gid] = {"date": s}
                if e and not has_end_value:
                    values[end_gid] = {"date": e}

            elif kind in ("homeowner_addendum", "construction_addendum"):
                new_end, new_rate = parse_addendum_dates_rate(notes)
                money_gid = ap_gid if kind == "homeowner_addendum" else ar_gid
                # Addendum's rate goes into its own A/R or A/P field
                if new_rate is not None and not has_money_value:
                    values[money_gid] = new_rate
                # Addendum's start date = end of previous lease period (use its earlier date in range)
                s_date, e_date = parse_dates_range(notes)
                # for an "extension", the addendum's own start is typically the earlier of the range
                if s_date and not has_start_value:
                    values[start_gid] = {"date": s_date}
                if new_end and not has_end_value:
                    values[end_gid] = {"date": new_end}

            if not values:
                continue

            ok, err = set_fields(t["gid"], values)
            if ok:
                totals[{
                    "homeowner_lease": "homeowner_leases",
                    "construction_lease": "construction_addendums",  # shouldn't happen (done earlier)
                    "homeowner_addendum": "homeowner_addendums",
                    "construction_addendum": "construction_addendums",
                }[kind]] += 1
                print(f"  ✓ [{pname[:35]:<35}] {tname[:45]:<45}  {kind}")
            else:
                totals["errors"] += 1
                print(f"  ✗ [{pname[:35]:<35}] {tname[:40]}: {err}")

    print("\n" + "="*70)
    print("  Homeowner leases populated:      ", totals["homeowner_leases"])
    print("  Homeowner addendums populated:   ", totals["homeowner_addendums"])
    print("  Construction addendums populated:", totals["construction_addendums"])
    print("  Skipped (already complete):      ", totals["skipped"])
    print("  Errors:                          ", totals["errors"])

if __name__ == "__main__":
    main()
