#!/usr/bin/env python3
"""
Hard Hat Housing - Master Dashboard Sync
Reads all projects from Asana portfolios, parses lease task notes,
and generates data/projects.json for the dashboard.
"""

import os, json, re, sys
from datetime import date
from calendar import monthrange

try:
    import requests
except ImportError:
    print("Installing requests..."); os.system("pip3 install requests -q")
    import requests

ASANA_TOKEN = os.environ.get("ASANA_TOKEN", "")
BASE_URL = "https://app.asana.com/api/1.0"
PORTFOLIO_GIDS = ["1206746778121935", "1207604445018695"]  # HHH + Rising Sun
COMMISSION_RATE = 0.20
SKIP_NAMES = ["template", "general to do", "xxxx", "hard hat housing template"]

# Dashboard months: Aug 2025 → Dec 2026
MONTHS = []
y, m = 2025, 8
while (y, m) <= (2026, 12):
    MONTHS.append((y, m))
    m += 1
    if m > 12: m = 1; y += 1

# ─── Asana API helpers ────────────────────────────────────────────────────────

def asana(endpoint, params=None):
    headers = {"Authorization": f"Bearer {ASANA_TOKEN}"}
    url = f"{BASE_URL}/{endpoint}"
    results = []
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 429:
            import time; time.sleep(int(r.headers.get("Retry-After", 5)))
            continue
        r.raise_for_status()
        data = r.json()
        body = data.get("data", [])
        if isinstance(body, list):
            results.extend(body)
        else:
            return body
        nxt = data.get("next_page")
        url = nxt["uri"] if nxt else None
        params = None
    return results

# ─── Parsing helpers ──────────────────────────────────────────────────────────

DATE_RE = re.compile(
    r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})"
)

def parse_date(s):
    if not s:
        return None
    s = s.strip().rstrip(".")
    m = DATE_RE.fullmatch(s)
    if not m:
        m = DATE_RE.search(s)
    if not m:
        return None
    mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if y < 100:
        y += 2000
    try:
        return date(y, mo, d)
    except ValueError:
        return None

def parse_date_pair(text):
    """Find first date range (start thru/- end) in text."""
    pattern = re.compile(
        r"(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})\s*(?:thru|through|to|\-{1,2}|–)\s*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})",
        re.IGNORECASE,
    )
    m = pattern.search(text)
    if m:
        return parse_date(m.group(1)), parse_date(m.group(2))
    return None, None

def parse_amount(text):
    """Extract largest plausible dollar amount from text."""
    amounts = re.findall(r"\$\s*([\d,]+(?:\.\d{1,2})?)", text)
    vals = []
    for a in amounts:
        try:
            v = float(a.replace(",", ""))
            if 100 < v < 500_000:
                vals.append(v)
        except ValueError:
            pass
    return max(vals) if vals else 0.0

def parse_crew(text):
    m = re.search(r"crew\s+size\s*:?\s*(\d+)", text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # Fallback: "N crew" or "crew of N"
    m = re.search(r"\b(\d+)\s+crew\b", text, re.IGNORECASE)
    return int(m.group(1)) if m else 0

def field(text, *names):
    """Extract value after 'FIELD_NAME: ...' from structured notes."""
    for name in names:
        p = re.compile(rf"{re.escape(name)}\s*:?\s*(.+?)(?:\n|$)", re.IGNORECASE)
        m = p.search(text)
        if m:
            return m.group(1).strip()
    return ""

def extension_end(notes):
    """Return the latest end date from any Extension #N lines."""
    ext_re = re.compile(
        r"extension\s*#?\d*\s*:?\s*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})\s*[-–to]+\s*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})",
        re.IGNORECASE,
    )
    latest = None
    latest_rate = None
    for m in ext_re.finditer(notes):
        d = parse_date(m.group(2))
        if d and (latest is None or d > latest):
            latest = d
            # Try to grab rate after this extension line
            after = notes[m.end():]
            rate_m = re.search(r"RATE\s*:?\s*\$?([\d,]+)", after[:200], re.IGNORECASE)
            if rate_m:
                try:
                    v = float(rate_m.group(1).replace(",", ""))
                    if 100 < v < 500_000:
                        latest_rate = v
                except ValueError:
                    pass
    return latest, latest_rate

# ─── Lease parsing ────────────────────────────────────────────────────────────

def parse_construction_lease(notes):
    if not notes:
        return None

    # Try structured fields first
    dates_text = field(notes, "PROJECT DATES", "Project Dates", "DATES")
    start, end = None, None
    if dates_text:
        start, end = parse_date_pair(dates_text)
    if not start:
        start, end = parse_date_pair(notes)

    rate_text = field(notes, "RENT PER MONTH", "Rent per Month", "RATE", "Rate")
    amount = parse_amount(rate_text) if rate_text else parse_amount(notes)
    crew = parse_crew(field(notes, "CREW SIZE", "Crew Size") or notes)

    if not start or not amount:
        return None

    return {
        "start_date": start.isoformat(),
        "end_date": end.isoformat() if end else None,
        "monthly_amount": amount,
        "crew_size": crew,
    }

def parse_homeowner_lease(notes):
    if not notes:
        return None

    dates_text = field(notes, "TIME FRAME", "Time Frame", "PROJECT DATES", "Project Dates", "DATES")
    start, end = None, None
    if dates_text:
        start, end = parse_date_pair(dates_text)
    if not start:
        start, end = parse_date_pair(notes)

    rate_text = field(notes, "RENT PER MONTH", "RATE", "Rate", "Monthly Rent")
    amount = parse_amount(rate_text) if rate_text else parse_amount(notes)

    if not start or not amount:
        return None

    return {
        "start_date": start.isoformat(),
        "end_date": end.isoformat() if end else None,
        "monthly_amount": amount,
    }

def apply_addendum(lease, notes):
    """Update a lease dict in-place based on addendum notes."""
    if not notes or not lease:
        return
    new_end, new_rate = extension_end(notes)
    if new_end:
        current = date.fromisoformat(lease["end_date"]) if lease.get("end_date") else None
        if current is None or new_end > current:
            lease["end_date"] = new_end.isoformat()
    if new_rate:
        lease["monthly_amount"] = new_rate

# ─── Project task processing ─────────────────────────────────────────────────

PREFIXED  = re.compile(r"^([A-Z])\s*:\s*(Construction Lease|Homeowner Lease|Construction Addendum|Homeowner Addendum)", re.IGNORECASE)
UNPREFIXED = re.compile(r"^(Construction Lease|Homeowner Lease|Construction Addendum|Homeowner Addendum)\b", re.IGNORECASE)

# Terms that mark a task as void / ignore
VOID_TERMS = ("did not send", "did not use", "cancelled", "termination", "terminated", "terminate",
              "cancellation", "back up", "backup", "not used", "duplicate")

def task_is_void(name):
    low = name.lower()
    return any(t in low for t in VOID_TERMS)

def process_tasks(tasks):
    """
    Given a list of Asana task dicts, extract house data.
    Returns dict: letter -> {construction, homeowner}
    """
    houses = {}

    for task in tasks:
        name = (task.get("name") or "").strip()
        notes = (task.get("notes") or "").strip()
        if not name or task_is_void(name):
            continue

        # Try prefixed match first: "A: Construction Lease"
        m = PREFIXED.match(name)
        if m:
            letter = m.group(1).upper()
            kind   = m.group(2).lower()
        else:
            # Unprefixed: "Construction Lease" → default letter A
            m2 = UNPREFIXED.match(name)
            if not m2:
                continue
            letter = "A"
            kind   = m2.group(1).lower()

        if letter not in houses:
            houses[letter] = {"letter": letter, "construction": None, "homeowner": None}

        if "construction lease" in kind:
            parsed = parse_construction_lease(notes)
            if parsed:
                # Only overwrite if we don't already have one (prefixed takes priority)
                if not houses[letter]["construction"]:
                    houses[letter]["construction"] = parsed

        elif "homeowner lease" in kind:
            parsed = parse_homeowner_lease(notes)
            if parsed and not houses[letter]["homeowner"]:
                houses[letter]["homeowner"] = parsed

        elif "construction addendum" in kind:
            apply_addendum(houses[letter].get("construction"), notes)

        elif "homeowner addendum" in kind:
            apply_addendum(houses[letter].get("homeowner"), notes)

    return houses

# ─── Monthly value calculation ────────────────────────────────────────────────

def monthly_value(lease, yr, mo):
    """Return pro-rated amount (and crew) for a lease in a given month."""
    if not lease or not lease.get("start_date") or not lease.get("monthly_amount"):
        return 0.0, 0

    start = date.fromisoformat(lease["start_date"])
    end_str = lease.get("end_date")
    end = date.fromisoformat(end_str) if end_str else date(2099, 12, 31)
    amount = lease["monthly_amount"]
    crew = lease.get("crew_size", 0)

    first = date(yr, mo, 1)
    last = date(yr, mo, monthrange(yr, mo)[1])

    if start > last or end < first:
        return 0.0, 0

    eff_start = max(start, first)
    eff_end = min(end, last)
    days = (eff_end - eff_start).days + 1

    if days <= 0:
        return 0.0, 0

    # Full month if covers entire month
    if eff_start == first and eff_end == last:
        return amount, crew

    # Pro-rate using 30-day convention
    return round(amount * days / 30, 2), crew

# ─── Metadata extraction ──────────────────────────────────────────────────────

SALESPEOPLE = ["Paul", "Zeke", "Matt", "Logan", "David", "Charlie", "Peyton"]

def extract_salesperson(name):
    for sp in SALESPEOPLE:
        if re.search(rf"\b{sp}\b", name, re.IGNORECASE):
            return sp
    return "Unknown"

def extract_number(name):
    m = re.search(r"\b(2\d{3})\b", name)
    return m.group(1) if m else ""

def project_status(start_str, end_str):
    today = date.today()
    start = date.fromisoformat(start_str) if start_str else None
    end = date.fromisoformat(end_str) if end_str else None
    if not start:
        return "Unknown"
    if start > today:
        return "Upcoming"
    if end and end < today:
        return "Closed"
    return "Active"

# ─── Main sync ────────────────────────────────────────────────────────────────

def sync():
    if not ASANA_TOKEN:
        print("ERROR: ASANA_TOKEN environment variable not set.")
        sys.exit(1)

    from datetime import datetime
    print(f"Starting sync at {datetime.now():%Y-%m-%d %H:%M}")

    # Collect all unique projects across portfolios
    all_projects = {}
    for pgid in PORTFOLIO_GIDS:
        try:
            items = asana(f"portfolios/{pgid}/items", {"opt_fields": "name,permalink_url"})
            for item in items:
                all_projects[item["gid"]] = item
        except Exception as e:
            print(f"  Warning: portfolio {pgid} error: {e}")

    print(f"Found {len(all_projects)} portfolio items")

    projects_data = []

    for gid, item in all_projects.items():
        pname = item.get("name", "")

        # Skip templates, meta-projects
        if any(skip in pname.lower() for skip in SKIP_NAMES):
            continue

        print(f"  → {pname}")

        try:
            tasks = asana("tasks", {
                "project": gid,
                "opt_fields": "name,notes",
                "limit": 100,
            })

            houses = process_tasks(tasks)
            if not houses:
                continue

            # Determine overall dates
            all_starts, all_ends = [], []
            for h in houses.values():
                cl = h.get("construction")
                if cl:
                    if cl.get("start_date"): all_starts.append(date.fromisoformat(cl["start_date"]))
                    if cl.get("end_date"):   all_ends.append(date.fromisoformat(cl["end_date"]))

            if not all_starts:
                continue

            start_date = min(all_starts).isoformat()
            end_date   = max(all_ends).isoformat() if all_ends else None

            # Base monthly values (sum of all leases at their standard rate)
            base_ar  = sum(h["construction"]["monthly_amount"] for h in houses.values() if h.get("construction"))
            base_ap  = sum(h["homeowner"]["monthly_amount"]    for h in houses.values() if h.get("homeowner"))
            base_crew = sum(h["construction"].get("crew_size", 0) for h in houses.values() if h.get("construction"))

            # Monthly breakdown
            monthly = {}
            for yr, mo in MONTHS:
                key = f"{yr}-{mo:02d}"
                ar, crew = 0.0, 0
                ap = 0.0
                for h in houses.values():
                    if h.get("construction"):
                        a, c = monthly_value(h["construction"], yr, mo)
                        ar += a; crew += c
                    if h.get("homeowner"):
                        a, _ = monthly_value(h["homeowner"], yr, mo)
                        ap += a
                if ar > 0 or ap > 0:
                    gp = ar - ap
                    monthly[key] = {
                        "crew": crew,
                        "ar":   round(ar, 2),
                        "ap":   round(ap, 2),
                        "net_gp": round(gp * (1 - COMMISSION_RATE), 2),
                    }

            # 2026 totals
            t_ar  = sum(v["ar"]     for k, v in monthly.items() if k.startswith("2026"))
            t_ap  = sum(v["ap"]     for k, v in monthly.items() if k.startswith("2026"))
            t_gp  = sum(v["net_gp"] for k, v in monthly.items() if k.startswith("2026"))

            projects_data.append({
                "gid": gid,
                "name": pname,
                "salesperson": extract_salesperson(pname),
                "project_number": extract_number(pname),
                "status": project_status(start_date, end_date),
                "start_date": start_date,
                "end_date": end_date,
                "base_ar": base_ar,
                "base_ap": base_ap,
                "base_crew": base_crew,
                "monthly": monthly,
                "total_2026": {"ar": round(t_ar,2), "ap": round(t_ap,2), "net_gp": round(t_gp,2)},
                "houses": {
                    lt: {"construction": h.get("construction"), "homeowner": h.get("homeowner")}
                    for lt, h in houses.items()
                },
            })

        except Exception as e:
            print(f"  ERROR on {pname}: {e}")

    # Monthly totals
    monthly_totals = {}
    for yr, mo in MONTHS:
        key = f"{yr}-{mo:02d}"
        ar   = sum(p["monthly"].get(key, {}).get("ar",     0) for p in projects_data)
        ap   = sum(p["monthly"].get(key, {}).get("ap",     0) for p in projects_data)
        crew = sum(p["monthly"].get(key, {}).get("crew",   0) for p in projects_data)
        gp   = (ar - ap) * (1 - COMMISSION_RATE)
        monthly_totals[key] = {
            "crew": crew,
            "ar":   round(ar, 2),
            "ap":   round(ap, 2),
            "net_gp": round(gp, 2),
            "gp_pct": round(gp / ar * 100 if ar else 0, 2),
        }

    # Salesperson summary
    sp_summary = {}
    for p in projects_data:
        sp = p["salesperson"]
        for key, v in p["monthly"].items():
            sp_summary.setdefault(sp, {}).setdefault(key, {"ar": 0, "ap": 0, "commission": 0})
            sp_summary[sp][key]["ar"] += v["ar"]
            sp_summary[sp][key]["ap"] += v["ap"]
            sp_summary[sp][key]["commission"] += round((v["ar"] - v["ap"]) * COMMISSION_RATE, 2)

    from datetime import datetime
    output = {
        "generated_at": datetime.now().isoformat(),
        "months": [f"{y}-{m:02d}" for y, m in MONTHS],
        "commission_rate": COMMISSION_RATE,
        "projects": projects_data,
        "monthly_totals": monthly_totals,
        "salesperson_summary": sp_summary,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/projects.json", "w") as f:
        json.dump(output, f, indent=2, default=str)

    n_active = sum(1 for p in projects_data if p["status"] == "Active")
    print(f"\n✓ Sync complete: {len(projects_data)} projects ({n_active} active)")
    print("  Output → data/projects.json")

if __name__ == "__main__":
    sync()
