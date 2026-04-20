#!/usr/bin/env python3
"""
HHH Master Dashboard Sync

Primary source: Asana task notes (construction + homeowner leases, addendums)
Safety net:     data/master_data.json (overrides Asana when notes are unparseable)

Flow:
  1. Scan Asana portfolios + whole workspace for projects matching HHH naming
  2. Parse each project's tasks → house data → monthly A/R, A/P, crew
  3. For any project in master_data.json that's missing or has bad Asana data,
     use the master_data values instead (flagged as source=master_sheet)
  4. Emit data/projects.json
"""

import os, json, re, sys
from datetime import date, datetime
from calendar import monthrange

try:
    import requests
except ImportError:
    os.system("pip3 install requests -q")
    import requests

ASANA_TOKEN    = os.environ.get("ASANA_TOKEN", "")
BASE_URL       = "https://app.asana.com/api/1.0"
WORKSPACE_GID  = "1203487090849714"
PORTFOLIO_GIDS = ["1206746778121935", "1207604445018695"]
COMMISSION_RATE = 0.20
MASTER_FILE    = "data/master_data.json"

# Projects whose names match "...Paul|Zeke|Matt|Logan|David|Charlie|Peyton...NNNN"
HHH_PROJECT_RE = re.compile(
    r"\b(Paul|Zeke|Matt|Logan|David|Charlie|Peyton)\b.*?-?\s*\d{4}",
    re.IGNORECASE,
)

# Skip obvious non-project items
SKIP_NAMES = ["template", "general to do", "xxxx", "hard hat housing template",
              "1501 richmond", "4709 orlando", "1113 taborlake"]

# Dashboard months: Aug 2025 → Dec 2026
MONTHS = []
y, m = 2025, 8
while (y, m) <= (2026, 12):
    MONTHS.append((y, m)); m += 1
    if m > 12: m = 1; y += 1

# ─── Asana API ────────────────────────────────────────────────────────────────

def asana(endpoint, params=None):
    if not ASANA_TOKEN:
        return []
    headers = {"Authorization": f"Bearer {ASANA_TOKEN}"}
    url = f"{BASE_URL}/{endpoint}"
    results = []
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 429:
            import time; time.sleep(int(r.headers.get("Retry-After", 5))); continue
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

def parse_date(s):
    if not s: return None
    s = s.strip().rstrip(".")
    m = re.fullmatch(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})", s) or \
        re.search (r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})", s)
    if not m: return None
    mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if y < 100: y += 2000
    try: return date(y, mo, d)
    except ValueError: return None

def parse_date_pair(text):
    pat = re.compile(
        r"(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})\s*(?:thru|through|to|\-{1,2}|–)\s*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})",
        re.IGNORECASE)
    m = pat.search(text)
    return (parse_date(m.group(1)), parse_date(m.group(2))) if m else (None, None)

def parse_amount(text):
    # Drop yearly/daily parentheticals
    text = re.sub(r"\(.*?(?:yearly|daily|annual|per year).*?\)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"Yearly\s+amount[^$\n]*\$[\d,\.]+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"Daily\s+rate[^$\n]*\$[\d,\.]+", "", text, flags=re.IGNORECASE)
    for a in re.findall(r"\$\s*([\d,]+(?:\.\d{1,2})?)", text):
        try:
            v = float(a.replace(",", ""))
            if 100 < v < 100_000:
                return v
        except ValueError:
            pass
    return 0.0

def parse_crew(text):
    m = re.search(r"crew\s+size\s*:?\s*(\d+)", text, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r"\b(\d+)\s+crew\b", text, re.IGNORECASE)
    return int(m.group(1)) if m else 0

def field(text, *names):
    for n in names:
        m = re.search(rf"{re.escape(n)}\s*:?\s*(.+?)(?:\n|$)", text, re.IGNORECASE)
        if m: return m.group(1).strip()
    return ""

def extension_end(notes):
    ext_re = re.compile(
        r"extension\s*#?\d*\s*:?\s*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})\s*[-–to]+\s*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})",
        re.IGNORECASE)
    latest = None; latest_rate = None
    for m in ext_re.finditer(notes):
        d = parse_date(m.group(2))
        if d and (latest is None or d > latest):
            latest = d
            after = notes[m.end():]
            r = re.search(r"RATE\s*:?\s*\$?([\d,]+)", after[:200], re.IGNORECASE)
            if r:
                try:
                    v = float(r.group(1).replace(",", ""))
                    if 100 < v < 100_000:
                        latest_rate = v
                except ValueError:
                    pass
    return latest, latest_rate

# ─── Lease/addendum parsing ──────────────────────────────────────────────────

def parse_construction_lease(notes):
    if not notes: return None
    dt = field(notes, "PROJECT DATES", "Project Dates", "DATES")
    s, e = parse_date_pair(dt) if dt else (None, None)
    if not s: s, e = parse_date_pair(notes)
    rt = field(notes, "RENT PER MONTH", "Rent per Month", "RATE", "Rate")
    amount = parse_amount(rt) if rt else parse_amount(notes)
    crew = parse_crew(field(notes, "CREW SIZE", "Crew Size") or notes)
    if not s or not amount: return None
    return {"start_date": s.isoformat(), "end_date": e.isoformat() if e else None,
            "monthly_amount": amount, "crew_size": crew}

def parse_homeowner_lease(notes):
    if not notes: return None
    dt = field(notes, "TIME FRAME", "Time Frame", "PROJECT DATES", "Project Dates", "DATES")
    s, e = parse_date_pair(dt) if dt else (None, None)
    if not s: s, e = parse_date_pair(notes)
    rt = field(notes, "RENT PER MONTH", "RATE", "Rate", "Monthly Rent")
    amount = parse_amount(rt) if rt else parse_amount(notes)
    if not s or not amount: return None
    return {"start_date": s.isoformat(), "end_date": e.isoformat() if e else None,
            "monthly_amount": amount}

def apply_addendum(lease, notes):
    if not notes or not lease: return
    new_end, new_rate = extension_end(notes)
    if new_end:
        cur = date.fromisoformat(lease["end_date"]) if lease.get("end_date") else None
        if cur is None or new_end > cur:
            lease["end_date"] = new_end.isoformat()
    if new_rate:
        lease["monthly_amount"] = new_rate

# ─── Task processing ─────────────────────────────────────────────────────────

PREFIXED  = re.compile(r"^([A-Z])\s*:\s*(Construction Lease|Homeowner Lease|Construction Addendum|Homeowner Addendum)", re.IGNORECASE)
UNPREFIXED = re.compile(r"^(Construction Lease|Homeowner Lease|Construction Addendum|Homeowner Addendum)\b", re.IGNORECASE)
VOID = ("did not send", "did not use", "cancelled", "termination", "terminated", "terminate",
        "cancellation", "back up", "backup", "not used", "duplicate")

def task_void(name):
    low = name.lower()
    return any(t in low for t in VOID)

def process_tasks(tasks):
    houses = {}
    for t in tasks:
        name  = (t.get("name")  or "").strip()
        notes = (t.get("notes") or "").strip()
        if not name or task_void(name): continue

        m = PREFIXED.match(name)
        if m:
            letter, kind = m.group(1).upper(), m.group(2).lower()
        else:
            m2 = UNPREFIXED.match(name)
            if not m2: continue
            letter, kind = "A", m2.group(1).lower()

        houses.setdefault(letter, {"letter": letter, "construction": None, "homeowner": None})

        if "construction lease" in kind:
            p = parse_construction_lease(notes)
            if p and not houses[letter]["construction"]:
                houses[letter]["construction"] = p
        elif "homeowner lease" in kind:
            p = parse_homeowner_lease(notes)
            if p and not houses[letter]["homeowner"]:
                houses[letter]["homeowner"] = p
        elif "construction addendum" in kind:
            apply_addendum(houses[letter].get("construction"), notes)
        elif "homeowner addendum" in kind:
            apply_addendum(houses[letter].get("homeowner"), notes)
    return houses

# ─── Monthly value & status ───────────────────────────────────────────────────

def monthly_value(lease, yr, mo):
    if not lease or not lease.get("start_date") or not lease.get("monthly_amount"):
        return 0.0, 0
    start = date.fromisoformat(lease["start_date"])
    end = date.fromisoformat(lease["end_date"]) if lease.get("end_date") else date(2099,12,31)
    amt = lease["monthly_amount"]; crew = lease.get("crew_size", 0)
    first = date(yr, mo, 1); last = date(yr, mo, monthrange(yr, mo)[1])
    if start > last or end < first: return 0.0, 0
    eff_start = max(start, first); eff_end = min(end, last)
    days = (eff_end - eff_start).days + 1
    if days <= 0: return 0.0, 0
    if eff_start == first and eff_end == last: return amt, crew
    return round(amt * days / 30, 2), crew

SALESPEOPLE = ["Paul","Zeke","Matt","Logan","David","Charlie","Peyton"]
def extract_salesperson(name):
    for sp in SALESPEOPLE:
        if re.search(rf"\b{sp}\b", name, re.IGNORECASE): return sp
    return "Unknown"

def extract_number(name):
    m = re.search(r"\b(2\d{3})\b", name); return m.group(1) if m else ""

def project_status(start_s, end_s, today):
    if not start_s: return "Unknown"
    s = date.fromisoformat(start_s)
    e = date.fromisoformat(end_s) if end_s else None
    if s > today: return "Upcoming"
    if e and e < today: return "Closed"
    return "Active"

# ─── Build one project from Asana houses ─────────────────────────────────────

def build_from_asana(gid, name, houses):
    all_starts, all_ends = [], []
    for h in houses.values():
        cl = h.get("construction")
        if cl:
            if cl.get("start_date"): all_starts.append(date.fromisoformat(cl["start_date"]))
            if cl.get("end_date"):   all_ends.append(date.fromisoformat(cl["end_date"]))
    if not all_starts: return None

    start = min(all_starts).isoformat()
    end   = max(all_ends).isoformat() if all_ends else None

    base_ar  = sum(h["construction"]["monthly_amount"]     for h in houses.values() if h.get("construction"))
    base_ap  = sum(h["homeowner"]["monthly_amount"]        for h in houses.values() if h.get("homeowner"))
    base_crew = sum(h["construction"].get("crew_size", 0)  for h in houses.values() if h.get("construction"))

    monthly = {}
    for yr, mo in MONTHS:
        key = f"{yr}-{mo:02d}"
        ar, crew, ap = 0.0, 0, 0.0
        for h in houses.values():
            if h.get("construction"):
                a, c = monthly_value(h["construction"], yr, mo); ar += a; crew += c
            if h.get("homeowner"):
                a, _ = monthly_value(h["homeowner"], yr, mo); ap += a
        if ar > 0 or ap > 0:
            monthly[key] = {"crew": crew, "ar": round(ar,2), "ap": round(ap,2),
                            "net_gp": round((ar-ap)*(1-COMMISSION_RATE), 2)}

    t_ar = sum(v["ar"]     for k,v in monthly.items() if k.startswith("2026"))
    t_ap = sum(v["ap"]     for k,v in monthly.items() if k.startswith("2026"))
    t_gp = sum(v["net_gp"] for k,v in monthly.items() if k.startswith("2026"))

    return {
        "gid": gid,
        "name": name,
        "salesperson": extract_salesperson(name),
        "project_number": extract_number(name),
        "status": project_status(start, end, date.today()),
        "start_date": start, "end_date": end,
        "base_ar": base_ar, "base_ap": base_ap, "base_crew": base_crew,
        "monthly": monthly,
        "total_2026": {"ar": round(t_ar,2), "ap": round(t_ap,2), "net_gp": round(t_gp,2)},
        "houses": {lt: {"construction": h.get("construction"), "homeowner": h.get("homeowner")}
                   for lt, h in houses.items()},
        "source": "asana",
    }

# ─── Build one project from master_data.json entry ───────────────────────────

def build_from_master(p):
    start = date.fromisoformat(p["start_date"])
    end   = date.fromisoformat(p["end_date"])
    monthly = {}
    for yr, mo in MONTHS:
        key = f"{yr}-{mo:02d}"
        first = date(yr, mo, 1); last = date(yr, mo, monthrange(yr, mo)[1])
        if start > last or end < first: continue
        eff_s = max(start, first); eff_e = min(end, last)
        days = (eff_e - eff_s).days + 1
        if days <= 0: continue
        if eff_s == first and eff_e == last:
            ar, ap = p["monthly_ar"], p["monthly_ap"]
        else:
            ar = round(p["monthly_ar"] * days / 30, 2)
            ap = round(p["monthly_ap"] * days / 30, 2)
        monthly[key] = {"crew": p["crew"], "ar": ar, "ap": ap,
                        "net_gp": round((ar-ap)*(1-COMMISSION_RATE), 2)}

    t_ar = sum(v["ar"]     for k,v in monthly.items() if k.startswith("2026"))
    t_ap = sum(v["ap"]     for k,v in monthly.items() if k.startswith("2026"))
    t_gp = sum(v["net_gp"] for k,v in monthly.items() if k.startswith("2026"))

    m = re.search(r"\b(2\d{3})\b", p["name"])
    return {
        "name": p["name"],
        "salesperson": p.get("salesperson", "Unknown"),
        "project_number": m.group(1) if m else "",
        "status": p.get("status") or project_status(p["start_date"], p["end_date"], date.today()),
        "start_date": p["start_date"], "end_date": p["end_date"],
        "base_ar": p["monthly_ar"], "base_ap": p["monthly_ap"], "base_crew": p["crew"],
        "monthly": monthly,
        "total_2026": {"ar": round(t_ar,2), "ap": round(t_ap,2), "net_gp": round(t_gp,2)},
        "source": "master_sheet",
    }

# ─── Name normalization for matching Asana ↔ master_data ────────────────────

def normalize(n):
    n = n.lower()
    n = re.sub(r"\[house [a-z]\]", "", n)
    n = re.sub(r"[^a-z0-9]+", " ", n)
    return " ".join(n.split())

def master_key(p):
    """Normalized key used to match a master_data project with an Asana project."""
    m = re.search(r"\b(2\d{3})\b", p["name"])
    return m.group(1) if m else normalize(p["name"])

# ─── Main sync ───────────────────────────────────────────────────────────────

def sync():
    print(f"=== HHH Sync @ {datetime.now():%Y-%m-%d %H:%M} ===")

    # ── Load master_data (safety net / source of truth for bad Asana data) ──
    master_projects = []
    if os.path.exists(MASTER_FILE):
        with open(MASTER_FILE) as f:
            master = json.load(f)
        master_projects = master.get("projects", [])
        print(f"Loaded {len(master_projects)} projects from master_data.json")

    # Build a lookup keyed by project number (2xxx), with list of entries
    # (multiple houses of same project number are allowed)
    master_by_num = {}
    for p in master_projects:
        k = master_key(p)
        master_by_num.setdefault(k, []).append(p)

    # ── Scan Asana for HHH projects ──
    all_asana = {}
    if ASANA_TOKEN:
        for pgid in PORTFOLIO_GIDS:
            try:
                for item in asana(f"portfolios/{pgid}/items", {"opt_fields": "name,permalink_url"}):
                    all_asana[item["gid"]] = item
            except Exception as e:
                print(f"  portfolio {pgid} err: {e}")

        try:
            ws = asana("projects", {
                "workspace": WORKSPACE_GID, "archived": "false",
                "opt_fields": "name,permalink_url", "limit": 100,
            })
            for item in ws:
                if item["gid"] in all_asana: continue
                if HHH_PROJECT_RE.search(item.get("name", "")):
                    all_asana[item["gid"]] = item
        except Exception as e:
            print(f"  workspace scan err: {e}")

        print(f"Found {len(all_asana)} Asana projects matching HHH pattern")
    else:
        print("No ASANA_TOKEN — falling back to master_data only")

    # ── Parse each Asana project ──
    projects = []
    used_master_nums = set()

    for gid, item in all_asana.items():
        pname = item.get("name","").strip()
        if any(s in pname.lower() for s in SKIP_NAMES): continue

        try:
            tasks  = asana("tasks", {"project": gid, "opt_fields":"name,notes", "limit": 100})
            houses = process_tasks(tasks)
            if not houses: continue

            proj = build_from_asana(gid, pname, houses)
            if not proj: continue

            # Validate Asana data against master_data for sanity
            num = extract_number(pname)
            master_candidates = master_by_num.get(num, [])

            bad_data = proj["base_ap"] > proj["base_ar"] * 1.5   # A/P >> A/R → parser bug
            if bad_data and master_candidates:
                # Replace with master data
                print(f"  ⚠ parser issue on {pname} — using master_sheet fallback")
                for mp in master_candidates:
                    projects.append(build_from_master(mp))
                    used_master_nums.add(master_key(mp))
                continue

            projects.append(proj)
            # Mark any matching master entries as "already covered"
            for mp in master_candidates:
                used_master_nums.add(master_key(mp))

        except Exception as e:
            print(f"  error on {pname}: {e}")

    # ── Add any master_data projects NOT found in Asana (by project number) ──
    added_from_master = 0
    for p in master_projects:
        k = master_key(p)
        if k in used_master_nums: continue
        projects.append(build_from_master(p))
        added_from_master += 1
    if added_from_master:
        print(f"  + {added_from_master} projects loaded from master_sheet (not in Asana)")

    # ── Deduplicate: if multiple entries share a project_number, keep all
    #                  (they're different houses/phases), but dedupe by exact name
    seen_names = set()
    unique = []
    for p in projects:
        key = p["name"].strip().lower()
        if key in seen_names: continue
        seen_names.add(key)
        unique.append(p)
    projects = unique

    # ── Aggregate totals ──
    monthly_totals = {}
    for yr, mo in MONTHS:
        key = f"{yr}-{mo:02d}"
        ar   = sum(p["monthly"].get(key, {}).get("ar", 0)   for p in projects)
        ap   = sum(p["monthly"].get(key, {}).get("ap", 0)   for p in projects)
        crew = sum(p["monthly"].get(key, {}).get("crew", 0) for p in projects)
        gp   = (ar - ap) * (1 - COMMISSION_RATE)
        monthly_totals[key] = {
            "crew": crew, "ar": round(ar,2), "ap": round(ap,2),
            "net_gp": round(gp,2),
            "gp_pct": round(gp/ar*100 if ar else 0, 2),
        }

    sp = {}
    for p in projects:
        for key, v in p.get("monthly", {}).items():
            sp.setdefault(p["salesperson"], {}).setdefault(key, {"ar":0, "ap":0, "commission":0})
            sp[p["salesperson"]][key]["ar"] += v["ar"]
            sp[p["salesperson"]][key]["ap"] += v["ap"]
            sp[p["salesperson"]][key]["commission"] += round((v["ar"]-v["ap"])*COMMISSION_RATE, 2)

    out = {
        "generated_at": datetime.now().isoformat(),
        "months": [f"{y}-{m:02d}" for y, m in MONTHS],
        "commission_rate": COMMISSION_RATE,
        "projects": projects,
        "monthly_totals": monthly_totals,
        "salesperson_summary": sp,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/projects.json", "w") as f:
        json.dump(out, f, indent=2, default=str)

    active   = sum(1 for p in projects if p["status"] == "Active")
    upcoming = sum(1 for p in projects if p["status"] == "Upcoming")
    closed   = sum(1 for p in projects if p["status"] == "Closed")
    asana_ct   = sum(1 for p in projects if p.get("source")=="asana")
    master_ct  = sum(1 for p in projects if p.get("source")=="master_sheet")
    print(f"\n✓ {len(projects)} projects  ·  {active} active · {upcoming} upcoming · {closed} closed")
    print(f"  sources: {asana_ct} Asana · {master_ct} master_sheet")
    print("  Output → data/projects.json")

if __name__ == "__main__":
    sync()
