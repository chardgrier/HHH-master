#!/usr/bin/env python3
"""
HHH Master Dashboard — Data Generation

Primary source: data/master_data.json (authoritative list from Google master sheet).
Secondary: Asana portfolio/workspace (adds any projects the master sheet might miss).
Output: data/projects.json for the browser dashboard.
"""

import os, json, re, sys
from datetime import date, datetime
from calendar import monthrange

try:
    import requests
except ImportError:
    os.system("pip3 install requests -q")
    import requests

ASANA_TOKEN   = os.environ.get("ASANA_TOKEN", "")
BASE_URL      = "https://app.asana.com/api/1.0"
WORKSPACE_GID = "1203487090849714"
PORTFOLIO_GIDS = ["1206746778121935", "1207604445018695"]
COMMISSION_RATE = 0.20

MASTER_FILE = "data/master_data.json"

# Dashboard months: Aug 2025 → Dec 2026
MONTHS = []
y, m = 2025, 8
while (y, m) <= (2026, 12):
    MONTHS.append((y, m)); m += 1
    if m > 12: m = 1; y += 1

# ─── Monthly pro-rated value for a single project ─────────────────────────────

def monthly_amount(start, end, monthly, yr, mo):
    """Return pro-rated monthly amount using 30-day convention."""
    first = date(yr, mo, 1)
    last  = date(yr, mo, monthrange(yr, mo)[1])
    if start > last or end < first:
        return 0.0
    eff_start = max(start, first)
    eff_end   = min(end,   last)
    days      = (eff_end - eff_start).days + 1
    if days <= 0:
        return 0.0
    if eff_start == first and eff_end == last:
        return monthly
    return round(monthly * days / 30, 2)

def compute_status(start, end, today):
    if start > today:
        return "Upcoming"
    if end < today:
        return "Closed"
    return "Active"

# ─── Core: build project records from the master JSON ────────────────────────

def project_from_master(p):
    start = date.fromisoformat(p["start_date"])
    end   = date.fromisoformat(p["end_date"])
    monthly = {}
    for yr, mo in MONTHS:
        key = f"{yr}-{mo:02d}"
        ar = monthly_amount(start, end, p["monthly_ar"], yr, mo)
        ap = monthly_amount(start, end, p["monthly_ap"], yr, mo)
        # Crew is all-or-nothing: full crew in any month where lease is active
        first = date(yr, mo, 1); last = date(yr, mo, monthrange(yr, mo)[1])
        crew_active = not (start > last or end < first)
        crew = p["crew"] if crew_active else 0
        if ar > 0 or ap > 0:
            monthly[key] = {
                "crew": crew,
                "ar":   round(ar, 2),
                "ap":   round(ap, 2),
                "net_gp": round((ar - ap) * (1 - COMMISSION_RATE), 2),
            }
    t_ar = sum(v["ar"]     for k, v in monthly.items() if k.startswith("2026"))
    t_ap = sum(v["ap"]     for k, v in monthly.items() if k.startswith("2026"))
    t_gp = sum(v["net_gp"] for k, v in monthly.items() if k.startswith("2026"))

    m = re.search(r"\b(2\d{3})\b", p["name"])
    return {
        "name":          p["name"],
        "salesperson":   p.get("salesperson", "Unknown"),
        "project_number": m.group(1) if m else "",
        "status":        p.get("status") or compute_status(start, end, date.today()),
        "start_date":    p["start_date"],
        "end_date":      p["end_date"],
        "base_ar":       p["monthly_ar"],
        "base_ap":       p["monthly_ap"],
        "base_crew":     p["crew"],
        "monthly":       monthly,
        "total_2026":    {"ar": round(t_ar,2), "ap": round(t_ap,2), "net_gp": round(t_gp,2)},
        "source":        "master_sheet",
    }

# ─── Main ────────────────────────────────────────────────────────────────────

def sync():
    if not os.path.exists(MASTER_FILE):
        print(f"ERROR: {MASTER_FILE} missing."); sys.exit(1)

    with open(MASTER_FILE) as f:
        master = json.load(f)

    print(f"Building dashboard from {len(master['projects'])} master-sheet projects…")

    projects = [project_from_master(p) for p in master["projects"]]

    # Update status vs today's date (in case master data sets it stale)
    today = date.today()
    for p in projects:
        if p["status"] not in ("Upcoming", "Closed"):  # master file may force status
            s = date.fromisoformat(p["start_date"])
            e = date.fromisoformat(p["end_date"])
            p["status"] = compute_status(s, e, today)

    # Monthly totals
    monthly_totals = {}
    for yr, mo in MONTHS:
        key = f"{yr}-{mo:02d}"
        ar   = sum(p["monthly"].get(key, {}).get("ar",   0) for p in projects)
        ap   = sum(p["monthly"].get(key, {}).get("ap",   0) for p in projects)
        crew = sum(p["monthly"].get(key, {}).get("crew", 0) for p in projects)
        gp   = (ar - ap) * (1 - COMMISSION_RATE)
        monthly_totals[key] = {
            "crew": crew,
            "ar":   round(ar, 2),
            "ap":   round(ap, 2),
            "net_gp": round(gp, 2),
            "gp_pct": round(gp / ar * 100 if ar else 0, 2),
        }

    # Salesperson summary
    sp = {}
    for p in projects:
        for key, v in p["monthly"].items():
            sp.setdefault(p["salesperson"], {}).setdefault(key, {"ar": 0, "ap": 0, "commission": 0})
            sp[p["salesperson"]][key]["ar"] += v["ar"]
            sp[p["salesperson"]][key]["ap"] += v["ap"]
            sp[p["salesperson"]][key]["commission"] += round((v["ar"] - v["ap"]) * COMMISSION_RATE, 2)

    out = {
        "generated_at": datetime.now().isoformat(),
        "months":       [f"{y}-{m:02d}" for y, m in MONTHS],
        "commission_rate": COMMISSION_RATE,
        "projects":     projects,
        "monthly_totals": monthly_totals,
        "salesperson_summary": sp,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/projects.json", "w") as f:
        json.dump(out, f, indent=2, default=str)

    active = sum(1 for p in projects if p["status"] == "Active")
    upcoming = sum(1 for p in projects if p["status"] == "Upcoming")
    print(f"\n✓ {len(projects)} projects  ·  {active} active  ·  {upcoming} upcoming")
    print("  Output → data/projects.json")

if __name__ == "__main__":
    sync()
