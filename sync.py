#!/usr/bin/env python3
"""
HHH Master Dashboard — Sync

Reads HHH custom fields on every Construction Lease task across the Asana
workspace and emits data/projects.json for the browser dashboard.

Every task with HHH:Monthly A/R + HHH:Start Date + HHH:End Date populated
becomes one row in the dashboard. Multi-phase and multi-house projects
naturally produce multiple rows.

Falls back to data/master_data.json for any projects without custom fields
(e.g. Rising Sun, which has no project number).
"""

import os, json, re, sys
from datetime import date, datetime
from calendar import monthrange

try:
    import requests
except ImportError:
    os.system("pip3 install requests -q"); import requests

ASANA_TOKEN    = os.environ.get("ASANA_TOKEN", "")
BASE_URL       = "https://app.asana.com/api/1.0"
WORKSPACE_GID  = "1203487090849714"
COMMISSION_RATE = 0.20
MASTER_FILE    = "data/master_data.json"
CF_GIDS_FILE   = "data/custom_field_gids.json"

SALESPEOPLE = ["Paul","Zeke","Matt","Logan","David","Charlie","Peyton"]
HHH_PROJECT_RE = re.compile(rf"\b({'|'.join(SALESPEOPLE)})\b.*?\d{{4}}", re.IGNORECASE)

SKIP_NAMES = ["template", "general to do", "xxxx", "hard hat housing template",
              "1501 richmond", "4709 orlando", "1113 taborlake"]
VOID_TASK  = ("did not send","did not use","cancelled","termination","terminated",
              "back up","backup","not used","duplicate")

# Dashboard months: Aug 2025 → Dec 2026
MONTHS = []
y, m = 2025, 8
while (y, m) <= (2026, 12):
    MONTHS.append((y, m)); m += 1
    if m > 12: m = 1; y += 1

# ─── Asana paginated GET ──────────────────────────────────────────────────────

def asana(endpoint, params=None):
    if not ASANA_TOKEN: return []
    headers = {"Authorization": f"Bearer {ASANA_TOKEN}"}
    url = f"{BASE_URL}/{endpoint}"
    results = []
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 429:
            import time; time.sleep(int(r.headers.get("Retry-After", 5))); continue
        r.raise_for_status()
        d = r.json()
        body = d.get("data", [])
        if isinstance(body, list):
            results.extend(body)
        else:
            return body
        nxt = d.get("next_page")
        url = nxt["uri"] if nxt else None
        params = None
    return results

# ─── Custom field extraction ─────────────────────────────────────────────────

def cf_value(task, field_gid):
    """Return the primitive value of a given custom field on a task."""
    for cf in task.get("custom_fields", []):
        if cf.get("gid") != field_gid: continue
        sub = cf.get("resource_subtype")
        if sub == "number":
            return cf.get("number_value")
        if sub == "date":
            dv = cf.get("date_value")
            return dv.get("date") if isinstance(dv, dict) else dv
        if sub == "enum":
            ev = cf.get("enum_value")
            return ev.get("name") if isinstance(ev, dict) else None
        if sub == "text":
            return cf.get("text_value")
    return None

# ─── Monthly pro-ration ───────────────────────────────────────────────────────

def prorate(start, end, monthly, yr, mo):
    first = date(yr, mo, 1); last = date(yr, mo, monthrange(yr, mo)[1])
    if start > last or end < first: return 0.0
    eff_s = max(start, first); eff_e = min(end, last)
    days = (eff_e - eff_s).days + 1
    if days <= 0: return 0.0
    if eff_s == first and eff_e == last: return monthly
    return round(monthly * days / 30, 2)

def build_monthly(start, end, ar, ap, crew):
    monthly = {}
    for yr, mo in MONTHS:
        key = f"{yr}-{mo:02d}"
        mo_ar = prorate(start, end, ar, yr, mo)
        mo_ap = prorate(start, end, ap, yr, mo)
        first = date(yr, mo, 1); last = date(yr, mo, monthrange(yr, mo)[1])
        mo_crew = crew if not (start > last or end < first) else 0
        if mo_ar > 0 or mo_ap > 0:
            monthly[key] = {
                "crew": mo_crew,
                "ar":   round(mo_ar, 2),
                "ap":   round(mo_ap, 2),
                "net_gp": round((mo_ar - mo_ap) * (1 - COMMISSION_RATE), 2),
            }
    return monthly

def compute_status(start, end, today):
    if start > today: return "Upcoming"
    if end < today:   return "Closed"
    return "Active"

def project_row(name, salesperson, start_s, end_s, ar, ap, crew, *, source, gid=None, task_gid=None):
    start = date.fromisoformat(start_s)
    end   = date.fromisoformat(end_s)
    monthly = build_monthly(start, end, ar, ap, crew)
    t_ar = sum(v["ar"]     for k,v in monthly.items() if k.startswith("2026"))
    t_ap = sum(v["ap"]     for k,v in monthly.items() if k.startswith("2026"))
    t_gp = sum(v["net_gp"] for k,v in monthly.items() if k.startswith("2026"))
    m = re.search(r"\b(2\d{3})\b", name)
    return {
        "gid": gid, "task_gid": task_gid,
        "name": name,
        "salesperson": salesperson or "Unknown",
        "project_number": m.group(1) if m else "",
        "status": compute_status(start, end, date.today()),
        "start_date": start_s, "end_date": end_s,
        "base_ar": ar, "base_ap": ap, "base_crew": crew,
        "monthly": monthly,
        "total_2026": {"ar": round(t_ar,2), "ap": round(t_ap,2), "net_gp": round(t_gp,2)},
        "source": source,
    }

# ─── Main sync ───────────────────────────────────────────────────────────────

def sync():
    print(f"=== HHH Sync @ {datetime.now():%Y-%m-%d %H:%M} ===")

    if not os.path.exists(CF_GIDS_FILE):
        print(f"ERROR: {CF_GIDS_FILE} missing — create custom fields first")
        sys.exit(1)
    with open(CF_GIDS_FILE) as f:
        CF = json.load(f)

    # ── Scan workspace for HHH projects ──
    all_projects = {}
    if ASANA_TOKEN:
        ws = asana("projects", {
            "workspace": WORKSPACE_GID, "archived": "false",
            "opt_fields": "name", "limit": 100,
        })
        for p in ws:
            if HHH_PROJECT_RE.search(p.get("name","")):
                if not any(s in p["name"].lower() for s in SKIP_NAMES):
                    all_projects[p["gid"]] = p
        # Also include the two portfolios
        for pgid in ("1206746778121935", "1207604445018695"):
            for p in asana(f"portfolios/{pgid}/items", {"opt_fields":"name"}):
                if p["gid"] not in all_projects:
                    if not any(s in p["name"].lower() for s in SKIP_NAMES):
                        all_projects[p["gid"]] = p
        print(f"Found {len(all_projects)} HHH-pattern projects in workspace")

    # ── For each project, pull tasks with custom fields populated ──
    rows = []
    seen_project_nums = set()

    ar_gid    = CF["HHH: Monthly A/R"]
    ap_gid    = CF["HHH: Monthly A/P"]
    start_gid = CF["HHH: Start Date"]
    end_gid   = CF["HHH: End Date"]
    crew_gid  = CF["HHH: Crew Size"]
    sp_gid    = CF["HHH: Salesperson"]

    for pgid, pitem in all_projects.items():
        pname = pitem["name"].strip()
        try:
            tasks = asana("tasks", {
                "project": pgid,
                "opt_fields":
                    "name,custom_fields.gid,custom_fields.resource_subtype,"
                    "custom_fields.number_value,custom_fields.date_value,"
                    "custom_fields.enum_value,custom_fields.text_value",
                "limit": 100,
            })
        except Exception as e:
            print(f"  ! {pname[:40]}: {e}"); continue

        for t in tasks:
            tname = (t.get("name") or "").strip()
            if not tname: continue
            if any(v in tname.lower() for v in VOID_TASK): continue

            ar    = cf_value(t, ar_gid)
            ap    = cf_value(t, ap_gid)
            start = cf_value(t, start_gid)
            end   = cf_value(t, end_gid)
            crew  = cf_value(t, crew_gid)
            sp    = cf_value(t, sp_gid)

            if ar is None or ap is None or not start or not end:
                continue

            # Build a display name for this row
            # Use project name as base, then append task qualifier if it adds info
            row_name = pname
            qual = re.search(r"^([A-Z])\s*:|Phase\s*\d+|House\s+[A-Z]", tname, re.I)
            if qual and re.search(r"construction lease", tname, re.I):
                q = qual.group(0).rstrip(":").strip()
                if q.upper() != "A" or "house" in tname.lower() or "phase" in tname.lower():
                    row_name = f"{pname} — {tname.split(':')[0].strip() if ':' in tname else q}"

            # Infer salesperson from project name if enum not set
            if not sp:
                for s in SALESPEOPLE:
                    if re.search(rf"\b{s}\b", pname, re.I): sp = s; break

            rows.append(project_row(
                row_name, sp, start, end,
                float(ar), float(ap), int(crew or 0),
                source="asana_custom_fields",
                gid=pgid, task_gid=t["gid"],
            ))
            m = re.search(r"\b(2\d{3})\b", pname)
            if m: seen_project_nums.add(m.group(1))

    print(f"  → {len(rows)} rows from Asana custom fields")

    # ── Fallback: include master_data entries that aren't in Asana by number ──
    if os.path.exists(MASTER_FILE):
        with open(MASTER_FILE) as f:
            master = json.load(f)
        added = 0
        for p in master.get("projects", []):
            mnum = re.search(r"\b(2\d{3})\b", p["name"])
            if mnum and mnum.group(1) in seen_project_nums:
                continue  # already have it from Asana
            # Also skip if name already matches a row (covers Rising Sun etc.)
            if any(p["name"].lower() in r["name"].lower() for r in rows):
                continue
            rows.append(project_row(
                p["name"], p.get("salesperson"),
                p["start_date"], p["end_date"],
                p["monthly_ar"], p["monthly_ap"], p["crew"],
                source="master_sheet_fallback",
            ))
            added += 1
        if added:
            print(f"  + {added} rows from master_sheet fallback (missing from Asana)")

    # ── Dedupe by (project name, start date) ──
    seen = set(); unique = []
    for r in rows:
        key = (r["name"].strip().lower(), r["start_date"])
        if key in seen: continue
        seen.add(key); unique.append(r)
    rows = unique

    # ── Aggregates ──
    monthly_totals = {}
    for yr, mo in MONTHS:
        key = f"{yr}-{mo:02d}"
        ar   = sum(r["monthly"].get(key, {}).get("ar",   0) for r in rows)
        ap   = sum(r["monthly"].get(key, {}).get("ap",   0) for r in rows)
        crew = sum(r["monthly"].get(key, {}).get("crew", 0) for r in rows)
        gp   = (ar - ap) * (1 - COMMISSION_RATE)
        monthly_totals[key] = {
            "crew": crew, "ar": round(ar,2), "ap": round(ap,2),
            "net_gp": round(gp,2),
            "gp_pct": round(gp/ar*100 if ar else 0, 2),
        }

    sp_summary = {}
    for r in rows:
        for key, v in r["monthly"].items():
            sp_summary.setdefault(r["salesperson"], {}).setdefault(key, {"ar":0, "ap":0, "commission":0})
            sp_summary[r["salesperson"]][key]["ar"] += v["ar"]
            sp_summary[r["salesperson"]][key]["ap"] += v["ap"]
            sp_summary[r["salesperson"]][key]["commission"] += round((v["ar"]-v["ap"])*COMMISSION_RATE, 2)

    out = {
        "generated_at": datetime.now().isoformat(),
        "months": [f"{y}-{m:02d}" for y, m in MONTHS],
        "commission_rate": COMMISSION_RATE,
        "projects": rows,
        "monthly_totals": monthly_totals,
        "salesperson_summary": sp_summary,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/projects.json", "w") as f:
        json.dump(out, f, indent=2, default=str)

    active   = sum(1 for r in rows if r["status"] == "Active")
    upcoming = sum(1 for r in rows if r["status"] == "Upcoming")
    closed   = sum(1 for r in rows if r["status"] == "Closed")
    asana_ct = sum(1 for r in rows if r.get("source") == "asana_custom_fields")
    fallback = sum(1 for r in rows if r.get("source") == "master_sheet_fallback")
    print(f"\n✓ {len(rows)} rows  ·  {active} active · {upcoming} upcoming · {closed} closed")
    print(f"  sources: {asana_ct} Asana custom fields · {fallback} master_sheet fallback")
    print("  Output → data/projects.json")

if __name__ == "__main__":
    sync()
