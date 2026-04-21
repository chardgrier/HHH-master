#!/usr/bin/env python3
"""
HHH Master Dashboard — Sync v3 (project-level aggregation)

Model:
  • Each Asana project = one dashboard row.
  • Monthly A/R = sum of all Construction Lease + Construction Addendum
    segments active that month (each segment has its own dates and $).
  • Monthly A/P = sum of all Homeowner Lease + Homeowner Addendum segments
    active that month.
  • Each segment's $ is rent + cleaning + other fees (security deposit excluded),
    which is what the HHH custom fields already hold.
  • Addendums override their parent lease's dates/rate for the extension period.

Falls back to data/master_data.json for any project with no HHH custom fields
populated (e.g. Rising Sun, Gray Construction Bristol — no project number).
"""
import os, json, re, sys
from datetime import date, datetime
from calendar import monthrange

try:
    import requests
except ImportError:
    os.system("pip3 install requests -q"); import requests

ASANA_TOKEN     = os.environ.get("ASANA_TOKEN", "")
BASE_URL        = "https://app.asana.com/api/1.0"
WORKSPACE_GID   = "1203487090849714"
COMMISSION_RATE = 0.20
MASTER_FILE     = "data/master_data.json"
CF_GIDS_FILE    = "data/custom_field_gids.json"

SALESPEOPLE = ["Paul","Zeke","Matt","Logan","David","Charlie","Peyton"]
HHH_PROJECT_RE = re.compile(rf"\b({'|'.join(SALESPEOPLE)})\b.*?\d{{4}}", re.IGNORECASE)

SKIP_NAMES = ["template", "general to do", "xxxx", "hard hat housing template",
              "1501 richmond", "4709 orlando", "1113 taborlake",
              # Rising Sun / RSD: all values come from data/rising_sun.json instead
              "rising sun", "rsd -", "rsd-"]
RISING_SUN_FILE = "data/rising_sun.json"
VOID_TASK = ("did not send","did not use","cancelled","termination","terminated",
             "back up","backup","not used","duplicate","did not")

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
    url = f"{BASE_URL}/{endpoint}"; results = []
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 429:
            import time; time.sleep(int(r.headers.get("Retry-After", 5))); continue
        r.raise_for_status()
        d = r.json(); body = d.get("data", [])
        if isinstance(body, list):
            results.extend(body)
        else:
            return body
        nxt = d.get("next_page")
        url = nxt["uri"] if nxt else None
        params = None
    return results

# ─── Custom field value extraction ────────────────────────────────────────────

def cf_value(task, field_gid):
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

def classify(name):
    """
    Classify a task. Only accepts CANONICAL lease/addendum names:
      - "Construction Lease"        (unprefixed)
      - "A: Construction Lease"     (letter prefix)
      - "Construction Lease Phase N"
      - ...and same variants for Homeowner/Addendum.

    Reject variants like "Construction Lease House - 10 Telluride Dr."
    (those are often one-off house notes, not the primary lease).
    """
    n = (name or "").strip().lower()
    if not n or any(v in n for v in VOID_TASK): return None

    # Canonical patterns (anchored)
    patterns = [
        (r"^(?:[a-z]\s*:\s*|phase\s+\d+\s+)?construction\s+addendum(?:\s*#?\s*\d+)?\s*$", "construction_addendum"),
        (r"^(?:[a-z]\s*:\s*|phase\s+\d+\s+)?homeowner\s+addendum(?:\s*#?\s*\d+)?\s*$",    "homeowner_addendum"),
        (r"^(?:[a-z]\s*:\s*)?construction\s+lease(?:\s+phase\s+\d+)?\s*$",                 "construction_lease"),
        (r"^(?:[a-z]\s*:\s*)?homeowner\s+lease(?:\s+phase\s+\d+|\s*[-–]\s*[a-z])?\s*$",    "homeowner_lease"),
    ]
    for pat, kind in patterns:
        if re.match(pat, n):
            return kind
    return None

def prefix_of(name):
    """Extract the letter/phase grouping key from a task name."""
    m = re.match(r"^([A-Z])\s*:", name)
    if m: return m.group(1).upper()
    m = re.search(r"phase\s+(\d+)", name, re.I)
    if m: return f"Phase{m.group(1)}"
    return "_main"

# ─── Pro-ration ───────────────────────────────────────────────────────────────

def prorate(start, end, monthly, yr, mo):
    first = date(yr, mo, 1); last = date(yr, mo, monthrange(yr, mo)[1])
    if start > last or end < first: return 0.0
    eff_s = max(start, first); eff_e = min(end, last)
    days = (eff_e - eff_s).days + 1
    if days <= 0: return 0.0
    if eff_s == first and eff_e == last: return monthly
    return round(monthly * days / 30, 2)

def compute_status(start, end, today):
    if start > today: return "Upcoming"
    if end < today:   return "Closed"
    return "Active"

# ─── Collect segments for a project ──────────────────────────────────────────

def build_house_segments(tasks, ar_gid, ap_gid, start_gid, end_gid, crew_gid):
    """
    Group tasks by prefix (letter or _main) and return a list of houses:
      [{"prefix": "A", "ar": segment or None, "ap": segment or None, "crew": int}]

    Each segment = {start,end,amount}. Addendums extend end + override rate.
    """
    groups = {}  # prefix → {type → {"lease":task|None, "addendums":[tasks...]}}

    for t in tasks:
        tname = (t.get("name") or "").strip()
        kind  = classify(tname)
        if not kind: continue

        gtype = "construction" if kind.startswith("construction") else "homeowner"
        pre = prefix_of(tname)
        g = groups.setdefault(pre, {}).setdefault(gtype, {"lease": None, "addendums": []})
        if "addendum" in kind:
            g["addendums"].append(t)
        elif g["lease"] is None:
            g["lease"] = t

    def build_segment(lease, addendums, amt_gid):
        base = lease or (addendums[0] if addendums else None)
        if not base: return None
        amount = cf_value(base, amt_gid)
        start  = cf_value(base, start_gid)
        end    = cf_value(base, end_gid)
        for add in addendums:
            ae = cf_value(add, end_gid)
            aa = cf_value(add, amt_gid)
            if ae and (end is None or ae > end): end = ae
            if aa and aa > 0: amount = aa  # latest addendum wins
        if amount is None or not start or not end: return None
        try:
            return {"start": date.fromisoformat(start),
                    "end":   date.fromisoformat(end),
                    "amount": float(amount)}
        except ValueError:
            return None

    # Handle inconsistent prefix naming: if a project has exactly ONE construction
    # lease (any prefix) and ONE homeowner lease (any prefix), they belong to the
    # same house even if prefixes differ (e.g. "Construction Lease" + "A: Homeowner Lease").
    all_c_prefixes = [pre for pre, types in groups.items() if types.get("construction", {}).get("lease")]
    all_h_prefixes = [pre for pre, types in groups.items() if types.get("homeowner",    {}).get("lease")]
    if len(all_c_prefixes) == 1 and len(all_h_prefixes) == 1 and all_c_prefixes[0] != all_h_prefixes[0]:
        # Merge the lone homeowner into the construction's prefix
        c_pre = all_c_prefixes[0]; h_pre = all_h_prefixes[0]
        groups[c_pre]["homeowner"] = groups[h_pre].pop("homeowner")
        if not groups[h_pre]:
            del groups[h_pre]

    houses = []
    for pre, types in groups.items():
        cs = types.get("construction", {"lease": None, "addendums": []})
        hs = types.get("homeowner",    {"lease": None, "addendums": []})
        ar_seg = build_segment(cs["lease"], cs["addendums"], ar_gid)
        ap_seg = build_segment(hs["lease"], hs["addendums"], ap_gid)

        # Fallback: if homeowner segment failed due to missing dates but has
        # an A/P value, use the construction lease's dates for it.
        if ap_seg is None and ar_seg is not None and hs["lease"]:
            h_amt = cf_value(hs["lease"], ap_gid)
            if h_amt and h_amt > 0:
                ap_seg = {"start": ar_seg["start"], "end": ar_seg["end"],
                          "amount": float(h_amt)}
            else:
                for add in hs["addendums"]:
                    aa = cf_value(add, ap_gid)
                    if aa and aa > 0:
                        ap_seg = {"start": ar_seg["start"], "end": ar_seg["end"],
                                  "amount": float(aa)}
                        break

        crew = 0
        if cs["lease"]:
            c = cf_value(cs["lease"], crew_gid)
            if c: crew = int(c)
        if ar_seg or ap_seg:
            houses.append({"prefix": pre, "ar": ar_seg, "ap": ap_seg, "crew": crew})
    return houses

def monthly_from_segments(segs, yr, mo):
    """Sum pro-rated monthly amounts across every segment active in (yr,mo)."""
    total = 0.0
    for seg in segs:
        total += prorate(seg["start"], seg["end"], seg["amount"], yr, mo)
    return round(total, 2)

# ─── Build a dashboard row for a project ─────────────────────────────────────

def make_row(name, salesperson, ar_segs, ap_segs, crew, *, source, gid=None):
    if not ar_segs and not ap_segs:
        return None

    all_starts = [s["start"] for s in ar_segs + ap_segs]
    all_ends   = [s["end"]   for s in ar_segs + ap_segs]
    start = min(all_starts); end = max(all_ends)

    # Determine current ("base") monthly amounts using a reference month = today's month
    today = date.today()
    base_ar = sum(s["amount"] for s in ar_segs if s["start"] <= today <= s["end"]) \
              or (sum(s["amount"] for s in ar_segs) if ar_segs else 0)
    base_ap = sum(s["amount"] for s in ap_segs if s["start"] <= today <= s["end"]) \
              or (sum(s["amount"] for s in ap_segs) if ap_segs else 0)

    monthly = {}
    for yr, mo in MONTHS:
        key = f"{yr}-{mo:02d}"
        ar = monthly_from_segments(ar_segs, yr, mo)
        ap = monthly_from_segments(ap_segs, yr, mo)
        if ar > 0 or ap > 0:
            first = date(yr, mo, 1); last = date(yr, mo, monthrange(yr, mo)[1])
            month_active = any(not (s["start"] > last or s["end"] < first) for s in ar_segs)
            monthly[key] = {
                "crew": crew if month_active else 0,
                "ar":   ar,
                "ap":   ap,
                "net_gp": round((ar - ap) * (1 - COMMISSION_RATE), 2),
            }

    t_ar = sum(v["ar"]     for k, v in monthly.items() if k.startswith("2026"))
    t_ap = sum(v["ap"]     for k, v in monthly.items() if k.startswith("2026"))
    t_gp = sum(v["net_gp"] for k, v in monthly.items() if k.startswith("2026"))

    m = re.search(r"\b(2\d{3})\b", name)
    return {
        "gid": gid, "name": name,
        "salesperson": salesperson or "Unknown",
        "project_number": m.group(1) if m else "",
        "status": compute_status(start, end, date.today()),
        "start_date": start.isoformat(),
        "end_date":   end.isoformat(),
        "base_ar": round(base_ar, 2),
        "base_ap": round(base_ap, 2),
        "base_crew": crew,
        "monthly": monthly,
        "total_2026": {"ar": round(t_ar,2), "ap": round(t_ap,2), "net_gp": round(t_gp,2)},
        "source": source,
    }

# ─── Main sync ───────────────────────────────────────────────────────────────

def sync():
    print(f"=== HHH Sync @ {datetime.now():%Y-%m-%d %H:%M} ===")

    if not os.path.exists(CF_GIDS_FILE):
        print(f"ERROR: {CF_GIDS_FILE} missing"); sys.exit(1)
    with open(CF_GIDS_FILE) as f: CF = json.load(f)

    ar_g    = CF["HHH: Monthly A/R"]
    ap_g    = CF["HHH: Monthly A/P"]
    start_g = CF["HHH: Start Date"]
    end_g   = CF["HHH: End Date"]
    crew_g  = CF["HHH: Crew Size"]
    sp_g    = CF["HHH: Salesperson"]

    # Find HHH-pattern projects
    all_projects = {}
    if ASANA_TOKEN:
        for p in asana("projects", {
            "workspace": WORKSPACE_GID, "archived":"false",
            "opt_fields":"name", "limit": 100,
        }):
            if not HHH_PROJECT_RE.search(p.get("name","")): continue
            if any(s in p["name"].lower() for s in SKIP_NAMES): continue
            all_projects[p["gid"]] = p
        for pgid in ("1206746778121935", "1207604445018695"):
            for p in asana(f"portfolios/{pgid}/items", {"opt_fields":"name"}):
                if p["gid"] in all_projects: continue
                if any(s in p["name"].lower() for s in SKIP_NAMES): continue
                all_projects[p["gid"]] = p
        print(f"Scanning {len(all_projects)} HHH-pattern projects…")

    rows = []
    seen_project_nums = set()

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

        houses = build_house_segments(tasks, ar_g, ap_g, start_g, end_g, crew_g)
        if not houses:
            continue

        # Determine salesperson once per project
        sp = None
        for t in tasks:
            sp = cf_value(t, sp_g)
            if sp: break
        if not sp:
            for s in SALESPEOPLE:
                if re.search(rf"\b{s}\b", pname, re.I): sp = s; break

        # Group houses by their construction end-date. Houses that share an end
        # date are aggregated into one row; different end dates → separate rows.
        # (This matches the master sheet structure: Miller Saint Peters = 1 row,
        #  Miller Lee's Summit = 3 rows, ARD Eastaboga = 4 phase rows.)
        by_end = {}
        for h in houses:
            ar_end = h["ar"]["end"] if h["ar"] else None
            ap_end = h["ap"]["end"] if h["ap"] else None
            # Cluster key: construction end date (preferred) or homeowner end date
            key = ar_end.isoformat() if ar_end else (ap_end.isoformat() if ap_end else "")
            by_end.setdefault(key, []).append(h)

        # Emit rows
        if len(by_end) == 1:
            # All houses share an end date → one aggregate row for the project
            hs = list(by_end.values())[0]
            ar_segs = [h["ar"] for h in hs if h["ar"]]
            ap_segs = [h["ap"] for h in hs if h["ap"]]
            crew_tot = sum(h["crew"] for h in hs)
            row = make_row(pname, sp, ar_segs, ap_segs, crew_tot,
                           source="asana_custom_fields", gid=pgid)
            if row: rows.append(row)
        else:
            # Multiple end-date groups → one row per group
            # Name each row by its distinguishing prefix(es) when possible
            for end_key, hs in by_end.items():
                prefixes = sorted({h["prefix"] for h in hs if h["prefix"] and h["prefix"] != "_main"})
                if not prefixes:
                    suffix = ""
                elif any(p.startswith("Phase") for p in prefixes):
                    suffix = " — " + ", ".join(p.replace("Phase","Phase ") for p in prefixes)
                elif len(prefixes) == 1:
                    suffix = f" [House {prefixes[0]}]"
                else:
                    suffix = f" [Houses {','.join(prefixes)}]"
                ar_segs = [h["ar"] for h in hs if h["ar"]]
                ap_segs = [h["ap"] for h in hs if h["ap"]]
                crew_tot = sum(h["crew"] for h in hs)
                row = make_row(pname + suffix, sp, ar_segs, ap_segs, crew_tot,
                               source="asana_custom_fields", gid=pgid)
                if row: rows.append(row)

        m = re.search(r"\b(2\d{3})\b", pname)
        if m: seen_project_nums.add(m.group(1))

    print(f"  → {len(rows)} rows from Asana projects with HHH fields")

    # Rising Sun: manually-edited monthly values (not from Asana)
    if os.path.exists(RISING_SUN_FILE):
        with open(RISING_SUN_FILE) as f: rs = json.load(f)
        rs_monthly = {}
        for key, v in rs.get("monthly", {}).items():
            ar = v.get("ar", 0); ap = v.get("ap", 0); crew = v.get("crew", 0)
            if ar > 0 or ap > 0:
                rs_monthly[key] = {
                    "crew": crew, "ar": round(ar, 2), "ap": round(ap, 2),
                    "net_gp": round((ar - ap) * (1 - COMMISSION_RATE), 2),
                }
        if rs_monthly:
            keys = sorted(rs_monthly.keys())
            t_ar = sum(v["ar"]     for k, v in rs_monthly.items() if k.startswith("2026"))
            t_ap = sum(v["ap"]     for k, v in rs_monthly.items() if k.startswith("2026"))
            t_gp = sum(v["net_gp"] for k, v in rs_monthly.items() if k.startswith("2026"))
            base_ar = rs_monthly[keys[-1]]["ar"]  # current monthly
            base_ap = rs_monthly[keys[-1]]["ap"]
            base_crew = rs_monthly[keys[-1]]["crew"]
            rs_row = {
                "name": rs.get("name", "Rising Sun Developing"),
                "salesperson": rs.get("salesperson", "HHH"),
                "project_number": "",
                "status": rs.get("status", "Active"),
                "start_date": keys[0] + "-01",
                "end_date":   keys[-1] + "-28",
                "base_ar": base_ar, "base_ap": base_ap, "base_crew": base_crew,
                "monthly": rs_monthly,
                "total_2026": {"ar": round(t_ar,2), "ap": round(t_ap,2), "net_gp": round(t_gp,2)},
                "source": "rising_sun_manual",
                "editable": True,
            }
            rows.append(rs_row)
            print(f"  + Rising Sun (manual entry, {len(rs_monthly)} months)")

    # Fallback for projects that have no HHH custom fields yet (e.g. no project #)
    if os.path.exists(MASTER_FILE):
        with open(MASTER_FILE) as f: master = json.load(f)
        fallback_count = 0
        for p in master.get("projects", []):
            pname_lower = p["name"].lower()
            # Skip Rising Sun — handled separately above
            if "rising sun" in pname_lower or "rsd" in pname_lower:
                continue
            mnum = re.search(r"\b(2\d{3})\b", p["name"])
            if mnum and mnum.group(1) in seen_project_nums:
                continue
            if any(p["name"].lower() in r["name"].lower() for r in rows):
                continue
            s = date.fromisoformat(p["start_date"]); e = date.fromisoformat(p["end_date"])
            ar_segs = [{"start": s, "end": e, "amount": p["monthly_ar"]}]
            ap_segs = [{"start": s, "end": e, "amount": p["monthly_ap"]}]
            row = make_row(p["name"], p.get("salesperson"), ar_segs, ap_segs, p["crew"],
                           source="master_sheet_fallback")
            if row:
                rows.append(row); fallback_count += 1
        if fallback_count:
            print(f"  + {fallback_count} rows from master_data fallback")

    # Aggregate totals
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
            sp_summary.setdefault(r["salesperson"], {}).setdefault(key, {"ar":0,"ap":0,"commission":0})
            sp_summary[r["salesperson"]][key]["ar"] += v["ar"]
            sp_summary[r["salesperson"]][key]["ap"] += v["ap"]
            sp_summary[r["salesperson"]][key]["commission"] += round((v["ar"]-v["ap"])*COMMISSION_RATE,2)

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
    print(f"\n✓ {len(rows)} rows · {active} active · {upcoming} upcoming · {closed} closed")
    print(f"  sources: {asana_ct} Asana · {fallback} fallback")
    print("  Output → data/projects.json")

if __name__ == "__main__":
    sync()
