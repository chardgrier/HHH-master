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

# ─── Per-project commission / A/P rules ──────────────────────────────────────
# Special rules by project-name pattern:
#   • Rising Sun / HousHak     → no commission
#   • Lexington, KY projects   → A/P = 65% of A/R, commission $100/crew/month
#   • Everything else          → 20% of gross profit
def project_rules(name):
    n = (name or "").lower()
    if "rising sun" in n or "rsd" in n or "houshak" in n:
        return {"model": "none", "rate": 0, "ap_pct": None}
    if "lexington" in n and "ky" in n:
        return {"model": "per_crew", "rate": 100, "ap_pct": 0.65}
    return {"model": "percent", "rate": COMMISSION_RATE, "ap_pct": None}

def monthly_commission(rules, ar, ap, crew):
    if rules["model"] == "none":
        return 0.0
    if rules["model"] == "per_crew":
        return round(rules["rate"] * crew, 2)
    return round((ar - ap) * rules["rate"], 2)

SALESPEOPLE = ["Paul","Zeke","Matt","Logan","David","Charlie","Peyton"]
HHH_PROJECT_RE = re.compile(rf"\b({'|'.join(SALESPEOPLE)})\b.*?\d{{4}}", re.IGNORECASE)

SKIP_NAMES = ["template", "general to do", "xxxx", "hard hat housing template",
              "1501 richmond", "4709 orlando", "1113 taborlake",
              # Rising Sun / RSD: all values come from data/rising_sun.json instead
              "rising sun", "rsd -", "rsd-"]
RISING_SUN_FILE = "data/rising_sun.json"
VOID_TASK = ("did not send","did not use","cancelled","termination","terminated",
             "back up","backup","not used","duplicate","did not")

# Dashboard months: Aug 2025 → Dec 2027 (covers multi-year projects like Miller Lee's Summit C, Heycon, Power Design Savannah)
MONTHS = []
y, m = 2025, 8
while (y, m) <= (2027, 12):
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
    # Phase suffix: "Phase 1", "Phase I", "- Phase I", etc.
    phase_suffix = r"(?:\s*[-–]?\s*phase\s+(?:\d+|[ivxlcm]+))?"
    patterns = [
        (rf"^(?:[a-z]\s*:\s*|phase\s+\d+\s+)?construction\s+addendum(?:\s*#?\s*\d+)?{phase_suffix}\s*$", "construction_addendum"),
        (rf"^(?:[a-z]\s*:\s*|phase\s+\d+\s+)?homeowner\s+addendum(?:\s*#?\s*\d+)?{phase_suffix}\s*$",    "homeowner_addendum"),
        # "Homeowner A Addendum" / "Homeowner A Addendum #1"
        (rf"^homeowner\s+[a-z]\s+addendum(?:\s*#?\s*\d+)?{phase_suffix}\s*$", "homeowner_addendum"),
        (rf"^construction\s+[a-z]\s+addendum(?:\s*#?\s*\d+)?{phase_suffix}\s*$", "construction_addendum"),
        (rf"^(?:[a-z]\s*:\s*)?construction\s+lease{phase_suffix}\s*$",          "construction_lease"),
        (rf"^(?:[a-z]\s*:\s*)?homeowner\s+lease(?:{phase_suffix}|\s*[-–]\s*[a-z])?\s*$", "homeowner_lease"),
        # "Homeowner A Lease - Phase I" etc. (letter in middle + phase suffix)
        (rf"^homeowner\s+[a-z]\s+lease{phase_suffix}\s*$",    "homeowner_lease"),
        (rf"^construction\s+[a-z]\s+lease{phase_suffix}\s*$", "construction_lease"),
    ]
    for pat, kind in patterns:
        if re.match(pat, n):
            return kind
    return None

ROMAN = {"I":1,"II":2,"III":3,"IV":4,"V":5,"VI":6,"VII":7,"VIII":8,"IX":9,"X":10}

def prefix_of(name):
    """Extract the letter/phase grouping key from a task name."""
    m = re.match(r"^([A-Z])\s*:", name)
    if m: return m.group(1).upper()
    m = re.match(r"^(?:homeowner|construction)\s+([A-Z])\s+(?:lease|addendum)", name, re.I)
    if m: return m.group(1).upper()
    m = re.search(r"phase\s+(\d+)", name, re.I)
    if m: return f"Phase{m.group(1)}"
    m = re.search(r"phase\s+([IVX]+)\b", name, re.I)
    if m:
        roman = m.group(1).upper()
        if roman in ROMAN: return f"Phase{ROMAN[roman]}"
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
      [{"prefix": "A", "ar_segs": [seg,...], "ap_segs": [seg,...], "crew": int}]

    Each segment = {start,end,amount}. Addendums with the same rate as the
    lease extend its end date (single segment). Addendums with a different rate
    create a new segment (earlier segment is trimmed to just before the
    addendum's start). This supports time-varying rates like Benco 2551
    ($10,900 → $6,900) and ARD Phase 2/3/4.
    """
    from datetime import timedelta

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

    def build_segments(lease, addendums, amt_gid):
        """Returns a list of non-overlapping segments."""
        segments = []
        # Start with the lease
        if lease:
            amt = cf_value(lease, amt_gid)
            s   = cf_value(lease, start_gid)
            e   = cf_value(lease, end_gid)
            if amt and s and e:
                try:
                    segments.append({"start": date.fromisoformat(s),
                                     "end":   date.fromisoformat(e),
                                     "amount": float(amt)})
                except ValueError: pass

        # Sort addendums by start date and apply each in order
        ads = []
        for a in addendums:
            s = cf_value(a, start_gid)
            if s:
                try: ads.append((a, date.fromisoformat(s)))
                except ValueError: pass
        ads.sort(key=lambda x: x[1])

        for add, a_start in ads:
            a_amt_raw = cf_value(add, amt_gid)
            a_end_s   = cf_value(add, end_gid)
            if not a_end_s: continue
            try:    a_end = date.fromisoformat(a_end_s)
            except ValueError: continue

            if not a_amt_raw or a_amt_raw == 0:
                # Date-only extension: extend latest segment's end
                if segments:
                    segments[-1]["end"] = max(segments[-1]["end"], a_end)
                continue

            a_amt = float(a_amt_raw)
            # Same rate as latest? Just extend end date.
            if segments and abs(segments[-1]["amount"] - a_amt) < 0.01:
                segments[-1]["end"] = max(segments[-1]["end"], a_end)
            else:
                # Different rate: trim previous if it overlaps, then append new segment
                if segments and segments[-1]["end"] >= a_start:
                    segments[-1]["end"] = a_start - timedelta(days=1)
                segments.append({"start": a_start, "end": a_end, "amount": a_amt})

        return segments

    # Reconcile orphan addendums: if an addendum's prefix group has no lease
    # of the same type, merge it into the group that DOES have that lease
    # (when there's exactly one such lease in the project). Handles the case
    # where naming is inconsistent, e.g. "Homeowner Lease" (no prefix) +
    # "A: Homeowner Addendum #1" (with prefix) — they belong together.
    for gtype in ("construction", "homeowner"):
        # Identify the sole lease group of this type, if any
        groups_with_lease = [pre for pre, types in groups.items()
                             if types.get(gtype, {}).get("lease")]
        if len(groups_with_lease) != 1:
            continue
        target_pre = groups_with_lease[0]
        target = groups[target_pre].setdefault(gtype, {"lease": None, "addendums": []})
        # Move addendums from any OTHER prefix group into the target
        for pre in list(groups.keys()):
            if pre == target_pre: continue
            g = groups[pre].get(gtype)
            if not g: continue
            if g["lease"] is None and g["addendums"]:
                target["addendums"].extend(g["addendums"])
                g["addendums"] = []

    # Clean up groups that now have no lease and no addendums after reconciling
    for pre in list(groups.keys()):
        for gtype in list(groups[pre].keys()):
            g = groups[pre][gtype]
            if g["lease"] is None and not g["addendums"]:
                del groups[pre][gtype]
        if not groups[pre]:
            del groups[pre]

    # Handle inconsistent prefix naming: if a project has exactly ONE construction
    # lease (any prefix) and ONE homeowner lease (any prefix) in different prefix
    # groups, merge the lone homeowner into the construction's prefix.
    all_c_prefixes = [pre for pre, types in groups.items() if types.get("construction", {}).get("lease")]
    all_h_prefixes = [pre for pre, types in groups.items() if types.get("homeowner",    {}).get("lease")]
    if len(all_c_prefixes) == 1 and len(all_h_prefixes) == 1 and all_c_prefixes[0] != all_h_prefixes[0]:
        c_pre = all_c_prefixes[0]; h_pre = all_h_prefixes[0]
        groups[c_pre]["homeowner"] = groups[h_pre].pop("homeowner")
        if not groups[h_pre]:
            del groups[h_pre]

    houses = []
    for pre, types in groups.items():
        cs = types.get("construction", {"lease": None, "addendums": []})
        hs = types.get("homeowner",    {"lease": None, "addendums": []})
        ar_segs = build_segments(cs["lease"], cs["addendums"], ar_gid)
        ap_segs = build_segments(hs["lease"], hs["addendums"], ap_gid)

        # Fallback 1: if homeowner has no A/P but a lease exists with dates,
        # try to pull A/P from the homeowner lease/addendum tasks themselves.
        if not ap_segs and ar_segs and hs["lease"]:
            h_amt = cf_value(hs["lease"], ap_gid)
            if h_amt and h_amt > 0:
                ap_segs = [{"start": ar_segs[0]["start"],
                            "end":   ar_segs[-1]["end"],
                            "amount": float(h_amt)}]

        # Fallback 2: if no homeowner lease in this group AND the construction
        # lease has an A/P value, use it (covers ARD Phase N cases where A/P is
        # on the Construction Lease aggregate).
        if not ap_segs and ar_segs and cs["lease"]:
            c_ap = cf_value(cs["lease"], ap_gid)
            if c_ap and c_ap > 0:
                ap_segs = [{"start": ar_segs[0]["start"],
                            "end":   ar_segs[-1]["end"],
                            "amount": float(c_ap)}]

        # ─── Auto-extension rule ────────────────────────────────────────────
        # Construction lease dates are the master range for the house.
        # If A/P doesn't cover the full construction range (no addendum exists
        # but construction extends), extrapolate the nearest homeowner rate.
        # Explicit homeowner addendum dates already produced their own segments
        # above, so they take precedence (they shrink the "gap" we fill here).
        if ar_segs and ap_segs:
            ar_start = min(s["start"] for s in ar_segs)
            ar_end   = max(s["end"]   for s in ar_segs)
            # Extrapolate backward: if construction starts before first A/P,
            # extend the earliest A/P segment's start backwards.
            first_ap = min(ap_segs, key=lambda s: s["start"])
            if ar_start < first_ap["start"]:
                first_ap["start"] = ar_start
            # Extend forward: if construction ends after last A/P, extend the
            # latest A/P segment's end to match construction end.
            last_ap = max(ap_segs, key=lambda s: s["end"])
            if ar_end > last_ap["end"]:
                last_ap["end"] = ar_end

        crew = 0
        if cs["lease"]:
            c = cf_value(cs["lease"], crew_gid)
            if c: crew = int(c)
        if ar_segs or ap_segs:
            houses.append({"prefix": pre, "ar_segs": ar_segs, "ap_segs": ap_segs, "crew": crew})
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

    rules = project_rules(name)

    all_starts = [s["start"] for s in ar_segs + ap_segs]
    all_ends   = [s["end"]   for s in ar_segs + ap_segs]
    start = min(all_starts); end = max(all_ends)

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

        # Lexington-KY rule: A/P = 65% of A/R (overrides whatever came from leases)
        if rules["ap_pct"] is not None and ar > 0:
            ap = round(ar * rules["ap_pct"], 2)

        if ar > 0 or ap > 0:
            first = date(yr, mo, 1); last = date(yr, mo, monthrange(yr, mo)[1])
            month_active = any(not (s["start"] > last or s["end"] < first) for s in ar_segs)
            month_crew = crew if month_active else 0
            commission = monthly_commission(rules, ar, ap, month_crew)
            monthly[key] = {
                "crew": month_crew,
                "ar":   ar,
                "ap":   ap,
                "commission": commission,
                "net_gp": round(ar - ap - commission, 2),
            }

    # Override base A/P if Lexington rule applies
    if rules["ap_pct"] is not None and base_ar > 0:
        base_ap = round(base_ar * rules["ap_pct"], 2)

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
        "commission_model": rules["model"],
        "commission_rate":  rules["rate"],
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

        # Group houses by their LATEST construction end-date.
        def house_end(h):
            ends = [s["end"] for s in h["ar_segs"]] + [s["end"] for s in h["ap_segs"]]
            return max(ends).isoformat() if ends else ""
        by_end = {}
        for h in houses:
            by_end.setdefault(house_end(h), []).append(h)

        if len(by_end) == 1:
            hs = list(by_end.values())[0]
            ar_segs = [s for h in hs for s in h["ar_segs"]]
            ap_segs = [s for h in hs for s in h["ap_segs"]]
            crew_tot = sum(h["crew"] for h in hs)
            row = make_row(pname, sp, ar_segs, ap_segs, crew_tot,
                           source="asana_custom_fields", gid=pgid)
            if row: rows.append(row)
        else:
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
                ar_segs = [s for h in hs for s in h["ar_segs"]]
                ap_segs = [s for h in hs for s in h["ap_segs"]]
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
            reconciled = v.get("reconciled", False)
            if ar > 0 or ap > 0:
                # Rising Sun: no commission
                rs_monthly[key] = {
                    "crew": crew, "ar": round(ar, 2), "ap": round(ap, 2),
                    "commission": 0,
                    "net_gp": round(ar - ap, 2),
                    "reconciled": bool(reconciled),
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
                "commission_model": "none",
                "commission_rate": 0,
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
        ar   = sum(r["monthly"].get(key, {}).get("ar",         0) for r in rows)
        ap   = sum(r["monthly"].get(key, {}).get("ap",         0) for r in rows)
        crew = sum(r["monthly"].get(key, {}).get("crew",       0) for r in rows)
        gp   = sum(r["monthly"].get(key, {}).get("net_gp",     0) for r in rows)
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
            sp_summary[r["salesperson"]][key]["commission"] += v.get("commission", 0)

    # ── Health / alerts metadata ─────────────────────────────────────────────
    today = date.today()
    def days_until(end_str):
        try: return (date.fromisoformat(end_str) - today).days
        except: return None
    health = {
        "missing_ap":      [],   # Active/Upcoming with A/R > 0 but A/P == 0
        "missing_crew":    [],   # Active/Upcoming with A/R > 0 but crew == 0
        "ap_exceeds_ar":   [],   # Sanity-check: A/P > A/R (parser gone wrong)
        "ending_in_30":    [],   # Active projects ending within 30 days
        "ending_in_31_45": [],   # Active projects ending 31-45 days out
    }
    for r in rows:
        if r["status"] in ("Active", "Upcoming"):
            if r.get("base_ar", 0) > 0 and r.get("base_ap", 0) == 0:
                health["missing_ap"].append({"name": r["name"], "ar": r["base_ar"]})
            if r.get("base_ar", 0) > 0 and r.get("base_crew", 0) == 0:
                health["missing_crew"].append({"name": r["name"], "ar": r["base_ar"]})
            if r.get("base_ap", 0) > r.get("base_ar", 0) * 1.1 and r.get("base_ar", 0) > 0:
                health["ap_exceeds_ar"].append({"name": r["name"], "ar": r["base_ar"], "ap": r["base_ap"]})
        if r["status"] == "Active" and r.get("end_date"):
            d = days_until(r["end_date"])
            if d is not None and 0 < d <= 30:
                health["ending_in_30"].append({"name": r["name"], "end_date": r["end_date"], "days": d,
                                                "salesperson": r.get("salesperson","")})
            elif d is not None and 30 < d <= 45:
                health["ending_in_31_45"].append({"name": r["name"], "end_date": r["end_date"], "days": d,
                                                   "salesperson": r.get("salesperson","")})
    # Sort alert lists by urgency
    health["ending_in_30"].sort(key=lambda x: x["days"])
    health["ending_in_31_45"].sort(key=lambda x: x["days"])

    out = {
        "generated_at": datetime.now().isoformat(),
        "months": [f"{y}-{m:02d}" for y, m in MONTHS],
        "commission_rate": COMMISSION_RATE,
        "projects": rows,
        "monthly_totals": monthly_totals,
        "salesperson_summary": sp_summary,
        "health": health,
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
