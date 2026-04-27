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
from datetime import date, datetime, timedelta
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

# Names hidden from the commissioned-sales views (leaderboard, commission table).
# These people manage projects but aren't on commission.
HIDE_FROM_SP_VIEWS = {"HHH", "David", "Unknown", ""}

# Asana "Client Success" project — source of maintenance tickets for the staff page.
CLIENT_SUCCESS_GID = "1211911843670288"
MAINT_SECTION_GIDS = {
    "1211911843670289": "General Updates",
    "1211911843670292": "Urgent Maintenance",
    "1211911843670291": "Emergency Maintenance",
    "1211911843670295": "Standard Maintenance",
}
# House letter extracted from maintenance task names: "Unit D - Roof Leak", "A: Internet", "House B - ..."
MAINT_HOUSE_RE = re.compile(r"^\s*(?:unit|house)?\s*([A-Z])\s*[:\-]", re.I)

# Google Sheet with daily KPI form submissions — one tab per rep, all publicly readable.
KPI_SHEET_ID = "1TlJbBoxmr9p81JT4ICHsl3gmz2Ih9Sc18L9sVayR2-w"
KPI_TABS     = ["Paul", "Zeke", "Logan", "Peyton"]
# Column indexes in each rep's daily form tab (header row is "Timestamp | Date of Submission | Your Name | ...").
KPI_COL_DATE     = 1
KPI_COL_CALLS    = 3
KPI_COL_MEETINGS = 4
KPI_COL_QUOTES   = 5
KPI_COL_DEALS    = 6
KPI_COL_NEPQ     = 7
KPI_TS_RE = re.compile(r"^\s*\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}")
# Targets per rep (hard-coded from the sheet's summary tables; adjust here if goals change).
KPI_TARGETS = {
    "Paul":   {"weekly": {"calls":75, "meetings":4, "quotes":3, "deals":1.25, "nepq":2.5},
               "monthly":{"calls":300,"meetings":16,"quotes":12,"deals":5, "nepq":10}},
    "Zeke":   {"weekly": {"calls":75, "meetings":4, "quotes":3, "deals":1.25, "nepq":2.5},
               "monthly":{"calls":300,"meetings":16,"quotes":12,"deals":5, "nepq":10}},
    "Logan":  {"weekly": {"calls":75, "meetings":3, "quotes":2, "deals":1.25, "nepq":2.5},
               "monthly":{"calls":300,"meetings":12,"quotes":8, "deals":5, "nepq":10}},
    "Peyton": {"weekly": {"calls":50, "meetings":1, "quotes":1, "deals":0.25,"nepq":2.5},
               "monthly":{"calls":200,"meetings":4, "quotes":4, "deals":1, "nepq":10}},
}

SKIP_NAMES = ["template", "general to do", "xxxx", "hard hat housing template",
              "1501 richmond", "4709 orlando", "1113 taborlake",
              # Rising Sun / RSD: all values come from data/manual_rows.json instead
              "rising sun", "rsd -", "rsd-"]
MANUAL_ROWS_FILE = "data/manual_rows.json"
RECONCILED_SNAPSHOT_FILE = "data/reconciled_snapshot.json"
VOID_TASK = ("did not send","did not use","cancelled",
             "back up","backup","not used","duplicate","did not",
             # Per Richard 2026-04-26: substring-match these to ignore the task.
             # "termin" covers terminate/terminated/termination/terminating.
             "termin","future","early","pet")

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
    Classify a task as construction_lease / construction_addendum /
    homeowner_lease / homeowner_addendum, or None.

    Permissive keyword match: any task whose name contains the pair
    (construction|homeowner) + (lease|addendum) is classified, EXCEPT
    those containing a VOID_TASK keyword (future/early/pet/terminate/etc.).

    This covers non-canonical names Richard surfaced in the 2026-04-26 audit:
      - "Construction Lease House - 10 Telluride Dr."
      - "Construction Lease A" (trailing letter)
      - "Construction Lease #1- FEMALE Unit"
      - "Send Construction lease for House 2"
      - "A: Homeowner Lease - Extension #1"
      - "Construction Addendum: Extension #N"
      - etc.

    The financial safeguards in build_house_segments() filter out reminder
    tasks (no $$ on a lease, no $$ AND no dates on an addendum), so things
    like "Send Addendum to Homeowner - Jack Young" or "Gather ACH info..."
    don't slip through and create phantom segments.
    """
    n = (name or "").strip().lower()
    if not n: return None
    if any(v in n for v in VOID_TASK): return None

    has_construction = bool(re.search(r"\bconstruction\b", n))
    has_homeowner    = bool(re.search(r"\bhomeowner\b", n))
    if has_construction == has_homeowner:  # both or neither → ambiguous, reject
        return None

    has_lease    = bool(re.search(r"\blease\b", n))
    has_addendum = bool(re.search(r"\baddendum\b", n))
    if has_lease == has_addendum:  # both or neither → reject
        return None

    role = "construction" if has_construction else "homeowner"
    kind = "lease"        if has_lease        else "addendum"
    return f"{role}_{kind}"

ROMAN = {"I":1,"II":2,"III":3,"IV":4,"V":5,"VI":6,"VII":7,"VIII":8,"IX":9,"X":10}

def prefix_of(name):
    """Extract the letter/phase grouping key from a task name."""
    # Multi-letter prefix like "A, B,C:" — take the first letter so it groups
    # with the other A-prefixed tasks. "A: Homeowner Lease" will still share
    # prefix "A" with "A, B,C: Construction Lease".
    m = re.match(r"^([A-Z])(?:\s*,\s*[A-Z])*\s*:", name)
    if m: return m.group(1).upper()
    # "Homeowner A Lease" / "Construction A Addendum"
    m = re.match(r"^(?:homeowner|construction)\s+([A-Z])\s+(?:lease|addendum)", name, re.I)
    if m: return m.group(1).upper()
    # Trailing letter: "Construction Lease A", "Homeowner Lease B - Lauren",
    # "Construction Lease B: (1 Male units)"
    m = re.match(r"^(?:homeowner|construction)\s+(?:lease|addendum)\s+([A-Z])\b", name, re.I)
    if m: return m.group(1).upper()
    # Quoted letter anywhere: `Lease "E"`, `House "E"`
    m = re.search(r'"([A-Z])"', name)
    if m: return m.group(1).upper()
    # House N / Houses #N: "Construction Addendum House 1", "Homeowner Addendum #2 Houses #1-5"
    m = re.search(r"\bhouses?\s*#?\s*(\d+)", name, re.I)
    if m: return f"House{m.group(1)}"
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

        # Skip ghost tasks so they don't create spurious prefix groups:
        #   • A LEASE task without any A/R or A/P contributes no financial data
        #     and its presence would fragment prefix groups
        #   • An ADDENDUM task is allowed to have only dates (for date-only
        #     extensions), but must have either an amount or dates
        is_lease = kind.endswith("_lease")
        has_money = cf_value(t, ar_gid) or cf_value(t, ap_gid)
        has_dates = cf_value(t, start_gid) or cf_value(t, end_gid)
        if is_lease and not has_money:
            continue
        if not is_lease and not (has_money or has_dates):
            continue

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

    # Does ANY prefix group in this project have a homeowner lease with A/P?
    # If yes, fallback #2 (use Construction Lease's A/P) must NOT trigger for
    # other groups, or we'd double-count (the ap from Construction + the ap
    # from the other group's homeowner lease sum together).
    project_has_homeowner_ap = False
    for pre, types in groups.items():
        hs = types.get("homeowner", {"lease": None, "addendums": []})
        if hs["lease"] and cf_value(hs["lease"], ap_gid):
            project_has_homeowner_ap = True; break
        for a in hs.get("addendums", []):
            if cf_value(a, ap_gid):
                project_has_homeowner_ap = True; break
        if project_has_homeowner_ap: break

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

        # Fallback 2: use Construction Lease's own A/P ONLY when the project
        # has no homeowner A/P anywhere else (covers ARD Phase N where the
        # aggregate is recorded on the Construction Lease Phase task). Skipping
        # this when any homeowner has A/P prevents double-counting (Heycon,
        # Stone and Lime where A/P is on BOTH the Construction Lease and a
        # Homeowner Lease).
        if (not ap_segs and ar_segs and cs["lease"]
                and not project_has_homeowner_ap):
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

        # Lexington-KY rule: A/P = 65% of A/R — but ONLY if no explicit A/P
        # was set on any lease/addendum. An explicit A/P (like Quality Walls
        # 2527's \$3,300 on Construction Lease) takes precedence.
        if rules["ap_pct"] is not None and ar > 0 and ap == 0:
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

    # Override base A/P if Lexington rule applies AND there's no explicit A/P
    if rules["ap_pct"] is not None and base_ar > 0 and base_ap == 0:
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

    # Load manual-rows project numbers to skip from Asana scan first
    manual_skip_nums = set()
    if os.path.exists(MANUAL_ROWS_FILE):
        with open(MANUAL_ROWS_FILE) as f: mr_preview = json.load(f)
        for item in mr_preview.get("rows", []):
            if item.get("skip_asana_match"):
                manual_skip_nums.add(str(item["skip_asana_match"]))

    # Find HHH-pattern projects
    all_projects = {}
    if ASANA_TOKEN:
        for p in asana("projects", {
            "workspace": WORKSPACE_GID, "archived":"false",
            "opt_fields":"name", "limit": 100,
        }):
            name = p.get("name","")
            if not HHH_PROJECT_RE.search(name): continue
            if any(s in name.lower() for s in SKIP_NAMES): continue
            if any(num in name for num in manual_skip_nums): continue  # handled by manual_rows
            all_projects[p["gid"]] = p
        for pgid in ("1206746778121935", "1207604445018695"):
            for p in asana(f"portfolios/{pgid}/items", {"opt_fields":"name"}):
                if p["gid"] in all_projects: continue
                if any(s in p["name"].lower() for s in SKIP_NAMES): continue
                if any(num in p["name"] for num in manual_skip_nums): continue
                all_projects[p["gid"]] = p
        print(f"Scanning {len(all_projects)} HHH-pattern projects (skipping {len(manual_skip_nums)} manual)…")

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

    # Manual rows: projects whose values are hand-maintained (Rising Sun, ARD
    # time-varying aggregate, Peak Event consolidated, etc.). Each manual row
    # has its own `skip_asana_match` (project number to exclude from Asana
    # sync) to prevent double-counting.
    manual_skip_nums = set()
    if os.path.exists(MANUAL_ROWS_FILE):
        with open(MANUAL_ROWS_FILE) as f: mr = json.load(f)
        today = date.today()
        for item in mr.get("rows", []):
            if item.get("skip_asana_match"):
                manual_skip_nums.add(str(item["skip_asana_match"]))
            monthly_out = {}
            for key, v in item.get("monthly", {}).items():
                ar = v.get("ar", 0); ap = v.get("ap", 0); crew = v.get("crew", 0)
                reconciled = v.get("reconciled", False)
                if ar > 0 or ap > 0:
                    # Apply project rules to manual rows too (commission logic)
                    rules = project_rules(item.get("name", ""))
                    if rules["ap_pct"] is not None and ar > 0:
                        ap = round(ar * rules["ap_pct"], 2)
                    commission = monthly_commission(rules, ar, ap, crew)
                    monthly_out[key] = {
                        "crew": crew, "ar": round(ar, 2), "ap": round(ap, 2),
                        "commission": commission,
                        "net_gp": round(ar - ap - commission, 2),
                        "reconciled": bool(reconciled),
                    }
            if not monthly_out: continue
            keys = sorted(monthly_out.keys())
            t_ar = sum(v["ar"]     for k, v in monthly_out.items() if k.startswith("2026"))
            t_ap = sum(v["ap"]     for k, v in monthly_out.items() if k.startswith("2026"))
            t_gp = sum(v["net_gp"] for k, v in monthly_out.items() if k.startswith("2026"))
            rules = project_rules(item.get("name", ""))
            m_num = re.search(r"\b(2\d{3})\b", item.get("name",""))
            rows.append({
                "name": item.get("name"),
                "salesperson": item.get("salesperson", "Unknown"),
                "project_number": m_num.group(1) if m_num else "",
                "status": item.get("status", "Active"),
                "start_date": keys[0] + "-01",
                "end_date":   keys[-1] + "-28",
                "base_ar": monthly_out[keys[-1]]["ar"],
                "base_ap": monthly_out[keys[-1]]["ap"],
                "base_crew": monthly_out[keys[-1]]["crew"],
                "commission_model": rules["model"],
                "commission_rate":  rules["rate"],
                "monthly": monthly_out,
                "total_2026": {"ar": round(t_ar,2), "ap": round(t_ap,2), "net_gp": round(t_gp,2)},
                "source": "manual_row",
                "editable": True,
            })
            print(f"  + manual: {item.get('name','')[:55]}")
    # Also add manual skip numbers to the dynamic skip set below
    SKIP_NUMS_FROM_MANUAL = manual_skip_nums

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
            # Skip anything we're serving from manual_rows.json
            if mnum and mnum.group(1) in manual_skip_nums:
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

    # ── Apply reconciled-months snapshot (locks historical values) ──────────
    if os.path.exists(RECONCILED_SNAPSHOT_FILE):
        with open(RECONCILED_SNAPSHOT_FILE) as f: snap = json.load(f)
        snap_through = snap.get("reconciled_through", "")
        snap_rows = {sr["name"]: sr["monthly"] for sr in snap.get("rows", [])}
        locked_cells = 0
        for r in rows:
            snap_monthly = snap_rows.get(r["name"])
            if not snap_monthly: continue
            for key, v in snap_monthly.items():
                if key <= snap_through:
                    r["monthly"][key] = {**dict(v), "reconciled": True}
                    locked_cells += 1
            # Recompute 2026 totals after lockbox override
            t_ar = sum(v["ar"]     for k, v in r["monthly"].items() if k.startswith("2026"))
            t_ap = sum(v["ap"]     for k, v in r["monthly"].items() if k.startswith("2026"))
            t_gp = sum(v["net_gp"] for k, v in r["monthly"].items() if k.startswith("2026"))
            r["total_2026"] = {"ar": round(t_ar,2), "ap": round(t_ap,2), "net_gp": round(t_gp,2)}
        if locked_cells:
            print(f"  ⚑ lockbox: {locked_cells} historical cells frozen through {snap_through}")

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
                                                "salesperson": r.get("salesperson",""),
                                                "gid": r.get("gid"),
                                                "project_number": r.get("project_number")})
            elif d is not None and 30 < d <= 45:
                health["ending_in_31_45"].append({"name": r["name"], "end_date": r["end_date"], "days": d,
                                                   "salesperson": r.get("salesperson",""),
                                                   "gid": r.get("gid"),
                                                   "project_number": r.get("project_number")})
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

    # ── Subpage data files (sanitized views for Cloudflare-gated pages) ──────
    write_sales_view(out)
    write_maintenance_view(out)

    active   = sum(1 for r in rows if r["status"] == "Active")
    upcoming = sum(1 for r in rows if r["status"] == "Upcoming")
    closed   = sum(1 for r in rows if r["status"] == "Closed")
    asana_ct = sum(1 for r in rows if r.get("source") == "asana_custom_fields")
    fallback = sum(1 for r in rows if r.get("source") == "master_sheet_fallback")
    print(f"\n✓ {len(rows)} rows · {active} active · {upcoming} upcoming · {closed} closed")
    print(f"  sources: {asana_ct} Asana · {fallback} fallback")
    print("  Output → data/projects.json")

def fetch_kpi_data():
    """Parse each rep's summary tables directly from their Google Sheet tab.

    Each tab has three section headers we care about:
      - "{rep} Weekly"   — next 5 rows: Calls, 2nd Meetings, Quotes, Deals, NEPQ
                           columns: [label, target, W1, W2, W3, W4, W5]
      - "{rep} Monthly"  — next 5 rows: same KPIs
                           columns: [label, target, M1, M2, M3] (current quarter)
      - (Quarterly section is ignored — we show weekly + monthly only.)

    The sheet auto-rolls: in April W1..W5 are this month's weeks; in May the
    sheet resets. Similarly Monthly columns cover the current quarter.
    """
    import csv as _csv, io as _io

    today = date.today()
    month_name = today.strftime("%b %Y")
    quarter = (today.month - 1) // 3
    q_month_keys = [f"{today.year}-{quarter*3 + i + 1:02d}" for i in range(3)]

    kpi_keys = ["calls", "meetings", "quotes", "deals", "nepq"]

    def to_num(s):
        try: return float((s or "").replace(",", "").strip() or "0")
        except Exception: return 0.0

    def section_rows(rows, marker):
        for i, row in enumerate(rows):
            if row and (row[0] or "").strip() == marker:
                return rows[i+1 : i+1+len(kpi_keys)]
        return None

    def pick(row, idx):
        return to_num(row[idx]) if row and idx < len(row) else 0.0

    weekly, monthly = {}, {}

    for rep in KPI_TABS:
        url = (f"https://docs.google.com/spreadsheets/d/{KPI_SHEET_ID}"
               f"/gviz/tq?tqx=out:csv&headers=0&sheet={rep}")
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
        except Exception as e:
            print(f"  ! KPI fetch failed for {rep}: {e}")
            continue
        rows = list(_csv.reader(_io.StringIO(r.text)))

        wk_rows = section_rows(rows, f"{rep} Weekly")
        if wk_rows and len(wk_rows) >= len(kpi_keys):
            weekly[rep] = {}
            for w in range(5):  # W1..W5
                label = f"{month_name} W{w+1}"
                weekly[rep][label] = {
                    kpi_keys[i]: pick(wk_rows[i], 2 + w) for i in range(len(kpi_keys))
                }

        mo_rows = section_rows(rows, f"{rep} Monthly")
        if mo_rows and len(mo_rows) >= len(kpi_keys):
            monthly[rep] = {}
            for m_idx, m_key in enumerate(q_month_keys):
                monthly[rep][m_key] = {
                    kpi_keys[i]: pick(mo_rows[i], 2 + m_idx) for i in range(len(kpi_keys))
                }

    return {"weekly": weekly, "monthly": monthly, "targets": KPI_TARGETS}


def fetch_maintenance_tasks():
    """Pull maintenance tasks from the Asana 'Client Success' project.

    Each task's membership list contains one entry per project the task lives in.
    Severity comes from the section name inside Client Success (General Updates /
    Urgent / Emergency / Standard). The matched construction project comes from
    any OTHER membership. Status is a single-select custom field on the task.
    """
    fields = ",".join([
        "name", "completed", "created_at", "permalink_url",
        "custom_fields.name", "custom_fields.enum_value.name", "custom_fields.resource_subtype",
        "memberships.section.name", "memberships.section.gid",
        "memberships.project.name", "memberships.project.gid",
    ])
    try:
        tasks = asana(f"projects/{CLIENT_SUCCESS_GID}/tasks", params={"opt_fields": fields, "limit": 100})
    except Exception as e:
        print(f"  ! maintenance fetch failed: {e}")
        return []

    items = []
    for t in tasks:
        if t.get("completed"): continue
        severity, proj_name, proj_gid = None, None, None
        for m in t.get("memberships", []):
            sec  = m.get("section") or {}
            proj = m.get("project") or {}
            if proj.get("gid") == CLIENT_SUCCESS_GID:
                severity = MAINT_SECTION_GIDS.get(sec.get("gid")) or severity
            elif proj.get("gid"):
                proj_gid = proj["gid"]; proj_name = proj.get("name")
        if not severity:
            continue  # Task is in Finance/Other/Templates/old-Resolved — not a maintenance ticket

        status = None
        for cf in t.get("custom_fields", []):
            if cf.get("name") == "Status" and cf.get("resource_subtype") == "enum":
                ev = cf.get("enum_value")
                status = ev.get("name") if isinstance(ev, dict) else None
                break

        hm = MAINT_HOUSE_RE.match(t.get("name", "") or "")
        house = hm.group(1).upper() if hm else None

        items.append({
            "task_id":    t.get("gid"),
            "name":       t.get("name"),
            "severity":   severity,
            "status":     status or "—",
            "project":    proj_name or "(unmatched)",
            "project_gid": proj_gid,
            "house":      house,
            "created_at": t.get("created_at"),
            "url":        t.get("permalink_url"),
        })
    return items

def compute_ar_aging(projects):
    """A/R aging buckets per salesperson, derived from data/qb_status.json.

    Buckets by days-past-due:
      current (not yet due), 1-30, 31-60, 61-90, 90+
    Returns: { salesperson: {bucket: {count, amount, invoices:[{...}]}} }
    Unpaid invoices only (balance > 0). Invoices without a due_date skipped.
    """
    path = "data/qb_status.json"
    if not os.path.exists(path): return {}
    try:
        with open(path) as f:
            qb = json.load(f)
    except Exception:
        return {}

    ar = qb.get("ar_status_by_project", {})
    if not ar: return {}

    today = date.today()
    sp_by_project = {p["name"]: (p.get("salesperson") or "Unknown") for p in projects}

    def bucket_for(days):
        if days <  0: return "current"
        if days <= 30: return "1-30"
        if days <= 60: return "31-60"
        if days <= 90: return "61-90"
        return "90+"

    BUCKETS = ["current", "1-30", "31-60", "61-90", "90+"]
    aging = {}

    for proj_name, months_dict in ar.items():
        sp = sp_by_project.get(proj_name, "Unknown")
        if sp in HIDE_FROM_SP_VIEWS: continue
        for invoices in months_dict.values():
            for inv in invoices:
                bal = float(inv.get("balance", 0) or 0)
                if bal <= 0.001: continue
                due_str = inv.get("due_date")
                if not due_str: continue
                try: due = date.fromisoformat(due_str)
                except Exception: continue

                days_overdue = (today - due).days
                bkt = bucket_for(days_overdue)
                agent = aging.setdefault(sp, {b: {"count":0, "amount":0.0, "invoices":[]} for b in BUCKETS})
                agent[bkt]["count"]  += 1
                agent[bkt]["amount"] += bal
                agent[bkt]["invoices"].append({
                    "project":      proj_name,
                    "invoice_id":   inv.get("invoice_id"),
                    "doc_number":   inv.get("doc_number"),
                    "amount":       round(bal, 2),
                    "days_overdue": days_overdue,
                    "due_date":     due_str,
                })

    # Round bucket totals + sort invoices within each bucket by days desc
    for sp, bkts in aging.items():
        for bkt in bkts.values():
            bkt["amount"] = round(bkt["amount"], 2)
            bkt["invoices"].sort(key=lambda x: -x["days_overdue"])

    return aging


def write_sales_view(master):
    """Write data/sales.json — leaderboard + commission-by-month for sales team.

    Includes per-project monthly A/R, A/P, commission, net GP — sales team is
    allowed to see each other's numbers per Richard's call.
    """
    sales_projects = []
    for r in master["projects"]:
        sp = r.get("salesperson") or "Unknown"
        if sp in HIDE_FROM_SP_VIEWS: continue
        sales_projects.append({
            "name":        r["name"],
            "salesperson": sp,
            "status":      r["status"],
            "start_date":  r.get("start_date"),
            "end_date":    r.get("end_date"),
            "monthly":     {k: {"ar": v.get("ar",0), "ap": v.get("ap",0),
                                "commission": v.get("commission",0),
                                "net_gp": v.get("net_gp",0)}
                            for k, v in r.get("monthly", {}).items()},
        })
    kpis = fetch_kpi_data()
    ar_aging = compute_ar_aging(master["projects"])

    out = {
        "generated_at":    master["generated_at"],
        "months":          master["months"],
        "commission_rate": master["commission_rate"],
        "projects":        sales_projects,
        "kpis":            kpis,
        "ar_aging":        ar_aging,
    }
    with open("data/sales.json", "w") as f:
        json.dump(out, f, indent=2, default=str)
    kpi_reps = len(kpis["weekly"])
    print(f"  Output → data/sales.json  ({len(sales_projects)} projects · {kpi_reps} KPI reps)")

def write_maintenance_view(master):
    """Write data/maintenance.json — project list + Asana maintenance tickets.

    Staff page: no financial fields. Shows active/upcoming projects, ending-soon
    warnings, and open maintenance tickets pulled from the Asana Client Success
    project.
    """
    maint_projects = []
    for r in master["projects"]:
        if r["status"] not in ("Active", "Upcoming"): continue
        maint_projects.append({
            "name":         r["name"],
            "status":       r["status"],
            "start_date":   r.get("start_date"),
            "end_date":     r.get("end_date"),
            "project_number": r.get("project_number"),
        })

    ending = []
    for x in master["health"].get("ending_in_30", []):
        ending.append({"name": x["name"], "end_date": x["end_date"], "days": x["days"], "urgency": "30",
                       "gid": x.get("gid"), "project_number": x.get("project_number")})
    for x in master["health"].get("ending_in_31_45", []):
        ending.append({"name": x["name"], "end_date": x["end_date"], "days": x["days"], "urgency": "45",
                       "gid": x.get("gid"), "project_number": x.get("project_number")})

    # ── Upcoming Check-ins: 48 hrs before project start, window −2 to +14 days ──
    today = date.today()
    checkins = []
    for r in master["projects"]:
        if r["status"] != "Upcoming": continue
        sd_str = r.get("start_date")
        if not sd_str: continue
        try: sd = date.fromisoformat(sd_str)
        except Exception: continue
        checkin_date = sd - timedelta(days=2)
        days = (checkin_date - today).days
        if days < -2 or days > 14: continue

        house_m = re.search(r"\[House\s+([A-Z])\]", r["name"])
        house = house_m.group(1) if house_m else None
        clean = re.sub(r"\s*\[House\s+[A-Z]\]\s*$", "", r["name"])

        checkins.append({
            "project":    clean,
            "house":      house,
            "start_date": sd_str,
            "date":       checkin_date.isoformat(),
            "days":       days,
            "salesperson": r.get("salesperson", ""),
            "gid":           r.get("gid"),
            "project_number": r.get("project_number"),
        })
    checkins.sort(key=lambda x: x["days"])

    tickets = fetch_maintenance_tasks()

    out = {
        "generated_at":      master["generated_at"],
        "projects":          maint_projects,
        "ending_soon":       ending,
        "checkins":          checkins,
        "maintenance_tasks": tickets,
    }
    with open("data/maintenance.json", "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"  Output → data/maintenance.json  ({len(maint_projects)} projects · {len(tickets)} open tickets)")


if __name__ == "__main__":
    sync()
