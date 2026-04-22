#!/usr/bin/env python3
"""
Weekly Asana data-quality scanner.

Scans every HHH-pattern project and flags:
  • Tasks whose names LOOK like leases/addendums but don't match the
    canonical naming patterns the sync parser recognizes.
  • Canonical lease/addendum tasks that are missing one or more of the
    6 HHH custom fields (Monthly A/R, A/P, Start, End, Crew, Salesperson).

Output: data/parse_issues.json — consumed by the dashboard.
"""
import os, json, re, sys, requests, time
from datetime import datetime

TOK = os.environ.get("ASANA_TOKEN", "")
if not TOK: print("ERROR: ASANA_TOKEN not set"); sys.exit(1)
WS = "1203487090849714"
BASE = "https://app.asana.com/api/1.0"
H = {"Authorization": f"Bearer {TOK}"}

with open("data/custom_field_gids.json") as f: CF = json.load(f)

SALESPEOPLE = ["Paul","Zeke","Matt","Logan","David","Charlie","Peyton"]
HHH_PROJECT_RE = re.compile(rf"\b({'|'.join(SALESPEOPLE)})\b.*?\d{{4}}", re.I)
SKIP_NAMES = ["template","general to do","xxxx","hard hat housing template","rising sun","rsd -","rsd-"]
VOID = ("did not send","did not use","cancelled","terminated","termination","backup","back up","not used","duplicate","did not")

# Canonical-match patterns (same as sync.py)
phase_suffix = r"(?:\s*[-–]?\s*phase\s+(?:\d+|[ivxlcm]+))?"
CANONICAL = [
    rf"^(?:[a-z]\s*:\s*|phase\s+\d+\s+)?construction\s+addendum(?:\s*#?\s*\d+)?{phase_suffix}\s*$",
    rf"^(?:[a-z]\s*:\s*|phase\s+\d+\s+)?homeowner\s+addendum(?:\s*#?\s*\d+)?{phase_suffix}\s*$",
    rf"^homeowner\s+[a-z]\s+addendum(?:\s*#?\s*\d+)?{phase_suffix}\s*$",
    rf"^construction\s+[a-z]\s+addendum(?:\s*#?\s*\d+)?{phase_suffix}\s*$",
    rf"^(?:[a-z]\s*:\s*)?construction\s+lease{phase_suffix}\s*$",
    rf"^(?:[a-z]\s*:\s*)?homeowner\s+lease(?:{phase_suffix}|\s*[-–]\s*[a-z])?\s*$",
    rf"^homeowner\s+[a-z]\s+lease{phase_suffix}\s*$",
    rf"^construction\s+[a-z]\s+lease{phase_suffix}\s*$",
]

def is_canonical(name):
    return any(re.match(p, name.strip(), re.I) for p in CANONICAL)

def looks_like_lease(name):
    low = name.lower()
    return (("construction" in low or "homeowner" in low)
            and ("lease" in low or "addendum" in low)
            and not any(v in low for v in VOID))

def paginate(endpoint, params=None):
    url = f"{BASE}/{endpoint}"; out = []
    while url:
        r = requests.get(url, headers=H, params=params, timeout=30)
        if r.status_code == 429: time.sleep(int(r.headers.get("Retry-After",5))); continue
        r.raise_for_status()
        d = r.json(); out.extend(d.get("data", []))
        nxt = d.get("next_page")
        url = nxt["uri"] if nxt else None; params = None
    return out

def main():
    print(f"=== Scanning Asana @ {datetime.now():%Y-%m-%d %H:%M} ===")
    projects = [p for p in paginate("projects", {
        "workspace": WS, "archived":"false","opt_fields":"name,permalink_url","limit":100
    }) if HHH_PROJECT_RE.search(p.get("name",""))
        and not any(s in p["name"].lower() for s in SKIP_NAMES)]

    print(f"Scanning {len(projects)} HHH projects…")

    unmatched_names = []   # Tasks that look like leases but don't match canonical patterns
    missing_fields  = []   # Canonical tasks with missing HHH custom fields

    ar_g = CF["HHH: Monthly A/R"]; ap_g = CF["HHH: Monthly A/P"]
    st_g = CF["HHH: Start Date"];  en_g = CF["HHH: End Date"]
    cr_g = CF["HHH: Crew Size"]

    for p in projects:
        pgid = p["gid"]; pname = p["name"]; plink = p.get("permalink_url","")
        try:
            tasks = paginate("tasks", {
                "project": pgid,
                "opt_fields": "name,custom_fields.gid,custom_fields.number_value,"
                              "custom_fields.date_value,permalink_url",
                "limit": 100,
            })
        except Exception as e:
            print(f"  ! {pname[:40]}: {e}"); continue

        for t in tasks:
            name = (t.get("name") or "").strip()
            if not name: continue
            low = name.lower()
            if any(v in low for v in VOID): continue

            is_canon = is_canonical(name)
            looks_it = looks_like_lease(name)

            if looks_it and not is_canon:
                unmatched_names.append({
                    "project": pname, "task": name,
                    "project_link": plink, "task_link": t.get("permalink_url",""),
                })
                continue

            if is_canon:
                # Check required custom fields for leases
                is_lease = "addendum" not in low
                missing = []
                has = {}
                for cf in t.get("custom_fields", []):
                    g = cf.get("gid")
                    if g == ar_g:   has["ar"]   = cf.get("number_value")
                    elif g == ap_g: has["ap"]   = cf.get("number_value")
                    elif g == st_g: has["start"]= cf.get("date_value")
                    elif g == en_g: has["end"]  = cf.get("date_value")
                    elif g == cr_g: has["crew"] = cf.get("number_value")

                is_construction = "construction" in low
                is_homeowner    = "homeowner"    in low
                # A Construction Lease needs A/R + dates + crew. Homeowner Lease needs A/P + dates.
                if is_lease:
                    if is_construction:
                        if not has.get("ar"):    missing.append("A/R")
                        if not has.get("start"): missing.append("Start Date")
                        if not has.get("end"):   missing.append("End Date")
                        if not has.get("crew"):  missing.append("Crew Size")
                    elif is_homeowner:
                        if not has.get("ap"):    missing.append("A/P")
                        if not has.get("start"): missing.append("Start Date")
                        if not has.get("end"):   missing.append("End Date")
                # Addendums: at least need End Date (to extend the lease)
                else:
                    if not has.get("end"): missing.append("End Date")

                if missing:
                    missing_fields.append({
                        "project": pname, "task": name, "missing": missing,
                        "task_link": t.get("permalink_url",""),
                    })

    out = {
        "generated_at": datetime.now().isoformat(),
        "unmatched_names": unmatched_names,
        "missing_fields":  missing_fields,
    }
    os.makedirs("data", exist_ok=True)
    with open("data/parse_issues.json", "w") as f:
        json.dump(out, f, indent=2)

    print(f"\n✓ {len(unmatched_names)} unmatched lease-like task names")
    print(f"✓ {len(missing_fields)} canonical tasks with missing custom fields")
    print("  Output → data/parse_issues.json")

if __name__ == "__main__":
    main()
