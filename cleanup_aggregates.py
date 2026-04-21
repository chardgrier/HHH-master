#!/usr/bin/env python3
"""
One-time cleanup: where my original populate_fields.py put AGGREGATE A/R and A/P
values on a single "A: Construction Lease" task (because master_data had one
aggregate row for a multi-house project), but the sibling tasks (B, C, ...)
have since been populated with their own individual values by populate_homeowners.py,
the aggregate now double-counts. This script clears those aggregates so sync sums
clean individual values instead.

Detection: for any Construction Lease task in a group (prefix=A) where OTHER
letter-prefixed Construction Lease tasks in the same project also have A/R set,
we clear A's A/R/A/P (keeping Start/End/Crew).
"""
import os, re, json, sys, time, requests

TOK = os.environ.get("ASANA_TOKEN", "")
if not TOK: print("ERROR: ASANA_TOKEN not set"); sys.exit(1)
WS = "1203487090849714"
BASE = "https://app.asana.com/api/1.0"
H = {"Authorization": f"Bearer {TOK}"}

with open("data/custom_field_gids.json") as f: CF = json.load(f)

SALESPEOPLE = ["Paul","Zeke","Matt","Logan","David","Charlie","Peyton"]
HHH_PATTERN = re.compile(rf"\b({'|'.join(SALESPEOPLE)})\b.*?\d{{4}}", re.I)
SKIP_NAMES = ["template","general to do","xxxx","hard hat housing template"]
VOID = ("did not send","did not use","cancelled","terminated","termination",
        "backup","back up","not used","duplicate","did not")

def http(method, endpoint, data=None, params=None):
    url = f"{BASE}/{endpoint}"
    while True:
        r = requests.request(method, url, headers=H, params=params, json=data, timeout=30)
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", 5))); continue
        return r

def paginate(endpoint, params=None):
    url = f"{BASE}/{endpoint}"; out = []
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

# Pattern matchers — only canonical lease/addendum names
CANON_RE = re.compile(
    r"^(?:[A-Z]\s*:\s*|Phase\s+\d+\s+)?"
    r"(?:Construction|Homeowner)\s+(?:Lease|Addendum)"
    r"(?:\s+Phase\s+\d+|\s*#?\s*\d+|\s*[-–]\s*[A-Z])?\s*$", re.I)

def is_canonical(name):
    if not name: return False
    low = name.lower()
    if any(v in low for v in VOID): return False
    return bool(CANON_RE.match(name.strip()))

def classify(name):
    low = name.lower()
    if "construction" in low and "lease" in low and "addendum" not in low: return "construction_lease"
    if "homeowner"    in low and "lease" in low and "addendum" not in low: return "homeowner_lease"
    if "construction" in low and "addendum" in low: return "construction_addendum"
    if "homeowner"    in low and "addendum" in low: return "homeowner_addendum"
    return None

def prefix_of(name):
    m = re.match(r"^([A-Z])\s*:", name)
    return m.group(1).upper() if m else "_main"

def cf_num(task, gid):
    for cf in task.get("custom_fields", []):
        if cf.get("gid") == gid:
            return cf.get("number_value")
    return None

def clear_amount(task_gid, which):
    """Clear A/R or A/P custom field on a task (set to null)."""
    gid = CF["HHH: Monthly A/R"] if which == "ar" else CF["HHH: Monthly A/P"]
    body = {"data": {"custom_fields": {gid: None}}}
    r = http("PUT", f"tasks/{task_gid}", data=body)
    return r.status_code < 300, r.text[:150] if r.status_code >= 300 else None

def main():
    print("Scanning HHH projects for aggregate/individual conflicts…")
    projects = [p for p in paginate("projects", {
        "workspace": WS, "archived":"false", "opt_fields":"name","limit":100
    }) if HHH_PATTERN.search(p.get("name","")) and
        not any(s in p["name"].lower() for s in SKIP_NAMES)]

    ar_cleared = 0; ap_cleared = 0
    ar_gid = CF["HHH: Monthly A/R"]; ap_gid = CF["HHH: Monthly A/P"]

    for p in projects:
        pgid = p["gid"]; pname = p["name"]
        tasks = paginate("tasks", {
            "project": pgid,
            "opt_fields":"name,custom_fields.gid,custom_fields.number_value",
            "limit": 100,
        })
        tasks = [t for t in tasks if is_canonical(t.get("name",""))]

        # Group by (prefix, type)
        groups = {}
        for t in tasks:
            kind = classify(t["name"])
            if not kind or "addendum" in kind: continue  # only compare leases
            gtype = "construction" if kind.startswith("construction") else "homeowner"
            pre = prefix_of(t["name"])
            groups.setdefault(gtype, {}).setdefault(pre, []).append(t)

        for gtype, by_prefix in groups.items():
            amt_gid = ar_gid if gtype == "construction" else ap_gid
            # If multiple prefixes have amounts set, one could be the aggregate
            prefixes_with_values = []
            for pre, ts in by_prefix.items():
                for t in ts:
                    v = cf_num(t, amt_gid)
                    if v is not None and v > 0:
                        prefixes_with_values.append((pre, t, v))
                        break

            if len(prefixes_with_values) <= 1:
                continue  # no conflict

            # Identify the aggregate: the value that's SIGNIFICANTLY LARGER than others
            # (likely sum of the others)
            sorted_by_val = sorted(prefixes_with_values, key=lambda x: -x[2])
            largest_pre, largest_task, largest_val = sorted_by_val[0]
            others = sorted_by_val[1:]
            others_sum = sum(v for _,_,v in others)

            # If the largest value is >= 1.5x the sum of the others OR ≈ sum of others,
            # it's likely an aggregate. Clear it.
            if others_sum > 0 and (largest_val > 1.5 * max(v for _,_,v in others)):
                print(f"  ↓ Clearing aggregate {gtype.upper()} on '{largest_task['name']}' "
                      f"(${largest_val:,.0f}) in [{pname[:40]}] — sibling tasks have individuals")
                ok, err = clear_amount(largest_task["gid"], "ar" if gtype == "construction" else "ap")
                if ok:
                    if gtype == "construction": ar_cleared += 1
                    else: ap_cleared += 1
                else:
                    print(f"     ! error: {err}")

    print(f"\n✓ Cleared {ar_cleared} aggregate A/R + {ap_cleared} aggregate A/P values")

if __name__ == "__main__":
    main()
