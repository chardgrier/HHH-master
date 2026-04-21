#!/usr/bin/env python3
"""
Migration: for each project where A/P is set on the Construction Lease task
(legacy from the first populator) but missing on the Homeowner Lease task(s),
copy A/P to the matching Homeowner Lease and clear it from Construction.

Also populates A/P from master_data.json when nothing is in Asana at all.
"""
import os, re, json, sys, time, requests

TOK = os.environ.get("ASANA_TOKEN", "")
if not TOK: print("ERROR: ASANA_TOKEN not set"); sys.exit(1)
WS = "1203487090849714"
BASE = "https://app.asana.com/api/1.0"
H = {"Authorization": f"Bearer {TOK}"}

with open("data/custom_field_gids.json") as f: CF = json.load(f)
with open("data/master_data.json") as f: MASTER = json.load(f)

AR = CF["HHH: Monthly A/R"]; AP = CF["HHH: Monthly A/P"]
START = CF["HHH: Start Date"]; END = CF["HHH: End Date"]

SALESPEOPLE = ["Paul","Zeke","Matt","Logan","David","Charlie","Peyton"]
VOID = ("did not send","did not use","cancelled","terminated","termination",
        "backup","back up","not used","duplicate","did not")

def http(m, e, data=None, params=None):
    url = f"{BASE}/{e}"
    while True:
        r = requests.request(m, url, headers=H, params=params, json=data, timeout=30)
        if r.status_code == 429: time.sleep(int(r.headers.get("Retry-After",5))); continue
        return r

def paginate(e, params=None):
    url = f"{BASE}/{e}"; out = []
    while url:
        r = requests.get(url, headers=H, params=params, timeout=30)
        if r.status_code == 429: time.sleep(int(r.headers.get("Retry-After",5))); continue
        r.raise_for_status()
        d = r.json(); out.extend(d.get("data",[]))
        nxt = d.get("next_page")
        url = nxt["uri"] if nxt else None; params = None
    return out

CANON = re.compile(
    r"^(?:[A-Z]\s*:\s*|Phase\s+\d+\s+)?"
    r"(?:Construction|Homeowner)\s+(?:Lease|Addendum)"
    r"(?:\s+Phase\s+\d+|\s*#?\s*\d+|\s*[-–]\s*[A-Z])?\s*$", re.I)

def is_canonical(n):
    if not n: return False
    if any(v in n.lower() for v in VOID): return False
    return bool(CANON.match(n.strip()))

def classify(n):
    low = n.lower()
    if "construction" in low and "lease" in low and "addendum" not in low: return "construction_lease"
    if "homeowner"    in low and "lease" in low and "addendum" not in low: return "homeowner_lease"
    if "construction" in low and "addendum" in low: return "construction_addendum"
    if "homeowner"    in low and "addendum" in low: return "homeowner_addendum"
    return None

def prefix_of(n):
    m = re.match(r"^([A-Z])\s*:", n)
    return m.group(1).upper() if m else "_main"

def cf_num(task, gid):
    for cf in task.get("custom_fields", []):
        if cf.get("gid") == gid: return cf.get("number_value")
    return None

def main():
    print("Finding HHH projects…")
    projects = [p for p in paginate("projects", {
        "workspace": WS, "archived":"false","opt_fields":"name","limit":100
    }) if re.search(rf"\b({'|'.join(SALESPEOPLE)})\b.*?\d{{4}}", p.get("name",""), re.I)]

    # Build master_data index by (project_number, house_letter)
    master_idx = {}
    for m in MASTER["projects"]:
        num = re.search(r"\b(2\d{3})\b", m["name"])
        if not num: continue
        house = re.search(r"\[House\s+([A-Z])\]", m["name"])
        phase = re.search(r"Phase\s+(\d+)", m["name"])
        sub   = re.search(r"-\s*\d{4}\s*-\s*([A-Z]{1,3})\s*$", m["name"])
        letter = (house.group(1) if house else
                 (f"Phase{phase.group(1)}" if phase else
                  (sub.group(1) if sub else "_main")))
        master_idx[(num.group(1), letter)] = m

    migrated = 0; populated = 0; cleared = 0
    for p in projects:
        pgid = p["gid"]; pname = p["name"]
        pnum_m = re.search(r"\b(2\d{3})\b", pname)
        if not pnum_m: continue
        pnum = pnum_m.group(1)

        tasks = paginate("tasks", {
            "project": pgid,
            "opt_fields":"name,custom_fields.gid,custom_fields.number_value",
            "limit":100,
        })
        tasks = [t for t in tasks if is_canonical(t.get("name",""))]
        if not tasks: continue

        # Group by prefix × type
        groups = {}
        for t in tasks:
            kind = classify(t["name"])
            if not kind: continue
            gtype = "construction" if kind.startswith("construction") else "homeowner"
            pre = prefix_of(t["name"])
            g = groups.setdefault(pre, {}).setdefault(gtype, {"lease": None, "addendums": []})
            if "addendum" in kind:
                g["addendums"].append(t)
            elif g["lease"] is None:
                g["lease"] = t

        # For each prefix group, ensure A/P is on a Homeowner Lease task
        for pre, types in groups.items():
            h_lease = types.get("homeowner", {}).get("lease")
            c_lease = types.get("construction", {}).get("lease")

            h_has_ap = h_lease and cf_num(h_lease, AP) not in (None, 0)
            if h_has_ap: continue  # already good

            # Source 1: copy from Construction Lease if it has A/P
            c_ap = cf_num(c_lease, AP) if c_lease else None
            if c_ap and c_ap > 0:
                if h_lease:
                    r = http("PUT", f"tasks/{h_lease['gid']}",
                             data={"data":{"custom_fields":{AP: c_ap}}})
                    if r.status_code < 300:
                        print(f"  ↻ migrated A/P ${c_ap:,.0f} → homeowner in [{pname[:40]}] / {pre}")
                        migrated += 1
                        # Clear from Construction Lease
                        http("PUT", f"tasks/{c_lease['gid']}",
                             data={"data":{"custom_fields":{AP: None}}})
                        cleared += 1
                    continue

            # Source 2: use master_data
            key = (pnum, pre)
            md = master_idx.get(key) or master_idx.get((pnum, "_main"))
            if md and md.get("monthly_ap"):
                ap_val = md["monthly_ap"]
                if h_lease:
                    r = http("PUT", f"tasks/{h_lease['gid']}",
                             data={"data":{"custom_fields":{AP: ap_val}}})
                    if r.status_code < 300:
                        print(f"  + populated A/P ${ap_val:,.0f} from master_data in [{pname[:40]}] / {pre}")
                        populated += 1

    print(f"\n✓ Migrated {migrated} · Populated {populated} from master_data · Cleared {cleared} redundant A/R-side values")

if __name__ == "__main__":
    main()
