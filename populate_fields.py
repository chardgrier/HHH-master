#!/usr/bin/env python3
"""
One-time (re-runnable) populator: matches each master_data.json entry to an
Asana task and fills the 6 HHH custom fields on that task. Purely additive —
never deletes task names, notes, assignees, or anything else.

Matching logic:
  1. Group master entries by project_number (e.g., 2506).
  2. Find the Asana project whose name contains that same project_number.
  3. For each master entry, find a task in that project matching its phase/house:
     - "[House A]"  → task starting with "A:" or "Homeowner Lease A" variant
     - "Phase 1"    → task containing "Phase 1" (or the plain "Construction Lease" if Phase 1 is implicit)
     - single entry → the plain "Construction Lease" task
  4. PUT the 6 custom fields on that task.
"""
import os, re, json, sys, time
import requests

TOK = os.environ.get("ASANA_TOKEN", "")
if not TOK:
    print("ERROR: ASANA_TOKEN not set"); sys.exit(1)
WS  = "1203487090849714"
BASE = "https://app.asana.com/api/1.0"
H = {"Authorization": f"Bearer {TOK}"}

with open("data/custom_field_gids.json") as f:
    CF = json.load(f)

with open("data/master_data.json") as f:
    MASTER = json.load(f)

SP_OPTIONS = {}  # filled lazily for the salesperson enum

# ─── Helpers ──────────────────────────────────────────────────────────────────

def asana(method, endpoint, params=None, data=None):
    url = f"{BASE}/{endpoint}"
    while True:
        r = requests.request(method, url, headers=H, params=params, json=data, timeout=30)
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", 5))); continue
        return r

def paginate(endpoint, params=None):
    url = f"{BASE}/{endpoint}"
    results = []
    while url:
        r = requests.get(url, headers=H, params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", 5))); continue
        r.raise_for_status()
        d = r.json()
        results.extend(d.get("data", []))
        nxt = d.get("next_page")
        url = nxt["uri"] if nxt else None
        params = None
    return results

def load_sp_options():
    """Fetch enum option GIDs for the salesperson field."""
    r = asana("GET", f"custom_fields/{CF['HHH: Salesperson']}")
    if r.status_code >= 300:
        print("  ! couldn't load sp enum:", r.text[:120]); return
    for opt in r.json().get("data", {}).get("enum_options", []):
        SP_OPTIONS[opt["name"]] = opt["gid"]

# ─── Find Asana project for a given project number ────────────────────────────

_project_cache = {}

def find_asana_project(project_number, name_hint):
    """Return (gid, name) of Asana project whose name contains project_number."""
    if project_number in _project_cache:
        return _project_cache[project_number]

    # Search API
    r = asana("GET", f"workspaces/{WS}/typeahead",
              params={"resource_type":"project", "query": project_number, "count":10,
                      "opt_fields":"name,permalink_url"})
    if r.status_code == 200:
        for p in r.json().get("data", []):
            if project_number in p.get("name",""):
                _project_cache[project_number] = (p["gid"], p["name"])
                return _project_cache[project_number]
    # Fallback: search by name hint
    r = asana("GET", f"workspaces/{WS}/typeahead",
              params={"resource_type":"project", "query": name_hint[:40], "count":5,
                      "opt_fields":"name"})
    for p in r.json().get("data", []):
        if project_number in p.get("name",""):
            _project_cache[project_number] = (p["gid"], p["name"])
            return _project_cache[project_number]

    _project_cache[project_number] = (None, None)
    return (None, None)

def find_asana_project_by_name(hint):
    """Project lookup by name keywords when there's no project number."""
    r = asana("GET", f"workspaces/{WS}/typeahead",
              params={"resource_type":"project", "query": hint[:40], "count":10,
                      "opt_fields":"name"})
    if r.status_code != 200: return (None, None)
    norm = re.sub(r"[^a-z0-9]+", " ", hint.lower()).strip()
    best = None
    for p in r.json().get("data", []):
        pn = re.sub(r"[^a-z0-9]+", " ", p.get("name","").lower()).strip()
        if norm in pn or pn in norm:
            best = p; break
    return (best["gid"], best["name"]) if best else (None, None)

# ─── Find the right task within a project ─────────────────────────────────────

_task_cache = {}

def get_project_tasks(project_gid):
    if project_gid in _task_cache:
        return _task_cache[project_gid]
    tasks = paginate("tasks", {"project": project_gid, "opt_fields":"name,resource_subtype", "limit": 100})
    _task_cache[project_gid] = tasks
    return tasks

def match_task(master_entry, project_gid):
    """Return the best Asana task to attach fields to, for a master entry."""
    tasks = get_project_tasks(project_gid)
    name = master_entry["name"]

    # What phase/house identifier are we looking for?
    house_m = re.search(r"\[House\s+([A-Z])\]", name)
    phase_m = re.search(r"\bPhase\s+(\d+)\b", name)
    sub_m   = re.search(r"-\s*(\d{4})\s*-\s*([A-Z]{1,3})\s*$", name)  # " - 2624 - CD"

    VOID = ("did not send","did not use","cancelled","termination","terminated",
            "back up","backup","not used","duplicate")
    def find(predicate):
        for t in tasks:
            tn = (t.get("name") or "").strip()
            if t.get("resource_subtype") == "section": continue
            low = tn.lower()
            if any(v in low for v in VOID): continue
            if predicate(tn): return t
        return None

    # By house letter (prefixed)
    if house_m:
        letter = house_m.group(1)
        return find(lambda n: re.match(rf"^{letter}\s*:\s*construction\s+lease", n, re.I)) \
            or find(lambda n: re.match(rf"^construction\s+lease\s+{letter}", n, re.I))

    # By phase number
    if phase_m:
        phase_num = phase_m.group(1)
        t = find(lambda n: re.search(rf"phase\s*{phase_num}", n, re.I) and re.search(r"construction\s+lease", n, re.I))
        if t: return t
        # Fallback: Phase 1 may be the implicit original "Construction Lease" task
        if phase_num == "1":
            return find(lambda n: re.fullmatch(r"construction\s+lease\s*", n, re.I))

    # Sub-letter ("Frankfort - Matt - 2624 - AB"): match "AB: Construction" or similar
    if sub_m:
        sub = sub_m.group(2)
        t = find(lambda n: re.search(rf"\b{sub}\b", n) and re.search(r"construction\s+lease", n, re.I))
        if t: return t

    # Plain single-phase / main entry: prefer plain "Construction Lease",
    # fall back to "A: Construction Lease" (first house), then any construction lease.
    return find(lambda n: re.fullmatch(r"construction\s+lease\s*", n, re.I)) \
        or find(lambda n: re.match(r"^a\s*:\s*construction\s+lease", n, re.I)) \
        or find(lambda n: re.search(r"construction\s+lease", n, re.I))

# ─── Attach custom fields to a project (required before setting on tasks) ────

_attached_cache = set()  # (project_gid, cf_gid)

def ensure_fields_attached(project_gid):
    """Attach all 6 HHH custom fields to this project if not already attached."""
    if project_gid in _attached_cache:
        return
    # Check what's already on the project
    r = asana("GET", f"projects/{project_gid}/custom_field_settings",
              params={"opt_fields":"custom_field.gid,custom_field.name"})
    attached_gids = set()
    if r.status_code == 200:
        for s in r.json().get("data", []):
            cf = s.get("custom_field") or {}
            if cf.get("gid"): attached_gids.add(cf["gid"])

    for name, gid in CF.items():
        if gid in attached_gids: continue
        body = {"data": {"custom_field": gid}}
        r = asana("POST", f"projects/{project_gid}/addCustomFieldSetting", data=body)
        if r.status_code >= 300:
            print(f"   ! couldn't attach {name} to project: {r.text[:120]}")
    _attached_cache.add(project_gid)

# ─── Set custom fields on a task ──────────────────────────────────────────────

def set_fields(task_gid, entry):
    cf_values = {
        CF["HHH: Monthly A/R"]: entry["monthly_ar"],
        CF["HHH: Monthly A/P"]: entry["monthly_ap"],
        CF["HHH: Start Date"]:  {"date": entry["start_date"]},
        CF["HHH: End Date"]:    {"date": entry["end_date"]},
        CF["HHH: Crew Size"]:   entry["crew"],
    }
    sp = entry.get("salesperson")
    if sp and sp in SP_OPTIONS:
        cf_values[CF["HHH: Salesperson"]] = SP_OPTIONS[sp]

    body = {"data": {"custom_fields": cf_values}}
    r = asana("PUT", f"tasks/{task_gid}", data=body)
    if r.status_code >= 300:
        return False, r.text[:200]
    return True, None

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    load_sp_options()
    print(f"Loaded {len(SP_OPTIONS)} salesperson options: {list(SP_OPTIONS)}")

    ok, missing, err = [], [], []
    for entry in MASTER["projects"]:
        name = entry["name"]
        m = re.search(r"\b(2\d{3})\b", name)
        if not m:
            missing.append((name, "no project number"))
            continue
        pnum = m.group(1) if m else ""
        if pnum:
            pgid, pname = find_asana_project(pnum, name)
        else:
            # No project number — try to match by name keywords
            hint = re.sub(r"\s*-\s*(Paul|Zeke|Matt|Logan|David|Charlie|Peyton)\s*.*$", "", name, flags=re.I)
            pgid, pname = find_asana_project_by_name(hint.strip())
        if not pgid:
            missing.append((name, f"no Asana project for #{pnum or name[:30]}"))
            continue

        task = match_task(entry, pgid)
        if not task:
            missing.append((name, f"no matching task in '{pname}'"))
            continue

        ensure_fields_attached(pgid)
        success, errmsg = set_fields(task["gid"], entry)
        if success:
            print(f"✓ {name[:55]:<55} → [{pname[:30]}] / {task['name'][:30]}")
            ok.append(name)
        else:
            err.append((name, errmsg))
            print(f"✗ {name[:55]}: {errmsg}")

    print(f"\n{'='*70}")
    print(f"Populated: {len(ok)}  ·  Unmatched: {len(missing)}  ·  Errors: {len(err)}")
    if missing:
        print("\nUnmatched entries (need manual review in Asana):")
        for n, why in missing:
            print(f"  - {n[:60]:<60}  {why}")
    if err:
        print("\nErrors:")
        for n, e in err:
            print(f"  - {n[:60]:<60}  {e}")

if __name__ == "__main__":
    main()
