#!/usr/bin/env python3
"""
Take a snapshot of the current dashboard state to LOCK historical months.

Reads data/projects.json → writes data/reconciled_snapshot.json with every
project's monthly values for months ≤ today. Sync will then use these
values for past months instead of recomputing from Asana — so future
updates to an Asana task cannot retroactively change any historical
number.

Run this whenever you want to close the books through today. Safe to
re-run: the file is completely rewritten each time.
"""
import os, json
from datetime import date

with open("data/projects.json") as f:
    d = json.load(f)

today = date.today()
cutoff_month = f"{today.year}-{today.month:02d}"  # e.g. "2026-04"

snap = {
    "_comment": "Historical lockbox. For every project, the monthly values "
                "for months ≤ reconciled_through are frozen. sync.py reads "
                "this file and uses these values instead of re-computing "
                "from Asana, so changing an Asana task does NOT retroactively "
                "change past numbers on the dashboard. Re-run "
                "`python3 snapshot_reconciliation.py` to refresh the lockbox "
                "through today.",
    "snapshot_date": today.isoformat(),
    "reconciled_through": cutoff_month,
    "rows": []
}

total_cells = 0
for r in d["projects"]:
    monthly = {k: v for k, v in r["monthly"].items() if k <= cutoff_month}
    if monthly:
        snap["rows"].append({
            "name": r["name"],
            "monthly": monthly,
        })
        total_cells += len(monthly)

with open("data/reconciled_snapshot.json", "w") as f:
    json.dump(snap, f, indent=2, default=str)

print(f"✓ Snapshot taken: {len(snap['rows'])} projects, "
      f"{total_cells} month-cells frozen through {cutoff_month}.")
print("  These values are now locked and future syncs won't change them.")
print("  Output → data/reconciled_snapshot.json")
