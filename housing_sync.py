#!/usr/bin/env python3
"""
HHH Housing Searches — Sync.

Reads:
  • Asana "2026 Housing Searches" project (sections drive status, custom fields
    "Close Reason" and "Project Number" enrich the data)
  • Google Form response sheet (salesperson, dates, crew, budget — the columns
    Asana doesn't capture)
  • data/projects.json (to link converted searches to their project number)

Writes:
  • data/housing.json — feeds housing.html
  • Master tracker Google Sheet — rewrites monthly tabs + scoreboard tab so the
    sheet stays a live artifact for anyone who likes that view

Required environment:
  ASANA_TOKEN                 — same token as sync.py
  GOOGLE_SHEETS_CREDENTIALS   — service-account JSON (full file contents)
  HOUSING_FORM_SHEET_ID       — Google Sheet ID for form responses
                                (master tracker ID is hard-coded below)

Setup (one-time, in Asana UI):
  In project "2026 Housing Searches" → Customize → Add field:
    1. "Close Reason"   (Text) — housing rep fills this when closing
    2. "Project Number" (Text) — housing rep fills this when moving to Projects
  No GIDs to configure here — we look these fields up by name.
"""
import json
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote

try:
    import requests
except ImportError:
    os.system("pip3 install requests -q"); import requests

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

ASANA_TOKEN          = os.environ.get("ASANA_TOKEN", "")
SHEETS_CREDS_JSON    = os.environ.get("GOOGLE_SHEETS_CREDENTIALS", "").strip()
FORM_SHEET_ID        = os.environ.get("HOUSING_FORM_SHEET_ID", "").strip()

BASE_URL             = "https://app.asana.com/api/1.0"
HOUSING_PROJECT_GID  = "1209569809329318"   # 2026 Housing Searches
MASTER_TRACKER_ID    = "1LUftq8dzNXP-66DwiZ-a-MuCNjtNSd5zVFq0fZyKCAg"

OUTPUT_FILE          = "data/housing.json"
PROJECTS_FILE        = "data/projects.json"

# Asana section gid → dashboard status
SECTION_TO_STATUS = {
    "1209569809329322": "Still Open",                  # New Housing Searches
    "1214035378782183": "Future",                      # Future Housing Searches
    "1209609481747495": "Pending",                     # Housing Search - pending approval
    "1209643393837736": "Closed",                      # Closed Housing Search
    "1210747825601111": "Converted to a Project",      # Closed Housing Search - Moved to Projects
    "1211582188924066": "Closed",                      # Add to Database (housekeeping)
}

# Names of the two custom fields we expect on tasks (look up by name, not GID,
# so renaming or recreating fields in Asana doesn't break sync).
CF_CLOSE_REASON   = "Close Reason"
CF_PROJECT_NUMBER = "Project Number"

# Status display order (matches sheet scoreboard rows).
STATUS_ORDER = ["Total Housing Searches", "Converted to a Project", "Closed", "Pending", "Still Open", "Future"]

# Salesperson name normalization — handles aliases / nicknames.
# Keys are lowercase first-name; values are the canonical display name.
SALESPERSON_ALIASES = {
    "pablo": "Paul",
}

MONTH_NAMES = ["January","February","March","April","May","June",
               "July","August","September","October","November","December"]

# Task title format: "New Housing Search for {Company} - {City, State}"
TASK_TITLE_RE = re.compile(r"^\s*new\s+housing\s+search\s+for\s+(.+?)\s+-\s+(.+?)\s*$", re.IGNORECASE)

# ─────────────────────────────────────────────────────────────────────────────
# Asana
# ─────────────────────────────────────────────────────────────────────────────

def asana(endpoint, params=None):
    if not ASANA_TOKEN:
        raise RuntimeError("ASANA_TOKEN not set")
    headers = {"Authorization": f"Bearer {ASANA_TOKEN}"}
    url = f"{BASE_URL}/{endpoint}"
    results = []
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 429:
            import time
            time.sleep(int(r.headers.get("Retry-After", 5)))
            continue
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


def fetch_housing_tasks():
    """Return all tasks in the housing project with the fields we need."""
    fields = ",".join([
        "name", "completed", "completed_at", "created_at", "due_on",
        "assignee.name", "assignee.email",
        "memberships.section.gid", "memberships.section.name",
        "memberships.project.gid",
        "custom_fields.name", "custom_fields.text_value",
        "custom_fields.display_value",
        "permalink_url",
    ])
    return asana(f"projects/{HOUSING_PROJECT_GID}/tasks",
                 params={"opt_fields": fields, "limit": 100})


def section_for_task(task):
    """Pick the section gid from this project's membership."""
    for m in task.get("memberships", []) or []:
        sec = m.get("section") or {}
        proj = m.get("project") or {}
        if proj.get("gid") == HOUSING_PROJECT_GID and sec.get("gid"):
            return sec["gid"]
    return None


def cf_text(task, field_name):
    for cf in task.get("custom_fields", []) or []:
        if (cf.get("name") or "").strip().lower() == field_name.lower():
            return (cf.get("text_value") or cf.get("display_value") or "").strip() or None
    return None


def parse_task_title(name):
    """'New Housing Search for ARD - Birmingham, AL' → ('ARD', 'Birmingham, AL')."""
    m = TASK_TITLE_RE.match(name or "")
    if not m:
        return None, None
    return m.group(1).strip(), m.group(2).strip()


def parse_city_state(loc):
    """'Birmingham, AL' → ('Birmingham', 'AL'). Tolerant of typos like 'Alachua. FL'."""
    if not loc:
        return None, None
    s = loc.replace(".", ",").strip()
    parts = [p.strip() for p in s.split(",") if p.strip()]
    if len(parts) >= 2:
        return parts[0], parts[-1]
    return parts[0] if parts else None, None


# ─────────────────────────────────────────────────────────────────────────────
# Google Sheets (form response + master tracker)
# ─────────────────────────────────────────────────────────────────────────────

def open_sheets_client():
    """Return an authorized gspread client, or None if creds aren't configured."""
    if not SHEETS_CREDS_JSON:
        print("  ! GOOGLE_SHEETS_CREDENTIALS not set — skipping sheet read/write")
        return None
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        os.system("pip3 install gspread google-auth -q")
        import gspread
        from google.oauth2.service_account import Credentials

    info = json.loads(SHEETS_CREDS_JSON)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ],
    )
    return gspread.authorize(creds)


def read_form_responses(gc):
    """Return list of dicts from the form response sheet's first tab.

    Uses get_all_values() rather than get_all_records() so we tolerate blank
    or duplicate header columns (Google Forms response sheets often have a
    trailing 'Column N' or empty extras from manual edits).
    """
    if not gc or not FORM_SHEET_ID:
        print("  ! HOUSING_FORM_SHEET_ID not set — no salesperson/budget enrichment")
        return []
    try:
        sh = gc.open_by_key(FORM_SHEET_ID)
        ws = _find_form_responses_tab(sh)
        if ws is None:
            print("  ! could not locate the form responses tab in this sheet")
            return []
        print(f"  · using tab: {ws.title!r}")
        values = ws.get_all_values()
        if len(values) < 2:
            print("  · form sheet has no data rows yet")
            return []
        headers = values[0]
        rows = []
        for raw in values[1:]:
            d = {}
            for i, h in enumerate(headers):
                if not h or not h.strip() or h in d:
                    continue   # skip blank/duplicate headers
                d[h] = raw[i] if i < len(raw) else ""
            rows.append(d)
        print(f"  ✓ read {len(rows)} form responses")
        return rows
    except Exception as e:
        print(f"  ! could not read form sheet: {e}")
        return []


def _find_form_responses_tab(sh):
    """Find the worksheet that's actually the Google Form responses.

    Strategy: prefer a tab literally named 'Form Responses 1' (Google Forms
    default), then any tab whose row-1 contains both 'Timestamp' and a
    'Sales Person Name'-style header. Falls through to None if nothing matches.
    """
    try:
        worksheets = sh.worksheets()
    except Exception:
        return None

    # First pass — exact-name match
    for ws in worksheets:
        if (ws.title or "").strip().lower().startswith("form responses"):
            return ws

    # Second pass — header sniffing
    for ws in worksheets:
        try:
            row1 = ws.row_values(1) or []
        except Exception:
            continue
        joined = " | ".join(row1).lower()
        if "timestamp" in joined and "sales person name" in joined:
            return ws
    return None


def form_get(row, *needles):
    """Get a value from a form row by partial-match on the column header.

    Form headers contain embedded newlines and trailing colons (e.g.
    'Sales Person Name\\n(First & Last Name):'), so an exact-match lookup is
    too brittle. We normalize whitespace + lowercase, then check whether any
    of the needles appears as a prefix.
    """
    if not row:
        return None
    norm_needles = [re.sub(r"\s+", " ", n).strip().lower() for n in needles]
    for k, v in row.items():
        if v is None or v == "":
            continue
        kn = re.sub(r"\s+", " ", str(k)).strip().lower()
        for n in norm_needles:
            if kn.startswith(n):
                return v
    return None


def normalize_company(s):
    if not s:
        return ""
    s = re.sub(r"[^\w\s]", " ", s.lower())
    s = re.sub(r"\b(inc|llc|corp|company|co|ltd|services|construction)\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def normalize_city(s):
    if not s:
        return ""
    return re.sub(r"\s+", " ", s.lower().replace(".", "").strip())


def best_form_match(form_rows, company, city, asana_created_at):
    """Find the form row that most likely corresponds to this Asana task.

    Match keys: company name (fuzzy) + city (exact-normalized) + closest in time.
    """
    if not form_rows or not company:
        return None
    try:
        from rapidfuzz import fuzz
    except ImportError:
        os.system("pip3 install rapidfuzz -q")
        from rapidfuzz import fuzz

    target_co = normalize_company(company)
    target_city = normalize_city(city)
    asana_dt = parse_iso(asana_created_at)

    candidates = []
    for r in form_rows:
        row_co = normalize_company(form_get(r, "Company Name") or "")
        row_city = normalize_city(form_get(r, "Project City") or "")
        if not row_co:
            continue
        co_score = fuzz.token_set_ratio(target_co, row_co)
        if co_score < 70:
            continue
        # City must match if both sides have one (otherwise lots of false joins for
        # companies with multiple simultaneous searches like Power Design).
        if target_city and row_city and target_city != row_city:
            # Allow fuzzy on city too — Birmingham/Birminham typos, or substring matches
            if target_city in row_city or row_city in target_city:
                pass
            else:
                city_score = fuzz.ratio(target_city, row_city)
                if city_score < 75:
                    continue
        # Time proximity: form fills before Asana task is created (by Zapier).
        ts_score = 0
        ts = parse_form_timestamp(form_get(r, "Timestamp"))
        if ts and asana_dt:
            delta_days = abs((asana_dt - ts).total_seconds()) / 86400
            ts_score = max(0, 100 - int(delta_days * 10))   # 10 pts per day
        candidates.append((co_score + ts_score, r))

    if not candidates:
        return None
    candidates.sort(key=lambda x: -x[0])
    return candidates[0][1]


def parse_iso(s):
    if not s:
        return None
    try:
        # Asana ISO: 2026-04-23T14:05:38.637Z
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def parse_form_timestamp(s):
    """Form timestamps look like '4/23/2026 14:05:38'."""
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def parse_form_date(s):
    """Form date fields → 'YYYY-MM-DD' string (or original string if unparseable)."""
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%-m/%-d/%Y", "%-m/%-d/%y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass
    return s   # leave as-is rather than drop the data


def parse_budget(s):
    """Pull a representative number out of free-text budget like '$3,500 for 4BR'."""
    if not s:
        return None
    s = str(s)
    m = re.search(r"\$?\s*([\d,]+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Conversion → project link
# ─────────────────────────────────────────────────────────────────────────────

def load_projects():
    if not os.path.exists(PROJECTS_FILE):
        return []
    try:
        with open(PROJECTS_FILE) as f:
            return (json.load(f) or {}).get("projects") or []
    except Exception:
        return []


def find_project_for_search(projects, project_number, company, city):
    """Try to match a converted search to a project number on the master dashboard."""
    if project_number:
        for p in projects:
            if str(p.get("project_number") or "") == str(project_number):
                return p
    if not company or not city:
        return None
    try:
        from rapidfuzz import fuzz
    except ImportError:
        return None
    target = normalize_company(company)
    city_norm = normalize_city(city)
    best = None
    best_score = 0
    for p in projects:
        name_norm = normalize_company(p.get("name") or "")
        co_score = fuzz.token_set_ratio(target, name_norm)
        if co_score < 70:
            continue
        if city_norm and city_norm not in normalize_city(p.get("name") or ""):
            continue
        score = co_score
        if score > best_score:
            best_score = score
            best = p
    return best


# ─────────────────────────────────────────────────────────────────────────────
# Build housing records
# ─────────────────────────────────────────────────────────────────────────────

def build_records(tasks, form_rows, projects):
    today = date.today()
    out = []
    for t in tasks:
        sec_gid = section_for_task(t)
        status = SECTION_TO_STATUS.get(sec_gid)
        if not status:
            continue   # skip tasks not in a known section

        # Only include searches created in 2026+ (per Richard: start fresh Jan 1, 2026)
        created_iso = t.get("created_at") or ""
        if created_iso and created_iso < "2026-01-01":
            continue

        company, location = parse_task_title(t.get("name") or "")
        # Skip junk tasks whose name doesn't follow the standard pattern —
        # they pollute the monthly tabs with blank/Unknown rows.
        if not company:
            continue
        city, state = parse_city_state(location)

        created = parse_iso(t.get("created_at"))
        completed = parse_iso(t.get("completed_at"))
        completed_on = completed.date() if completed else None

        form = best_form_match(form_rows, company, city, t.get("created_at"))
        salesperson = form_get(form, "Sales Person Name", "Salesperson") or "Unknown"
        salesperson_first = salesperson.strip().split()[0] if salesperson and salesperson != "Unknown" else "Unknown"
        salesperson_first = SALESPERSON_ALIASES.get(salesperson_first.lower(), salesperson_first)

        start_date = parse_form_date(form_get(form, "Estimated Start Date"))
        end_date   = parse_form_date(form_get(form, "Estimated End Date"))
        crew       = form_get(form, "Total Number of Crew", "# of Crew") or ""
        budget_raw = form_get(form, "Budget: What is the cost", "Budget") or ""

        close_reason   = cf_text(t, CF_CLOSE_REASON)
        project_number = cf_text(t, CF_PROJECT_NUMBER)

        # Try to link converted searches back to a master-dashboard project
        linked_project = None
        if status == "Converted to a Project":
            p = find_project_for_search(projects, project_number, company, city)
            if p:
                linked_project = {
                    "name": p.get("name"),
                    "project_number": p.get("project_number"),
                    "gid": p.get("gid"),
                }
                if not project_number:
                    project_number = p.get("project_number")

        # Bucket month — by created_at (matches sheet's "Date" column)
        bucket = created.date().isoformat()[:7] if created else None

        # Days open / time-in-status
        if status in ("Closed", "Converted to a Project"):
            days_open = (completed_on - created.date()).days if completed and created else None
        else:
            days_open = (today - created.date()).days if created else None

        # Days past presentation (for Pending bucket — "stale" if too long)
        due_on = t.get("due_on")
        days_past_due = None
        if due_on:
            try:
                d = date.fromisoformat(due_on)
                days_past_due = (today - d).days
            except Exception:
                pass

        out.append({
            "gid": t.get("gid"),
            "name": t.get("name"),
            "url": t.get("permalink_url"),
            "status": status,
            "section_gid": sec_gid,
            "company": company,
            "city": city,
            "state": state,
            "location": location,
            "created_at": t.get("created_at"),
            "created_date": created.date().isoformat() if created else None,
            "completed_at": t.get("completed_at"),
            "completed_date": completed_on.isoformat() if completed_on else None,
            "due_on": due_on,
            "days_open": days_open,
            "days_past_due": days_past_due,
            "month_bucket": bucket,
            "salesperson": salesperson_first,
            "salesperson_full": salesperson,
            "housing_rep": (t.get("assignee") or {}).get("name"),
            "housing_rep_email": (t.get("assignee") or {}).get("email"),
            "start_date": start_date,
            "end_date": end_date,
            "crew": crew,
            "budget": budget_raw,
            "budget_num": parse_budget(budget_raw),
            "close_reason": close_reason,
            "project_number": project_number,
            "linked_project": linked_project,
            "form_matched": form is not None,
        })
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Aggregations for dashboard panels
# ─────────────────────────────────────────────────────────────────────────────

def build_scoreboard(records):
    """Status × month matrix matching the sheet's scoreboard tab.

    For any year that has at least one record, include ALL 12 months — so the
    scorecard always shows future months as empty cells (matches the layout
    the housing team is used to).
    """
    years_with_data = sorted({r["month_bucket"][:4] for r in records if r["month_bucket"]})
    months = []
    for y in years_with_data:
        for m in range(1, 13):
            months.append(f"{y}-{m:02d}")
    scoreboard = {s: {m: 0 for m in months} for s in STATUS_ORDER}
    for r in records:
        m = r["month_bucket"]
        if not m:
            continue
        scoreboard["Total Housing Searches"][m] += 1
        if r["status"] in scoreboard:
            scoreboard[r["status"]][m] += 1
    totals = {s: sum(scoreboard[s].values()) for s in STATUS_ORDER}
    return {"months": months, "rows": scoreboard, "totals": totals}


def build_conversion(records, key):
    """Conversion rate by salesperson or housing rep."""
    by = {}
    for r in records:
        k = r.get(key) or "Unknown"
        if k == "Unknown" or not k:
            continue
        by.setdefault(k, {"total": 0, "converted": 0, "closed": 0, "pending": 0, "still_open": 0, "future": 0})
        by[k]["total"] += 1
        if r["status"] == "Converted to a Project":
            by[k]["converted"] += 1
        elif r["status"] == "Closed":
            by[k]["closed"] += 1
        elif r["status"] == "Pending":
            by[k]["pending"] += 1
        elif r["status"] == "Still Open":
            by[k]["still_open"] += 1
        elif r["status"] == "Future":
            by[k]["future"] += 1
    for k, v in by.items():
        decided = v["converted"] + v["closed"]
        v["conversion_pct"] = round(v["converted"] / decided * 100, 1) if decided else None
    return by


def build_attention(records):
    """'Needs attention' lists for the dashboard."""
    today = date.today()
    stale_open = []   # Still Open ≥ 7 days
    stale_pending = []   # Pending ≥ 5 days past presentation date
    for r in records:
        if r["status"] == "Still Open" and (r.get("days_open") or 0) >= 7:
            stale_open.append(r)
        if r["status"] == "Pending" and r.get("days_past_due") is not None and r["days_past_due"] >= 5:
            stale_pending.append(r)
    return {
        "stale_still_open": sorted(stale_open, key=lambda r: -(r.get("days_open") or 0)),
        "stale_pending":   sorted(stale_pending, key=lambda r: -(r.get("days_past_due") or 0)),
    }


def build_geo(records):
    """Top cities/states by volume."""
    cities = {}
    states = {}
    for r in records:
        if r.get("city") and r.get("state"):
            key = f"{r['city']}, {r['state']}"
            cities[key] = cities.get(key, 0) + 1
        if r.get("state"):
            states[r["state"]] = states.get(r["state"], 0) + 1
    top_cities = sorted(cities.items(), key=lambda x: -x[1])[:15]
    top_states = sorted(states.items(), key=lambda x: -x[1])[:10]
    return {
        "top_cities": [{"city": c, "count": n} for c, n in top_cities],
        "top_states": [{"state": s, "count": n} for s, n in top_states],
    }


def build_avg_days_to_close(records):
    """Mean days from creation to outcome, by outcome."""
    buckets = {"Converted to a Project": [], "Closed": []}
    for r in records:
        if r["status"] in buckets and r.get("days_open") is not None:
            buckets[r["status"]].append(r["days_open"])
    return {k: (round(sum(v)/len(v), 1) if v else None) for k, v in buckets.items()}


# ─────────────────────────────────────────────────────────────────────────────
# Master sheet write-back
# ─────────────────────────────────────────────────────────────────────────────

MONTH_TAB_HEADER = ["Date", "Salesperson", "Company", "Project City/State",
                    "Start Date", "End Date", "# of Crew", "Budget",
                    "Status", "If no conversion, why?"]


def write_back_master_sheet(gc, records):
    """Rewrite monthly tabs + scoreboard tab on the master tracker sheet."""
    if not gc:
        return
    try:
        sh = gc.open_by_key(MASTER_TRACKER_ID)
    except Exception as e:
        print(f"  ! could not open master tracker sheet: {e}")
        return

    # ─── Monthly tabs ────────────────────────────────────────────────────────
    # Skip "Future" status from monthly tabs — they're forward-looking and
    # the existing data validation only allows Still Open/Pending/Closed/
    # Converted to a Project.
    by_month = {}
    for r in records:
        m = r.get("month_bucket")
        if not m:
            continue
        if r.get("status") == "Future":
            continue
        by_month.setdefault(m, []).append(r)

    months_written = 0
    for month_key, rows in sorted(by_month.items()):
        y, mn = month_key.split("-")
        tab_name = f"{MONTH_NAMES[int(mn)-1]} {y}"
        try:
            ws = sh.worksheet(tab_name)
        except Exception:
            print(f"  · tab not found, skipping: {tab_name}")
            continue

        rows.sort(key=lambda r: r.get("created_at") or "")
        body = []
        for r in rows:
            body.append([
                _short_date(r.get("created_date")),
                r.get("salesperson") or "",
                r.get("company") or "",
                r.get("location") or "",
                _short_date(r.get("start_date")) or "",
                _short_date(r.get("end_date")) or "",
                str(r.get("crew") or ""),
                str(r.get("budget") or ""),
                r.get("status") or "",
                r.get("close_reason") or "",
            ])

        # Clear rows below the header (row 1) and rewrite.
        try:
            ws.batch_clear([f"A2:J{max(len(body) + 50, 100)}"])
            if body:
                ws.update(values=body, range_name=f"A2:J{1 + len(body)}", value_input_option="USER_ENTERED")
                apply_status_styling(ws, body)
            months_written += 1
        except Exception as e:
            print(f"  ! failed writing {tab_name}: {e}")

    print(f"  ✓ wrote {months_written} monthly tab(s)")

    # ─── Scoreboard tab ──────────────────────────────────────────────────────
    sb = build_scoreboard(records)
    months = sb["months"]
    if not months:
        return

    # Find the scoreboard tab — it's the one whose A1 is "Status".
    scoreboard_ws = None
    for ws in sh.worksheets():
        try:
            if (ws.acell("A1").value or "").strip().lower() == "status":
                scoreboard_ws = ws
                break
        except Exception:
            continue
    if not scoreboard_ws:
        print("  · no scoreboard tab found (looking for one with A1='Status')")
        return

    # Build header + body
    header = ["Status"] + [_pretty_month(m) for m in months] + ["Total"]
    body = [header]
    for s in STATUS_ORDER:
        row = [s] + [sb["rows"][s].get(m, 0) for m in months] + [sb["totals"][s]]
        body.append(row)

    cols_letter = _col_letter(len(header))
    try:
        # Clear a generous range so we wipe any leftover columns/rows from a
        # previous layout (e.g., months that are no longer in the data).
        scoreboard_ws.batch_clear(["A1:Z100"])
        scoreboard_ws.update(values=body, range_name=f"A1:{cols_letter}{len(body)}", value_input_option="USER_ENTERED")
        print(f"  ✓ wrote scoreboard ({len(months)} months × {len(STATUS_ORDER)} status rows)")
    except Exception as e:
        print(f"  ! failed writing scoreboard: {e}")


STATUS_COLORS = {
    "Closed":                  {"red": 0.95, "green": 0.72, "blue": 0.72},
    "Converted to a Project":  {"red": 0.72, "green": 0.92, "blue": 0.78},
    "Pending":                 {"red": 1.00, "green": 0.95, "blue": 0.70},
    "Still Open":              {"red": 0.78, "green": 0.88, "blue": 1.00},
}
ALLOWED_SHEET_STATUSES = ["Still Open", "Pending", "Closed", "Converted to a Project"]


def apply_status_styling(ws, body):
    """Apply data validation + per-row background color to the Status column (I).

    Validation: dropdown limited to the 4 statuses the team uses on the sheet.
    Coloring: each row's status cell gets a background matching its value, so
    new rows look the same as the rows that already had conditional formatting
    on the original sheet.
    """
    sheet_id = ws._properties["sheetId"]
    n_rows = len(body)
    if n_rows == 0:
        return

    requests = [{
        "setDataValidation": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": n_rows + 1,
                "startColumnIndex": 8,   # column I (0-indexed)
                "endColumnIndex": 9,
            },
            "rule": {
                "condition": {
                    "type": "ONE_OF_LIST",
                    "values": [{"userEnteredValue": v} for v in ALLOWED_SHEET_STATUSES],
                },
                "showCustomUi": True,
                "strict": True,
            },
        }
    }]

    # Per-row background color based on the status value in column I (index 8).
    for i, row in enumerate(body):
        status = row[8] if len(row) > 8 else ""
        color = STATUS_COLORS.get(status)
        if not color:
            continue
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": i + 1,    # body starts at sheet row 2
                    "endRowIndex": i + 2,
                    "startColumnIndex": 8,
                    "endColumnIndex": 9,
                },
                "cell": {"userEnteredFormat": {"backgroundColor": color}},
                "fields": "userEnteredFormat.backgroundColor",
            }
        })

    # One round-trip per tab covering validation + all colors.
    try:
        ws.spreadsheet.batch_update({"requests": requests})
    except Exception as e:
        print(f"  ! styling failed for {ws.title}: {e}")


def _short_date(iso_str):
    """'2026-04-23' → '4/23/26' to match how it looks in the existing sheet."""
    if not iso_str:
        return ""
    try:
        d = date.fromisoformat(iso_str)
        return f"{d.month}/{d.day}/{str(d.year)[-2:]}"
    except Exception:
        return iso_str


def _pretty_month(key):
    y, m = key.split("-")
    return f"{MONTH_NAMES[int(m)-1]} {y}"


def _col_letter(n):
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print(f"=== Housing sync @ {datetime.utcnow().isoformat()}Z ===")
    print("· fetching Asana housing tasks…")
    tasks = fetch_housing_tasks()
    print(f"  ✓ {len(tasks)} task(s)")

    gc = open_sheets_client()
    print("· reading form responses…")
    form_rows = read_form_responses(gc)

    print("· loading projects.json (for converted-search linking)…")
    projects = load_projects()

    print("· building records…")
    records = build_records(tasks, form_rows, projects)
    matched = sum(1 for r in records if r["form_matched"])
    print(f"  ✓ {len(records)} records ({matched} joined to form rows)")

    # Diagnostic: if 0 form joins despite having form rows + records, dump info
    if matched == 0 and form_rows and records:
        print("· DIAGNOSTIC — 0 joins despite data on both sides:")
        sample_row = form_rows[0]
        print(f"  · form row keys ({len(sample_row)}): {list(sample_row.keys())[:10]}")
        co_key_sample = form_get(sample_row, "Company Name")
        city_key_sample = form_get(sample_row, "Project City")
        print(f"  · sample form Company Name: {co_key_sample!r}")
        print(f"  · sample form Project City: {city_key_sample!r}")
        sample_rec = records[0]
        print(f"  · sample Asana company={sample_rec['company']!r} city={sample_rec['city']!r}")
        # Try a manual match to see what's happening
        try:
            from rapidfuzz import fuzz
            for r in form_rows[:3]:
                co = form_get(r, "Company Name") or ""
                cy = form_get(r, "Project City") or ""
                ts = form_get(r, "Timestamp")
                score = fuzz.token_set_ratio(
                    normalize_company(sample_rec['company'] or ""),
                    normalize_company(co))
                print(f"    form: co={co!r:30} city={cy!r:20} ts={ts!r:25} score_vs_first_asana={score}")
        except Exception as e:
            print(f"  · diag failed: {e}")

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "records": records,
        "scoreboard": build_scoreboard(records),
        "conversion_by_salesperson": build_conversion(records, "salesperson"),
        "conversion_by_housing_rep": build_conversion(records, "housing_rep"),
        "attention": build_attention(records),
        "geo": build_geo(records),
        "avg_days_to_close": build_avg_days_to_close(records),
        "totals": {
            "all": len(records),
            "still_open": sum(1 for r in records if r["status"] == "Still Open"),
            "pending":    sum(1 for r in records if r["status"] == "Pending"),
            "converted":  sum(1 for r in records if r["status"] == "Converted to a Project"),
            "closed":     sum(1 for r in records if r["status"] == "Closed"),
            "future":     sum(1 for r in records if r["status"] == "Future"),
        },
    }

    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    print(f"  ✓ wrote {OUTPUT_FILE}")

    if gc:
        print("· writing back to master tracker sheet…")
        write_back_master_sheet(gc, records)

    print("✓ done")


if __name__ == "__main__":
    main()
