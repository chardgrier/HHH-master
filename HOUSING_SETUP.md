# Housing Searches Dashboard — Setup

The housing dashboard pulls from two sources every morning: the Asana
"2026 Housing Searches" project and the Google Form response sheet. It writes
back into the master tracker sheet (monthly tabs + scoreboard) and emails the
housing team a digest.

## One-time setup

### 1. Asana custom fields (housing reps will fill these in)

Open the **2026 Housing Searches** project → **Customize** → **Add field**.
Add two text fields, exactly named:

- `Close Reason` — housing rep types in why a search closed without converting,
  when moving the card to *Closed Housing Search*. Replaces the old
  "If no conversion, why?" column on the sheet.
- `Project Number` — housing rep types in the new project number (e.g. `2616`)
  when moving the card to *Closed Housing Search - Moved to Projects*. This
  links the converted search back to the master dashboard.

The dashboard reads these fields by **name**, so you can reorder or change
descriptions later without breaking anything. Just don't rename them.

### 2. Google Cloud service account (for sheet access)

You can reuse the existing **HHH Sync** Google Cloud project — no need for a
new one. Inside that project:

1. **APIs & Services → Library** — enable both:
   - Google Sheets API
   - Google Drive API
2. **Credentials → Create Credentials → Service Account**
   - Name: `hhh-housing-sheets` (or anything; the name is just for you)
   - Skip the "Grant access" step
3. On the service account → **Keys → Add Key → Create new key → JSON**.
   Downloads a file like `hhh-sync-abcd1234.json`. Don't lose it.
4. Open the JSON, copy the `client_email` value.
   It looks like `hhh-housing-sheets@hhh-sync.iam.gserviceaccount.com`.
5. **Share both Google Sheets** with that email (Editor access):
   - Master tracker (HHH Housing Searches Tracker)
   - The form response sheet (whichever one Google Forms writes into)

### 3. GitHub repository secrets

In the repo → Settings → Secrets and variables → Actions, add:

| Secret | Value |
|---|---|
| `GOOGLE_SHEETS_CREDENTIALS` | Paste the entire contents of the JSON file from step 2.3 |
| `HOUSING_FORM_SHEET_ID` | The Google Sheet ID of the form response sheet (the long string in its URL between `/d/` and `/edit`) |

`ASANA_TOKEN`, `BOT_EMAIL_ADDRESS`, `BOT_EMAIL_PASSWORD` are already configured
from the earlier setup — no need to add them again.

### 4. First run

Trigger the workflow manually to verify everything works:

GitHub repo → **Actions** → **Housing Searches Sync** → **Run workflow**.

Expected output in the log:

```
✓ N task(s)
✓ read M form responses
✓ N records (M joined to form rows)
✓ wrote data/housing.json
✓ wrote 4 monthly tab(s)
✓ wrote scoreboard
```

The dashboard appears at `dashboard.hardhat-housing.com/housing.html`.

## How the housing team uses it day-to-day

Status changes happen in **Asana**, not the sheet:

- **New form submission** → Zapier creates an Asana task in *New Housing Searches*
- Housing rep finds options, presents to client → **drag card to** *Housing Search - pending approval*
- Client signs → **drag to** *Closed Housing Search - Moved to Projects*, fill in `Project Number`
- Client passes → **drag to** *Closed Housing Search*, fill in `Close Reason`
- Search isn't actively being worked yet → leave in *Future Housing Searches*

The nightly sync rewrites the master sheet's monthly tabs and scoreboard from
Asana, so nobody needs to edit the sheet manually anymore.

## Email digest

Sent every morning at 7am EDT to:
- carrie, briana, carlos, may, patrice, david @hardhathousing.com

Includes:
- Summary counts (still open / pending / future / stale)
- New searches in the last 24 hours
- Stale alerts: Still Open ≥ 7 days, Pending ≥ 5 days past presentation date
- Active searches grouped by housing rep

Skipped automatically if there are zero active searches.
