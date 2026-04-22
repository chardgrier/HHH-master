# QuickBooks Online Integration — Setup Status

## Current state: PAUSED

The OAuth flow works end-to-end, but we're connected to a sandbox test
company because the app only has Development keys. Getting real Hard Hat
Housing data flowing requires Intuit Production keys.

## What's already done

- `qb_oauth_helper.py` — local script for one-time authorization.
- `qb_sync.py` — pulls invoices + bills from QB, matches to dashboard
  projects by customer name. Reads `QB_ENV=sandbox|production` env var
  for endpoint selection.
- GitHub secrets stored: `QB_CLIENT_ID`, `QB_CLIENT_SECRET`,
  `QB_REFRESH_TOKEN`, `QB_REALM_ID` (currently sandbox).
- Redirect URI registered: `http://localhost:8765/callback`.

## When you're ready to switch to production

1. Go to **developer.intuit.com → My Apps → HHH Dashboard Sync → Keys & Credentials**.
2. Click the **Production** tab.
3. Complete the compliance checklist (about 50 minutes total):
   - Review Intuit Developer Portal profile and verify email
   - Add EULA + privacy policy URL (free generators online)
   - Add host domain / launch URL / connect URL
     (you can use `https://chardgrier.github.io/HHH-master/` for all three)
   - Select a category
   - Answer regulated-industries question
   - Say "Self-hosted" for hosting
4. Production credentials will unlock. Copy the Production **Client ID**
   and **Client Secret** and share with Claude, who will:
   - Update the `QB_CLIENT_ID` + `QB_CLIENT_SECRET` GitHub secrets.
   - Walk you through re-running `qb_oauth_helper.py` against your real
     Hard Hat Housing company.
   - Update `QB_REFRESH_TOKEN` + `QB_REALM_ID` secrets.
   - Flip the `QB_ENV` default to `production` in `qb_sync.py`.
   - Add `qb_sync.py` to the nightly workflow.
   - Build the dashboard cell icons (paid / sent / overdue / none).

## What dashboard features are pending

- Per-month cell icons showing invoice/bill status.
- Click-cell-to-act: create invoice, mark sent, mark paid.
- Homeowner-vendor → specific-project matching.
- Pending for now until production keys are in place.
