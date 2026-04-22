#!/usr/bin/env python3
"""
One-time QuickBooks Online OAuth helper (Production flow).

For Production apps Intuit requires an HTTPS redirect URI, so we use
the public GitHub Pages URL instead of a localhost callback. The flow
is a tiny bit more manual:

  1. This script prints an authorization URL.
  2. You open it in your browser and approve access to your QB company.
  3. Intuit redirects you to https://chardgrier.github.io/HHH-master/
     with ?code=... and &realmId=... appended to the URL.
  4. You copy the full URL from your browser's address bar and paste
     it back into this script.
  5. The script extracts the code, exchanges it for a refresh token,
     and prints the values you need to paste to Claude.
"""
import os, sys, json, webbrowser, secrets, base64
from urllib.parse import urlencode, urlparse, parse_qs

try:
    import requests
except ImportError:
    os.system("pip3 install requests -q --user"); import requests

CLIENT_ID     = os.environ.get("QB_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("QB_CLIENT_SECRET", "")
REDIRECT_URI  = "https://chardgrier.github.io/HHH-master/"
SCOPE         = "com.intuit.quickbooks.accounting"

DISCOVERY_URL = "https://developer.api.intuit.com/.well-known/openid_configuration"

def fetch_endpoints():
    r = requests.get(DISCOVERY_URL, timeout=10)
    r.raise_for_status()
    d = r.json()
    return d["authorization_endpoint"], d["token_endpoint"]

if not CLIENT_ID or not CLIENT_SECRET:
    print("ERROR: set QB_CLIENT_ID and QB_CLIENT_SECRET env vars before running.")
    sys.exit(1)

AUTH_URL, TOKEN_URL = fetch_endpoints()
STATE = secrets.token_urlsafe(16)

def main():
    params = {
        "client_id":     CLIENT_ID,
        "response_type": "code",
        "scope":         SCOPE,
        "redirect_uri":  REDIRECT_URI,
        "state":         STATE,
    }
    url = f"{AUTH_URL}?{urlencode(params)}"

    print("\n" + "═" * 70)
    print("STEP 1: OPEN THIS URL IN YOUR BROWSER")
    print("═" * 70)
    print(url)
    print("═" * 70)
    print()
    print("Your browser should open automatically. If not, copy/paste the URL above.")
    print("Sign in to QuickBooks → select Hard Hat Housing → click Connect.")
    print()
    webbrowser.open(url)

    print("═" * 70)
    print("STEP 2: PASTE THE REDIRECT URL")
    print("═" * 70)
    print("After clicking Connect, your browser will redirect to the HHH dashboard")
    print("with a URL like:")
    print("  https://chardgrier.github.io/HHH-master/?code=XXX&state=YYY&realmId=ZZZ")
    print()
    print("Copy the ENTIRE URL from your browser's address bar and paste it below,")
    print("then press Enter:")
    print("-" * 70)
    callback_url = input("URL: ").strip()

    u = urlparse(callback_url)
    q = parse_qs(u.query)
    code     = q.get("code",    [""])[0]
    state    = q.get("state",   [""])[0]
    realm_id = q.get("realmId", [""])[0]
    error    = q.get("error",   [""])[0]

    if error:
        print(f"\n❌ Intuit returned error: {error}"); sys.exit(1)
    if not code or not realm_id:
        print("\n❌ URL missing code or realmId. Make sure you copied the full redirect URL.")
        sys.exit(1)
    if state != STATE:
        print(f"\n⚠ State mismatch (expected {STATE!r}, got {state!r}).")
        print("  This can happen if you ran the script twice — re-run to be safe.")
        sys.exit(1)

    print("\n✓ Got authorization code, exchanging for refresh token…")

    basic = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    r = requests.post(TOKEN_URL,
        headers={"Authorization": f"Basic {basic}",
                 "Accept": "application/json",
                 "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type":"authorization_code",
              "code": code,
              "redirect_uri": REDIRECT_URI},
        timeout=30)
    if r.status_code >= 300:
        print(f"❌ Token exchange failed: {r.status_code}\n{r.text}"); sys.exit(1)
    t = r.json()

    print("\n" + "═" * 70)
    print("SUCCESS — copy these two values and paste them back to Claude:")
    print("═" * 70)
    print(f"  QB_REFRESH_TOKEN = {t['refresh_token']}")
    print(f"  QB_REALM_ID      = {realm_id}")
    print("═" * 70)
    print("\n(These will be stored as GitHub secrets for the nightly sync.)")

if __name__ == "__main__":
    main()
