#!/usr/bin/env python3
"""
One-time QuickBooks Online OAuth helper.

Run this locally. It will:
  1. Open your browser to the Intuit authorization page.
  2. You sign in (to your QuickBooks company) and click Authorize.
  3. Intuit redirects back to http://localhost:8765/callback with a code
     + your Realm ID (QB company ID).
  4. This script exchanges the code for a refresh token and prints it.

Paste the REFRESH TOKEN and REALM ID it prints back to Claude; they get
stored as GitHub secrets and the nightly sync uses them from there.

Refresh tokens are valid for 100 days from last use, and re-validate
every time the sync script runs, so they effectively don't expire as
long as the nightly job keeps running.
"""
import os, sys, json, webbrowser, secrets, base64
from urllib.parse import urlencode, urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, HTTPServer

try:
    import requests
except ImportError:
    os.system("pip3 install requests -q --user"); import requests

# These get filled in from the env at runtime
CLIENT_ID     = os.environ.get("QB_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("QB_CLIENT_SECRET", "")
REDIRECT_URI  = "http://localhost:8765/callback"
SCOPE         = "com.intuit.quickbooks.accounting"

AUTH_URL  = "https://appcenter.intuit.com/connect/oauth2"
TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

if not CLIENT_ID or not CLIENT_SECRET:
    print("ERROR: set QB_CLIENT_ID and QB_CLIENT_SECRET env vars before running.")
    print("       export QB_CLIENT_ID='...'")
    print("       export QB_CLIENT_SECRET='...'")
    sys.exit(1)

STATE   = secrets.token_urlsafe(16)
RESULT  = {}

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a, **k): pass
    def do_GET(self):
        u = urlparse(self.path)
        if u.path != "/callback":
            self.send_response(404); self.end_headers(); return
        q = parse_qs(u.query)
        if q.get("state", [""])[0] != STATE:
            self.send_response(400); self.end_headers()
            self.wfile.write(b"State mismatch."); return
        RESULT["code"]     = q.get("code",    [""])[0]
        RESULT["realmId"]  = q.get("realmId", [""])[0]
        RESULT["error"]    = q.get("error",   [""])[0]
        self.send_response(200); self.send_header("Content-Type","text/html"); self.end_headers()
        msg = "✓ Authorization received. You can close this window and return to the terminal."
        if RESULT["error"]:
            msg = f"❌ Error: {RESULT['error']}"
        self.wfile.write(f"<html><body style='font-family:sans-serif;padding:40px'><h2>{msg}</h2></body></html>".encode())

def main():
    params = {
        "client_id":     CLIENT_ID,
        "response_type": "code",
        "scope":         SCOPE,
        "redirect_uri":  REDIRECT_URI,
        "state":         STATE,
    }
    url = f"{AUTH_URL}?{urlencode(params)}"
    print("\n═══ QuickBooks OAuth ═══")
    print("Opening your browser to Intuit's authorization page…")
    print(f"(If it doesn't open, copy/paste this URL manually:\n  {url}\n)")
    print("\nWaiting for callback on http://localhost:8765 …")
    webbrowser.open(url)

    server = HTTPServer(("127.0.0.1", 8765), Handler)
    server.handle_request()  # handles exactly one request, then exits

    if RESULT.get("error"):
        print(f"\n❌ OAuth failed: {RESULT['error']}"); sys.exit(1)
    if not RESULT.get("code"):
        print("\n❌ No code received."); sys.exit(1)

    print("✓ Got authorization code, exchanging for refresh token…")

    # Exchange the code for an access + refresh token
    basic = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    r = requests.post(TOKEN_URL,
        headers={"Authorization": f"Basic {basic}",
                 "Accept": "application/json",
                 "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type":"authorization_code",
              "code": RESULT["code"],
              "redirect_uri": REDIRECT_URI},
        timeout=30)
    if r.status_code >= 300:
        print(f"❌ Token exchange failed: {r.status_code}\n{r.text}"); sys.exit(1)
    t = r.json()

    print("\n" + "═" * 70)
    print("SUCCESS — copy these two values and paste them back to Claude:")
    print("═" * 70)
    print(f"  QB_REFRESH_TOKEN = {t['refresh_token']}")
    print(f"  QB_REALM_ID      = {RESULT['realmId']}")
    print("═" * 70)
    print("\n(These will be stored as GitHub secrets and used by the nightly sync.)")

if __name__ == "__main__":
    main()
