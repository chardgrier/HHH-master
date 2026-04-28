#!/usr/bin/env python3
"""
Email helper for HHH dashboard automations.

Reads BOT_EMAIL_ADDRESS + BOT_EMAIL_PASSWORD from env (Gmail SMTP via app
password). Use send_email() from notification scripts.
"""
import os
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587


def send_email(to, subject, html_body, cc=None, text_body=None):
    """Send a multipart email via Gmail SMTP. Raises on failure."""
    sender = os.environ.get("BOT_EMAIL_ADDRESS", "").strip()
    password = os.environ.get("BOT_EMAIL_PASSWORD", "").strip()
    if not sender or not password:
        raise RuntimeError("BOT_EMAIL_ADDRESS / BOT_EMAIL_PASSWORD not set")

    if isinstance(to, str):
        to = [to]
    if cc is None:
        cc = []
    elif isinstance(cc, str):
        cc = [cc]

    msg = MIMEMultipart("alternative")
    msg["From"] = sender
    msg["To"] = ", ".join(to)
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg["Subject"] = subject

    if text_body:
        msg.attach(MIMEText(text_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    recipients = to + cc
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        s.starttls()
        # Gmail app passwords often shown with spaces — strip ALL whitespace
        # including non-breaking spaces (\xa0) which sneak in via copy-paste.
        s.login(sender, re.sub(r"\s+", "", password))
        s.sendmail(sender, recipients, msg.as_string())
    print(f"  ✉ sent to {', '.join(recipients)}: {subject}")
