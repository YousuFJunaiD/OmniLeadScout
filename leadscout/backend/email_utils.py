import os
from typing import Optional

import requests
from dotenv import load_dotenv
from env_utils import require_env


load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

RESEND_API_KEY = require_env("RESEND_API_KEY")
EMAIL_FROM = os.getenv("RESEND_FROM_EMAIL", "LeadScout <no-reply@example.com>").strip()


def send_email(to: str, subject: str, html: str) -> bool:
    if not RESEND_API_KEY or not to:
        return False
    try:
        response = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "from": EMAIL_FROM,
                "to": [to],
                "subject": subject,
                "html": html,
            },
            timeout=8,
        )
        return response.status_code < 400
    except Exception:
        return False
