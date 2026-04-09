import logging
import os
from typing import Optional

import requests
from dotenv import load_dotenv
from env_utils import require_env


load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

_RAW_RESEND_API_KEY = require_env("RESEND_API_KEY")
RESEND_API_KEY = _RAW_RESEND_API_KEY[_RAW_RESEND_API_KEY.find("re_"):] if "re_" in _RAW_RESEND_API_KEY and not _RAW_RESEND_API_KEY.startswith("re_") else _RAW_RESEND_API_KEY
EMAIL_FROM = os.getenv("RESEND_FROM_EMAIL", "LeadScout <onboarding@resend.dev>").strip()


import httpx

async def send_email(to: str, subject: str, html: str) -> bool:
    if not RESEND_API_KEY or not to:
        return False
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            response = await client.post(
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
                }
            )
            if response.status_code >= 400:
                logging.error("Resend email failed: status=%s body=%s", response.status_code, response.text[:500])
            return response.status_code < 400
    except Exception as exc:
        logging.exception("Resend email request crashed: %s", exc)
        return False
