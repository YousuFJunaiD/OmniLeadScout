import base64
import hashlib
import hmac
import logging
import os
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv
from env_utils import require_env


load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

RAZORPAY_KEY_ID = require_env("RAZORPAY_KEY_ID")
RAZORPAY_KEY_SECRET = require_env("RAZORPAY_KEY_SECRET")

PLAN_PRICES = {
    "starter": {"monthly": 0, "annual": 0},
    "pro": {"monthly": 249900, "annual": 187400},
    "growth": {"monthly": 549900, "annual": 412400},
}

ADDON_PRICES = {
    "leads": 29900,
    "retention": 19900,
    "excel": 39900,
    "scoring": 49900,
    "alerts": 59900,
    "websource": 69900,
    "crm": 99900,
    "automation": 129900,
}

# Backward-compatible alias for older imports.
PLAN_AMOUNTS = {plan: prices["monthly"] for plan, prices in PLAN_PRICES.items()}


import httpx


logger = logging.getLogger(__name__)


def _razorpay_mode(key_id: str) -> str:
    if key_id.startswith("rzp_live_"):
        return "live"
    if key_id.startswith("rzp_test_"):
        return "test"
    return "unknown"

async def create_razorpay_order(plan: str, receipt: str, amount: int | None = None) -> Dict[str, Any]:
    if plan not in PLAN_PRICES:
        raise ValueError("Invalid plan")
    if amount is None:
        amount = PLAN_AMOUNTS.get(plan, 0)
    if amount <= 0:
        return {"order_id": None, "amount": 0, "key_id": RAZORPAY_KEY_ID}
    if not RAZORPAY_KEY_ID or not RAZORPAY_KEY_SECRET:
        raise RuntimeError("Razorpay configuration missing")
    if _razorpay_mode(RAZORPAY_KEY_ID) == "unknown":
        raise RuntimeError("Invalid Razorpay key id format")
    if receipt and len(receipt) > 40:
        raise RuntimeError(f"Razorpay receipt exceeds 40 characters ({len(receipt)})")

    auth = base64.b64encode(f"{RAZORPAY_KEY_ID}:{RAZORPAY_KEY_SECRET}".encode("utf-8")).decode("utf-8")

    payload = {
        "amount": amount,
        "currency": "INR",
        "receipt": receipt,
        "payment_capture": 1,
    }

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(
                "https://api.razorpay.com/v1/orders",
                headers={
                    "Authorization": f"Basic {auth}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
    except httpx.HTTPError as exc:
        logger.exception(
            "Razorpay request failed for plan=%s amount=%s currency=%s mode=%s receipt=%s",
            plan,
            amount,
            payload["currency"],
            _razorpay_mode(RAZORPAY_KEY_ID),
            receipt,
        )
        raise RuntimeError(f"Razorpay request failed: {exc}") from exc

    if response.status_code >= 400:
        body = response.text
        logger.error(
            "Razorpay order creation failed status=%s mode=%s plan=%s amount=%s receipt=%s body=%s",
            response.status_code,
            _razorpay_mode(RAZORPAY_KEY_ID),
            plan,
            amount,
            receipt,
            body,
        )
        try:
            error_data = response.json().get("error", {})
            description = error_data.get("description") or error_data.get("reason") or body
        except Exception:
            description = body
        raise RuntimeError(f"Razorpay order creation failed: {description}")
    data = response.json()
    return {
        "order_id": data.get("id"),
        "amount": amount,
        "key_id": RAZORPAY_KEY_ID,
    }


def compute_payment_amount(plan: str, billing: str = "monthly", addons: List[str] | None = None) -> Dict[str, Any]:
    cleaned_plan = (plan or "").strip().lower()
    cleaned_billing = (billing or "monthly").strip().lower()
    if cleaned_plan not in PLAN_PRICES:
        raise ValueError("Invalid plan")
    if cleaned_plan == "starter":
        return {
            "plan": cleaned_plan,
            "billing": cleaned_billing,
            "addons": [],
            "base_amount": 0,
            "addons_amount": 0,
            "amount": 0,
        }
    if cleaned_billing not in ("monthly", "annual"):
        raise ValueError("Invalid billing cycle")

    unique_addons: list[str] = []
    for addon in addons or []:
        cleaned_addon = (addon or "").strip().lower()
        if not cleaned_addon:
            continue
        if cleaned_addon not in ADDON_PRICES:
            raise ValueError(f"Invalid add-on: {cleaned_addon}")
        if cleaned_addon not in unique_addons:
            unique_addons.append(cleaned_addon)

    base_amount = PLAN_PRICES[cleaned_plan][cleaned_billing]
    addons_amount = sum(ADDON_PRICES[addon] for addon in unique_addons)
    return {
        "plan": cleaned_plan,
        "billing": cleaned_billing,
        "addons": unique_addons,
        "base_amount": base_amount,
        "addons_amount": addons_amount,
        "amount": base_amount + addons_amount,
    }

def verify_razorpay_signature(order_id: str, payment_id: str, signature: str) -> bool:
    if not RAZORPAY_KEY_SECRET:
        return False
    payload = f"{order_id}|{payment_id}".encode("utf-8")
    expected = hmac.new(
        RAZORPAY_KEY_SECRET.encode("utf-8"),
        payload,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature or "")
