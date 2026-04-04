import base64
import hashlib
import hmac
import os
from typing import Any, Dict

import requests
from dotenv import load_dotenv
from env_utils import require_env


load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

RAZORPAY_KEY_ID = require_env("RAZORPAY_KEY_ID")
RAZORPAY_KEY_SECRET = require_env("RAZORPAY_KEY_SECRET")

PLAN_AMOUNTS = {
    "starter": 0,
    "pro": 49900,
    "growth": 149900,
}


def create_razorpay_order(plan: str, receipt: str) -> Dict[str, Any]:
    amount = PLAN_AMOUNTS.get(plan, 0)
    if plan not in PLAN_AMOUNTS:
        raise ValueError("Invalid plan")
    if amount <= 0:
        return {"order_id": None, "amount": 0, "key_id": RAZORPAY_KEY_ID}
    if not RAZORPAY_KEY_ID or not RAZORPAY_KEY_SECRET:
        raise RuntimeError("Razorpay configuration missing")

    auth = base64.b64encode(f"{RAZORPAY_KEY_ID}:{RAZORPAY_KEY_SECRET}".encode("utf-8")).decode("utf-8")
    response = requests.post(
        "https://api.razorpay.com/v1/orders",
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json",
        },
        json={
            "amount": amount,
            "currency": "INR",
            "receipt": receipt,
            "payment_capture": 1,
        },
        timeout=10,
    )
    if response.status_code >= 400:
        raise RuntimeError("Failed to create payment order")
    data = response.json()
    return {
        "order_id": data.get("id"),
        "amount": amount,
        "key_id": RAZORPAY_KEY_ID,
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
