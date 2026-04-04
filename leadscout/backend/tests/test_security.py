import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "service-key")
os.environ.setdefault("JWT_SECRET", "test-jwt-secret")
os.environ.setdefault("RAZORPAY_KEY_ID", "rzp_test_key")
os.environ.setdefault("RAZORPAY_KEY_SECRET", "rzp_test_secret")
os.environ.setdefault("RESEND_API_KEY", "re_test_key")
os.environ.setdefault("FRONTEND_ORIGIN", "http://localhost:5173")

import main


class SecurityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(main.app)

    def setUp(self):
        main.user_request_log.clear()
        main.auth_attempt_log.clear()
        main.email_request_log.clear()

    def test_invalid_token_fails(self):
        response = self.client.get(
            "/auth/me",
            headers={"Authorization": "Bearer invalid-token"},
        )
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"], "Invalid authorization token")

    def test_rate_limit_blocks_after_threshold(self):
        with patch.object(main, "decode_access_token", return_value={"sub": "user-1"}), patch.object(
            main,
            "get_user_by_id",
            return_value={"id": "user-1", "email": "test@example.com", "plan": "starter"},
        ), patch.object(
            main,
            "calculate_usage",
            return_value={
                "plan": "starter",
                "leads_used_this_month": 0,
                "searches_today": 0,
                "limits": main.PLAN_LIMITS["starter"],
            },
        ):
            headers = {"Authorization": "Bearer good-token"}
            for _ in range(main.RATE_LIMIT_REQUESTS):
                response = self.client.get("/user/usage", headers=headers)
                self.assertEqual(response.status_code, 200)

            blocked = self.client.get("/user/usage", headers=headers)
            self.assertEqual(blocked.status_code, 429)
            self.assertEqual(blocked.json()["detail"], "Rate limit exceeded")

    def test_bad_input_is_rejected(self):
        with patch.object(main, "decode_access_token", return_value={"sub": "user-1"}), patch.object(
            main,
            "get_user_by_id",
            return_value={"id": "user-1", "email": "test@example.com", "plan": "starter"},
        ):
            response = self.client.post(
                "/scrape/v2/start",
                headers={"Authorization": "Bearer good-token"},
                json={
                    "user_id": "user-1",
                    "niche": "contractors",
                    "city": "",
                    "queries": ["roofing"],
                    "enable_maps": True,
                    "enable_justdial": False,
                    "enable_indiamart": False,
                    "website_filter": "minimal",
                    "max_per_query": 25,
                    "use_proxy": False,
                },
            )
        self.assertEqual(response.status_code, 422)

    def test_unauthorized_scrape_is_blocked(self):
        response = self.client.post(
            "/scrape/v2/start",
            json={
                "user_id": "user-1",
                "niche": "contractors",
                "city": "Mumbai",
                "queries": ["roofing"],
                "enable_maps": True,
                "enable_justdial": False,
                "enable_indiamart": False,
                "website_filter": "minimal",
                "max_per_query": 25,
                "use_proxy": False,
            },
        )
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"], "Missing authorization token")

    def test_payment_verification_requires_valid_signature(self):
        with patch.object(main, "decode_access_token", return_value={"sub": "user-1"}), patch.object(
            main,
            "get_user_by_id",
            return_value={"id": "user-1", "email": "test@example.com", "plan": "starter", "role": "user"},
        ), patch.object(main, "verify_razorpay_signature", return_value=False):
            response = self.client.post(
                "/payment/verify",
                headers={"Authorization": "Bearer good-token"},
                json={
                    "razorpay_order_id": "order_123",
                    "razorpay_payment_id": "pay_123",
                    "razorpay_signature": "bad-signature",
                    "plan": "pro",
                },
            )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "Invalid payment signature")

    def test_admin_access_is_restricted(self):
        with patch.object(main, "decode_access_token", return_value={"sub": "user-1"}), patch.object(
            main,
            "get_user_by_id",
            return_value={"id": "user-1", "email": "test@example.com", "plan": "starter", "role": "user"},
        ):
            response = self.client.get(
                "/admin/users",
                headers={"Authorization": "Bearer good-token"},
            )
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["detail"], "Admin access required")


if __name__ == "__main__":
    unittest.main()
