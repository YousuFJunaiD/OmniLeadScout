"""
LeadScout Backend — FastAPI
Run: uvicorn main:app --reload --port 8000
Place this file inside: leadscout/backend/
"""

import asyncio
import collections
import sys
import json
import os
import csv
import random
import secrets
import time
import uuid
import hashlib
import sqlite3
from datetime import datetime, timedelta
# from functools import lru_cache
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, field_validator
import logging

from auth_utils import create_access_token, decode_access_token, hash_password_bcrypt, should_refresh_token, verify_password
from email_utils import send_email
from env_utils import require_env, validate_required_env
import payment_utils
from supabase_db import (
    PLAN_LIMITS,
    calculate_usage,
    create_scrape_job,
    create_user,
    enforce_plan,
    get_scrape_job,
    get_user_by_email,
    get_user_by_id,
    list_all_users,
    list_user_history as list_user_history_supabase,
    list_user_leads,
    save_leads as save_leads_supabase,
    update_user_plan,
    update_user_fields,
    update_scrape_job,
)

class EndpointFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return not any(
            x in msg for x in [
                "GET /scrape/status/",
                "GET /user/history/",
                "GET /scrape/heartbeat/"
            ]
        )

# Add to uvicorn access logger
logging.getLogger("uvicorn.access").addFilter(EndpointFilter())

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
validate_required_env(
    [
        "SUPABASE_URL",
        "SUPABASE_SERVICE_KEY",
        "JWT_SECRET",
        "RAZORPAY_KEY_ID",
        "RAZORPAY_KEY_SECRET",
        "RESEND_API_KEY",
    ]
)

FRONTEND_ORIGIN = require_env("FRONTEND_ORIGIN")
PRIMARY_FRONTEND_ORIGIN = next(
    (origin.strip().rstrip("/") for origin in FRONTEND_ORIGIN.split(",") if origin.strip()),
    "http://localhost:5173",
)
MAX_REQUEST_BYTES = int(os.getenv("MAX_REQUEST_BYTES", "1048576"))
ALLOWED_ORIGINS = sorted(
    {
        origin.strip()
        for origin in [
            *FRONTEND_ORIGIN.split(","),
            "https://omnimate.org",
            "https://www.omnimate.org",
            "https://omni-lead-scout.vercel.app",
        ]
        if origin.strip()
    }
)

app = FastAPI(title="LeadScout API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def success_response(data=None, message: str = "OK", **extra):
    payload = {
        "success": True,
        "message": message,
        "data": data,
    }
    payload.update(extra)
    return payload


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    detail = exc.detail
    message = detail if isinstance(detail, str) else (detail.get("message") or detail.get("error") or "Request failed")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "success": False,
            "message": message,
            "error": detail,
            "data": None,
            "detail": detail,
        },
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    sanitized_errors = []
    for item in exc.errors():
        sanitized = dict(item)
        ctx = sanitized.get("ctx")
        if isinstance(ctx, dict):
            sanitized["ctx"] = {key: str(value) for key, value in ctx.items()}
        sanitized_errors.append(sanitized)
    return JSONResponse(
        status_code=422,
        content={
            "success": False,
            "message": "Validation failed",
            "error": sanitized_errors,
            "data": None,
            "detail": sanitized_errors,
        },
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logging.exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "message": "Internal server error",
            "error": "Internal server error",
            "data": None,
            "detail": "Internal server error",
        },
    )

_job_queue = None


def build_razorpay_receipt(user_id: str) -> str:
    safe_user = "".join(ch for ch in str(user_id or "") if ch.isalnum()).lower()
    return f"ord_{safe_user[:12]}_{uuid.uuid4().hex[:12]}"[:40]

async def job_worker_loop(worker_id: int):
    while True:
        try:
            if _job_queue is None:
                await asyncio.sleep(1)
                continue
            job_id, job_type = await _job_queue.get()
            try:
                job = active_jobs.get(job_id)
                if not job or job.get("status") != "pending":
                    continue
                    
                job["status"] = "running"
                job["progress_message"] = "Searching sources..."
                try:
                    update_job_db(job_id, "running", job.get("lead_count", 0))
                    update_scrape_job(job_id, status="running", leads_found=int(job.get("lead_count", 0)))
                except Exception as sync_exc:
                    logging.warning("Failed to mark job %s as running: %s", job_id, sync_exc)
                try:
                    if job_type == "v2":
                        await asyncio.wait_for(run_job_v2(job_id), timeout=300)
                    else:
                        await asyncio.wait_for(run_job(job_id), timeout=300)
                except Exception as e:
                    logging.error(f"[Worker {worker_id}] Job {job_id} failed: {e}")
                    if not job.get("retried"):
                        job["retried"] = True
                        job["status"] = "pending"
                        await asyncio.sleep(3)
                        await _job_queue.put((job_id, job_type))
                    else:
                        job["status"] = "failed"
            finally:
                _job_queue.task_done()
        except Exception as global_err:
            logging.error(f"[Worker {worker_id}] Critical loop error: {global_err}")
            await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    global _job_queue
    _job_queue = asyncio.Queue(maxsize=100)
    for i in range(5):
        asyncio.create_task(job_worker_loop(i))

@app.on_event("shutdown")
async def shutdown_event():
    logging.info("Shutting down workers gracefully...")
    # Drain tasks if necessary or let process terminate natively

DB_PATH    = "leadscout.db"
OUTPUT_DIR = Path("../output")
OUTPUT_DIR.mkdir(exist_ok=True)
active_jobs = {}
auth_scheme = HTTPBearer(auto_error=False)

EVENT_BUFFER_LIMIT = 800
WS_QUEUE_MAXSIZE = 120

_history_cache = {}
_usage_cache = {}
CACHE_TTL = 30

def _get_cached_history(user_id):
    cached = _history_cache.get(user_id)
    if cached and time.time() - cached[1] < CACHE_TTL:
        return cached[0]
    data = list_user_history_supabase(user_id)
    _history_cache[user_id] = (data, time.time())
    return data

def _get_cached_usage(user_id):
    cached = _usage_cache.get(user_id)
    if cached and time.time() - cached[1] < CACHE_TTL:
        return cached[0]
    data = calculate_usage(user_id)
    _usage_cache[user_id] = (data, time.time())
    return data

def _env_int(name: str, default: int, min_value: int, max_value: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(min_value, min(max_value, value))


DB_FLUSH_EVERY = _env_int("LEADSCOUT_DB_FLUSH_EVERY", 80, 10, 500)
AREA_CONCURRENCY = _env_int("LEADSCOUT_AREA_CONCURRENCY", 1, 1, 10)
BLOCK_COOLDOWN_SECONDS = _env_int("LEADSCOUT_BLOCK_COOLDOWN_SECONDS", 300, 30, 1800)
BLOCK_RECOVERY_MAX_ATTEMPTS = _env_int("LEADSCOUT_BLOCK_RECOVERY_MAX_ATTEMPTS", 12, 1, 100)
V2_BATCH_SIZE = _env_int("LEADSCOUT_V2_BATCH_SIZE", 5, 1, 10)
V2_SCRAPE_CONCURRENCY = _env_int("LEADSCOUT_V2_SCRAPE_CONCURRENCY", 5, 1, 10)
V2_WEBSITE_CONCURRENCY = _env_int("LEADSCOUT_V2_WEBSITE_CONCURRENCY", 8, 1, 20)
MAPS_BROWSER_CONCURRENCY = _env_int("LEADSCOUT_MAPS_BROWSER_CONCURRENCY", 1, 1, 3)
RATE_LIMIT_REQUESTS = _env_int("LEADSCOUT_RATE_LIMIT_REQUESTS", 200, 1, 1000)
RATE_LIMIT_WINDOW_SECONDS = _env_int("LEADSCOUT_RATE_LIMIT_WINDOW_SECONDS", 60, 10, 3600)
REQUEST_TIMEOUT_SECONDS = _env_int("LEADSCOUT_REQUEST_TIMEOUT_SECONDS", 8, 5, 10)
REQUEST_DELAY_MIN_MS = _env_int("LEADSCOUT_REQUEST_DELAY_MIN_MS", 500, 100, 5000)
REQUEST_DELAY_MAX_MS = _env_int("LEADSCOUT_REQUEST_DELAY_MAX_MS", 2000, 200, 8000)
AUTH_RATE_LIMIT_REQUESTS = _env_int("LEADSCOUT_AUTH_RATE_LIMIT_REQUESTS", 5, 1, 50)
AUTH_RATE_LIMIT_WINDOW_SECONDS = _env_int("LEADSCOUT_AUTH_RATE_LIMIT_WINDOW_SECONDS", 300, 10, 3600)
EMAIL_RATE_LIMIT_REQUESTS = _env_int("LEADSCOUT_EMAIL_RATE_LIMIT_REQUESTS", 3, 1, 20)
maps_browser_semaphore = asyncio.Semaphore(MAPS_BROWSER_CONCURRENCY)
EMAIL_RATE_LIMIT_WINDOW_SECONDS = _env_int("LEADSCOUT_EMAIL_RATE_LIMIT_WINDOW_SECONDS", 3600, 60, 86400)
user_request_log: dict[str, collections.deque] = {}
auth_attempt_log: dict[str, collections.deque] = {}
email_request_log: dict[str, collections.deque] = {}


def _env_proxy_list() -> list[dict[str, str]]:
    raw = os.getenv("LEADSCOUT_PROXIES", "").strip()
    if not raw:
        return []
    proxies = []
    for item in raw.split(","):
        value = item.strip()
        if not value:
            continue
        proxies.append({"http": value, "https": value})
    return proxies


ENV_PROXIES = _env_proxy_list()


def _enforce_rate_limit(bucket_map: dict[str, collections.deque], key: str, limit: int, window_seconds: int) -> bool:
    now = time.time()
    bucket = bucket_map.setdefault(key, collections.deque())
    while bucket and now - bucket[0] > window_seconds:
        bucket.popleft()
    if len(bucket) >= limit:
        return False
    bucket.append(now)
    return True


def _auth_rate_limit_key(request: Request, email: str = "") -> str:
    host = request.client.host if request and request.client else "unknown"
    return f"{host}:{(email or '').strip().lower()}"


@app.middleware("http")
async def request_size_limit_middleware(request: Request, call_next):
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > MAX_REQUEST_BYTES:
                return JSONResponse(status_code=413, content={"detail": "Request too large"})
        except ValueError:
            return JSONResponse(status_code=400, content={"detail": "Invalid content length"})
    return await call_next(request)


def _validate_short_text(value: str, field_name: str, max_len: int = 120) -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        raise ValueError(f"{field_name} is required")
    if len(cleaned) > max_len:
        raise ValueError(f"{field_name} is too long")
    return cleaned


def _validate_text_list(values: list[str], field_name: str, max_items: int = 25, max_len: int = 120) -> list[str]:
    cleaned = []
    for value in values or []:
        item = (value or "").strip()
        if not item:
            continue
        if len(item) > max_len:
            raise ValueError(f"{field_name} item is too long")
        cleaned.append(item)
    if not cleaned:
        raise ValueError(f"{field_name} must contain at least one item")
    if len(cleaned) > max_items:
        raise ValueError(f"{field_name} has too many items")
    return cleaned


def _public_user(user: dict) -> dict:
    plan = user.get("plan") or "starter"
    return {
        "id": user.get("id"),
        "name": user.get("full_name") or user.get("name") or "",
        "email": user.get("email") or "",
        "plan": plan.lower() if isinstance(plan, str) and plan else "starter",
        "role": user.get("role") or "user",
    }

def create_auth_response(user: dict, message: str = "Authenticated successfully") -> dict:
    public_user = _public_user(user)
    logging.info(
        "Auth response user_id=%s email=%s role=%s",
        public_user["id"],
        public_user["email"],
        public_user["role"],
    )
    token = create_access_token(
        {
            "sub": public_user["id"],
            "email": public_user["email"],
            "plan": public_user["plan"],
            "role": public_user["role"],
        }
    )
    return success_response(
        {"token": token, "user": public_user},
        message=message,
        token=token,
        user=public_user,
    )


def get_cached_user(user_id: str):
    user = get_user_by_id(user_id)
    if user:
        logging.info(
            "Auth lookup user_id=%s email=%s db_role=%s",
            user.get("id"),
            user.get("email"),
            user.get("role") or "user",
        )
        return {k: v for k, v in user.items() if k not in ["hashed_password", "password_hash", "token", "access_token"]}
    return None

def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(auth_scheme),
):
    if not credentials or credentials.scheme.lower() != "bearer":
        logging.warning("Auth failed: missing bearer token for %s", request.url.path)
        raise HTTPException(401, "Missing authorization token")
    payload = decode_access_token(credentials.credentials)
    user_id = (payload or {}).get("sub")
    if not user_id:
        logging.warning("Auth failed: invalid token for %s", request.url.path)
        raise HTTPException(401, "Invalid authorization token")
    try:
        user = get_cached_user(user_id)
    except Exception as exc:
        logging.error("Auth backend unavailable on %s: %s", request.url.path, exc)
        raise HTTPException(500, "Authentication backend unavailable")
    if not user:
        logging.warning("Auth failed: unknown user %s for %s", user_id, request.url.path)
        raise HTTPException(401, "User not found")
    logging.info(
        "Auth current user path=%s user_id=%s role=%s",
        request.url.path,
        user.get("id"),
        user.get("role") or "user",
    )
    if request.url.path.startswith("/scrape") or request.url.path.startswith("/user") or request.url.path.startswith("/admin"):
        if not _enforce_rate_limit(user_request_log, user_id, RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW_SECONDS):
            logging.warning("Rate limit exceeded for user %s on %s", user_id, request.url.path)
            raise HTTPException(429, "Rate limit exceeded")
            
    return user


def require_admin_user(current_user=Depends(get_current_user)):
    if (current_user.get("role") or "user").lower() != "admin":
        logging.warning("Admin access denied for user %s", current_user.get("id"))
        raise HTTPException(403, "Admin access required")
    return current_user


def _safe_slug(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in (value or ""))
    return cleaned.strip("_") or "leads"


def build_job_csv_path(niche: str, job_id: str) -> str:
    return str(OUTPUT_DIR / f"{_safe_slug(niche)}_{job_id[:8]}.csv")


CSV_EXPORT_HEADERS = ["name", "phone", "email", "website", "city", "category", "source"]


def _normalize_csv_value(value: object, fallback: str = "") -> str:
    text = str(value or "").strip()
    return text or fallback


def _csv_export_row(record: dict) -> dict:
    return {
        "name": _normalize_csv_value(record.get("name") or record.get("Name")),
        "phone": _normalize_csv_value(record.get("phone") or record.get("Phone"), "Not Available"),
        "email": _normalize_csv_value(
            record.get("email")
            or record.get("Email")
            or record.get("Owner_Email_Guesses"),
            "Not Available",
        ),
        "website": _normalize_csv_value(record.get("website") or record.get("Website")),
        "city": _normalize_csv_value(record.get("city") or record.get("City")),
        "category": _normalize_csv_value(record.get("category") or record.get("Category")),
        "source": _normalize_csv_value(record.get("source") or record.get("Source")),
    }


def _can_download_csv_for_user(user: dict) -> bool:
    role = str(user.get("role") or "user").strip().lower()
    if role == "admin":
        return True
    plan = str(user.get("plan") or "starter").strip().lower()
    return plan in {"pro", "growth", "team"}


async def _send_welcome_email(user: dict):
    await send_email(
        user.get("email", ""),
        "Welcome to LeadScout",
        f"""
        <h2>Welcome to LeadScout</h2>
        <p>Hi {user.get("full_name") or user.get("name") or "there"},</p>
        <p>Your account is ready. Choose a plan and start scraping leads.</p>
        """,
    )


async def _send_payment_email(user: dict, plan: str, amount: int):
    await send_email(
        user.get("email", ""),
        "LeadScout payment confirmed",
        f"""
        <h2>Payment successful</h2>
        <p>Your <strong>{plan.title()}</strong> plan is now active.</p>
        <p>Amount received: INR {amount / 100:.2f}</p>
        """,
    )


async def _send_scrape_ready_email(user: dict, lead_count: int, status: str):
    if status != "completed":
        return
    await send_email(
        user.get("email", ""),
        "Your LeadScout leads are ready",
        f"""
        <h2>Your leads are ready</h2>
        <p>Your latest scrape completed successfully.</p>
        <p>Total leads collected: <strong>{lead_count}</strong></p>
        <p>Sign in to download your CSV and review the results.</p>
        """,
    )


def _store_payment_record(user_id: str, plan: str, amount: int, provider_order_id: str, provider_payment_id: str, status: str):
    conn = get_db()
    conn.execute(
        """
        INSERT INTO payments (user_id, plan, amount, provider, provider_order_id, provider_payment_id, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (user_id, plan, amount, "razorpay", provider_order_id, provider_payment_id, status),
    )
    conn.commit()
    conn.close()


def _create_password_reset_token(user: dict) -> str:
    token = secrets.token_urlsafe(32)
    expires_at = (datetime.utcnow() + timedelta(hours=1)).isoformat()
    conn = get_db()
    conn.execute("UPDATE password_reset_tokens SET used=1 WHERE user_id=?", (user["id"],))
    conn.execute(
        """
        INSERT INTO password_reset_tokens (user_id, email, token, expires_at, used)
        VALUES (?, ?, ?, ?, 0)
        """,
        (user["id"], user["email"], token, expires_at),
    )
    conn.commit()
    conn.close()
    return token


def _admin_user_stats() -> dict[str, dict]:
    conn = get_db()
    job_counts = {
        row["user_id"]: {"job_count": row["job_count"] or 0, "total_leads": row["total_leads"] or 0}
        for row in conn.execute(
            """
            SELECT j.user_id, COUNT(j.job_id) AS job_count, COALESCE(SUM(j.lead_count), 0) AS total_leads
            FROM jobs j
            GROUP BY j.user_id
            """
        ).fetchall()
    }
    conn.close()
    return job_counts


def generate_job_csv_from_db(job_id: str, niche: str) -> Optional[str]:
    conn = get_db()
    rows = conn.execute(
        "SELECT data FROM leads WHERE job_id=? ORDER BY id ASC",
        (job_id,),
    ).fetchall()
    conn.close()

    records = []
    for row in rows:
        try:
            records.append(json.loads(row["data"]))
        except Exception:
            continue

    if not records:
        return None

    rows = [_csv_export_row(rec) for rec in records]

    csv_path = build_job_csv_path(niche, job_id)
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_EXPORT_HEADERS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    return csv_path


def _write_records_to_csv(records: list[dict], export_path: Path) -> Optional[str]:
    if not records:
        return None
    rows = [_csv_export_row(rec) for rec in records]

    with open(export_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_EXPORT_HEADERS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    return str(export_path)


def _collect_records_by_job_ids(job_ids: list[str], user_id: Optional[str] = None) -> list[dict]:
    if not job_ids:
        return []

    placeholders = ",".join("?" for _ in job_ids)
    sql = f"SELECT data FROM leads WHERE job_id IN ({placeholders})"
    params = list(job_ids)
    if user_id:
        sql += " AND user_id=?"
        params.append(user_id)
    sql += " ORDER BY id ASC"

    conn = get_db()
    rows = conn.execute(sql, tuple(params)).fetchall()
    conn.close()

    records = []
    for row in rows:
        try:
            records.append(json.loads(row["data"]))
        except Exception:
            continue
    return records


def generate_niche_csv_for_user(user_id: str, niche: str) -> Optional[str]:
    conn = get_db()
    job_rows = conn.execute(
        "SELECT job_id FROM jobs WHERE user_id=? AND niche=? ORDER BY created_at ASC",
        (user_id, niche),
    ).fetchall()
    conn.close()

    job_ids = [r["job_id"] for r in job_rows if r and r["job_id"]]
    records = _collect_records_by_job_ids(job_ids, user_id=user_id)
    export_path = OUTPUT_DIR / f"{_safe_slug(niche)}_{_safe_slug(user_id)}_combined.csv"
    return _write_records_to_csv(records, export_path)


def update_job_db(job_id: str, status: str, lead_count: Optional[int] = None, csv_path: Optional[str] = None):
    conn = get_db()
    if lead_count is None and csv_path is None:
        conn.execute("UPDATE jobs SET status=? WHERE job_id=?", (status, job_id))
    elif csv_path is None:
        conn.execute("UPDATE jobs SET status=?,lead_count=? WHERE job_id=?", (status, lead_count, job_id))
    else:
        conn.execute(
            "UPDATE jobs SET status=?,lead_count=?,csv_path=? WHERE job_id=?",
            (status, lead_count or 0, csv_path, job_id)
        )
    conn.commit()
    conn.close()


def publish_event(job: dict, message: dict):
    events = job.setdefault("events", [])
    events.append(message)
    if len(events) > EVENT_BUFFER_LIMIT:
        del events[: len(events) - EVENT_BUFFER_LIMIT]

    if message.get("type") == "lead":
        job["lead_count"] = int(job.get("lead_count", 0)) + 1
        job["progress_message"] = f"Saved {job['lead_count']} leads"
    elif message.get("type") == "progress":
        data = message.get("data") or {}
        job["current_query"] = str(data.get("query") or job.get("current_query") or "")
        job["progress_message"] = f"{int(data.get('current', 0))}/{int(data.get('total', 0))} steps • {job['current_query']}".strip()
    elif message.get("type") in ("info", "error", "block_wait"):
        data = message.get("data") or ""
        if isinstance(data, dict):
          job["progress_message"] = str(data.get("reason") or data.get("query") or data)
        else:
          job["progress_message"] = str(data)

    dead_listeners = []
    for q in list(job.get("listeners", set())):
        try:
            q.put_nowait(message)
        except Exception:
            dead_listeners.append(q)

    for q in dead_listeners:
        job.get("listeners", set()).discard(q)


def resolve_final_job_status(cancelled: bool, total_found: int, accepted_leads: int, source_failures: list[str] | None = None) -> str:
    if cancelled:
        return "stopped"
    total_found = int(total_found or 0)
    accepted_leads = int(accepted_leads or 0)
    if total_found <= 0 and accepted_leads <= 0 and source_failures:
        return "source_error"
    if total_found <= 0 and accepted_leads <= 0:
        return "no_results"
    if total_found > 0 and accepted_leads <= 0:
        return "low_data"
    return "completed"


def final_status_message(status: str) -> str:
    if status == "completed":
        return "Saving leads complete."
    if status == "no_results":
        return "No data found. Try broader query or different location."
    if status == "low_data":
        return "Low data found. Results were filtered out. Try broader query or different location."
    if status == "source_error":
        return "Source timeout or block detected. Try broader query or different location."
    if status == "stopped":
        return "Scrape stopped."
    if status == "failed":
        return "Scrape failed. Try broader query or different location."
    return "Scrape finished."


def _normalize_supabase_queries(value) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        except Exception:
            cleaned = value.strip()
            return [cleaned] if cleaned else []
    return []


def _build_supabase_job_status_payload(job_row: dict) -> dict:
    queries = _normalize_supabase_queries(job_row.get("queries"))
    recent_events = job_row.get("recent_events") or []
    if isinstance(recent_events, str):
        try:
            recent_events = json.loads(recent_events)
        except Exception:
            recent_events = []
    status = job_row.get("status") or "queued"
    payload = {
        "job_id": job_row.get("id"),
        "status": status,
        "lead_count": int(job_row.get("leads_found") or 0),
        "processed_areas": int(job_row.get("processed_areas") or 0),
        "total_areas": int(job_row.get("total_areas") or (len(queries) if queries else 0)),
        "current_query": job_row.get("current_query") or "",
        "progress_message": job_row.get("progress_message") or ("Queued for worker" if status == "queued" else final_status_message(status)),
        "recent_events": list(recent_events),
        "running": status in ("queued", "running", "stopping"),
        "csv_path": job_row.get("csv_path"),
        "profession": ", ".join(queries),
        "location": job_row.get("city") or "",
    }
    return payload


def _normalize_supabase_progress_marker(value) -> dict:
    if isinstance(value, dict):
        marker = dict(value)
    elif isinstance(value, str):
        try:
            marker = json.loads(value)
        except Exception:
            marker = {}
    else:
        marker = {}
    marker.setdefault("version", 1)
    marker.setdefault("items", {})
    marker.setdefault("current_item", None)
    return marker


async def run_job(job_id: str):
    job = active_jobs.get(job_id)
    if not job:
        return

    profession = job["profession"]
    areas = job["areas"]
    user_id = job["user_id"]
    niche = job["niche"]

    conn = None
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from scraper_core import scrape_area_yields
        from utils import load_seen_leads, save_seen_leads, make_lead_key

        seen = load_seen_leads()
        total = len(areas)
        accepted_leads = 0
        raw_found = 0
        source_failures: list[str] = []
        completed_indexes = set()
        csv_path = build_job_csv_path(niche, job_id)
        pending_rows = []

        conn = get_db()
        conn.execute("UPDATE jobs SET csv_path=? WHERE job_id=?", (csv_path, job_id))
        conn.commit()
        job["total_areas"] = total

        def flush_pending(force_save_seen: bool = False):
            nonlocal accepted_leads, pending_rows
            if not pending_rows:
                return

            lead_payloads = []
            for _, _, data in pending_rows:
                try:
                    lead_payloads.append(json.loads(data))
                except Exception:
                    pass

            unique_payloads = reserve_unique_leads(conn, user_id, job_id, lead_payloads)
            unique_rows = [(job_id, user_id, json.dumps(lead)) for lead in unique_payloads]
            if unique_rows:
                conn.executemany(
                    "INSERT INTO leads (job_id,user_id,data) VALUES (?,?,?)",
                    unique_rows,
                )
            if unique_payloads:
                try:
                    save_leads_supabase(job_id, user_id, unique_payloads)
                except Exception as exc:
                    publish_event(job, {"type": "info", "data": f"Supabase lead sync failed: {exc}"})
            accepted_leads += len(unique_rows)
            conn.execute("UPDATE jobs SET lead_count=? WHERE job_id=?", (accepted_leads, job_id))
            conn.commit()
            pending_rows = []

            if force_save_seen or accepted_leads % 300 == 0:
                save_seen_leads(seen)

        state_lock = asyncio.Lock()
        area_queue = asyncio.Queue()
        for i, area in enumerate(areas):
            area_queue.put_nowait((i, area))

        async def worker(worker_id: int):
            nonlocal completed_indexes
            while not job.get("cancelled"):
                try:
                    i, area = area_queue.get_nowait()
                except asyncio.QueueEmpty:
                    return

                query = f"{profession} in {area}"
                async with state_lock:
                    job["current_query"] = query
                    current_done = len(completed_indexes)

                publish_event(job, {
                    "type": "progress",
                    "data": {"current": current_done, "total": total, "query": query, "maps_url": ""}
                })

                finished_naturally = False
                area_raw_found = 0
                area_saved = 0
                failure_reason = ""
                try:
                    recovery_attempt = 0
                    while not job.get("cancelled") and recovery_attempt < BLOCK_RECOVERY_MAX_ATTEMPTS:
                        blocked_detected = False
                        blocked_reason = "Google Maps temporarily blocked requests"
                        got_terminal_event = False

                        async for event in scrape_area_yields(query, profession):
                            if job.get("cancelled"):
                                break

                            evt_type = (event or {}).get("type")
                            evt_data = (event or {}).get("data")

                            if evt_type == "lead":
                                lead = evt_data or {}
                                area_raw_found += 1
                                lead["Search Query"] = query
                                maps_url = lead.get("Maps URL", "")
                                website_url = lead.get("Website", "")
                                should_publish = False
                                async with state_lock:
                                    lead_key = make_lead_key(lead)
                                    if lead_key not in seen:
                                        seen.add(lead_key)
                                        pending_rows.append((job_id, user_id, json.dumps(lead)))
                                        if len(pending_rows) >= DB_FLUSH_EVERY:
                                            flush_pending()
                                        should_publish = True

                                if should_publish:
                                    area_saved += 1
                                    publish_event(job, {"type": "lead", "data": lead})
                                    publish_event(job, {
                                        "type": "url",
                                        "data": {"maps_url": maps_url, "website_url": website_url, "name": lead.get("Name", "")}
                                    })
                            elif evt_type == "info":
                                publish_event(job, {"type": "info", "data": str(evt_data or "")})
                            elif evt_type == "count":
                                publish_event(job, {"type": "info", "data": f"Candidates discovered: {evt_data}"})
                            elif evt_type == "blocked":
                                blocked_detected = True
                                blocked_reason = str((evt_data or {}).get("reason") or blocked_reason)
                                failure_reason = blocked_reason
                                got_terminal_event = True
                                break
                            elif evt_type == "error":
                                msg = str(evt_data or "")
                                if "blocked" in msg.lower() or "captcha" in msg.lower() or "unusual traffic" in msg.lower():
                                    blocked_detected = True
                                    blocked_reason = msg
                                    failure_reason = msg
                                else:
                                    failure_reason = msg
                                    publish_event(job, {"type": "info", "data": msg})
                                got_terminal_event = True
                                break
                            elif evt_type == "done":
                                got_terminal_event = True
                                break

                            await asyncio.sleep(0)

                        if blocked_detected:
                            recovery_attempt += 1
                            publish_event(job, {
                                "type": "block_wait",
                                "data": {
                                    "query": query,
                                    "reason": blocked_reason,
                                    "retry_attempt": recovery_attempt,
                                    "retry_limit": BLOCK_RECOVERY_MAX_ATTEMPTS,
                                    "wait_seconds": BLOCK_COOLDOWN_SECONDS,
                                },
                            })
                            for _ in range(BLOCK_COOLDOWN_SECONDS):
                                if job.get("cancelled"):
                                    break
                                await asyncio.sleep(1)
                            continue

                        if got_terminal_event:
                            finished_naturally = True
                        else:
                            # Subprocess exited without an explicit terminal event.
                            # Consider this area completed to avoid endless resume loops.
                            finished_naturally = True
                        break

                    if not finished_naturally and recovery_attempt >= BLOCK_RECOVERY_MAX_ATTEMPTS:
                        failure_reason = f"blocked after {BLOCK_RECOVERY_MAX_ATTEMPTS} recovery attempts"
                        publish_event(job, {
                            "type": "error",
                            "data": (
                                f"Stopped query after {BLOCK_RECOVERY_MAX_ATTEMPTS} block-recovery attempts: {query}. "
                                "You can resume later from history."
                            ),
                        })
                finally:
                    async with state_lock:
                        raw_found += area_raw_found
                        if failure_reason:
                            source_failures.append(f"{query}: {failure_reason}")
                        if finished_naturally and not job.get("cancelled"):
                            completed_indexes.add(i)
                        processed_areas = len(completed_indexes)
                        job["processed_areas"] = processed_areas
                        flush_pending(force_save_seen=True)
                        conn.execute(
                            "UPDATE jobs SET processed_areas=?,completed_area_indexes=? WHERE job_id=?",
                            (processed_areas, json.dumps(sorted(completed_indexes)), job_id),
                        )
                        conn.commit()
                    logging.info(
                        "Scrape V1 area query=%s location=%s platform=google_maps raw_found=%s leads_saved=%s failure_reason=%s",
                        profession,
                        area,
                        area_raw_found,
                        area_saved,
                        failure_reason or "-",
                    )
                    area_queue.task_done()

        worker_count = min(AREA_CONCURRENCY, total) if total > 0 else 1
        workers = [asyncio.create_task(worker(i)) for i in range(worker_count)]
        await asyncio.gather(*workers, return_exceptions=True)

        flush_pending(force_save_seen=True)
        save_seen_leads(seen)

        final_status = resolve_final_job_status(job.get("cancelled"), raw_found, accepted_leads, source_failures)
        job["progress_message"] = final_status_message(final_status)
        logging.info(
            "Scrape V1 finished user=%s job=%s status=%s raw_found=%s saved_leads=%s processed_areas=%s/%s failure_reason=%s",
            user_id,
            job_id,
            final_status,
            raw_found,
            accepted_leads,
            job.get("processed_areas", 0),
            total,
            " | ".join(source_failures) if source_failures else "-",
        )

        generated = generate_job_csv_from_db(job_id, niche)
        # Keep a rolling combined export per niche so users can always fetch one complete file.
        generate_niche_csv_for_user(user_id, niche)
        update_job_db(job_id, final_status, accepted_leads, generated)
        try:
            update_scrape_job(job_id, status=final_status, leads_found=accepted_leads)
            usage_user = get_user_by_id(user_id)
            calculate_usage(user_id)
            if usage_user:
                await _send_scrape_ready_email(usage_user, accepted_leads, final_status)
        except Exception as exc:
            publish_event(job, {"type": "info", "data": f"Supabase job sync failed: {exc}"})
        job["status"] = final_status

        publish_event(job, {
            "type": "done",
            "data": {
                "total": accepted_leads,
                "status": final_status,
                "job_id": job_id,
                "raw_found": raw_found,
                "message": job["progress_message"],
            },
        })
    except Exception as e:
        job["status"] = "failed"
        update_job_db(job_id, "failed")
        try:
            update_scrape_job(job_id, status="failed")
        except Exception:
            pass
        publish_event(job, {"type": "error", "data": str(e)})
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        job["finished_at"] = datetime.utcnow().isoformat()


def find_matching_running_job(user_id: str, profession: str, location: str, niche: str, areas_json: str):
    conn = get_db()
    rows = conn.execute(
        """
        SELECT job_id, status
        FROM jobs
        WHERE user_id=?
          AND profession=?
          AND location=?
          AND niche=?
                    AND areas=?
          AND status IN ('running','stopping')
        ORDER BY created_at DESC
        """,
                (user_id, profession, location, niche, areas_json),
    ).fetchall()
    conn.close()

    for row in rows:
        jid = row["job_id"]
        if jid in active_jobs and active_jobs[jid].get("status") in ("running", "stopping"):
            return jid

    # Clean stale rows where DB says running but worker is not active.
    if rows:
        conn = get_db()
        for row in rows:
            jid = row["job_id"]
            if jid not in active_jobs:
                conn.execute("UPDATE jobs SET status='stopped' WHERE job_id=?", (jid,))
        conn.commit()
        conn.close()

    return None


def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


def _clean_fingerprint_text(value: object) -> str:
    text = " ".join(str(value or "").strip().lower().split())
    return text[:300]


def build_lead_fingerprint(user_id: str, lead: dict) -> str:
    source = _clean_fingerprint_text(lead.get("source") or lead.get("Source"))
    name = _clean_fingerprint_text(lead.get("Name") or lead.get("name"))
    phone = _clean_fingerprint_text(lead.get("Phone") or lead.get("phone"))
    address = _clean_fingerprint_text(lead.get("Address") or lead.get("address"))
    city = _clean_fingerprint_text(lead.get("City") or lead.get("city"))
    base = f"{_clean_fingerprint_text(user_id)}|{source}|{name}|{phone}|{address}|{city}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def reserve_unique_leads(conn: sqlite3.Connection, user_id: str, job_id: str, lead_payloads: list[dict]) -> list[dict]:
    unique_payloads: list[dict] = []
    for lead in lead_payloads:
        dedupe_key = build_lead_fingerprint(user_id, lead)
        inserted = conn.execute(
            """
            INSERT OR IGNORE INTO lead_fingerprints
            (user_id, job_id, dedupe_key, source, name, phone, address, city)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                job_id,
                dedupe_key,
                str(lead.get("source") or lead.get("Source") or ""),
                str(lead.get("Name") or lead.get("name") or ""),
                str(lead.get("Phone") or lead.get("phone") or ""),
                str(lead.get("Address") or lead.get("address") or ""),
                str(lead.get("City") or lead.get("city") or ""),
            ),
        )
        if inserted.rowcount:
            payload = dict(lead)
            payload["dedupe_key"] = dedupe_key
            unique_payloads.append(payload)
    return unique_payloads


def load_job_progress_marker(row: sqlite3.Row | None) -> dict:
    if not row:
        return {"version": 1, "items": {}, "current_item": None}
    raw = row["progress_marker"] if "progress_marker" in row.keys() else None
    if not raw:
        return {"version": 1, "items": {}, "current_item": None}
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            data.setdefault("version", 1)
            data.setdefault("items", {})
            data.setdefault("current_item", None)
            return data
    except Exception:
        pass
    return {"version": 1, "items": {}, "current_item": None}


def save_job_progress_marker(conn: sqlite3.Connection, job_id: str, marker: dict):
    conn.execute(
        "UPDATE jobs SET progress_marker=? WHERE job_id=?",
        (json.dumps(marker), job_id),
    )


def make_job_item_key(platform_name: str, query: str) -> str:
    return f"{platform_name}::{query}"


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT DEFAULT 'user',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            profession TEXT,
            location TEXT,
            niche TEXT,
            areas TEXT,
            status TEXT DEFAULT 'running',
            lead_count INTEGER DEFAULT 0,
            processed_areas INTEGER DEFAULT 0,
            completed_area_indexes TEXT,
            total_areas INTEGER DEFAULT 0,
            csv_path TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            data TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS lead_fingerprints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            job_id TEXT NOT NULL,
            dedupe_key TEXT NOT NULL,
            source TEXT,
            name TEXT,
            phone TEXT,
            address TEXT,
            city TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, dedupe_key)
        );
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            plan TEXT NOT NULL,
            amount INTEGER NOT NULL,
            provider TEXT NOT NULL,
            provider_order_id TEXT,
            provider_payment_id TEXT,
            status TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS password_reset_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            email TEXT NOT NULL,
            token TEXT NOT NULL UNIQUE,
            expires_at TEXT NOT NULL,
            used INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
    if "processed_areas" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN processed_areas INTEGER DEFAULT 0")
    if "completed_area_indexes" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN completed_area_indexes TEXT")
    if "total_areas" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN total_areas INTEGER DEFAULT 0")
    if "root_job_id" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN root_job_id TEXT")
    if "resumed_from_job_id" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN resumed_from_job_id TEXT")
    if "progress_marker" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN progress_marker TEXT")
    conn.execute("UPDATE jobs SET root_job_id=job_id WHERE root_job_id IS NULL OR root_job_id='' ")
    conn.execute("UPDATE jobs SET completed_area_indexes='[]' WHERE completed_area_indexes IS NULL OR completed_area_indexes='' ")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_lead_fingerprints_user_key ON lead_fingerprints(user_id, dedupe_key)"
    )

    # If backend restarted, in-memory workers are gone; avoid stale "running" rows.
    conn.execute(
        "UPDATE jobs SET status='stopped' WHERE status IN ('running','stopping')"
    )

    conn.commit()
    conn.close()


init_db()


class RegisterBody(BaseModel):
    name: str
    email: str
    password: str

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str):
        return _validate_short_text(value, "name", max_len=120)

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str):
        cleaned = (value or "").strip().lower()
        if not cleaned or "@" not in cleaned or len(cleaned) > 254:
            raise ValueError("email is invalid")
        return cleaned

    @field_validator("password")
    @classmethod
    def validate_password(cls, value: str):
        if len((value or "").strip()) < 8:
            raise ValueError("password must be at least 8 characters")
        return value


class LoginBody(BaseModel):
    email: str
    password: str

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str):
        cleaned = (value or "").strip().lower()
        if not cleaned or "@" not in cleaned or len(cleaned) > 254:
            raise ValueError("email is invalid")
        return cleaned


class SelectPlanBody(BaseModel):
    plan: str

    @field_validator("plan")
    @classmethod
    def validate_plan(cls, value: str):
        cleaned = (value or "").strip().lower()
        if cleaned not in ("starter", "pro", "growth", "team"):
            raise ValueError("Invalid plan")
        return cleaned


class CreateOrderBody(BaseModel):
    plan: str
    billing: str = "monthly"
    addons: list[str] = []

    @field_validator("plan")
    @classmethod
    def validate_plan(cls, value: str):
        cleaned = (value or "").strip().lower()
        if cleaned not in ("starter", "pro", "growth"):
            raise ValueError("Invalid plan")
        return cleaned

    @field_validator("billing")
    @classmethod
    def validate_billing(cls, value: str):
        cleaned = (value or "monthly").strip().lower()
        if cleaned not in ("monthly", "annual"):
            raise ValueError("Invalid billing cycle")
        return cleaned

    @field_validator("addons")
    @classmethod
    def validate_addons(cls, value: list[str]):
        cleaned = []
        for addon in value or []:
            addon_id = (addon or "").strip().lower()
            if not addon_id:
                continue
            if addon_id not in payment_utils.ADDON_PRICES:
                raise ValueError(f"Invalid add-on: {addon_id}")
            if addon_id not in cleaned:
                cleaned.append(addon_id)
        return cleaned


class VerifyPaymentBody(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str
    plan: str
    billing: str = "monthly"
    addons: list[str] = []

    @field_validator("plan")
    @classmethod
    def validate_plan(cls, value: str):
        cleaned = (value or "").strip().lower()
        if cleaned not in ("pro", "growth"):
            raise ValueError("Invalid plan")
        return cleaned

    @field_validator("billing")
    @classmethod
    def validate_billing(cls, value: str):
        cleaned = (value or "monthly").strip().lower()
        if cleaned not in ("monthly", "annual"):
            raise ValueError("Invalid billing cycle")
        return cleaned

    @field_validator("addons")
    @classmethod
    def validate_addons(cls, value: list[str]):
        cleaned = []
        for addon in value or []:
            addon_id = (addon or "").strip().lower()
            if not addon_id:
                continue
            if addon_id not in payment_utils.ADDON_PRICES:
                raise ValueError(f"Invalid add-on: {addon_id}")
            if addon_id not in cleaned:
                cleaned.append(addon_id)
        return cleaned


class UpdateUserBody(BaseModel):
    user_id: str
    plan: Optional[str] = None
    role: Optional[str] = None

    @field_validator("plan")
    @classmethod
    def validate_plan(cls, value: Optional[str]):
        if value in (None, ""):
            return None
        cleaned = value.strip().lower()
        if cleaned not in ("starter", "pro", "growth", "team"):
            raise ValueError("Invalid plan")
        return cleaned

    @field_validator("role")
    @classmethod
    def validate_role(cls, value: Optional[str]):
        if value in (None, ""):
            return None
        cleaned = value.strip().lower()
        if cleaned not in ("user", "admin"):
            raise ValueError("Invalid role")
        return cleaned


class ForgotPasswordBody(BaseModel):
    email: str

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str):
        cleaned = (value or "").strip().lower()
        if not cleaned or "@" not in cleaned or len(cleaned) > 254:
            raise ValueError("email is invalid")
        return cleaned


class ResetPasswordBody(BaseModel):
    token: str
    password: str

    @field_validator("token")
    @classmethod
    def validate_token(cls, value: str):
        cleaned = (value or "").strip()
        if len(cleaned) < 16:
            raise ValueError("invalid token")
        return cleaned

    @field_validator("password")
    @classmethod
    def validate_password(cls, value: str):
        if len((value or "").strip()) < 8:
            raise ValueError("password must be at least 8 characters")
        return value


class ScrapeBody(BaseModel):
    profession: str
    areas: list[str]
    user_id: str
    niche: str
    location: Optional[str] = ""
    root_job_id: Optional[str] = None
    resumed_from_job_id: Optional[str] = None

    @field_validator("profession")
    @classmethod
    def validate_profession(cls, value: str):
        return _validate_short_text(value, "profession", max_len=120)

    @field_validator("niche")
    @classmethod
    def validate_niche(cls, value: str):
        return _validate_short_text(value, "niche", max_len=160)

    @field_validator("location")
    @classmethod
    def validate_location(cls, value: Optional[str]):
        if value in (None, ""):
            return ""
        return _validate_short_text(value, "location", max_len=120)

    @field_validator("areas")
    @classmethod
    def validate_areas(cls, value: list[str]):
        return _validate_text_list(value, "areas", max_items=50, max_len=120)


class ResumeBody(BaseModel):
    user_id: str
    niche: Optional[str] = None
    restart_from_beginning: bool = False

    @field_validator("niche")
    @classmethod
    def validate_niche(cls, value: Optional[str]):
        if value in (None, ""):
            return value
        return _validate_short_text(value, "niche", max_len=160)


class ScrapeV2Body(BaseModel):
    user_id: str
    niche: str
    city: str
    queries: list[str]
    enable_maps: bool = True
    enable_justdial: bool = True
    enable_indiamart: bool = True
    website_filter: str = "minimal"   # no_website | minimal | all
    max_per_query: int = 25
    root_job_id: Optional[str] = None
    resumed_from_job_id: Optional[str] = None
    progress_marker: Optional[dict] = None

    @field_validator("niche")
    @classmethod
    def validate_niche(cls, value: str):
        return _validate_short_text(value, "niche", max_len=160)

    @field_validator("city")
    @classmethod
    def validate_city(cls, value: str):
        return _validate_short_text(value, "city", max_len=120)

    @field_validator("queries")
    @classmethod
    def validate_queries(cls, value: list[str]):
        return _validate_text_list(value, "queries", max_items=25, max_len=120)

    @field_validator("website_filter")
    @classmethod
    def validate_website_filter(cls, value: str):
        cleaned = (value or "").strip().lower()
        if cleaned not in ("no_website", "minimal", "all"):
            raise ValueError("invalid website filter")
        return cleaned


async def run_job_v2(job_id: str):
    job = active_jobs.get(job_id)
    if not job:
        return

    city           = job["city"]
    queries        = job["queries"]
    enable_maps    = job["enable_maps"]
    enable_justdial = job["enable_justdial"]
    enable_indiamart = job["enable_indiamart"]
    website_filter = job["website_filter"]
    max_per_query  = job["max_per_query"]
    user_id        = job["user_id"]
    niche          = job["niche"]

    conn = None
    try:
        # Import new-stack scrapers (same directory as main.py)
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).parent))
        from scraper_maps import scrape as maps_scrape
        from scraper_justdial import scrape as jd_scrape
        from scraper_indiamart import scrape as im_scrape
        from utils import (
            Lead, should_keep, deduplicate, async_enrich_websites,
            make_runtime_lead_key,
            save_csv, output_path,
        )
        from dataclasses import asdict as _asdict

        maps_worker_path = str(Path(__file__).parent / "scraper_maps.py")

        async def _run_maps_subprocess(query: str, resume_state: dict):
            cmd = [
                sys.executable,
                maps_worker_path,
                "--query",
                query,
                "--city",
                city,
                "--max-results",
                str(max_per_query),
                "--resume-state",
                json.dumps(resume_state or {}),
            ]
            logging.info("Maps subprocess start job=%s query=%s city=%s cmd=%s", job_id, query, city, cmd[:5] + ["..."])
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(Path(__file__).parent),
            )
            result_payload = None

            async def _consume_stdout():
                nonlocal result_payload
                assert proc.stdout is not None
                while True:
                    line = await proc.stdout.readline()
                    if not line:
                        break
                    text = line.decode("utf-8", errors="replace").strip()
                    if not text:
                        continue
                    if text.startswith("__MAPS_PROGRESS__"):
                        payload = json.loads(text[len("__MAPS_PROGRESS__"):])
                        _progress_callback(payload)
                        logging.info("Maps worker progress job=%s query=%s payload=%s", job_id, query, payload)
                    elif text.startswith("__MAPS_RESULT__"):
                        result_payload = json.loads(text[len("__MAPS_RESULT__"):])
                    else:
                        logging.info("Maps worker stdout job=%s query=%s line=%s", job_id, query, text)

            async def _consume_stderr():
                assert proc.stderr is not None
                while True:
                    line = await proc.stderr.readline()
                    if not line:
                        break
                    text = line.decode("utf-8", errors="replace").strip()
                    if text:
                        logging.warning("Maps worker stderr job=%s query=%s line=%s", job_id, query, text)

            await asyncio.gather(_consume_stdout(), _consume_stderr())
            exit_code = await proc.wait()
            if exit_code != 0:
                raise RuntimeError(f"Maps worker exited with code {exit_code}")
            if not result_payload:
                raise RuntimeError("Maps worker finished without result payload")
            return [Lead(**lead) for lead in result_payload.get("leads", [])]

        # ── Proxy pool ────────────────────────────────────────
        proxy_pool = None
        try:
            import config as _cfg
            from proxy_manager import ProxyPool
            proxy_pool = ProxyPool(
                protocols=_cfg.PROXY_PROTOCOLS,
                test_timeout=_cfg.PROXY_TEST_TIMEOUT,
                test_workers=_cfg.PROXY_TEST_WORKERS,
                max_failures=_cfg.PROXY_MAX_FAILURES,
                extra_proxies=_cfg.EXTRA_PROXIES,
                verbose=False,
            )
            loop = asyncio.get_event_loop()
            await asyncio.wait_for(loop.run_in_executor(None, proxy_pool.load), timeout=5)
            s = proxy_pool.stats()
            logging.info(
                "Automatic proxy pool ready job=%s live=%s fastest_ms=%s by_protocol=%s",
                job_id,
                s.get("live"),
                s.get("fastest_ms"),
                s.get("by_protocol"),
            )
        except Exception as e:
            logging.info("Automatic proxy pool unavailable for job %s: %s", job_id, e)
            proxy_pool = None

        def _get_proxy():
            candidates = []
            if proxy_pool:
                pool_proxy = proxy_pool.get()
                if pool_proxy:
                    candidates.append(pool_proxy)
            if ENV_PROXIES:
                candidates.extend(ENV_PROXIES)
            return random.choice(candidates) if candidates else None

        # ── Prep ──────────────────────────────────────────────
        platforms = sum([enable_maps, enable_justdial, enable_indiamart])
        total_steps = len(queries) * platforms
        job["total_areas"] = total_steps

        csv_path = build_job_csv_path(niche, job_id)
        conn = get_db()
        conn.execute(
            "UPDATE jobs SET csv_path=?,total_areas=? WHERE job_id=?",
            (csv_path, total_steps, job_id),
        )
        conn.commit()
        row = conn.execute(
            "SELECT progress_marker, completed_area_indexes FROM jobs WHERE job_id=?",
            (job_id,),
        ).fetchone()
        progress_marker = load_job_progress_marker(row)
        progress_items = progress_marker.setdefault("items", {})

        all_leads: list = []
        pending_rows: list = []
        accepted_leads = 0
        current_step = sum(1 for item in progress_items.values() if item.get("completed"))
        total_raw_found = 0
        source_failures: list[str] = []
        seen_runtime_keys: set[str] = set()

        def flush_pending():
            nonlocal accepted_leads, pending_rows
            if not pending_rows:
                return
            publish_event(job, {"type": "info", "data": "Saving leads..."})
            lead_payloads = []
            for _, _, data in pending_rows:
                try:
                    lead_payloads.append(json.loads(data))
                except Exception:
                    pass
            unique_payloads = reserve_unique_leads(conn, user_id, job_id, lead_payloads)
            unique_rows = [(job_id, user_id, json.dumps(lead)) for lead in unique_payloads]
            if unique_rows:
                conn.executemany(
                    "INSERT INTO leads (job_id,user_id,data) VALUES (?,?,?)",
                    unique_rows,
                )
            if lead_payloads:
                try:
                    save_leads_supabase(job_id, user_id, unique_payloads)
                except Exception as exc:
                    publish_event(job, {"type": "info", "data": f"Supabase lead sync failed: {exc}"})
            accepted_leads += len(unique_rows)
            conn.execute("UPDATE jobs SET lead_count=? WHERE job_id=?", (accepted_leads, job_id))
            conn.commit()
            pending_rows = []

        plan = (get_cached_user(user_id) or {}).get("plan", "free").lower()
        if plan == "starter": concurrency = 2
        elif plan == "pro": concurrency = 5
        elif plan == "growth": concurrency = 10
        elif plan == "team": concurrency = 999
        else: concurrency = 1
        scrape_semaphore = asyncio.Semaphore(concurrency)
        work_items = []
        for query in queries:
            if enable_maps:
                work_items.append(("Maps", query, maps_scrape))
            if enable_justdial:
                work_items.append(("JustDial", query, jd_scrape))
            if enable_indiamart:
                work_items.append(("IndiaMart", query, im_scrape))

        async def run_scrape_item(platform_name: str, query: str, scraper_fn):
            async with scrape_semaphore:
                if job.get("cancelled"):
                    return platform_name, query, [], ""
                item_key = make_job_item_key(platform_name, query)
                item_state = progress_items.setdefault(
                    item_key,
                    {
                        "platform": platform_name,
                        "query": query,
                        "completed": False,
                        "resume_state": {},
                    },
                )
                progress_marker["current_item"] = item_key
                save_job_progress_marker(conn, job_id, progress_marker)
                conn.commit()

                def _progress_callback(resume_state: dict):
                    state = progress_items.setdefault(item_key, {})
                    state.update(
                        {
                            "platform": platform_name,
                            "query": query,
                            "completed": False,
                            "resume_state": resume_state or {},
                        }
                    )
                    progress_marker["current_item"] = item_key
                    save_job_progress_marker(conn, job_id, progress_marker)
                    conn.commit()

                attempts = 3
                try:
                    logging.info(
                        "Scrape V2 item start query=%s location=%s platform=%s resume_state=%s",
                        query,
                        city,
                        platform_name,
                        item_state.get("resume_state") or {},
                    )
                    await asyncio.sleep(random.uniform(0.5, 1.2))
                    last_error = ""
                    for attempt in range(1, attempts + 1):
                        try:
                            if platform_name == "Maps":
                                logging.info(
                                    "Maps browser slot wait job=%s query=%s location=%s attempt=%s/%s concurrency=%s",
                                    job_id,
                                    query,
                                    city,
                                    attempt,
                                    attempts,
                                    MAPS_BROWSER_CONCURRENCY,
                                )
                                async with maps_browser_semaphore:
                                    logging.info(
                                        "Maps browser slot acquired job=%s query=%s location=%s attempt=%s/%s",
                                        job_id,
                                        query,
                                        city,
                                        attempt,
                                        attempts,
                                    )
                                    results = await asyncio.wait_for(
                                        _run_maps_subprocess(
                                            query,
                                            item_state.get("resume_state") or {},
                                        ),
                                        timeout=180,
                                    )
                            else:
                                results = await asyncio.wait_for(
                                    scraper_fn(
                                        query,
                                        city,
                                        max_per_query,
                                        _get_proxy(),
                                        resume_state=item_state.get("resume_state") or {},
                                        progress_callback=_progress_callback,
                                    ),
                                    timeout=120,
                                )
                            progress_items[item_key] = {
                                "platform": platform_name,
                                "query": query,
                                "completed": True,
                                "resume_state": item_state.get("resume_state") or {},
                            }
                            save_job_progress_marker(conn, job_id, progress_marker)
                            conn.commit()
                            return platform_name, query, results, ""
                        except Exception as exc:
                            last_error = str(exc)
                            logging.warning(
                                "Scrape V2 item retry query=%s location=%s platform=%s attempt=%s/%s error=%s",
                                query,
                                city,
                                platform_name,
                                attempt,
                                attempts,
                                last_error,
                            )
                            if attempt >= attempts:
                                raise
                            await asyncio.sleep(min(5, attempt * 1.5))
                except Exception as e:
                    error_text = str(e)
                    source_failures.append(f"{platform_name}:{query}:{error_text}")
                    publish_event(job, {"type": "info", "data": f"[{platform_name}] Error: {e}"})
                    return platform_name, query, [], error_text

        def to_lead_dict(lead: Lead):
            d = _asdict(lead)
            return {
                "Name":           d.get("name", ""),
                "Category":       d.get("category", ""),
                "Phone":          d.get("phone", ""),
                "Email":          d.get("email", ""),
                "Address":        d.get("address", ""),
                "City":           d.get("city", ""),
                "Website":        d.get("website", ""),
                "website_status": d.get("website_status", ""),
                "Rating":         d.get("rating", ""),
                "Reviews":        d.get("reviews", ""),
                "source":         d.get("source", ""),
                "listing_url":    d.get("listing_url", ""),
                "query":          d.get("query", ""),
                "Maps URL":       d.get("listing_url", ""),
            }

        pending_work_items = []
        for platform_name, query, scraper_fn in work_items:
            item_key = make_job_item_key(platform_name, query)
            item_state = progress_items.get(item_key) or {}
            if item_state.get("completed"):
                continue
            pending_work_items.append((platform_name, query, scraper_fn))

        for batch_start in range(0, len(pending_work_items), V2_BATCH_SIZE):
            if job.get("cancelled"):
                break

            batch = pending_work_items[batch_start: batch_start + V2_BATCH_SIZE]
            batch_started = time.perf_counter()
            for platform_name, query, _ in batch:
                label = f"{query} in {city}  [{platform_name}]"
                publish_event(job, {"type": "info", "data": "Searching sources..."})
                publish_event(job, {"type": "progress", "data": {
                    "current": current_step, "total": total_steps, "query": label,
                }})

            batch_results = await asyncio.gather(
                *[run_scrape_item(platform_name, query, scraper_fn) for platform_name, query, scraper_fn in batch]
            )

            batch_leads: list[Lead] = []
            raw_count = 0
            kept_by_item: dict[tuple[str, str], int] = {}
            for platform_name, query, results, failure_reason in batch_results:
                raw_count += len(results)
                total_raw_found += len(results)
                current_step += 1
                label = f"{query} in {city}  [{platform_name}]"
                item_key = make_job_item_key(platform_name, query)
                item_state = progress_items.setdefault(item_key, {})
                item_state["completed"] = True
                progress_marker["current_item"] = None
                save_job_progress_marker(conn, job_id, progress_marker)
                publish_event(job, {"type": "info", "data": f"[{platform_name}] {len(results)} leads for '{query}'"})
                if failure_reason:
                    publish_event(job, {"type": "info", "data": f"[{platform_name}] Source issue: {failure_reason}"})
                publish_event(job, {"type": "progress", "data": {
                    "current": current_step, "total": total_steps, "query": label,
                }})
                conn.execute(
                    "UPDATE jobs SET processed_areas=? WHERE job_id=?",
                    (current_step, job_id),
                )
                conn.commit()
                batch_leads.extend(results)

            if not batch_leads:
                continue

            publish_event(job, {
                "type": "info",
                "data": "Processing results...",
            })
            await async_enrich_websites(
                batch_leads,
                timeout=10,
                concurrency=V2_WEBSITE_CONCURRENCY,
                retries=3,
            )

            kept_this_batch = 0
            for lead in batch_leads:
                if job.get("cancelled"):
                    break
                if not should_keep(lead, website_filter):
                    continue
                lead_key = make_runtime_lead_key(lead)
                if lead_key in seen_runtime_keys:
                    continue
                seen_runtime_keys.add(lead_key)
                all_leads.append(lead)
                lead_dict = to_lead_dict(lead)
                pending_rows.append((job_id, user_id, json.dumps(lead_dict)))
                publish_event(job, {"type": "lead", "data": lead_dict})
                kept_this_batch += 1
                key = (lead_dict.get("source") or "", lead_dict.get("query") or "")
                kept_by_item[key] = kept_by_item.get(key, 0) + 1
                if len(pending_rows) >= DB_FLUSH_EVERY:
                    flush_pending()
                await asyncio.sleep(0)

            flush_pending()
            batch_seconds = time.perf_counter() - batch_started
            batch_log = (
                f"Batch complete — size={len(batch)} | "
                f"time={batch_seconds:.2f}s | leads_collected={kept_this_batch}"
            )
            logging.info(batch_log)
            platform_source_key = {
                "Maps": "google_maps",
                "JustDial": "justdial",
                "IndiaMart": "indiamart",
            }
            for platform_name, query, results, failure_reason in batch_results:
                saved_for_item = kept_by_item.get((platform_source_key.get(platform_name, ""), query), 0)
                logging.info(
                    "Scrape V2 item final query=%s location=%s platform=%s raw_found=%s leads_saved=%s failure_reason=%s",
                    query,
                    city,
                    platform_name,
                    len(results),
                    saved_for_item,
                    failure_reason or "-",
                )
            publish_event(job, {
                "type": "info",
                "data": f"{batch_log} | raw_scraped={raw_count}",
            })

        deduped = deduplicate(all_leads)
        publish_event(job, {"type": "info", "data": f"After dedup: {len(deduped)} unique leads"})

        # ── Save merged CSV ───────────────────────────────────
        try:
            save_csv(deduped, csv_path)
        except Exception:
            pass

        final_status = resolve_final_job_status(job.get("cancelled"), total_raw_found, accepted_leads, source_failures)
        job["progress_message"] = final_status_message(final_status)
        logging.info(
            "Scrape V2 finished user=%s job=%s status=%s city=%s queries=%s raw_found=%s saved_leads=%s failure_reason=%s",
            user_id,
            job_id,
            final_status,
            city,
            queries,
            total_raw_found,
            accepted_leads,
            " | ".join(source_failures) if source_failures else "-",
        )
        update_job_db(job_id, final_status, accepted_leads, csv_path)
        try:
            update_scrape_job(job_id, status=final_status, leads_found=accepted_leads)
            usage_user = get_user_by_id(user_id)
            calculate_usage(user_id)
            if usage_user:
                await _send_scrape_ready_email(usage_user, accepted_leads, final_status)
        except Exception as exc:
            publish_event(job, {"type": "info", "data": f"Supabase job sync failed: {exc}"})
        job["status"] = final_status

        publish_event(job, {
            "type": "done",
            "data": {"total": accepted_leads, "status": final_status, "job_id": job_id, "raw_found": total_raw_found, "message": job["progress_message"]},
        })

    except Exception as e:
        job["status"] = "failed"
        update_job_db(job_id, "failed")
        try:
            update_scrape_job(job_id, status="failed")
        except Exception:
            pass
        publish_event(job, {"type": "error", "data": str(e)})
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        job["finished_at"] = datetime.utcnow().isoformat()


@app.post("/scrape/v2/start")
async def start_scrape_v2(body: ScrapeV2Body, background_tasks: BackgroundTasks, current_user=Depends(get_current_user)):


    user_id = current_user["id"]
    requested_platforms = {
        "maps": body.enable_maps,
        "justdial": body.enable_justdial,
        "indiamart": body.enable_indiamart,
    }
    logging.info(
        "Scrape V2 request user=%s city=%s queries=%s platforms=%s website_filter=%s max_per_query=%s",
        user_id,
        body.city,
        body.queries,
        requested_platforms,
        body.website_filter,
        body.max_per_query,
    )
    usage = None
    try:
        usage = calculate_usage(user_id)
        logging.info("Scrape V2 Start -> PLAN: %s, LEADS USED: %s, SEARCHES TODAY: %s, LIMITS: %s", usage['plan'], usage['leads_used_this_month'], usage['searches_today'], usage['limits'])
        enforce_plan(user_id, requested_platforms)
    except PermissionError as exc:
        usage = usage or calculate_usage(user_id)
        raise HTTPException(
            status_code=403,
            detail={
                "error": str(exc),
                "plan": usage["plan"],
                "leads_used": usage["leads_used_this_month"],
                "searches_today": usage["searches_today"],
                "limits": usage["limits"],
            },
        )

    plan = (usage.get("plan") or "free").lower()
    is_admin = (current_user.get("role") or "user").lower() == "admin"
    max_jobs = 999 if is_admin else 999 if plan == "team" else 2 if plan == "starter" else 3 if plan == "pro" else 5 if plan == "growth" else 1
    history_rows = list_user_history_supabase(user_id)
    active_for_user = sum(1 for row in history_rows if (row.get("status") or "").lower() in ("pending", "queued", "running", "stopping"))
    
    if active_for_user >= max_jobs:
        raise HTTPException(
            status_code=400,
            detail={"error": f"Max active jobs ({max_jobs}) reached for plan {plan}. Upgrade to run more simultaneously."}
        )

    job_id    = str(uuid.uuid4())
    platforms = sum([body.enable_maps, body.enable_justdial, body.enable_indiamart])
    total     = len(body.queries) * platforms
    conn = get_db()
    worker_type = "maps_worker" if body.enable_maps else "api"
    initial_status = "queued" if worker_type == "maps_worker" else "pending"
    conn.execute(
        """INSERT INTO jobs
           (job_id,user_id,profession,location,niche,areas,status,
            processed_areas,completed_area_indexes,total_areas,root_job_id,resumed_from_job_id,progress_marker)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            job_id, user_id,
            ", ".join(body.queries),
            body.city,
            body.niche,
            json.dumps(body.queries),
            initial_status, 0, "[]", total, body.root_job_id or job_id,
            body.resumed_from_job_id,
            json.dumps(body.progress_marker or {"version": 1, "items": {}, "current_item": None}),
        ),
    )
    conn.commit()
    conn.close()
    create_scrape_job(
        job_id=job_id,
        user_id=user_id,
        city=body.city,
        queries=body.queries,
        platforms=requested_platforms,
        website_filter=body.website_filter,
        status=initial_status,
        worker_type=worker_type,
        max_per_query=body.max_per_query,
        niche=body.niche,
        progress_message="Queued for Maps worker" if worker_type == "maps_worker" else "Queued for worker",
        current_query="",
        total_areas=total,
        processed_areas=0,
        progress_marker=body.progress_marker or {"version": 1, "items": {}, "current_item": None},
        recent_events=[],
        root_job_id=body.root_job_id or job_id,
        resumed_from_job_id=body.resumed_from_job_id,
        cancel_requested=False,
    )

    if worker_type == "maps_worker":
        return success_response({"job_id": job_id}, message="Scrape job queued", job_id=job_id)

    active_jobs[job_id] = {
        "status":          "pending",
        "type":            "v2",
        "city":            body.city,
        "queries":         body.queries,
        "enable_maps":     body.enable_maps,
        "enable_justdial": body.enable_justdial,
        "enable_indiamart": body.enable_indiamart,
        "website_filter":  body.website_filter,
        "max_per_query":   body.max_per_query,
        "user_id":         user_id,
        "niche":           body.niche,
        "profession":      ", ".join(body.queries),   # for history display
        "areas":           body.queries,
        "location":        body.city,
        "cancelled":       False,
        "lead_count":      0,
        "processed_areas": 0,
        "total_areas":     total,
        "events":          [],
        "listeners":       set(),
        "finished_at":     None,
        "current_query":   "",
        "progress_message": "Queued for worker",
        "progress_marker": body.progress_marker or {"version": 1, "items": {}, "current_item": None},
    }
    try:
        _job_queue.put_nowait((job_id, "v2"))
        logging.info(f"System Queue Size: {_job_queue.qsize()} / 100")
    except asyncio.QueueFull:
        logging.error("Job Queue is globally full under heavy load")
        raise HTTPException(429, "System is currently at maximum capacity. Please try again later.")
    return success_response({"job_id": job_id}, message="Scrape job queued", job_id=job_id)


@app.post("/auth/register")
def register(body: RegisterBody, request: Request, background_tasks: BackgroundTasks):
    email = (body.email or "").strip().lower()
    name = (body.name or "").strip()
    if not email or "@" not in email:
        raise HTTPException(400, "Invalid email format")
    if not body.password or len(body.password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")
    if not name:
        raise HTTPException(400, "Name is required")

    rate_key = _auth_rate_limit_key(request, email)
    if not _enforce_rate_limit(auth_attempt_log, rate_key, AUTH_RATE_LIMIT_REQUESTS, AUTH_RATE_LIMIT_WINDOW_SECONDS):
        logging.warning("Auth rate limit exceeded on register for %s", email)
        raise HTTPException(429, "Too many authentication attempts")
    if get_user_by_email(email):
        logging.warning("Register failed: duplicate email %s", email)
        raise HTTPException(400, "Email already registered")
    user = create_user(
        email=email,
        full_name=name,
        hashed_password=hash_password_bcrypt(body.password),
    )
    logging.info("Register created email=%s role=%s", user.get("email"), user.get("role") or "user")
    background_tasks.add_task(_send_welcome_email, user)
    return create_auth_response(user, message="Account created successfully")


@app.post("/auth/login")
def login(body: LoginBody, request: Request):
    email = (body.email or "").strip().lower()
    if not email or "@" not in email:
        raise HTTPException(400, "Invalid email format")
    if not body.password:
        raise HTTPException(400, "Password is required")

    rate_key = _auth_rate_limit_key(request, email)
    if not _enforce_rate_limit(auth_attempt_log, rate_key, AUTH_RATE_LIMIT_REQUESTS, AUTH_RATE_LIMIT_WINDOW_SECONDS):
        logging.warning("Auth rate limit exceeded on login for %s", email)
        raise HTTPException(429, "Too many authentication attempts")
    user = get_user_by_email(email)
    logging.info(
        "Login lookup email=%s found=%s db_role=%s",
        email,
        bool(user),
        (user or {}).get("role") or "missing",
    )
    if not user or not verify_password(body.password, user.get("hashed_password", "")):
        logging.warning("Login failed for email %s", email)
        raise HTTPException(401, "Invalid email or password")
    return create_auth_response(user, message="Signed in successfully")


@app.get("/auth/me")
def auth_me(current_user=Depends(get_current_user)):
    logging.info(
        "Auth me response user_id=%s email=%s role=%s",
        current_user.get("id"),
        current_user.get("email"),
        current_user.get("role") or "user",
    )
    public_user = _public_user(current_user)
    return success_response({"user": public_user}, message="Current user loaded", user=public_user)


@app.post("/auth/refresh")
def auth_refresh(current_user=Depends(get_current_user), credentials: HTTPAuthorizationCredentials = Depends(auth_scheme)):
    payload = decode_access_token(credentials.credentials if credentials else "")
    if not should_refresh_token(payload):
        public_user = _public_user(current_user)
        return success_response({"token": None, "user": public_user}, message="Session still valid", token=None, user=public_user)
    refreshed = create_auth_response(current_user)
    return refreshed


@app.post("/auth/logout")
def logout():
    return success_response(None, message="Signed out successfully", ok=True)


@app.post("/auth/forgot-password")
async def forgot_password(body: ForgotPasswordBody, request: Request):
    rate_key = _auth_rate_limit_key(request, body.email)
    if not _enforce_rate_limit(email_request_log, rate_key, EMAIL_RATE_LIMIT_REQUESTS, EMAIL_RATE_LIMIT_WINDOW_SECONDS):
        logging.warning("Password reset email rate limit exceeded for %s", body.email)
        raise HTTPException(429, "Too many email requests")
    user = get_user_by_email(body.email)
    if user:
        token = _create_password_reset_token(user)
        reset_url = f"{PRIMARY_FRONTEND_ORIGIN}/reset-password?token={token}"
        sent = await send_email(
            user.get("email", ""),
            "Reset your LeadScout password",
            f"""
            <h2>Password reset</h2>
            <p>Use the link below to set a new password:</p>
            <p><a href="{reset_url}">{reset_url}</a></p>
            <p>This link expires in 1 hour.</p>
            """,
        )
        if not sent:
            logging.error("Password reset email failed to send for %s", user.get("email", ""))
    return success_response(None, message="If that email exists, a reset link has been sent", ok=True)


@app.post("/auth/reset-password")
def reset_password(body: ResetPasswordBody, request: Request):
    if not _enforce_rate_limit(auth_attempt_log, _auth_rate_limit_key(request, ""), AUTH_RATE_LIMIT_REQUESTS, AUTH_RATE_LIMIT_WINDOW_SECONDS):
        logging.warning("Auth rate limit exceeded on reset-password")
        raise HTTPException(429, "Too many authentication attempts")
    conn = get_db()
    row = conn.execute(
        """
        SELECT * FROM password_reset_tokens
        WHERE token=? AND used=0
        ORDER BY created_at DESC LIMIT 1
        """,
        (body.token,),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(400, "Invalid or expired reset token")
    if row["expires_at"] < datetime.utcnow().isoformat():
        conn.close()
        raise HTTPException(400, "Invalid or expired reset token")
    conn.execute("UPDATE password_reset_tokens SET used=1 WHERE id=?", (row["id"],))
    conn.commit()
    conn.close()
    update_user_fields(row["user_id"], hashed_password=hash_password_bcrypt(body.password))
    return success_response(None, message="Password reset successful", ok=True)


@app.post("/user/select-plan")
def select_plan(body: SelectPlanBody, current_user=Depends(get_current_user)):
    plan = (body.plan or "").strip().lower()
    if plan not in ("starter", "pro", "growth", "team"):
        raise HTTPException(400, "Invalid plan")
    user = update_user_plan(current_user["id"], plan)
    public_user = _public_user(user)
    return success_response({"user": public_user}, message="Plan updated successfully", user=public_user)


@app.post("/payment/create-order")
async def payment_create_order(body: CreateOrderBody, current_user=Depends(get_current_user)):
    try:
        receipt = build_razorpay_receipt(current_user["id"])
        pricing = payment_utils.compute_payment_amount(body.plan, body.billing, body.addons)
        logging.info(
            "Payment create-order requested user=%s plan=%s billing=%s addons=%s backend_total_paise=%s",
            current_user["id"],
            pricing["plan"],
            pricing["billing"],
            pricing["addons"],
            pricing["amount"],
        )
        order = await payment_utils.create_razorpay_order(body.plan, receipt=receipt, amount=pricing["amount"])
        logging.info(
            "Payment order created for user %s plan %s billing %s addons %s amount %s",
            current_user["id"],
            pricing["plan"],
            pricing["billing"],
            pricing["addons"],
            order["amount"],
        )
        order["billing"] = pricing["billing"]
        order["addons"] = pricing["addons"]
        order["base_amount"] = pricing["base_amount"]
        order["addons_amount"] = pricing["addons_amount"]
        return success_response(order, message="Payment order created", **order)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    except Exception as exc:
        logging.exception("Payment order creation failed for user %s plan %s", current_user.get("id"), body.plan)
        raise HTTPException(500, str(exc))


@app.post("/payment/verify")
def payment_verify(body: VerifyPaymentBody, background_tasks: BackgroundTasks, current_user=Depends(get_current_user)):
    if not payment_utils.verify_razorpay_signature(
        body.razorpay_order_id,
        body.razorpay_payment_id,
        body.razorpay_signature,
    ):
        logging.warning("Payment signature verification failed for user %s", current_user["id"])
        raise HTTPException(400, "Invalid payment signature")

    pricing = payment_utils.compute_payment_amount(body.plan, body.billing, body.addons)
    user = update_user_plan(current_user["id"], body.plan)
    _store_payment_record(
        current_user["id"],
        body.plan,
        pricing["amount"],
        body.razorpay_order_id,
        body.razorpay_payment_id,
        "paid",
    )
    logging.info(
        "Payment verified for user %s plan %s billing %s addons %s total_paise %s order %s payment %s",
        current_user["id"],
        body.plan,
        pricing["billing"],
        pricing["addons"],
        pricing["amount"],
        body.razorpay_order_id,
        body.razorpay_payment_id,
    )
    background_tasks.add_task(_send_payment_email, user, body.plan, pricing["amount"])
    public_user = _public_user(user)
    return success_response({"user": public_user}, message="Payment verified successfully", ok=True, user=public_user)


@app.post("/scrape/start")
async def start_scrape(body: ScrapeBody, current_user=Depends(get_current_user)):


    user_id = current_user["id"]
    usage = None
    try:
        usage = calculate_usage(user_id)
        logging.info("Scrape V1 Start -> PLAN: %s, LEADS USED: %s, SEARCHES TODAY: %s, LIMITS: %s", usage['plan'], usage['leads_used_this_month'], usage['searches_today'], usage['limits'])
        enforce_plan(user_id, {"maps": True, "justdial": False, "indiamart": False})
    except PermissionError as exc:
        usage = usage or calculate_usage(user_id)
        raise HTTPException(
            status_code=403,
            detail={
                "error": str(exc),
                "plan": usage["plan"],
                "leads_used": usage["leads_used_this_month"],
                "searches_today": usage["searches_today"],
                "limits": usage["limits"],
            },
        )

    plan = (usage.get("plan") or "free").lower()
    is_admin = (current_user.get("role") or "user").lower() == "admin"
    max_jobs = 999 if is_admin else 999 if plan == "team" else 2 if plan == "starter" else 3 if plan == "pro" else 5 if plan == "growth" else 1
    active_for_user = sum(1 for j in active_jobs.values() if j.get("user_id") == user_id and j.get("status") in ("pending", "running", "stopping"))
    
    if active_for_user >= max_jobs:
        raise HTTPException(
            status_code=400,
            detail={"error": f"Max active jobs ({max_jobs}) reached for plan {plan}. Upgrade to run more simultaneously."}
        )

    job_id   = str(uuid.uuid4())
    location = body.location or (body.areas[0] if body.areas else "unknown")
    root_job_id = body.root_job_id or job_id
    conn = get_db()
    conn.execute(
        "INSERT INTO jobs (job_id,user_id,profession,location,niche,areas,status,processed_areas,completed_area_indexes,total_areas,root_job_id,resumed_from_job_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            job_id,
            user_id,
            body.profession,
            location,
            body.niche,
            json.dumps(body.areas),
            "pending",
            0,
            "[]",
            len(body.areas),
            root_job_id,
            body.resumed_from_job_id,
        )
    )
    conn.commit()
    conn.close()
    create_scrape_job(
        job_id=job_id,
        user_id=user_id,
        city=location,
        queries=body.areas,
        platforms={"maps": True, "justdial": False, "indiamart": False},
        website_filter="all",
        status="pending",
    )
    active_jobs[job_id] = {
        "status": "pending",
        "type": "v1",
        "profession": body.profession,
        "areas": body.areas,
        "user_id": user_id,
        "niche": body.niche,
        "cancelled": False,
        "lead_count": 0,
        "processed_areas": 0,
        "total_areas": len(body.areas),
        "events": [],
        "listeners": set(),
        "finished_at": None,
        "current_query": "",
        "location": location,
    }
    try:
        _job_queue.put_nowait((job_id, "v1"))
        logging.info(f"System Queue Size: {_job_queue.qsize()} / 100")
    except asyncio.QueueFull:
        logging.error("Job Queue is globally full under heavy load")
        raise HTTPException(429, "System is currently at maximum capacity. Please try again later.")
    return success_response({"job_id": job_id}, message="Scrape job queued", job_id=job_id)


@app.post("/scrape/stop/{job_id}")
def stop_scrape(job_id: str, current_user=Depends(get_current_user)):
    if job_id in active_jobs and active_jobs[job_id].get("user_id") == current_user["id"]:
        current_status = active_jobs[job_id].get("status")
        if current_status in ("pending", "running"):
            active_jobs[job_id]["cancelled"] = True
            active_jobs[job_id]["status"] = "stopping"
            try:
                update_scrape_job(job_id, status="stopping")
            except Exception:
                pass
        else:
            return success_response(
                {"job_id": job_id, "already_finished": True, "status": current_status},
                message="Scrape job already finished",
                ok=True,
                job_id=job_id,
                already_finished=True,
                status=current_status,
            )
    else:
        remote_job = get_scrape_job(job_id)
        if remote_job and remote_job.get("worker_type") == "maps_worker" and remote_job.get("user_id") == current_user["id"]:
            remote_status = (remote_job.get("status") or "").lower()
            if remote_status in ("queued", "running"):
                update_scrape_job(
                    job_id,
                    status="stopping",
                    cancel_requested=True,
                    progress_message="Stopping scrape...",
                )
            else:
                return success_response(
                    {"job_id": job_id, "already_finished": True, "status": remote_status},
                    message="Scrape job already finished",
                    ok=True,
                    job_id=job_id,
                    already_finished=True,
                    status=remote_status,
                )
    return success_response({"job_id": job_id}, message="Scrape stop requested", ok=True, job_id=job_id)


@app.delete("/scrape/job/{job_id}")
def delete_job(job_id: str, current_user=Depends(get_current_user), x_user_id: str = Header(None)):
    """Delete a scrape job and its associated leads."""
    effective_user_id = current_user["id"]
    conn = get_db()
    row = conn.execute("SELECT user_id,niche FROM jobs WHERE job_id=?", (job_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Job not found")
    if row["user_id"] != effective_user_id:
        conn.close()
        raise HTTPException(403, "You can only delete your own jobs")
    if x_user_id and row["user_id"] != x_user_id:
        conn.close()
        raise HTTPException(403, "You can only delete your own jobs")

    # Stop if running
    if job_id in active_jobs:
        active_jobs[job_id]["cancelled"] = True
        active_jobs[job_id]["status"] = "stopped"

    conn.execute("DELETE FROM leads WHERE job_id=?", (job_id,))
    conn.execute("DELETE FROM jobs WHERE job_id=?", (job_id,))
    conn.commit()
    conn.close()

    csv_path = build_job_csv_path(row["niche"] or "leads", job_id)
    try:
        if Path(csv_path).exists():
            Path(csv_path).unlink()
    except Exception:
        pass

    active_jobs.pop(job_id, None)
    return success_response({"deleted_job_id": job_id}, message="Scrape job deleted", ok=True, deleted_job_id=job_id)


@app.get("/scrape/heartbeat/{job_id}")
def heartbeat(job_id: str, current_user=Depends(get_current_user)):
    """Lightweight heartbeat for sleep/wake detection."""
    if job_id in active_jobs:
        job = active_jobs[job_id]
        if job.get("user_id") != current_user["id"]:
            raise HTTPException(403, "Access denied")
        return success_response({
            "alive": True,
            "status": job.get("status", "unknown"),
            "lead_count": int(job.get("lead_count", 0)),
            "processed_areas": int(job.get("processed_areas", 0)),
        }, message="Heartbeat loaded", alive=True, status=job.get("status", "unknown"))
    remote_job = get_scrape_job(job_id)
    if remote_job and remote_job.get("worker_type") == "maps_worker" and remote_job.get("user_id") == current_user["id"]:
        status = (remote_job.get("status") or "queued").lower()
        return success_response(
            {
                "alive": status in ("queued", "running", "stopping"),
                "status": status,
                "lead_count": int(remote_job.get("leads_found") or 0),
                "processed_areas": int(remote_job.get("processed_areas") or 0),
            },
            message="Heartbeat loaded",
            alive=status in ("queued", "running", "stopping"),
            status=status,
        )
    return success_response({"alive": False, "status": "not_running"}, message="No active job", alive=False, status="not_running")


@app.post("/scrape/resume/{job_id}")
async def resume_scrape(job_id: str, body: ResumeBody, current_user=Depends(get_current_user)):
    conn = get_db()
    row = conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Job not found")

    if row["user_id"] != current_user["id"]:
        raise HTTPException(403, "You can only resume your own jobs")

    try:
        original_areas = json.loads(row["areas"] or "[]")
    except Exception:
        original_areas = []
    progress_marker = load_job_progress_marker(row)

    processed = int(row["processed_areas"] or 0)
    completed_indexes = set()
    try:
        completed_indexes = set(int(i) for i in json.loads(row["completed_area_indexes"] or "[]"))
    except Exception:
        completed_indexes = set()

    if body.restart_from_beginning:
        remaining = original_areas
    else:
        # Resume replays ALL areas not fully completed — including the one that was mid-scrape.
        remaining = [area for idx, area in enumerate(original_areas) if idx not in completed_indexes]

    if not remaining:
        raise HTTPException(400, "No remaining areas to resume")

    target_niche = body.niche or row["niche"] or "resumed_scrape"
    target_location = remaining[0] if remaining else (row["location"] or "")
    target_areas_json = json.dumps(remaining)
    existing_job_id = find_matching_running_job(
        current_user["id"],
        row["profession"] or "",
        target_location,
        target_niche,
        target_areas_json,
    )
    if existing_job_id:
        return success_response({"job_id": existing_job_id, "reused": True}, message="Existing running job reused", job_id=existing_job_id, reused=True)

    history_rows = list_user_history_supabase(current_user["id"])
    history_row = next((item for item in history_rows if item.get("job_id") == row["job_id"]), None)
    platforms = (history_row or {}).get("platforms") or {}
    is_v2_job = bool(platforms) or (bool(original_areas) and (row["location"] or "") not in original_areas)

    if is_v2_job:
        remote_job = get_scrape_job(job_id) or {}
        remote_queries = _normalize_supabase_queries(remote_job.get("queries")) or original_areas
        remote_progress_marker = _normalize_supabase_progress_marker(remote_job.get("progress_marker")) if remote_job else progress_marker
        resumed_progress_marker = {"version": 1, "items": {}, "current_item": None} if body.restart_from_beginning else remote_progress_marker
        return await start_scrape_v2(
            ScrapeV2Body(
                user_id=current_user["id"],
                niche=target_niche,
                city=(remote_job.get("city") or row["location"] or target_location),
                queries=remote_queries or remaining,
                enable_maps=bool(platforms.get("maps", True)),
                enable_justdial=bool(platforms.get("justdial", False)),
                enable_indiamart=bool(platforms.get("indiamart", False)),
                website_filter=(remote_job.get("website_filter") or (history_row or {}).get("website_filter") or "minimal"),
                max_per_query=int(remote_job.get("max_per_query") or 25),
                root_job_id=remote_job.get("root_job_id") or row["root_job_id"] or row["job_id"],
                resumed_from_job_id=row["job_id"],
                progress_marker=resumed_progress_marker,
            ),
            BackgroundTasks(),
            current_user=current_user,
        )

    return await start_scrape(
        ScrapeBody(
            profession=row["profession"] or "",
            areas=remaining,
            user_id=current_user["id"],
            niche=target_niche,
            location=target_location,
            root_job_id=row["root_job_id"] or row["job_id"],
            resumed_from_job_id=row["job_id"],
        ),
        current_user=current_user,
    )


@app.get("/scrape/status/{job_id}")
def scrape_status(job_id: str, current_user=Depends(get_current_user)):
    if job_id in active_jobs:
        job = active_jobs[job_id]
        if job.get("user_id") != current_user["id"]:
            raise HTTPException(403, "Access denied")
        recent_events = list(job.get("events", []))[-20:]
        payload = {
            "job_id": job_id,
            "status": job.get("status", "running"),
            "lead_count": int(job.get("lead_count", 0)),
            "processed_areas": int(job.get("processed_areas", 0)),
            "total_areas": int(job.get("total_areas", 0)),
            "current_query": job.get("current_query", ""),
            "progress_message": job.get("progress_message", ""),
            "recent_events": recent_events,
            "running": job.get("status") in ("running", "stopping"),
            "profession": job.get("profession", ""),
            "location": job.get("location", ""),
        }
        return success_response(payload, message="Scrape status loaded", **payload)

    remote_job = get_scrape_job(job_id)
    if remote_job and remote_job.get("worker_type") == "maps_worker":
        if remote_job.get("user_id") != current_user["id"]:
            raise HTTPException(403, "Access denied")
        payload = _build_supabase_job_status_payload(remote_job)
        return success_response(payload, message="Scrape status loaded", **payload)

    conn = get_db()
    row = conn.execute(
        "SELECT job_id,user_id,status,lead_count,csv_path,processed_areas,total_areas,profession,location FROM jobs WHERE job_id=?",
        (job_id,),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Job not found")
    if row["user_id"] != current_user["id"]:
        raise HTTPException(403, "Access denied")

    status = row["status"]
    if status in ("running", "stopping"):
        # DB row says running, but no active in-memory worker exists.
        status = "stopped"
        conn = get_db()
        conn.execute("UPDATE jobs SET status='stopped' WHERE job_id=?", (job_id,))
        conn.commit()
        conn.close()

    payload = {
        "job_id": row["job_id"],
        "status": status,
        "lead_count": row["lead_count"] or 0,
        "processed_areas": row["processed_areas"] or 0,
        "total_areas": row["total_areas"] or 0,
        "current_query": "",
        "progress_message": final_status_message(status) if status in ("completed", "stopped", "failed", "no_results", "low_data", "source_error") else "Waiting for worker",
        "recent_events": [],
        "running": False,
        "csv_path": row["csv_path"],
        "profession": row["profession"] or "",
        "location": row["location"] or "",
    }
    return success_response(payload, message="Scrape status loaded", **payload)


@app.get("/scrape/download/{job_id}")
def download_csv(job_id: str, current_user=Depends(get_current_user)):
    if not _can_download_csv_for_user(current_user):
        raise HTTPException(403, "CSV export is available on Golden, Team, or Admin accounts only")
    conn = get_db()
    job  = conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
    conn.close()
    if not job:
        raise HTTPException(404, "Job not found")
    if job["user_id"] != current_user["id"] and (current_user.get("role") or "user").lower() != "admin":
        raise HTTPException(403, "Access denied")

    csv_path = job["csv_path"]
    if not csv_path or not Path(csv_path).exists():
        generated = generate_job_csv_from_db(job_id, job["niche"] or "leads")
        if generated:
            csv_path = generated
            conn = get_db()
            conn.execute("UPDATE jobs SET csv_path=? WHERE job_id=?", (csv_path, job_id))
            conn.commit()
            conn.close()
        else:
            raise HTTPException(404, "CSV not ready yet")

    return FileResponse(csv_path, media_type="text/csv", filename=Path(csv_path).name)


@app.get("/scrape/download/all/{user_id}")
def download_all_csv(user_id: str, current_user=Depends(get_current_user)):
    if not _can_download_csv_for_user(current_user):
        raise HTTPException(403, "CSV export is available on Golden, Team, or Admin accounts only")
    is_admin = (current_user.get("role") or "user").lower() == "admin"
    if user_id != current_user["id"] and not is_admin:
        raise HTTPException(403, "Access denied")
    conn = get_db()
    job_rows = conn.execute(
        "SELECT job_id FROM jobs WHERE user_id=? ORDER BY created_at ASC",
        (user_id,),
    ).fetchall()
    conn.close()

    job_ids = [r["job_id"] for r in job_rows if r and r["job_id"]]
    records = _collect_records_by_job_ids(job_ids, user_id=user_id)

    if not records:
        raise HTTPException(404, "No scraped leads found for this user")

    export_path = OUTPUT_DIR / f"all_leads_{_safe_slug(user_id)}.csv"
    written = _write_records_to_csv(records, export_path)
    if not written:
        raise HTTPException(404, "No scraped leads found for this user")

    return FileResponse(str(export_path), media_type="text/csv", filename=export_path.name)


@app.get("/scrape/download/niche/{user_id}/{niche}")
def download_niche_csv(user_id: str, niche: str, current_user=Depends(get_current_user)):
    if not _can_download_csv_for_user(current_user):
        raise HTTPException(403, "CSV export is available on Golden, Team, or Admin accounts only")
    is_admin = (current_user.get("role") or "user").lower() == "admin"
    if user_id != current_user["id"] and not is_admin:
        raise HTTPException(403, "Access denied")
    path = generate_niche_csv_for_user(user_id, niche)
    if not path:
        raise HTTPException(404, "No scraped leads found for this niche")
    return FileResponse(path, media_type="text/csv", filename=Path(path).name)


@app.get("/scrape/download/merged/{user_id}")
def download_merged_job_ids_csv(user_id: str, job_ids: str, current_user=Depends(get_current_user)):
    if not _can_download_csv_for_user(current_user):
        raise HTTPException(403, "CSV export is available on Golden, Team, or Admin accounts only")
    is_admin = (current_user.get("role") or "user").lower() == "admin"
    if user_id != current_user["id"] and not is_admin:
        raise HTTPException(403, "Access denied")
    parsed_ids = [j.strip() for j in (job_ids or "").split(",") if j.strip()]
    if not parsed_ids:
        raise HTTPException(400, "No job_ids provided")

    conn = get_db()
    rows = conn.execute(
        f"SELECT job_id FROM jobs WHERE user_id=? AND job_id IN ({','.join('?' for _ in parsed_ids)})",
        tuple([user_id] + parsed_ids),
    ).fetchall()
    conn.close()

    allowed_ids = [r["job_id"] for r in rows if r and r["job_id"]]
    if not allowed_ids:
        raise HTTPException(404, "No matching jobs found")

    records = _collect_records_by_job_ids(allowed_ids, user_id=user_id)
    if not records:
        raise HTTPException(404, "No scraped leads found for selected jobs")

    merged_slug = _safe_slug("_".join(allowed_ids[:3]))
    export_path = OUTPUT_DIR / f"merged_{_safe_slug(user_id)}_{merged_slug}.csv"
    written = _write_records_to_csv(records, export_path)
    if not written:
        raise HTTPException(404, "No scraped leads found for selected jobs")

    return FileResponse(written, media_type="text/csv", filename=Path(written).name)


_ws_connect_timers = {}

@app.websocket("/ws/{job_id}")
async def ws_scrape(ws: WebSocket, job_id: str):
    now = time.time()
    last_conn = _ws_connect_timers.get(job_id, 0)
    if now - last_conn < 2.0:
        await asyncio.sleep(2.0 - (now - last_conn))
    _ws_connect_timers[job_id] = time.time()

    await ws.accept()
    logging.info(f"[WS] Accepted connection for job {job_id[:8]}")

    if job_id not in active_jobs:
        logging.warning(f"[WS] Job {job_id[:8]} not in active_jobs — sending error and closing")
        await ws.send_json({"type": "error", "data": "Job not found"})
        await ws.close()
        return

    job = active_jobs[job_id]
    queue = asyncio.Queue(maxsize=WS_QUEUE_MAXSIZE)
    job.setdefault("listeners", set()).add(queue)

    async def send_loop():
        try:
            # CRITICAL: Copy the list so concurrent appends/trims by publish_event
            # during our async iteration (at each await) don't corrupt the iterator.
            cached_events = list(job.get("events", []))
            logging.info(f"[WS] Replaying {len(cached_events)} cached events for {job_id[:8]}")
            for _evt in cached_events:
                try:
                    await ws.send_json(_evt)
                    await asyncio.sleep(0.002)
                except Exception as e:
                    logging.error(f"[WS] Failed to send cached event: {e}")

            logging.info(f"[WS] Entering live stream for {job_id[:8]}")
            while True:
                _evt = await queue.get()
                try:
                    await ws.send_json(_evt)
                except Exception as e:
                    logging.error(f"[WS] Failed to send real-time event: {e}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.error(f"[WS] Send loop fatal error for {job_id[:8]}: {e}", exc_info=True)

    async def receive_loop():
        try:
            while True:
                await ws.receive_text()
        except WebSocketDisconnect:
            logging.info(f"[WS] Client disconnected for {job_id[:8]}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.error(f"[WS] Receive loop error for {job_id[:8]}: {e}", exc_info=True)

    send_task = asyncio.create_task(send_loop())
    receive_task = asyncio.create_task(receive_loop())

    done, pending = await asyncio.wait(
        [send_task, receive_task], 
        return_when=asyncio.FIRST_COMPLETED
    )

    # Log which task completed first (helps diagnose who closed the connection)
    for task in done:
        if task == send_task:
            logging.info(f"[WS] Send loop ended first for {job_id[:8]}")
        else:
            logging.info(f"[WS] Receive loop ended first for {job_id[:8]}")

    for task in pending:
        task.cancel()
        
    job.get("listeners", set()).discard(queue)
    try:
        await ws.close()
    except Exception:
        pass
    logging.info(f"[WS] Connection fully closed for {job_id[:8]}")


@app.get("/user/history")
def user_history(current_user=Depends(get_current_user)):
    history = _get_cached_history(current_user["id"])
    return success_response({"history": history}, message="User history loaded", history=history)


@app.get("/user/history/{user_id}")
def user_history_compat(user_id: str, current_user=Depends(get_current_user)):
    if user_id != current_user["id"]:
        raise HTTPException(403, "Access denied")
    history = list_user_history_supabase(current_user["id"])
    return success_response({"history": history}, message="User history loaded", history=history)


@app.get("/user/leads")
def user_leads(
    search: str = Query(default=""),
    source: str = Query(default=""),
    website_status: str = Query(default=""),
    current_user=Depends(get_current_user),
):
    rows = list_user_leads(
        current_user["id"],
        search=search or None,
        source=source or None,
        website_status=website_status or None,
    )
    return success_response({"leads": rows}, message="User leads loaded", leads=rows)


@app.get("/user/usage")
def user_usage(current_user=Depends(get_current_user)):
    usage = _get_cached_usage(current_user["id"])
    return success_response(usage, message="Usage loaded", **usage)


@app.get("/scrape/job/{job_id}/leads")
def get_job_leads(job_id: str, current_user=Depends(get_current_user), x_user_id: str = Header(None)):
    """Fetch all leads for a given job to display in the UI Data Explorer."""
    conn = get_db()
    row = conn.execute("SELECT user_id FROM jobs WHERE job_id=?", (job_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Job not found")
        
    is_admin = (current_user.get("role") or "user").lower() == "admin"
    if row["user_id"] != current_user["id"] and not is_admin:
        conn.close()
        raise HTTPException(403, "Access denied")
    if x_user_id and row["user_id"] != x_user_id and not is_admin:
        conn.close()
        raise HTTPException(403, "Access denied")

    # If this is a merged job representation, we might need multiple IDs. For now, fetch just this job's leads.
    leads_rows = conn.execute("SELECT data FROM leads WHERE job_id=?", (job_id,)).fetchall()
    conn.close()

    leads = []
    for r in leads_rows:
        try:
            leads.append(json.loads(r["data"]))
        except Exception:
            pass
            
    return success_response({"leads": leads}, message="Job leads loaded", leads=leads)


@app.get("/admin/users")
def admin_users(current_user=Depends(require_admin_user)):
    stats_by_user = _admin_user_stats()
    result = []
    for user in list_all_users():
        row = {
            "id": user.get("id"),
            "name": user.get("full_name") or "",
            "email": user.get("email") or "",
            "plan": user.get("plan"),
            "role": user.get("role") or "user",
            "created_at": user.get("created_at"),
            "job_count": stats_by_user.get(user.get("id"), {}).get("job_count", 0),
            "total_leads": stats_by_user.get(user.get("id"), {}).get("total_leads", 0),
        }
        result.append(row)
    return success_response({"users": result}, message="Admin users loaded", users=result)


@app.put("/admin/update-user")
def admin_update_user(body: UpdateUserBody, current_user=Depends(require_admin_user)):
    user = update_user_fields(body.user_id, plan=body.plan, role=body.role)
    logging.info(
        "Admin action by %s updated user %s plan=%s role=%s",
        current_user["id"],
        body.user_id,
        body.plan,
        body.role,
    )
    public_user = _public_user(user)
    return success_response({"user": public_user}, message="User updated successfully", user=public_user)


@app.get("/admin/jobs")
def admin_jobs(current_user=Depends(require_admin_user)):
    conn = get_db()
    jobs = conn.execute("""
        SELECT
            j.*,
            u.name AS user_name,
            COALESCE(l.persisted_leads, 0) AS persisted_leads,
            CASE
                WHEN COALESCE(l.persisted_leads, 0) > COALESCE(j.lead_count, 0)
                THEN COALESCE(l.persisted_leads, 0)
                ELSE COALESCE(j.lead_count, 0)
            END AS effective_lead_count
        FROM jobs j
        LEFT JOIN users u ON j.user_id=u.id
        LEFT JOIN (
            SELECT job_id, COUNT(*) AS persisted_leads
            FROM leads
            GROUP BY job_id
        ) l ON l.job_id = j.job_id
        ORDER BY j.created_at DESC LIMIT 200
    """).fetchall()
    conn.close()
    rows = [dict(j) for j in jobs]
    return success_response({"jobs": rows}, message="Admin jobs loaded", jobs=rows)


@app.get("/admin/stats")
def admin_stats(current_user=Depends(require_admin_user)):
    conn        = get_db()
    total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    total_jobs  = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    total_leads = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    top_pro     = conn.execute("SELECT profession,COUNT(*) as c FROM jobs GROUP BY profession ORDER BY c DESC LIMIT 1").fetchone()
    top_loc     = conn.execute("SELECT location,COUNT(*) as c FROM jobs GROUP BY location ORDER BY c DESC LIMIT 1").fetchone()
    avg_leads   = conn.execute("SELECT AVG(c) FROM (SELECT COUNT(*) AS c FROM leads GROUP BY job_id)").fetchone()[0]
    csv_files   = len(list(OUTPUT_DIR.glob("*.csv")))
    conn.close()
    payload = {
        "total_users":    total_users,
        "total_jobs":     total_jobs,
        "total_leads":    int(total_leads),
        "active_jobs":    sum(1 for j in active_jobs.values() if j.get("status") in ("running", "stopping")),
        "total_files":    csv_files,
        "top_profession": top_pro[0] if top_pro else None,
        "top_location":   top_loc[0] if top_loc else None,
        "avg_leads":      round(avg_leads) if avg_leads else 0,
    }
    return success_response(payload, message="Admin stats loaded", **payload)

@app.get("/health")
def health():
    return success_response({"status": "ok", "version": "1.0.0"}, message="Health check passed", status="ok", version="1.0.0")
