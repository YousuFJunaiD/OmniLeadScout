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
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, field_validator
import logging

from auth_utils import create_access_token, decode_access_token, hash_password_bcrypt, should_refresh_token, verify_password
from email_utils import send_email
from env_utils import require_env, validate_required_env
from payment_utils import PLAN_AMOUNTS, create_razorpay_order, verify_razorpay_signature
from supabase_db import (
    PLAN_LIMITS,
    calculate_usage,
    create_scrape_job,
    create_user,
    enforce_plan,
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
MAX_REQUEST_BYTES = int(os.getenv("MAX_REQUEST_BYTES", "1048576"))

app = FastAPI(title="LeadScout API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "x-user-id"],
)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logging.exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})

DB_PATH    = "leadscout.db"
OUTPUT_DIR = Path("../output")
OUTPUT_DIR.mkdir(exist_ok=True)
active_jobs = {}
auth_scheme = HTTPBearer(auto_error=False)

EVENT_BUFFER_LIMIT = 800
WS_QUEUE_MAXSIZE = 120


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
RATE_LIMIT_REQUESTS = _env_int("LEADSCOUT_RATE_LIMIT_REQUESTS", 10, 1, 200)
RATE_LIMIT_WINDOW_SECONDS = _env_int("LEADSCOUT_RATE_LIMIT_WINDOW_SECONDS", 60, 10, 3600)
REQUEST_TIMEOUT_SECONDS = _env_int("LEADSCOUT_REQUEST_TIMEOUT_SECONDS", 8, 5, 10)
REQUEST_DELAY_MIN_MS = _env_int("LEADSCOUT_REQUEST_DELAY_MIN_MS", 500, 100, 5000)
REQUEST_DELAY_MAX_MS = _env_int("LEADSCOUT_REQUEST_DELAY_MAX_MS", 2000, 200, 8000)
AUTH_RATE_LIMIT_REQUESTS = _env_int("LEADSCOUT_AUTH_RATE_LIMIT_REQUESTS", 5, 1, 50)
AUTH_RATE_LIMIT_WINDOW_SECONDS = _env_int("LEADSCOUT_AUTH_RATE_LIMIT_WINDOW_SECONDS", 300, 10, 3600)
EMAIL_RATE_LIMIT_REQUESTS = _env_int("LEADSCOUT_EMAIL_RATE_LIMIT_REQUESTS", 3, 1, 20)
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
    plan = user.get("plan")
    return {
        "id": user.get("id"),
        "name": user.get("full_name") or user.get("name") or "",
        "email": user.get("email") or "",
        "plan": plan.lower() if isinstance(plan, str) and plan else None,
        "role": user.get("role") or "user",
    }


def create_auth_response(user: dict) -> dict:
    public_user = _public_user(user)
    token = create_access_token(
        {
            "sub": public_user["id"],
            "email": public_user["email"],
            "plan": public_user["plan"],
            "role": public_user["role"],
        }
    )
    return {"token": token, "user": public_user}


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
        user = get_user_by_id(user_id)
    except Exception as exc:
        logging.error("Auth backend unavailable on %s: %s", request.url.path, exc)
        raise HTTPException(500, "Authentication backend unavailable")
    if not user:
        logging.warning("Auth failed: unknown user %s for %s", user_id, request.url.path)
        raise HTTPException(401, "User not found")
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


def _send_welcome_email(user: dict):
    send_email(
        user.get("email", ""),
        "Welcome to LeadScout",
        f"""
        <h2>Welcome to LeadScout</h2>
        <p>Hi {user.get("full_name") or user.get("name") or "there"},</p>
        <p>Your account is ready. Choose a plan and start scraping leads.</p>
        """,
    )


def _send_payment_email(user: dict, plan: str, amount: int):
    send_email(
        user.get("email", ""),
        "LeadScout payment confirmed",
        f"""
        <h2>Payment successful</h2>
        <p>Your <strong>{plan.title()}</strong> plan is now active.</p>
        <p>Amount received: INR {amount / 100:.2f}</p>
        """,
    )


def _send_scrape_ready_email(user: dict, lead_count: int, status: str):
    if status != "completed":
        return
    send_email(
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

    fieldnames = []
    seen_fields = set()
    for rec in records:
        for key in rec.keys():
            if key not in seen_fields:
                seen_fields.add(key)
                fieldnames.append(key)

    if not fieldnames:
        return None

    csv_path = build_job_csv_path(niche, job_id)
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

    return csv_path


def _write_records_to_csv(records: list[dict], export_path: Path) -> Optional[str]:
    if not records:
        return None

    fieldnames = []
    seen_fields = set()
    for rec in records:
        for key in rec.keys():
            if key not in seen_fields:
                seen_fields.add(key)
                fieldnames.append(key)

    if not fieldnames:
        return None

    with open(export_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

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

    dead_listeners = []
    for q in list(job.get("listeners", set())):
        try:
            q.put_nowait(message)
        except Exception:
            dead_listeners.append(q)

    for q in dead_listeners:
        job.get("listeners", set()).discard(q)


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

            conn.executemany(
                "INSERT INTO leads (job_id,user_id,data) VALUES (?,?,?)",
                pending_rows,
            )
            if lead_payloads:
                try:
                    save_leads_supabase(job_id, user_id, lead_payloads)
                except Exception as exc:
                    publish_event(job, {"type": "info", "data": f"Supabase lead sync failed: {exc}"})
            accepted_leads += len(pending_rows)
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
                                got_terminal_event = True
                                break
                            elif evt_type == "error":
                                msg = str(evt_data or "")
                                if "blocked" in msg.lower() or "captcha" in msg.lower() or "unusual traffic" in msg.lower():
                                    blocked_detected = True
                                    blocked_reason = msg
                                else:
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
                        publish_event(job, {
                            "type": "error",
                            "data": (
                                f"Stopped query after {BLOCK_RECOVERY_MAX_ATTEMPTS} block-recovery attempts: {query}. "
                                "You can resume later from history."
                            ),
                        })
                finally:
                    async with state_lock:
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
                    area_queue.task_done()

        worker_count = min(AREA_CONCURRENCY, total) if total > 0 else 1
        workers = [asyncio.create_task(worker(i)) for i in range(worker_count)]
        await asyncio.gather(*workers, return_exceptions=True)

        flush_pending(force_save_seen=True)
        save_seen_leads(seen)

        final_status = "stopped" if job.get("cancelled") else "completed"

        generated = generate_job_csv_from_db(job_id, niche)
        # Keep a rolling combined export per niche so users can always fetch one complete file.
        generate_niche_csv_for_user(user_id, niche)
        update_job_db(job_id, final_status, accepted_leads, generated)
        try:
            update_scrape_job(job_id, status=final_status, leads_found=accepted_leads)
            usage_user = get_user_by_id(user_id)
            calculate_usage(user_id)
            if usage_user:
                _send_scrape_ready_email(usage_user, accepted_leads, final_status)
        except Exception as exc:
            publish_event(job, {"type": "info", "data": f"Supabase job sync failed: {exc}"})
        job["status"] = final_status

        publish_event(job, {
            "type": "done",
            "data": {
                "total": accepted_leads,
                "status": final_status,
                "job_id": job_id,
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
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


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
    conn.execute("UPDATE jobs SET root_job_id=job_id WHERE root_job_id IS NULL OR root_job_id='' ")
    conn.execute("UPDATE jobs SET completed_area_indexes='[]' WHERE completed_area_indexes IS NULL OR completed_area_indexes='' ")

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
        if cleaned not in ("starter", "pro", "growth", "enterprise"):
            raise ValueError("Invalid plan")
        return cleaned


class CreateOrderBody(BaseModel):
    plan: str

    @field_validator("plan")
    @classmethod
    def validate_plan(cls, value: str):
        cleaned = (value or "").strip().lower()
        if cleaned not in ("starter", "pro", "growth"):
            raise ValueError("Invalid plan")
        return cleaned


class VerifyPaymentBody(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str
    plan: str

    @field_validator("plan")
    @classmethod
    def validate_plan(cls, value: str):
        cleaned = (value or "").strip().lower()
        if cleaned not in ("pro", "growth"):
            raise ValueError("Invalid plan")
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
        if cleaned not in ("starter", "pro", "growth", "enterprise"):
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
    use_proxy: bool = False

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
    use_proxy      = job["use_proxy"]
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

        # ── Proxy pool ────────────────────────────────────────
        proxy_pool = None
        if use_proxy:
            publish_event(job, {"type": "info", "data": "Loading proxy pool…"})
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
                await loop.run_in_executor(None, proxy_pool.load)
                s = proxy_pool.stats()
                publish_event(job, {
                    "type": "proxy_stats",
                    "data": {
                        "live": s["live"],
                        "fastest_ms": s["fastest_ms"],
                        "by_protocol": s["by_protocol"],
                    },
                })
                publish_event(job, {
                    "type": "info",
                    "data": f"Proxy pool ready — {s['live']} live | fastest {s['fastest_ms']}ms",
                })
            except Exception as e:
                publish_event(job, {
                    "type": "info",
                    "data": f"Proxy pool failed ({e}), continuing without proxies",
                })
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

        all_leads: list = []
        pending_rows: list = []
        accepted_leads = 0
        current_step = 0
        seen_runtime_keys: set[str] = set()

        def flush_pending():
            nonlocal accepted_leads, pending_rows
            if not pending_rows:
                return
            lead_payloads = []
            for _, _, data in pending_rows:
                try:
                    lead_payloads.append(json.loads(data))
                except Exception:
                    pass
            conn.executemany(
                "INSERT INTO leads (job_id,user_id,data) VALUES (?,?,?)",
                pending_rows,
            )
            if lead_payloads:
                try:
                    save_leads_supabase(job_id, user_id, lead_payloads)
                except Exception as exc:
                    publish_event(job, {"type": "info", "data": f"Supabase lead sync failed: {exc}"})
            accepted_leads += len(pending_rows)
            conn.execute("UPDATE jobs SET lead_count=? WHERE job_id=?", (accepted_leads, job_id))
            conn.commit()
            pending_rows = []

        scrape_semaphore = asyncio.Semaphore(V2_SCRAPE_CONCURRENCY)
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
                    return platform_name, query, []
                try:
                    await asyncio.sleep(random.uniform(0.5, 1.2))
                    results = await scraper_fn(query, city, max_per_query, _get_proxy())
                    return platform_name, query, results
                except Exception as e:
                    publish_event(job, {"type": "info", "data": f"[{platform_name}] Error: {e}"})
                    return platform_name, query, []

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

        for batch_start in range(0, len(work_items), V2_BATCH_SIZE):
            if job.get("cancelled"):
                break

            batch = work_items[batch_start: batch_start + V2_BATCH_SIZE]
            batch_started = time.perf_counter()
            for platform_name, query, _ in batch:
                label = f"{query} in {city}  [{platform_name}]"
                publish_event(job, {"type": "progress", "data": {
                    "current": current_step, "total": total_steps, "query": label,
                }})

            batch_results = await asyncio.gather(
                *[run_scrape_item(platform_name, query, scraper_fn) for platform_name, query, scraper_fn in batch]
            )

            batch_leads: list[Lead] = []
            raw_count = 0
            for platform_name, query, results in batch_results:
                raw_count += len(results)
                current_step += 1
                label = f"{query} in {city}  [{platform_name}]"
                publish_event(job, {"type": "info", "data": f"[{platform_name}] {len(results)} leads for '{query}'"})
                publish_event(job, {"type": "progress", "data": {
                    "current": current_step, "total": total_steps, "query": label,
                }})
                batch_leads.extend(results)

            if not batch_leads:
                continue

            publish_event(job, {
                "type": "info",
                "data": f"Checking websites for batch of {len(batch_leads)} leads…",
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

        final_status = "stopped" if job.get("cancelled") else "completed"
        update_job_db(job_id, final_status, accepted_leads, csv_path)
        try:
            update_scrape_job(job_id, status=final_status, leads_found=accepted_leads)
            usage_user = get_user_by_id(user_id)
            calculate_usage(user_id)
            if usage_user:
                _send_scrape_ready_email(usage_user, accepted_leads, final_status)
        except Exception as exc:
            publish_event(job, {"type": "info", "data": f"Supabase job sync failed: {exc}"})
        job["status"] = final_status

        publish_event(job, {
            "type": "done",
            "data": {"total": accepted_leads, "status": final_status, "job_id": job_id},
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
async def start_scrape_v2(body: ScrapeV2Body, current_user=Depends(get_current_user)):
    user_id = current_user["id"]
    requested_platforms = {
        "maps": body.enable_maps,
        "justdial": body.enable_justdial,
        "indiamart": body.enable_indiamart,
    }
    usage = None
    try:
        usage = calculate_usage(user_id)
        print(f"PLAN: {usage['plan']}")
        print(f"LEADS USED: {usage['leads_used_this_month']}")
        print(f"SEARCHES TODAY: {usage['searches_today']}")
        print(f"LIMITS: {usage['limits']}")
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

    job_id    = str(uuid.uuid4())
    platforms = sum([body.enable_maps, body.enable_justdial, body.enable_indiamart])
    total     = len(body.queries) * platforms
    conn = get_db()
    conn.execute(
        """INSERT INTO jobs
           (job_id,user_id,profession,location,niche,areas,status,
            processed_areas,completed_area_indexes,total_areas,root_job_id)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (
            job_id, user_id,
            ", ".join(body.queries),
            body.city,
            body.niche,
            json.dumps(body.queries),
            "running", 0, "[]", total, job_id,
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
        status="running",
    )

    active_jobs[job_id] = {
        "status":          "running",
        "city":            body.city,
        "queries":         body.queries,
        "enable_maps":     body.enable_maps,
        "enable_justdial": body.enable_justdial,
        "enable_indiamart": body.enable_indiamart,
        "website_filter":  body.website_filter,
        "max_per_query":   body.max_per_query,
        "use_proxy":       body.use_proxy,
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
    }
    active_jobs[job_id]["task"] = asyncio.create_task(run_job_v2(job_id))
    return {"job_id": job_id}


@app.post("/auth/register")
def register(body: RegisterBody, request: Request):
    rate_key = _auth_rate_limit_key(request, body.email)
    if not _enforce_rate_limit(auth_attempt_log, rate_key, AUTH_RATE_LIMIT_REQUESTS, AUTH_RATE_LIMIT_WINDOW_SECONDS):
        logging.warning("Auth rate limit exceeded on register for %s", body.email)
        raise HTTPException(429, "Too many authentication attempts")
    if get_user_by_email(body.email):
        logging.warning("Register failed: duplicate email %s", body.email)
        raise HTTPException(400, "Email already registered")
    user = create_user(
        email=body.email,
        full_name=body.name,
        hashed_password=hash_password_bcrypt(body.password),
    )
    _send_welcome_email(user)
    return create_auth_response(user)


@app.post("/auth/login")
def login(body: LoginBody, request: Request):
    rate_key = _auth_rate_limit_key(request, body.email)
    if not _enforce_rate_limit(auth_attempt_log, rate_key, AUTH_RATE_LIMIT_REQUESTS, AUTH_RATE_LIMIT_WINDOW_SECONDS):
        logging.warning("Auth rate limit exceeded on login for %s", body.email)
        raise HTTPException(429, "Too many authentication attempts")
    user = get_user_by_email(body.email)
    if not user or not verify_password(body.password, user.get("hashed_password", "")):
        logging.warning("Login failed for email %s", body.email)
        raise HTTPException(401, "Invalid email or password")
    return create_auth_response(user)


@app.get("/auth/me")
def auth_me(current_user=Depends(get_current_user)):
    return {"user": _public_user(current_user)}


@app.post("/auth/refresh")
def auth_refresh(current_user=Depends(get_current_user), credentials: HTTPAuthorizationCredentials = Depends(auth_scheme)):
    payload = decode_access_token(credentials.credentials if credentials else "")
    if not should_refresh_token(payload):
        return {"token": None, "user": _public_user(current_user)}
    refreshed = create_auth_response(current_user)
    return refreshed


@app.post("/auth/logout")
def logout():
    return {"ok": True}


@app.post("/auth/forgot-password")
def forgot_password(body: ForgotPasswordBody, request: Request):
    rate_key = _auth_rate_limit_key(request, body.email)
    if not _enforce_rate_limit(email_request_log, rate_key, EMAIL_RATE_LIMIT_REQUESTS, EMAIL_RATE_LIMIT_WINDOW_SECONDS):
        logging.warning("Password reset email rate limit exceeded for %s", body.email)
        raise HTTPException(429, "Too many email requests")
    user = get_user_by_email(body.email)
    if user:
        token = _create_password_reset_token(user)
        reset_url = f"http://localhost:5173/reset-password?token={token}"
        send_email(
            user.get("email", ""),
            "Reset your LeadScout password",
            f"""
            <h2>Password reset</h2>
            <p>Use the link below to set a new password:</p>
            <p><a href="{reset_url}">{reset_url}</a></p>
            <p>This link expires in 1 hour.</p>
            """,
        )
    return {"ok": True}


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
    return {"ok": True}


@app.post("/user/select-plan")
def select_plan(body: SelectPlanBody, current_user=Depends(get_current_user)):
    plan = (body.plan or "").strip().lower()
    if plan not in ("starter", "pro", "growth", "enterprise"):
        raise HTTPException(400, "Invalid plan")
    user = update_user_plan(current_user["id"], plan)
    return {"user": _public_user(user)}


@app.post("/payment/create-order")
def payment_create_order(body: CreateOrderBody, current_user=Depends(get_current_user)):
    try:
        order = create_razorpay_order(body.plan, receipt=f"{current_user['id']}_{uuid.uuid4().hex[:12]}")
        logging.info("Payment order created for user %s plan %s amount %s", current_user["id"], body.plan, order["amount"])
        return order
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    except Exception:
        raise HTTPException(500, "Unable to create payment order")


@app.post("/payment/verify")
def payment_verify(body: VerifyPaymentBody, current_user=Depends(get_current_user)):
    if not verify_razorpay_signature(
        body.razorpay_order_id,
        body.razorpay_payment_id,
        body.razorpay_signature,
    ):
        logging.warning("Payment signature verification failed for user %s", current_user["id"])
        raise HTTPException(400, "Invalid payment signature")

    amount = PLAN_AMOUNTS.get(body.plan, 0)
    user = update_user_plan(current_user["id"], body.plan)
    _store_payment_record(
        current_user["id"],
        body.plan,
        amount,
        body.razorpay_order_id,
        body.razorpay_payment_id,
        "paid",
    )
    logging.info(
        "Payment verified for user %s plan %s order %s payment %s",
        current_user["id"],
        body.plan,
        body.razorpay_order_id,
        body.razorpay_payment_id,
    )
    _send_payment_email(user, body.plan, amount)
    return {"ok": True, "user": _public_user(user)}


@app.post("/scrape/start")
async def start_scrape(body: ScrapeBody, current_user=Depends(get_current_user)):
    user_id = current_user["id"]
    usage = None
    try:
        usage = calculate_usage(user_id)
        print(f"PLAN: {usage['plan']}")
        print(f"LEADS USED: {usage['leads_used_this_month']}")
        print(f"SEARCHES TODAY: {usage['searches_today']}")
        print(f"LIMITS: {usage['limits']}")
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
            "running",
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
        status="running",
    )
    active_jobs[job_id] = {
        "status": "running",
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
    active_jobs[job_id]["task"] = asyncio.create_task(run_job(job_id))
    return {"job_id": job_id}


@app.post("/scrape/stop/{job_id}")
def stop_scrape(job_id: str, current_user=Depends(get_current_user)):
    if job_id in active_jobs and active_jobs[job_id].get("user_id") == current_user["id"]:
        active_jobs[job_id]["cancelled"] = True
        active_jobs[job_id]["status"] = "stopping"
        try:
            update_scrape_job(job_id, status="stopping")
        except Exception:
            pass
    return {"ok": True}


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
    return {"ok": True, "deleted_job_id": job_id}


@app.get("/scrape/heartbeat/{job_id}")
def heartbeat(job_id: str, current_user=Depends(get_current_user)):
    """Lightweight heartbeat for sleep/wake detection."""
    if job_id in active_jobs:
        job = active_jobs[job_id]
        if job.get("user_id") != current_user["id"]:
            raise HTTPException(403, "Access denied")
        return {
            "alive": True,
            "status": job.get("status", "unknown"),
            "lead_count": int(job.get("lead_count", 0)),
            "processed_areas": int(job.get("processed_areas", 0)),
        }
    return {"alive": False, "status": "not_running"}


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
        return {"job_id": existing_job_id, "reused": True}

    return await start_scrape(ScrapeBody(
        profession=row["profession"] or "",
        areas=remaining,
        user_id=current_user["id"],
        niche=target_niche,
        location=target_location,
        root_job_id=row["root_job_id"] or row["job_id"],
        resumed_from_job_id=row["job_id"],
    ))


@app.get("/scrape/status/{job_id}")
def scrape_status(job_id: str, current_user=Depends(get_current_user)):
    if job_id in active_jobs:
        job = active_jobs[job_id]
        if job.get("user_id") != current_user["id"]:
            raise HTTPException(403, "Access denied")
        return {
            "job_id": job_id,
            "status": job.get("status", "running"),
            "lead_count": int(job.get("lead_count", 0)),
            "processed_areas": int(job.get("processed_areas", 0)),
            "total_areas": int(job.get("total_areas", 0)),
            "current_query": job.get("current_query", ""),
            "running": job.get("status") in ("running", "stopping"),
            "profession": job.get("profession", ""),
            "location": job.get("location", ""),
        }

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

    return {
        "job_id": row["job_id"],
        "status": status,
        "lead_count": row["lead_count"] or 0,
        "processed_areas": row["processed_areas"] or 0,
        "total_areas": row["total_areas"] or 0,
        "current_query": "",
        "running": False,
        "csv_path": row["csv_path"],
        "profession": row["profession"] or "",
        "location": row["location"] or "",
    }


@app.get("/scrape/download/{job_id}")
def download_csv(job_id: str, current_user=Depends(get_current_user)):
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
    if user_id != current_user["id"]:
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
    if user_id != current_user["id"]:
        raise HTTPException(403, "Access denied")
    path = generate_niche_csv_for_user(user_id, niche)
    if not path:
        raise HTTPException(404, "No scraped leads found for this niche")
    return FileResponse(path, media_type="text/csv", filename=Path(path).name)


@app.get("/scrape/download/merged/{user_id}")
def download_merged_job_ids_csv(user_id: str, job_ids: str, current_user=Depends(get_current_user)):
    if user_id != current_user["id"]:
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


@app.websocket("/ws/{job_id}")
async def ws_scrape(ws: WebSocket, job_id: str):
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
    return {"history": list_user_history_supabase(current_user["id"])}


@app.get("/user/history/{user_id}")
def user_history_compat(user_id: str, current_user=Depends(get_current_user)):
    if user_id != current_user["id"]:
        raise HTTPException(403, "Access denied")
    return {"history": list_user_history_supabase(current_user["id"])}


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
    return {"leads": rows}


@app.get("/user/usage")
def user_usage(current_user=Depends(get_current_user)):
    return calculate_usage(current_user["id"])


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
            
    return {"leads": leads}


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
    return {"users": result}


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
    return {"user": _public_user(user)}


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
    return {"jobs": [dict(j) for j in jobs]}


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
    return {
        "total_users":    total_users,
        "total_jobs":     total_jobs,
        "total_leads":    int(total_leads),
        "active_jobs":    sum(1 for j in active_jobs.values() if j.get("status") in ("running", "stopping")),
        "total_files":    csv_files,
        "top_profession": top_pro[0] if top_pro else None,
        "top_location":   top_loc[0] if top_loc else None,
        "avg_leads":      round(avg_leads) if avg_leads else 0,
    }


@app.get("/health")
def health():
    return {"status": "ok", "version": "1.0.0"}
