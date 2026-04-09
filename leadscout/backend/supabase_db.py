import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from supabase import Client, create_client
from env_utils import require_env

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

SUPABASE_URL = require_env("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY", "")
if not SUPABASE_KEY:
    raise RuntimeError("Missing required environment variable: SUPABASE_SERVICE_KEY")

PLAN_LIMITS = {
    "starter": {
        "leads": 100,
        "searches": 3,
        "platforms": ["google_maps"],
    },
    "pro": {
        "leads": 3500,
        "searches": 50,
        "platforms": ["google_maps", "justdial"],
    },
    "growth": {
        "leads": 10000,
        "searches": float("inf"),
        "platforms": ["google_maps", "justdial", "web"],
    },
    "enterprise": {
        "leads": float("inf"),
        "searches": float("inf"),
        "platforms": ["google_maps", "justdial", "web"],
    },
}

_supabase: Optional[Client] = None


def get_supabase() -> Client:
    global _supabase
    if _supabase is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise RuntimeError("Supabase configuration is missing in backend/.env")
        _supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _supabase


def _rows(response: Any) -> List[Dict[str, Any]]:
    return list(getattr(response, "data", None) or [])


def _single(response: Any) -> Optional[Dict[str, Any]]:
    rows = _rows(response)
    return rows[0] if rows else None


def _clean_text(value: Any, max_len: int = 500) -> str:
    text = str(value or "").replace("\x00", " ").strip()
    return text[:max_len]


def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    resp = (
        get_supabase()
        .table("users")
        .select("*")
        .eq("email", email.strip().lower())
        .limit(1)
        .execute()
    )
    return _single(resp)


def get_user_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    resp = (
        get_supabase()
        .table("users")
        .select("*")
        .eq("id", user_id)
        .limit(1)
        .execute()
    )
    return _single(resp)


def create_user(email: str, full_name: str, hashed_password: str) -> Dict[str, Any]:
    payload = {
        "email": email.strip().lower(),
        "full_name": full_name.strip(),
        "hashed_password": hashed_password,
        "plan": "starter",
        "role": "user",
    }
    resp = get_supabase().table("users").insert(payload).execute()
    row = _single(resp)
    if not row:
        raise RuntimeError("Failed to create user")
    return row


def update_user_plan(user_id: str, plan: str) -> Dict[str, Any]:
    resp = (
        get_supabase()
        .table("users")
        .update({"plan": plan})
        .eq("id", user_id)
        .execute()
    )
    row = _single(resp)
    if not row:
        row = get_user_by_id(user_id)
    if not row:
        raise RuntimeError("Failed to update user plan")
    return row


def update_user_role(user_id: str, role: str) -> Dict[str, Any]:
    resp = (
        get_supabase()
        .table("users")
        .update({"role": role})
        .eq("id", user_id)
        .execute()
    )
    row = _single(resp)
    if not row:
        row = get_user_by_id(user_id)
    if not row:
        raise RuntimeError("Failed to update user role")
    return row


def update_user_fields(user_id: str, **fields: Any) -> Dict[str, Any]:
    payload = {key: value for key, value in fields.items() if value is not None}
    if not payload:
        row = get_user_by_id(user_id)
        if not row:
            raise RuntimeError("User not found")
        return row
    resp = (
        get_supabase()
        .table("users")
        .update(payload)
        .eq("id", user_id)
        .execute()
    )
    row = _single(resp)
    if not row:
        row = get_user_by_id(user_id)
    if not row:
        raise RuntimeError("Failed to update user")
    return row


def list_all_users() -> List[Dict[str, Any]]:
    resp = (
        get_supabase()
        .table("users")
        .select("*")
        .order("created_at", desc=True)
        .execute()
    )
    return _rows(resp)


def create_scrape_job(
    job_id: str,
    user_id: str,
    city: str,
    queries: List[str],
    platforms: Dict[str, bool],
    website_filter: str,
    status: str = "running",
) -> None:
    payload = {
        "id": job_id,
        "user_id": user_id,
        "city": city,
        "queries": queries,
        "platforms": platforms,
        "website_filter": website_filter,
        "status": status,
        "leads_found": 0,
    }
    get_supabase().table("scrape_jobs").upsert(payload).execute()


def update_scrape_job(job_id: str, **fields: Any) -> None:
    if not fields:
        return
    get_supabase().table("scrape_jobs").update(fields).eq("id", job_id).execute()


def save_leads(job_id: str, user_id: str, leads: List[Dict[str, Any]]) -> None:
    if not leads:
        return
    rows = []
    seen = set()
    for lead in leads:
        dedupe_key = (
            _clean_text(lead.get("Name") or lead.get("name")),
            _clean_text(lead.get("Phone") or lead.get("phone")),
            _clean_text(lead.get("Address") or lead.get("address")),
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        rows.append(
            {
                "job_id": job_id,
                "user_id": user_id,
                "name": _clean_text(lead.get("Name") or lead.get("name")),
                "category": _clean_text(lead.get("Category") or lead.get("category")),
                "phone": _clean_text(lead.get("Phone") or lead.get("phone")),
                "email": _clean_text(lead.get("Email") or lead.get("email")),
                "address": _clean_text(lead.get("Address") or lead.get("address")),
                "city": _clean_text(lead.get("City") or lead.get("city")),
                "website": _clean_text(lead.get("Website") or lead.get("website")),
                "website_status": _clean_text(lead.get("website_status") or lead.get("website_status")),
                "rating": _clean_text(lead.get("Rating") or lead.get("rating")),
                "reviews": _clean_text(lead.get("Reviews") or lead.get("reviews")),
                "source": _clean_text(lead.get("source") or lead.get("Source")),
                "listing_url": _clean_text(lead.get("listing_url") or lead.get("Maps URL") or lead.get("listing_url")),
            }
        )
    if rows:
        get_supabase().table("leads").insert(rows).execute()


def list_user_history(user_id: str) -> List[Dict[str, Any]]:
    resp = (
        get_supabase()
        .table("scrape_jobs")
        .select("*")
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .execute()
    )
    history = []
    for row in _rows(resp):
        queries = row.get("queries") or []
        if isinstance(queries, str):
            try:
                queries = json.loads(queries)
            except Exception:
                queries = [queries]
        history.append(
            {
                **row,
                "job_id": row.get("id"),
                "profession": ", ".join(queries) if isinstance(queries, list) else str(queries or ""),
                "location": row.get("city") or "",
                "lead_count": row.get("leads_found", 0),
                "effective_lead_count": row.get("leads_found", 0),
                "persisted_leads": row.get("leads_found", 0),
                "areas": json.dumps(queries if isinstance(queries, list) else []),
                "total_areas": len(queries) if isinstance(queries, list) else 0,
                "processed_areas": len(queries) if row.get("status") in ("completed", "stopped", "no_results") and isinstance(queries, list) else 0,
                "niche": f"{(queries[0] if isinstance(queries, list) and queries else 'leads')}_{row.get('city') or 'city'}".replace(" ", "_").lower(),
            }
        )
    return history


def list_user_leads(
    user_id: str,
    search: Optional[str] = None,
    source: Optional[str] = None,
    website_status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    query = (
        get_supabase()
        .table("leads")
        .select("*")
        .eq("user_id", user_id)
        .order("scraped_at", desc=True)
    )
    if source:
        query = query.eq("source", source)
    if website_status:
        query = query.eq("website_status", website_status)
    resp = query.execute()
    rows = _rows(resp)
    if search:
        needle = search.strip().lower()
        rows = [
            row for row in rows
            if needle in (row.get("name") or "").lower()
            or needle in (row.get("city") or "").lower()
        ]
    return rows


def _month_prefix() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _today_prefix() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def calculate_usage(user_id: str) -> Dict[str, Any]:
    user = get_user_by_id(user_id)
    if not user:
        raise RuntimeError("User not found")

    jobs = list_user_history(user_id)
    leads = list_user_leads(user_id)
    month_prefix = _month_prefix()
    today_prefix = _today_prefix()

    leads_used = sum(
        1 for lead in leads
        if str(lead.get("scraped_at") or "").startswith(month_prefix)
    )
    searches_today = sum(
        1 for job in jobs
        if str(job.get("created_at") or "").startswith(today_prefix)
    )

    plan = (user.get("plan") or "starter").lower()
    limits = PLAN_LIMITS.get(plan, PLAN_LIMITS["starter"])
    get_supabase().table("users").update(
        {
            "leads_used_this_month": leads_used,
            "searches_today": searches_today,
        }
    ).eq("id", user_id).execute()

    return {
        "plan": plan,
        "leads_used_this_month": leads_used,
        "searches_today": searches_today,
        "limits": limits,
    }


def normalize_requested_platforms(requested_platforms: Dict[str, bool]) -> List[str]:
    mapping = {
        "maps": "google_maps",
        "google_maps": "google_maps",
        "justdial": "justdial",
        "indiamart": "web",
        "web": "web",
    }
    normalized = []
    for name, enabled in (requested_platforms or {}).items():
        if not enabled:
            continue
        mapped = mapping.get(name, name)
        if mapped not in normalized:
            normalized.append(mapped)
    return normalized


def enforce_plan(user_id: str, requested_platforms: Dict[str, bool]) -> Dict[str, Any]:
    usage = calculate_usage(user_id)
    user = get_user_by_id(user_id) or {}
    if (user.get("role") or "user").lower() == "admin":
        usage["limits"] = {
            "leads": float("inf"),
            "searches": float("inf"),
            "platforms": ["google_maps", "justdial", "web"],
        }
        return usage
    limits = usage["limits"]
    requested = normalize_requested_platforms(requested_platforms)

    if usage["leads_used_this_month"] >= limits["leads"]:
        raise PermissionError("Monthly lead limit reached")

    if usage["searches_today"] >= limits["searches"]:
        raise PermissionError("Daily search limit reached")

    for requested_platform in requested:
        if requested_platform not in limits["platforms"]:
            raise PermissionError("Upgrade to access this data source")

    return usage
