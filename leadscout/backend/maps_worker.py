import asyncio
import json
import logging
import os
import random
import sys
import traceback
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import config as app_config
from proxy_manager import ProxyPool
from scraper_indiamart import scrape as indiamart_scrape
from scraper_justdial import scrape as justdial_scrape
from supabase_db import (
    claim_scrape_job,
    get_scrape_job,
    get_user_by_id,
    list_worker_scrape_jobs,
    save_leads,
    update_scrape_job,
)
from utils import Lead, async_enrich_websites, make_runtime_lead_key, should_keep

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

WORKER_TYPE = "maps_worker"
WORKER_ID = os.getenv("LEADSCOUT_MAPS_WORKER_ID") or f"maps-worker-{os.getpid()}"
POLL_SECONDS = max(1.0, float(os.getenv("LEADSCOUT_MAPS_WORKER_POLL_SECONDS", "2")))
MAX_RECENT_EVENTS = 20
SCRAPE_ITEM_RETRIES = max(1, int(os.getenv("LEADSCOUT_MAPS_ITEM_RETRIES", "3")))
WEBSITE_CONCURRENCY = max(1, int(os.getenv("LEADSCOUT_V2_WEBSITE_CONCURRENCY", "8")))
V2_BATCH_SIZE = max(1, int(os.getenv("LEADSCOUT_V2_BATCH_SIZE", "3")))
ALLOW_DIRECT_FALLBACK = os.getenv("LEADSCOUT_ALLOW_DIRECT_MAPS_FALLBACK", "0").strip().lower() in {"1", "true", "yes"}
PROXY_ROTATION_ENABLED = bool(getattr(app_config, "USE_PROXY_POOL", True))

proxy_pool = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_queries(value: Any) -> List[str]:
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


def _normalize_platforms(value: Any) -> Dict[str, bool]:
    if isinstance(value, dict):
        return {str(key): bool(enabled) for key, enabled in value.items()}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return {str(key): bool(enabled) for key, enabled in parsed.items()}
        except Exception:
            return {}
    return {}


def _normalize_progress_marker(value: Any) -> Dict[str, Any]:
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


def _job_item_key(platform_name: str, query: str) -> str:
    return f"{platform_name}::{query}"


def _event(state: Dict[str, Any], event_type: str, data: Any) -> None:
    payload = {"type": event_type, "data": data, "ts": _utc_now()}
    events = state.setdefault("recent_events", [])
    events.append(payload)
    if len(events) > MAX_RECENT_EVENTS:
        del events[: len(events) - MAX_RECENT_EVENTS]

    if event_type == "progress":
        progress = data or {}
        state["current_query"] = str(progress.get("query") or state.get("current_query") or "")
        state["progress_message"] = f"{int(progress.get('current', 0))}/{int(progress.get('total', 0))} steps • {state['current_query']}".strip()
    elif event_type in ("info", "error"):
        state["progress_message"] = str(data or "")
    elif event_type == "lead":
        state["lead_count"] = int(state.get("lead_count", 0)) + 1
        state["progress_message"] = f"Saved {state['lead_count']} leads"


def _final_status(cancelled: bool, total_found: int, total_saved: int, source_failures: List[str]) -> str:
    if cancelled:
        return "stopped"
    if total_found <= 0 and total_saved <= 0 and source_failures:
        return "source_error"
    if total_found <= 0 and total_saved <= 0:
        return "no_results"
    if total_found > 0 and total_saved <= 0:
        return "low_data"
    return "completed"


def _final_status_message(status: str) -> str:
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


def _lead_to_payload(lead: Lead) -> Dict[str, Any]:
    data = asdict(lead)
    return {
        "Name": data.get("name", ""),
        "Category": data.get("category", ""),
        "Phone": data.get("phone", ""),
        "Email": data.get("email", ""),
        "Address": data.get("address", ""),
        "City": data.get("city", ""),
        "Website": data.get("website", ""),
        "website_status": data.get("website_status", ""),
        "Rating": data.get("rating", ""),
        "Reviews": data.get("reviews", ""),
        "source": data.get("source", ""),
        "listing_url": data.get("listing_url", ""),
        "query": data.get("query", ""),
        "Maps URL": data.get("listing_url", ""),
    }


def _ensure_proxy_pool():
    global proxy_pool
    if proxy_pool is not None:
        return proxy_pool
    if not PROXY_ROTATION_ENABLED:
        return None
    try:
        pool = ProxyPool(
            protocols=getattr(app_config, "PROXY_PROTOCOLS", None),
            test_timeout=getattr(app_config, "PROXY_TEST_TIMEOUT", 8),
            test_workers=getattr(app_config, "PROXY_TEST_WORKERS", 120),
            max_failures=getattr(app_config, "PROXY_MAX_FAILURES", 3),
            quarantine_seconds=max(120, int(os.getenv("LEADSCOUT_PROXY_QUARANTINE_SECONDS", "600"))),
            extra_proxies=getattr(app_config, "EXTRA_PROXIES", None),
            verbose=False,
        )
        pool.load()
        proxy_pool = pool
        logging.info("Maps worker proxy pool ready stats=%s", pool.stats())
    except Exception as exc:
        logging.warning("Maps worker proxy pool unavailable error=%s", exc)
        proxy_pool = None
    return proxy_pool


def _pick_proxy(previous_urls: set[str]) -> Dict[str, Any]:
    pool = _ensure_proxy_pool()
    if not pool:
        return {"proxy": None, "rotated": False, "selection": "direct_only" if ALLOW_DIRECT_FALLBACK else "no_proxy_available"}
    chosen = None
    for _ in range(max(1, len(pool))):
        candidate = pool.get()
        if not candidate:
            break
        if (candidate.get("http") or "") not in previous_urls:
            chosen = candidate
            break
    if not chosen:
        chosen = pool.get()
    return {
        "proxy": chosen,
        "rotated": bool(chosen),
        "selection": "healthy_proxy" if chosen else ("direct_only" if ALLOW_DIRECT_FALLBACK else "pool_exhausted"),
        "proxy_info": pool.info(chosen) if chosen else None,
    }


def _report_proxy_outcome(proxy: Dict[str, str] | None, blocker_type: str | None, success: bool) -> None:
    pool = _ensure_proxy_pool()
    if not pool or not proxy:
        return
    if success:
        pool.report_success(proxy)
    else:
        pool.report_failure(proxy, blocker_type or "source_error")


async def _run_maps_subprocess(query: str, city: str, max_per_query: int, resume_state: Dict[str, Any], progress_callback, proxy_dict=None):
    worker_script = str(Path(__file__).parent / "scraper_maps.py")
    cmd = [
        sys.executable,
        worker_script,
        "--query",
        query,
        "--city",
        city,
        "--max-results",
        str(max_per_query),
        "--resume-state",
        json.dumps(resume_state or {}),
    ]
    if proxy_dict:
        cmd.extend(["--proxy-json", json.dumps(proxy_dict)])
    logging.info("Maps subprocess start worker=%s query=%s city=%s", WORKER_ID, query, city)
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
                progress_callback(payload)
            elif text.startswith("__MAPS_RESULT__"):
                result_payload = json.loads(text[len("__MAPS_RESULT__"):])
            else:
                logging.info("Maps worker stdout query=%s line=%s", query, text)

    async def _consume_stderr():
        assert proc.stderr is not None
        while True:
            line = await proc.stderr.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").strip()
            if text:
                logging.warning("Maps worker stderr query=%s line=%s", query, text)

    await asyncio.gather(_consume_stdout(), _consume_stderr())
    exit_code = await proc.wait()
    if exit_code != 0:
        raise RuntimeError(f"Maps worker exited with code {exit_code}")
    if not result_payload:
        raise RuntimeError("Maps worker finished without result payload")
    return [Lead(**lead) for lead in result_payload.get("leads", [])]


async def _save_job_state(job_id: str, state: Dict[str, Any], **extra: Any) -> None:
    payload = {
        "status": state.get("status"),
        "leads_found": int(state.get("saved_count", 0)),
        "processed_areas": int(state.get("processed_areas", 0)),
        "total_areas": int(state.get("total_areas", 0)),
        "current_query": state.get("current_query", ""),
        "progress_message": state.get("progress_message", ""),
        "progress_marker": state.get("progress_marker"),
        "recent_events": state.get("recent_events", []),
        "error_message": state.get("error_message"),
        "finished_at": state.get("finished_at"),
    }
    payload.update({key: value for key, value in extra.items() if value is not None})
    update_scrape_job(job_id, **payload)


def _job_cancel_requested(job_id: str) -> bool:
    row = get_scrape_job(job_id) or {}
    return bool(row.get("cancel_requested"))


async def process_job(job_row: Dict[str, Any]) -> None:
    job_id = job_row["id"]
    user_id = job_row["user_id"]
    city = job_row.get("city") or ""
    queries = _normalize_queries(job_row.get("queries"))
    platforms = _normalize_platforms(job_row.get("platforms"))
    progress_marker = _normalize_progress_marker(job_row.get("progress_marker"))
    progress_items = progress_marker.setdefault("items", {})
    recent_events = list(job_row.get("recent_events") or [])
    total_areas = int(job_row.get("total_areas") or max(1, len(queries) * sum(1 for value in platforms.values() if value)))
    max_per_query = int(job_row.get("max_per_query") or 25)
    website_filter = (job_row.get("website_filter") or "minimal").strip().lower()

    state = {
        "status": "running",
        "lead_count": 0,
        "saved_count": int(job_row.get("leads_found") or 0),
        "processed_areas": int(job_row.get("processed_areas") or 0),
        "total_areas": total_areas,
        "current_query": job_row.get("current_query") or "",
        "progress_message": "Searching sources...",
        "progress_marker": progress_marker,
        "recent_events": recent_events[-MAX_RECENT_EVENTS:],
        "error_message": None,
        "finished_at": None,
    }
    await _save_job_state(job_id, state)
    logging.info("Maps worker claimed job=%s user=%s city=%s queries=%s platforms=%s", job_id, user_id, city, queries, platforms)

    enabled_platforms = []
    if platforms.get("maps"):
        enabled_platforms.append(("Maps", "google_maps"))
    if platforms.get("justdial"):
        enabled_platforms.append(("JustDial", "justdial"))
    if platforms.get("indiamart"):
        enabled_platforms.append(("IndiaMart", "indiamart"))

    all_leads: List[Lead] = []
    total_found = 0
    source_failures: List[str] = []
    seen_runtime_keys = set()

    async def _run_item(platform_name: str, query: str, resume_state: Dict[str, Any]) -> List[Lead]:
        def _progress_callback(progress_state: Dict[str, Any]) -> None:
            item_state = progress_items.setdefault(
                _job_item_key(platform_name, query),
                {"platform": platform_name, "query": query, "completed": False, "resume_state": {}},
            )
            item_state["resume_state"] = progress_state or {}
            progress_marker["current_item"] = _job_item_key(platform_name, query)
            progress_items[_job_item_key(platform_name, query)] = item_state

            phase = progress_state.get("phase") if isinstance(progress_state, dict) else None
            phase_message = {
                "launching_browser": "Launching browser...",
                "build_search": "Launching browser...",
                "page_opened": "Launching browser...",
                "navigating_maps": "Navigating Maps...",
                "search_loaded": "Navigating Maps...",
                "waiting_for_results": "Waiting for results...",
                "search_results": "Searching sources...",
                "listing_page": "Searching sources...",
                "fetch_page": "Searching sources...",
                "consent_detected": "Consent detected...",
                "captcha_detected": "Captcha detected...",
                "rotating_proxy": "Rotating proxy...",
                "source_blocked": "Source blocked...",
                "selector_failure": "Results UI not ready...",
            }.get(phase, "Searching sources...")
            state["progress_message"] = phase_message
            state["progress_marker"] = progress_marker
            if phase in {"consent_detected", "captcha_detected", "rotating_proxy", "source_blocked", "selector_failure"}:
                _event(state, "info", phase_message)
            debug_artifacts = progress_state.get("debug_artifacts") if isinstance(progress_state, dict) else None
            if debug_artifacts:
                _event(state, "info", f"Debug artifacts saved: {debug_artifacts}")

        last_error = None
        used_proxy_urls = set()
        for attempt in range(1, SCRAPE_ITEM_RETRIES + 1):
            proxy_context = {"proxy": None, "rotated": False, "selection": "direct_only", "proxy_info": None}
            try:
                if platform_name == "Maps":
                    proxy_context = _pick_proxy(used_proxy_urls)
                    selected_proxy = proxy_context["proxy"]
                    selected_proxy_url = (selected_proxy or {}).get("http") or ""
                    if not selected_proxy and not ALLOW_DIRECT_FALLBACK:
                        raise RuntimeError(f"blocker=source_blocked stage=proxy_selection proxy=direct error=No healthy proxy available and direct fallback is disabled")
                    if selected_proxy_url:
                        used_proxy_urls.add(selected_proxy_url)
                    logging.info(
                        "Maps worker proxy selection job=%s query=%s attempt=%s/%s proxy=%s rotated=%s selection=%s info=%s",
                        job_id,
                        query,
                        attempt,
                        SCRAPE_ITEM_RETRIES,
                        selected_proxy_url or "direct",
                        proxy_context["rotated"],
                        proxy_context["selection"],
                        proxy_context["proxy_info"],
                    )
                    if selected_proxy:
                        _event(state, "info", f"Rotating proxy... using {selected_proxy_url}")
                logging.info(
                    "Maps worker item start job=%s platform=%s query=%s city=%s attempt=%s/%s resume=%s",
                    job_id,
                    platform_name,
                    query,
                    city,
                    attempt,
                    SCRAPE_ITEM_RETRIES,
                    resume_state,
                )
                if platform_name == "Maps":
                    results = await asyncio.wait_for(
                        _run_maps_subprocess(query, city, max_per_query, resume_state, _progress_callback, proxy_dict=proxy_context["proxy"]),
                        timeout=240,
                    )
                    _report_proxy_outcome(proxy_context.get("proxy"), None, success=True)
                    return results
                if platform_name == "JustDial":
                    return await asyncio.wait_for(
                        justdial_scrape(query, city, max_per_query, None, resume_state=resume_state, progress_callback=_progress_callback),
                        timeout=150,
                    )
                if platform_name == "IndiaMart":
                    return await asyncio.wait_for(
                        indiamart_scrape(query, city, max_per_query, None, resume_state=resume_state, progress_callback=_progress_callback),
                        timeout=120,
                    )
                return []
            except Exception as exc:
                last_error = exc
                blocker_type = "source_error"
                exc_text = str(exc)
                if "blocker=" in exc_text:
                    match = exc_text.split("blocker=", 1)[1].split()[0]
                    blocker_type = match.strip()
                selected_proxy = proxy_context.get("proxy")
                selected_proxy_url = (selected_proxy or {}).get("http") or "direct"
                _report_proxy_outcome(selected_proxy, blocker_type, success=False)
                logging.warning(
                    "Maps worker item retry job=%s platform=%s query=%s attempt=%s/%s proxy=%s blocker=%s error=%s",
                    job_id,
                    platform_name,
                    query,
                    attempt,
                    SCRAPE_ITEM_RETRIES,
                    selected_proxy_url,
                    blocker_type,
                    exc,
                )
                if platform_name == "Maps":
                    _event(state, "info", f"Rotating proxy... blocker={blocker_type} proxy={selected_proxy_url}")
                if attempt >= SCRAPE_ITEM_RETRIES:
                    raise
                await asyncio.sleep(min(2.5, attempt))
        raise RuntimeError(str(last_error or "Unknown worker error"))

    try:
        work_items: List[tuple[str, str]] = []
        for query in queries:
            for platform_name, _ in enabled_platforms:
                item_key = _job_item_key(platform_name, query)
                item_state = progress_items.get(item_key) or {}
                if item_state.get("completed"):
                    continue
                work_items.append((platform_name, query))

        for batch_start in range(0, len(work_items), V2_BATCH_SIZE):
            if _job_cancel_requested(job_id):
                state["status"] = "stopped"
                break

            batch = work_items[batch_start: batch_start + V2_BATCH_SIZE]
            for platform_name, query in batch:
                label = f"{query} in {city}  [{platform_name}]"
                _event(state, "info", "Searching sources...")
                _event(
                    state,
                    "progress",
                    {"current": state["processed_areas"], "total": total_areas, "query": label},
                )
            await _save_job_state(job_id, state)

            batch_leads: List[Lead] = []
            for platform_name, query in batch:
                item_key = _job_item_key(platform_name, query)
                item_state = progress_items.setdefault(
                    item_key,
                    {"platform": platform_name, "query": query, "completed": False, "resume_state": {}},
                )
                state["current_query"] = f"{query} in {city}  [{platform_name}]"
                state["progress_marker"]["current_item"] = item_key
                await _save_job_state(job_id, state)
                try:
                    results = await _run_item(platform_name, query, item_state.get("resume_state") or {})
                    item_state["completed"] = True
                    progress_items[item_key] = item_state
                    state["progress_marker"]["current_item"] = None
                    total_found += len(results)
                    batch_leads.extend(results)
                    _event(state, "info", f"[{platform_name}] {len(results)} leads for '{query}'")
                except Exception as exc:
                    error_text = str(exc)
                    source_failures.append(f"{platform_name}:{query}:{error_text}")
                    _event(state, "info", f"[{platform_name}] Source issue: {error_text}")
                    logging.error(
                        "Maps worker item failed job=%s platform=%s query=%s error=%s\n%s",
                        job_id,
                        platform_name,
                        query,
                        error_text,
                        traceback.format_exc(),
                    )
                finally:
                    state["processed_areas"] = min(total_areas, int(state["processed_areas"]) + 1)
                    _event(
                        state,
                        "progress",
                        {"current": state["processed_areas"], "total": total_areas, "query": state["current_query"]},
                    )
                    await _save_job_state(job_id, state)

            if not batch_leads:
                continue

            _event(state, "info", "Processing results...")
            await _save_job_state(job_id, state)
            await async_enrich_websites(
                batch_leads,
                timeout=10,
                concurrency=WEBSITE_CONCURRENCY,
                retries=3,
            )

            _event(state, "info", "Saving leads...")
            await _save_job_state(job_id, state)
            payloads = []
            for lead in batch_leads:
                if _job_cancel_requested(job_id):
                    state["status"] = "stopped"
                    break
                if not should_keep(lead, website_filter):
                    continue
                lead_key = make_runtime_lead_key(lead)
                if lead_key in seen_runtime_keys:
                    continue
                seen_runtime_keys.add(lead_key)
                payload = _lead_to_payload(lead)
                payloads.append(payload)
                all_leads.append(lead)
                _event(state, "lead", payload)
            if payloads:
                save_leads(job_id, user_id, payloads)
                state["saved_count"] = int(state.get("saved_count", 0)) + len(payloads)
                await _save_job_state(job_id, state)

        final_status = _final_status(state["status"] == "stopped", total_found, state["saved_count"], source_failures)
        state["status"] = final_status
        state["progress_message"] = _final_status_message(final_status)
        state["error_message"] = " | ".join(source_failures) if source_failures else None
        state["finished_at"] = _utc_now()
        _event(
            state,
            "done",
            {
                "status": final_status,
                "job_id": job_id,
                "total": state["saved_count"],
                "raw_found": total_found,
                "message": state["progress_message"],
            },
        )
        await _save_job_state(job_id, state, cancel_requested=False)
        user = get_user_by_id(user_id) or {}
        logging.info(
            "Maps worker finished job=%s user=%s email=%s status=%s raw_found=%s saved=%s failure_reason=%s",
            job_id,
            user_id,
            user.get("email"),
            final_status,
            total_found,
            state["saved_count"],
            state["error_message"] or "-",
        )
    except Exception as exc:
        state["status"] = "failed"
        state["progress_message"] = _final_status_message("failed")
        state["error_message"] = str(exc)
        state["finished_at"] = _utc_now()
        _event(state, "error", str(exc))
        await _save_job_state(job_id, state, cancel_requested=False)
        logging.error("Maps worker fatal job=%s error=%s\n%s", job_id, exc, traceback.format_exc())


async def worker_loop() -> None:
    logging.info("Maps worker started worker_id=%s poll_seconds=%s", WORKER_ID, POLL_SECONDS)
    while True:
        try:
            queued_jobs = list_worker_scrape_jobs(WORKER_TYPE, ["queued"], limit=5)
            claimed_job = None
            for job in queued_jobs:
                claimed_job = claim_scrape_job(job["id"], WORKER_TYPE, WORKER_ID)
                if claimed_job:
                    break
            if not claimed_job:
                await asyncio.sleep(POLL_SECONDS)
                continue
            await process_job(claimed_job)
        except Exception as exc:
            logging.error("Maps worker loop error=%s\n%s", exc, traceback.format_exc())
            await asyncio.sleep(POLL_SECONDS)


if __name__ == "__main__":
    asyncio.run(worker_loop())
