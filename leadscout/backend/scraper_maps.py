# ============================================================
#  scraper_maps.py  —  Google Maps Lead Scraper
# ============================================================
import argparse
import asyncio
import json
import logging
import os
import re
import time
import traceback
import random
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, List, Optional
from urllib.parse import quote_plus

from utils import Lead, clean, clean_phone, rua

log = logging.getLogger("LeadScout.Maps")

MAPS_MAX_ATTEMPTS = 3
MAPS_NAVIGATION_TIMEOUT_MS = 30000
MAPS_ACTION_TIMEOUT_MS = 9000
MAPS_RESULTS_WAIT_MS = 9000
MAPS_SCROLL_LIMIT = 20
MAPS_VIEWPORT = {"width": 1366, "height": 768}
MAPS_DEBUG_DIR = Path(os.getenv("LEADSCOUT_MAPS_DEBUG_DIR", str(Path(__file__).parent / "output" / "maps_debug")))
MAPS_DEBUG_DIR.mkdir(parents=True, exist_ok=True)
MAPS_TIMEZONE = os.getenv("LEADSCOUT_MAPS_TIMEZONE", "Asia/Kolkata")
MAPS_LOCALE = os.getenv("LEADSCOUT_MAPS_LOCALE", "en-IN")
MAPS_HEADLESS = os.getenv("LEADSCOUT_MAPS_HEADLESS", "0").strip().lower() in {"1", "true", "yes"}
MAPS_LAUNCH_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-software-rasterizer",
    "--disable-extensions",
    "--disable-background-networking",
    "--disable-background-timer-throttling",
    "--disable-renderer-backgrounding",
    "--disable-blink-features=AutomationControlled",
    "--no-first-run",
    "--no-default-browser-check",
    "--lang=en-IN",
]

RESULT_LINK_SELECTORS = [
    'a[href*="/maps/place/"]',
    'div[role="article"] a[href*="/maps/place/"]',
    'div[role="feed"] a[href*="/maps/place/"]',
]
RESULT_CONTAINER_SELECTORS = [
    'div[role="feed"]',
    'div[aria-label*="Results for"]',
    'div[aria-label*="Results"]',
    'div[role="main"]',
]

PHONE_TEXT_RE = re.compile(r"(?:\+?\d[\d()\-\s]{6,}\d)")
BLOCKER_TEXT_PATTERNS = {
    "captcha_block": [
        "unusual traffic",
        "not a robot",
        "captcha",
        "sorry, but your computer or network may be sending automated queries",
    ],
    "signin_block": [
        "sign in",
        "use google maps",
    ],
    "consent_block": [
        "before you continue",
        "accept all",
        "reject all",
        "privacy & terms",
    ],
    "no_results_ui": [
        "no results found",
        "couldn't find",
        "did not match any locations",
    ],
}
CONSENT_BUTTONS = [
    "Accept all",
    "Accept All",
    "Reject all",
    "I agree",
    "Agree and continue",
    "Not now",
]


class MapsScrapeError(RuntimeError):
    def __init__(self, blocker_type: str, stage: str, message: str, debug_artifacts: Optional[dict] = None, dom_markers: Optional[dict] = None):
        super().__init__(message)
        self.blocker_type = blocker_type
        self.stage = stage
        self.debug_artifacts = debug_artifacts or {}
        self.dom_markers = dom_markers or {}


def _emit_worker_progress(progress_callback, payload: dict):
    if progress_callback:
        progress_callback(payload)


async def _human_pause(min_ms: int = 600, max_ms: int = 1400):
    await asyncio.sleep(random.uniform(min_ms / 1000, max_ms / 1000))


def _proxy_server(proxy_dict) -> Optional[str]:
    if not proxy_dict:
        return None
    return proxy_dict.get("http") or proxy_dict.get("https")


def _proxy_log_value(proxy_dict) -> str:
    server = _proxy_server(proxy_dict)
    if not server:
        return "direct"
    return server


async def _save_debug_artifacts(page, query: str, city: str, stage: str, blocker_type: str) -> dict:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    slug = re.sub(r"[^a-z0-9]+", "_", f"{query}_{city}_{stage}_{blocker_type}".lower()).strip("_")[:80]
    base = MAPS_DEBUG_DIR / f"{timestamp}_{slug}"
    screenshot_path = f"{base}.png"
    html_path = f"{base}.html"
    snapshot_path = f"{base}.json"
    payload = {
        "screenshot_path": screenshot_path,
        "html_path": html_path,
        "snapshot_path": snapshot_path,
    }
    try:
        await page.screenshot(path=screenshot_path, full_page=True)
    except Exception:
        payload.pop("screenshot_path", None)
    try:
        html = await page.content()
        Path(html_path).write_text(html, encoding="utf-8")
    except Exception:
        payload.pop("html_path", None)
        html = ""
    try:
        snapshot = {
            "url": page.url,
            "title": await page.title(),
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "html_excerpt": html[:8000] if html else "",
        }
        Path(snapshot_path).write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        payload.pop("snapshot_path", None)
    return payload


async def _dom_markers(page) -> dict:
    body_text = ""
    try:
        body_text = (await page.locator("body").inner_text(timeout=1500))[:8000]
    except Exception:
        body_text = ""
    lowered = body_text.lower()
    return {
        "url": page.url,
        "title": await page.title(),
        "has_feed": await page.locator('div[role="feed"]').count() > 0,
        "has_result_links": await page.locator('a[href*="/maps/place/"]').count() > 0,
        "result_link_count": await page.locator('a[href*="/maps/place/"]').count(),
        "has_main": await page.locator('div[role="main"]').count() > 0,
        "consent_detected": any(token in lowered for token in BLOCKER_TEXT_PATTERNS["consent_block"]),
        "captcha_detected": any(token in lowered for token in BLOCKER_TEXT_PATTERNS["captcha_block"]),
        "signin_detected": any(token in lowered for token in BLOCKER_TEXT_PATTERNS["signin_block"]),
        "no_results_detected": any(token in lowered for token in BLOCKER_TEXT_PATTERNS["no_results_ui"]),
        "body_excerpt": body_text[:1200],
    }


def _has_results_markers(markers: Optional[dict]) -> bool:
    markers = markers or {}
    return bool(markers.get("has_feed") or markers.get("has_result_links") or int(markers.get("result_link_count") or 0) > 0)


async def _dismiss_consent(page) -> bool:
    for txt in CONSENT_BUTTONS:
        try:
            locator = page.get_by_role("button", name=txt)
            if await locator.count():
                await locator.first.click(timeout=2500)
                await asyncio.sleep(1.2)
                return True
        except Exception:
            pass
    return False


async def _dismiss_signin_overlay(page) -> bool:
    candidates = [
        ("button", "Not now"),
        ("button", "Skip"),
        ("button", "Close"),
        ("button", "No thanks"),
    ]
    for role, label in candidates:
        try:
            locator = page.get_by_role(role, name=label)
            if await locator.count():
                await locator.first.click(timeout=2000)
                await _human_pause(700, 1200)
                return True
        except Exception:
            pass

    close_selectors = [
        'button[aria-label="Close"]',
        'button[aria-label="close"]',
        'button[jsaction*="close"]',
        'div[role="dialog"] button[aria-label*="Close"]',
        'div[role="dialog"] svg',
    ]
    for selector in close_selectors:
        try:
            locator = page.locator(selector)
            if await locator.count():
                await locator.first.click(timeout=1500)
                await _human_pause(700, 1200)
                return True
        except Exception:
            pass

    try:
        removed = await page.evaluate(
            """
            () => {
              const selectors = [
                'div[role="dialog"]',
                'div[aria-modal="true"]',
                'div[aria-label*="Sign in"]',
                'div[jscontroller][role="dialog"]'
              ];
              let removedCount = 0;
              for (const selector of selectors) {
                for (const node of document.querySelectorAll(selector)) {
                  node.remove();
                  removedCount += 1;
                }
              }
              document.body.classList.remove('overflow-hidden');
              document.body.style.overflow = 'auto';
              document.documentElement.style.overflow = 'auto';
              return removedCount;
            }
            """
        )
        if removed:
            await _human_pause(700, 1200)
            return True
    except Exception:
        pass
    return False


async def _wait_for_results_ready(page) -> tuple[bool, dict]:
    start = time.perf_counter()
    last_markers = {}
    while (time.perf_counter() - start) * 1000 < MAPS_RESULTS_WAIT_MS:
        try:
            markers = await _dom_markers(page)
        except Exception:
            await asyncio.sleep(0.75)
            continue
        last_markers = markers
        if _has_results_markers(markers):
            return True, markers
        if markers["captcha_detected"]:
            return False, {**markers, "blocker_type": "captcha_block"}
        if markers["signin_detected"] and not _has_results_markers(markers):
            return False, {**markers, "blocker_type": "signin_block"}
        if markers["no_results_detected"]:
            return False, {**markers, "blocker_type": "no_results_ui"}
        await asyncio.sleep(0.9)
    return False, last_markers


async def _extract_visible_phone(page) -> str:
    selectors = [
        'button[data-tooltip="Copy phone number"]',
        'button[aria-label*="Phone"]',
        'button[aria-label*="phone"]',
        'a[href^="tel:"]',
        'button[data-item-id*="phone"]',
        'div[data-tooltip="Copy phone number"]',
    ]
    for selector in selectors:
        try:
            nodes = await page.query_selector_all(selector)
            for node in nodes:
                text = ""
                try:
                    text = await node.inner_text()
                except Exception:
                    text = await node.get_attribute("aria-label") or await node.get_attribute("href") or ""
                phone = clean_phone(text.replace("tel:", ""))
                if phone:
                    return phone
        except Exception:
            pass

    text_blocks = []
    for selector in [
        'div[role="main"]',
        'div[role="dialog"]',
        'body',
    ]:
        try:
            locator = page.locator(selector)
            if await locator.count():
                text_blocks.append(await locator.first.inner_text(timeout=1200))
        except Exception:
            continue
    merged = "\n".join(text_blocks)
    for match in PHONE_TEXT_RE.findall(merged or ""):
        phone = clean_phone(match)
        if len(re.sub(r"[^\d]", "", phone)) >= 7:
            return phone
    return ""


async def _extract_visible_website(page) -> str:
    for sel in [
        'a[data-tooltip="Open website"]',
        'a[aria-label*="website"]',
        'a[aria-label*="Website"]',
        'a[data-item-id*="authority"]',
    ]:
        try:
            elements = await page.query_selector_all(sel)
            for el in elements:
                href = await el.get_attribute("href") or ""
                if href and "google.com" not in href:
                    return href
        except Exception:
            pass
    return ""


async def _raise_blocker(page, query: str, city: str, stage: str, blocker_type: str, message: str, dom_markers: Optional[dict] = None):
    markers = dom_markers or await _dom_markers(page)
    artifacts = await _save_debug_artifacts(page, query, city, stage, blocker_type)
    log.error(
        "  [Maps] blocker stage=%s type=%s url=%s title=%s markers=%s artifacts=%s",
        stage,
        blocker_type,
        markers.get("url"),
        markers.get("title"),
        {k: v for k, v in markers.items() if k != "body_excerpt"},
        artifacts,
    )
    raise MapsScrapeError(blocker_type, stage, message, debug_artifacts=artifacts, dom_markers=markers)


async def scrape(
    query: str,
    city: str,
    max_results: int,
    proxy_dict=None,
    resume_state: Optional[dict] = None,
    progress_callback: Optional[Callable[[dict], None]] = None,
) -> List[Lead]:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.error("Run: pip install playwright && playwright install chromium")
        return []

    resume_state = resume_state or {}
    search = f"{query} in {city}"
    maps_url = f"https://www.google.com/maps/search/{quote_plus(search)}"
    last_error: Exception | None = None
    last_stage = "startup"
    last_blocker = "unknown"
    seen = set(resume_state.get("seen_labels", []))
    scroll_attempts = max(0, int(resume_state.get("scroll_attempt", 0) or 0))
    proxy_server = _proxy_server(proxy_dict)

    _emit_worker_progress(progress_callback, {
        "platform": "Maps",
        "phase": "launching_browser",
        "query": query,
        "city": city,
        "maps_url": maps_url,
        "proxy": _proxy_log_value(proxy_dict),
    })
    log.info("  [Maps] search=%s proxy=%s resume_scroll=%s seen=%s", search, _proxy_log_value(proxy_dict), scroll_attempts, len(seen))

    for attempt in range(1, MAPS_MAX_ATTEMPTS + 1):
        browser = None
        ctx = None
        page = None
        leads: list[Lead] = []
        try:
            async with async_playwright() as pw:
                last_stage = "browser_launch"
                launch_kwargs = dict(
                    headless=MAPS_HEADLESS,
                    args=MAPS_LAUNCH_ARGS,
                    timeout=MAPS_NAVIGATION_TIMEOUT_MS,
                    handle_sigint=False,
                    handle_sigterm=False,
                    handle_sighup=False,
                )
                if proxy_server:
                    launch_kwargs["proxy"] = {"server": proxy_server}
                log.info("  [Maps] attempt=%s/%s launch proxy=%s", attempt, MAPS_MAX_ATTEMPTS, _proxy_log_value(proxy_dict))
                browser = await pw.chromium.launch(**launch_kwargs)
                browser.on("disconnected", lambda: log.error("  [Maps] browser disconnected attempt=%s stage=%s", attempt, last_stage))
                _emit_worker_progress(progress_callback, {
                    "platform": "Maps",
                    "phase": "launching_browser",
                    "query": query,
                    "city": city,
                    "attempt": attempt,
                    "proxy": _proxy_log_value(proxy_dict),
                })

                last_stage = "context_create"
                ctx = await browser.new_context(
                    user_agent=rua(),
                    viewport=MAPS_VIEWPORT,
                    locale=MAPS_LOCALE,
                    timezone_id=MAPS_TIMEZONE,
                    ignore_https_errors=True,
                    extra_http_headers={
                        "Accept-Language": "en-IN,en;q=0.9",
                        "Upgrade-Insecure-Requests": "1",
                    },
                )
                await ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")

                last_stage = "page_create"
                page = await ctx.new_page()
                page.set_default_timeout(MAPS_ACTION_TIMEOUT_MS)
                page.set_default_navigation_timeout(MAPS_NAVIGATION_TIMEOUT_MS)
                page.on("crash", lambda: log.error("  [Maps] page crash attempt=%s stage=%s", attempt, last_stage))

                last_stage = "navigation"
                _emit_worker_progress(progress_callback, {
                    "platform": "Maps",
                    "phase": "navigating_maps",
                    "query": query,
                    "city": city,
                    "attempt": attempt,
                    "proxy": _proxy_log_value(proxy_dict),
                })
                await page.goto(maps_url, wait_until="domcontentloaded", timeout=MAPS_NAVIGATION_TIMEOUT_MS)
                await _human_pause(1800, 2800)
                markers = await _dom_markers(page)
                log.info(
                    "  [Maps] loaded attempt=%s url=%s title=%s feed=%s links=%s link_count=%s consent=%s captcha=%s signin=%s",
                    attempt,
                    markers.get("url"),
                    markers.get("title"),
                    markers.get("has_feed"),
                    markers.get("has_result_links"),
                    markers.get("result_link_count"),
                    markers.get("consent_detected"),
                    markers.get("captcha_detected"),
                    markers.get("signin_detected"),
                )

                if "google.com/maps" not in (markers.get("url") or ""):
                    await _raise_blocker(page, query, city, "navigation", "navigation_failure", "Google Maps did not enter search state", markers)

                last_stage = "consent_check"
                if markers.get("consent_detected"):
                    _emit_worker_progress(progress_callback, {
                        "platform": "Maps",
                        "phase": "consent_detected",
                        "query": query,
                        "city": city,
                        "attempt": attempt,
                        "proxy": _proxy_log_value(proxy_dict),
                    })
                    dismissed = await _dismiss_consent(page)
                    await asyncio.sleep(1.0)
                    markers = await _dom_markers(page)
                    if not dismissed and markers.get("consent_detected"):
                        await _raise_blocker(page, query, city, "consent_check", "consent_block", "Consent screen blocked the Maps results", markers)

                if markers.get("captcha_detected"):
                    _emit_worker_progress(progress_callback, {
                        "platform": "Maps",
                        "phase": "captcha_detected",
                        "query": query,
                        "city": city,
                        "attempt": attempt,
                        "proxy": _proxy_log_value(proxy_dict),
                    })
                    await _raise_blocker(page, query, city, "captcha_check", "captcha_block", "CAPTCHA or unusual traffic block detected", markers)

                if markers.get("signin_detected") and not _has_results_markers(markers):
                    _emit_worker_progress(progress_callback, {
                        "platform": "Maps",
                        "phase": "selector_failure",
                        "query": query,
                        "city": city,
                        "attempt": attempt,
                        "proxy": _proxy_log_value(proxy_dict),
                        "blocker_type": "signin_overlay",
                    })
                    dismissed_signin = await _dismiss_signin_overlay(page)
                    await _human_pause(900, 1600)
                    try:
                        feed = page.locator('div[role="feed"]')
                        if await feed.count():
                            await feed.first.evaluate("el => el.scrollBy(0, 900)")
                        else:
                            await page.evaluate("window.scrollBy(0, 900)")
                        await _human_pause(900, 1400)
                    except Exception:
                        pass
                    markers = await _dom_markers(page)
                    if not dismissed_signin and markers.get("signin_detected") and not _has_results_markers(markers):
                        await _raise_blocker(page, query, city, "signin_check", "selector_mismatch", "Google sign-in prompt blocked the results layout", markers)

                last_stage = "results_ready"
                _emit_worker_progress(progress_callback, {
                    "platform": "Maps",
                    "phase": "waiting_for_results",
                    "query": query,
                    "city": city,
                    "attempt": attempt,
                    "proxy": _proxy_log_value(proxy_dict),
                })
                ready, ready_markers = await _wait_for_results_ready(page)
                if not ready:
                    blocker_type = ready_markers.get("blocker_type") or ("no_results_ui" if ready_markers.get("no_results_detected") else "selector_mismatch")
                    phase = {
                        "captcha_block": "captcha_detected",
                        "no_results_ui": "selector_failure",
                        "selector_mismatch": "selector_failure",
                    }.get(blocker_type, "selector_failure")
                    _emit_worker_progress(progress_callback, {
                        "platform": "Maps",
                        "phase": phase,
                        "query": query,
                        "city": city,
                        "attempt": attempt,
                        "proxy": _proxy_log_value(proxy_dict),
                        "blocker_type": blocker_type,
                    })
                    await _raise_blocker(page, query, city, "results_ready", blocker_type, "Google Maps results UI did not become ready", ready_markers)

                try:
                    feed = page.locator('div[role="feed"]')
                    if await feed.count():
                        await feed.first.evaluate("el => el.scrollBy(0, 900)")
                    else:
                        await page.evaluate("window.scrollBy(0, 900)")
                    await _human_pause(900, 1400)
                except Exception:
                    pass

                completed_scrolls = scroll_attempts
                replay_scrolls = 0
                while replay_scrolls < completed_scrolls:
                    replay_scrolls += 1
                    last_stage = f"replay_scroll_{replay_scrolls}"
                    try:
                        feed = page.locator('div[role="feed"]')
                        await feed.evaluate("el => el.scrollBy(0, 1400)")
                        await asyncio.sleep(1.0)
                    except Exception:
                        await page.evaluate("window.scrollBy(0, 1400)")
                        await asyncio.sleep(1.0)

                while len(leads) < max_results and scroll_attempts < MAPS_SCROLL_LIMIT:
                    scroll_attempts += 1
                    last_stage = f"scroll_{scroll_attempts}"
                    _emit_worker_progress(progress_callback, {
                        "platform": "Maps",
                        "phase": "searching_sources",
                        "query": query,
                        "city": city,
                        "attempt": attempt,
                        "scroll_attempt": scroll_attempts,
                        "seen_labels": list(seen)[-100:],
                        "proxy": _proxy_log_value(proxy_dict),
                    })

                    cards = []
                    for selector in RESULT_LINK_SELECTORS:
                        cards = await page.query_selector_all(selector)
                        if cards:
                            break
                    log.info("  [Maps] listings seen attempt=%s scroll=%s cards=%s selector=%s", attempt, scroll_attempts, len(cards), selector if cards else "-")

                    for card_idx, card in enumerate(cards, start=1):
                        if len(leads) >= max_results:
                            break
                        try:
                            last_stage = f"extract_listing_{scroll_attempts}_{card_idx}"
                            label = await card.get_attribute("aria-label") or ""
                            if not label or label in seen:
                                continue
                            seen.add(label)
                            await card.click()
                            await _human_pause(1200, 2200)

                            lead = Lead(
                                name=clean(label),
                                query=query,
                                city=city,
                                category=query,
                                source="google_maps",
                                listing_url=page.url,
                            )

                            lead.phone = await _extract_visible_phone(page)
                            lead.website = await _extract_visible_website(page)

                            for sel in [
                                'button[data-tooltip="Copy address"]',
                                'button[aria-label*="Address"]',
                                'button[aria-label*="address"]',
                            ]:
                                try:
                                    el = await page.query_selector(sel)
                                    if el:
                                        lead.address = clean(await el.inner_text())
                                        break
                                except Exception:
                                    pass

                            try:
                                el = await page.query_selector('div.fontDisplayLarge')
                                if el:
                                    lead.rating = clean(await el.inner_text())
                            except Exception:
                                pass

                            try:
                                el = await page.query_selector('button[aria-label*="reviews"]')
                                if el:
                                    txt = await el.inner_text()
                                    lead.reviews = re.sub(r"[^\d,]", "", txt)
                            except Exception:
                                pass

                            try:
                                el = await page.query_selector('button.DkEaL')
                                if el:
                                    lead.category = clean(await el.inner_text())
                            except Exception:
                                pass

                            leads.append(lead)
                            log.info(
                                "    [Maps] ✓ %s phone_found=%s website_found=%s email_found=%s",
                                lead.name[:50],
                                bool(lead.phone),
                                bool(lead.website),
                                bool(lead.email),
                            )
                        except Exception as card_exc:
                            log.warning("  [Maps] extraction failed attempt=%s stage=%s type=%s error=%s", attempt, last_stage, type(card_exc).__name__, card_exc)

                    last_stage = f"scroll_feed_{scroll_attempts}"
                    try:
                        feed = page.locator('div[role="feed"]')
                        if await feed.count():
                            await feed.first.evaluate("el => el.scrollBy(0, 1400)")
                        else:
                            await page.evaluate("window.scrollBy(0, 1400)")
                        await _human_pause(900, 1600)
                    except Exception:
                        await page.evaluate("window.scrollBy(0, 1400)")
                        await _human_pause(900, 1600)

                log.info("  [Maps] complete attempt=%s leads=%s search=%s proxy=%s", attempt, len(leads), search, _proxy_log_value(proxy_dict))
                return leads
        except MapsScrapeError as exc:
            last_error = exc
            last_stage = exc.stage
            last_blocker = exc.blocker_type
            _emit_worker_progress(progress_callback, {
                "platform": "Maps",
                "phase": "source_blocked" if exc.blocker_type in ("consent_block", "captcha_block") else "selector_failure",
                "query": query,
                "city": city,
                "attempt": attempt,
                "proxy": _proxy_log_value(proxy_dict),
                "blocker_type": exc.blocker_type,
                "stage": exc.stage,
                "debug_artifacts": exc.debug_artifacts,
            })
            if exc.blocker_type in ("consent_block", "captcha_block", "no_results_ui", "selector_mismatch", "navigation_failure"):
                break
            await asyncio.sleep(min(3, attempt))
        except Exception as exc:
            last_error = exc
            inferred_blocker = "navigation_failure" if last_stage == "navigation" else "source_error"
            last_blocker = inferred_blocker
            log.error("  [Maps] attempt failed attempt=%s/%s stage=%s type=%s error=%s", attempt, MAPS_MAX_ATTEMPTS, last_stage, type(exc).__name__, exc)
            log.error("  [Maps] traceback attempt=%s\n%s", attempt, traceback.format_exc())
            debug_artifacts = {}
            if page is not None:
                try:
                    debug_artifacts = await _save_debug_artifacts(page, query, city, last_stage, inferred_blocker)
                except Exception:
                    debug_artifacts = {}
            _emit_worker_progress(progress_callback, {
                "platform": "Maps",
                "phase": "rotating_proxy" if attempt < MAPS_MAX_ATTEMPTS else "source_blocked",
                "query": query,
                "city": city,
                "attempt": attempt,
                "proxy": _proxy_log_value(proxy_dict),
                "blocker_type": inferred_blocker,
                "stage": last_stage,
                "scroll_attempt": scroll_attempts,
                "seen_labels": list(seen)[-100:],
                "debug_artifacts": debug_artifacts,
            })
            await asyncio.sleep(min(3, attempt))
        finally:
            for closer in (page, ctx, browser):
                if closer is None:
                    continue
                try:
                    await closer.close()
                except Exception:
                    pass

    blocker_text = getattr(last_error, "blocker_type", last_blocker)
    raise RuntimeError(
        f"Google Maps scrape failed blocker={blocker_text} stage={last_stage} proxy={_proxy_log_value(proxy_dict)} error={last_error}"
    )


def scrape_in_isolated_loop(
    query: str,
    city: str,
    max_results: int,
    proxy_dict=None,
    resume_state: Optional[dict] = None,
    progress_callback: Optional[Callable[[dict], None]] = None,
) -> List[Lead]:
    return asyncio.run(
        scrape(
            query,
            city,
            max_results,
            proxy_dict=proxy_dict,
            resume_state=resume_state,
            progress_callback=progress_callback,
        )
    )


def _worker_progress(payload: dict):
    print("__MAPS_PROGRESS__" + json.dumps(payload), flush=True)


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", required=True)
    parser.add_argument("--city", required=True)
    parser.add_argument("--max-results", type=int, required=True)
    parser.add_argument("--resume-state", default="{}")
    parser.add_argument("--proxy-json", default="")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        resume_state = json.loads(args.resume_state or "{}")
    except Exception:
        resume_state = {}
    try:
        proxy_dict = json.loads(args.proxy_json) if args.proxy_json else None
    except Exception:
        proxy_dict = None
    leads = scrape_in_isolated_loop(
        args.query,
        args.city,
        args.max_results,
        proxy_dict=proxy_dict,
        resume_state=resume_state,
        progress_callback=_worker_progress,
    )
    print("__MAPS_RESULT__" + json.dumps({"leads": [asdict(lead) for lead in leads]}), flush=True)
