# ============================================================
#  scraper_maps.py  —  Google Maps Lead Scraper
#  Finds businesses with no / minimal website presence
# ============================================================
import argparse
import asyncio, re, logging, traceback, json
from dataclasses import asdict
from typing import Callable, List, Optional
from urllib.parse import quote_plus
from utils import Lead, clean, clean_phone, rua, delay

log = logging.getLogger("LeadScout.Maps")

MAPS_MAX_ATTEMPTS = 3
MAPS_NAVIGATION_TIMEOUT_MS = 45000
MAPS_ACTION_TIMEOUT_MS = 12000
MAPS_SCROLL_LIMIT = 20
MAPS_VIEWPORT = {"width": 1366, "height": 768}
MAPS_LAUNCH_ARGS = [
    "--headless=new",
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
]


def _emit_worker_progress(progress_callback, payload: dict):
    if progress_callback:
        progress_callback(payload)


async def scrape(
    query: str,
    city: str,
    max_results: int,
    proxy_dict=None,
    resume_state: Optional[dict] = None,
    progress_callback: Optional[Callable[[dict], None]] = None,
) -> List[Lead]:
    """
    Scrape Google Maps for `query` in `city`.
    Returns a list of Lead objects (website_status NOT yet set — done centrally).
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.error("Run: pip install playwright && playwright install chromium")
        return []

    resume_state = resume_state or {}
    search = f"{query} in {city}"
    maps_url = f"https://www.google.com/maps/search/{quote_plus(search)}"
    log.info("  [Maps] search=%s proxy_configured=%s", search, bool(proxy_dict))

    if progress_callback:
        progress_callback({
            "platform": "Maps",
            "phase": "build_search",
            "query": query,
            "city": city,
            "maps_url": maps_url,
        })

    last_error: Exception | None = None
    last_stage = "startup"

    for attempt in range(1, MAPS_MAX_ATTEMPTS + 1):
        browser = None
        ctx = None
        page = None
        leads: list[Lead] = []
        seen = set(resume_state.get("seen_labels", []))
        scroll_attempts = max(0, int(resume_state.get("scroll_attempt", 0) or 0))
        try:
            log.info(
                "  [Maps] attempt=%s/%s stage=launch search=%s resume_scroll=%s seen=%s",
                attempt,
                MAPS_MAX_ATTEMPTS,
                search,
                scroll_attempts,
                len(seen),
            )
            async with async_playwright() as pw:
                last_stage = "browser_launch"
                browser = await pw.chromium.launch(
                    headless=True,
                    args=MAPS_LAUNCH_ARGS,
                    timeout=MAPS_NAVIGATION_TIMEOUT_MS,
                    handle_sigint=False,
                    handle_sigterm=False,
                    handle_sighup=False,
                )
                log.info("  [Maps] browser launched attempt=%s search=%s", attempt, search)

                def _on_browser_disconnected():
                    log.error("  [Maps] browser disconnected attempt=%s search=%s stage=%s", attempt, search, last_stage)

                browser.on("disconnected", _on_browser_disconnected)

                last_stage = "context_create"
                ctx = await browser.new_context(
                    user_agent=rua(),
                    viewport=MAPS_VIEWPORT,
                    locale="en-IN",
                    ignore_https_errors=True,
                )
                await ctx.add_init_script(
                    "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
                )
                log.info("  [Maps] context opened attempt=%s search=%s", attempt, search)

                last_stage = "page_create"
                page = await ctx.new_page()
                page.set_default_timeout(MAPS_ACTION_TIMEOUT_MS)
                page.set_default_navigation_timeout(MAPS_NAVIGATION_TIMEOUT_MS)
                page.on("crash", lambda: log.error("  [Maps] page crash attempt=%s search=%s stage=%s", attempt, search, last_stage))
                page.on("close", lambda: log.info("  [Maps] page closed attempt=%s search=%s stage=%s", attempt, search, last_stage))
                log.info("  [Maps] page opened attempt=%s search=%s", attempt, search)

                if progress_callback:
                    _emit_worker_progress(progress_callback, {
                        "platform": "Maps",
                        "phase": "page_opened",
                        "query": query,
                        "city": city,
                        "attempt": attempt,
                    })

                last_stage = "goto_search"
                log.info("  [Maps] navigating attempt=%s url=%s", attempt, maps_url)
                await page.goto(maps_url, wait_until="domcontentloaded", timeout=MAPS_NAVIGATION_TIMEOUT_MS)
                await asyncio.sleep(3)
                log.info("  [Maps] search loaded attempt=%s url=%s", attempt, page.url)

                if progress_callback:
                    _emit_worker_progress(progress_callback, {
                        "platform": "Maps",
                        "phase": "search_loaded",
                        "query": query,
                        "city": city,
                        "attempt": attempt,
                        "maps_url": page.url,
                    })

                last_stage = "dismiss_consent"
                for txt in ["Accept all", "Accept All", "I agree", "Agree and continue"]:
                    try:
                        await page.click(f'button:has-text("{txt}")', timeout=2000)
                        await asyncio.sleep(1)
                        log.info("  [Maps] consent dismissed attempt=%s button=%s", attempt, txt)
                        break
                    except Exception:
                        pass

                last_stage = "wait_for_results"
                try:
                    await page.wait_for_selector('a[href*="/maps/place/"], div[role="feed"]', timeout=15000)
                except Exception:
                    log.warning("  [Maps] results container not ready attempt=%s search=%s", attempt, search)

                completed_scrolls = scroll_attempts
                replay_scrolls = 0
                while replay_scrolls < completed_scrolls:
                    replay_scrolls += 1
                    last_stage = f"replay_scroll_{replay_scrolls}"
                    try:
                        feed = page.locator('div[role="feed"]')
                        await feed.evaluate("el => el.scrollBy(0, 1400)")
                        await asyncio.sleep(1.2)
                    except Exception:
                        await page.evaluate("window.scrollBy(0, 1400)")
                        await asyncio.sleep(1.2)

                while len(leads) < max_results and scroll_attempts < MAPS_SCROLL_LIMIT:
                    scroll_attempts += 1
                    last_stage = f"scroll_{scroll_attempts}"
                    log.info(
                        "  [Maps] scroll attempt=%s/%s search=%s seen=%s leads=%s",
                        scroll_attempts,
                        MAPS_SCROLL_LIMIT,
                        search,
                        len(seen),
                        len(leads),
                    )
                    if progress_callback:
                        _emit_worker_progress(progress_callback, {
                            "platform": "Maps",
                            "scroll_attempt": scroll_attempts,
                            "phase": "search_results",
                            "query": query,
                            "city": city,
                            "seen_labels": list(seen)[-100:],
                        })

                    cards = await page.query_selector_all('a[href*="/maps/place/"]')
                    log.info("  [Maps] listings seen attempt=%s scroll=%s cards=%s", attempt, scroll_attempts, len(cards))

                    for card_idx, card in enumerate(cards, start=1):
                        if len(leads) >= max_results:
                            break
                        try:
                            last_stage = f"extract_listing_{scroll_attempts}_{card_idx}"
                            label = await card.get_attribute("aria-label") or ""
                            if not label or label in seen:
                                continue
                            seen.add(label)
                            log.info("  [Maps] extraction started attempt=%s listing=%s label=%s", attempt, card_idx, clean(label)[:80])
                            await card.click()
                            await asyncio.sleep(2)

                            lead = Lead(
                                name=clean(label),
                                query=query,
                                city=city,
                                category=query,
                                source="google_maps",
                                listing_url=page.url,
                            )

                            for sel in [
                                'button[data-tooltip="Copy phone number"]',
                                'button[aria-label*="Phone"]',
                                'button[aria-label*="phone"]',
                            ]:
                                try:
                                    el = await page.query_selector(sel)
                                    if el:
                                        lead.phone = clean_phone(await el.inner_text())
                                        break
                                except Exception:
                                    pass

                            for sel in [
                                'a[data-tooltip="Open website"]',
                                'a[aria-label*="website"]',
                                'a[aria-label*="Website"]',
                            ]:
                                try:
                                    el = await page.query_selector(sel)
                                    if el:
                                        href = await el.get_attribute("href") or ""
                                        if href and "google.com" not in href:
                                            lead.website = href
                                        break
                                except Exception:
                                    pass

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
                            log.info("    [Maps] ✓ %s", lead.name[:50])
                        except Exception as card_exc:
                            log.warning(
                                "  [Maps] extraction failed attempt=%s stage=%s type=%s error=%s",
                                attempt,
                                last_stage,
                                type(card_exc).__name__,
                                card_exc,
                            )

                    last_stage = f"scroll_feed_{scroll_attempts}"
                    try:
                        feed = page.locator('div[role="feed"]')
                        await feed.evaluate("el => el.scrollBy(0, 1400)")
                        await asyncio.sleep(1.8)
                    except Exception:
                        await page.evaluate("window.scrollBy(0, 1400)")
                        await asyncio.sleep(1.8)

                log.info("  [Maps] complete attempt=%s leads=%s search=%s", attempt, len(leads), search)
                return leads
        except Exception as exc:
            last_error = exc
            log.error(
                "  [Maps] attempt failed attempt=%s/%s stage=%s type=%s error=%s",
                attempt,
                MAPS_MAX_ATTEMPTS,
                last_stage,
                type(exc).__name__,
                exc,
            )
            log.error("  [Maps] traceback attempt=%s\n%s", attempt, traceback.format_exc())
            if progress_callback:
                _emit_worker_progress(progress_callback, {
                    "platform": "Maps",
                    "phase": "failure",
                    "query": query,
                    "city": city,
                    "attempt": attempt,
                    "stage": last_stage,
                    "scroll_attempt": scroll_attempts,
                    "seen_labels": list(seen)[-100:],
                })
            await asyncio.sleep(min(5, attempt * 1.5))
        finally:
            for closer, label in ((page, "page"), (ctx, "context"), (browser, "browser")):
                if closer is None:
                    continue
                try:
                    await closer.close()
                except Exception as close_exc:
                    if "has been closed" not in str(close_exc).lower():
                        log.warning("  [Maps] close warning %s attempt=%s error=%s", label, attempt, close_exc)

    raise RuntimeError(
        f"Google Maps scrape failed at stage '{last_stage}' after {MAPS_MAX_ATTEMPTS} attempts: {last_error}"
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
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        resume_state = json.loads(args.resume_state or "{}")
    except Exception:
        resume_state = {}
    leads = scrape_in_isolated_loop(
        args.query,
        args.city,
        args.max_results,
        resume_state=resume_state,
        progress_callback=_worker_progress,
    )
    print(
        "__MAPS_RESULT__" + json.dumps(
            {
                "leads": [asdict(lead) for lead in leads],
            }
        ),
        flush=True,
    )
