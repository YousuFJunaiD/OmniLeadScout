# ============================================================
#  scraper_justdial.py  —  JustDial Lead Scraper
# ============================================================
import asyncio, json, re, logging
from typing import Callable, List, Optional
from urllib.parse import urljoin
from utils import Lead, clean, clean_phone, rua

log = logging.getLogger("LeadScout.JustDial")

JD_BASE = "https://www.justdial.com"


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
    leads = []
    city_slug  = city.replace(" ", "-")
    query_slug = query.replace(" ", "-")
    url = f"{JD_BASE}/{city_slug}/{query_slug}"
    log.info(f"  [JD] {query} in {city}")

    launch_kwargs = dict(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage",
              "--disable-blink-features=AutomationControlled"],
    )
    # Note: proxies not passed to browser — Playwright handles its own routing

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(**launch_kwargs)
        ctx = await browser.new_context(
            user_agent=rua(),
            viewport={"width": 1440, "height": 900},
            locale="en-IN",
        )
        await ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )
        page = await ctx.new_page()

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)

            # Dismiss login modal if it appears
            for sel in ['button[aria-label="Close"]', 'span.modal__close', '.cross']:
                try:
                    await page.click(sel, timeout=1500)
                    await asyncio.sleep(0.5)
                    break
                except Exception:
                    pass

            page_num = max(0, int(resume_state.get("page", 1) or 1) - 1)
            seen_names = set(resume_state.get("seen_names", []))

            while len(leads) < max_results and page_num < 8:
                page_num += 1
                if progress_callback:
                    progress_callback({
                        "platform": "JustDial",
                        "page": page_num,
                        "phase": "listing_page",
                        "query": query,
                        "city": city,
                        "seen_names": list(seen_names)[-100:],
                    })

                # ── Strategy 1: extract from __NEXT_DATA__ JSON ──────────────
                try:
                    nd_text = await page.evaluate("""
                        () => { const el = document.getElementById('__NEXT_DATA__');
                                return el ? el.textContent : null; }
                    """)
                    if nd_text:
                        data  = json.loads(nd_text)
                        items = _walk_json(data)
                        for item in items:
                            if len(leads) >= max_results:
                                break
                            name = item.get("name", "")
                            if not name or name in seen_names:
                                continue
                            seen_names.add(name)
                            lead = Lead(
                                name=clean(name),
                                category=clean(item.get("category", query)),
                                phone=clean_phone(item.get("phone", "")),
                                address=clean(item.get("address", "")),
                                city=city,
                                website=item.get("website", ""),
                                rating=str(item.get("rating", "")),
                                reviews=str(item.get("reviews", "")),
                                source="justdial",
                                listing_url=item.get("url", url),
                                query=query,
                            )
                            leads.append(lead)
                            log.info(f"    [JD] ✓ {lead.name[:50]}")
                        if leads:
                            # Got data from JSON — scroll and try for more
                            await page.evaluate("window.scrollBy(0, 2000)")
                            await asyncio.sleep(2)
                            continue
                except Exception:
                    pass

                # ── Strategy 2: DOM scraping ─────────────────────────────────
                selectors = [
                    "li.cntanr",
                    ".jsx-card",
                    "[class*='resultbox']",
                    ".store-details",
                    "div.row.resultbox_info",
                ]
                cards = []
                for sel in selectors:
                    cards = await page.query_selector_all(sel)
                    if cards:
                        break

                for card in cards:
                    if len(leads) >= max_results:
                        break
                    try:
                        lead = await _parse_card(card, city, query, url)
                        if lead and lead.name and lead.name not in seen_names:
                            seen_names.add(lead.name)
                            leads.append(lead)
                            log.info(f"    [JD] ✓ {lead.name[:50]}")
                    except Exception as e:
                        log.debug(f"    [JD] card error: {e}")

                # Scroll for more
                await page.evaluate("window.scrollBy(0, 2000)")
                await asyncio.sleep(2.5)

                # Next page button
                try:
                    nxt = await page.query_selector('a[aria-label="Next page"], a.next, li.next a')
                    if nxt:
                        await nxt.click()
                        await asyncio.sleep(3)
                    else:
                        break
                except Exception:
                    break

        except Exception as e:
            log.error(f"  [JD] Page error: {e}")
        finally:
            await browser.close()

    log.info(f"  [JD] → {len(leads)} leads for '{query}'")
    return leads


def _walk_json(obj, depth=0) -> list:
    """Recursively find business listing dicts in JustDial's __NEXT_DATA__."""
    if depth > 12:
        return []
    results = []
    if isinstance(obj, dict):
        name = (obj.get("businessname") or obj.get("display_name") or
                obj.get("company_name") or obj.get("title") or "")
        if name and len(str(name)) > 2:
            phone = (obj.get("mobileno") or obj.get("phone") or
                     obj.get("mobile") or obj.get("primaryno") or "")
            results.append({
                "name":     clean(str(name)),
                "phone":    clean_phone(str(phone)),
                "address":  clean(str(obj.get("address") or obj.get("area_details") or "")),
                "category": clean(str(obj.get("cats") or obj.get("catname") or "")),
                "website":  str(obj.get("website") or obj.get("web_url") or ""),
                "rating":   str(obj.get("rating") or obj.get("overall_rating") or ""),
                "reviews":  str(obj.get("reviewscount") or obj.get("review_count") or ""),
                "url":      str(obj.get("jdurl") or obj.get("profile_url") or ""),
            })
        for v in obj.values():
            results.extend(_walk_json(v, depth + 1))
    elif isinstance(obj, list):
        for item in obj:
            results.extend(_walk_json(item, depth + 1))
    return results


async def _parse_card(card, city: str, query: str, base_url: str) -> Optional[Lead]:
    lead = Lead(source="justdial", city=city, query=query, category=query)

    # Name
    for sel in ["h2.jd_rated span", "span.lng_lnk_tel",
                ".resultbox_title_anchor span", "h2 a", "h2 span", ".jd_header h1"]:
        try:
            el = await card.query_selector(sel)
            if el:
                t = clean(await el.inner_text())
                if t:
                    lead.name = t
                    break
        except Exception:
            pass

    if not lead.name:
        return None

    # Phone
    for sel in ["a.tel_href", "a[href^='tel:']", ".contact-info span"]:
        try:
            el = await card.query_selector(sel)
            if el:
                raw = await el.get_attribute("href") or await el.inner_text()
                lead.phone = clean_phone(raw.replace("tel:", ""))
                break
        except Exception:
            pass

    # Address
    for sel in ["p.address-info", ".address-info span", ".jd_txt.address"]:
        try:
            el = await card.query_selector(sel)
            if el:
                lead.address = clean(await el.inner_text())
                break
        except Exception:
            pass

    # Rating
    try:
        el = await card.query_selector(".green-box, .rating-box, span.rateCount")
        if el:
            lead.rating = clean(await el.inner_text())
    except Exception:
        pass

    # Reviews
    try:
        el = await card.query_selector(".ratingCount, .reviewCount")
        if el:
            t = clean(await el.inner_text())
            lead.reviews = re.sub(r"[^\d,]", "", t)
    except Exception:
        pass

    # Listing URL
    try:
        el = await card.query_selector("a.store-name, a.resultbox_title_anchor, h2 a")
        if el:
            href = await el.get_attribute("href") or ""
            if href:
                lead.listing_url = urljoin(JD_BASE, href)
    except Exception:
        pass

    return lead
