# ============================================================
#  scraper_maps.py  —  Google Maps Lead Scraper
#  Finds businesses with no / minimal website presence
# ============================================================
import asyncio, re, logging
from typing import List
from urllib.parse import quote_plus
from utils import Lead, clean, clean_phone, rua, delay

log = logging.getLogger("LeadScout.Maps")


async def scrape(query: str, city: str, max_results: int, proxy_dict=None) -> List[Lead]:
    """
    Scrape Google Maps for `query` in `city`.
    Returns a list of Lead objects (website_status NOT yet set — done centrally).
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.error("Run: pip install playwright && playwright install chromium")
        return []

    leads    = []
    seen     = set()
    search   = f"{query} in {city}"
    maps_url = f"https://www.google.com/maps/search/{quote_plus(search)}"
    log.info(f"  [Maps] {search}")

    # Build launch args (optionally route through proxy)
    launch_kwargs = dict(
        headless=True,
        args=[
            "--no-sandbox", "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
        ],
    )
    # Note: proxies not passed to browser — Playwright handles its own routing

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(**launch_kwargs)
        ctx = await browser.new_context(
            user_agent=rua(),
            viewport={"width": 1366, "height": 768},
            locale="en-IN",
        )
        # Anti-detection
        await ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )
        page = await ctx.new_page()

        try:
            await page.goto(maps_url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)

            # Dismiss consent / cookies
            for txt in ["Accept all", "Accept All", "I agree", "Agree and continue"]:
                try:
                    await page.click(f'button:has-text("{txt}")', timeout=2000)
                    await asyncio.sleep(1)
                    break
                except Exception:
                    pass

            scroll_attempts = 0

            while len(leads) < max_results and scroll_attempts < 20:
                scroll_attempts += 1

                # Collect listing links from the sidebar feed
                cards = await page.query_selector_all('a[href*="/maps/place/"]')

                for card in cards:
                    if len(leads) >= max_results:
                        break
                    try:
                        label = await card.get_attribute("aria-label") or ""
                        if not label or label in seen:
                            continue
                        seen.add(label)

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

                        # Phone
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

                        # Website
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

                        # Address
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

                        # Rating
                        try:
                            el = await page.query_selector('div.fontDisplayLarge')
                            if el:
                                lead.rating = clean(await el.inner_text())
                        except Exception:
                            pass

                        # Review count
                        try:
                            el = await page.query_selector('button[aria-label*="reviews"]')
                            if el:
                                txt = await el.inner_text()
                                lead.reviews = re.sub(r"[^\d,]", "", txt)
                        except Exception:
                            pass

                        # Business category (refined)
                        try:
                            el = await page.query_selector('button.DkEaL')
                            if el:
                                lead.category = clean(await el.inner_text())
                        except Exception:
                            pass

                        leads.append(lead)
                        log.info(f"    [Maps] ✓ {lead.name[:50]}")

                    except Exception as e:
                        log.debug(f"    [Maps] card error: {e}")

                # Scroll the sidebar to load more results
                try:
                    feed = page.locator('div[role="feed"]')
                    await feed.evaluate("el => el.scrollBy(0, 1400)")
                    await asyncio.sleep(1.8)
                except Exception:
                    await page.evaluate("window.scrollBy(0, 1400)")
                    await asyncio.sleep(1.8)

        except Exception as e:
            log.error(f"  [Maps] Page error: {e}")
        finally:
            await browser.close()

    log.info(f"  [Maps] → {len(leads)} leads for '{query}'")
    return leads
