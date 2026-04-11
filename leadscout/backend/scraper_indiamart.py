# ============================================================
#  scraper_indiamart.py  —  IndiaMart Directory Scraper
# ============================================================
import asyncio, json, re, logging, random, os
from typing import Callable, List, Optional
from urllib.parse import quote_plus
import httpx
from bs4 import BeautifulSoup
from utils import Lead, clean, clean_phone, rua

log = logging.getLogger("LeadScout.IndiaMart")
REQUEST_TIMEOUT = max(5, min(10, int(os.getenv("LEADSCOUT_REQUEST_TIMEOUT_SECONDS", "8"))))
REQUEST_DELAY_MIN = max(0.5, min(2.0, int(os.getenv("LEADSCOUT_REQUEST_DELAY_MIN_MS", "500")) / 1000))
REQUEST_DELAY_MAX = max(REQUEST_DELAY_MIN, min(2.0, int(os.getenv("LEADSCOUT_REQUEST_DELAY_MAX_MS", "2000")) / 1000))

IM_BASE    = "https://dir.indiamart.com"
IM_SEARCH  = IM_BASE + "/search.mp"

_HEADERS = lambda: {
    "User-Agent": rua(),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": IM_BASE + "/",
}


async def _fetch_with_retry(client: httpx.AsyncClient, url: str, retries: int = 3) -> Optional[str]:
    for attempt in range(retries):
        try:
            await asyncio.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
            r = await client.get(url, headers=_HEADERS())
            if r.status_code != 200 or not (r.text or "").strip():
                raise RuntimeError(f"blocked_or_empty status={r.status_code}")
            return r.text
        except Exception as e:
            if attempt == retries - 1:
                log.warning(f"  [IM] Request failed/blocked: {e}")
                return None
            await asyncio.sleep(0.35 * (attempt + 1))
    return None


async def scrape(
    query: str,
    city: str,
    max_results: int,
    proxy_dict=None,
    resume_state: Optional[dict] = None,
    progress_callback: Optional[Callable[[dict], None]] = None,
) -> List[Lead]:
    """Async scraper — no browser needed for IndiaMart."""
    leads  = []
    seen   = set()
    resume_state = resume_state or {}
    page = max(1, int(resume_state.get("page", 1) or 1))
    consecutive_failures = 0
    log.info(f"  [IM] {query} in {city}")

    proxy_url = (proxy_dict or {}).get("http") if proxy_dict else None
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(REQUEST_TIMEOUT),
        follow_redirects=True,
        verify=False,
        proxy=proxy_url,
    ) as client:
        while len(leads) < max_results:
            if progress_callback:
                progress_callback({
                    "platform": "IndiaMart",
                    "page": page,
                    "phase": "fetch_page",
                    "city": city,
                    "query": query,
                })
            url = (
                f"{IM_SEARCH}?ss={quote_plus(query)}"
                f"&cq={quote_plus(city)}&biz=&src=ss-dir"
                + (f"&page={page}" if page > 1 else "")
            )

            html = await _fetch_with_retry(client, url)
            if not html:
                consecutive_failures += 1
                log.info(f"  [IM] Skipping page {page} after retries")
                page += 1
                if consecutive_failures >= 3:
                    break
                continue
            consecutive_failures = 0

            soup = BeautifulSoup(html, "lxml")

            # ── Strategy 1: JSON-LD structured data ─────────────────────────────
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    data  = json.loads(script.string or "[]")
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        if len(leads) >= max_results:
                            break
                        itype = item.get("@type", "")
                        if itype not in ("LocalBusiness", "Organization", "Store",
                                        "ProfessionalService", "Product"):
                            continue
                        name = clean(item.get("name", ""))
                        if not name or name in seen:
                            continue
                        seen.add(name)
                        addr = item.get("address", {})
                        if isinstance(addr, dict):
                            addr_str = clean(" ".join(filter(None, [
                                addr.get("streetAddress", ""),
                                addr.get("addressLocality", ""),
                                addr.get("addressRegion", ""),
                                addr.get("postalCode", ""),
                            ])))
                        else:
                            addr_str = clean(str(addr))

                        lead = Lead(
                            name=name,
                            category=query,
                            phone=clean_phone(str(item.get("telephone", ""))),
                            address=addr_str,
                            city=city,
                            website=item.get("url", ""),
                            rating=str(item.get("aggregateRating", {}).get("ratingValue", "")),
                            reviews=str(item.get("aggregateRating", {}).get("reviewCount", "")),
                            source="indiamart",
                            query=query,
                        )
                        leads.append(lead)
                        log.info(f"    [IM] ✓ {lead.name[:50]}")
                except Exception:
                    pass

            # ── Strategy 2: DOM card parsing ─────────────────────────────────────
            card_selectors = [
                ".companyDiv", ".dir-comp-det",
                ".lstng-det-bx", "article.company",
                "[class*='listing']", "[class*='company']",
            ]
            cards = []
            for sel in card_selectors:
                cards = soup.select(sel)
                if cards:
                    break

            for card in cards:
                if len(leads) >= max_results:
                    break
                lead = _parse_card(card, city, query)
                if lead and lead.name and lead.name not in seen:
                    seen.add(lead.name)
                    leads.append(lead)
                    log.info(f"    [IM] ✓ {lead.name[:50]}")

            # ── Pagination ────────────────────────────────────────────────────────
            nxt = soup.select_one("a[rel='next'], a.next, .pagination .next, li.next a")
            if not nxt or len(leads) >= max_results:
                break

            page += 1
            await asyncio.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

    log.info(f"  [IM] → {len(leads)} leads for '{query}'")
    return leads


def _parse_card(card, city: str, query: str) -> Optional[Lead]:
    lead = Lead(source="indiamart", city=city, category=query, query=query)

    # Name
    for sel in ["h2", "h3", ".comp-name", ".company-name",
                "[class*='companyname']", "[class*='name']"]:
        el = card.select_one(sel)
        if el and el.get_text(strip=True):
            lead.name = clean(el.get_text(" "))
            break

    if not lead.name:
        return None

    # Phone
    for sel in [".phone", "[class*='phone']", "a[href^='tel:']"]:
        el = card.select_one(sel)
        if el:
            raw = el.get("href", "") or el.get_text(strip=True)
            lead.phone = clean_phone(raw.replace("tel:", ""))
            if lead.phone:
                break

    # Address
    for sel in [".address", "[class*='address']", ".location", ".locLink"]:
        el = card.select_one(sel)
        if el:
            lead.address = clean(el.get_text(" "))
            break

    # Website (any external link)
    for a in card.select("a[href]"):
        href = a.get("href", "")
        if href.startswith("http") and "indiamart.com" not in href:
            lead.website = href
            break

    # Rating
    el = card.select_one(".rating, [class*='rating'] span, .starRating")
    if el:
        lead.rating = el.get_text(strip=True)

    # Listing URL
    for a in card.select("a[href*='indiamart.com']"):
        lead.listing_url = a.get("href", "")
        break

    return lead
