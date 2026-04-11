# ============================================================
#  utils.py  —  Shared helpers for all LeadScout scrapers
# ============================================================
import asyncio, re, time, random, logging, os, csv, hashlib
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Tuple
from urllib.parse import urljoin, urlparse
import requests
import httpx
from bs4 import BeautifulSoup

log = logging.getLogger("LeadScout")
ASYNC_REQUEST_TIMEOUT = max(5, min(10, int(os.getenv("LEADSCOUT_REQUEST_TIMEOUT_SECONDS", "8"))))
REQUEST_DELAY_MIN = max(0.5, min(2.0, int(os.getenv("LEADSCOUT_REQUEST_DELAY_MIN_MS", "500")) / 1000))
REQUEST_DELAY_MAX = max(REQUEST_DELAY_MIN, min(2.0, int(os.getenv("LEADSCOUT_REQUEST_DELAY_MAX_MS", "2000")) / 1000))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1",
]

SOCIAL_DOMAINS = [
    "facebook.com", "fb.com", "instagram.com", "twitter.com", "x.com",
    "youtube.com", "linkedin.com", "wa.me", "whatsapp.com",
    "t.me", "telegram.me", "snapchat.com", "pinterest.com",
    "justdial.com", "indiamart.com", "google.com",
]

# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class Lead:
    # Core identity
    name:           str = ""
    category:       str = ""
    # Contact
    phone:          str = ""
    email:          str = ""
    # Location
    address:        str = ""
    city:           str = ""
    # Web presence
    website:        str = ""
    website_status: str = ""   # no_website | social_only | minimal | full | unreachable
    # Engagement signals
    rating:         str = ""
    reviews:        str = ""
    # Meta
    source:         str = ""   # google_maps | justdial | indiamart
    listing_url:    str = ""
    query:          str = ""
    scraped_at:     str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M"))

CSV_COLUMNS = [
    "name", "category", "phone", "email",
    "address", "city",
    "website", "website_status",
    "rating", "reviews",
    "source", "listing_url", "query", "scraped_at",
]

EMAIL_RE = re.compile(r"(?i)\b[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}\b")
CONTACT_LINK_HINTS = ("contact", "about", "reach", "support", "connect", "get-in-touch")

# ── Text helpers ──────────────────────────────────────────────────────────────

def clean(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()

def clean_phone(raw: str) -> str:
    digits = re.sub(r"[^\d+\-() ]", "", raw or "")
    return digits[:20].strip()


def find_emails(text: str) -> List[str]:
    seen = []
    for match in EMAIL_RE.findall(text or ""):
        email = clean(match).strip(".,;:()[]{}<>").lower()
        if email and email not in seen:
            seen.append(email)
    return seen

def rua() -> str:
    return random.choice(USER_AGENTS)

def delay(min_s=2.0, max_s=4.5):
    time.sleep(random.uniform(min_s, max_s))


def make_runtime_lead_key(lead) -> str:
    name = clean(getattr(lead, "name", "") if hasattr(lead, "name") else lead.get("name") or lead.get("Name") or "")
    phone = clean_phone(getattr(lead, "phone", "") if hasattr(lead, "phone") else lead.get("phone") or lead.get("Phone") or "")
    address = clean(getattr(lead, "address", "") if hasattr(lead, "address") else lead.get("address") or lead.get("Address") or "")
    base = f"{name.lower()}|{phone}|{address.lower()}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def has_strong_business_metadata(lead: Lead) -> bool:
    return bool(
        clean(lead.address)
        or clean(lead.category)
        or clean(lead.rating)
        or clean(lead.reviews)
        or clean(lead.website_status) in {"minimal", "full"}
    )


def has_meaningful_contact_value(lead: Lead) -> bool:
    return bool(
        clean_phone(lead.phone)
        or clean(lead.email)
        or clean(lead.website)
        or has_strong_business_metadata(lead)
    )


def is_usable_lead(lead: Lead) -> bool:
    name = bool(clean(lead.name))
    phone = bool(clean_phone(lead.phone))
    email = bool(clean(lead.email))
    website = bool(clean(lead.website))
    listing_url = bool(clean(lead.listing_url))
    city = bool(clean(lead.city))
    strong_meta = has_strong_business_metadata(lead)

    if name and (city or listing_url):
        return True

    if phone or email or website or listing_url:
        return True
    if strong_meta and not looks_weak_listing(lead):
        return True
    return False


def looks_weak_listing(lead: Lead) -> bool:
    name = clean(lead.name).lower()
    weak_name_patterns = [
        "suggest an edit",
        "claim this business",
        "add missing information",
        "temporarily closed",
        "permanently closed",
        "update business",
    ]
    if any(pattern in name for pattern in weak_name_patterns):
        return True
    if not clean(lead.phone) and not clean(lead.website) and not clean(lead.address) and not clean(lead.rating):
        return True
    return False


def lead_quality_score(lead: Lead) -> int:
    score = 0
    name = bool(clean(lead.name))
    phone = bool(clean_phone(lead.phone))
    email = bool(clean(lead.email))
    website = bool(clean(lead.website))
    listing_url = bool(clean(lead.listing_url))
    city = bool(clean(lead.city))
    category = bool(clean(lead.category))
    location_meta = bool(clean(lead.address) or clean(lead.city))
    strong_meta = has_strong_business_metadata(lead)

    if phone:
        score += 5
    if email:
        score += 4
    if website:
        score += 4
    if listing_url:
        score += 3
    if category:
        score += 2
    if location_meta:
        score += 2
    if strong_meta:
        score += 2
    if name and city:
        score += 2
    if lead.source == "justdial":
        score += 2
    if lead.source == "indiamart":
        score += 2
    if lead.source == "google_maps" and strong_meta:
        score += 1
    if email and website:
        score += 2
    if website and strong_meta:
        score += 1
    if not phone and not email and not website and not listing_url:
        score -= 3
    if looks_weak_listing(lead) and not (phone or email or website or listing_url):
        score -= 3
    if not strong_meta and not (phone or email or website or listing_url):
        score -= 2
    return score


def lead_quality_class(score: int) -> str:
    if score >= 8:
        return "high_quality"
    if score >= 3:
        return "medium_quality"
    return "low_quality"


def should_keep_quality(lead: Lead, plan: str, role: str = "user") -> bool:
    normalized_role = clean(role).lower() or "user"
    normalized_plan = clean(plan).lower() or "starter"
    if normalized_role == "admin" or normalized_plan in {"growth", "team"}:
        return is_usable_lead(lead)
    if normalized_plan == "pro":
        return is_usable_lead(lead)
    return True


def fallback_keep_quality(lead: Lead) -> bool:
    if not is_usable_lead(lead):
        return False
    if clean(lead.name) and (clean(lead.city) or clean(lead.listing_url)):
        return True
    if looks_weak_listing(lead) and not (
        clean_phone(lead.phone) or clean(lead.email) or clean(lead.website) or clean(lead.listing_url)
    ):
        return False
    return True


def lead_sort_key(lead: Lead) -> Tuple[int, int, int, int, int, int]:
    score = lead_quality_score(lead)
    return (
        score,
        1 if clean_phone(lead.phone) else 0,
        1 if clean(lead.email) else 0,
        1 if clean(lead.website) else 0,
        1 if clean(lead.listing_url) else 0,
        1 if has_strong_business_metadata(lead) else 0,
    )


def choose_richer_lead(existing: Lead, incoming: Lead) -> Lead:
    if lead_sort_key(incoming) > lead_sort_key(existing):
        richer = incoming
        weaker = existing
    else:
        richer = existing
        weaker = incoming
    source_labels = sorted(set(filter(None, (richer.source or "").split("+") + (weaker.source or "").split("+"))))
    richer.source = "+".join(source_labels)
    for f in ["phone", "email", "address", "website", "rating", "reviews", "listing_url", "category", "city", "website_status"]:
        if not getattr(richer, f) and getattr(weaker, f):
            setattr(richer, f, getattr(weaker, f))
    return richer


def rank_and_deduplicate_leads(
    leads: List[Lead],
    plan: str,
    role: str = "user",
    limit: Optional[int] = None,
    allow_fallback: bool = False,
    fallback_limit: Optional[int] = None,
) -> List[Lead]:
    merged = {}
    for lead in leads:
        k = _key(lead.name)
        if not k:
            continue
        if k not in merged:
            merged[k] = lead
        else:
            merged[k] = choose_richer_lead(merged[k], lead)
    ranked = sorted(merged.values(), key=lead_sort_key, reverse=True)
    filtered = [lead for lead in ranked if should_keep_quality(lead, plan, role)]
    if not filtered and allow_fallback:
        fallback_ranked = [lead for lead in ranked if fallback_keep_quality(lead)]
        cap = fallback_limit if fallback_limit is not None else limit
        if cap is not None:
            return fallback_ranked[: max(0, int(cap))]
        return fallback_ranked
    if limit is not None:
        return filtered[: max(0, int(limit))]
    return filtered


def _classify_website_response(input_url: str, final_url: str, html: str) -> str:
    size_kb  = len((html or "").encode()) / 1024
    soup     = BeautifulSoup(html or "", "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    words    = len(soup.get_text(" ").split())
    base     = final_url.split("/")[2] if "/" in final_url else ""
    nav_links = {
        a["href"] for a in soup.find_all("a", href=True)
        if a["href"].startswith("/") or base in a.get("href", "")
    }

    fw_markers = [
        "react", "angular", "vue", "next.js", "gatsby", "nuxt",
        "wordpress", "shopify", "wix", "squarespace", "webflow",
        "app.js", "chunk.", "bundle.", "__NEXT_DATA__",
    ]
    lowered = (html or "").lower()
    for marker in fw_markers:
        if marker in lowered:
            return "full"

    if size_kb > 40 or words > 600 or len(nav_links) > 8:
        return "full"
    if size_kb < 8 or words < 150 or len(nav_links) < 4:
        return "minimal"
    return "minimal"

# ── Website quality checker ───────────────────────────────────────────────────

def check_website(url: str, timeout: int = 10) -> str:
    """
    Returns one of:
      no_website   — blank / N/A / missing
      social_only  — only Facebook / Instagram / WhatsApp page
      minimal      — real domain but thin content (static 1-pager)
      full         — proper multi-page website
      unreachable  — domain present but server error / timeout
    """
    url = (url or "").strip()
    if not url or url.lower() in ("", "n/a", "-", "none", "na", "not listed"):
        return "no_website"

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    for dom in SOCIAL_DOMAINS:
        if dom in url.lower():
            return "social_only"

    try:
        r = requests.get(
            url, timeout=timeout, allow_redirects=True,
            headers={"User-Agent": rua(),
                     "Accept": "text/html,application/xhtml+xml"},
        )
        if r.status_code >= 400:
            return "unreachable"
        return _classify_website_response(url, str(r.url), r.text)

    except requests.exceptions.Timeout:
        return "unreachable"
    except Exception:
        return "unreachable"


async def async_check_website(
    url: str,
    client: httpx.AsyncClient,
    timeout: int = ASYNC_REQUEST_TIMEOUT,
    retries: int = 3,
) -> str:
    url = (url or "").strip()
    if not url or url.lower() in ("", "n/a", "-", "none", "na", "not listed"):
        return "no_website"

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    for dom in SOCIAL_DOMAINS:
        if dom in url.lower():
            return "social_only"

    for attempt in range(retries):
        try:
            await asyncio.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
            r = await client.get(
                url,
                timeout=timeout,
                follow_redirects=True,
                headers={
                    "User-Agent": rua(),
                    "Accept": "text/html,application/xhtml+xml",
                },
            )
            if r.status_code >= 400:
                return "unreachable"
            return _classify_website_response(url, str(r.url), r.text)
        except httpx.TimeoutException:
            if attempt == retries - 1:
                return "unreachable"
        except Exception:
            if attempt == retries - 1:
                return "unreachable"
        await asyncio.sleep(0.25 * (attempt + 1))
    return "unreachable"


def _extract_email_from_html(base_url: str, html: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")

    mailtos = []
    for tag in soup.find_all("a", href=True):
        href = (tag.get("href") or "").strip()
        if href.lower().startswith("mailto:"):
            mailtos.extend(find_emails(href.replace("mailto:", "", 1)))
    if mailtos:
        return mailtos[0]

    emails = find_emails(soup.get_text(" ", strip=True))
    return emails[0] if emails else ""


def _candidate_contact_links(base_url: str, html: str) -> List[str]:
    soup = BeautifulSoup(html or "", "lxml")
    parsed_base = urlparse(base_url)
    links = []
    for tag in soup.find_all("a", href=True):
        href = (tag.get("href") or "").strip()
        label = clean(tag.get_text(" ", strip=True)).lower()
        haystack = f"{href.lower()} {label}"
        if not any(token in haystack for token in CONTACT_LINK_HINTS):
            continue
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        if parsed.netloc and parsed_base.netloc and parsed.netloc != parsed_base.netloc:
            continue
        if absolute not in links:
            links.append(absolute)
    return links[:2]


async def async_extract_website_email(
    url: str,
    client: httpx.AsyncClient,
    timeout: int = ASYNC_REQUEST_TIMEOUT,
    retries: int = 2,
) -> tuple[str, str]:
    url = (url or "").strip()
    if not url or url.lower() in ("", "n/a", "-", "none", "na", "not listed"):
        return "", "no_website"

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    for dom in SOCIAL_DOMAINS:
        if dom in url.lower():
            return "", "social_only"

    last_error = ""
    for attempt in range(retries):
        try:
            await asyncio.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
            response = await client.get(
                url,
                timeout=timeout,
                follow_redirects=True,
                headers={
                    "User-Agent": rua(),
                    "Accept": "text/html,application/xhtml+xml",
                },
            )
            if response.status_code >= 400:
                return "", "website_unreachable"

            homepage_email = _extract_email_from_html(str(response.url), response.text)
            if homepage_email:
                return homepage_email, "homepage"

            for contact_url in _candidate_contact_links(str(response.url), response.text):
                try:
                    await asyncio.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
                    contact_response = await client.get(
                        contact_url,
                        timeout=timeout,
                        follow_redirects=True,
                        headers={
                            "User-Agent": rua(),
                            "Accept": "text/html,application/xhtml+xml",
                        },
                    )
                    if contact_response.status_code >= 400:
                        continue
                    contact_email = _extract_email_from_html(str(contact_response.url), contact_response.text)
                    if contact_email:
                        return contact_email, "contact_page"
                except Exception:
                    continue

            return "", "not_found"
        except httpx.TimeoutException:
            last_error = "timeout"
        except Exception as exc:
            last_error = type(exc).__name__
        await asyncio.sleep(0.25 * (attempt + 1))
    return "", f"failed:{last_error or 'unknown'}"


async def async_enrich_websites(
    leads: List[Lead],
    timeout: int = ASYNC_REQUEST_TIMEOUT,
    concurrency: int = 8,
    retries: int = 3,
) -> List[Lead]:
    if not leads:
        return leads

    semaphore = asyncio.Semaphore(max(1, concurrency))
    client_timeout = httpx.Timeout(timeout)

    async with httpx.AsyncClient(timeout=client_timeout, verify=False) as client:
        async def _check(lead: Lead):
            async with semaphore:
                note = "skipped"
                if not lead.website_status:
                    lead.website_status = await async_check_website(
                        lead.website,
                        client=client,
                        timeout=timeout,
                        retries=retries,
                    )
                if not lead.website:
                    note = "no_website"
                elif lead.email:
                    note = "email_present"
                elif lead.website_status in ("no_website", "social_only", "unreachable"):
                    note = "enrichment_skipped"
                else:
                    email, source = await async_extract_website_email(
                        lead.website,
                        client=client,
                        timeout=timeout,
                        retries=max(1, retries - 1),
                    )
                    if email:
                        lead.email = email
                        note = f"email_found:{source}"
                    else:
                        note = source or "not_found"
                log.info(
                    "Enrichment lead=%s source=%s phone_found=%s email_found=%s website=%s website_status=%s note=%s",
                    clean(lead.name)[:80],
                    lead.source or "-",
                    bool(clean_phone(lead.phone)),
                    bool(clean(lead.email)),
                    bool(clean(lead.website)),
                    lead.website_status or "",
                    note,
                )

        await asyncio.gather(*[_check(lead) for lead in leads])

    return leads


def should_keep(lead: Lead, mode: str) -> bool:
    """Filter by website status based on config.WEBSITE_FILTER."""
    if is_usable_lead(lead):
        return True
    if mode == "all":
        return True
    if mode == "no_website":
        return lead.website_status in ("no_website", "social_only")
    # "minimal" (default)
    if lead.website_status in ("no_website", "social_only", "minimal", "unreachable"):
        return True
    if clean(lead.email) and clean(lead.website):
        return True
    if clean(lead.website) and has_strong_business_metadata(lead):
        return True
    return False

# ── Deduplication ─────────────────────────────────────────────────────────────

def _key(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())[:30]

def deduplicate(leads: List[Lead]) -> List[Lead]:
    """Merge duplicates across sources, keeping the richest entry."""
    seen = {}
    for lead in leads:
        k = _key(lead.name)
        if not k:
            continue
        if k not in seen:
            seen[k] = lead
        else:
            seen[k] = choose_richer_lead(seen[k], lead)
    return list(seen.values())

# ── CSV output ────────────────────────────────────────────────────────────────

def save_csv(leads: List[Lead], path: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for lead in leads:
            row = asdict(lead)
            writer.writerow({col: row.get(col, "") for col in CSV_COLUMNS})
    log.info(f"  Saved {len(leads)} leads → {path}")

def output_path(niche: str, suffix: str, output_dir: str = "output") -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    name = f"{niche}_{suffix}_{ts}.csv" if suffix else f"{niche}_{ts}.csv"
    return os.path.join(output_dir, name)
