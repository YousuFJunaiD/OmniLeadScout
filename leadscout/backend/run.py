#!/usr/bin/env python3
# ============================================================
#  run.py  —  LeadScout Orchestrator
#  Runs all three scrapers, checks websites, merges results
#
#  Usage:
#    python run.py
#    python run.py --city Delhi --queries "schools,gyms" --max 30
#    python run.py --filter no_website --no-proxy
# ============================================================
import asyncio, logging, argparse, time, random
import config
from utils import (
    Lead, save_csv, output_path, check_website,
    should_keep, deduplicate
)
import scraper_maps
import scraper_justdial
import scraper_indiamart

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("LeadScout")

# ── Helpers ───────────────────────────────────────────────────────────────────

def _section(title: str):
    log.info("")
    log.info("=" * 58)
    log.info(f"  {title}")
    log.info("=" * 58)

def _enrich(leads, cfg):
    """Check every lead's website and classify it."""
    log.info(f"\n  Checking website presence for {len(leads)} leads…")
    for i, lead in enumerate(leads, 1):
        if not lead.website_status:
            lead.website_status = check_website(
                lead.website,
                timeout=cfg.WEB_CHECK_TIMEOUT,
            )
        pct = i / len(leads) * 100
        log.info(
            f"    [{i:>4}/{len(leads)}] {lead.name[:35]:<35} "
            f"website={lead.website_status}"
        )
        time.sleep(random.uniform(0.4, 1.0))
    return leads

# ── Main flow ─────────────────────────────────────────────────────────────────

async def run(city, queries, max_per, website_filter, use_proxy, cfg):
    all_leads = []

    # ── Proxy pool ────────────────────────────────────────────────────────────
    proxy_pool = None
    if use_proxy:
        _section("PROXY POOL — Loading")
        from proxy_manager import ProxyPool
        proxy_pool = ProxyPool(
            protocols=cfg.PROXY_PROTOCOLS,
            test_timeout=cfg.PROXY_TEST_TIMEOUT,
            test_workers=cfg.PROXY_TEST_WORKERS,
            max_failures=cfg.PROXY_MAX_FAILURES,
            extra_proxies=cfg.EXTRA_PROXIES,
            verbose=True,
        )
        proxy_pool.load()
        s = proxy_pool.stats()
        log.info(
            f"\n  Pool ready — {s['live']} live proxies | "
            f"fastest {s['fastest_ms']}ms | {s['by_protocol']}"
        )

    def _get_proxy():
        if proxy_pool:
            return proxy_pool.get()
        return None

    def _bad(p):
        if proxy_pool and p:
            proxy_pool.bad(p)

    def _good(p):
        if proxy_pool and p:
            proxy_pool.ok(p)

    # ── Per-query scraping ────────────────────────────────────────────────────
    maps_leads = []
    jd_leads   = []
    im_leads   = []

    for qi, query in enumerate(queries, 1):
        _section(f"Query {qi}/{len(queries)}: '{query}' in {city}")

        # Google Maps
        if cfg.ENABLE_GOOGLE_MAPS:
            log.info("\n  [1/3] Google Maps")
            proxy = _get_proxy()
            try:
                results = await scraper_maps.scrape(query, city, max_per, proxy)
                _good(proxy)
            except Exception as e:
                _bad(proxy)
                log.error(f"  [Maps] Error: {e}")
                results = []
            maps_leads.extend(results)
            log.info(f"  [Maps] {len(results)} leads this query")
            await asyncio.sleep(random.uniform(3, 6))

        # JustDial
        if cfg.ENABLE_JUSTDIAL:
            log.info("\n  [2/3] JustDial")
            proxy = _get_proxy()
            try:
                results = await scraper_justdial.scrape(query, city, max_per, proxy)
                _good(proxy)
            except Exception as e:
                _bad(proxy)
                log.error(f"  [JD] Error: {e}")
                results = []
            jd_leads.extend(results)
            log.info(f"  [JD] {len(results)} leads this query")
            await asyncio.sleep(random.uniform(3, 6))

        # IndiaMart
        if cfg.ENABLE_INDIAMART:
            log.info("\n  [3/3] IndiaMart")
            proxy = _get_proxy()
            try:
                results = scraper_indiamart.scrape(query, city, max_per, proxy)
                _good(proxy)
            except Exception as e:
                _bad(proxy)
                log.error(f"  [IM] Error: {e}")
                results = []
            im_leads.extend(results)
            log.info(f"  [IM] {len(results)} leads this query")

        if qi < len(queries):
            wait = random.uniform(5, 10)
            log.info(f"\n  Cooling down {wait:.0f}s before next query…")
            await asyncio.sleep(wait)

    # ── Save per-platform CSVs ────────────────────────────────────────────────
    if cfg.SAVE_PER_PLATFORM:
        _section("SAVING PER-PLATFORM CSVs")
        for tag, bucket in [("google_maps", maps_leads),
                             ("justdial", jd_leads),
                             ("indiamart", im_leads)]:
            if bucket:
                path = output_path(cfg.NICHE, tag, cfg.OUTPUT_DIR)
                save_csv(bucket, path)

    # ── Website check + filter + merge ────────────────────────────────────────
    all_leads = maps_leads + jd_leads + im_leads
    _section(f"WEBSITE CHECK  ({len(all_leads)} total leads)")
    all_leads = _enrich(all_leads, cfg)

    _section("FILTER + DEDUPLICATE")
    filtered = [l for l in all_leads if should_keep(l, website_filter)]
    log.info(f"  After filter ({website_filter}): {len(filtered)}/{len(all_leads)}")

    deduped = deduplicate(filtered)
    log.info(f"  After dedup: {len(deduped)} unique leads")

    # ── Final merged CSV ──────────────────────────────────────────────────────
    merged_path = output_path(cfg.NICHE, "merged", cfg.OUTPUT_DIR)
    save_csv(deduped, merged_path)

    # ── Summary ───────────────────────────────────────────────────────────────
    _section("SUMMARY")
    status_counts = {}
    for l in deduped:
        status_counts[l.website_status] = status_counts.get(l.website_status, 0) + 1
    for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
        bar = "█" * min(count, 40)
        log.info(f"  {status:<15}  {count:>4}  {bar}")
    log.info(f"\n  Total warm leads  : {len(deduped)}")
    log.info(f"  Output            : {merged_path}")
    log.info("")

    if proxy_pool:
        s = proxy_pool.stats()
        log.info(f"  Proxy pool final  : {s['live']} live / {s['total']} total")

    return deduped


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LeadScout — multi-source lead scraper")
    parser.add_argument("--city",    default=None,  help="Override config.CITY")
    parser.add_argument("--queries", default=None,  help="Comma-separated queries (overrides config)")
    parser.add_argument("--max",     default=None,  type=int, help="Max results per query per platform")
    parser.add_argument("--filter",  default=None,
                        choices=["no_website", "minimal", "all"],
                        help="Website filter mode")
    parser.add_argument("--no-proxy", action="store_true", help="Disable proxy pool")
    args = parser.parse_args()

    city           = args.city    or config.CITY
    queries        = [q.strip() for q in args.queries.split(",")] if args.queries else config.SEARCH_QUERIES
    max_per        = args.max     or config.MAX_PER_QUERY
    website_filter = args.filter  or config.WEBSITE_FILTER
    use_proxy      = config.USE_PROXY_POOL and not args.no_proxy

    _section("LEADSCOUT — Starting")
    log.info(f"  City      : {city}")
    log.info(f"  Queries   : {', '.join(queries)}")
    log.info(f"  Max/query : {max_per} per platform")
    log.info(f"  Filter    : {website_filter}")
    log.info(f"  Platforms : "
             f"{'Maps ' if config.ENABLE_GOOGLE_MAPS else ''}"
             f"{'JustDial ' if config.ENABLE_JUSTDIAL else ''}"
             f"{'IndiaMart' if config.ENABLE_INDIAMART else ''}")
    log.info(f"  Proxies   : {'auto pool' if use_proxy else 'disabled'}")

    asyncio.run(run(city, queries, max_per, website_filter, use_proxy, config))
