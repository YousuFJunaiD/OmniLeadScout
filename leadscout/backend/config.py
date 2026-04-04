# ============================================================
#  config.py  —  LeadScout Configuration
#  ⭐ THIS IS THE FILE YOU EDIT
# ============================================================

# ── WHAT TO SEARCH ──────────────────────────────────────────
# Output filename prefix (timestamp + .csv added automatically)
NICHE = "leads_mumbai"

# Business categories to search
# Each query runs across ALL enabled platforms
SEARCH_QUERIES = [
    "schools",
    "restaurants",
    "clinics",
    "hotels",
    "gyms",
    "salons",
    "pharmacies",
    "hardware stores",
    "coaching centres",
    "sweet shops",
]

# City to search in
CITY = "Mumbai"

# ── PLATFORMS TO SCRAPE ─────────────────────────────────────
ENABLE_GOOGLE_MAPS = True
ENABLE_JUSTDIAL    = True
ENABLE_INDIAMART   = True

# Max results per query per platform
# Total max leads = MAX_PER_QUERY × 3 platforms × len(SEARCH_QUERIES)
MAX_PER_QUERY = 25

# ── WEBSITE FILTER ──────────────────────────────────────────
# Which businesses to keep in final output:
#
#   "no_website"  → only businesses with zero web presence        ← best leads
#   "minimal"     → no website OR thin/static/1-page site         ← recommended
#   "all"         → everyone (no filter, largest output)
#
WEBSITE_FILTER = "minimal"

# ── BROWSER SETTINGS ────────────────────────────────────────
HEADLESS = True                # False = see the browser (useful for debugging)
DELAY_BETWEEN_LISTINGS = 2000  # ms between clicking listings (Google Maps)
SCROLL_PAUSE = 1500            # ms between scroll steps

# ── PROXY SETTINGS ──────────────────────────────────────────
# Auto-fetches hundreds of verified proxies from:
#   https://github.com/iplocate/free-proxy-list  (updated every 30 min)
#
USE_PROXY_POOL = True          # Recommended — keeps you from getting blocked

# Which proxy types to fetch (all = maximum pool size)
PROXY_PROTOCOLS = ["http", "https", "socks4", "socks5"]

# Max response time for proxy health check (seconds)
PROXY_TEST_TIMEOUT = 8

# Maximum parallel threads for testing proxies at startup
PROXY_TEST_WORKERS = 120

# Retire a proxy after this many consecutive failures
PROXY_MAX_FAILURES = 3

# Optional: add your own premium proxies here (used alongside the pool)
# Format: "http://user:pass@ip:port"  or  "http://ip:port"
EXTRA_PROXIES = []

# ── OUTPUT ──────────────────────────────────────────────────
OUTPUT_DIR = "output"          # folder where CSVs are saved

# Save separate CSVs per platform in addition to the merged file
SAVE_PER_PLATFORM = True

# ── ADVANCED ────────────────────────────────────────────────
# Website checker: reject websites faster than these thresholds
WEB_CHECK_TIMEOUT   = 10       # seconds
WEB_CHECK_MAX_SIZE  = 8        # KB — sites under this are "minimal"
WEB_CHECK_MIN_WORDS = 150      # word count — under this = minimal
WEB_CHECK_MIN_LINKS = 4        # nav links — under this = minimal
