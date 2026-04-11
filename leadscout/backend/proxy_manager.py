# ============================================================
#  proxy_manager.py  —  Auto-Rotating Proxy Pool
#  Source: github.com/iplocate/free-proxy-list (30-min updates)
# ============================================================
import time, random, threading, logging, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional, List, Dict
import requests

log = logging.getLogger("ProxyPool")

# Raw GitHub URLs — auto-updated every 30 minutes by the source repo
_SOURCES = {
    "http":   "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/http.txt",
    "https":  "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/https.txt",
    "socks4": "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/socks4.txt",
    "socks5": "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/protocols/socks5.txt",
}
_TEST_URL      = "http://api.iplocate.io/ip"   # free, no rate limits
_REFRESH_SECS  = 30 * 60                       # match repo cadence


@dataclass
class _Proxy:
    host: str
    port: int
    protocol: str
    speed_ms:  float = 9999.0
    failures:  int   = 0
    alive:     bool  = False
    success_count: int = 0
    failure_count: int = 0
    last_failure_reason: str = ""
    last_success_at: float = 0.0
    cooldown_until: float = 0.0
    average_latency_ms: float = 9999.0

    @property
    def url(self) -> str:
        return f"{self.protocol}://{self.host}:{self.port}"

    @property
    def as_dict(self) -> Dict[str, str]:
        return {"http": self.url, "https": self.url}

    @property
    def score(self) -> float:
        cooldown_penalty = 5000 if self.cooldown_until > time.time() else 0
        return min(self.speed_ms, self.average_latency_ms) + self.failures * 1500 + self.failure_count * 250 + cooldown_penalty


class ProxyPool:
    """
    Thread-safe rotating proxy pool.

    Usage:
        pool = ProxyPool(protocols=["http","https","socks5"])
        pool.load()                   # ~30-60s on first call
        proxy = pool.get()            # {"http": "...", "https": "..."}
        pool.bad(proxy)               # report failure
        pool.ok(proxy)                # report success
    """

    def __init__(
        self,
        protocols=None,
        test_timeout=8,
        test_workers=120,
        max_failures=3,
        max_speed_ms=6000,
        quarantine_seconds=300,
        extra_proxies=None,
        verbose=True,
    ):
        self.protocols    = protocols or list(_SOURCES.keys())
        self.test_timeout = test_timeout
        self.test_workers = test_workers
        self.max_failures = max_failures
        self.max_speed_ms = max_speed_ms
        self.quarantine_seconds = quarantine_seconds
        self.extra_proxies = extra_proxies or []
        self.verbose      = verbose

        self._all:  Dict[str, _Proxy] = {}
        self._live: List[_Proxy]      = []
        self._lock  = threading.RLock()
        self._idx   = 0
        self._ready = False

    # ── public ────────────────────────────────────────────────

    def load(self):
        """Fetch + test all proxies. Blocks until pool is ready."""
        raw   = self._fetch()
        alive = self._test(raw)
        with self._lock:
            for p in alive:
                self._all[p.url] = p
            self._sort()
        self._ready = True
        self._start_refresh()
        if self.verbose:
            s = self.stats()
            log.info(f"[Proxy] Pool ready — {s['live']} live | "
                     f"fastest {s['fastest_ms']}ms | {s['by_protocol']}")

    def get(self) -> Optional[Dict[str, str]]:
        """Return next proxy (round-robin by speed). None = no proxies available."""
        with self._lock:
            if not self._live:
                return None
            now = time.time()
            healthy = [p for p in self._live if p.cooldown_until <= now]
            pool = healthy or self._live
            self._idx = self._idx % len(pool)
            p = pool[self._idx]
            self._idx += 1
            return p.as_dict

    def bad(self, proxy: Optional[Dict]):
        """Report a proxy as failed."""
        self.report_failure(proxy, "unknown_failure")

    def report_failure(self, proxy: Optional[Dict], reason: str):
        if not proxy:
            return
        url = proxy.get("http", "")
        with self._lock:
            p = self._all.get(url)
            if p:
                p.failures += 1
                p.failure_count += 1
                p.last_failure_reason = str(reason or "unknown_failure")
                p.cooldown_until = time.time() + min(self.quarantine_seconds * max(1, p.failures), self.quarantine_seconds * 6)
                if p.failures >= self.max_failures:
                    p.alive = False
                self._sort()

    def ok(self, proxy: Optional[Dict]):
        """Report a proxy as working."""
        self.report_success(proxy)

    def report_success(self, proxy: Optional[Dict], latency_ms: Optional[float] = None):
        if not proxy:
            return
        url = proxy.get("http", "")
        with self._lock:
            p = self._all.get(url)
            if p:
                p.failures = max(0, p.failures - 1)
                p.success_count += 1
                p.last_success_at = time.time()
                p.cooldown_until = 0.0
                p.alive = True
                if latency_ms is not None:
                    latency_ms = float(latency_ms)
                    if p.average_latency_ms >= 9999:
                        p.average_latency_ms = latency_ms
                    else:
                        p.average_latency_ms = (p.average_latency_ms * 0.7) + (latency_ms * 0.3)
                    p.speed_ms = min(p.speed_ms, latency_ms)
                self._sort()

    def info(self, proxy: Optional[Dict]) -> Optional[Dict[str, object]]:
        if not proxy:
            return None
        url = proxy.get("http", "")
        with self._lock:
            p = self._all.get(url)
            if not p:
                return None
            return {
                "url": p.url,
                "protocol": p.protocol,
                "failures": p.failures,
                "success_count": p.success_count,
                "failure_count": p.failure_count,
                "last_failure_reason": p.last_failure_reason,
                "last_success_at": p.last_success_at,
                "cooldown_until": p.cooldown_until,
                "average_latency_ms": round(p.average_latency_ms if p.average_latency_ms < 9999 else p.speed_ms, 2),
                "alive": p.alive,
            }

    def stats(self) -> Dict:
        with self._lock:
            speeds = sorted(p.speed_ms for p in self._live)
            by_proto = {}
            for p in self._live:
                by_proto[p.protocol] = by_proto.get(p.protocol, 0) + 1
            return {
                "total":    len(self._all),
                "live":     len(self._live),
                "healthy":  sum(1 for p in self._live if p.cooldown_until <= time.time()),
                "quarantined": sum(1 for p in self._all.values() if p.cooldown_until > time.time()),
                "fastest_ms": round(speeds[0]) if speeds else 0,
                "median_ms":  round(speeds[len(speeds)//2]) if speeds else 0,
                "by_protocol": by_proto,
            }

    def __len__(self):
        with self._lock:
            return len(self._live)

    # ── internal ──────────────────────────────────────────────

    def _fetch(self) -> List[_Proxy]:
        proxies, seen = [], set()

        # Extra static proxies first
        for raw_url in self.extra_proxies:
            m = re.match(r"(https?|socks[45])://(?:.+@)?(\d+\.\d+\.\d+\.\d+):(\d+)", raw_url)
            if m:
                proto, host, port = m.group(1), m.group(2), int(m.group(3))
                key = f"{proto}://{host}:{port}"
                if key not in seen:
                    seen.add(key)
                    proxies.append(_Proxy(host, port, proto))

        # GitHub lists
        for proto in self.protocols:
            url = _SOURCES.get(proto)
            if not url:
                continue
            try:
                r = requests.get(url, timeout=15)
                r.raise_for_status()
                count = 0
                for line in r.text.strip().splitlines():
                    line = line.strip()
                    m = re.match(r"^(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})$", line)
                    if m:
                        host, port = m.group(1), int(m.group(2))
                        key = f"{proto}://{host}:{port}"
                        if key not in seen:
                            seen.add(key)
                            proxies.append(_Proxy(host, port, proto))
                            count += 1
                if self.verbose:
                    log.info(f"[Proxy] {proto:6s} → {count} fetched")
            except Exception as e:
                log.warning(f"[Proxy] Fetch failed ({proto}): {e}")
        return proxies

    def _test_one(self, p: _Proxy) -> _Proxy:
        try:
            t0 = time.perf_counter()
            r  = requests.get(
                _TEST_URL, proxies=p.as_dict,
                timeout=self.test_timeout,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            ms = (time.perf_counter() - t0) * 1000
            if r.status_code == 200 and ms < self.max_speed_ms:
                p.alive, p.speed_ms = True, ms
        except Exception:
            p.alive = False
        return p

    def _test(self, proxies: List[_Proxy]) -> List[_Proxy]:
        alive, done = [], 0
        if self.verbose:
            log.info(f"[Proxy] Testing {len(proxies)} proxies "
                     f"({self.test_workers} workers, {self.test_timeout}s timeout)…")
        with ThreadPoolExecutor(max_workers=self.test_workers) as ex:
            futs = {ex.submit(self._test_one, p): p for p in proxies}
            for fut in as_completed(futs):
                done += 1
                p = fut.result()
                if p.alive:
                    alive.append(p)
                if self.verbose and done % 200 == 0:
                    log.info(f"[Proxy]   {done}/{len(proxies)} tested — {len(alive)} alive")
        alive.sort(key=lambda p: p.score)
        if self.verbose:
            log.info(f"[Proxy] {len(alive)}/{len(proxies)} passed health check")
        return alive

    def _sort(self):
        self._live = sorted(
            [p for p in self._all.values() if p.alive],
            key=lambda p: p.score,
        )
        if self._live:
            self._idx = self._idx % len(self._live)

    def _refresh(self):
        log.info("[Proxy] Background refresh starting…")
        raw   = self._fetch()
        alive = self._test(raw)
        with self._lock:
            for p in alive:
                existing = self._all.get(p.url)
                if existing:
                    existing.alive    = True
                    existing.speed_ms = p.speed_ms
                    existing.failures = max(0, existing.failures - 1)
                else:
                    self._all[p.url] = p
            self._sort()
        log.info(f"[Proxy] Refresh done — {len(self._live)} live proxies")

    def _start_refresh(self):
        def _loop():
            while True:
                time.sleep(_REFRESH_SECS)
                try:
                    self._refresh()
                except Exception as e:
                    log.error(f"[Proxy] Refresh error: {e}")
        t = threading.Thread(target=_loop, daemon=True, name="ProxyRefresh")
        t.start()
        log.info(f"[Proxy] Auto-refresh every {_REFRESH_SECS//60}min (matches source repo)")


# ── Module singleton ──────────────────────────────────────────────────────────
_pool: Optional[ProxyPool] = None
_pool_lock = threading.Lock()

def init_pool(protocols=None, test_timeout=8, test_workers=120,
              max_failures=3, extra_proxies=None, verbose=True) -> ProxyPool:
    """Initialise the global pool (call once at startup)."""
    global _pool
    with _pool_lock:
        if _pool is None:
            _pool = ProxyPool(
                protocols=protocols,
                test_timeout=test_timeout,
                test_workers=test_workers,
                max_failures=max_failures,
                extra_proxies=extra_proxies or [],
                verbose=verbose,
            )
            _pool.load()
    return _pool

def next_proxy() -> Optional[Dict[str, str]]:
    return _pool.get() if _pool else None

def proxy_bad(p):
    if _pool: _pool.bad(p)

def proxy_ok(p):
    if _pool: _pool.ok(p)


# ── CLI self-test ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-8s  %(message)s",
                        datefmt="%H:%M:%S")
    protos = sys.argv[1:] or None
    pool = ProxyPool(protocols=protos, verbose=True)
    pool.load()
    s = pool.stats()
    print(f"\nTotal fetched : {s['total']}")
    print(f"Live          : {s['live']}")
    print(f"Fastest       : {s['fastest_ms']} ms")
    print(f"Median        : {s['median_ms']} ms")
    print(f"By protocol   : {s['by_protocol']}")
    print("\nTop 5 fastest:")
    for _ in range(min(5, len(pool))):
        pr = pool._live[_]
        print(f"  {pr.protocol:6s}  {pr.host}:{pr.port}  {pr.speed_ms:.0f}ms")
