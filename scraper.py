"""
CSJ crawl engine — orchestrates fetching, parsing, and persistence.

Architecture
------------
The crawler uses a **dual-queue** design:

- ``crawl_queue``  — URLs discovered by following links on already-crawled pages.
  These are processed **first** so that in-site BFS explores a domain's content
  graph before moving to the next seed.
- ``seed_queue``   — URLs from seeds and sitemaps.  Processed only when
  ``crawl_queue`` is empty, ensuring seeds don't starve discovered links.

**Resume:** Crawl progress is persisted to ``_state.json`` every
``STATE_SAVE_INTERVAL`` pages.  On resume, the visited set is rebuilt from
``pages.csv`` / ``crawl_errors.csv``, and the queue is restored from state.

**HTTP 503:** Treated as temporary overload / rate limiting — the host’s queued
URLs are deprioritised and the current URL is re-queued at the back (no
``crawl_errors.csv`` row until deferrals are exhausted). A per-host cooldown
is then applied so the next fetch respects ``MAX_GLOBAL_REQUESTS_PER_MINUTE``.

**Module-global caches** (thread-safety caveat):
``_robots_cache``, ``_blocked_origins``, ``_domain_last_fetch``, and
``_domain_fail_count`` are process-global dicts shared across concurrent
GUI crawls.  This is acceptable for robots and rate-limit data (they are
per-origin, not per-crawl) but means two crawls targeting the same origin
will share back-off state.

Key entry points:

- ``crawl()``             — main loop; safe for concurrent use when *cfg*
  and *ctx* are provided.
- ``collect_start_items`` — gather seed + sitemap URLs before crawling.
- ``fetch_page``          — GET with retries and exponential back-off.
- ``normalise_url``       — heavy-duty URL canonicalisation for dedup.
"""

from __future__ import annotations

import hashlib
import json
import heapq
import logging
import os
import random
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse, urlunparse
from urllib.robotparser import RobotFileParser, RuleLine

import requests

import config
from config import CrawlConfig
from outbound_http import request_get_streaming, request_head_follow, validate_outbound_url
import parser as parser_module
import sitemap as sitemap_module
import storage
from storage import StorageContext
import utils

logger = logging.getLogger(__name__)

_tls_verify_warned = False


def _http_session(cfg: Optional[CrawlConfig] = None) -> requests.Session:
    """Build a ``requests.Session`` with per-crawl TLS and redirect limits."""
    global _tls_verify_warned
    sess = requests.Session()
    verify = cfg.HTTP_VERIFY_SSL if cfg else config.HTTP_VERIFY_SSL
    sess.verify = verify
    max_r = cfg.HTTP_MAX_REDIRECTS if cfg else config.HTTP_MAX_REDIRECTS
    try:
        max_r = int(max_r)
    except (TypeError, ValueError):
        max_r = 30
    # urllib3/requests enforce a positive redirect budget; cap for sanity.
    sess.max_redirects = max(1, min(max_r, 1000))
    if verify is False and not _tls_verify_warned:
        logger.warning(
            "HTTP_VERIFY_SSL is False — TLS certificate verification disabled (insecure).",
        )
        _tls_verify_warned = True
    return sess


def _drain_response_chunk(resp: requests.Response, max_bytes: int = 1024) -> None:
    """Read at most *max_bytes* from a streaming response, then close."""
    try:
        for chunk in resp.iter_content(chunk_size=max_bytes):
            break
    finally:
        resp.close()


def _outbound_request_outcome(
    sess: requests.Session,
    target: str,
    timeout: float,
    get_fallback: bool,
    max_redirects: int,
    block_private: bool,
    max_get_bytes: int,
) -> Tuple[int, str, str]:
    """HEAD (optional GET fallback). Returns ``(status, final_url, message)``.

    *message* is empty on success; on total failure it holds a short diagnostic.
    """
    try:
        status, final, _ctype, err, _rh = request_head_follow(
            sess, target, timeout=timeout, max_redirects=max_redirects, block_private=block_private,
        )
        if err:
            return 0, target, err
        if get_fallback and status in (403, 405, 501):
            raw, st2, fin2, _ct2, err2, _rh2, _rc2, _lr2 = request_get_streaming(
                sess,
                target,
                timeout=timeout,
                max_redirects=max_redirects,
                max_body_bytes=max_get_bytes,
                block_private=block_private,
                headers={"Range": "bytes=0-0"},
            )
            if err2:
                return 0, target, err2
            return st2, fin2, ""
        return status, final, ""
    except Exception as exc:
        return 0, target, f"{type(exc).__name__}: {str(exc)[:400]}"


# ── URL priority scoring ─────────────────────────────────────────────────

_PRIORITY_PATH_BOOST = {
    "/": -5, "": -5,
}
_PRIORITY_PATH_PENALTY_SUBSTRINGS = (
    "/tag/", "/tags/", "/page/", "/wp-content/", "/wp-includes/",
    "/feed/", "/author/", "/attachment/", "/trackback/",
    "/comment-page-", "/?replytocom=", "/print/",
)

# Extra score added so deferred / deprioritised URLs sort after normal work.
_HTTP_503_QUEUE_BUMP = 1_000_000.0
# After this many 503 deferrals for the same URL, record a crawl error and stop re-queuing.
_MAX_503_DEFERRALS_PER_URL = 12


def _score_url(url: str, depth: int, is_seed: bool) -> float:
    """Compute a priority score for a URL (lower = higher priority).

    Scoring factors:
    - Seeds get a large bonus (processed first)
    - Shallow depth is preferred over deep
    - Homepage and root paths get a bonus
    - Low-value URL patterns get a penalty
    """
    score = depth * 10.0

    if is_seed:
        score -= 100.0

    try:
        path = urlparse(url).path.lower().rstrip("/")
    except Exception:
        path = ""

    if path in _PRIORITY_PATH_BOOST:
        score += _PRIORITY_PATH_BOOST[path]

    for prefix in _PRIORITY_PATH_PENALTY_SUBSTRINGS:
        if prefix in path:
            score += 20.0
            break

    return score


class _PriorityQueue:
    """Thread-safe priority queue backed by heapq.

    Each item is ``(score, counter, url, referrer, depth)`` where *counter*
    breaks ties in FIFO order.
    """

    def __init__(self):
        self._heap: List = []
        self._counter = 0
        self._lock = threading.Lock()

    def push(self, url: str, referrer: str, depth: int, is_seed: bool = False):
        score = _score_url(url, depth, is_seed)
        with self._lock:
            heapq.heappush(self._heap, (score, self._counter, url, referrer, depth))
            self._counter += 1

    def pop(self) -> Tuple[str, str, int]:
        with self._lock:
            _, _, url, referrer, depth = heapq.heappop(self._heap)
            return url, referrer, depth

    def try_pop(self) -> Optional[Tuple[str, str, int]]:
        """Pop the next URL or return ``None`` if the queue is empty (non-blocking)."""
        with self._lock:
            if not self._heap:
                return None
            _, _, url, referrer, depth = heapq.heappop(self._heap)
            return (url, referrer, depth)

    def __len__(self):
        with self._lock:
            return len(self._heap)

    def __bool__(self):
        with self._lock:
            return bool(self._heap)

    def to_list(self) -> List:
        """Serialise for state persistence."""
        with self._lock:
            return [(url, ref, d) for _, _, url, ref, d in self._heap]

    def push_at_back(self, url: str, referrer: str, depth: int, is_seed: bool = False) -> None:
        """Enqueue *url* with lowest priority (after normal and deprioritised work)."""
        score = _score_url(url, depth, is_seed) + _HTTP_503_QUEUE_BUMP
        with self._lock:
            heapq.heappush(self._heap, (score, self._counter, url, referrer, depth))
            self._counter += 1

    def deprioritise_hostname(self, hostname: str) -> int:
        """Raise the priority score of queued URLs whose host matches *hostname*.

        Used after HTTP 503 so other origins are crawled before retrying this host.
        Returns the number of queue entries adjusted.
        """
        hn = (hostname or "").lower()
        if not hn:
            return 0
        with self._lock:
            if not self._heap:
                return 0
            items = []
            moved = 0
            while self._heap:
                items.append(heapq.heappop(self._heap))
            for score, counter, url, ref, depth in items:
                u_host = (urlparse(url).hostname or "").lower()
                if u_host == hn:
                    score += _HTTP_503_QUEUE_BUMP
                    moved += 1
                heapq.heappush(self._heap, (score, counter, url, ref, depth))
            return moved


class _ThreadSafeSet:
    """Thread-safe set wrapper for visited/queued URLs in concurrent mode."""

    def __init__(self, initial: Optional[set] = None):
        self._set: set = initial or set()
        self._lock = threading.Lock()

    def add(self, item):
        with self._lock:
            self._set.add(item)

    def __contains__(self, item):
        with self._lock:
            return item in self._set

    def __len__(self):
        with self._lock:
            return len(self._set)

    def discard(self, item) -> None:
        with self._lock:
            self._set.discard(item)


class _ThreadSafeDict:
    """Thread-safe dict wrapper for content hashes in concurrent mode."""

    def __init__(self):
        self._dict: Dict[str, str] = {}
        self._lock = threading.Lock()

    def get(self, key, default=None):
        with self._lock:
            return self._dict.get(key, default)

    def __getitem__(self, key):
        with self._lock:
            return self._dict[key]

    def __contains__(self, key):
        with self._lock:
            return key in self._dict

    def __setitem__(self, key, value):
        with self._lock:
            self._dict[key] = value

    def increment_int(self, key: str) -> int:
        """Increment an integer counter for *key*; return the new value."""
        with self._lock:
            n = int(self._dict.get(key, 0)) + 1
            self._dict[key] = n
            return n

    def to_dict(self) -> Dict[str, str]:
        with self._lock:
            return dict(self._dict)

    def __bool__(self):
        with self._lock:
            return bool(self._dict)


# ── DNS cache ─────────────────────────────────────────────────────────────

_DNS_TTL = 300  # 5 minutes
_dns_cache: Dict[Tuple, Tuple[float, Any]] = {}
_dns_lock = threading.Lock()
_original_getaddrinfo = socket.getaddrinfo


def _cached_getaddrinfo(*args, **kwargs):
    """Process-global DNS cache with TTL to avoid repeated lookups."""
    key = (args[0], args[1]) if len(args) >= 2 else args
    now = time.monotonic()
    with _dns_lock:
        entry = _dns_cache.get(key)
        if entry and (now - entry[0]) < _DNS_TTL:
            return entry[1]
    result = _original_getaddrinfo(*args, **kwargs)
    with _dns_lock:
        _dns_cache[key] = (now, result)
    return result


socket.getaddrinfo = _cached_getaddrinfo


def normalise_url(url: str) -> str:
    """Canonicalise a URL for deduplication.

    Handles: fragment removal, trailing-slash stripping, empty query
    stripping, scheme normalisation (http→https), default port removal,
    and sorted query parameters.
    """
    try:
        p = urlparse(url)
        scheme = "https" if p.scheme in ("http", "https") else p.scheme
        netloc = p.netloc.lower()
        if netloc.endswith(":443"):
            netloc = netloc[:-4]
        elif netloc.endswith(":80"):
            netloc = netloc[:-3]
        path = p.path or "/"
        query = urlencode(sorted(parse_qsl(p.query, keep_blank_values=True)))
        normalised = urlunparse((scheme, netloc, path, p.params, query, ""))
        return normalised.rstrip("/") or normalised
    except Exception:
        return url.rstrip("/") or url


# ── Robots.txt caching ────────────────────────────────────────────────────

# Single lock protecting all four process-global crawl-state dicts below.
# Required because the GUI runs Flask with threaded=True and concurrent
# crawls can read/write these dicts from different threads simultaneously.
_global_state_lock = threading.Lock()

# Per-origin robots.txt parsers, populated lazily on first access.
_robots_cache: Dict[str, RobotFileParser] = {}
# Origins whose root path is fully disallowed — short-circuit future checks.
_blocked_origins: Set[str] = set()


def _origin_of(url: str) -> str:
    """Extract ``scheme://netloc`` as the cache key for robots / rate-limit lookups."""
    try:
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}"
    except Exception:
        return ""


_crawl_delay_cache: Dict[str, Optional[float]] = {}


def _robots_for_url(url: str, cfg: Optional[CrawlConfig] = None) -> RobotFileParser:
    """Return a cached ``RobotFileParser`` for *url*'s origin, fetching if needed."""
    origin = _origin_of(url)
    with _global_state_lock:
        if origin in _robots_cache:
            return _robots_cache[origin]
    rp = RobotFileParser()
    crawl_delay = None
    robots_url = origin.rstrip("/") + "/robots.txt"
    block = config.BLOCK_PRIVATE_OUTBOUND if cfg is None else cfg.BLOCK_PRIVATE_OUTBOUND
    try:
        verr = validate_outbound_url(robots_url) if block else None
        if verr:
            logger.debug("robots.txt fetch blocked for %s: %s", origin, verr)
        else:
            _ua = cfg.USER_AGENT if cfg else config.USER_AGENT
            _timeout = cfg.REQUEST_TIMEOUT_SECONDS if cfg else config.REQUEST_TIMEOUT_SECONDS
            _max_r = cfg.HTTP_MAX_REDIRECTS if cfg else config.HTTP_MAX_REDIRECTS
            _max_body = cfg.MAX_ROBOTS_TXT_BYTES if cfg else config.MAX_ROBOTS_TXT_BYTES
            with _http_session(cfg) as sess:
                sess.headers.update({"User-Agent": _ua})
                raw, _st, _fin, _ct, err, _rh, _rc, _lr = request_get_streaming(
                    sess,
                    robots_url,
                    timeout=float(_timeout),
                    max_redirects=int(_max_r),
                    max_body_bytes=int(_max_body),
                    block_private=bool(block),
                )
            if err:
                logger.debug("Could not fetch robots.txt for %s: %s", origin, err)
            elif raw:
                rp.set_url(robots_url)
                text = raw.decode("utf-8", errors="replace")
                rp.parse(text.splitlines())
                try:
                    cd = rp.crawl_delay("*")
                    if cd is not None:
                        crawl_delay = float(cd)
                except Exception:
                    pass
    except Exception as e:
        logger.debug("Could not fetch robots.txt for %s: %s", origin, e)
    with _global_state_lock:
        if origin not in _robots_cache:
            _robots_cache[origin] = rp
            _crawl_delay_cache[origin] = crawl_delay
            if crawl_delay:
                logger.info("Crawl-delay for %s: %.1fs", origin, crawl_delay)
        return _robots_cache[origin]


def _get_crawl_delay(url: str, cfg: Optional[CrawlConfig] = None) -> Optional[float]:
    """Return the Crawl-delay directive for this URL's origin, or None."""
    origin = _origin_of(url)
    _robots_for_url(url, cfg)  # ensure cache is populated
    with _global_state_lock:
        return _crawl_delay_cache.get(origin)


def can_fetch(url: str, cfg: Optional[CrawlConfig] = None) -> bool:
    """Check robots.txt permission for *url*.

    Uses the ``_blocked_origins`` set as a fast-path: if an origin's root
    path is disallowed, every URL under that origin is rejected without
    re-parsing the robots.txt file.
    """
    _respect = cfg.RESPECT_ROBOTS_TXT if cfg else config.RESPECT_ROBOTS_TXT
    _ua = cfg.USER_AGENT if cfg else config.USER_AGENT
    if not _respect:
        return True
    origin = _origin_of(url)
    with _global_state_lock:
        if origin in _blocked_origins:
            return False
    rp = _robots_for_url(url, cfg)
    try:
        allowed = rp.can_fetch(_ua, url)
        if not allowed:
            root_blocked = not rp.can_fetch(_ua, origin + "/")
            if root_blocked:
                with _global_state_lock:
                    _blocked_origins.add(origin)
                logger.info(
                    "Origin %s fully blocked by robots.txt — skipping all URLs", origin
                )
        return allowed
    except Exception:
        return True


def _normalised_robots_path(url: str) -> str:
    """Path + query fragment for robots rule matching (mirrors stdlib ``can_fetch``)."""
    try:
        parsed_url = urlparse(unquote(url))
        inner = urlunparse(
            ("", "", parsed_url.path, parsed_url.params, parsed_url.query, parsed_url.fragment),
        )
        inner = quote(inner)
        if not inner:
            inner = "/"
        return inner
    except Exception:
        return "/"


def _robots_blocking_rule_hits(url: str, user_agent: str, rp: RobotFileParser) -> List[str]:
    """Return ``Disallow`` path patterns that deny *url* for *user_agent* (longest last)."""
    if getattr(rp, "disallow_all", False):
        return ["(disallow all)"]
    if not getattr(rp, "last_checked", 0):
        return []
    path = _normalised_robots_path(url)
    hits: List[str] = []

    def _collect(entry: Any) -> None:
        if not entry:
            return
        try:
            if not entry.applies_to(user_agent):
                return
        except Exception:
            return
        for line in getattr(entry, "rulelines", []) or []:
            if not isinstance(line, RuleLine) or line.allowance:
                continue
            if line.applies_to(path):
                hits.append(line.path)

    for ent in getattr(rp, "entries", []) or []:
        _collect(ent)
    _collect(getattr(rp, "default_entry", None))
    # Prefer the most specific rule (longest path) for the log message.
    hits.sort(key=len)
    return hits


def _format_robots_rule_hint(url: str, user_agent: str, rp: RobotFileParser) -> str:
    """Human-readable robots.txt rule summary when ``can_fetch`` is false."""
    hits = _robots_blocking_rule_hits(url, user_agent, rp)
    if not hits:
        return "matched: (no explicit deny; default allow or unknown)"
    worst = hits[-1]
    if len(hits) == 1:
        return f"Disallow: {worst}"
    return f"Disallow: {worst} (also {len(hits) - 1} other pattern(s))"


def _failure_class_for_message(message: str, error_type: str) -> str:
    """Coarse failure category for analytics (derived from message and type)."""
    m = (message or "").lower()
    if error_type == "robots_disallowed":
        return "robots_disallowed"
    if error_type == "non_html":
        return "non_html"
    if error_type == "content_duplicate":
        return "content_duplicate"
    if error_type == "parse_error":
        return "parse_error"
    if error_type != "fetch_failed":
        return error_type or "unknown"
    if "timeout" in m:
        return "timeout"
    if "connectionerror" in m or "connection refused" in m:
        return "connection"
    if "ssl" in m or "certificate" in m:
        return "ssl"
    if "blocked:" in m:
        return "blocked_private"
    if "redirect limit" in m:
        return "redirect_limit"
    if m.startswith("http "):
        return "http_error"
    return "fetch_failed"


def _error_row_base(
    *,
    requested_url: str,
    final_url: str,
    referrer: str,
    depth: int,
    error_type: str,
    message: str,
    http_status: Any,
    content_type: str,
    failure_class: str,
    redirect_count: int,
    last_redirect_url: str,
    attempt_number: int,
    robots_txt_rule: str,
    worker_id: int,
) -> Dict[str, Any]:
    return {
        "url": requested_url,
        "final_url": final_url,
        "referrer_url": referrer,
        "depth": depth,
        "error_type": error_type,
        "failure_class": failure_class,
        "message": message,
        "http_status": http_status,
        "content_type": content_type,
        "redirect_count": redirect_count,
        "last_redirect_url": last_redirect_url,
        "attempt_number": attempt_number,
        "robots_txt_rule": robots_txt_rule,
        "worker_id": worker_id,
        "discovered_at": _now_iso(),
    }


# ── Domain scope ──────────────────────────────────────────────────────────

def is_allowed_domain(url: str, cfg: Optional[CrawlConfig] = None) -> bool:
    """Match *url*'s hostname against the configured allowed-domain list at a dot boundary.

    Delegates to ``utils.is_allowed_domain`` using either *cfg*'s domain list
    or the module-level ``config.ALLOWED_DOMAINS`` when *cfg* is ``None``.
    """
    domains = cfg.ALLOWED_DOMAINS if cfg else config.ALLOWED_DOMAINS
    return utils.is_allowed_domain(url, domains)


def _is_excluded_domain(url: str, cfg: Optional[CrawlConfig] = None) -> bool:
    """Return True if the URL's hostname matches any excluded domain."""
    excluded = cfg.EXCLUDED_DOMAINS if cfg else getattr(config, "EXCLUDED_DOMAINS", [])
    if not excluded:
        return False
    try:
        host = (urlparse(url).hostname or "").lower()
        return any(
            host == d.lower() or host.endswith("." + d.lower())
            for d in excluded
        )
    except Exception:
        return False


def _matches_url_patterns(url: str, patterns: list) -> bool:
    """Return True if the URL contains any of the pattern substrings."""
    if not patterns:
        return False
    url_lower = url.lower()
    return any(p.lower() in url_lower for p in patterns if p)


def is_url_allowed(url: str, cfg: Optional[CrawlConfig] = None) -> bool:
    """Full URL scope check: allowed domains, excluded domains, and URL patterns."""
    if not is_allowed_domain(url, cfg):
        return False
    if _is_excluded_domain(url, cfg):
        return False
    exclude_patterns = cfg.URL_EXCLUDE_PATTERNS if cfg else getattr(config, "URL_EXCLUDE_PATTERNS", [])
    if exclude_patterns and _matches_url_patterns(url, exclude_patterns):
        return False
    include_patterns = cfg.URL_INCLUDE_PATTERNS if cfg else getattr(config, "URL_INCLUDE_PATTERNS", [])
    if include_patterns and not _matches_url_patterns(url, include_patterns):
        return False
    return True


# ── Helpers ───────────────────────────────────────────────────────────────

_now_iso = utils.now_iso


def _is_probably_html(content_type: str) -> bool:
    """Return ``True`` if the Content-Type looks like HTML (or is absent)."""
    ct = (content_type or "").lower()
    if not ct:
        return True
    return "text/html" in ct or "application/xhtml" in ct


# ── Per-domain rate limiting ──────────────────────────────────────────────

# Monotonic timestamps of the last fetch per hostname (for polite delays).
_domain_last_fetch: Dict[str, float] = {}
# Consecutive failure count per hostname (drives adaptive back-off).
_domain_fail_count: Dict[str, int] = {}
# After HTTP 503 deferral: earliest monotonic time a hostname may be fetched again
# (respects MAX_GLOBAL_REQUESTS_PER_MINUTE on retry).
_domain_503_cooldown_until: Dict[str, float] = {}


def _record_domain_success(hostname: str) -> None:
    """Reset the failure counter on a successful fetch so back-off resets."""
    with _global_state_lock:
        _domain_fail_count.pop(hostname, None)
        _domain_503_cooldown_until.pop(hostname, None)


def _record_domain_failure(hostname: str) -> None:
    """Increment the failure counter, triggering exponential back-off."""
    with _global_state_lock:
        _domain_fail_count[hostname] = _domain_fail_count.get(hostname, 0) + 1


def _per_domain_delay(hostname: str, base_delay: Union[float, Tuple[float, float]]) -> float:
    """Base delay plus adaptive back-off for repeatedly failing domains.

    The back-off doubles with each consecutive failure (capped at 5
    doublings / 60 s extra) so that flaky or rate-limiting hosts are
    given progressively more breathing room.
    """
    base = _resolve_delay(base_delay)
    with _global_state_lock:
        fails = _domain_fail_count.get(hostname, 0)
    if fails > 0:
        extra = min(base * (2 ** min(fails, 5)), 60)
        return base + extra
    return base


def _min_seconds_between_requests_for_global_rpm_cap(cfg: Optional[CrawlConfig]) -> float:
    """Minimum spacing implied by ``MAX_GLOBAL_REQUESTS_PER_MINUTE`` (60 / rpm)."""
    if cfg is not None and getattr(cfg, "MAX_GLOBAL_REQUESTS_PER_MINUTE", None) is not None:
        rpm = cfg.MAX_GLOBAL_REQUESTS_PER_MINUTE
    else:
        rpm = config.MAX_GLOBAL_REQUESTS_PER_MINUTE
    try:
        r = int(rpm)
    except (TypeError, ValueError):
        r = 30
    return 60.0 / float(max(1, r))


def _schedule_503_host_cooldown(hostname: str, cfg: Optional[CrawlConfig]) -> None:
    """After deferring a 503, block this host until the global RPM cap allows a retry."""
    hn = (hostname or "").lower()
    if not hn:
        return
    gap = _min_seconds_between_requests_for_global_rpm_cap(cfg)
    with _global_state_lock:
        now = time.monotonic()
        until = now + gap
        prev = _domain_503_cooldown_until.get(hn, 0)
        _domain_503_cooldown_until[hn] = max(prev, until)


def _wait_for_domain(
    hostname: str,
    delay_cfg: Union[float, Tuple[float, float]],
    url: str = "",
    cfg: Optional[CrawlConfig] = None,
) -> None:
    """Sleep to respect per-domain rate limiting and Crawl-delay directives."""
    delay = _per_domain_delay(hostname, delay_cfg)
    # Crawl-delay from robots.txt is only applied when obeying robots.
    # If RESPECT_ROBOTS_TXT is False, skip it: otherwise every URL would
    # fetch robots.txt just for Crawl-delay, and hosts with e.g. 10s
    # Crawl-delay would cap throughput far below REQUEST_DELAY_SECONDS.
    _respect = cfg.RESPECT_ROBOTS_TXT if cfg else config.RESPECT_ROBOTS_TXT
    if url and _respect:
        robots_delay = _get_crawl_delay(url, cfg)
        if robots_delay is not None and robots_delay > delay:
            delay = robots_delay
    hn = (hostname or "").lower()
    with _global_state_lock:
        now = time.monotonic()
        last = _domain_last_fetch.get(hn, 0)
        wait = max(0, delay - (now - last))
        cool = _domain_503_cooldown_until.get(hn, 0)
        if cool > now:
            wait = max(wait, cool - now)
        _domain_last_fetch[hn] = now + wait
    if wait > 0:
        time.sleep(wait)


# ── Fetch with exponential back-off ──────────────────────────────────────

def fetch_page(
    url: str, cfg: Optional[CrawlConfig] = None,
) -> Tuple[
    Optional[str],
    int,
    str,
    str,
    Dict[str, str],
    str,
    int,
    str,
    int,
]:
    """
    GET *url*.  Returns
    ``(body, status_code, final_url, content_type, response_meta, error_detail,
    redirect_count, last_redirect_url, attempt_number)``.

    *error_detail* is an empty string on success, or a human-readable
    diagnostic when the fetch fails.
    *attempt_number* is 1-based for the successful or final failed attempt
    within the retry loop.
    """
    _ua = cfg.USER_AGENT if cfg else config.USER_AGENT
    _retries = cfg.MAX_RETRIES if cfg else config.MAX_RETRIES
    _timeout = cfg.REQUEST_TIMEOUT_SECONDS if cfg else config.REQUEST_TIMEOUT_SECONDS
    _capture = cfg.CAPTURE_RESPONSE_HEADERS if cfg else config.CAPTURE_RESPONSE_HEADERS
    _max_r = cfg.HTTP_MAX_REDIRECTS if cfg else config.HTTP_MAX_REDIRECTS
    _max_body = cfg.MAX_RESPONSE_BYTES if cfg else config.MAX_RESPONSE_BYTES
    _block = cfg.BLOCK_PRIVATE_OUTBOUND if cfg else config.BLOCK_PRIVATE_OUTBOUND

    empty_meta: Dict[str, str] = {}
    last_error = ""
    last_rc = 0
    last_lr = ""

    with _http_session(cfg) as sess:
        sess.headers.update({"User-Agent": _ua})
        for attempt in range(_retries + 1):
            if attempt > 0:
                backoff = min(2 ** attempt, 30)
                time.sleep(backoff)
            try:
                raw, status, final, ctype, err, rh, redir_cnt, last_redir = (
                    request_get_streaming(
                        sess,
                        url,
                        timeout=float(_timeout),
                        max_redirects=int(_max_r),
                        max_body_bytes=int(_max_body),
                        block_private=bool(_block),
                    )
                )
                last_rc = redir_cnt
                last_lr = last_redir or ""
                meta = empty_meta
                if err:
                    last_error = err
                    if "blocked:" in err or "redirect limit" in err.lower():
                        return (
                            None, 0, final, ctype, meta, last_error,
                            redir_cnt, last_lr, attempt + 1,
                        )
                    if attempt == _retries:
                        return (
                            None, 0, final, ctype, meta, last_error,
                            redir_cnt, last_lr, attempt + 1,
                        )
                    logger.warning("Fetch failed for %s: %s", url, last_error)
                    continue
                if raw is not None and len(raw) >= _max_body:
                    last_error = f"response truncated at {_max_body} bytes"
                    logger.warning("Size cap for %s: %s", url, last_error)
                if _capture:
                    meta = {
                        "last_modified": (rh.get("last-modified") or "").strip(),
                        "etag": (rh.get("etag") or "").strip(),
                        "x_robots_tag": (rh.get("x-robots-tag") or "").strip(),
                        "server": (rh.get("server") or "").strip(),
                        "x_powered_by": (rh.get("x-powered-by") or "").strip(),
                    }
                if status >= 400:
                    last_error = f"HTTP {status}"
                    if attempt == _retries:
                        return (
                            None, status, final, ctype, meta, last_error,
                            redir_cnt, last_lr, attempt + 1,
                        )
                    continue
                text = (raw or b"").decode("utf-8", errors="replace")
                return (
                    text, status, final, ctype, meta, "",
                    redir_cnt, last_lr, attempt + 1,
                )
            except requests.exceptions.Timeout:
                last_error = f"Timeout (attempt {attempt + 1}/{_retries + 1})"
                logger.warning("Timeout fetching %s (attempt %s)", url, attempt + 1)
            except requests.exceptions.ConnectionError as e:
                last_error = f"ConnectionError: {str(e)[:200]}"
                logger.warning("Connection error for %s: %s", url, last_error)
            except requests.exceptions.RequestException as e:
                status = getattr(e.response, "status_code", None) if hasattr(e, "response") else None
                last_error = f"RequestException: {str(e)[:200]} (status={status})"
                logger.warning("Request failed for %s: %s", url, last_error)
                if status == 429:
                    time.sleep(5)
            except Exception as e:
                last_error = f"{type(e).__name__}: {str(e)[:200]}"
                logger.warning("Error fetching %s: %s", url, last_error)

    return (
        None, 0, url, "", empty_meta, last_error,
        last_rc, last_lr, _retries + 1,
    )


def head_asset(url: str, cfg: Optional[CrawlConfig] = None) -> Tuple[str, str]:
    """Return (content_type, content_length) from HEAD, or empty strings."""
    _ua = cfg.USER_AGENT if cfg else config.USER_AGENT
    _timeout = cfg.HEAD_TIMEOUT_SECONDS if cfg else config.HEAD_TIMEOUT_SECONDS
    _max_r = cfg.HTTP_MAX_REDIRECTS if cfg else config.HTTP_MAX_REDIRECTS
    _block = cfg.BLOCK_PRIVATE_OUTBOUND if cfg else config.BLOCK_PRIVATE_OUTBOUND
    try:
        with _http_session(cfg) as sess:
            sess.headers.update({"User-Agent": _ua})
            _st, _fin, ct, err, rh = request_head_follow(
                sess,
                url,
                timeout=float(_timeout),
                max_redirects=int(_max_r),
                block_private=bool(_block),
            )
        if err:
            return "", ""
        cl = (rh.get("content-length") or "").strip()
        return ct, cl
    except Exception:
        return "", ""


# ── Sitemap helpers ───────────────────────────────────────────────────────


def _discovery_fingerprint(cfg: Optional[CrawlConfig]) -> str:
    """Stable hash of settings that affect seed/sitemap discovery (for reuse cache)."""
    _seeds = cfg.SEED_URLS if cfg else config.SEED_URLS
    _sitemaps = cfg.SITEMAP_URLS if cfg else config.SITEMAP_URLS
    _load = cfg.LOAD_SITEMAPS_FROM_ROBOTS if cfg else config.LOAD_SITEMAPS_FROM_ROBOTS
    _max = cfg.MAX_SITEMAP_URLS if cfg else config.MAX_SITEMAP_URLS
    blob = json.dumps(
        {
            "seeds": sorted(_seeds),
            "sitemaps": sorted(_sitemaps),
            "load_robots": _load,
            "max_sitemap_urls": _max,
        },
        sort_keys=True,
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _sitemaps_from_robots(
    origin: str, cfg: Optional[CrawlConfig] = None,
) -> List[str]:
    """Parse ``Sitemap:`` directives from *origin*'s ``robots.txt``."""
    _ua = cfg.USER_AGENT if cfg else config.USER_AGENT
    _timeout = cfg.REQUEST_TIMEOUT_SECONDS if cfg else config.REQUEST_TIMEOUT_SECONDS
    _max_r = cfg.HTTP_MAX_REDIRECTS if cfg else config.HTTP_MAX_REDIRECTS
    _max_body = cfg.MAX_ROBOTS_TXT_BYTES if cfg else config.MAX_ROBOTS_TXT_BYTES
    _block = cfg.BLOCK_PRIVATE_OUTBOUND if cfg else config.BLOCK_PRIVATE_OUTBOUND
    robots_url = origin.rstrip("/") + "/robots.txt"
    try:
        with _http_session(cfg) as sess:
            sess.headers.update({"User-Agent": _ua})
            raw, _st, _fin, _ct, err, _rh, _rc, _lr = request_get_streaming(
                sess,
                robots_url,
                timeout=float(_timeout),
                max_redirects=int(_max_r),
                max_body_bytes=int(_max_body),
                block_private=bool(_block),
            )
        if err:
            logger.debug("robots.txt fetch failed for %s: %s", origin, err)
            return []
        text = (raw or b"").decode("utf-8", errors="replace")
    except Exception as e:
        logger.debug("robots.txt fetch failed for %s: %s", origin, e)
        return []
    return utils.parse_robots_for_sitemaps(text)


def collect_start_items(
    cfg: Optional[CrawlConfig] = None,
    ctx: Optional[StorageContext] = None,
    on_phase: Optional[Callable[[str, str], None]] = None,
    project_slug: Optional[str] = None,
) -> Tuple[List[Tuple[str, str]], Dict[str, Dict[str, str]]]:
    """
    Return ``(items, sitemap_meta)`` where *items* is a de-duplicated list
    of ``(url, referrer_label)`` and *sitemap_meta* maps normalised URL →
    ``{"sitemap_lastmod": ..., "source_sitemap": ...}``.
    """
    _seeds = cfg.SEED_URLS if cfg else config.SEED_URLS
    _sitemaps = cfg.SITEMAP_URLS if cfg else config.SITEMAP_URLS
    _max_sm = cfg.MAX_SITEMAP_URLS if cfg else config.MAX_SITEMAP_URLS
    _load_robots = cfg.LOAD_SITEMAPS_FROM_ROBOTS if cfg else config.LOAD_SITEMAPS_FROM_ROBOTS
    _mode = (cfg.SITEMAP_DISCOVERY_MODE if cfg else config.SITEMAP_DISCOVERY_MODE) or "refresh"
    _mode = str(_mode).strip().lower()
    if _mode not in ("refresh", "reuse"):
        _mode = "refresh"

    logger.info(
        "Seed/sitemap discovery: %d seed URL(s), %d configured sitemap(s), "
        "LOAD_SITEMAPS_FROM_ROBOTS=%s, MAX_SITEMAP_URLS=%s, SITEMAP_DISCOVERY_MODE=%s",
        len(_seeds),
        len(_sitemaps),
        _load_robots,
        _max_sm,
        _mode,
    )

    if project_slug and _mode == "reuse":
        fp = _discovery_fingerprint(cfg)
        cached = storage.load_discovered_sitemaps_cache(project_slug)
        if cached and cached.get("fingerprint") == fp:
            raw_items = cached.get("items") or []
            meta = cached.get("sitemap_meta") or {}
            items = [
                (str(a[0]), str(a[1]))
                for a in raw_items
                if isinstance(a, (list, tuple)) and len(a) >= 2
            ]
            logger.info(
                "Reusing cached sitemap discovery (%d URLs, project=%s)",
                len(items),
                project_slug,
            )
            if on_phase:
                on_phase(
                    "discovering_sitemaps",
                    f"Reusing cached discovery — {len(items):,} URLs (no robots/sitemap fetch)",
                )
            return items, meta

    items: List[Tuple[str, str]] = []
    seen: Set[str] = set()
    sitemap_meta: Dict[str, Dict[str, str]] = {}
    sitemap_url_count = 0

    def _notify(detail: str) -> None:
        if on_phase:
            on_phase("discovering_sitemaps", detail)

    def add(u: str, ref: str) -> None:
        u = (u or "").strip()
        if not u:
            return
        key = normalise_url(u)
        if key in seen:
            return
        seen.add(key)
        items.append((key, ref))

    def _process_sitemap(sm_url: str) -> None:
        nonlocal sitemap_url_count
        _notify(f"Parsing sitemap: {sm_url}")
        try:
            entries = sitemap_module.collect_urls_from_sitemap(
                sm_url,
                max_urls=_max_sm,
                http_verify=cfg.HTTP_VERIFY_SSL if cfg else config.HTTP_VERIFY_SSL,
                http_max_redirects=cfg.HTTP_MAX_REDIRECTS if cfg else config.HTTP_MAX_REDIRECTS,
                block_private_outbound=cfg.BLOCK_PRIVATE_OUTBOUND if cfg else config.BLOCK_PRIVATE_OUTBOUND,
                max_sitemap_bytes=cfg.MAX_SITEMAP_RESPONSE_BYTES if cfg else config.MAX_SITEMAP_RESPONSE_BYTES,
            )
        except Exception as e:
            logger.warning("Sitemap crawl failed for %s: %s", sm_url, e)
            return
        label = f"sitemap:{sm_url}"
        now = _now_iso()
        for loc, lastmod in entries:
            loc = loc.strip()
            norm = normalise_url(loc)
            meta = {"sitemap_lastmod": lastmod, "source_sitemap": sm_url}
            if norm not in sitemap_meta:
                sitemap_meta[norm] = meta
            row = {
                "url": loc, "lastmod": lastmod,
                "source_sitemap": sm_url, "discovered_at": now,
            }
            if ctx:
                ctx.write_sitemap_url(row)
            else:
                storage.write_sitemap_url(row)
            sitemap_url_count += 1
            add(loc, label)
        _notify(
            f"{sitemap_url_count:,} sitemap URLs discovered, "
            f"{len(items):,} queued"
        )

    if on_phase:
        on_phase("seeding", f"Preparing {len(_seeds):,} seed URLs")

    for u in _seeds:
        add(u.strip(), "seed")

    for sm in _sitemaps:
        _process_sitemap(sm)

    if _load_robots:
        origins: Set[str] = set()
        for u in _seeds:
            try:
                p = urlparse(u)
                origins.add(f"{p.scheme}://{p.netloc}")
            except Exception:
                continue
        total_origins = len(origins)
        for idx, origin in enumerate(origins, 1):
            _notify(
                f"Fetching robots.txt {idx}/{total_origins}: {origin}"
            )
            for sm in _sitemaps_from_robots(origin, cfg):
                _process_sitemap(sm)

    if on_phase:
        on_phase(
            "seeding",
            f"Seeding complete — {len(items):,} URLs queued"
            + (f", {sitemap_url_count:,} sitemap URLs indexed"
               if sitemap_url_count else ""),
        )

    if project_slug:
        fp = _discovery_fingerprint(cfg)
        storage.save_discovered_sitemaps_cache(project_slug, fp, items, sitemap_meta)

    return items, sitemap_meta


# ── Outbound link checks ─────────────────────────────────────────────────

def _check_outbound_links(
    edge_rows: List[Dict[str, str]],
    discovered_at: str,
    cfg: Optional[CrawlConfig] = None,
    ctx: Optional[StorageContext] = None,
) -> None:
    """HEAD-check a sample of outbound link targets and write results."""
    _ua = cfg.USER_AGENT if cfg else config.USER_AGENT
    _timeout = cfg.HEAD_TIMEOUT_SECONDS if cfg else config.HEAD_TIMEOUT_SECONDS
    _max_checks = cfg.MAX_LINK_CHECKS_PER_PAGE if cfg else config.MAX_LINK_CHECKS_PER_PAGE
    _delay = cfg.LINK_CHECK_DELAY_SECONDS if cfg else config.LINK_CHECK_DELAY_SECONDS
    _get_fb = cfg.LINK_CHECK_GET_FALLBACK if cfg else config.LINK_CHECK_GET_FALLBACK
    _max_r = cfg.HTTP_MAX_REDIRECTS if cfg else config.HTTP_MAX_REDIRECTS
    _block = cfg.BLOCK_PRIVATE_OUTBOUND if cfg else config.BLOCK_PRIVATE_OUTBOUND
    _link_cap = cfg.MAX_LINK_CHECK_RESPONSE_BYTES if cfg else config.MAX_LINK_CHECK_RESPONSE_BYTES

    seen: Set[str] = set()
    checked = 0
    with _http_session(cfg) as sess:
        sess.headers.update({"User-Agent": _ua})
        for row in edge_rows:
            target = row["to_url"]
            if target in seen:
                continue
            seen.add(target)
            if checked >= _max_checks:
                break
            check_status, check_final, check_message = _outbound_request_outcome(
                sess,
                target,
                float(_timeout),
                _get_fb,
                int(_max_r),
                bool(_block),
                int(_link_cap),
            )
            lc_row = {
                "from_url": row["from_url"],
                "to_url": target,
                "check_status": check_status,
                "check_final_url": check_final,
                "check_message": check_message,
                "discovered_at": discovered_at,
            }
            if ctx:
                ctx.write_link_check(lc_row)
            else:
                storage.write_link_check(lc_row)
            checked += 1
            time.sleep(_delay)


def _resolve_delay(value: Union[float, Tuple[float, float]]) -> float:
    """Return a sleep duration — fixed or random within a (min, max) range."""
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return random.uniform(value[0], value[1])
    return float(value)


# ── Pre-flight robots report ─────────────────────────────────────────────

def _preflight_robots_report(
    queue_items: List[Tuple[str, str, int]],
    cfg: Optional[CrawlConfig] = None,
    on_phase: Optional[Callable[[str, str], None]] = None,
) -> None:
    """Check robots.txt for every unique origin in the queue and log results."""
    origins: Dict[str, int] = {}
    for url, _ref, _depth in queue_items:
        o = _origin_of(url)
        origins[o] = origins.get(o, 0) + 1

    total = len(origins)
    _respect = cfg.RESPECT_ROBOTS_TXT if cfg else config.RESPECT_ROBOTS_TXT
    if not _respect:
        logger.info(
            "Pre-flight robots check skipped (RESPECT_ROBOTS_TXT is False); "
            "%d unique origin(s) in queue",
            total,
        )
        return

    blocked_count = 0
    for idx, (origin, url_count) in enumerate(sorted(origins.items()), 1):
        if on_phase:
            on_phase(
                "preflight_robots",
                f"Checking robots.txt {idx}/{total}: {origin}",
            )
        test_url = origin + "/"
        if not can_fetch(test_url, cfg):
            logger.warning(
                "Pre-flight: %s is BLOCKED by robots.txt (%d queued URLs will be skipped)",
                origin, url_count,
            )
            blocked_count += 1

    if blocked_count:
        logger.warning(
            "Pre-flight: %d of %d origins blocked by robots.txt",
            blocked_count, total,
        )
    else:
        logger.info("Pre-flight: all %d origins allow crawling", total)


# ── Main crawl loop ──────────────────────────────────────────────────────


def _init_run(
    cfg: CrawlConfig,
    ctx: StorageContext,
    seed_urls: Optional[List[str]],
    run_name: Optional[str],
    run_folder: Optional[str],
    resume: bool,
    max_pages: int,
    delay_cfg: Any,
    max_depth: Optional[int],
    on_phase: Optional[Callable] = None,
    visited_seed: Optional[Set[str]] = None,
    project_slug: Optional[str] = None,
) -> Tuple[
    str,                                       # run_dir
    _PriorityQueue,                            # pq
    Set[str],                                  # queued
    Set[str],                                  # visited
    Dict[str, Dict[str, str]],                 # sitemap_meta
    int,                                       # pages_crawled
    int,                                       # assets_from_pages
    Optional[Dict[str, Any]],                  # saved_state
    CrawlConfig,                               # possibly updated cfg
]:
    """Initialise (or resume) a crawl run directory and seed / restore the queues.

    *visited_seed* (normalised URLs from a prior run) is merged into *visited*
    when starting a **new** run so the same seeds/sitemap pass does not
    re-fetch pages already inventoried elsewhere.

    Returns a tuple of all mutable crawl-state components.
    """
    pq = _PriorityQueue()
    queued = _ThreadSafeSet()
    visited = _ThreadSafeSet()
    sitemap_meta: Dict[str, Dict[str, str]] = {}
    pages_crawled = 0
    assets_from_pages = 0
    saved_state: Optional[Dict[str, Any]] = None

    if resume and run_folder:
        run_dir = os.path.join(ctx.output_dir, run_folder)
        saved_cfg = storage.load_run_config(run_dir)
        if saved_cfg:
            cfg = CrawlConfig.from_dict(saved_cfg, base=cfg)
            ctx.cfg = cfg
        logger.info("Resuming run — loading prior outputs from %s", run_folder)
        ctx.resume_outputs(run_folder)
    elif run_folder:
        run_dir = os.path.join(ctx.output_dir, run_folder)
        saved_cfg = storage.load_run_config(run_dir)
        if saved_cfg:
            cfg = CrawlConfig.from_dict(saved_cfg, base=cfg)
            ctx.cfg = cfg
        logger.info(
            "Initialising new crawl outputs (run folder %s)",
            run_folder,
        )
        ctx.initialise_outputs(run_folder=run_folder, run_name=run_name)
    else:
        logger.info("Initialising new crawl outputs (new run)")
        ctx.initialise_outputs(run_name=run_name)

    run_dir = ctx.get_active_run_dir()
    logger.info("Active run directory: %s", run_dir)

    if not resume and visited_seed:
        n_seed = 0
        for u in visited_seed:
            if u:
                visited.add(u)
                n_seed += 1
        logger.info(
            "Continuing from prior run: %d URL(s) marked as already crawled "
            "(will not be fetched again if re-queued)",
            n_seed,
        )

    if resume and run_folder:
        visited = _ThreadSafeSet(storage.rebuild_visited_from_csvs(run_dir))
        sitemap_meta = storage.rebuild_sitemap_meta_from_csv(run_dir)
        saved_state = storage.load_crawl_state(run_dir)
        if saved_state:
            pages_crawled = saved_state.get("pages_crawled", 0)
            assets_from_pages = saved_state.get("assets_from_pages", 0)
            for item in saved_state.get("queue", []):
                if isinstance(item, (list, tuple)) and len(item) == 3:
                    u, ref, depth = item
                    if u not in visited and u not in queued:
                        pq.push(u, ref, int(depth), is_seed=(depth == 0))
                        queued.add(u)
        logger.info(
            "Resumed: %d visited, %d in queue, %d pages already crawled",
            len(visited), len(pq), pages_crawled,
        )
    else:
        logger.info(
            "Building crawl queue from seeds and sitemaps "
            "(this may take several minutes for large sitemap indexes)",
        )
        _seed_queues(
            cfg, ctx, seed_urls, pq, queued, sitemap_meta, on_phase,
            project_slug=project_slug,
        )

    return (
        run_dir, pq, queued, visited,
        sitemap_meta, pages_crawled, assets_from_pages, saved_state, cfg,
    )


def _seed_queues(
    cfg: CrawlConfig,
    ctx: StorageContext,
    seed_urls: Optional[List[str]],
    pq: _PriorityQueue,
    queued: Set[str],
    sitemap_meta: Dict[str, Dict[str, str]],
    on_phase: Optional[Callable] = None,
    project_slug: Optional[str] = None,
) -> None:
    """Populate the priority queue from seed URLs or sitemap discovery.

    Asset-only URLs are written to the asset CSV directly rather than being
    enqueued for HTML crawling.  Mutates *pq*, *queued*, and
    *sitemap_meta* in-place.
    """
    if seed_urls is not None:
        start_items: List[Tuple[str, str]] = [
            (normalise_url(u.strip()), "seed") for u in seed_urls if u.strip()
        ]
        sitemap_meta.clear()
        logger.info(
            "Using %d explicit seed URL(s) from the caller (no config sitemap pass)",
            len(start_items),
        )
    else:
        start_items, new_sitemap_meta = collect_start_items(
            cfg, ctx, on_phase, project_slug=project_slug,
        )
        sitemap_meta.update(new_sitemap_meta)

    now0 = _now_iso()
    n_asset_rows = 0
    n_html_enqueued = 0
    n_skipped_scope = 0
    for u, ref in start_items:
        if not is_url_allowed(u, cfg):
            n_skipped_scope += 1
            continue
        cat = parser_module.asset_category_for_url(u)
        if cat is not None:
            row = {
                "referrer_page_url": ref,
                "asset_url": u,
                "link_text": "",
                "category": cat,
                "head_content_type": "",
                "head_content_length": "",
                "discovered_at": now0,
            }
            if cfg.ASSET_HEAD_METADATA:
                ct, cl = head_asset(u, cfg)
                row["head_content_type"] = ct
                row["head_content_length"] = cl
            ctx.write_asset(row, cat)
            n_asset_rows += 1
            continue
        if u not in queued:
            pq.push(u, ref, 0, is_seed=True)
            queued.add(u)
            n_html_enqueued += 1

    logger.info(
        "Queue seeding finished: %d HTML URL(s) enqueued, %d asset-only row(s) "
        "written, %d URL(s) skipped by scope rules (from %d start item(s))",
        n_html_enqueued,
        n_asset_rows,
        n_skipped_scope,
        len(start_items),
    )


def _keyword_log_rows(
    page_row: Dict[str, Any],
    keyword_hits: List[Tuple[str, int]],
) -> List[Dict[str, Any]]:
    """Build ``keyword_log.csv`` row dicts from a pages.csv row and hit list."""
    rows: List[Dict[str, Any]] = []
    for kw, cnt in keyword_hits:
        rows.append({
            "keyword": kw,
            "match_count": cnt,
            "requested_url": page_row.get("requested_url", ""),
            "final_url": page_row.get("final_url", ""),
            "domain": page_row.get("domain", ""),
            "http_status": page_row.get("http_status", ""),
            "content_type": page_row.get("content_type", ""),
            "title": page_row.get("title", ""),
            "meta_description": page_row.get("meta_description", ""),
            "lang": page_row.get("lang", ""),
            "canonical_url": page_row.get("canonical_url", ""),
            "url_content_hint": page_row.get("url_content_hint", ""),
            "content_kind_guess": page_row.get("content_kind_guess", ""),
            "author": page_row.get("author", ""),
            "publisher": page_row.get("publisher", ""),
            "json_ld_types": page_row.get("json_ld_types", ""),
            "og_type": page_row.get("og_type", ""),
            "tags_all": page_row.get("tags_all", ""),
            "word_count": page_row.get("word_count", ""),
            "date_published": page_row.get("date_published", ""),
            "date_modified": page_row.get("date_modified", ""),
            "cms_generator": page_row.get("cms_generator", ""),
            "breadcrumb_schema": page_row.get("breadcrumb_schema", ""),
            "training_related_flag": page_row.get("training_related_flag", ""),
            "referrer_url": page_row.get("referrer_url", ""),
            "depth": page_row.get("depth", ""),
            "discovered_at": page_row.get("discovered_at", ""),
        })
    return rows


def _process_one_url(
    url: str,
    referrer: str,
    depth: int,
    visited: Set[str],
    queued: Set[str],
    pq: _PriorityQueue,
    sitemap_meta: Dict[str, Dict[str, str]],
    cfg: CrawlConfig,
    ctx: StorageContext,
    max_depth: Optional[int],
    delay_cfg: Any = None,
    worker_id: int = -1,
    **kwargs,
) -> Tuple[bool, int, str]:
    """Fetch and process one URL from the queue.

    Returns ``(page_written, new_assets, final_url)`` where *page_written* is
    ``True`` when a full inventory row was recorded, *new_assets* is the count
    of newly discovered asset rows, and *final_url* is the resolved URL after
    redirects (useful for progress callbacks).  Returns ``(False, 0, url)``
    when the URL was skipped.
    """
    url_key = normalise_url(url)
    if url_key in visited:
        return False, 0, url
    visited.add(url_key)

    if not can_fetch(url, cfg):
        _ua = cfg.USER_AGENT
        rp = _robots_for_url(url, cfg)
        rule_hint = _format_robots_rule_hint(url, _ua, rp)
        ctx.write_error(
            _error_row_base(
                requested_url=url,
                final_url=url,
                referrer=referrer,
                depth=depth,
                error_type="robots_disallowed",
                message="Blocked by robots.txt",
                http_status="",
                content_type="",
                failure_class="robots_disallowed",
                redirect_count=0,
                last_redirect_url="",
                attempt_number=1,
                robots_txt_rule=rule_hint,
                worker_id=worker_id,
            ),
        )
        return False, 0, url

    hostname = (urlparse(url).hostname or "").lower()
    _effective_delay = delay_cfg if delay_cfg is not None else cfg.REQUEST_DELAY_SECONDS
    _wait_for_domain(hostname, _effective_delay, url=url, cfg=cfg)

    _render_js = cfg.RENDER_JAVASCRIPT

    _t0 = time.perf_counter()
    (
        html,
        status,
        final_url,
        ctype,
        resp_meta,
        error_detail,
        redir_cnt,
        last_redir,
        attempt_no,
    ) = fetch_page(url, cfg)
    fetch_ms = (time.perf_counter() - _t0) * 1000.0
    if html is None:
        _record_domain_failure(hostname)
        msg = error_detail or "No response body or HTTP error"
        deferrals = kwargs.get("http_503_deferrals")
        if (
            status == 503
            and hostname
            and deferrals is not None
            and deferrals.increment_int(url_key) <= _MAX_503_DEFERRALS_PER_URL
        ):
            visited.discard(url_key)
            n_moved = pq.deprioritise_hostname(hostname)
            pq.push_at_back(url, referrer, depth, is_seed=False)
            _schedule_503_host_cooldown(hostname, cfg)
            gap = _min_seconds_between_requests_for_global_rpm_cap(cfg)
            logger.info(
                "HTTP 503 for %s — moving host %s to back of queue "
                "(%d other queued URL(s) from this host deprioritised); "
                "next fetch to this host after ≥%.1fs (MAX_GLOBAL_REQUESTS_PER_MINUTE)",
                url,
                hostname,
                n_moved,
                gap,
            )
            return False, 0, url
        ctx.write_error(
            _error_row_base(
                requested_url=url,
                final_url=final_url,
                referrer=referrer,
                depth=depth,
                error_type="fetch_failed",
                message=msg,
                http_status=status,
                content_type=ctype,
                failure_class=_failure_class_for_message(msg, "fetch_failed"),
                redirect_count=redir_cnt,
                last_redirect_url=last_redir,
                attempt_number=attempt_no,
                robots_txt_rule="",
                worker_id=worker_id,
            ),
        )
        return False, 0, url

    # A body was returned but the HTTP status was an error (e.g. a
    # server that sends an HTML error page with a 4xx code).  Record
    # the failure so back-off applies, then skip the page.
    if status >= 400:
        _record_domain_failure(hostname)
        msg = f"HTTP {status}"
        ctx.write_error(
            _error_row_base(
                requested_url=url,
                final_url=final_url,
                referrer=referrer,
                depth=depth,
                error_type="fetch_failed",
                message=msg,
                http_status=status,
                content_type=ctype,
                failure_class=_failure_class_for_message(msg, "fetch_failed"),
                redirect_count=redir_cnt,
                last_redirect_url=last_redir,
                attempt_number=attempt_no,
                robots_txt_rule="",
                worker_id=worker_id,
            ),
        )
        return False, 0, url

    # JS rendering fallback: re-fetch via headless browser when the
    # static HTML body is suspiciously thin (likely a JS-rendered SPA).
    if _render_js and html and len(html.strip()) < 2000:
        try:
            import render as render_module
            if render_module.is_available():
                rendered = render_module.render_page(
                    url,
                    user_agent=cfg.USER_AGENT,
                )
                if rendered is not None:
                    r_html, r_status, r_final, r_ct, r_headers = rendered
                    if r_html and len(r_html.strip()) > len(html.strip()):
                        original_len = len(html)
                        html = r_html
                        status = r_status
                        final_url = r_final
                        ctype = r_ct
                        resp_meta.update({
                            "x_robots_tag": (r_headers.get("x-robots-tag") or "").strip(),
                            "server": (r_headers.get("server") or resp_meta.get("server", "")).strip(),
                        })
                        logger.debug("JS-rendered %s (%d → %d bytes)", url, original_len, len(r_html))
        except ImportError:
            pass
        except Exception as render_err:
            logger.debug("JS render fallback failed for %s: %s", url, render_err)

    _record_domain_success(hostname)

    if not _is_probably_html(ctype):
        msg = f"Content-Type not HTML: {ctype}"
        ctx.write_error(
            _error_row_base(
                requested_url=url,
                final_url=final_url,
                referrer=referrer,
                depth=depth,
                error_type="non_html",
                message=msg,
                http_status=status,
                content_type=ctype,
                failure_class="non_html",
                redirect_count=redir_cnt,
                last_redirect_url=last_redir,
                attempt_number=attempt_no,
                robots_txt_rule="",
                worker_id=worker_id,
            ),
        )
        return False, 0, final_url

    # Content-hash deduplication: skip pages with identical visible text
    content_hashes = kwargs.get("content_hashes")
    content_hash = ""
    if content_hashes is not None:
        from bs4 import BeautifulSoup as _BS
        visible = parser_module.get_visible_text(_BS(html, parser_module._bs4_parser()))
        content_hash = hashlib.sha256(visible.encode("utf-8", errors="replace")).hexdigest()[:16]
        if content_hash in content_hashes:
            logger.debug("Content dedup: %s matches %s", url, content_hashes[content_hash])
            msg = f"Identical content hash as {content_hashes[content_hash]}"
            ctx.write_error(
                _error_row_base(
                    requested_url=url,
                    final_url=final_url,
                    referrer=referrer,
                    depth=depth,
                    error_type="content_duplicate",
                    message=msg,
                    http_status=status,
                    content_type=ctype,
                    failure_class="content_duplicate",
                    redirect_count=redir_cnt,
                    last_redirect_url=last_redir,
                    attempt_number=attempt_no,
                    robots_txt_rule="",
                    worker_id=worker_id,
                ),
            )
            return False, 0, final_url
        content_hashes[content_hash] = url

    # Change detection: compare hash against previous run
    prev_hashes = kwargs.get("prev_hashes")
    content_changed = ""
    if prev_hashes and content_hash:
        prev_hash = prev_hashes.get(content_hash)
        if prev_hash:
            content_changed = "unchanged"
        else:
            content_changed = "changed"

    sm = sitemap_meta.get(normalise_url(url)) or sitemap_meta.get(normalise_url(final_url)) or {}
    now = _now_iso()
    new_assets = 0

    try:
        page_row, tag_rows, keyword_hits = parser_module.build_page_inventory_row(
            html,
            requested_url=url,
            final_url=final_url,
            http_status=status,
            content_type=ctype,
            referrer_url=referrer,
            depth=depth,
            discovered_at=now,
            response_meta=resp_meta,
            sitemap_meta=sm,
            fetch_time_ms=fetch_ms,
        )
        if content_changed:
            page_row["content_changed"] = content_changed
        if content_hash:
            page_row["content_hash"] = content_hash
        ctx.write_page(page_row)
        for tr in tag_rows:
            ctx.write_tag_row(tr)
        if cfg.WRITE_KEYWORD_LOG_CSV and keyword_hits:
            for kl in _keyword_log_rows(page_row, keyword_hits):
                ctx.write_keyword_log_row(kl)
        if cfg.WRITE_NAV_LINKS_CSV:
            try:
                from bs4 import BeautifulSoup as _BS
                nav_rows = parser_module.extract_nav_links(
                    _BS(html, parser_module._bs4_parser()), final_url, now,
                )
                for nr in nav_rows:
                    ctx.write_nav_link(nr)
            except Exception as nav_err:
                logger.debug("Nav extraction failed for %s: %s", final_url, nav_err)
    except Exception as e:
        logger.exception("Inventory parse failed for %s: %s", final_url, e)
        msg = str(e)[:500]
        ctx.write_error(
            _error_row_base(
                requested_url=url,
                final_url=final_url,
                referrer=referrer,
                depth=depth,
                error_type="parse_error",
                message=msg,
                http_status=status,
                content_type=ctype,
                failure_class=_failure_class_for_message(msg, "parse_error"),
                redirect_count=redir_cnt,
                last_redirect_url=last_redir,
                attempt_number=attempt_no,
                robots_txt_rule="",
                worker_id=worker_id,
            ),
        )
        return False, 0, final_url

    try:
        html_links, asset_rows, edge_rows, phone_rows = parser_module.extract_classified_links(
            html, final_url, now, allowed_domains=cfg.ALLOWED_DOMAINS,
        )
    except Exception as e:
        logger.debug("Link extraction error on %s: %s", final_url, e)
        html_links, asset_rows, edge_rows, phone_rows = set(), [], [], []

    for e_row in edge_rows:
        ctx.write_edge(e_row)

    for p_row in phone_rows:
        ctx.write_phone_number(p_row)

    if cfg.CHECK_OUTBOUND_LINKS and edge_rows:
        _check_outbound_links(edge_rows, now, cfg, ctx)

    for ar in asset_rows:
        if cfg.ASSET_HEAD_METADATA:
            ct, cl = head_asset(ar["asset_url"], cfg)
            ar["head_content_type"] = ct
            ar["head_content_length"] = cl
        ctx.write_asset(ar, ar["category"])
        new_assets += 1

    try:
        inline_assets = parser_module.extract_inline_assets(html, final_url, now)
        for ia in inline_assets:
            ctx.write_asset(ia, ia["category"])
            new_assets += 1
    except Exception as e:
        logger.debug("Inline asset extraction error on %s: %s", final_url, e)

    for link in html_links:
        norm = normalise_url(link)
        if norm in visited or norm in queued:
            continue
        if not is_url_allowed(link, cfg):
            continue
        if parser_module.asset_category_for_url(link) is not None:
            continue
        new_depth = depth + 1
        if max_depth is not None and new_depth > max_depth:
            continue
        pq.push(norm, final_url, new_depth, is_seed=False)
        queued.add(norm)

    return True, new_assets, final_url


def _persist_state_if_needed(
    run_dir: str,
    pages_crawled: int,
    last_state_save: int,
    state_interval: int,
    assets_from_pages: int,
    combined_queue_fn: Any,
    started_at: str,
) -> int:
    """Save crawl state if *state_interval* pages have elapsed since last save.

    Returns the updated *last_state_save* value (unchanged if no save occurred).
    """
    if state_interval and (pages_crawled - last_state_save) >= state_interval:
        try:
            storage.save_crawl_state(
                run_dir,
                status="running",
                pages_crawled=pages_crawled,
                assets_from_pages=assets_from_pages,
                queue=combined_queue_fn(),
                started_at=started_at,
            )
        except Exception as save_err:
            logger.warning("Periodic state save failed: %s", save_err)
        return pages_crawled
    return last_state_save


def visited_keys_from_prior_run(run_dir: str) -> Set[str]:
    """Normalised URL keys from *run_dir* CSVs for continuing without duplicate fetches.

    Used when starting a **new** run after a completed or stopped run: URLs already
    present in ``pages.csv`` / ``crawl_errors.csv`` are treated as visited so they
    are not fetched again if the same seeds/sitemap re-queue them.
    """
    out: Set[str] = set()
    for u in storage.rebuild_visited_from_csvs(run_dir):
        if u:
            out.add(normalise_url(u))
    return out


def visited_keys_from_all_prior_runs(
    runs_dir: str, exclude_run: Optional[str] = None
) -> Set[str]:
    """Union of normalised URL keys from every ``run_*`` under *runs_dir*.

    *exclude_run* is typically the active run folder so its (empty) CSVs are
    not used. Used for cumulative “continue from all prior runs” behaviour.
    """
    out: Set[str] = set()
    if not os.path.isdir(runs_dir):
        return out
    for name in sorted(os.listdir(runs_dir)):
        if not name.startswith("run_"):
            continue
        if exclude_run and name == exclude_run:
            continue
        sub = os.path.join(runs_dir, name)
        if not os.path.isdir(sub):
            continue
        out |= visited_keys_from_prior_run(sub)
    return out


def _finalise_run(
    run_dir: str,
    interrupted: bool,
    pages_crawled: int,
    assets_from_pages: int,
    combined_queue_fn: Any,
    started_at: str,
) -> None:
    """Write the terminal crawl-state record (completed or interrupted)."""
    final_status = "interrupted" if interrupted else "completed"
    try:
        storage.save_crawl_state(
            run_dir,
            status=final_status,
            pages_crawled=pages_crawled,
            assets_from_pages=assets_from_pages,
            queue=combined_queue_fn(),
            started_at=started_at,
            stopped_at=_now_iso(),
        )
    except Exception as final_save_err:
        logger.error("Final state save failed: %s", final_save_err)


def crawl(
    seed_urls: Optional[List[str]] = None,
    max_pages: Optional[int] = None,
    delay: Optional[Union[float, Tuple[float, float]]] = None,
    on_progress: Optional[Callable[[int, int, str], None]] = None,
    on_phase: Optional[Callable[[str, str], None]] = None,
    on_worker_urls: Optional[Callable[[List[str]], None]] = None,
    should_stop: Optional[Callable[[], bool]] = None,
    run_name: Optional[str] = None,
    run_folder: Optional[str] = None,
    resume: bool = False,
    visited_seed: Optional[Set[str]] = None,
    project_slug: Optional[str] = None,
    cfg: Optional[CrawlConfig] = None,
    ctx: Optional[StorageContext] = None,
) -> Tuple[int, int]:
    """Crawl HTML pages up to *max_pages*, recording inventory and linked assets.

    Args:
        seed_urls: Override seed URLs (bypasses ``config.SEED_URLS``).
        max_pages: Cap on HTML pages to fetch (default from config).
        delay: Per-request delay override (float or ``(min, max)`` tuple).
        on_progress: Callback ``(pages, assets, last_url)`` after each page.
        on_phase: Callback ``(phase_name, detail)`` for UI status updates.
        on_worker_urls: Callback with one string per worker slot (URL being fetched,
            or empty when idle). Used by the GUI during parallel crawls and when stopping.
        should_stop: Callable returning ``True`` to abort early (GUI stop).
        run_name: Human-friendly label written to the run folder.
        run_folder: Existing folder name to use (or resume into).
        resume: If ``True``, rebuild visited set and queue from prior state.
        visited_seed: Normalised URLs already crawled in another run; merged into
            ``visited`` when ``resume`` is ``False`` so those URLs are skipped.
        project_slug: When set, enables ``SITEMAP_DISCOVERY_MODE=reuse`` cache under
            the project directory.
        cfg: Isolated ``CrawlConfig`` — required for thread-safe concurrent
            crawls.  Falls back to module-level globals when ``None``.
        ctx: Isolated ``StorageContext`` — paired with *cfg* for concurrency.

    Returns:
        ``(pages_crawled, assets_from_pages)`` counts.
    """
    if cfg is None:
        cfg = CrawlConfig.from_module()
    if ctx is None:
        ctx = StorageContext(cfg.OUTPUT_DIR, cfg)

    logger.info(
        "Crawl setup starting — output %s, run_folder=%s, resume=%s, "
        "max_pages=%s, concurrent_workers=%s",
        ctx.output_dir,
        run_folder or "(new)",
        resume,
        max_pages if max_pages is not None else cfg.MAX_PAGES_TO_CRAWL,
        max(1, cfg.CONCURRENT_WORKERS),
    )

    _max_pages = max_pages if max_pages is not None else cfg.MAX_PAGES_TO_CRAWL
    delay_cfg = delay if delay is not None else cfg.REQUEST_DELAY_SECONDS
    max_depth = cfg.MAX_DEPTH
    state_interval = cfg.STATE_SAVE_INTERVAL

    (
        run_dir, pq, queued, visited,
        sitemap_meta, pages_crawled, assets_from_pages, saved_state, cfg,
    ) = _init_run(
        cfg, ctx, seed_urls, run_name, run_folder, resume,
        _max_pages, delay_cfg, max_depth, on_phase=on_phase,
        visited_seed=visited_seed,
        project_slug=project_slug,
    )

    # Update per-request delay from possibly-reloaded cfg
    delay_cfg = cfg.REQUEST_DELAY_SECONDS if delay is None else delay
    max_depth = cfg.MAX_DEPTH

    workers = max(1, cfg.CONCURRENT_WORKERS if cfg else 1)
    logger.info(
        "Queue ready: %d URL(s) waiting, %d worker thread(s), "
        "request_delay=%s, max_depth=%s",
        len(pq),
        workers,
        delay_cfg,
        max_depth,
    )

    all_queued = pq.to_list()
    if all_queued and not (resume and saved_state):
        logger.info(
            "Running robots.txt pre-flight for %d queued URL(s) "
            "(%d unique origin(s))",
            len(all_queued),
            len({_origin_of(u) for u, _, _ in all_queued}),
        )
        _preflight_robots_report(
            [(u, r, d) for u, r, d in all_queued], cfg, on_phase,
        )
    elif all_queued and resume and saved_state:
        logger.info(
            "Skipping robots pre-flight (resumed run — queue restored from state)",
        )

    if resume and run_folder and saved_state and saved_state.get("started_at"):
        started_at = saved_state["started_at"]
    else:
        started_at = _now_iso()

    def _combined_queue() -> List[Any]:
        return pq.to_list()

    storage.save_crawl_state(
        run_dir,
        status="running",
        pages_crawled=pages_crawled,
        assets_from_pages=assets_from_pages,
        queue=_combined_queue(),
        started_at=started_at,
    )
    logger.info("Saved crawl state (status=running); beginning fetches")

    # If this resume session writes no new pages (e.g. empty queue from an
    # imported project), we must not mark the run *completed* — that would
    # wrongly imply the crawl finished. Treat as interrupted so the run can
    # be extended (new seeds / config) and resumed again.
    pages_at_session_start = pages_crawled

    interrupted = False
    last_state_save = pages_crawled
    content_hashes = _ThreadSafeDict()
    http_503_deferrals = _ThreadSafeDict()

    # Change detection: load hashes from the previous run if enabled
    prev_hashes: Dict[str, str] = {}
    if cfg.CHANGE_DETECTION and resume and run_folder:
        prev_hashes = storage.load_content_hashes(run_dir)
        if prev_hashes:
            logger.info("Loaded %d content hashes from previous run for change detection", len(prev_hashes))

    queue_total = len(pq)
    if queue_total:
        if on_phase:
            on_phase("crawling", f"Starting crawl \u2014 {queue_total:,} URLs queued")
    else:
        logger.warning(
            "No URLs in the crawl queue — check seeds, sitemaps, and scope filters "
            "(ALLOWED_DOMAINS, URL_INCLUDE_PATTERNS, URL_EXCLUDE_PATTERNS)",
        )

    def _emit_slots(slots: List[str]) -> None:
        if on_worker_urls:
            on_worker_urls(list(slots))

    try:
        if workers <= 1:
            # Sequential mode — single slot reflects the URL currently being fetched.
            worker_slots = [""]
            _emit_slots(worker_slots)
            while pq and pages_crawled < _max_pages:
                if should_stop and should_stop():
                    interrupted = True
                    logger.info("Stop requested — exiting after current fetch (sequential crawl)")
                    if on_phase:
                        on_phase(
                            "stopping",
                            "Stop requested — will not start another page after the current fetch",
                        )
                    break
                url, referrer, depth = pq.pop()
                worker_slots[0] = url
                _emit_slots(worker_slots)
                try:
                    page_written, new_assets, final_url = _process_one_url(
                        url, referrer, depth,
                        visited, queued, pq,
                        sitemap_meta, cfg, ctx, max_depth,
                        delay_cfg=delay_cfg,
                        worker_id=0,
                        content_hashes=content_hashes if cfg.CONTENT_DEDUP else None,
                        prev_hashes=prev_hashes if cfg.CHANGE_DETECTION else None,
                        http_503_deferrals=http_503_deferrals,
                    )
                finally:
                    worker_slots[0] = ""
                    _emit_slots(worker_slots)
                if page_written:
                    pages_crawled += 1
                    assets_from_pages += new_assets
                    if on_progress:
                        on_progress(pages_crawled, assets_from_pages, final_url)
                    last_state_save = _persist_state_if_needed(
                        run_dir, pages_crawled, last_state_save, state_interval,
                        assets_from_pages, _combined_queue, started_at,
                    )
        else:
            # Concurrent mode — shared priority queue with independent workers.
            # Each worker pops the next URL as soon as it is free (no batch barrier),
            # so a slow page on one origin does not idle other workers. Per-domain
            # rate limiting remains inside _process_one_url.
            worker_slots = [""] * workers
            _emit_slots(worker_slots)

            pages_lock = threading.Lock()
            in_flight_lock = threading.Lock()
            in_flight = 0
            stop_accepting = threading.Event()
            stop_main_logged = threading.Event()
            drain_logged = threading.Event()

            def _log_stop_main() -> None:
                nonlocal interrupted
                if stop_main_logged.is_set():
                    return
                stop_main_logged.set()
                interrupted = True
                logger.info(
                    "Stop requested — no new URLs will start "
                    "(%d URL(s) left in queue for resume)",
                    len(pq),
                )
                if on_phase:
                    on_phase(
                        "stopping",
                        "Stop requested — not starting new pages "
                        "(%d left in queue for resume)" % len(pq),
                    )

            def _log_stop_drain() -> None:
                if drain_logged.is_set():
                    return
                drain_logged.set()
                busy = [u for u in worker_slots if u]
                nbusy = len(busy)
                logger.info(
                    "Stop requested — draining %d parallel worker(s) "
                    "still fetching (finish current page each)…",
                    nbusy,
                )
                for j, u in enumerate(worker_slots):
                    if u:
                        logger.info("  Worker %d: %s", j + 1, u)
                if on_phase:
                    detail = (
                        "Waiting for %d parallel worker(s) to finish "
                        "current page(s)…" % nbusy
                        if nbusy
                        else "Stop requested — finishing in-flight fetches…"
                    )
                    on_phase("stopping", detail)

            def _worker_loop(slot_idx: int) -> None:
                nonlocal pages_crawled, assets_from_pages, last_state_save, in_flight
                while True:
                    with pages_lock:
                        at_cap = pages_crawled >= _max_pages
                        if at_cap:
                            stop_accepting.set()
                    if at_cap:
                        return

                    if should_stop and should_stop():
                        if not stop_accepting.is_set():
                            stop_accepting.set()
                            _log_stop_main()
                        _log_stop_drain()

                    item = pq.try_pop() if not stop_accepting.is_set() else None

                    if item is None:
                        with in_flight_lock:
                            idle = in_flight == 0
                        qempty = len(pq) == 0
                        if idle and qempty:
                            return
                        if stop_accepting.is_set() and idle:
                            return
                        time.sleep(0.01)
                        continue

                    url, referrer, depth = item
                    worker_slots[slot_idx] = url
                    _emit_slots(worker_slots)
                    with in_flight_lock:
                        in_flight += 1
                    try:
                        page_written, new_assets, final_url = _process_one_url(
                            url, referrer, depth,
                            visited, queued, pq,
                            sitemap_meta, cfg, ctx, max_depth,
                            delay_cfg=delay_cfg,
                            worker_id=slot_idx,
                            content_hashes=content_hashes if cfg.CONTENT_DEDUP else None,
                            prev_hashes=prev_hashes if cfg.CHANGE_DETECTION else None,
                            http_503_deferrals=http_503_deferrals,
                        )
                    except Exception as exc:
                        logger.warning("Worker error for %s: %s", url, exc)
                        page_written, new_assets, final_url = False, 0, url
                    finally:
                        with in_flight_lock:
                            in_flight -= 1
                        worker_slots[slot_idx] = ""
                        _emit_slots(worker_slots)

                    if page_written:
                        with pages_lock:
                            pages_crawled += 1
                            assets_from_pages += new_assets
                            last_state_save = _persist_state_if_needed(
                                run_dir, pages_crawled, last_state_save,
                                state_interval, assets_from_pages,
                                _combined_queue, started_at,
                            )
                            if pages_crawled >= _max_pages:
                                stop_accepting.set()
                        if on_progress:
                            on_progress(pages_crawled, assets_from_pages, final_url)

            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(_worker_loop, i) for i in range(workers)]
                for fut in futures:
                    fut.result()

    finally:
        if on_worker_urls:
            on_worker_urls([""] * workers)
        if content_hashes:
            try:
                storage.save_content_hashes(run_dir, content_hashes.to_dict())
            except Exception as hash_err:
                logger.warning("Failed to save content hashes: %s", hash_err)

        # Close Playwright browser if it was started during this crawl
        if cfg.RENDER_JAVASCRIPT:
            try:
                import render as render_module
                render_module.close()
            except Exception:
                pass

        if resume and not interrupted and pages_crawled == pages_at_session_start:
            interrupted = True
            logger.info(
                "Resume finished with no new pages written — "
                "keeping status interrupted (not completed) so the run stays resumable",
            )

        _finalise_run(
            run_dir, interrupted, pages_crawled,
            assets_from_pages, _combined_queue, started_at,
        )

    return pages_crawled, assets_from_pages
