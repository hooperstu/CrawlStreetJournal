"""
CSJ crawler: robots.txt, fetch, sitemap seeding, inventory rows,
per-domain rate limiting, two-tier priority queue, and polite throttling.
"""

from __future__ import annotations

import logging
import os
import random
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from urllib.robotparser import RobotFileParser

import requests

import config
from config import CrawlConfig
import parser as parser_module
import sitemap as sitemap_module
import storage
from storage import StorageContext

logger = logging.getLogger(__name__)


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

_robots_cache: Dict[str, RobotFileParser] = {}
_blocked_origins: Set[str] = set()


def _origin_of(url: str) -> str:
    try:
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}"
    except Exception:
        return ""


def _robots_for_url(url: str) -> RobotFileParser:
    origin = _origin_of(url)
    if origin not in _robots_cache:
        rp = RobotFileParser()
        try:
            rp.set_url(origin + "/robots.txt")
            rp.read()
        except Exception as e:
            logger.debug("Could not fetch robots.txt for %s: %s", origin, e)
        _robots_cache[origin] = rp
    return _robots_cache[origin]


def can_fetch(url: str, cfg: Optional[CrawlConfig] = None) -> bool:
    _respect = cfg.RESPECT_ROBOTS_TXT if cfg else config.RESPECT_ROBOTS_TXT
    _ua = cfg.USER_AGENT if cfg else config.USER_AGENT
    if not _respect:
        return True
    origin = _origin_of(url)
    if origin in _blocked_origins:
        return False
    rp = _robots_for_url(url)
    try:
        allowed = rp.can_fetch(_ua, url)
        if not allowed:
            root_blocked = not rp.can_fetch(_ua, origin + "/")
            if root_blocked:
                _blocked_origins.add(origin)
                logger.info(
                    "Origin %s fully blocked by robots.txt — skipping all URLs", origin
                )
        return allowed
    except Exception:
        return True


# ── Domain scope ──────────────────────────────────────────────────────────

def is_allowed_domain(url: str, cfg: Optional[CrawlConfig] = None) -> bool:
    """Match hostname at dot boundary (suffix match)."""
    domains = cfg.ALLOWED_DOMAINS if cfg else config.ALLOWED_DOMAINS
    try:
        host = (urlparse(url).hostname or "").lower()
        normalised_domains = [
            str(d).strip().lower().lstrip(".")
            for d in domains
            if str(d).strip()
        ]
        return any(
            host == d or host.endswith("." + d)
            for d in normalised_domains
        )
    except Exception:
        return False


# ── Helpers ───────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _is_probably_html(content_type: str) -> bool:
    ct = (content_type or "").lower()
    if not ct:
        return True
    return "text/html" in ct or "application/xhtml" in ct


# ── Per-domain rate limiting ──────────────────────────────────────────────

_domain_last_fetch: Dict[str, float] = {}
_domain_fail_count: Dict[str, int] = {}


def _record_domain_success(hostname: str) -> None:
    _domain_fail_count.pop(hostname, None)


def _record_domain_failure(hostname: str) -> None:
    _domain_fail_count[hostname] = _domain_fail_count.get(hostname, 0) + 1


def _per_domain_delay(hostname: str, base_delay: Union[float, Tuple[float, float]]) -> float:
    """Base delay plus adaptive back-off for repeatedly failing domains."""
    base = _resolve_delay(base_delay)
    fails = _domain_fail_count.get(hostname, 0)
    if fails > 0:
        extra = min(base * (2 ** min(fails, 5)), 60)
        return base + extra
    return base


def _wait_for_domain(hostname: str, delay_cfg: Union[float, Tuple[float, float]]) -> None:
    """Sleep to respect per-domain rate limiting."""
    delay = _per_domain_delay(hostname, delay_cfg)
    now = time.monotonic()
    last = _domain_last_fetch.get(hostname, 0)
    wait = max(0, delay - (now - last))
    if wait > 0:
        time.sleep(wait)
    _domain_last_fetch[hostname] = time.monotonic()


# ── Fetch with exponential back-off ──────────────────────────────────────

def fetch_page(
    url: str, cfg: Optional[CrawlConfig] = None,
) -> Tuple[Optional[str], int, str, str, Dict[str, str], str]:
    """
    GET *url*.  Returns
    ``(body, status_code, final_url, content_type, response_meta, error_detail)``.

    *error_detail* is an empty string on success, or a human-readable
    diagnostic when the fetch fails.
    """
    _ua = cfg.USER_AGENT if cfg else config.USER_AGENT
    _retries = cfg.MAX_RETRIES if cfg else config.MAX_RETRIES
    _timeout = cfg.REQUEST_TIMEOUT_SECONDS if cfg else config.REQUEST_TIMEOUT_SECONDS
    _capture = cfg.CAPTURE_RESPONSE_HEADERS if cfg else config.CAPTURE_RESPONSE_HEADERS

    headers = {"User-Agent": _ua}
    empty_meta: Dict[str, str] = {}
    last_error = ""

    for attempt in range(_retries + 1):
        if attempt > 0:
            backoff = min(2 ** attempt, 30)
            time.sleep(backoff)
        try:
            resp = requests.get(
                url, headers=headers, timeout=_timeout, allow_redirects=True,
            )
            final = resp.url
            ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip()
            status = resp.status_code
            meta = empty_meta
            if _capture:
                meta = {
                    "last_modified": (resp.headers.get("Last-Modified") or "").strip(),
                    "etag": (resp.headers.get("ETag") or "").strip(),
                }
            if status >= 400:
                last_error = f"HTTP {status}"
                if attempt == _retries:
                    return None, status, final, ctype, meta, last_error
                continue
            return resp.text, status, final, ctype, meta, ""
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

    return None, 0, url, "", empty_meta, last_error


def head_asset(url: str, cfg: Optional[CrawlConfig] = None) -> Tuple[str, str]:
    """Return (content_type, content_length) from HEAD, or empty strings."""
    _ua = cfg.USER_AGENT if cfg else config.USER_AGENT
    _timeout = cfg.HEAD_TIMEOUT_SECONDS if cfg else config.HEAD_TIMEOUT_SECONDS
    headers = {"User-Agent": _ua}
    try:
        resp = requests.head(
            url, headers=headers, timeout=_timeout, allow_redirects=True,
        )
        ct = (resp.headers.get("Content-Type") or "").split(";")[0].strip()
        cl = (resp.headers.get("Content-Length") or "").strip()
        return ct, cl
    except Exception:
        return "", ""


# ── Sitemap helpers ───────────────────────────────────────────────────────

def _sitemaps_from_robots(
    origin: str, cfg: Optional[CrawlConfig] = None,
) -> List[str]:
    _ua = cfg.USER_AGENT if cfg else config.USER_AGENT
    _timeout = cfg.REQUEST_TIMEOUT_SECONDS if cfg else config.REQUEST_TIMEOUT_SECONDS
    try:
        r = requests.get(
            origin.rstrip("/") + "/robots.txt",
            headers={"User-Agent": _ua}, timeout=_timeout,
        )
        r.raise_for_status()
    except Exception as e:
        logger.debug("robots.txt fetch failed for %s: %s", origin, e)
        return []
    found: List[str] = []
    for line in r.text.splitlines():
        line = line.strip()
        if line.lower().startswith("sitemap:"):
            found.append(line.split(":", 1)[1].strip())
    return found


def collect_start_items(
    cfg: Optional[CrawlConfig] = None,
    ctx: Optional[StorageContext] = None,
    on_phase: Optional[Callable[[str, str], None]] = None,
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
                sm_url, max_urls=_max_sm,
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

    seen: Set[str] = set()
    checked = 0
    for row in edge_rows:
        target = row["to_url"]
        if target in seen:
            continue
        seen.add(target)
        if checked >= _max_checks:
            break
        try:
            resp = requests.head(
                target, headers={"User-Agent": _ua},
                timeout=_timeout, allow_redirects=True,
            )
            check_status = resp.status_code
            check_final = resp.url
        except Exception:
            check_status = 0
            check_final = target
        lc_row = {
            "from_url": row["from_url"], "to_url": target,
            "check_status": check_status, "check_final_url": check_final,
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
    _respect = cfg.RESPECT_ROBOTS_TXT if cfg else config.RESPECT_ROBOTS_TXT
    origins: Dict[str, int] = {}
    for url, _ref, _depth in queue_items:
        o = _origin_of(url)
        origins[o] = origins.get(o, 0) + 1

    total = len(origins)
    blocked_count = 0
    for idx, (origin, url_count) in enumerate(sorted(origins.items()), 1):
        if not _respect:
            break
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
            blocked_count, len(origins),
        )
    else:
        logger.info("Pre-flight: all %d origins allow crawling", len(origins))


# ── Main crawl loop ──────────────────────────────────────────────────────

def crawl(
    seed_urls: Optional[List[str]] = None,
    max_pages: Optional[int] = None,
    delay: Optional[Union[float, Tuple[float, float]]] = None,
    on_progress: Optional[Callable[[int, int, str], None]] = None,
    on_phase: Optional[Callable[[str, str], None]] = None,
    should_stop: Optional[Callable[[], bool]] = None,
    run_name: Optional[str] = None,
    run_folder: Optional[str] = None,
    resume: bool = False,
    cfg: Optional[CrawlConfig] = None,
    ctx: Optional[StorageContext] = None,
) -> Tuple[int, int]:
    """
    Crawl HTML pages up to max_pages; record inventory and linked assets.
    Returns (pages_crawled, assets_recorded_from_page_links).

    When *cfg* and *ctx* are provided the crawl is fully isolated and safe
    to run concurrently with other crawls in separate threads.  When
    omitted, module-level globals are used (backward compat for CLI).
    """
    if cfg is None:
        cfg = CrawlConfig.from_module()
    if ctx is None:
        ctx = StorageContext(cfg.OUTPUT_DIR, cfg)

    max_pages = max_pages if max_pages is not None else cfg.MAX_PAGES_TO_CRAWL
    delay_cfg = delay if delay is not None else cfg.REQUEST_DELAY_SECONDS
    max_depth = cfg.MAX_DEPTH
    state_interval = cfg.STATE_SAVE_INTERVAL

    run_dir = None

    if resume and run_folder:
        run_dir = os.path.join(ctx.output_dir, run_folder)
        saved_cfg = storage.load_run_config(run_dir)
        if saved_cfg:
            cfg = CrawlConfig.from_dict(saved_cfg, base=cfg)
            ctx.cfg = cfg
            max_pages = cfg.MAX_PAGES_TO_CRAWL
            delay_cfg = cfg.REQUEST_DELAY_SECONDS
            max_depth = cfg.MAX_DEPTH
        ctx.resume_outputs(run_folder)
    elif run_folder:
        run_dir = os.path.join(ctx.output_dir, run_folder)
        saved_cfg = storage.load_run_config(run_dir)
        if saved_cfg:
            cfg = CrawlConfig.from_dict(saved_cfg, base=cfg)
            ctx.cfg = cfg
            max_pages = cfg.MAX_PAGES_TO_CRAWL
            delay_cfg = cfg.REQUEST_DELAY_SECONDS
            max_depth = cfg.MAX_DEPTH
        ctx.initialise_outputs(run_folder=run_folder, run_name=run_name)
    else:
        ctx.initialise_outputs(run_name=run_name)

    run_dir = ctx.get_active_run_dir()

    crawl_queue: deque[Tuple[str, str, int]] = deque()
    seed_queue: deque[Tuple[str, str, int]] = deque()
    queued: Set[str] = set()
    visited: Set[str] = set()
    sitemap_meta: Dict[str, Dict[str, str]] = {}
    pages_crawled = 0
    assets_from_pages = 0
    saved_state: Optional[Dict[str, Any]] = None

    if resume and run_folder:
        visited = storage.rebuild_visited_from_csvs(run_dir)
        sitemap_meta = storage.rebuild_sitemap_meta_from_csv(run_dir)
        saved_state = storage.load_crawl_state(run_dir)
        if saved_state:
            pages_crawled = saved_state.get("pages_crawled", 0)
            assets_from_pages = saved_state.get("assets_from_pages", 0)
            for item in saved_state.get("queue", []):
                if isinstance(item, (list, tuple)) and len(item) == 3:
                    u, ref, depth = item
                    if u not in visited and u not in queued:
                        target = crawl_queue if depth > 0 else seed_queue
                        target.append((u, ref, int(depth)))
                        queued.add(u)
        logger.info(
            "Resumed: %d visited, %d in queue (%d crawl + %d seed), "
            "%d pages already crawled",
            len(visited), len(crawl_queue) + len(seed_queue),
            len(crawl_queue), len(seed_queue), pages_crawled,
        )
    else:
        if seed_urls is not None:
            start_items = [(normalise_url(u.strip()), "seed") for u in seed_urls if u.strip()]
        else:
            start_items, sitemap_meta = collect_start_items(cfg, ctx, on_phase)

        now0 = _now_iso()
        for u, ref in start_items:
            if not is_allowed_domain(u, cfg):
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
                continue
            if u not in queued:
                seed_queue.append((u, ref, 0))
                queued.add(u)

    all_queued = list(seed_queue) + list(crawl_queue)
    if all_queued and not (resume and saved_state):
        _preflight_robots_report(all_queued, cfg, on_phase)

    if resume and run_folder and saved_state and saved_state.get("started_at"):
        started_at = saved_state["started_at"]
    else:
        started_at = _now_iso()

    def _combined_queue() -> List[Any]:
        return list(crawl_queue) + list(seed_queue)

    storage.save_crawl_state(
        run_dir,
        status="running",
        pages_crawled=pages_crawled,
        assets_from_pages=assets_from_pages,
        queue=_combined_queue(),
        started_at=started_at,
    )

    interrupted = False
    last_state_save = pages_crawled

    queue_total = len(crawl_queue) + len(seed_queue)
    if on_phase and queue_total:
        on_phase("crawling", f"Starting crawl — {queue_total:,} URLs queued")

    try:
        while (crawl_queue or seed_queue) and pages_crawled < max_pages:
            if should_stop and should_stop():
                interrupted = True
                break

            if crawl_queue:
                url, referrer, depth = crawl_queue.popleft()
            else:
                url, referrer, depth = seed_queue.popleft()

            url_key = normalise_url(url)
            if url_key in visited:
                continue
            visited.add(url_key)

            if not can_fetch(url, cfg):
                ctx.write_error({
                    "url": url,
                    "error_type": "robots_disallowed",
                    "message": "Blocked by robots.txt",
                    "http_status": "",
                    "discovered_at": _now_iso(),
                })
                continue

            hostname = (urlparse(url).hostname or "").lower()
            _wait_for_domain(hostname, delay_cfg)

            html, status, final_url, ctype, resp_meta, error_detail = fetch_page(url, cfg)
            if html is None:
                _record_domain_failure(hostname)
                ctx.write_error({
                    "url": url,
                    "error_type": "fetch_failed",
                    "message": error_detail or "No response body or HTTP error",
                    "http_status": status,
                    "discovered_at": _now_iso(),
                })
                continue

            _record_domain_success(hostname)

            if not _is_probably_html(ctype):
                ctx.write_error({
                    "url": final_url,
                    "error_type": "non_html",
                    "message": f"Content-Type not HTML: {ctype}",
                    "http_status": status,
                    "discovered_at": _now_iso(),
                })
                continue

            sm = sitemap_meta.get(normalise_url(url)) or sitemap_meta.get(normalise_url(final_url)) or {}

            pages_crawled += 1
            now = _now_iso()
            try:
                page_row, tag_rows = parser_module.build_page_inventory_row(
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
                )
                ctx.write_page(page_row)
                for tr in tag_rows:
                    ctx.write_tag_row(tr)
                if cfg.WRITE_NAV_LINKS_CSV:
                    try:
                        from bs4 import BeautifulSoup as _BS
                        nav_rows = parser_module.extract_nav_links(
                            _BS(html, "lxml"), final_url, now,
                        )
                        for nr in nav_rows:
                            ctx.write_nav_link(nr)
                    except Exception as nav_err:
                        logger.debug("Nav extraction failed for %s: %s", final_url, nav_err)
            except Exception as e:
                logger.exception("Inventory parse failed for %s: %s", final_url, e)
                ctx.write_error({
                    "url": final_url,
                    "error_type": "parse_error",
                    "message": str(e)[:500],
                    "http_status": status,
                    "discovered_at": now,
                })
                continue

            try:
                html_links, asset_rows, edge_rows = parser_module.extract_classified_links(
                    html, final_url, now
                )
            except Exception as e:
                logger.debug("Link extraction error on %s: %s", final_url, e)
                html_links, asset_rows, edge_rows = set(), [], []

            for e_row in edge_rows:
                ctx.write_edge(e_row)

            if cfg.CHECK_OUTBOUND_LINKS and edge_rows:
                _check_outbound_links(edge_rows, now, cfg, ctx)

            for ar in asset_rows:
                if cfg.ASSET_HEAD_METADATA:
                    ct, cl = head_asset(ar["asset_url"], cfg)
                    ar["head_content_type"] = ct
                    ar["head_content_length"] = cl
                ctx.write_asset(ar, ar["category"])
                assets_from_pages += 1

            try:
                inline_assets = parser_module.extract_inline_assets(html, final_url, now)
                for ia in inline_assets:
                    ctx.write_asset(ia, ia["category"])
                    assets_from_pages += 1
            except Exception as e:
                logger.debug("Inline asset extraction error on %s: %s", final_url, e)

            for link in html_links:
                norm = normalise_url(link)
                if norm in visited or norm in queued:
                    continue
                if not is_allowed_domain(link, cfg):
                    continue
                if parser_module.asset_category_for_url(link) is not None:
                    continue
                new_depth = depth + 1
                if max_depth is not None and new_depth > max_depth:
                    continue
                crawl_queue.append((norm, final_url, new_depth))
                queued.add(norm)

            if on_progress:
                on_progress(pages_crawled, assets_from_pages, final_url)

            if state_interval and (pages_crawled - last_state_save) >= state_interval:
                last_state_save = pages_crawled
                try:
                    storage.save_crawl_state(
                        run_dir,
                        status="running",
                        pages_crawled=pages_crawled,
                        assets_from_pages=assets_from_pages,
                        queue=_combined_queue(),
                        started_at=started_at,
                    )
                except Exception as save_err:
                    logger.warning("Periodic state save failed: %s", save_err)

    finally:
        final_status = "interrupted" if interrupted else "completed"
        try:
            storage.save_crawl_state(
                run_dir,
                status=final_status,
                pages_crawled=pages_crawled,
                assets_from_pages=assets_from_pages,
                queue=_combined_queue(),
                started_at=started_at,
                stopped_at=_now_iso(),
            )
        except Exception as final_save_err:
            logger.error("Final state save failed: %s", final_save_err)

    return pages_crawled, assets_from_pages
