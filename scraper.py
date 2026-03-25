"""
NHS Collector crawler: robots.txt, fetch, sitemap seeding, inventory rows,
and polite rate limiting.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests

import config
import parser as parser_module
import sitemap as sitemap_module
import storage

logger = logging.getLogger(__name__)

_robots_cache: Dict[str, RobotFileParser] = {}


def _robots_for_url(url: str) -> RobotFileParser:
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    if origin not in _robots_cache:
        rp = RobotFileParser()
        try:
            rp.set_url(origin + "/robots.txt")
            rp.read()
        except Exception as e:
            logger.debug("Could not fetch robots.txt for %s: %s", origin, e)
        _robots_cache[origin] = rp
    return _robots_cache[origin]


def can_fetch(url: str) -> bool:
    rp = _robots_for_url(url)
    try:
        return rp.can_fetch(config.USER_AGENT, url)
    except Exception:
        return True


def is_allowed_domain(url: str) -> bool:
    try:
        netloc = urlparse(url).netloc.lower()
        return any(d in netloc for d in config.ALLOWED_DOMAINS)
    except Exception:
        return False


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _is_probably_html(content_type: str) -> bool:
    ct = (content_type or "").lower()
    if not ct:
        return True
    return "text/html" in ct or "application/xhtml" in ct


def fetch_page(url: str) -> Tuple[Optional[str], int, str, str]:
    """
    GET url. Returns (body, status_code, final_url, content_type).
    On failure body is None; final_url and content_type may be empty.
    """
    headers = {"User-Agent": config.USER_AGENT}
    for attempt in range(config.MAX_RETRIES + 1):
        try:
            resp = requests.get(
                url,
                headers=headers,
                timeout=config.REQUEST_TIMEOUT_SECONDS,
                allow_redirects=True,
            )
            final = resp.url
            ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip()
            status = resp.status_code
            if status >= 400:
                return None, status, final, ctype
            return resp.text, status, final, ctype
        except requests.exceptions.Timeout:
            logger.warning("Timeout fetching %s (attempt %s)", url, attempt + 1)
            if attempt == config.MAX_RETRIES:
                return None, 0, url, ""
        except requests.exceptions.RequestException as e:
            status = getattr(e.response, "status_code", None) if hasattr(e, "response") else None
            logger.warning("Request failed for %s: %s (status=%s)", url, e, status)
            if status == 429:
                time.sleep(5)
            if attempt == config.MAX_RETRIES:
                return None, status or 0, url, ""
        except Exception as e:
            logger.warning("Error fetching %s: %s", url, e)
            if attempt == config.MAX_RETRIES:
                return None, 0, url, ""
    return None, 0, url, ""


def head_asset(url: str) -> Tuple[str, str]:
    """Return (content_type, content_length) from HEAD, or empty strings."""
    headers = {"User-Agent": config.USER_AGENT}
    try:
        resp = requests.head(
            url,
            headers=headers,
            timeout=config.HEAD_TIMEOUT_SECONDS,
            allow_redirects=True,
        )
        ct = (resp.headers.get("Content-Type") or "").split(";")[0].strip()
        cl = (resp.headers.get("Content-Length") or "").strip()
        return ct, cl
    except Exception:
        return "", ""


def _sitemaps_from_robots(origin: str) -> List[str]:
    try:
        r = requests.get(
            origin.rstrip("/") + "/robots.txt",
            headers={"User-Agent": config.USER_AGENT},
            timeout=config.REQUEST_TIMEOUT_SECONDS,
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


def collect_start_items() -> List[Tuple[str, str]]:
    """
    Return (url, referrer_label) for every seed and sitemap location,
    de-duplicated by URL.
    """
    items: List[Tuple[str, str]] = []
    seen: Set[str] = set()

    def add(u: str, ref: str) -> None:
        u = (u or "").strip()
        if not u:
            return
        key = u.rstrip("/") or u
        if key in seen:
            return
        seen.add(key)
        items.append((key, ref))

    for u in config.SEED_URLS:
        add(u.strip(), "seed")

    for sm in config.SITEMAP_URLS:
        try:
            locs = sitemap_module.collect_urls_from_sitemap(
                sm, max_urls=config.MAX_SITEMAP_URLS
            )
        except Exception as e:
            logger.warning("Sitemap crawl failed for %s: %s", sm, e)
            locs = []
        label = f"sitemap:{sm}"
        for loc in locs:
            add(loc.strip(), label)

    if config.LOAD_SITEMAPS_FROM_ROBOTS:
        origins: Set[str] = set()
        for u in config.SEED_URLS:
            try:
                p = urlparse(u)
                origins.add(f"{p.scheme}://{p.netloc}")
            except Exception:
                continue
        for origin in origins:
            for sm in _sitemaps_from_robots(origin):
                try:
                    locs = sitemap_module.collect_urls_from_sitemap(
                        sm, max_urls=config.MAX_SITEMAP_URLS
                    )
                except Exception as e:
                    logger.warning("Sitemap from robots failed %s: %s", sm, e)
                    continue
                label = f"sitemap:{sm}"
                for loc in locs:
                    add(loc.strip(), label)

    return items


def crawl(
    seed_urls: Optional[List[str]] = None,
    max_pages: Optional[int] = None,
    delay: Optional[float] = None,
    on_progress: Optional[Callable[[int, int, str], None]] = None,
    should_stop: Optional[Callable[[], bool]] = None,
) -> Tuple[int, int]:
    """
    Crawl HTML pages up to max_pages; record inventory and linked assets.
    Returns (pages_crawled, assets_recorded_from_page_links).
    """
    max_pages = max_pages if max_pages is not None else config.MAX_PAGES_TO_CRAWL
    delay = delay if delay is not None else config.REQUEST_DELAY_SECONDS

    storage.initialise_outputs()

    queue: deque[Tuple[str, str, int]] = deque()
    queued: Set[str] = set()
    visited: Set[str] = set()

    if seed_urls is not None:
        start_items = [(u.strip().rstrip("/") or u.strip(), "seed") for u in seed_urls if u.strip()]
    else:
        start_items = collect_start_items()

    now0 = _now_iso()
    for u, ref in start_items:
        if not is_allowed_domain(u):
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
            if config.ASSET_HEAD_METADATA:
                ct, cl = head_asset(u)
                row["head_content_type"] = ct
                row["head_content_length"] = cl
            storage.write_asset(row, cat)
            continue
        if u not in queued:
            queue.append((u, ref, 0))
            queued.add(u)

    pages_crawled = 0
    assets_from_pages = 0

    while queue and pages_crawled < max_pages:
        if should_stop and should_stop():
            break
        url, referrer, depth = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        if not can_fetch(url):
            storage.write_error({
                "url": url,
                "error_type": "robots_disallowed",
                "message": "Blocked by robots.txt",
                "http_status": "",
                "discovered_at": _now_iso(),
            })
            continue

        html, status, final_url, ctype = fetch_page(url)
        if html is None:
            storage.write_error({
                "url": url,
                "error_type": "fetch_failed",
                "message": "No response body or HTTP error",
                "http_status": status,
                "discovered_at": _now_iso(),
            })
            continue

        if not _is_probably_html(ctype):
            storage.write_error({
                "url": final_url,
                "error_type": "non_html",
                "message": f"Content-Type not HTML: {ctype}",
                "http_status": status,
                "discovered_at": _now_iso(),
            })
            continue

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
            )
            storage.write_page(page_row)
            for tr in tag_rows:
                storage.write_tag_row(tr)
        except Exception as e:
            logger.exception("Inventory parse failed for %s: %s", final_url, e)
            storage.write_error({
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
            storage.write_edge(e_row)

        for ar in asset_rows:
            if config.ASSET_HEAD_METADATA:
                ct, cl = head_asset(ar["asset_url"])
                ar["head_content_type"] = ct
                ar["head_content_length"] = cl
            storage.write_asset(ar, ar["category"])
            assets_from_pages += 1

        for link in html_links:
            if link in visited:
                continue
            norm = link.rstrip("/") or link
            if norm in queued:
                continue
            if not is_allowed_domain(link):
                continue
            if parser_module.asset_category_for_url(link) is not None:
                continue
            queue.append((norm, final_url, depth + 1))
            queued.add(norm)

        if on_progress:
            on_progress(pages_crawled, assets_from_pages, final_url)

        time.sleep(delay)

    return pages_crawled, assets_from_pages
