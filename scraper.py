"""
Collector crawler: robots.txt, fetch, sitemap seeding, inventory rows,
and polite rate limiting.
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
import parser as parser_module
import sitemap as sitemap_module
import storage

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
        # Strip default ports (80 and 443 are never meaningful)
        if netloc.endswith(":443"):
            netloc = netloc[:-4]
        elif netloc.endswith(":80"):
            netloc = netloc[:-3]
        path = p.path or "/"
        # Sort query parameters for consistent ordering
        query = urlencode(sorted(parse_qsl(p.query, keep_blank_values=True)))
        normalised = urlunparse((scheme, netloc, path, p.params, query, ""))
        return normalised.rstrip("/") or normalised
    except Exception:
        return url.rstrip("/") or url

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
    if not config.RESPECT_ROBOTS_TXT:
        return True
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


def fetch_page(url: str) -> Tuple[Optional[str], int, str, str, Dict[str, str]]:
    """
    GET *url*.  Returns ``(body, status_code, final_url, content_type, response_meta)``.

    *response_meta* carries selected headers (``Last-Modified``, ``ETag``)
    when ``CAPTURE_RESPONSE_HEADERS`` is enabled; otherwise an empty dict.
    On failure *body* is ``None``; *final_url* and *content_type* may be empty.
    """
    headers = {"User-Agent": config.USER_AGENT}
    empty_meta: Dict[str, str] = {}
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
            meta = empty_meta
            if config.CAPTURE_RESPONSE_HEADERS:
                meta = {
                    "last_modified": (resp.headers.get("Last-Modified") or "").strip(),
                    "etag": (resp.headers.get("ETag") or "").strip(),
                }
            if status >= 400:
                return None, status, final, ctype, meta
            return resp.text, status, final, ctype, meta
        except requests.exceptions.Timeout:
            logger.warning("Timeout fetching %s (attempt %s)", url, attempt + 1)
            if attempt == config.MAX_RETRIES:
                return None, 0, url, "", empty_meta
        except requests.exceptions.RequestException as e:
            status = getattr(e.response, "status_code", None) if hasattr(e, "response") else None
            logger.warning("Request failed for %s: %s (status=%s)", url, e, status)
            if status == 429:
                time.sleep(5)
            if attempt == config.MAX_RETRIES:
                return None, status or 0, url, "", empty_meta
        except Exception as e:
            logger.warning("Error fetching %s: %s", url, e)
            if attempt == config.MAX_RETRIES:
                return None, 0, url, "", empty_meta
    return None, 0, url, "", empty_meta


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


def collect_start_items() -> Tuple[List[Tuple[str, str]], Dict[str, Dict[str, str]]]:
    """
    Return ``(items, sitemap_meta)`` where *items* is a de-duplicated list
    of ``(url, referrer_label)`` and *sitemap_meta* maps normalised URL →
    ``{"sitemap_lastmod": ..., "source_sitemap": ...}``.
    """
    items: List[Tuple[str, str]] = []
    seen: Set[str] = set()
    sitemap_meta: Dict[str, Dict[str, str]] = {}

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
        try:
            entries = sitemap_module.collect_urls_from_sitemap(
                sm_url, max_urls=config.MAX_SITEMAP_URLS
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
            storage.write_sitemap_url({
                "url": loc,
                "lastmod": lastmod,
                "source_sitemap": sm_url,
                "discovered_at": now,
            })
            add(loc, label)

    for u in config.SEED_URLS:
        add(u.strip(), "seed")

    for sm in config.SITEMAP_URLS:
        _process_sitemap(sm)

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
                _process_sitemap(sm)

    return items, sitemap_meta


def _check_outbound_links(
    edge_rows: List[Dict[str, str]], discovered_at: str,
) -> None:
    """HEAD-check a sample of outbound link targets and write results."""
    seen: Set[str] = set()
    checked = 0
    for row in edge_rows:
        target = row["to_url"]
        if target in seen:
            continue
        seen.add(target)
        if checked >= config.MAX_LINK_CHECKS_PER_PAGE:
            break
        try:
            resp = requests.head(
                target,
                headers={"User-Agent": config.USER_AGENT},
                timeout=config.HEAD_TIMEOUT_SECONDS,
                allow_redirects=True,
            )
            check_status = resp.status_code
            check_final = resp.url
        except Exception:
            check_status = 0
            check_final = target
        storage.write_link_check({
            "from_url": row["from_url"],
            "to_url": target,
            "check_status": check_status,
            "check_final_url": check_final,
            "discovered_at": discovered_at,
        })
        checked += 1
        time.sleep(config.LINK_CHECK_DELAY_SECONDS)


def _resolve_delay(value: Union[float, Tuple[float, float]]) -> float:
    """Return a sleep duration — fixed or random within a (min, max) range."""
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return random.uniform(value[0], value[1])
    return float(value)


def crawl(
    seed_urls: Optional[List[str]] = None,
    max_pages: Optional[int] = None,
    delay: Optional[Union[float, Tuple[float, float]]] = None,
    on_progress: Optional[Callable[[int, int, str], None]] = None,
    should_stop: Optional[Callable[[], bool]] = None,
    run_name: Optional[str] = None,
    run_folder: Optional[str] = None,
    resume: bool = False,
) -> Tuple[int, int]:
    """
    Crawl HTML pages up to max_pages; record inventory and linked assets.
    Returns (pages_crawled, assets_recorded_from_page_links).

    When *run_folder* is supplied with *resume=True*, the crawl picks up
    from where it left off: visited URLs are rebuilt from existing CSVs,
    the queue is restored from saved state, and no headers are rewritten.

    When *run_folder* is supplied without resume, a fresh crawl is started
    inside that pre-created run folder (overwriting any previous data).
    """
    max_pages = max_pages if max_pages is not None else config.MAX_PAGES_TO_CRAWL
    delay_cfg = delay if delay is not None else config.REQUEST_DELAY_SECONDS

    run_dir = None

    if resume and run_folder:
        run_dir = os.path.join(config.OUTPUT_DIR, run_folder)
        saved_cfg = storage.load_run_config(run_dir)
        if saved_cfg:
            storage.apply_run_config(saved_cfg)
            max_pages = saved_cfg.get("MAX_PAGES_TO_CRAWL", max_pages)
            delay_cfg = saved_cfg.get("REQUEST_DELAY_SECONDS", delay_cfg)
            if isinstance(delay_cfg, list) and len(delay_cfg) == 2:
                delay_cfg = tuple(delay_cfg)
        storage.resume_outputs(run_folder)
    elif run_folder:
        run_dir = os.path.join(config.OUTPUT_DIR, run_folder)
        saved_cfg = storage.load_run_config(run_dir)
        if saved_cfg:
            storage.apply_run_config(saved_cfg)
            max_pages = saved_cfg.get("MAX_PAGES_TO_CRAWL", max_pages)
            delay_cfg = saved_cfg.get("REQUEST_DELAY_SECONDS", delay_cfg)
            if isinstance(delay_cfg, list) and len(delay_cfg) == 2:
                delay_cfg = tuple(delay_cfg)
        storage.initialise_outputs(run_folder=run_folder, run_name=run_name)
    else:
        storage.initialise_outputs(run_name=run_name)

    run_dir = storage.get_active_run_dir()

    queue: deque[Tuple[str, str, int]] = deque()
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
                        queue.append((u, ref, int(depth)))
                        queued.add(u)
        logger.info(
            "Resumed: %d visited, %d in queue, %d pages already crawled",
            len(visited), len(queue), pages_crawled,
        )
    else:
        if seed_urls is not None:
            start_items = [(normalise_url(u.strip()), "seed") for u in seed_urls if u.strip()]
        else:
            start_items, sitemap_meta = collect_start_items()

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

    if resume and run_folder and saved_state and saved_state.get("started_at"):
        started_at = saved_state["started_at"]
    else:
        started_at = _now_iso()
    storage.save_crawl_state(
        run_dir,
        status="running",
        pages_crawled=pages_crawled,
        assets_from_pages=assets_from_pages,
        queue=list(queue),
        started_at=started_at,
    )

    interrupted = False

    while queue and pages_crawled < max_pages:
        if should_stop and should_stop():
            interrupted = True
            break
        url, referrer, depth = queue.popleft()
        url_key = normalise_url(url)
        if url_key in visited:
            continue
        visited.add(url_key)

        if not can_fetch(url):
            storage.write_error({
                "url": url,
                "error_type": "robots_disallowed",
                "message": "Blocked by robots.txt",
                "http_status": "",
                "discovered_at": _now_iso(),
            })
            continue

        html, status, final_url, ctype, resp_meta = fetch_page(url)
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
            storage.write_page(page_row)
            for tr in tag_rows:
                storage.write_tag_row(tr)
            if config.WRITE_NAV_LINKS_CSV:
                try:
                    from bs4 import BeautifulSoup as _BS
                    nav_rows = parser_module.extract_nav_links(
                        _BS(html, "lxml"), final_url, now,
                    )
                    for nr in nav_rows:
                        storage.write_nav_link(nr)
                except Exception as nav_err:
                    logger.debug("Nav extraction failed for %s: %s", final_url, nav_err)
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

        if config.CHECK_OUTBOUND_LINKS and edge_rows:
            _check_outbound_links(edge_rows, now)

        for ar in asset_rows:
            if config.ASSET_HEAD_METADATA:
                ct, cl = head_asset(ar["asset_url"])
                ar["head_content_type"] = ct
                ar["head_content_length"] = cl
            storage.write_asset(ar, ar["category"])
            assets_from_pages += 1

        for link in html_links:
            norm = normalise_url(link)
            if norm in visited or norm in queued:
                continue
            if not is_allowed_domain(link):
                continue
            if parser_module.asset_category_for_url(link) is not None:
                continue
            queue.append((norm, final_url, depth + 1))
            queued.add(norm)

        if on_progress:
            on_progress(pages_crawled, assets_from_pages, final_url)

        time.sleep(_resolve_delay(delay_cfg))

    final_status = "interrupted" if interrupted else "completed"
    storage.save_crawl_state(
        run_dir,
        status=final_status,
        pages_crawled=pages_crawled,
        assets_from_pages=assets_from_pages,
        queue=list(queue),
        started_at=started_at,
        stopped_at=_now_iso(),
    )

    return pages_crawled, assets_from_pages
