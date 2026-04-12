"""
Fetch and parse sitemap XML (urlset or sitemap index) into location URLs
with optional ``<lastmod>`` timestamps.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from collections import deque
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests

import config
from outbound_http import request_get_streaming

logger = logging.getLogger(__name__)


def _local_tag(tag: str) -> str:
    """Strip the XML namespace URI from an element tag, e.g. ``{http://...}url`` -> ``url``."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _fetch_xml(
    url: str,
    user_agent: str = "",
    timeout: int = 0,
    verify: Optional[bool] = None,
    max_redirects: Optional[int] = None,
    block_private: Optional[bool] = None,
    max_body_bytes: Optional[int] = None,
) -> str:
    ua = user_agent or config.USER_AGENT
    to = timeout or config.REQUEST_TIMEOUT_SECONDS
    v = config.HTTP_VERIFY_SSL if verify is None else verify
    mr = config.HTTP_MAX_REDIRECTS if max_redirects is None else max_redirects
    try:
        mr = int(mr)
    except (TypeError, ValueError):
        mr = 30
    mr = max(1, min(mr, 1000))
    block = config.BLOCK_PRIVATE_OUTBOUND if block_private is None else block_private
    cap = config.MAX_SITEMAP_RESPONSE_BYTES if max_body_bytes is None else int(max_body_bytes)
    with requests.Session() as sess:
        sess.verify = v
        sess.headers.update({"User-Agent": ua})
        raw, status, _fin, _ct, err, _rh, _rc, _lr = request_get_streaming(
            sess,
            url,
            timeout=float(to),
            max_redirects=mr,
            max_body_bytes=cap,
            block_private=bool(block),
        )
    if err:
        raise RuntimeError(err)
    if status >= 400:
        raise RuntimeError(f"HTTP {status}")
    return (raw or b"").decode("utf-8", errors="replace")


def parse_sitemap_xml(
    xml_text: str, base_url: str,
) -> Tuple[Set[str], Dict[str, str]]:
    """
    Returns (child_sitemap_urls, page_url_to_lastmod).

    ``page_url_to_lastmod`` maps each ``<loc>`` to its ``<lastmod>`` value
    (empty string when not present in the XML).
    """
    child_maps: Set[str] = set()
    page_urls: Dict[str, str] = {}
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.warning("Sitemap XML parse error for base %s: %s", base_url, e)
        return child_maps, page_urls

    for el in root.iter():
        # Sitemap XML may declare a default namespace (e.g.
        # xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"); _local_tag
        # strips it so tag names compare cleanly regardless.
        name = _local_tag(el.tag)
        if name == "sitemap":
            loc = None
            for child in el:
                if _local_tag(child.tag) == "loc" and child.text:
                    loc = child.text.strip()
                    break
            if loc:
                child_maps.add(urljoin(base_url, loc))
        elif name == "url":
            loc = None
            lastmod = ""
            for child in el:
                child_name = _local_tag(child.tag)
                if child_name == "loc" and child.text:
                    loc = child.text.strip()
                elif child_name == "lastmod" and child.text:
                    lastmod = child.text.strip()
            if loc:
                page_urls[urljoin(base_url, loc)] = lastmod

    return child_maps, page_urls


def collect_urls_from_sitemap(
    start_url: str,
    max_urls: int,
    visited_maps: Set[str] | None = None,
    *,
    http_verify: Optional[bool] = None,
    http_max_redirects: Optional[int] = None,
    block_private_outbound: Optional[bool] = None,
    max_sitemap_bytes: Optional[int] = None,
) -> List[Tuple[str, str]]:
    """
    Recursively expand sitemap indexes up to *max_urls* page locations.

    Returns a list of ``(url, lastmod)`` pairs.  *lastmod* is the raw
    W3C date string from the sitemap, or ``""`` when absent.
    """
    visited_maps = visited_maps if visited_maps is not None else set()
    out: List[Tuple[str, str]] = []
    queue: deque[str] = deque([start_url])

    # BFS: sitemap indexes are expanded breadth-first so that top-level
    # entries (typically the most important pages) are collected first.
    while queue and len(out) < max_urls:
        sm_url = queue.popleft()
        if sm_url in visited_maps:
            continue
        visited_maps.add(sm_url)
        try:
            xml_text = _fetch_xml(
                sm_url,
                verify=http_verify,
                max_redirects=http_max_redirects,
                block_private=block_private_outbound,
                max_body_bytes=max_sitemap_bytes,
            )
        except Exception as e:
            logger.warning("Could not fetch sitemap %s: %s", sm_url, e)
            continue
        children, page_map = parse_sitemap_xml(xml_text, sm_url)
        for url, lastmod in page_map.items():
            if len(out) >= max_urls:
                break
            out.append((url, lastmod))
        for child_map in children:
            if child_map not in visited_maps:
                queue.append(child_map)

    return out[:max_urls]
