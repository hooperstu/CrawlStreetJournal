"""
Fetch and parse sitemap XML (urlset or sitemap index) into location URLs
with optional ``<lastmod>`` timestamps.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from typing import Dict, List, Set, Tuple
from urllib.parse import urljoin

import requests

import config

logger = logging.getLogger(__name__)


def _local_tag(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _fetch_xml(url: str) -> str:
    headers = {"User-Agent": config.USER_AGENT}
    resp = requests.get(
        url,
        headers=headers,
        timeout=config.REQUEST_TIMEOUT_SECONDS,
        allow_redirects=True,
    )
    resp.raise_for_status()
    return resp.text


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
) -> List[Tuple[str, str]]:
    """
    Recursively expand sitemap indexes up to *max_urls* page locations.

    Returns a list of ``(url, lastmod)`` pairs.  *lastmod* is the raw
    W3C date string from the sitemap, or ``""`` when absent.
    """
    visited_maps = visited_maps if visited_maps is not None else set()
    out: List[Tuple[str, str]] = []
    queue: List[str] = [start_url]

    while queue and len(out) < max_urls:
        sm_url = queue.pop(0)
        if sm_url in visited_maps:
            continue
        visited_maps.add(sm_url)
        try:
            xml_text = _fetch_xml(sm_url)
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
