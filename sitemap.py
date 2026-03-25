"""
Fetch and parse sitemap XML (urlset or sitemap index) into location URLs.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from typing import List, Set
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


def parse_sitemap_xml(xml_text: str, base_url: str) -> tuple[Set[str], Set[str]]:
    """
    Returns (child_sitemap_urls, page_urls).
    """
    child_maps: Set[str] = set()
    page_urls: Set[str] = set()
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
                full = urljoin(base_url, loc)
                child_maps.add(full)
        elif name == "url":
            loc = None
            for child in el:
                if _local_tag(child.tag) == "loc" and child.text:
                    loc = child.text.strip()
                    break
            if loc:
                page_urls.add(urljoin(base_url, loc))

    return child_maps, page_urls


def collect_urls_from_sitemap(
    start_url: str,
    max_urls: int,
    visited_maps: Set[str] | None = None,
) -> List[str]:
    """
    Recursively expand sitemap indexes up to max_urls page locations.
    """
    visited_maps = visited_maps if visited_maps is not None else set()
    out: List[str] = []
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
        children, pages = parse_sitemap_xml(xml_text, sm_url)
        for p in pages:
            if len(out) >= max_urls:
                break
            out.append(p)
        for child_map in children:
            if child_map not in visited_maps:
                queue.append(child_map)

    return out[:max_urls]
