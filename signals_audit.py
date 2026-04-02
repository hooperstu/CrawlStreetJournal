"""
Signal audit module for The Crawl Street Journal.

Inventories every metadata signal present on a page — whether currently
extracted by the main parser or not.  Designed for research: run against a
diverse corpus and aggregate the output to discover which signals are most
common in the wild, then prioritise new extractors accordingly.

Usage::

    from signals_audit import audit_page, summarise_audit
    report = audit_page(html, url="https://example.com/page",
                        response_headers={"Server": "nginx"})
    summary = summarise_audit(report)
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup


def audit_page(
    html: str,
    url: str = "",
    response_headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Return a comprehensive inventory of metadata signals on the page.

    The result dict groups signals by category, each containing a list of
    found items.  This is intended for analysis, not for CSV output.
    """
    soup = BeautifulSoup(html, "lxml")
    headers = response_headers or {}

    return {
        "url": url,
        "meta_tags": _audit_meta_tags(soup),
        "link_tags": _audit_link_tags(soup, url),
        "json_ld": _audit_json_ld(soup),
        "microdata": _audit_microdata(soup),
        "rdfa": _audit_rdfa(soup),
        "open_graph": _audit_open_graph(soup),
        "twitter_cards": _audit_twitter_cards(soup),
        "html_signals": _audit_html_signals(soup),
        "response_headers": _audit_response_headers(headers),
        "data_attributes": _audit_data_attributes(soup),
        "time_elements": _audit_time_elements(soup),
    }


# ── Meta tags ─────────────────────────────────────────────────────────────

def _audit_meta_tags(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """Catalogue every ``<meta>`` tag."""
    results: List[Dict[str, str]] = []
    for tag in soup.find_all("meta"):
        entry: Dict[str, str] = {}
        for attr in ("name", "property", "http-equiv", "charset", "itemprop"):
            val = tag.get(attr)
            if val:
                entry[attr] = str(val).strip()
        content = tag.get("content")
        if content is not None:
            entry["content"] = str(content).strip()[:500]
        if entry:
            results.append(entry)
    return results


# ── Link tags ─────────────────────────────────────────────────────────────

def _audit_link_tags(
    soup: BeautifulSoup, base_url: str,
) -> List[Dict[str, str]]:
    """Catalogue every ``<link>`` tag."""
    results: List[Dict[str, str]] = []
    for tag in soup.find_all("link"):
        entry: Dict[str, str] = {}
        rel = tag.get("rel", [])
        if isinstance(rel, list):
            rel = " ".join(rel)
        entry["rel"] = str(rel).strip()
        href = tag.get("href", "")
        if href:
            try:
                entry["href"] = urljoin(base_url, href.strip())
            except Exception:
                entry["href"] = href.strip()
        for attr in ("type", "hreflang", "media", "title", "sizes"):
            val = tag.get(attr)
            if val:
                entry[attr] = str(val).strip()
        if entry.get("rel") or entry.get("href"):
            results.append(entry)
    return results


# ── JSON-LD ───────────────────────────────────────────────────────────────

def _flatten_json_ld(obj: Any) -> List[dict]:
    """Recursively collect JSON-LD node dicts (handling ``@graph``)."""
    nodes: List[dict] = []
    if isinstance(obj, dict):
        if "@graph" in obj and isinstance(obj["@graph"], list):
            for item in obj["@graph"]:
                nodes.extend(_flatten_json_ld(item))
        else:
            nodes.append(obj)
    elif isinstance(obj, list):
        for item in obj:
            nodes.extend(_flatten_json_ld(item))
    return nodes


def _audit_json_ld(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Parse and return all JSON-LD blocks with their top-level keys."""
    results: List[Dict[str, Any]] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            results.append({"_parse_error": True, "_raw_preview": raw[:200]})
            continue
        for node in _flatten_json_ld(data):
            if not isinstance(node, dict):
                continue
            results.append({
                "@type": node.get("@type", ""),
                "@id": node.get("@id", ""),
                "_keys": sorted(node.keys()),
            })
    return results


# ── Microdata ─────────────────────────────────────────────────────────────

def _audit_microdata(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Catalogue Microdata (``itemscope``/``itemprop``) on the page."""
    results: List[Dict[str, Any]] = []
    for el in soup.find_all(attrs={"itemscope": True}):
        item_type = el.get("itemtype", "")
        props: List[Dict[str, str]] = []
        for prop_el in el.find_all(attrs={"itemprop": True}):
            prop_name = prop_el.get("itemprop", "")
            prop_val = (
                prop_el.get("content")
                or prop_el.get("href")
                or prop_el.get("src")
                or prop_el.get("datetime")
                or prop_el.get_text(strip=True)[:200]
            )
            props.append({
                "prop": str(prop_name),
                "value": str(prop_val or "")[:200],
            })
        results.append({"itemtype": item_type, "properties": props})
    return results


# ── RDFa ──────────────────────────────────────────────────────────────────

def _audit_rdfa(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Catalogue RDFa (``typeof``/``property``) on the page."""
    results: List[Dict[str, Any]] = []
    for el in soup.find_all(attrs={"typeof": True}):
        rdfa_type = el.get("typeof", "")
        props: List[Dict[str, str]] = []
        for prop_el in el.find_all(attrs={"property": True}):
            prop_name = prop_el.get("property", "")
            if prop_name.startswith("og:"):
                continue
            prop_val = (
                prop_el.get("content")
                or prop_el.get("href")
                or prop_el.get("src")
                or prop_el.get_text(strip=True)[:200]
            )
            props.append({
                "prop": str(prop_name),
                "value": str(prop_val or "")[:200],
            })
        if props:
            results.append({"typeof": rdfa_type, "properties": props})
    return results


# ── Open Graph ────────────────────────────────────────────────────────────

def _audit_open_graph(soup: BeautifulSoup) -> Dict[str, str]:
    """Collect all Open Graph meta properties."""
    og: Dict[str, str] = {}
    _OG_PREFIXES = ("og:", "article:", "music:", "video:", "book:", "profile:")
    for tag in soup.find_all("meta", attrs={"property": True}):
        prop = str(tag.get("property", "")).strip()
        if any(prop.startswith(p) for p in _OG_PREFIXES):
            content = str(tag.get("content", "")).strip()[:500]
            if prop in og:
                og[prop] += f"|{content}"
            else:
                og[prop] = content
    return og


# ── Twitter Cards ─────────────────────────────────────────────────────────

def _audit_twitter_cards(soup: BeautifulSoup) -> Dict[str, str]:
    """Collect all Twitter Card meta tags."""
    tc: Dict[str, str] = {}
    for tag in soup.find_all("meta", attrs={"name": True}):
        name = str(tag.get("name", "")).strip()
        if name.startswith("twitter:"):
            tc[name] = str(tag.get("content", "")).strip()[:500]
    return tc


# ── HTML structural signals ──────────────────────────────────────────────

def _audit_html_signals(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extract structural HTML signals."""
    html_tag = soup.find("html")
    body_tag = soup.find("body")
    return {
        "html_lang": str(html_tag.get("lang", "")) if html_tag else "",
        "html_dir": str(html_tag.get("dir", "")) if html_tag else "",
        "body_classes": " ".join(body_tag.get("class", [])) if body_tag else "",
        "body_id": str(body_tag.get("id", "")) if body_tag else "",
        "has_main": bool(soup.find("main") or soup.find(attrs={"role": "main"})),
        "has_nav": bool(soup.find("nav")),
        "has_article": bool(soup.find("article")),
        "has_aside": bool(soup.find("aside")),
        "has_header": bool(soup.find("header")),
        "has_footer": bool(soup.find("footer")),
        "has_section": bool(soup.find("section")),
        "has_form": bool(soup.find("form")),
        "heading_tags": [
            h.name for h in soup.find_all(re.compile(r"^h[1-6]$"), limit=20)
        ],
    }


# ── Response headers ─────────────────────────────────────────────────────

def _audit_response_headers(headers: Dict[str, str]) -> Dict[str, str]:
    """Extract interesting response headers (case-insensitive lookup)."""
    _INTERESTING = (
        "Server", "X-Powered-By", "X-Generator", "X-Robots-Tag",
        "X-Content-Type-Options", "X-Frame-Options",
        "Content-Security-Policy", "Strict-Transport-Security",
        "Link", "X-Drupal-Cache", "X-Drupal-Dynamic-Cache",
        "X-Varnish", "X-Cache", "Via",
    )
    result: Dict[str, str] = {}
    headers_lower = {k.lower(): v for k, v in headers.items()}
    for key in _INTERESTING:
        val = headers_lower.get(key.lower(), "")
        if val:
            result[key] = str(val).strip()[:500]
    return result


# ── data-* attributes ────────────────────────────────────────────────────

def _audit_data_attributes(soup: BeautifulSoup) -> List[str]:
    """Collect unique ``data-*`` attribute names from key structural elements."""
    attrs_found: set = set()
    _TAGS = ("body", "main", "article", "div", "section", "header", "footer")
    for el in soup.find_all(_TAGS, limit=200):
        for attr_name in el.attrs:
            if isinstance(attr_name, str) and attr_name.startswith("data-"):
                attrs_found.add(attr_name)
    return sorted(attrs_found)


# ── <time> elements ──────────────────────────────────────────────────────

def _audit_time_elements(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """Catalogue ``<time>`` elements with ``datetime`` attributes."""
    results: List[Dict[str, str]] = []
    for t in soup.find_all("time", limit=20):
        entry: Dict[str, str] = {}
        if t.get("datetime"):
            entry["datetime"] = str(t["datetime"]).strip()
        for attr in ("itemprop", "class", "pubdate"):
            val = t.get(attr)
            if val:
                if isinstance(val, list):
                    val = " ".join(val)
                entry[attr] = str(val).strip()
        entry["text"] = t.get_text(strip=True)[:200]
        results.append(entry)
    return results


# ── Summary for CSV output ───────────────────────────────────────────────

def summarise_audit(report: Dict[str, Any]) -> Dict[str, Any]:
    """Create a flat summary suitable for CSV output from an audit report."""
    meta_names = [
        m.get("name", "") for m in report.get("meta_tags", []) if m.get("name")
    ]
    meta_properties = [
        m.get("property", "") for m in report.get("meta_tags", [])
        if m.get("property")
    ]
    jld_types = [
        j.get("@type", "") for j in report.get("json_ld", [])
        if j.get("@type")
    ]
    jld_keys: set = set()
    for j in report.get("json_ld", []):
        jld_keys.update(j.get("_keys", []))
    md_types = [
        m.get("itemtype", "") for m in report.get("microdata", [])
        if m.get("itemtype")
    ]
    rdfa_types = [
        r.get("typeof", "") for r in report.get("rdfa", [])
        if r.get("typeof")
    ]

    html_signals = report.get("html_signals", {})
    resp_headers = report.get("response_headers", {})

    return {
        "url": report.get("url", ""),
        "meta_names": "|".join(sorted(set(meta_names))),
        "meta_properties": "|".join(sorted(set(meta_properties))),
        "json_ld_types": "|".join(jld_types),
        "json_ld_keys": "|".join(sorted(jld_keys)),
        "microdata_types": "|".join(md_types),
        "rdfa_types": "|".join(rdfa_types),
        "og_properties": "|".join(sorted(report.get("open_graph", {}).keys())),
        "twitter_properties": "|".join(
            sorted(report.get("twitter_cards", {}).keys())
        ),
        "generator": next(
            (m.get("content", "") for m in report.get("meta_tags", [])
             if (m.get("name") or "").lower() == "generator"),
            "",
        ),
        "server": resp_headers.get("Server", ""),
        "x_powered_by": resp_headers.get("X-Powered-By", ""),
        "body_classes": html_signals.get("body_classes", ""),
        "has_microdata": bool(md_types),
        "has_rdfa": bool(rdfa_types),
        "has_json_ld": bool(jld_types),
        "data_attributes": "|".join(report.get("data_attributes", [])),
    }
