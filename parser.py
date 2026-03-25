"""
HTML parsing: links (HTML vs downloadable assets), metadata, tags, JSON-LD,
and URL-based content hints for NHS Collector.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

import config


def _normalise_url(url: str, base_url: str) -> Optional[str]:
    try:
        full = urljoin(base_url, url)
        parsed = urlparse(full)
        without_fragment = parsed._replace(fragment="").geturl()
        return without_fragment.rstrip("/") or without_fragment
    except Exception:
        return None


def _is_allowed_domain(url: str) -> bool:
    try:
        netloc = urlparse(url).netloc.lower()
        return any(domain in netloc for domain in config.ALLOWED_DOMAINS)
    except Exception:
        return False


def _path_extension_lower(path: str) -> str:
    lower = path.lower()
    for ext in sorted(config.SKIP_EXTENSIONS, key=len, reverse=True):
        if lower.endswith(ext):
            return ext
    return ""


def asset_category_for_url(url: str) -> Optional[str]:
    """Return asset category for a downloadable URL, or None if not a skipped extension."""
    ext = _path_extension_lower(urlparse(url).path)
    if not ext:
        return None
    if ext not in tuple(config.SKIP_EXTENSIONS):
        return None
    return config.ASSET_CATEGORY_BY_EXT.get(ext, "other")


def is_skippable_asset_url(url: str) -> bool:
    return asset_category_for_url(url) is not None


def get_visible_text(soup: BeautifulSoup) -> str:
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)


def _meta_content(soup: BeautifulSoup, attrs: dict) -> str:
    tag = soup.find("meta", attrs=attrs)
    if tag and tag.get("content"):
        return str(tag["content"]).strip()
    return ""


def _all_meta_properties(soup: BeautifulSoup, prop: str) -> List[str]:
    out: List[str] = []
    for tag in soup.find_all("meta", attrs={"property": prop}):
        c = tag.get("content")
        if c:
            out.append(str(c).strip())
    return out


def _collect_json_ld_nodes(obj: Any) -> List[dict]:
    nodes: List[dict] = []
    if isinstance(obj, dict):
        if "@graph" in obj and isinstance(obj["@graph"], list):
            for item in obj["@graph"]:
                nodes.extend(_collect_json_ld_nodes(item))
        else:
            nodes.append(obj)
    elif isinstance(obj, list):
        for item in obj:
            nodes.extend(_collect_json_ld_nodes(item))
    return nodes


def _extract_json_ld(soup: BeautifulSoup) -> Tuple[List[str], List[str]]:
    types: List[str] = []
    keywords: List[str] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for node in _collect_json_ld_nodes(data):
            if not isinstance(node, dict):
                continue
            t = node.get("@type")
            if isinstance(t, list):
                types.extend(str(x) for x in t)
            elif t:
                types.append(str(t))
            kw = node.get("keywords")
            if isinstance(kw, list):
                keywords.extend(str(x).strip() for x in kw if str(x).strip())
            elif isinstance(kw, str) and kw.strip():
                keywords.append(kw.strip())
    # De-duplicate preserving order
    def dedupe(seq: List[str]) -> List[str]:
        seen: Set[str] = set()
        out: List[str] = []
        for x in seq:
            k = x.lower()
            if k not in seen:
                seen.add(k)
                out.append(x)
        return out

    return dedupe(types), dedupe(keywords)


def _rel_tag_hrefs(soup: BeautifulSoup, base_url: str) -> List[str]:
    tags: List[str] = []
    for a in soup.find_all("a", rel=True, href=True):
        rels = a.get("rel") or []
        if isinstance(rels, str):
            rels = rels.split()
        if "tag" not in [r.lower() for r in rels]:
            continue
        text = (a.get_text() or "").strip()
        if text:
            tags.append(text)
    return tags


def _collect_all_tags(soup: BeautifulSoup, base_url: str) -> List[Tuple[str, str]]:
    """Return list of (value, source)."""
    pairs: List[Tuple[str, str]] = []

    for name in ("news_keywords", "keywords", "subject"):
        content = _meta_content(soup, attrs={"name": name})
        if content:
            for part in re.split(r"[,;|]", content):
                p = part.strip()
                if p:
                    pairs.append((p, f"meta:{name}"))

    for prop in ("article:tag", "article:section"):
        for val in _all_meta_properties(soup, prop):
            if val:
                pairs.append((val, f"og:{prop}"))

    _, jld_kw = _extract_json_ld(soup)
    for k in jld_kw:
        pairs.append((k, "json_ld:keywords"))

    for t in _rel_tag_hrefs(soup, base_url):
        pairs.append((t, "rel:tag"))

    seen: Set[Tuple[str, str]] = set()
    unique: List[Tuple[str, str]] = []
    for p in pairs:
        key = (p[0].lower(), p[1])
        if key not in seen:
            seen.add(key)
            unique.append(p)
    return unique


def url_content_hint(url: str) -> str:
    """Heuristic label from path segments (blog, news, etc.)."""
    path = urlparse(url).path.lower()
    hints: List[str] = []
    for token, label in (
        ("/blog", "blog_path"),
        ("/blogs/", "blog_path"),
        ("/news", "news_path"),
        ("/newsroom", "news_path"),
        ("/publication", "publication_path"),
        ("/publications", "publication_path"),
        ("/gp/", "gp_path"),
        ("/service", "service_path"),
        ("/services/", "service_path"),
        ("/about", "about_path"),
        ("/contact", "contact_path"),
        ("/jobs", "jobs_path"),
        ("/careers", "careers_path"),
    ):
        if token in path:
            hints.append(label)
    return "|".join(hints) if hints else ""


def guess_content_kind(
    url_hint: str,
    json_ld_types: List[str],
    og_type: str,
    path: str,
) -> str:
    types_lower = " ".join(json_ld_types).lower()
    og_l = (og_type or "").lower()
    path_l = path.lower()
    if "blogposting" in types_lower or "blog_path" in url_hint:
        return "blog"
    if "newsarticle" in types_lower or "news_path" in url_hint:
        return "news"
    if "article" in types_lower and "blog" not in path_l:
        return "article"
    if "webpage" in types_lower or og_l in ("website", "article"):
        return "webpage"
    if "medicalwebpage" in types_lower or "medicalbusiness" in types_lower:
        return "medical_page"
    if "collectionpage" in types_lower or "itemlist" in types_lower:
        return "listing"
    if "faqpage" in types_lower:
        return "faq"
    if "contact_path" in url_hint:
        return "contact"
    if "about_path" in url_hint:
        return "about"
    return "unknown"


def extract_classified_links(
    html: str,
    base_url: str,
    discovered_at: str,
) -> Tuple[Set[str], List[dict], List[dict]]:
    """
    Returns:
      html_urls — same-host URLs to crawl as HTML (not asset extensions).
      asset_rows — dicts ready for storage.write_asset (without head_* filled).
      edge_rows — dicts for storage.write_edge.
    """
    soup = BeautifulSoup(html, "lxml")
    html_urls: Set[str] = set()
    asset_rows: List[dict] = []
    edge_rows: List[dict] = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:"):
            continue
        if href.startswith("javascript:"):
            continue
        full = _normalise_url(href, base_url)
        if not full or not _is_allowed_domain(full):
            continue
        anchor = (a.get_text() or "").strip()
        edge_rows.append({
            "from_url": base_url,
            "to_url": full,
            "link_text": anchor,
            "discovered_at": discovered_at,
        })
        cat = asset_category_for_url(full)
        if cat is not None:
            asset_rows.append({
                "referrer_page_url": base_url,
                "asset_url": full,
                "link_text": anchor,
                "category": cat,
                "head_content_type": "",
                "head_content_length": "",
                "discovered_at": discovered_at,
            })
        else:
            html_urls.add(full)

    return html_urls, asset_rows, edge_rows


def _document_title_from_soup(soup: BeautifulSoup) -> str:
    """Text inside <title>, including when the tag has nested elements."""
    tag = soup.find("title")
    if not tag:
        return ""
    text = tag.get_text(separator=" ", strip=True)
    return text.strip()


def build_page_inventory_row(
    html: str,
    requested_url: str,
    final_url: str,
    http_status: int,
    content_type: str,
    referrer_url: str,
    depth: int,
    discovered_at: str,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Returns the pages.csv row and tag detail rows for tags.csv.
    """
    soup = BeautifulSoup(html, "lxml")

    desc = _meta_content(soup, attrs={"name": "description"})
    if not desc:
        desc = _meta_content(soup, attrs={"property": "og:description"})

    lang = ""
    html_tag = soup.find("html")
    if html_tag and html_tag.get("lang"):
        lang = str(html_tag["lang"]).strip()

    canonical = ""
    link_can = soup.find("link", attrs={"rel": "canonical", "href": True})
    if link_can and link_can.get("href"):
        canonical = _normalise_url(link_can["href"], final_url) or link_can["href"].strip()

    og_title = _meta_content(soup, attrs={"property": "og:title"})
    og_type = _meta_content(soup, attrs={"property": "og:type"})
    og_description = _meta_content(soup, attrs={"property": "og:description"})
    twitter_card = _meta_content(soup, attrs={"name": "twitter:card"})

    json_types, _ = _extract_json_ld(soup)
    json_ld_types_str = "|".join(json_types)

    tag_pairs = _collect_all_tags(soup, final_url)
    tags_all = "|".join(t[0] for t in tag_pairs)

    hint = url_content_hint(final_url)
    path = urlparse(final_url).path
    kind = guess_content_kind(hint, json_types, og_type, path)

    h1_texts: List[str] = []
    for h in soup.find_all("h1", limit=5):
        t = h.get_text(separator=" ", strip=True)
        if t:
            h1_texts.append(t)
    h1_joined = " | ".join(h1_texts)

    doc_title = _document_title_from_soup(soup)
    title = doc_title or og_title or (h1_texts[0] if h1_texts else "")

    visible = get_visible_text(BeautifulSoup(html, "lxml"))
    word_count = len(visible.split()) if visible else 0

    domain = urlparse(final_url).netloc.lower()

    page_row = {
        "requested_url": requested_url,
        "final_url": final_url,
        "domain": domain,
        "http_status": http_status,
        "content_type": content_type,
        "title": title,
        "meta_description": desc,
        "lang": lang,
        "canonical_url": canonical or "",
        "og_title": og_title,
        "og_type": og_type,
        "og_description": og_description,
        "twitter_card": twitter_card,
        "json_ld_types": json_ld_types_str,
        "tags_all": tags_all,
        "url_content_hint": hint,
        "content_kind_guess": kind,
        "h1_joined": h1_joined,
        "word_count": word_count,
        "referrer_url": referrer_url,
        "depth": depth,
        "discovered_at": discovered_at,
    }

    tag_rows = [
        {
            "page_url": final_url,
            "tag_value": tv,
            "tag_source": src,
            "discovered_at": discovered_at,
        }
        for tv, src in tag_pairs
    ]

    return page_row, tag_rows
