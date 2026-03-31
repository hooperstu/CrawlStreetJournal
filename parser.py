"""
HTML parsing: links (HTML vs downloadable assets), metadata, tags, JSON-LD,
and URL-based content hints for Collector.
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
    # Retry with case-insensitive value matching (covers ASP.NET
    # sites that emit e.g. NAME="DESCRIPTION" instead of name="description").
    ci_attrs = {
        k: re.compile(re.escape(v), re.IGNORECASE) if isinstance(v, str) else v
        for k, v in attrs.items()
    }
    tag = soup.find("meta", attrs=ci_attrs)
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


def _extract_json_ld(
    soup: BeautifulSoup,
) -> Tuple[List[str], List[str], List[str]]:
    """Return ``(types, keywords, sections)`` from JSON-LD blocks.

    *sections* collects ``articleSection`` and ``genre`` values which many
    WordPress + Yoast sites emit as categorisation signals.
    """
    types: List[str] = []
    keywords: List[str] = []
    sections: List[str] = []
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
            for field in ("articleSection", "genre"):
                val = node.get(field)
                if isinstance(val, list):
                    sections.extend(str(x).strip() for x in val if str(x).strip())
                elif isinstance(val, str) and val.strip():
                    sections.append(val.strip())

    def dedupe(seq: List[str]) -> List[str]:
        seen: Set[str] = set()
        out: List[str] = []
        for x in seq:
            k = x.lower()
            if k not in seen:
                seen.add(k)
                out.append(x)
        return out

    return dedupe(types), dedupe(keywords), dedupe(sections)


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

    _, jld_kw, jld_sections = _extract_json_ld(soup)
    for k in jld_kw:
        pairs.append((k, "json_ld:keywords"))
    for s in jld_sections:
        pairs.append((s, "json_ld:articleSection"))

    for t in _rel_tag_hrefs(soup, base_url):
        pairs.append((t, "rel:tag"))

    # /category/ and /tag/ href links (WordPress convention)
    _TAG_HREF_RE = re.compile(r"/(?:tag|category|topic)/([^/?#]+)", re.IGNORECASE)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text() or "").strip()
        if text and len(text) < 60 and _TAG_HREF_RE.search(href):
            if not re.search(r"/page/\d", href):
                pairs.append((text, "href:category"))

    # Elements with class "topics"
    for el in soup.find_all(class_=re.compile(r"\btopics?\b", re.IGNORECASE)):
        if el.name in ("nav", "header", "footer", "form"):
            continue
        for child in el.find_all(["a", "span", "li"]):
            t = child.get_text(strip=True)
            if t and len(t) < 60:
                pairs.append((t, "class:topics"))

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
        ("/events", "events_path"),
        ("/event/", "events_path"),
        ("/patients", "patients_path"),
        ("/patient-", "patients_path"),
        ("/explore-roles", "careers_path"),
        ("/working-health", "careers_path"),
        ("/training", "training_path"),
        ("/guidance", "guidance_path"),
        ("/statistics", "statistics_path"),
    ):
        if token in path:
            hints.append(label)
    return "|".join(hints) if hints else ""


def guess_content_kind(
    url_hint: str,
    json_ld_types: List[str],
    og_type: str,
    path: str,
    breadcrumb: str = "",
    body_classes: str = "",
) -> str:
    types_lower = " ".join(json_ld_types).lower()
    og_l = (og_type or "").lower()
    path_l = path.lower()
    bc_lower = breadcrumb.lower()
    body_lower = body_classes.lower()

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

    # Fallback: breadcrumb and body class signals
    if any(w in bc_lower for w in ("blog", "blogs")):
        return "blog"
    if any(w in bc_lower for w in ("news", "newsroom")):
        return "news"
    if any(w in bc_lower for w in ("publication", "publications")):
        return "publication"
    if any(w in bc_lower for w in ("event", "events")):
        return "event"
    if "about" in bc_lower:
        return "about"
    if any(w in bc_lower for w in ("contact", "get in touch")):
        return "contact"
    if any(w in bc_lower for w in ("service", "services")):
        return "service"

    if "single-post" in body_lower or "post-template" in body_lower:
        return "blog"
    if "page-template" in body_lower or "page-id" in body_lower:
        return "webpage"
    if "archive" in body_lower:
        return "listing"

    # SilverStripe page-type body classes (e.g. AboutOverviewPage,
    # NewsItemListingPage, HomePage, ContactPage)
    _SS_BODY_MAP = (
        ("homepage", "homepage"),
        ("aboutoverviewpage", "about"),
        ("newsitemlistingpage", "news"),
        ("newsitempage", "news"),
        ("contactpage", "contact"),
        ("eventpage", "event"),
        ("resourcepage", "resource"),
        ("staffpage", "staff"),
        ("blogentry", "blog"),
    )
    for token, kind in _SS_BODY_MAP:
        if token in body_lower:
            return kind

    # Drupal body class patterns (e.g. page-node-type-section-page)
    _DRUPAL_BODY_RE = re.compile(r"page-node-type-(\S+)")
    drupal_match = _DRUPAL_BODY_RE.search(body_lower)
    if drupal_match:
        node_type = drupal_match.group(1)
        if "article" in node_type or "blog" in node_type:
            return "blog"
        if "news" in node_type:
            return "news"
        if "event" in node_type:
            return "event"
        if "page" in node_type or "section" in node_type:
            return "webpage"

    if "home" in body_lower and path_l in ("/", ""):
        return "homepage"

    if path_l in ("/", ""):
        return "homepage"

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


_VISIBLE_DATE_RE = re.compile(
    r"(?:"
    r"(?:date\s+)?last\s+(?:updated|reviewed|modified)"
    r"|(?:date\s+)?published"
    r"|(?:page\s+)?last\s+(?:reviewed|updated)"
    r"|review\s+date"
    r"|posted(?:\s+on)?"
    r"|created(?:\s+on)?"
    r")"
    r"\s*[:\-–]?\s*"
    r"("
    r"\d{1,2}(?:st|nd|rd|th)?\s+\w+,?\s+\d{4}"
    r"|\w+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}"
    r"|\d{4}-\d{2}-\d{2}"
    r"|\w+\s+\d{4}"
    r")",
    re.IGNORECASE,
)

_ANALYTICS_TOKENS = (
    "googletagmanager.com",
    "google-analytics.com",
    "gtag/js",
    "dataLayer",
    "analytics.js",
    "ga.js",
)

_PRIVACY_HREF_PATTERNS = (
    "/privacy-policy",
    "/privacy",
    "/legal/privacy",
    "/cookies",
    "/cookie-policy",
)


def _extract_heading_outline(soup: BeautifulSoup, max_items: int = 40) -> str:
    """Pipe-separated ``H2:text|H3:text|…`` outline (H2–H6)."""
    parts: List[str] = []
    for tag in soup.find_all(re.compile(r"^h[2-6]$"), limit=max_items):
        level = tag.name.upper()
        text = tag.get_text(separator=" ", strip=True)[:120]
        if text:
            parts.append(f"{level}:{text}")
    return "|".join(parts)


def _extract_structured_dates(
    soup: BeautifulSoup,
) -> Tuple[str, str]:
    """
    Return ``(date_published, date_modified)`` from structured markup:
    JSON-LD ``datePublished`` / ``dateModified``, Open Graph
    ``article:published_time``, and ``<time datetime>``.
    """
    published = ""
    modified = ""

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for node in _collect_json_ld_nodes(data):
            if not isinstance(node, dict):
                continue
            if not published:
                published = str(node.get("datePublished") or "").strip()
            if not modified:
                modified = str(node.get("dateModified") or "").strip()

    if not published:
        published = _meta_content(soup, {"property": "article:published_time"})
    if not modified:
        modified = _meta_content(soup, {"property": "article:modified_time"})
        if not modified:
            modified = _meta_content(soup, {"property": "og:updated_time"})

    if not published:
        time_tag = soup.find("time", attrs={"datetime": True})
        if time_tag:
            published = str(time_tag["datetime"]).strip()

    # CSS class date containers (e.g. .article-date__pub, .date, .nhsd-m-card__date)
    if not published or not modified:
        _PUB_CLASS = re.compile(r"date.*pub|publish|posted|created", re.I)
        _MOD_CLASS = re.compile(r"date.*(?:last|updat|modif)|last.*(?:updat|modif)", re.I)
        _DATE_IN_TEXT = re.compile(
            r"\d{1,2}(?:st|nd|rd|th)?\s+\w+,?\s+\d{4}"
            r"|\w+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}"
            r"|\d{4}-\d{2}-\d{2}",
        )
        for el in soup.find_all(class_=_PUB_CLASS):
            if published:
                break
            t = el.get_text(strip=True)
            m = _DATE_IN_TEXT.search(t)
            if m:
                published = m.group().strip()
        for el in soup.find_all(class_=_MOD_CLASS):
            if modified:
                break
            t = el.get_text(strip=True)
            m = _DATE_IN_TEXT.search(t)
            if m:
                modified = m.group().strip()

    return published, modified


def _extract_visible_dates(
    html: str, max_matches: int = 3, soup: Optional[BeautifulSoup] = None,
) -> str:
    """Scan visible text and date-class elements for date patterns.

    Runs the regex against rendered visible text so that labels split across
    elements like ``<span>Date published</span>: 9 May, 2023`` are matched.
    Also inspects elements whose CSS classes contain ``date``, ``publish``,
    ``updat``, ``review``, ``posted``, or ``created``.

    When *soup* is provided, all three extraction steps operate on the same
    complete parse tree — visible text is extracted by walking NavigableString
    nodes (without mutating the shared tree) so there is no inconsistency
    between the regex steps and the CSS-class step.
    """
    _MAX_TEXT = 200_000
    _SKIP_TAGS = frozenset({"script", "style", "noscript"})
    found: List[str] = []

    # 1. Regex on visible text (tag-stripped).
    #    Re-use the caller's soup when available; walk NavigableString nodes
    #    to skip script/style content without mutating the shared tree.
    if soup is not None:
        from bs4 import Comment, NavigableString
        parts: List[str] = []
        for node in soup.descendants:
            if isinstance(node, NavigableString) and not isinstance(node, Comment):
                parent = node.parent
                if parent and getattr(parent, "name", None) in _SKIP_TAGS:
                    continue
                t = node.strip()
                if t:
                    parts.append(t)
        vis_text = " ".join(parts)[:_MAX_TEXT]
    else:
        vis_text = re.sub(r"<[^>]+>", " ", html[:_MAX_TEXT])

    for m in _VISIBLE_DATE_RE.finditer(vis_text):
        val = m.group(1).strip()
        if val and val not in found:
            found.append(val)

    # 2. Also check raw HTML in case visible-text extraction missed a pattern
    for m in _VISIBLE_DATE_RE.finditer(html[:_MAX_TEXT]):
        val = m.group(1).strip()
        if val and val not in found:
            found.append(val)

    # 3. Date-bearing CSS class containers (same soup as step 1)
    if soup is not None:
        _DATE_CLASS_RE = re.compile(
            r"date|publish|updat|review|posted|created|modified", re.IGNORECASE,
        )
        _DATE_VAL_RE = re.compile(
            r"\d{1,2}(?:st|nd|rd|th)?\s+\w+,?\s+\d{4}"
            r"|\w+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}"
            r"|\d{4}-\d{2}-\d{2}",
        )
        for el in soup.find_all(class_=_DATE_CLASS_RE):
            t = el.get_text(strip=True)
            if not t or len(t) > 200:
                continue
            for dm in _DATE_VAL_RE.finditer(t):
                val = dm.group().strip()
                if val and val not in found:
                    found.append(val)

    return "|".join(found[:max_matches])


def _count_links(
    soup: BeautifulSoup, page_url: str,
) -> Tuple[int, int, int]:
    """Return ``(internal, external, total)`` link counts."""
    page_host = urlparse(page_url).netloc.lower()
    internal = 0
    external = 0
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("javascript:"):
            continue
        full = _normalise_url(href, page_url)
        if not full:
            continue
        target_host = urlparse(full).netloc.lower()
        if target_host == page_host:
            internal += 1
        else:
            external += 1
    return internal, external, internal + external


def _count_images(soup: BeautifulSoup) -> Tuple[int, int]:
    """Return ``(total_images, missing_alt_count)``."""
    imgs = soup.find_all("img")
    total = len(imgs)
    missing_alt = sum(1 for i in imgs if not (i.get("alt") or "").strip())
    return total, missing_alt


def _compute_readability(visible_text: str) -> str:
    """Flesch–Kincaid grade level if enabled and text is long enough."""
    if not config.CAPTURE_READABILITY:
        return ""
    if not visible_text or len(visible_text.split()) < 30:
        return ""
    try:
        import textstat  # optional dependency
        score = textstat.flesch_kincaid_grade(visible_text)
        return str(round(score, 1))
    except Exception:
        return ""


def _find_privacy_policy_url(soup: BeautifulSoup, page_url: str) -> str:
    """Best-effort privacy/cookie policy link from the page."""
    for a in soup.find_all("a", href=True):
        href_lower = a["href"].strip().lower()
        for pattern in _PRIVACY_HREF_PATTERNS:
            if pattern in href_lower:
                resolved = _normalise_url(a["href"].strip(), page_url)
                if resolved:
                    return resolved
    return ""


def _detect_analytics(html: str) -> str:
    """Return pipe-separated analytics tokens found in raw HTML."""
    found: List[str] = []
    html_lower = html[:500_000].lower()
    for token in _ANALYTICS_TOKENS:
        if token.lower() in html_lower:
            found.append(token)
    return "|".join(found)


def _detect_training_keywords(url: str, title: str, h1: str) -> str:
    """Return pipe-separated training/events keywords matched in URL, title, or H1."""
    combined = f"{url} {title} {h1}".lower()
    hits: List[str] = []
    for kw in config.TRAINING_KEYWORDS:
        if kw.lower() in combined:
            hits.append(kw)
    return "|".join(hits)


def _count_nav_links(soup: BeautifulSoup) -> int:
    """Count distinct links inside ``<nav>`` or ``[role=navigation]``."""
    seen: Set[str] = set()
    for nav in soup.find_all(["nav"]) + soup.find_all(attrs={"role": "navigation"}):
        for a in nav.find_all("a", href=True):
            seen.add(a["href"].strip())
    return len(seen)


def extract_nav_links(
    soup: BeautifulSoup, page_url: str, discovered_at: str,
) -> List[Dict[str, str]]:
    """Return one dict per distinct nav link, ready for ``storage.write_nav_link``."""
    seen: Set[str] = set()
    rows: List[Dict[str, str]] = []
    for nav in soup.find_all(["nav"]) + soup.find_all(attrs={"role": "navigation"}):
        for a in nav.find_all("a", href=True):
            href = a["href"].strip()
            if href in seen:
                continue
            seen.add(href)
            resolved = _normalise_url(href, page_url) or href
            rows.append({
                "page_url": page_url,
                "nav_href": resolved,
                "nav_text": (a.get_text() or "").strip()[:200],
                "discovered_at": discovered_at,
            })
    return rows


def _extract_first_paragraph(soup: BeautifulSoup, min_len: int = 50) -> str:
    """Best-effort description from the first substantial ``<p>`` inside
    ``<main>``, ``<article>``, ``#content``, or ``[role=main]``."""
    container = (
        soup.find("main")
        or soup.find("article")
        or soup.find(id="content")
        or soup.find(attrs={"role": "main"})
        or soup.find(class_=re.compile(
            r"main-content|page-content|entry-content|content-area", re.I,
        ))
    )
    if not container:
        return ""
    for p in container.find_all("p"):
        text = p.get_text(separator=" ", strip=True)
        if len(text) >= min_len:
            return text[:300]
    return ""


def _extract_breadcrumb_text(soup: BeautifulSoup) -> str:
    """Return pipe-separated breadcrumb trail text, or empty string."""
    bc = soup.find(class_=re.compile(r"breadcrumb", re.I))
    if not bc:
        bc = soup.find(attrs={"aria-label": re.compile(r"breadcrumb", re.I)})
    if not bc:
        for nav in soup.find_all("nav"):
            if "breadcrumb" in (nav.get("aria-label") or "").lower():
                bc = nav
                break
    if not bc:
        return ""
    parts: List[str] = []
    for a_or_span in bc.find_all(["a", "span", "li"]):
        t = a_or_span.get_text(strip=True)
        if t and t not in parts and len(t) < 80:
            parts.append(t)
    return "|".join(parts[:8])


def build_page_inventory_row(
    html: str,
    requested_url: str,
    final_url: str,
    http_status: int,
    content_type: str,
    referrer_url: str,
    depth: int,
    discovered_at: str,
    response_meta: Optional[Dict[str, str]] = None,
    sitemap_meta: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Returns the pages.csv row and tag detail rows for tags.csv.
    """
    response_meta = response_meta or {}
    sitemap_meta = sitemap_meta or {}
    soup = BeautifulSoup(html, "lxml")

    desc = _meta_content(soup, attrs={"name": "description"})
    if not desc:
        desc = _meta_content(soup, attrs={"property": "og:description"})
    if not desc:
        desc = _meta_content(soup, attrs={"name": "twitter:description"})
    if not desc:
        desc = _extract_first_paragraph(soup)

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

    json_types, _, _ = _extract_json_ld(soup)
    json_ld_types_str = "|".join(json_types)

    tag_pairs = _collect_all_tags(soup, final_url)
    tags_all = "|".join(t[0] for t in tag_pairs)

    hint = url_content_hint(final_url)
    path = urlparse(final_url).path
    breadcrumb = _extract_breadcrumb_text(soup)
    body_tag = soup.find("body")
    body_classes = " ".join(body_tag.get("class", [])) if body_tag else ""
    kind = guess_content_kind(
        hint, json_types, og_type, path,
        breadcrumb=breadcrumb, body_classes=body_classes,
    )

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

    # Phase 2 — structured headings, dates, quality, trust signals
    heading_outline = _extract_heading_outline(soup)
    date_published, date_modified = _extract_structured_dates(soup)
    visible_dates = _extract_visible_dates(html, soup=soup)
    link_int, link_ext, link_total = _count_links(soup, final_url)
    img_count, img_no_alt = _count_images(soup)
    readability = _compute_readability(visible)
    privacy_url = _find_privacy_policy_url(soup, final_url)
    analytics = _detect_analytics(html)
    training_flag = _detect_training_keywords(final_url, title, h1_joined)
    nav_count = _count_nav_links(soup)

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
        # Phase 1 — freshness / provenance
        "http_last_modified": response_meta.get("last_modified", ""),
        "etag": response_meta.get("etag", ""),
        "sitemap_lastmod": sitemap_meta.get("sitemap_lastmod", ""),
        "referrer_sitemap_url": sitemap_meta.get("source_sitemap", ""),
        # Phase 2 — on-page quality / trust
        "heading_outline": heading_outline,
        "date_published": date_published,
        "date_modified": date_modified,
        "visible_dates": visible_dates,
        "link_count_internal": link_int,
        "link_count_external": link_ext,
        "link_count_total": link_total,
        "img_count": img_count,
        "img_missing_alt_count": img_no_alt,
        "readability_fk_grade": readability,
        "privacy_policy_url": privacy_url,
        "analytics_signals": analytics,
        "training_related_flag": training_flag,
        # Phase 3
        "nav_link_count": nav_count,
        # common
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