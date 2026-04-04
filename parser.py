"""
HTML parsing and metadata extraction for The Crawl Street Journal.

Transforms raw HTML into structured ``pages.csv`` rows, tag detail rows,
edge rows, asset rows, and navigation link rows.  The main entry points are:

- ``build_page_inventory_row``  — full page-level metadata extraction.
- ``extract_classified_links``  — separate <a> hrefs into crawlable HTML
  URLs, downloadable asset rows, and edge (link-graph) rows.
- ``extract_inline_assets``     — images, stylesheets, scripts, and media
  embedded on the page (not discovered via <a> links).
- ``extract_nav_links``         — links inside <nav> / [role=navigation].
- ``guess_content_kind``        — heuristic page-type label from signals.
- ``url_content_hint``          — lightweight label from URL path segments.

Domain filtering uses ``config.ALLOWED_DOMAINS`` via the private helper
``_is_allowed_domain``.  Note that ``scraper.normalise_url`` performs heavier
canonicalisation (scheme, ports, sorted query params) for deduplication,
whereas ``_normalise_url`` here only resolves relatives and strips fragments
— the two are intentionally different in scope.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

import config
import utils


# ── URL helpers ───────────────────────────────────────────────────────────


def _normalise_url(url: str, base_url: str) -> Optional[str]:
    """Resolve *url* against *base_url*, strip the fragment, and remove a trailing slash.

    This is a lightweight normalisation used for link extraction within a
    page.  For crawl-queue deduplication the heavier ``scraper.normalise_url``
    should be used instead.
    """
    try:
        full = urljoin(base_url, url)
        parsed = urlparse(full)
        without_fragment = parsed._replace(fragment="").geturl()
        return without_fragment.rstrip("/") or without_fragment
    except Exception:
        return None


def _is_allowed_domain(url: str, allowed_domains=None) -> bool:
    """Check whether *url*'s hostname matches the allowed-domain list.

    When *allowed_domains* is ``None``, falls back to the module-level
    ``config.ALLOWED_DOMAINS``.  Callers in threaded contexts should pass
    the domains explicitly to avoid reading a potentially stale global.
    """
    domains = allowed_domains if allowed_domains is not None else config.ALLOWED_DOMAINS
    return utils.is_allowed_domain(url, domains)


def _path_extension_lower(path: str) -> str:
    """Return the matching skip-extension suffix from *path*, or ``""``.

    Extensions are tested longest-first so that e.g. ``.tar.gz`` is
    preferred over ``.gz``.
    """
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
    return config.ASSET_CATEGORY_BY_EXT.get(ext, "other")


def is_skippable_asset_url(url: str) -> bool:
    """``True`` when *url* has a file extension that marks it as a downloadable asset."""
    return asset_category_for_url(url) is not None


def get_visible_text(soup: BeautifulSoup) -> str:
    """Return user-visible text from *soup*, excluding script/style/noscript elements.

    This implementation is non-mutating: it collects text nodes whose parent
    element is not an invisible tag, rather than decomposing those tags from
    the tree.  The supplied ``soup`` is left unchanged, so callers may safely
    reuse it after this call.
    """
    invisible = {"script", "style", "noscript"}
    return " ".join(
        t.strip()
        for t in soup.find_all(string=True)
        if t.parent.name not in invisible and t.strip()
    )


# ── Meta-tag extraction ───────────────────────────────────────────────────


def _meta_content(soup: BeautifulSoup, attrs: dict) -> str:
    """Return the ``content`` attribute of the first ``<meta>`` matching *attrs*.

    Falls back to a case-insensitive match on attribute *values* to handle
    servers (notably ASP.NET) that emit upper-case attribute names.
    """
    tag = soup.find("meta", attrs=attrs)
    if tag and tag.get("content"):
        return str(tag["content"]).strip()
    ci_attrs = {
        k: re.compile(re.escape(v), re.IGNORECASE) if isinstance(v, str) else v
        for k, v in attrs.items()
    }
    tag = soup.find("meta", attrs=ci_attrs)
    if tag and tag.get("content"):
        return str(tag["content"]).strip()
    return ""


def _all_meta_properties(soup: BeautifulSoup, prop: str) -> List[str]:
    """Collect ``content`` values from every ``<meta property="...">`` matching *prop*."""
    out: List[str] = []
    for tag in soup.find_all("meta", attrs={"property": prop}):
        c = tag.get("content")
        if c:
            out.append(str(c).strip())
    return out


# ── Structured data (JSON-LD / Microdata / RDFa) ─────────────────────────


def _dedupe_case_insensitive(seq: List[str]) -> List[str]:
    """Return *seq* with case-insensitive duplicates removed, preserving order."""
    seen: Set[str] = set()
    out: List[str] = []
    for x in seq:
        k = x.lower()
        if k not in seen:
            seen.add(k)
            out.append(x)
    return out


def _collect_json_ld_nodes(obj: Any) -> List[dict]:
    """Recursively flatten a JSON-LD object (or ``@graph`` array) into a list of node dicts.

    Thin wrapper around ``utils.flatten_json_ld`` kept for backwards compatibility.
    """
    return utils.flatten_json_ld(obj)


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

    return (
        _dedupe_case_insensitive(types),
        _dedupe_case_insensitive(keywords),
        _dedupe_case_insensitive(sections),
    )


# ── Tag / keyword collection ──────────────────────────────────────────────


def _rel_tag_hrefs(soup: BeautifulSoup) -> List[str]:
    """Return visible text of ``<a rel="tag">`` links (common in WordPress)."""
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

    for t in _rel_tag_hrefs(soup):
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


# ── Content-kind heuristics ───────────────────────────────────────────────


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
        # Phase 4 — expanded path hints
        ("/products/", "product_path"),
        ("/product/", "product_path"),
        ("/shop/", "product_path"),
        ("/collections/", "product_path"),
        ("/recipe", "recipe_path"),
        ("/recipes/", "recipe_path"),
        ("/faq", "faq_path"),
        ("/help/", "help_path"),
        ("/support/", "help_path"),
        ("/forum", "forum_path"),
        ("/community/", "forum_path"),
        ("/wiki/", "wiki_path"),
        ("/docs/", "docs_path"),
        ("/documentation/", "docs_path"),
        ("/portfolio/", "portfolio_path"),
        ("/case-stud", "case_study_path"),
        ("/review", "review_path"),
        ("/testimonial", "review_path"),
        ("/pricing", "pricing_path"),
        ("/plans/", "pricing_path"),
        ("/team/", "team_path"),
        ("/staff/", "team_path"),
        ("/people/", "team_path"),
        ("/press/", "press_path"),
        ("/media/", "press_path"),
        ("/legal/", "legal_path"),
        ("/terms", "legal_path"),
        ("/privacy", "legal_path"),
        ("/policy/", "legal_path"),
        ("/signin", "auth_path"),
        ("/login", "auth_path"),
        ("/register", "auth_path"),
        ("/signup", "auth_path"),
        ("/account/", "auth_path"),
        ("/search", "search_path"),
    ):
        if token in path:
            hints.append(label)
    return "|".join(hints) if hints else ""


# ── Content-kind classification tables ────────────────────────────────────
#
# Each entry below maps a set of detection tokens to a content-kind label.
# Add new rows here to extend classification; order matters (first match wins).

# Simple OR rules: (schema_token_in_types, url_hint_token, label).
# An empty string for the url-hint position means "URL hint not checked".
_SCHEMA_URL_KIND_RULES: List[Tuple[str, str, str]] = [
    ("blogposting",          "blog_path",      "blog"),
    ("newsarticle",          "news_path",       "news"),
    ("product",              "product_path",    "product"),
    ("jobposting",           "jobs_path",       "job_posting"),
    ("recipe",               "recipe_path",     "recipe"),
    ("howto",                "",                "how_to"),
    ("course",               "training_path",   "course"),
    ("qapage",               "forum_path",      "qa"),
    ("videoobject",          "",                "video"),
    ("softwareapplication",  "",                "software"),
    ("review",               "review_path",     "review"),
    ("faqpage",              "faq_path",        "faq"),
]

# URL-hint-only rules (JSON-LD not required): (url_hint_token, label).
_URL_ONLY_KIND_RULES: List[Tuple[str, str]] = [
    ("contact_path",    "contact"),
    ("about_path",      "about"),
    ("pricing_path",    "pricing"),
    ("case_study_path", "case_study"),
    ("docs_path",       "documentation"),
    ("legal_path",      "legal"),
    ("search_path",     "search"),
]

# Breadcrumb keyword rules: (keywords_tuple, label).
_BREADCRUMB_KIND_RULES: List[Tuple[Tuple[str, ...], str]] = [
    (("blog", "blogs"),                  "blog"),
    (("news", "newsroom"),               "news"),
    (("publication", "publications"),    "publication"),
    (("event", "events"),                "event"),
    (("about",),                         "about"),
    (("contact", "get in touch"),        "contact"),
    (("service", "services"),            "service"),
]

# SilverStripe page-type body-class tokens → label.
_SS_BODY_KIND_MAP: Tuple[Tuple[str, str], ...] = (
    ("homepage",            "homepage"),
    ("aboutoverviewpage",   "about"),
    ("newsitemlistingpage", "news"),
    ("newsitempage",        "news"),
    ("contactpage",         "contact"),
    ("eventpage",           "event"),
    ("resourcepage",        "resource"),
    ("staffpage",           "staff"),
    ("blogentry",           "blog"),
)

# Drupal ``page-node-type-<type>`` body class — compiled once at module level.
_DRUPAL_BODY_RE: re.Pattern[str] = re.compile(r"page-node-type-(\S+)")


def guess_content_kind(
    url_hint: str,
    json_ld_types: List[str],
    og_type: str,
    path: str,
    breadcrumb: str = "",
    body_classes: str = "",
) -> str:
    """Classify a page into a content-kind label using a priority cascade.

    The cascade checks, in order:
    1. JSON-LD ``@type`` values (most reliable structured signal), using the
       rules in ``_SCHEMA_URL_KIND_RULES``.
    2. URL path hints from ``url_content_hint`` (``_URL_ONLY_KIND_RULES``).
    3. OpenGraph ``og:type``.
    4. Breadcrumb text (``_BREADCRUMB_KIND_RULES``).
    5. Body CSS classes — WordPress patterns, then SilverStripe
       (``_SS_BODY_KIND_MAP``), then Drupal (``_DRUPAL_BODY_RE``).
    6. Root-path homepage fallback.

    Returns a short lowercase label such as ``"blog"``, ``"news"``,
    ``"product"``, ``"homepage"``, or ``"unknown"``.
    """
    types_lower = " ".join(json_ld_types).lower()
    og_l = (og_type or "").lower()
    path_l = path.lower()
    bc_lower = breadcrumb.lower()
    body_lower = body_classes.lower()

    # 1. Schema / URL-hint combined rules
    for schema_tok, hint_tok, label in _SCHEMA_URL_KIND_RULES:
        if schema_tok in types_lower or (hint_tok and hint_tok in url_hint):
            return label

    # Special cases that don't fit the simple OR pattern
    if "article" in types_lower and "blog" not in path_l:
        return "article"
    if "event" in types_lower and "events_path" in url_hint:
        return "event"
    if "localbusiness" in types_lower or "restaurant" in types_lower:
        return "local_business"
    if "webpage" in types_lower or og_l in ("website", "article"):
        return "webpage"
    if "medicalwebpage" in types_lower or "medicalbusiness" in types_lower:
        return "medical_page"
    if "collectionpage" in types_lower or "itemlist" in types_lower:
        return "listing"

    # 2. URL-hint-only rules
    for hint_tok, label in _URL_ONLY_KIND_RULES:
        if hint_tok in url_hint:
            return label

    # 3. Breadcrumb signals
    for keywords, label in _BREADCRUMB_KIND_RULES:
        if any(w in bc_lower for w in keywords):
            return label

    # 4. WordPress body class patterns
    if "single-post" in body_lower or "post-template" in body_lower:
        return "blog"
    if "page-template" in body_lower or "page-id" in body_lower:
        return "webpage"
    if "archive" in body_lower:
        return "listing"

    # 5. SilverStripe page-type body classes
    for token, kind in _SS_BODY_KIND_MAP:
        if token in body_lower:
            return kind

    # 6. Drupal page-node-type body classes
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


# ── Link classification ───────────────────────────────────────────────────


def extract_classified_links(
    html: str,
    base_url: str,
    discovered_at: str,
    allowed_domains=None,
) -> Tuple[Set[str], List[dict], List[dict], List[dict]]:
    """Partition ``<a href>`` links into crawlable HTML URLs, assets, edges, and phone numbers.

    Args:
        html: Raw HTML of the page.
        base_url: Fully-qualified URL used to resolve relative hrefs.
        discovered_at: ISO timestamp for provenance tracking.

    Returns:
        A 4-tuple of:
        - *html_urls* — same-host URLs to enqueue for HTML crawling.
        - *asset_rows* — dicts matching ``ASSET_FIELDS`` (``head_*`` empty).
        - *edge_rows* — dicts for the link-graph CSV (``edges.csv``).
        - *phone_rows* — dicts matching ``PHONE_NUMBER_FIELDS`` for ``tel:`` hrefs.
    """
    soup = BeautifulSoup(html, "lxml")
    html_urls: Set[str] = set()
    asset_rows: List[dict] = []
    edge_rows: List[dict] = []
    phone_rows: List[dict] = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:"):
            continue
        if href.startswith("javascript:"):
            continue
        if href.lower().startswith("tel:"):
            phone_number = href[4:].strip()
            if not phone_number:
                continue
            anchor = (a.get_text() or "").strip()
            phone_rows.append({
                "page_url": base_url,
                "raw_href": href,
                "phone_number": phone_number,
                "link_text": anchor,
                "discovered_at": discovered_at,
            })
            continue
        full = _normalise_url(href, base_url)
        if not full or not _is_allowed_domain(full, allowed_domains):
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

    return html_urls, asset_rows, edge_rows, phone_rows


def _document_title_from_soup(soup: BeautifulSoup) -> str:
    """Text inside <title>, including when the tag has nested elements."""
    tag = soup.find("title")
    if not tag:
        return ""
    text = tag.get_text(separator=" ", strip=True)
    return text.strip()


# ── Date extraction ───────────────────────────────────────────────────────

# Regex that matches labelled dates in visible text, e.g.
# "Last updated: 9 May, 2023" or "Posted on 2024-01-15".
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

# ── Quality / trust signal helpers ─────────────────────────────────────────

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

    # CSS class date containers (e.g. .article-date__pub, .date)
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


_VAGUE_LINK_TEXTS = frozenset({
    "click here", "read more", "more", "here", "link", "this", "learn more",
})


# ── Phase 4 — extended extraction (author, publisher, CMS, robots, etc.) ──

def _extract_author(soup: BeautifulSoup) -> str:
    """Best-effort author from meta, JSON-LD, or byline elements."""
    author = _meta_content(soup, attrs={"name": "author"})
    if author:
        return author

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
            a = node.get("author")
            if isinstance(a, dict):
                name = a.get("name", "")
                if name:
                    return str(name).strip()
            elif isinstance(a, list):
                names = [
                    str(x.get("name", "")).strip() if isinstance(x, dict)
                    else str(x).strip()
                    for x in a
                ]
                names = [n for n in names if n]
                if names:
                    return " | ".join(names)
            elif isinstance(a, str) and a.strip():
                return a.strip()

    _BYLINE_CLASS = re.compile(r"byline|author|writer|correspondent", re.I)
    for el in soup.find_all(class_=_BYLINE_CLASS):
        text = el.get_text(strip=True)
        if text and 2 < len(text) < 100:
            return text
    return ""


def _extract_publisher(soup: BeautifulSoup) -> str:
    """Publisher name from JSON-LD or OG ``og:site_name``."""
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
            p = node.get("publisher")
            if isinstance(p, dict):
                name = p.get("name", "")
                if name:
                    return str(name).strip()
            elif isinstance(p, str) and p.strip():
                return p.strip()

    site_name = _meta_content(soup, attrs={"property": "og:site_name"})
    if site_name:
        return site_name
    return ""


def _extract_json_ld_id(soup: BeautifulSoup) -> str:
    """Return the first ``@id`` found across JSON-LD nodes."""
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (script.string or script.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for node in _collect_json_ld_nodes(data):
            if isinstance(node, dict) and node.get("@id"):
                return str(node["@id"]).strip()
    return ""


def _detect_cms_generator(soup: BeautifulSoup) -> str:
    """Detect CMS/platform from ``meta[name=generator]`` and other signals."""
    gen = _meta_content(soup, attrs={"name": "generator"})
    if gen:
        return gen

    _CMS_SIGNATURES = (
        ("meta", {"name": "shopify-checkout-api-token"}, "Shopify"),
        ("meta", {"name": "wix-dynamic-custom-elements"}, "Wix"),
        ("meta", {"content": re.compile(r"Squarespace", re.I)}, "Squarespace"),
    )
    for tag_name, attrs, label in _CMS_SIGNATURES:
        if soup.find(tag_name, attrs=attrs):
            return label

    html_str = str(soup)[:50_000].lower()
    if "static.parastorage.com" in html_str or "wix-warmup-data" in html_str:
        return "Wix"
    if "static.squarespace.com" in html_str or "sqs-block" in html_str:
        return "Squarespace"
    if "cdn.shopify.com" in html_str:
        return "Shopify"
    if "assets.website-files.com" in html_str:
        return "Webflow"
    if "js.hs-scripts.com" in html_str:
        return "HubSpot"
    if "/content/dam/" in html_str:
        return "Adobe Experience Manager"
    _body = soup.find("body")
    body_classes = _body.get("class", []) if _body else []
    if "ghost-" in " ".join(body_classes).lower():
        return "Ghost"

    return ""


def _extract_robots_directives(
    soup: BeautifulSoup,
    response_meta: Optional[Dict[str, str]] = None,
) -> str:
    """Combine ``meta[name=robots]`` and ``X-Robots-Tag`` header."""
    parts: List[str] = []
    robots = _meta_content(soup, attrs={"name": "robots"})
    if robots:
        parts.append(f"meta:{robots}")
    if response_meta:
        x_robots = response_meta.get("x_robots_tag", "")
        if x_robots:
            parts.append(f"header:{x_robots}")
    return "|".join(parts)


def _extract_hreflang_links(
    soup: BeautifulSoup, base_url: str,
) -> str:
    """Pipe-separated ``lang=url`` pairs from ``link[rel=alternate][hreflang]``."""
    pairs: List[str] = []
    for link in soup.find_all("link", attrs={"rel": "alternate", "hreflang": True}):
        lang = str(link.get("hreflang", "")).strip()
        href = link.get("href", "")
        if lang and href:
            try:
                resolved = urljoin(base_url, href.strip())
            except Exception:
                resolved = href.strip()
            pairs.append(f"{lang}={resolved}")
    return "|".join(pairs[:20])


def _extract_feed_urls(soup: BeautifulSoup, base_url: str) -> str:
    """RSS/Atom feed URLs from ``link[rel=alternate]`` with feed types."""
    _FEED_TYPES = ("application/rss+xml", "application/atom+xml")
    urls: List[str] = []
    for link in soup.find_all("link", attrs={"rel": "alternate", "href": True}):
        link_type = str(link.get("type", "")).strip().lower()
        if link_type in _FEED_TYPES:
            href = link.get("href", "")
            if href:
                try:
                    urls.append(urljoin(base_url, href.strip()))
                except Exception:
                    urls.append(href.strip())
    return "|".join(urls[:5])


def _extract_pagination(soup: BeautifulSoup, base_url: str) -> Tuple[str, str]:
    """Return ``(next_url, prev_url)`` from ``link[rel=next/prev]``."""
    def _find_link(rel: str) -> str:
        tag = soup.find("link", attrs={"rel": rel, "href": True})
        if tag:
            href = tag.get("href", "")
            if href:
                try:
                    return urljoin(base_url, href.strip())
                except Exception:
                    return href.strip()
        return ""
    return _find_link("next"), _find_link("prev")


def _extract_breadcrumb_schema(soup: BeautifulSoup) -> str:
    """Extract ``BreadcrumbList`` items from JSON-LD."""
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
            t = node.get("@type", "")
            if (isinstance(t, str) and t == "BreadcrumbList") or (
                isinstance(t, list) and "BreadcrumbList" in t
            ):
                items = node.get("itemListElement", [])
                if isinstance(items, list):
                    names: List[str] = []
                    for item in sorted(
                        items,
                        key=lambda x: x.get("position", 0) if isinstance(x, dict) else 0,
                    ):
                        if isinstance(item, dict):
                            name = item.get("name", "")
                            nested = item.get("item", {})
                            if not name and isinstance(nested, dict):
                                name = nested.get("name", "")
                            if name:
                                names.append(str(name).strip())
                    if names:
                        return " > ".join(names)
    return ""


def _extract_microdata(soup: BeautifulSoup) -> str:
    """Extract top-level Microdata ``itemscope`` types (pipe-separated)."""
    types: List[str] = []
    for el in soup.find_all(attrs={"itemscope": True}):
        parent_scope = el.find_parent(attrs={"itemscope": True})
        if parent_scope is None:
            item_type = el.get("itemtype", "")
            if item_type:
                short = str(item_type).replace("http://schema.org/", "").replace(
                    "https://schema.org/", ""
                )
                if short and short not in types:
                    types.append(short)
    return "|".join(types)


def _extract_rdfa_types(soup: BeautifulSoup) -> str:
    """Extract top-level RDFa ``typeof`` types (pipe-separated)."""
    types: List[str] = []
    for el in soup.find_all(attrs={"typeof": True}):
        rdfa_type = str(el.get("typeof", "")).strip()
        if rdfa_type and rdfa_type not in types:
            types.append(rdfa_type)
    return "|".join(types)


def _extract_product_fields(node: dict, result: Dict[str, str]) -> None:
    """Populate Product schema fields from *node* into *result* (in-place)."""
    offers = node.get("offers", {})
    if isinstance(offers, list) and offers:
        offers = offers[0]
    if isinstance(offers, dict):
        if not result["schema_price"]:
            result["schema_price"] = str(
                offers.get("price", offers.get("lowPrice", ""))
            ).strip()
            result["schema_currency"] = str(
                offers.get("priceCurrency", "")
            ).strip()
            result["schema_availability"] = str(
                offers.get("availability", "")
            ).strip().rsplit("/", 1)[-1]


def _extract_rating_fields(node: dict, result: Dict[str, str]) -> None:
    """Populate aggregateRating / review fields from *node* into *result* (in-place)."""
    rating = node.get("aggregateRating") or node.get("review")
    if isinstance(rating, dict) and not result["schema_rating"]:
        result["schema_rating"] = str(rating.get("ratingValue", "")).strip()
        result["schema_review_count"] = str(
            rating.get("reviewCount", rating.get("ratingCount", ""))
        ).strip()


def _extract_event_fields(node: dict, result: Dict[str, str]) -> None:
    """Populate Event schema fields from *node* into *result* (in-place)."""
    if not result["schema_event_date"]:
        result["schema_event_date"] = str(node.get("startDate", "")).strip()
    loc = node.get("location", {})
    if isinstance(loc, dict) and not result["schema_event_location"]:
        result["schema_event_location"] = str(
            loc.get("name", loc.get("address", ""))
        ).strip()[:200]
    elif isinstance(loc, str) and not result["schema_event_location"]:
        result["schema_event_location"] = loc.strip()[:200]


def _extract_job_fields(node: dict, result: Dict[str, str]) -> None:
    """Populate JobPosting schema fields from *node* into *result* (in-place)."""
    if not result["schema_job_title"]:
        result["schema_job_title"] = str(node.get("title", "")).strip()
    jl = node.get("jobLocation", {})
    if isinstance(jl, dict) and not result["schema_job_location"]:
        addr = jl.get("address", {})
        if isinstance(addr, dict):
            result["schema_job_location"] = str(
                addr.get("addressLocality", "")
            ).strip()
        elif isinstance(addr, str):
            result["schema_job_location"] = addr.strip()


def _extract_recipe_fields(node: dict, result: Dict[str, str]) -> None:
    """Populate Recipe schema fields from *node* into *result* (in-place)."""
    if not result["schema_recipe_time"]:
        result["schema_recipe_time"] = str(
            node.get("totalTime", node.get("cookTime", ""))
        ).strip()


def _extract_schema_specific(
    soup: BeautifulSoup,
) -> Dict[str, str]:
    """Extract domain-specific fields from JSON-LD schemas.

    Returns a dict with optional keys like ``schema_price``,
    ``schema_availability``, ``schema_rating``, etc.  Empty strings
    for fields not found on the page.  Per-family extraction is handled
    by ``_extract_product_fields``, ``_extract_event_fields``,
    ``_extract_job_fields``, and ``_extract_recipe_fields``.
    """
    result: Dict[str, str] = {
        "schema_price": "",
        "schema_currency": "",
        "schema_availability": "",
        "schema_rating": "",
        "schema_review_count": "",
        "schema_event_date": "",
        "schema_event_location": "",
        "schema_job_title": "",
        "schema_job_location": "",
        "schema_recipe_time": "",
    }
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
            t = node.get("@type", "")
            types_list = t if isinstance(t, list) else [t]
            types_lower = [str(x).lower() for x in types_list]

            if "product" in types_lower:
                _extract_product_fields(node, result)
            _extract_rating_fields(node, result)
            if "event" in types_lower:
                _extract_event_fields(node, result)
            if "jobposting" in types_lower:
                _extract_job_fields(node, result)
            if "recipe" in types_lower:
                _extract_recipe_fields(node, result)

    return result


def _compute_extraction_coverage(page_row: Dict[str, Any]) -> str:
    """Return the percentage of non-empty content fields in the row."""
    _SKIP = {
        "requested_url", "final_url", "domain", "http_status",
        "content_type", "referrer_url", "depth", "discovered_at",
        "http_last_modified", "etag", "sitemap_lastmod",
        "referrer_sitemap_url",
    }
    total = 0
    filled = 0
    for key, val in page_row.items():
        if key in _SKIP:
            continue
        total += 1
        if val not in (None, "", 0, "0"):
            filled += 1
    if total == 0:
        return ""
    return str(round(filled / total * 100, 1))


def _assess_wcag_static(soup: BeautifulSoup, lang: str, title: str) -> dict:
    """Static-HTML WCAG checks per page, returned as string values for CSV.

    Covers testable Level A and AA criteria from WCAG 2.1 that can be
    assessed from the raw HTML without rendering or user interaction.
    """
    # 3.1.1 Language of Page (Level A)
    lang_valid = bool(lang and len(lang) >= 2)

    # 1.3.1 Info and Relationships — heading hierarchy (Level A)
    levels = [int(h.name[1]) for h in soup.find_all(re.compile(r"^h[1-6]$"))]
    heading_ok = True
    for i in range(1, len(levels)):
        if levels[i] > levels[i - 1] + 1:
            heading_ok = False
            break

    # 2.4.2 Page Titled (Level A)
    title_present = bool(title.strip())

    # 1.3.1 Info and Relationships — form labels (Level A)
    inputs = soup.find_all("input", attrs={
        "type": lambda t: t not in (None, "hidden", "submit", "button", "reset", "image"),
    })
    labelled = 0
    for inp in inputs:
        inp_id = inp.get("id", "")
        has_for = bool(inp_id and soup.find("label", attrs={"for": inp_id}))
        has_wrap = bool(inp.find_parent("label"))
        has_aria = bool(inp.get("aria-label") or inp.get("aria-labelledby"))
        if has_for or has_wrap or has_aria:
            labelled += 1
    form_labels_pct = labelled / len(inputs) if inputs else 1.0

    # 2.4.1 Bypass Blocks (Level A)
    has_main = bool(soup.find("main") or soup.find(attrs={"role": "main"}))
    has_skip = bool(soup.find("a", href=re.compile(r"^#(main|content|skip)", re.I)))
    landmarks_present = has_main or has_skip

    # 2.4.4 Link Purpose (Level A)
    links = soup.find_all("a", href=True)
    vague_count = sum(
        1 for a in links if a.get_text(strip=True).lower() in _VAGUE_LINK_TEXTS
    )
    vague_link_pct = vague_count / len(links) if links else 0.0

    # 1.1.1 Non-text Content — images without alt (Level A)
    imgs = soup.find_all("img")
    imgs_total = len(imgs)
    imgs_no_alt = sum(1 for i in imgs if not (i.get("alt") or "").strip())
    img_alt_pct = 1.0 - (imgs_no_alt / imgs_total) if imgs_total else 1.0

    # 2.4.6 Headings and Labels — empty headings (Level AA)
    all_headings = soup.find_all(re.compile(r"^h[1-6]$"))
    empty_headings = sum(1 for h in all_headings if not h.get_text(strip=True))

    # 4.1.1 Parsing — duplicate IDs (Level A)
    ids_seen: dict = {}
    duplicate_ids = 0
    for el in soup.find_all(attrs={"id": True}):
        eid = el.get("id", "")
        if eid in ids_seen:
            duplicate_ids += 1
        ids_seen[eid] = True

    # 4.1.2 Name, Role, Value — buttons/links without accessible names (Level A)
    buttons = soup.find_all(["button", "input"])
    empty_buttons = 0
    for btn in buttons:
        if btn.name == "input" and btn.get("type") in ("hidden", None):
            continue
        if btn.name == "input" and btn.get("type") in ("submit", "button", "reset"):
            if not (btn.get("value") or btn.get("aria-label") or "").strip():
                empty_buttons += 1
        elif btn.name == "button":
            if not (btn.get_text(strip=True) or btn.get("aria-label") or "").strip():
                empty_buttons += 1

    empty_links = sum(
        1 for a in links
        if not (a.get_text(strip=True) or a.get("aria-label") or
                a.get("aria-labelledby") or a.get("title") or "").strip()
        and not a.find("img", attrs={"alt": True})
    )

    # 1.3.1 Info and Relationships — data tables (Level A)
    tables = soup.find_all("table")
    tables_no_headers = sum(1 for t in tables if not t.find("th"))

    # 1.3.5 Identify Input Purpose — autocomplete on form fields (Level AA)
    _AUTOCOMPLETE_TYPES = {"text", "email", "tel", "url", "search", "password"}
    autocomplete_inputs = [
        inp for inp in soup.find_all("input")
        if (inp.get("type") or "text").lower() in _AUTOCOMPLETE_TYPES
    ]
    autocomplete_present = sum(
        1 for inp in autocomplete_inputs if inp.get("autocomplete")
    )
    autocomplete_pct = (
        autocomplete_present / len(autocomplete_inputs)
        if autocomplete_inputs else 1.0
    )

    # 2.4.5 Multiple Ways (Level AA) — site has search, nav, and sitemap link
    has_search = bool(
        soup.find("input", attrs={"type": "search"}) or
        soup.find("form", attrs={"role": "search"}) or
        soup.find("input", attrs={"name": re.compile(r"search|query|q$", re.I)})
    )
    has_nav = bool(soup.find("nav"))

    return {
        "wcag_lang_valid": "1" if lang_valid else "0",
        "wcag_heading_order_valid": "1" if heading_ok else "0",
        "wcag_title_present": "1" if title_present else "0",
        "wcag_form_labels_pct": str(round(form_labels_pct, 3)),
        "wcag_landmarks_present": "1" if landmarks_present else "0",
        "wcag_vague_link_pct": str(round(vague_link_pct, 3)),
        # Extended WCAG checks
        "wcag_img_alt_pct": str(round(img_alt_pct, 3)),
        "wcag_empty_headings": str(empty_headings),
        "wcag_duplicate_ids": str(duplicate_ids),
        "wcag_empty_buttons": str(empty_buttons),
        "wcag_empty_links": str(empty_links),
        "wcag_tables_no_headers": str(tables_no_headers),
        "wcag_autocomplete_pct": str(round(autocomplete_pct, 3)),
        "wcag_has_search": "1" if has_search else "0",
        "wcag_has_nav": "1" if has_nav else "0",
    }


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


# ── Master row builder ────────────────────────────────────────────────────


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
    """Build the full ``pages.csv`` row and companion ``tags.csv`` rows for one page.

    Orchestrates every extraction helper in this module.  The returned
    *page_row* dict keys match ``storage.PAGES_FIELDS`` exactly — column
    ordering is handled by ``storage.write_page``.

    Args:
        html: Raw HTML body.
        requested_url: The URL that was asked for (before any redirects).
        final_url: The URL after following all redirects.
        http_status: HTTP status code of the final response.
        content_type: ``Content-Type`` header value.
        referrer_url: URL of the page where this link was discovered.
        depth: Crawl depth (hops from a seed URL).
        discovered_at: ISO timestamp.
        response_meta: Optional dict of HTTP response headers
            (keys: ``last_modified``, ``etag``, ``x_robots_tag``).
        sitemap_meta: Optional dict of sitemap provenance
            (keys: ``sitemap_lastmod``, ``source_sitemap``).

    Returns:
        A 2-tuple of (*page_row*, *tag_rows*).
    """
    response_meta = response_meta or {}
    sitemap_meta = sitemap_meta or {}
    soup = BeautifulSoup(html, "lxml")

    seo = _extract_seo_meta(soup, final_url)
    quality = _extract_quality_signals(
        soup, html, seo["title"], seo["h1_joined"], seo["lang"], final_url, response_meta,
    )
    phase4 = _extract_phase4(soup, final_url, response_meta)

    tag_pairs = _collect_all_tags(soup, final_url)
    tags_all = "|".join(t[0] for t in tag_pairs)

    page_row = _assemble_page_row(
        requested_url=requested_url,
        final_url=final_url,
        http_status=http_status,
        content_type=content_type,
        referrer_url=referrer_url,
        depth=depth,
        discovered_at=discovered_at,
        response_meta=response_meta,
        sitemap_meta=sitemap_meta,
        tags_all=tags_all,
        seo=seo,
        quality=quality,
        phase4=phase4,
    )

    page_row["extraction_coverage_pct"] = _compute_extraction_coverage(page_row)

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


# ── build_page_inventory_row sub-functions ────────────────────────────────


def _extract_seo_meta(soup: BeautifulSoup, final_url: str) -> Dict[str, Any]:
    """Extract SEO metadata: title, description, Open Graph, canonical, lang, h1s, content kind."""
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

    hint = url_content_hint(final_url)
    path = urlparse(final_url).path
    breadcrumb_text = _extract_breadcrumb_text(soup)
    body_tag = soup.find("body")
    body_classes = " ".join(body_tag.get("class", [])) if body_tag else ""
    kind = guess_content_kind(
        hint, json_types, og_type, path,
        breadcrumb=breadcrumb_text, body_classes=body_classes,
    )

    h1_texts: List[str] = []
    for h in soup.find_all("h1", limit=5):
        t = h.get_text(separator=" ", strip=True)
        if t:
            h1_texts.append(t)
    h1_joined = " | ".join(h1_texts)

    doc_title = _document_title_from_soup(soup)
    title = doc_title or og_title or (h1_texts[0] if h1_texts else "")

    visible = get_visible_text(soup)
    word_count = len(visible.split()) if visible else 0

    return {
        "desc": desc,
        "lang": lang,
        "canonical": canonical,
        "og_title": og_title,
        "og_type": og_type,
        "og_description": og_description,
        "twitter_card": twitter_card,
        "json_ld_types_str": json_ld_types_str,
        "json_types": json_types,
        "hint": hint,
        "kind": kind,
        "h1_joined": h1_joined,
        "title": title,
        "word_count": word_count,
        "visible": visible,
    }


def _extract_quality_signals(
    soup: BeautifulSoup,
    html: str,
    title: str,
    h1_joined: str,
    lang: str,
    final_url: str,
    response_meta: Dict[str, str],
) -> Dict[str, Any]:
    """Extract Phase 2 quality and trust signals."""
    heading_outline = _extract_heading_outline(soup)
    date_published, date_modified = _extract_structured_dates(soup)
    visible_dates = _extract_visible_dates(html, soup=soup)
    link_int, link_ext, link_total = _count_links(soup, final_url)
    img_count, img_no_alt = _count_images(soup)
    visible_text = get_visible_text(soup)
    readability = _compute_readability(visible_text)
    privacy_url = _find_privacy_policy_url(soup, final_url)
    analytics = _detect_analytics(html)
    training_flag = _detect_training_keywords(final_url, title, h1_joined)
    nav_count = _count_nav_links(soup)
    wcag = _assess_wcag_static(soup, lang, title)
    return {
        "heading_outline": heading_outline,
        "date_published": date_published,
        "date_modified": date_modified,
        "visible_dates": visible_dates,
        "link_int": link_int,
        "link_ext": link_ext,
        "link_total": link_total,
        "img_count": img_count,
        "img_no_alt": img_no_alt,
        "readability": readability,
        "privacy_url": privacy_url,
        "analytics": analytics,
        "training_flag": training_flag,
        "nav_count": nav_count,
        "wcag": wcag,
    }


def _extract_phase4(
    soup: BeautifulSoup,
    final_url: str,
    response_meta: Dict[str, str],
) -> Dict[str, Any]:
    """Extract Phase 4 extended signals: authorship, CMS, structured data, SEO directives."""
    return {
        "author": _extract_author(soup),
        "publisher": _extract_publisher(soup),
        "json_ld_id": _extract_json_ld_id(soup),
        "cms_generator": _detect_cms_generator(soup),
        "robots_directives": _extract_robots_directives(soup, response_meta),
        "hreflang_links": _extract_hreflang_links(soup, final_url),
        "feed_urls": _extract_feed_urls(soup, final_url),
        "pagination": _extract_pagination(soup, final_url),
        "breadcrumb_schema": _extract_breadcrumb_schema(soup),
        "microdata_types": _extract_microdata(soup),
        "rdfa_types": _extract_rdfa_types(soup),
        "schema_specific": _extract_schema_specific(soup),
    }


def _assemble_page_row(
    requested_url: str,
    final_url: str,
    http_status: int,
    content_type: str,
    referrer_url: str,
    depth: int,
    discovered_at: str,
    response_meta: Dict[str, str],
    sitemap_meta: Dict[str, str],
    tags_all: str,
    seo: Dict[str, Any],
    quality: Dict[str, Any],
    phase4: Dict[str, Any],
) -> Dict[str, Any]:
    """Assemble the final ``pages.csv`` row dict from extracted sub-groups."""
    pagination_next, pagination_prev = phase4["pagination"]
    domain = urlparse(final_url).netloc.lower()
    return {
        "requested_url": requested_url,
        "final_url": final_url,
        "domain": domain,
        "http_status": http_status,
        "content_type": content_type,
        "title": seo["title"],
        "meta_description": seo["desc"],
        "lang": seo["lang"],
        "canonical_url": seo["canonical"] or "",
        "og_title": seo["og_title"],
        "og_type": seo["og_type"],
        "og_description": seo["og_description"],
        "twitter_card": seo["twitter_card"],
        "json_ld_types": seo["json_ld_types_str"],
        "tags_all": tags_all,
        "url_content_hint": seo["hint"],
        "content_kind_guess": seo["kind"],
        "h1_joined": seo["h1_joined"],
        "word_count": seo["word_count"],
        # Phase 1 — freshness / provenance
        "http_last_modified": response_meta.get("last_modified", ""),
        "etag": response_meta.get("etag", ""),
        "sitemap_lastmod": sitemap_meta.get("sitemap_lastmod", ""),
        "referrer_sitemap_url": sitemap_meta.get("source_sitemap", ""),
        # Phase 2 — on-page quality / trust
        "heading_outline": quality["heading_outline"],
        "date_published": quality["date_published"],
        "date_modified": quality["date_modified"],
        "visible_dates": quality["visible_dates"],
        "link_count_internal": quality["link_int"],
        "link_count_external": quality["link_ext"],
        "link_count_total": quality["link_total"],
        "img_count": quality["img_count"],
        "img_missing_alt_count": quality["img_no_alt"],
        "readability_fk_grade": quality["readability"],
        "privacy_policy_url": quality["privacy_url"],
        "analytics_signals": quality["analytics"],
        "training_related_flag": quality["training_flag"],
        # Phase 3
        "nav_link_count": quality["nav_count"],
        # WCAG static checks
        **quality["wcag"],
        # Phase 4 — extended extraction
        "author": phase4["author"],
        "publisher": phase4["publisher"],
        "json_ld_id": phase4["json_ld_id"],
        "cms_generator": phase4["cms_generator"],
        "robots_directives": phase4["robots_directives"],
        "hreflang_links": phase4["hreflang_links"],
        "feed_urls": phase4["feed_urls"],
        "pagination_next": pagination_next,
        "pagination_prev": pagination_prev,
        "breadcrumb_schema": phase4["breadcrumb_schema"],
        "microdata_types": phase4["microdata_types"],
        "rdfa_types": phase4["rdfa_types"],
        **phase4["schema_specific"],
        # common
        "referrer_url": referrer_url,
        "depth": depth,
        "discovered_at": discovered_at,
    }


# ── Inline asset extraction ────────────────────────────────────────────────


def extract_inline_assets(
    html: str,
    base_url: str,
    discovered_at: str,
) -> List[dict]:
    """Extract assets from ``<img>``, ``<link>``, ``<script>``, ``<video>``,
    ``<audio>``, and ``<source>`` elements — i.e. resources embedded in the
    page that are not discovered via ``<a href>`` links.

    Returns dicts matching the ``ASSET_FIELDS`` schema (with empty
    ``head_content_type`` / ``head_content_length``; HEAD is skipped for
    inline assets to avoid excessive requests).
    """
    soup = BeautifulSoup(html, "lxml")
    assets: List[dict] = []
    seen: Set[str] = set()

    def _add(url: Optional[str], text: str, category: str) -> None:
        if not url or url in seen:
            return
        seen.add(url)
        assets.append({
            "referrer_page_url": base_url,
            "asset_url": url,
            "link_text": text,
            "category": category,
            "head_content_type": "",
            "head_content_length": "",
            "discovered_at": discovered_at,
        })

    for img in soup.find_all("img", src=True):
        src = _normalise_url(img["src"].strip(), base_url)
        if src:
            _add(src, (img.get("alt") or "").strip()[:200], "image")

    for link in soup.find_all("link", href=True):
        rel = link.get("rel", [])
        if isinstance(rel, str):
            rel = rel.split()
        href = _normalise_url(link["href"].strip(), base_url)
        if not href:
            continue
        if "stylesheet" in rel:
            _add(href, "", "stylesheet")
        elif any(r in rel for r in ("icon", "apple-touch-icon", "shortcut")):
            _add(href, "", "image")
        else:
            ext = _path_extension_lower(urlparse(href).path)
            cat = config.ASSET_CATEGORY_BY_EXT.get(ext)
            if cat:
                _add(href, "", cat)

    for script in soup.find_all("script", src=True):
        src = _normalise_url(script["src"].strip(), base_url)
        if src:
            _add(src, "", "script")

    for video in soup.find_all("video"):
        if video.get("src"):
            src = _normalise_url(video["src"].strip(), base_url)
            if src:
                _add(src, "", "video")
        for source in video.find_all("source", src=True):
            src = _normalise_url(source["src"].strip(), base_url)
            if src:
                _add(src, "", "video")

    for audio in soup.find_all("audio"):
        if audio.get("src"):
            src = _normalise_url(audio["src"].strip(), base_url)
            if src:
                _add(src, "", "audio")
        for source in audio.find_all("source", src=True):
            src = _normalise_url(source["src"].strip(), base_url)
            if src:
                _add(src, "", "audio")

    return assets
