"""
Ecosystem mapping — data aggregation helpers.

Data flow
---------
Each crawl run produces a directory of CSV files (pages.csv, edges.csv,
tags.csv, nav_links.csv, crawl_errors.csv, assets_*.csv).  Functions in
this module read those CSVs, apply optional cross-cutting filters, and
return JSON-serialisable dicts / lists.  The ``viz_api`` Blueprint
delegates to these functions and serves the results as JSON to the D3.js
reports dashboard on the front end.

All functions are pure (no side effects on disk or global state) and
accept a *run_dirs* list so the dashboard can aggregate across one or
more crawl runs within a project.
"""
from __future__ import annotations

import csv
import os
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import tldextract

import config

import logging

logger = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────────

def _safe_int(val: str, default: int = 0) -> int:
    """Coerce a CSV cell to int, returning *default* on blank/malformed values."""
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _safe_float(val: str, default: float = 0.0) -> float:
    """Coerce a CSV cell to float, returning *default* on blank/malformed values."""
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _read_csv(path: str) -> List[Dict[str, str]]:
    """Load a CSV into a list of row dicts.

    Returns ``[]`` silently when the file does not exist.  Logs a warning and
    returns ``[]`` when the file exists but cannot be read or parsed.
    """
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception as e:
        logger.warning("Could not read CSV %s: %s", path, e)
        return []


def _read_csv_multi(run_dirs: List[str], filename: str) -> List[Dict[str, str]]:
    """Concatenate a named CSV from one or more run directories."""
    rows: List[Dict[str, str]] = []
    for rd in run_dirs:
        rows.extend(_read_csv(os.path.join(rd, filename)))
    return rows


# -- Global Filter --------------------------------------------------------

def filter_pages(
    rows: List[Dict[str, str]],
    filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, str]]:
    """Apply cross-cutting filters to page rows.

    Filters are combined with AND logic — every active filter must pass
    for a row to be included.  This function is the single choke-point
    that all aggregate_* functions call, so adding a new filter dimension
    here automatically propagates to every endpoint.

    Args:
        rows: List of page row dicts (from pages.csv via ``_read_csv``).
        filters: Dict whose keys select filter dimensions.  All keys are
            optional; only those present and non-empty are applied.

            - ``domains`` — list of domain strings (exact match).
            - ``cms`` — list of CMS generator substrings (case-insensitive).
            - ``content_kinds`` — list of content_kind_guess values.
            - ``schema_formats`` — list from {"json_ld", "microdata", "rdfa"}.
            - ``schema_types`` — list of schema type substrings.
            - ``date_from`` — YYYY-MM-DD lower bound on date_published /
              date_modified.
            - ``date_to`` — YYYY-MM-DD upper bound (rows with no date pass).
            - ``min_coverage`` — float 0-100 minimum extraction_coverage_pct.

    Returns:
        Filtered subset of *rows* (may be the original list if no filters).
    """
    if not filters:
        return rows

    result = rows

    if filters.get("domains"):
        allowed = {d.lower() for d in filters["domains"]}
        result = [r for r in result if r.get("domain", "").lower() in allowed]

    if filters.get("cms"):
        cms_lower = [c.lower() for c in filters["cms"]]
        result = [
            r for r in result
            if any(c in r.get("cms_generator", "").lower() for c in cms_lower)
        ]

    if filters.get("content_kinds"):
        allowed_kinds = {k.lower() for k in filters["content_kinds"]}
        result = [
            r for r in result
            if r.get("content_kind_guess", "").lower() in allowed_kinds
        ]

    if filters.get("schema_formats"):
        fmts = {f.lower() for f in filters["schema_formats"]}

        def _has_format(row: Dict[str, str]) -> bool:
            if "json_ld" in fmts and row.get("json_ld_types", "").strip():
                return True
            if "microdata" in fmts and row.get("microdata_types", "").strip():
                return True
            if "rdfa" in fmts and row.get("rdfa_types", "").strip():
                return True
            return False
        result = [r for r in result if _has_format(r)]

    if filters.get("schema_types"):
        types_lower = [t.lower() for t in filters["schema_types"]]

        def _has_type(row: Dict[str, str]) -> bool:
            combined = (
                row.get("json_ld_types", "") + "|" +
                row.get("microdata_types", "") + "|" +
                row.get("rdfa_types", "")
            ).lower()
            return any(t in combined for t in types_lower)
        result = [r for r in result if _has_type(r)]

    if filters.get("date_from"):
        df = filters["date_from"]
        result = [
            r for r in result
            if (r.get("date_published", "") >= df or
                r.get("date_modified", "") >= df)
        ]

    if filters.get("date_to"):
        dt = filters["date_to"]
        # Rows with no date at all are kept so they are not silently
        # excluded when the user sets an upper date bound.
        result = [
            r for r in result
            if (r.get("date_published", "")[:10] <= dt or
                r.get("date_modified", "")[:10] <= dt or
                (not r.get("date_published") and not r.get("date_modified")))
        ]

    if filters.get("min_coverage") is not None:
        mc = float(filters["min_coverage"])
        result = [
            r for r in result
            if _safe_float(r.get("extraction_coverage_pct", "0")) >= mc
        ]

    return result


def _extract_domain(url: str) -> str:
    """Return the hostname portion of *url*, or '' on failure."""
    try:
        return urlparse(url).hostname or ""
    except Exception:
        return ""


def _page_url_for_row(row: Dict[str, str]) -> str:
    """Prefer ``final_url`` for display; fall back to ``requested_url``."""
    return (row.get("final_url") or row.get("requested_url") or "").strip()


def _robots_noindex_sources(robots_directives: str) -> Tuple[bool, bool]:
    """Return whether *robots_directives* implies noindex via meta or HTTP header.

    Parsed segments follow ``meta:...`` / ``header:...`` (see
    ``parser._extract_robots_directives``).  If ``noindex`` appears outside
    those prefixes, it is treated as meta-like for counting purposes.
    """
    if not robots_directives or "noindex" not in robots_directives.lower():
        return False, False
    from_meta = False
    from_header = False
    for seg in robots_directives.split("|"):
        piece = seg.strip()
        if not piece or "noindex" not in piece.lower():
            continue
        low = piece.lower()
        if low.startswith("meta:"):
            from_meta = True
        elif low.startswith("header:"):
            from_header = True
        else:
            from_meta = True
    return from_meta, from_header


def _parse_date(raw: str) -> Optional[str]:
    """Normalise heterogeneous date strings to YYYY-MM-DD.

    Tries an ordered list of strptime patterns first (ISO-8601, RFC-2822,
    etc.) then falls back to a regex extraction so that partial or
    non-standard date strings still yield a usable date for timeline
    aggregation.

    Returns:
        A 'YYYY-MM-DD' string, or None if no date could be extracted.
    """
    raw = raw.strip()
    if not raw:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S+00:00",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%a, %d %b %Y %H:%M:%S GMT",
    ):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Fallback: extract the first YYYY-MM-DD substring
    m = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
    if m:
        return m.group(1)
    return None


def _build_ownership_map(all_domains: List[str]) -> Dict[str, str]:
    """Derive ownership categories from the crawl's domain set.

    Priority order:
    1. Manual ``DOMAIN_OWNERSHIP_RULES`` (first match wins).
    2. Registered domain via ``tldextract`` — groups subdomains of the
       same organisation together (e.g. ``www.nhsbsa.nhs.uk`` and
       ``learning.nhsbsa.nhs.uk`` both map to ``nhsbsa.nhs.uk``) while
       keeping distinct organisations separate.

    Args:
        all_domains: Unique domain strings drawn from the ``domain``
            column of pages.csv.

    Returns:
        Mapping of each domain to its ownership label string.
    """
    ownership: Dict[str, str] = {}

    for dom in all_domains:
        d = dom.lower()
        matched = False
        # Manual rules take precedence — suffix match lets a single rule
        # claim an entire TLD family (e.g. ".nhs.uk" -> "NHS").
        for suffix, label in config.DOMAIN_OWNERSHIP_RULES:
            if d.endswith(suffix.lower()) or d == suffix.lower():
                ownership[dom] = label
                matched = True
                break
        if not matched:
            # tldextract decomposes the domain into (subdomain, domain,
            # suffix) using the Public Suffix List, so
            # top_domain_under_public_suffix reliably groups subdomains
            # under one organisation even for multi-part TLDs like
            # .co.uk or .nhs.uk.
            ext = tldextract.extract(dom)
            ownership[dom] = ext.top_domain_under_public_suffix or dom

    return ownership


def _ownership_fallback(domain: str) -> str:
    """Single-domain ownership lookup for domains outside the pre-built map.

    Used by ``aggregate_domain_graph`` for edge-endpoint domains that
    were not present in the pages.csv domain set.
    """
    d = domain.lower()
    for suffix, label in config.DOMAIN_OWNERSHIP_RULES:
        if d.endswith(suffix.lower()) or d == suffix.lower():
            return label
    return tldextract.extract(domain).top_domain_under_public_suffix or domain


# -- Domain Aggregation ---------------------------------------------------


def _domain_accumulator_defaults(
    dom: str,
    ownership_map: Dict[str, str],
) -> Dict[str, Any]:
    """Return a fresh accumulator dict for one domain, ready for per-row updates."""
    return {
        "domain": dom,
        "ownership": ownership_map.get(dom, config.DOMAIN_OWNERSHIP_DEFAULT),
        "page_count": 0,
        "total_words": 0,
        "total_images": 0,
        "images_missing_alt": 0,
        "readability_sum": 0.0,
        "readability_n": 0,
        "training_pages": 0,
        "status_codes": Counter(),
        "content_kinds": Counter(),
        "analytics_tools": set(),
        "analytics_tool_pages": Counter(),
        "has_privacy_policy": False,
        "privacy_policy_pages": 0,
        "dates_modified": [],
        "dates_published": [],
        "max_depth": 0,
        "link_internal_sum": 0,
        "link_external_sum": 0,
        "titles": [],
        "wcag_lang_valid_count": 0,
        "wcag_heading_order_valid_count": 0,
        "wcag_title_present_count": 0,
        "wcag_form_labels_sum": 0.0,
        "wcag_form_labels_n": 0,
        "wcag_landmarks_present_count": 0,
        "wcag_vague_link_sum": 0.0,
        "wcag_vague_link_n": 0,
        # Phase 4 accumulators
        "cms_generators": Counter(),
        "authors": Counter(),
        "publishers": Counter(),
        "has_json_ld_count": 0,
        "has_microdata_count": 0,
        "has_rdfa_count": 0,
        "has_hreflang_count": 0,
        "has_feed_count": 0,
        "has_pagination_count": 0,
        "has_breadcrumb_schema_count": 0,
        "robots_noindex_count": 0,
        "schema_types": Counter(),
        "extraction_coverage_sum": 0.0,
        "extraction_coverage_n": 0,
        "extraction_coverage_core_sum": 0.0,
        "extraction_coverage_core_n": 0,
    }


def _accumulate_page_row(d: Dict[str, Any], r: Dict[str, str]) -> None:
    """Update domain accumulator *d* in-place with data from page row *r*."""
    d["page_count"] += 1
    d["total_words"] += _safe_int(r.get("word_count", "0"))
    d["total_images"] += _safe_int(r.get("img_count", "0"))
    d["images_missing_alt"] += _safe_int(r.get("img_missing_alt_count", "0"))

    rk = _safe_float(r.get("readability_fk_grade", ""))
    if rk > 0:
        d["readability_sum"] += rk
        d["readability_n"] += 1

    if r.get("training_related_flag", "").strip():
        d["training_pages"] += 1

    d["status_codes"][r.get("http_status", "?")] += 1
    kind = r.get("content_kind_guess", "").strip() or "(unclassified)"
    d["content_kinds"][kind] += 1

    # analytics_signals is pipe-delimited (e.g. "GA4|GTM|Hotjar")
    for sig in r.get("analytics_signals", "").split("|"):
        sig = sig.strip()
        if sig:
            d["analytics_tools"].add(sig)
            d["analytics_tool_pages"][sig] += 1

    if r.get("privacy_policy_url", "").strip():
        d["has_privacy_policy"] = True
        d["privacy_policy_pages"] += 1

    dm = _parse_date(r.get("date_modified", ""))
    if dm:
        d["dates_modified"].append(dm)
    dp = _parse_date(r.get("date_published", ""))
    if dp:
        d["dates_published"].append(dp)

    # Sitemap lastmod is folded into dates_modified so the freshness
    # timeline treats it equivalently to in-page date_modified.
    sm = _parse_date(r.get("sitemap_lastmod", ""))
    if sm:
        d["dates_modified"].append(sm)

    depth = _safe_int(r.get("depth", "0"))
    if depth > d["max_depth"]:
        d["max_depth"] = depth

    d["link_internal_sum"] += _safe_int(r.get("link_count_internal", "0"))
    d["link_external_sum"] += _safe_int(r.get("link_count_external", "0"))

    title = r.get("title", "").strip()
    if title and len(d["titles"]) < 3:
        d["titles"].append(title)

    if r.get("wcag_lang_valid", "") == "1":
        d["wcag_lang_valid_count"] += 1
    if r.get("wcag_heading_order_valid", "") == "1":
        d["wcag_heading_order_valid_count"] += 1
    if r.get("wcag_title_present", "") == "1":
        d["wcag_title_present_count"] += 1
    fl = _safe_float(r.get("wcag_form_labels_pct", ""))
    if fl >= 0:
        d["wcag_form_labels_sum"] += fl
        d["wcag_form_labels_n"] += 1
    if r.get("wcag_landmarks_present", "") == "1":
        d["wcag_landmarks_present_count"] += 1
    vl = _safe_float(r.get("wcag_vague_link_pct", ""))
    if vl >= 0:
        d["wcag_vague_link_sum"] += vl
        d["wcag_vague_link_n"] += 1

    # Phase 4 accumulation
    cms = r.get("cms_generator", "").strip()
    if cms:
        d["cms_generators"][cms] += 1
    author = r.get("author", "").strip()
    if author:
        d["authors"][author] += 1
    publisher = r.get("publisher", "").strip()
    if publisher:
        d["publishers"][publisher] += 1
    if r.get("json_ld_types", "").strip():
        d["has_json_ld_count"] += 1
        for t in r["json_ld_types"].split("|"):
            t = t.strip()
            if t:
                d["schema_types"][t] += 1
    if r.get("microdata_types", "").strip():
        d["has_microdata_count"] += 1
        for t in r["microdata_types"].split("|"):
            t = t.strip()
            if t:
                d["schema_types"][t] += 1
    if r.get("rdfa_types", "").strip():
        d["has_rdfa_count"] += 1
    if r.get("hreflang_links", "").strip():
        d["has_hreflang_count"] += 1
    if r.get("feed_urls", "").strip():
        d["has_feed_count"] += 1
    if r.get("pagination_next", "").strip() or r.get("pagination_prev", "").strip():
        d["has_pagination_count"] += 1
    if r.get("breadcrumb_schema", "").strip():
        d["has_breadcrumb_schema_count"] += 1
    robots = r.get("robots_directives", "").lower()
    if "noindex" in robots:
        d["robots_noindex_count"] += 1
    cov = _safe_float(r.get("extraction_coverage_pct", ""))
    if cov > 0:
        d["extraction_coverage_sum"] += cov
        d["extraction_coverage_n"] += 1
    cov_core = _safe_float(r.get("extraction_coverage_core_pct", ""))
    if cov_core > 0:
        d["extraction_coverage_core_sum"] += cov_core
        d["extraction_coverage_core_n"] += 1


def _domain_row_to_json(
    dom: str,
    d: Dict[str, Any],
    error_ctr: Dict[str, int],
    asset_counts: Dict[str, Dict[str, int]],
) -> Dict[str, Any]:
    """Flatten a domain accumulator dict into the final JSON-serialisable shape."""
    pc = d["page_count"]
    avg_words = round(d["total_words"] / pc) if pc else 0
    avg_readability = round(d["readability_sum"] / d["readability_n"], 1) if d["readability_n"] else 0
    alt_pct = round(d["images_missing_alt"] / d["total_images"] * 100, 1) if d["total_images"] else 0

    all_dates = sorted(d["dates_modified"] + d["dates_published"])
    latest_date = all_dates[-1] if all_dates else None
    oldest_date = all_dates[0] if all_dates else None

    top_cms = d["cms_generators"].most_common(1)
    primary_cms = top_cms[0][0] if top_cms else ""

    return {
        "domain": dom,
        "ownership": d["ownership"],
        "page_count": pc,
        "avg_word_count": avg_words,
        "avg_readability": avg_readability,
        "total_images": d["total_images"],
        "images_missing_alt": d["images_missing_alt"],
        "alt_missing_pct": alt_pct,
        "training_pages": d["training_pages"],
        "error_count": error_ctr.get(dom, 0),
        "status_codes": dict(d["status_codes"]),
        "content_kinds": dict(d["content_kinds"]),
        "analytics_tools": sorted(d["analytics_tools"]),
        "analytics_tool_pages": dict(d["analytics_tool_pages"]),
        "has_privacy_policy": d["has_privacy_policy"],
        "privacy_policy_pages": d["privacy_policy_pages"],
        "latest_date": latest_date,
        "oldest_date": oldest_date,
        "date_count": len(all_dates),
        "max_depth": d["max_depth"],
        "avg_internal_links": round(d["link_internal_sum"] / pc, 1) if pc else 0,
        "avg_external_links": round(d["link_external_sum"] / pc, 1) if pc else 0,
        "titles": d["titles"],
        "assets": dict(asset_counts.get(dom, {})),
        "total_assets": sum(asset_counts.get(dom, {}).values()),
        "wcag_lang_pct": round(d["wcag_lang_valid_count"] / pc * 100, 1) if pc else 0,
        "wcag_heading_order_pct": round(d["wcag_heading_order_valid_count"] / pc * 100, 1) if pc else 0,
        "wcag_title_pct": round(d["wcag_title_present_count"] / pc * 100, 1) if pc else 0,
        "wcag_form_labels_pct": round(
            d["wcag_form_labels_sum"] / d["wcag_form_labels_n"] * 100, 1,
        ) if d["wcag_form_labels_n"] else 100.0,
        "wcag_landmarks_pct": round(d["wcag_landmarks_present_count"] / pc * 100, 1) if pc else 0,
        "wcag_vague_link_pct": round(
            d["wcag_vague_link_sum"] / d["wcag_vague_link_n"] * 100, 1,
        ) if d["wcag_vague_link_n"] else 0.0,
        # Phase 4 fields
        "cms_generator": primary_cms,
        "cms_generators": dict(d["cms_generators"]),
        "top_authors": [a for a, _ in d["authors"].most_common(5)],
        "top_publishers": [p for p, _ in d["publishers"].most_common(3)],
        "has_json_ld_pct": round(d["has_json_ld_count"] / pc * 100, 1) if pc else 0,
        "has_microdata_pct": round(d["has_microdata_count"] / pc * 100, 1) if pc else 0,
        "has_rdfa_pct": round(d["has_rdfa_count"] / pc * 100, 1) if pc else 0,
        "has_hreflang_pct": round(d["has_hreflang_count"] / pc * 100, 1) if pc else 0,
        "has_feed_pct": round(d["has_feed_count"] / pc * 100, 1) if pc else 0,
        "has_pagination_pct": round(d["has_pagination_count"] / pc * 100, 1) if pc else 0,
        "has_breadcrumb_schema_pct": round(d["has_breadcrumb_schema_count"] / pc * 100, 1) if pc else 0,
        "robots_noindex_pct": round(d["robots_noindex_count"] / pc * 100, 1) if pc else 0,
        "schema_types": dict(d["schema_types"].most_common(20)),
        "avg_extraction_coverage": round(
            d["extraction_coverage_sum"] / d["extraction_coverage_n"], 1,
        ) if d["extraction_coverage_n"] else 0,
        "avg_extraction_coverage_core": round(
            d["extraction_coverage_core_sum"] / d["extraction_coverage_core_n"], 1,
        ) if d["extraction_coverage_core_n"] else 0,
    }


def aggregate_domains(
    run_dirs: List[str],
    filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Build per-domain summary dicts from pages.csv, errors CSV, and asset CSVs.

    This is the primary data source for the dashboard's domain table and
    bubble chart.  It performs a single pass over filtered page rows to
    accumulate counts (via ``_accumulate_page_row``), then a second pass
    to flatten accumulators into the final JSON-friendly shape
    (via ``_domain_row_to_json``).

    Args:
        run_dirs: Absolute paths to one or more crawl run directories.
        filters: Optional cross-cutting filter dict (see ``filter_pages``).

    Returns:
        List of dicts sorted by ``page_count`` descending.  Key groups:
        - identity: ``domain``, ``ownership``
        - volume: ``page_count``, ``avg_word_count``, ``total_images``
        - quality: ``avg_readability``, ``alt_missing_pct``,
          ``avg_extraction_coverage``
        - dates: ``latest_date``, ``oldest_date``, ``date_count``
        - accessibility: ``wcag_lang_pct``, ``wcag_heading_order_pct``, etc.
        - structured data: ``has_json_ld_pct``, ``schema_types``, etc.
        - SEO: ``robots_noindex_pct``, ``has_hreflang_pct``
        - assets: ``assets`` (dict of category -> count), ``total_assets``

        The JS dashboard reads these keys directly for D3 bindings, so
        renaming any key requires a coordinated front-end change.
    """
    pages = filter_pages(
        _read_csv_multi(run_dirs, config.PAGES_CSV), filters,
    )
    if not pages:
        return []

    all_doms = list({r.get("domain", "unknown") for r in pages})
    ownership_map = _build_ownership_map(all_doms)

    domains: Dict[str, Dict[str, Any]] = {}
    for r in pages:
        dom = r.get("domain", "unknown")
        if dom not in domains:
            domains[dom] = _domain_accumulator_defaults(dom, ownership_map)
        _accumulate_page_row(domains[dom], r)

    # Merge error rows — these pages are absent from pages.csv so their
    # HTTP status codes only appear in crawl_errors.csv.
    errors = _read_csv_multi(run_dirs, config.ERRORS_CSV)
    error_ctr: Dict[str, int] = Counter()
    for e in errors:
        edom = _extract_domain(e.get("url", ""))
        if not edom:
            edom = e.get("url", "").split("/")[2] if "/" in e.get("url", "") else "unknown"
        error_ctr[edom] += 1
        err_status = e.get("http_status", "").strip()
        if err_status and err_status != "0" and edom in domains:
            domains[edom]["status_codes"][err_status] += 1

    # Scan per-type asset CSVs (assets_pdf.csv, assets_doc.csv, etc.)
    # and attribute each asset to the referring page's domain.
    asset_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for rd in run_dirs:
        if not os.path.isdir(rd):
            continue
        for name in sorted(os.listdir(rd)):
            if name.startswith("assets_") and name.endswith(".csv"):
                cat = name[len("assets_"):-len(".csv")]
                rows = _read_csv(os.path.join(rd, name))
                for ar in rows:
                    ref = ar.get("referrer_page_url", "")
                    adom = _extract_domain(ref)
                    if adom:
                        asset_counts[adom][cat] += 1

    result = [
        _domain_row_to_json(dom, d, error_ctr, asset_counts)
        for dom, d in domains.items()
    ]
    result.sort(key=lambda x: -x["page_count"])
    return result


# -- Domain Graph ---------------------------------------------------------

def aggregate_domain_graph(
    run_dirs: List[str], filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Collapse page-level edges into a weighted domain-to-domain graph.

    Reads pages.csv (for the domain set and ownership) and edges.csv
    (``from_url`` / ``to_url`` columns).  Self-links within the same
    domain are excluded so the result only shows inter-domain linking.

    Args:
        run_dirs: Absolute paths to one or more crawl run directories.
        filters: Optional cross-cutting filter dict (see ``filter_pages``).

    Returns:
        ``{"nodes": [...], "links": [...]}`` consumed by D3 force, chord,
        and sankey layouts.  Each node has ``id``, ``index``, ``pages``,
        ``ownership``; each link has ``source``, ``target``, ``weight``.
    """
    pages = filter_pages(
        _read_csv_multi(run_dirs, config.PAGES_CSV), filters,
    )
    all_doms = list({r.get("domain", "unknown") for r in pages})
    ownership_map = _build_ownership_map(all_doms)

    domain_pages: Counter[str] = Counter()
    for r in pages:
        dom = r.get("domain", "unknown")
        domain_pages[dom] += 1

    edge_weights: Counter[Tuple[str, str]] = Counter()
    for rd in run_dirs:
        edges_path = os.path.join(rd, config.EDGES_CSV)
        if not os.path.isfile(edges_path):
            continue
        try:
            with open(edges_path, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    src = _extract_domain(row.get("from_url", ""))
                    tgt = _extract_domain(row.get("to_url", ""))
                    if src and tgt and src != tgt:
                        if src in domain_pages or tgt in domain_pages:
                            edge_weights[(src, tgt)] += 1
        except Exception:
            pass

    all_domains = set(domain_pages.keys())
    for s, t in edge_weights:
        all_domains.add(s)
        all_domains.add(t)

    domain_idx = {d: i for i, d in enumerate(sorted(all_domains))}

    nodes = []
    for dom in sorted(all_domains):
        nodes.append({
            "id": dom,
            "index": domain_idx[dom],
            "pages": domain_pages.get(dom, 0),
            "ownership": ownership_map.get(dom, _ownership_fallback(dom)),
        })

    links = []
    for (src, tgt), weight in edge_weights.most_common():
        links.append({
            "source": src,
            "target": tgt,
            "weight": weight,
        })

    return {"nodes": nodes, "links": links}


# -- Tag Aggregation ------------------------------------------------------

def aggregate_tags(
    run_dirs: List[str], filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Compute tag frequencies and pairwise co-occurrence for word clouds.

    Reads tags.csv (``page_url``, ``tag_value``, ``tag_source`` columns).
    When filters are active the tag set is restricted to URLs surviving
    the filter applied to pages.csv — tags.csv itself is not filtered
    directly because it lacks the columns the filter inspects.

    Args:
        run_dirs: Absolute paths to one or more crawl run directories.
        filters: Optional cross-cutting filter dict (see ``filter_pages``).

    Returns:
        Dict with keys:
        - ``tags``: list of ``{tag, count}`` (top 150 by frequency).
        - ``sources``: dict of tag_source -> count (top 20).
        - ``cooccurrence``: list of ``{source, target, weight}`` pairs
          (top 200) — only among the top-150 tags, suitable for a D3
          force-directed tag network.
    """
    # When filters are active, restrict to tags from filtered page URLs
    allowed_urls: Optional[set] = None
    if filters:
        pages = filter_pages(
            _read_csv_multi(run_dirs, config.PAGES_CSV), filters,
        )
        allowed_urls = {r.get("final_url", "") for r in pages}

    tags_rows = _read_csv_multi(run_dirs, config.TAGS_CSV)

    freq: Counter[str] = Counter()
    source_freq: Counter[str] = Counter()
    page_tags: Dict[str, List[str]] = defaultdict(list)

    for r in tags_rows:
        if allowed_urls is not None and r.get("page_url", "") not in allowed_urls:
            continue
        tag = r.get("tag_value", "").strip()
        if not tag or len(tag) > 80:
            continue
        tag_lower = tag.lower()
        freq[tag_lower] += 1
        source_freq[r.get("tag_source", "")] += 1
        page_tags[r.get("page_url", "")].append(tag_lower)

    top_tags = freq.most_common(150)

    top_set = {t for t, _ in top_tags}
    cooccur: Counter[Tuple[str, str]] = Counter()
    for url, tag_list in page_tags.items():
        filtered = list(set(t for t in tag_list if t in top_set))
        for i in range(len(filtered)):
            for j in range(i + 1, len(filtered)):
                a, b = sorted([filtered[i], filtered[j]])
                cooccur[(a, b)] += 1

    top_pairs = cooccur.most_common(200)

    return {
        "tags": [{"tag": t, "count": c} for t, c in top_tags],
        "sources": dict(source_freq.most_common(20)),
        "cooccurrence": [
            {"source": a, "target": b, "weight": w}
            for (a, b), w in top_pairs
        ],
    }


# -- Navigation Hierarchy -------------------------------------------------

def _build_internal_nav_tree(entries: List[Dict[str, Any]], max_depth: int) -> List[Dict[str, Any]]:
    """Recursively group internal nav entries by URL path segments.

    Each entry is a dict with keys ``name`` (nav_text), ``href``, and
    ``segments`` (split path parts of the target URL).  Entries are
    grouped by their path segment at each recursion level up to
    *max_depth*; beyond that they are emitted as leaf nodes.

    Args:
        entries:   List of entry dicts produced by ``aggregate_navigation``.
        max_depth: Maximum number of URL path segments to recurse through
                   before emitting remaining entries as leaves.  A value
                   of 1 groups only by the first segment (root-section
                   level), matching the previous behaviour.

    Returns:
        List of D3-compatible node dicts (``name``, ``group``, ``children``
        for groups; ``name``, ``href``, ``size`` for leaves).
    """
    def recurse(items: List[Dict[str, Any]], depth: int) -> List[Dict[str, Any]]:
        if depth >= max_depth or not items:
            # Emit all remaining items as leaf nodes (deduplicate by name).
            seen: Dict[str, str] = {}
            for item in items:
                if item["name"] not in seen:
                    seen[item["name"]] = item["href"]
            return [{"name": n, "href": h, "size": 1} for n, h in sorted(seen.items())]

        by_seg: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for item in items:
            seg = item["segments"][depth] if depth < len(item["segments"]) else ""
            by_seg[seg].append(item)

        result: List[Dict[str, Any]] = []
        for seg in sorted(by_seg):
            group = by_seg[seg]
            if not seg:
                # This item's path ends here — emit as leaf(ves).
                seen: Dict[str, str] = {}
                for item in group:
                    if item["name"] not in seen:
                        seen[item["name"]] = item["href"]
                result.extend(
                    {"name": n, "href": h, "size": 1}
                    for n, h in sorted(seen.items())
                )
            elif len(group) == 1:
                item = group[0]
                result.append({"name": item["name"], "href": item["href"], "size": 1})
            else:
                children = recurse(group, depth + 1)
                if len(children) == 1:
                    result.append(children[0])
                else:
                    result.append({
                        "name": "/" + seg,
                        "group": True,
                        "children": children,
                    })
        return result

    return recurse(entries, 0)


def aggregate_navigation(
    run_dirs: List[str], domain: Optional[str] = None,
    max_depth: int = 2,
    filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a hierarchical navigation tree from nav_links.csv.

    Reads ``page_url``, ``nav_text``, ``nav_href`` columns.  When
    *domain* is supplied the tree is structured as::

        root (source domain)
        +-- Internal Links
        |   +-- /section-a
        |   |   +-- /sub-section   (present when max_depth > 1)
        |   |   |   +-- Link Label ...
        |   |   +-- Link Label ...
        |   +-- /section-b
        |       +-- ...
        +-- External Links
            +-- target-domain-1
            |   +-- Link Label ...
            +-- target-domain-2

    Internal links are grouped recursively by URL path segments up to
    *max_depth* levels; external links are always grouped by target domain.

    Args:
        run_dirs:  Absolute paths to one or more crawl run directories.
        domain:    If supplied, return the full tree for this domain only.
                   Otherwise return a per-domain summary list.
        max_depth: How many URL path segments to recurse through for
                   internal links (1 = section level only, 2 = section +
                   sub-section, etc.).  Defaults to 2.
        filters:   Not currently used (nav_links.csv lacks filter columns)
                   but accepted for API signature consistency.

    Returns:
        ``{"domains": [...], "tree": {...} | None}`` — the D3 treemap /
        sunburst layout reads the ``tree`` key when a single domain is
        selected; the domain picker reads ``domains``.
    """
    nav_rows = _read_csv_multi(run_dirs, config.NAV_LINKS_CSV)
    if not nav_rows:
        return {"domains": [], "tree": None}

    by_domain: Dict[str, Dict[str, set]] = defaultdict(lambda: defaultdict(set))
    for r in nav_rows:
        page_dom = _extract_domain(r.get("page_url", ""))
        nav_text = r.get("nav_text", "").strip()
        nav_href = r.get("nav_href", "").strip()
        if page_dom and nav_text:
            by_domain[page_dom][nav_text].add(nav_href)

    if domain and domain in by_domain:
        items = by_domain[domain]

        internal_entries: List[Dict[str, Any]] = []
        external_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        for text, hrefs in sorted(items.items()):
            href = next(iter(hrefs), "")
            target_dom = _extract_domain(href) if href else ""
            is_external = bool(target_dom and target_dom != domain)

            if is_external:
                external_groups[target_dom or "other"].append({
                    "name": text,
                    "href": href,
                    "external": True,
                    "size": len(hrefs),
                })
            else:
                parsed = urlparse(href)
                segments = [s for s in parsed.path.strip("/").split("/") if s]
                internal_entries.append({
                    "name": text,
                    "href": href,
                    "segments": segments,
                })

        children = []

        if internal_entries:
            int_children = _build_internal_nav_tree(internal_entries, max(1, max_depth))
            if int_children:
                children.append({
                    "name": "Internal",
                    "group": True,
                    "children": int_children,
                })

        if external_groups:
            ext_children = []
            for tgt_dom in sorted(external_groups):
                leaves = external_groups[tgt_dom]
                if len(leaves) == 1:
                    ext_children.append(leaves[0])
                else:
                    ext_children.append({
                        "name": tgt_dom,
                        "group": True,
                        "external": True,
                        "children": sorted(leaves, key=lambda leaf: leaf["name"]),
                    })
            children.append({
                "name": "External",
                "group": True,
                "external": True,
                "children": ext_children,
            })

        return {
            "domains": list(sorted(by_domain.keys())),
            "tree": {"name": domain, "children": children},
        }

    summaries = []
    for dom in sorted(by_domain.keys()):
        items = by_domain[dom]
        ext_count = 0
        for text, hrefs in items.items():
            for h in hrefs:
                if _extract_domain(h) != dom:
                    ext_count += 1
        summaries.append({
            "domain": dom,
            "nav_items": len(items),
            "external_links": ext_count,
        })
    summaries.sort(key=lambda x: -x["nav_items"])
    return {"domains": summaries, "tree": None}


# -- Freshness Timeline ---------------------------------------------------

def aggregate_freshness(
    run_dirs: List[str], filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Compute per-domain freshness data for the timeline chart.

    Merges ``date_modified``, ``sitemap_lastmod``, and
    ``date_published`` columns from pages.csv and buckets them by
    year-month.

    Args:
        run_dirs: Absolute paths to one or more crawl run directories.
        filters: Optional cross-cutting filter dict (see ``filter_pages``).

    Returns:
        ``{"today": "YYYY-MM-DD", "domains": [...]}`` where each domain
        entry has ``latest``, ``oldest``, ``total_dates``, and
        ``buckets`` (a ``{YYYY-MM: count}`` dict for histogram rendering).
    """
    pages = filter_pages(
        _read_csv_multi(run_dirs, config.PAGES_CSV), filters,
    )
    domain_dates: Dict[str, List[str]] = defaultdict(list)
    today = datetime.now().strftime("%Y-%m-%d")

    for r in pages:
        dom = r.get("domain", "unknown")
        for field in ("date_modified", "sitemap_lastmod", "date_published"):
            d = _parse_date(r.get(field, ""))
            if d:
                domain_dates[dom].append(d)

    result = []
    for dom, dates in sorted(domain_dates.items()):
        if not dates:
            continue
        dates_sorted = sorted(dates)
        latest = dates_sorted[-1]
        oldest = dates_sorted[0]

        buckets: Counter[str] = Counter()
        for d in dates_sorted:
            ym = d[:7]
            buckets[ym] += 1

        result.append({
            "domain": dom,
            "latest": latest,
            "oldest": oldest,
            "total_dates": len(dates_sorted),
            "buckets": dict(sorted(buckets.items())),
        })

    result.sort(key=lambda x: x.get("latest", ""), reverse=True)
    return {"today": today, "domains": result}


# -- Chord Matrix ---------------------------------------------------------

def aggregate_chord(
    run_dirs: List[str], top_n: int = 30,
    filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a square adjacency matrix for the D3 chord diagram.

    Only the top *top_n* domains by page count are included so the chord
    diagram remains readable.  Self-links are excluded.

    Args:
        run_dirs: Absolute paths to one or more crawl run directories.
        top_n: Number of highest-traffic domains to include (default 30).
        filters: Optional cross-cutting filter dict (see ``filter_pages``).

    Returns:
        ``{"domains": [str, ...], "matrix": [[int, ...], ...]}`` where
        ``matrix[i][j]`` is the number of edges from ``domains[i]`` to
        ``domains[j]``.
    """
    pages = filter_pages(
        _read_csv_multi(run_dirs, config.PAGES_CSV), filters,
    )
    domain_pages: Counter[str] = Counter()
    for r in pages:
        domain_pages[r.get("domain", "unknown")] += 1

    top_domains = [d for d, _ in domain_pages.most_common(top_n)]
    dom_set = set(top_domains)
    dom_idx = {d: i for i, d in enumerate(top_domains)}
    n = len(top_domains)
    matrix = [[0] * n for _ in range(n)]

    for rd in run_dirs:
        edges_path = os.path.join(rd, config.EDGES_CSV)
        if not os.path.isfile(edges_path):
            continue
        try:
            with open(edges_path, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    src = _extract_domain(row.get("from_url", ""))
                    tgt = _extract_domain(row.get("to_url", ""))
                    if src in dom_set and tgt in dom_set and src != tgt:
                        matrix[dom_idx[src]][dom_idx[tgt]] += 1
        except Exception:
            pass

    return {"domains": top_domains, "matrix": matrix}


# -- CMS / Schema Aggregation ---------------------------------------------

def aggregate_technology(
    run_dirs: List[str], filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Aggregate CMS, structured data, schema types, and SEO signals.

    Reads ``cms_generator``, ``json_ld_types``, ``microdata_types``,
    ``rdfa_types``, ``canonical_url``, ``hreflang_links``, ``feed_urls``,
    ``pagination_next/prev``, ``breadcrumb_schema``, ``robots_directives``,
    and ``extraction_coverage_pct`` columns from pages.csv.

    Args:
        run_dirs: Absolute paths to one or more crawl run directories.
        filters: Optional cross-cutting filter dict (see ``filter_pages``).

    Returns:
        Dict with five keys consumed by separate D3 panels:
        - ``cms_distribution``: per-CMS page/domain counts.
        - ``structured_data_adoption``: totals for JSON-LD / Microdata /
          RDFa presence.
        - ``schema_type_frequency``: top 40 schema types with counts.
        - ``seo_readiness``: per-domain counts of canonical, hreflang,
          feed, pagination, breadcrumb, robots, and structured data.
        - ``coverage_histogram``: 10-percentage-point buckets of
          extraction_coverage_pct for a bar chart.
    """
    pages = filter_pages(
        _read_csv_multi(run_dirs, config.PAGES_CSV), filters,
    )
    if not pages:
        return {
            "cms_distribution": [], "structured_data_adoption": {},
            "schema_type_frequency": [], "seo_readiness": [],
            "coverage_histogram": [],
        }

    # CMS distribution
    cms_domains: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    cms_pages: Counter = Counter()
    for r in pages:
        cms = r.get("cms_generator", "").strip() or "(undetected)"
        dom = r.get("domain", "unknown")
        cms_domains[cms][dom] += 1
        cms_pages[cms] += 1

    cms_distribution = []
    for cms, doms in sorted(cms_domains.items(), key=lambda x: -cms_pages[x[0]]):
        cms_distribution.append({
            "cms": cms,
            "page_count": cms_pages[cms],
            "domain_count": len(doms),
            "domains": [
                {"domain": d, "pages": c}
                for d, c in sorted(doms.items(), key=lambda x: -x[1])[:20]
            ],
        })

    # Structured data adoption
    total = len(pages)
    has_jld = sum(1 for r in pages if r.get("json_ld_types", "").strip())
    has_md = sum(1 for r in pages if r.get("microdata_types", "").strip())
    has_rdfa = sum(1 for r in pages if r.get("rdfa_types", "").strip())
    has_any = sum(
        1 for r in pages
        if (r.get("json_ld_types", "").strip() or
            r.get("microdata_types", "").strip() or
            r.get("rdfa_types", "").strip())
    )

    structured_data_adoption = {
        "total_pages": total,
        "json_ld": has_jld,
        "microdata": has_md,
        "rdfa": has_rdfa,
        "any": has_any,
        "none": total - has_any,
    }

    # Schema type frequency
    type_freq: Counter = Counter()
    for r in pages:
        for field in ("json_ld_types", "microdata_types"):
            for t in r.get(field, "").split("|"):
                t = t.strip()
                if t:
                    type_freq[t] += 1

    schema_type_frequency = [
        {"type": t, "count": c} for t, c in type_freq.most_common(40)
    ]

    # SEO readiness per domain
    dom_seo: Dict[str, Dict[str, int]] = {}
    for r in pages:
        dom = r.get("domain", "unknown")
        if dom not in dom_seo:
            dom_seo[dom] = {
                "domain": dom, "pages": 0,
                "has_canonical": 0, "has_hreflang": 0, "has_feed": 0,
                "has_pagination": 0, "has_breadcrumb_schema": 0,
                "has_robots": 0, "has_structured_data": 0,
            }
        s = dom_seo[dom]
        s["pages"] += 1
        if r.get("canonical_url", "").strip():
            s["has_canonical"] += 1
        if r.get("hreflang_links", "").strip():
            s["has_hreflang"] += 1
        if r.get("feed_urls", "").strip():
            s["has_feed"] += 1
        if r.get("pagination_next", "").strip() or r.get("pagination_prev", "").strip():
            s["has_pagination"] += 1
        if r.get("breadcrumb_schema", "").strip():
            s["has_breadcrumb_schema"] += 1
        if r.get("robots_directives", "").strip():
            s["has_robots"] += 1
        if (r.get("json_ld_types", "").strip() or
                r.get("microdata_types", "").strip() or
                r.get("rdfa_types", "").strip()):
            s["has_structured_data"] += 1

    seo_readiness = sorted(dom_seo.values(), key=lambda x: -x["pages"])

    # Coverage histogram — clamp to 90 so 100% pages fall into the 90-99 bucket
    # rather than creating a lone 100-109 outlier bucket.
    cov_buckets: Counter = Counter()
    for r in pages:
        cov = _safe_float(r.get("extraction_coverage_pct", "0"))
        bucket = min(int(cov // 10) * 10, 90)
        cov_buckets[bucket] += 1
    coverage_histogram = [
        {"bucket": f"{b}-{b + 9}%", "count": cov_buckets.get(b, 0)}
        for b in range(0, 100, 10)
    ]

    return {
        "cms_distribution": cms_distribution,
        "structured_data_adoption": structured_data_adoption,
        "schema_type_frequency": schema_type_frequency,
        "seo_readiness": seo_readiness,
        "coverage_histogram": coverage_histogram,
    }


# -- Authorship & Provenance -----------------------------------------------

def aggregate_authorship(
    run_dirs: List[str], filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Map author-to-domain and publisher-to-domain relationships.

    Reads ``author``, ``publisher``, and ``domain`` columns from
    pages.csv.  Produces both flat ranked lists (for tables) and a
    bipartite node/link structure (for D3 force-directed layout).

    Args:
        run_dirs: Absolute paths to one or more crawl run directories.
        filters: Optional cross-cutting filter dict (see ``filter_pages``).

    Returns:
        Dict with keys:
        - ``authors``: top 50 authors with per-domain page counts.
        - ``publishers``: top 30 publishers with per-domain page counts.
        - ``author_network``: ``{nodes, links}`` bipartite graph of
          the top 30 authors and the domains they publish on.
    """
    pages = filter_pages(
        _read_csv_multi(run_dirs, config.PAGES_CSV), filters,
    )
    if not pages:
        return {"authors": [], "publishers": [], "author_domains": []}

    author_doms: Dict[str, Counter] = defaultdict(Counter)
    publisher_doms: Dict[str, Counter] = defaultdict(Counter)

    for r in pages:
        author = r.get("author", "").strip()
        pub = r.get("publisher", "").strip()
        dom = r.get("domain", "unknown")
        if author:
            author_doms[author][dom] += 1
        if pub:
            publisher_doms[pub][dom] += 1

    authors = []
    for name, doms in sorted(
        author_doms.items(),
        key=lambda x: -sum(x[1].values()),
    )[:50]:
        authors.append({
            "author": name,
            "total_pages": sum(doms.values()),
            "domain_count": len(doms),
            "domains": [
                {"domain": d, "pages": c}
                for d, c in doms.most_common(10)
            ],
        })

    publishers = []
    for name, doms in sorted(
        publisher_doms.items(),
        key=lambda x: -sum(x[1].values()),
    )[:30]:
        publishers.append({
            "publisher": name,
            "total_pages": sum(doms.values()),
            "domain_count": len(doms),
            "domains": [
                {"domain": d, "pages": c}
                for d, c in doms.most_common(10)
            ],
        })

    # Author-domain network (nodes + links for force layout)
    author_nodes = []
    author_links = []
    top_authors = authors[:30]
    all_doms_in_network: set = set()
    for a in top_authors:
        for d in a["domains"]:
            all_doms_in_network.add(d["domain"])

    for a in top_authors:
        author_nodes.append({
            "id": "author:" + a["author"],
            "label": a["author"],
            "type": "author",
            "pages": a["total_pages"],
        })
    for dom in sorted(all_doms_in_network):
        author_nodes.append({
            "id": "domain:" + dom,
            "label": dom,
            "type": "domain",
            "pages": sum(
                d["pages"] for a in top_authors
                for d in a["domains"] if d["domain"] == dom
            ),
        })
    for a in top_authors:
        for d in a["domains"]:
            author_links.append({
                "source": "author:" + a["author"],
                "target": "domain:" + d["domain"],
                "weight": d["pages"],
            })

    return {
        "authors": authors,
        "publishers": publishers,
        "author_network": {"nodes": author_nodes, "links": author_links},
    }


# -- Schema Insights (vertical-specific) -----------------------------------

def aggregate_schema_insights(
    run_dirs: List[str], filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Extract vertical-specific structured data: Product, Event, Job, Recipe.

    Each vertical is gated by the presence of a sentinel column in
    pages.csv (e.g. ``schema_price`` for Product).  Summaries are only
    included when at least 3 items exist to avoid sparse noise.

    Args:
        run_dirs: Absolute paths to one or more crawl run directories.
        filters: Optional cross-cutting filter dict (see ``filter_pages``).

    Returns:
        Dict with keys ``products``, ``events``, ``jobs``, ``recipes``
        — each is either a summary dict or None.  Summary dicts contain
        per-domain breakdowns and top items (e.g. top-rated products,
        soonest events) consumed by D3 scatter/table panels.
    """
    pages = filter_pages(
        _read_csv_multi(run_dirs, config.PAGES_CSV), filters,
    )

    products = []
    events = []
    jobs = []
    recipes = []

    for r in pages:
        price = r.get("schema_price", "").strip()
        if price:
            products.append({
                "domain": r.get("domain", ""),
                "title": r.get("title", "")[:80],
                "price": _safe_float(price),
                "currency": r.get("schema_currency", ""),
                "availability": r.get("schema_availability", ""),
                "rating": _safe_float(r.get("schema_rating", "")),
                "review_count": _safe_int(r.get("schema_review_count", "")),
                "url": r.get("final_url", ""),
            })

        event_date = r.get("schema_event_date", "").strip()
        if event_date:
            events.append({
                "domain": r.get("domain", ""),
                "title": r.get("title", "")[:80],
                "date": event_date,
                "location": r.get("schema_event_location", ""),
                "url": r.get("final_url", ""),
            })

        job_title = r.get("schema_job_title", "").strip()
        if job_title:
            jobs.append({
                "domain": r.get("domain", ""),
                "job_title": job_title,
                "location": r.get("schema_job_location", ""),
                "url": r.get("final_url", ""),
            })

        recipe_time = r.get("schema_recipe_time", "").strip()
        if recipe_time:
            recipes.append({
                "domain": r.get("domain", ""),
                "title": r.get("title", "")[:80],
                "time": recipe_time,
                "url": r.get("final_url", ""),
            })

    # Product summary
    product_summary = None
    if len(products) >= 3:
        prices = [p["price"] for p in products if p["price"] > 0]
        avail = Counter(p["availability"] for p in products if p["availability"])
        dom_counts = Counter(p["domain"] for p in products)
        product_summary = {
            "count": len(products),
            "price_min": round(min(prices), 2) if prices else 0,
            "price_max": round(max(prices), 2) if prices else 0,
            "price_avg": round(sum(prices) / len(prices), 2) if prices else 0,
            "availability": dict(avail.most_common(10)),
            "by_domain": [
                {"domain": d, "count": c} for d, c in dom_counts.most_common(20)
            ],
            "top_rated": sorted(
                [p for p in products if p["rating"] > 0],
                key=lambda x: -x["rating"],
            )[:10],
        }

    event_summary = None
    if len(events) >= 3:
        dom_counts = Counter(e["domain"] for e in events)
        event_summary = {
            "count": len(events),
            "by_domain": [
                {"domain": d, "count": c} for d, c in dom_counts.most_common(20)
            ],
            "events": sorted(events, key=lambda x: x["date"])[:50],
        }

    job_summary = None
    if len(jobs) >= 3:
        dom_counts = Counter(j["domain"] for j in jobs)
        loc_counts = Counter(j["location"] for j in jobs if j["location"])
        job_summary = {
            "count": len(jobs),
            "by_domain": [
                {"domain": d, "count": c} for d, c in dom_counts.most_common(20)
            ],
            "by_location": [
                {"location": loc, "count": c} for loc, c in loc_counts.most_common(20)
            ],
            "jobs": jobs[:50],
        }

    recipe_summary = None
    if len(recipes) >= 3:
        dom_counts = Counter(rec["domain"] for rec in recipes)
        recipe_summary = {
            "count": len(recipes),
            "by_domain": [
                {"domain": d, "count": c} for d, c in dom_counts.most_common(20)
            ],
            "recipes": recipes[:50],
        }

    return {
        "products": product_summary,
        "events": event_summary,
        "jobs": job_summary,
        "recipes": recipe_summary,
    }


# -- Page Depth Analysis ---------------------------------------------------

def aggregate_page_depth(
    run_dirs: List[str], filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Per-depth aggregates: page counts, avg word count, avg coverage.

    Produces data for a depth histogram and a depth-vs-richness scatter
    so the dashboard can visualise how content quality varies with crawl
    depth.

    Args:
        run_dirs: Absolute paths to one or more crawl run directories.
        filters: Optional cross-cutting filter dict (see ``filter_pages``).

    Returns:
        Dict with keys:
        - ``depth_histogram``: list of ``{depth, count}`` for each depth level.
        - ``depth_quality``: list of ``{depth, avg_words, avg_coverage,
          avg_coverage_core, avg_readability, page_count}`` for scatter / line overlay.
        - ``domain_depth``: top 30 domains with ``{domain, depths}`` where
          ``depths`` is a list of ``{depth, count}``.
    """
    pages = filter_pages(
        _read_csv_multi(run_dirs, config.PAGES_CSV), filters,
    )
    if not pages:
        return {"depth_histogram": [], "depth_quality": [], "domain_depth": []}

    depth_counts: Counter = Counter()
    depth_words: Dict[int, List[int]] = defaultdict(list)
    depth_coverage: Dict[int, List[float]] = defaultdict(list)
    depth_coverage_core: Dict[int, List[float]] = defaultdict(list)
    depth_readability: Dict[int, List[float]] = defaultdict(list)
    domain_depth: Dict[str, Counter] = defaultdict(Counter)

    for r in pages:
        d = _safe_int(r.get("depth", "0"))
        depth_counts[d] += 1
        depth_words[d].append(_safe_int(r.get("word_count", "0")))
        cov = _safe_float(r.get("extraction_coverage_pct", "0"))
        if cov > 0:
            depth_coverage[d].append(cov)
        cov_c = _safe_float(r.get("extraction_coverage_core_pct", "0"))
        if cov_c > 0:
            depth_coverage_core[d].append(cov_c)
        rk = _safe_float(r.get("readability_fk_grade", ""))
        if rk > 0:
            depth_readability[d].append(rk)
        dom = r.get("domain", "unknown")
        domain_depth[dom][d] += 1

    max_depth = max(depth_counts.keys()) if depth_counts else 0

    depth_histogram = [
        {"depth": d, "count": depth_counts.get(d, 0)}
        for d in range(0, max_depth + 1)
    ]

    depth_quality = []
    for d in range(0, max_depth + 1):
        words = depth_words.get(d, [])
        covs = depth_coverage.get(d, [])
        covs_c = depth_coverage_core.get(d, [])
        reads = depth_readability.get(d, [])
        depth_quality.append({
            "depth": d,
            "avg_words": round(sum(words) / len(words)) if words else 0,
            "avg_coverage": round(sum(covs) / len(covs), 1) if covs else 0,
            "avg_coverage_core": round(sum(covs_c) / len(covs_c), 1) if covs_c else 0,
            "avg_readability": round(sum(reads) / len(reads), 1) if reads else 0,
            "page_count": depth_counts.get(d, 0),
        })

    dom_totals = {dom: sum(c.values()) for dom, c in domain_depth.items()}
    top_doms = sorted(dom_totals, key=lambda x: -dom_totals[x])[:30]
    domain_depth_list = []
    for dom in top_doms:
        domain_depth_list.append({
            "domain": dom,
            "depths": [
                {"depth": d, "count": domain_depth[dom].get(d, 0)}
                for d in range(0, max_depth + 1)
            ],
        })

    return {
        "depth_histogram": depth_histogram,
        "depth_quality": depth_quality,
        "domain_depth": domain_depth_list,
    }


# -- Content Health Matrix -------------------------------------------------

def aggregate_content_health(
    run_dirs: List[str], filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a domain × signal health matrix for the heatmap.

    Each cell value is a percentage (0–100) indicating what proportion
    of a domain's pages have the given quality signal present.  Signals
    span SEO, accessibility, structured data, and content completeness.

    Args:
        run_dirs: Absolute paths to one or more crawl run directories.
        filters: Optional cross-cutting filter dict (see ``filter_pages``).

    Returns:
        Dict with keys:
        - ``domains``: list of domain strings (sorted by page count).
        - ``signals``: list of signal name strings.
        - ``matrix``: 2-D list ``matrix[domain_idx][signal_idx]`` of
          percentage floats.
        - ``page_counts``: list of page counts parallel to ``domains``.
    """
    pages = filter_pages(
        _read_csv_multi(run_dirs, config.PAGES_CSV), filters,
    )
    if not pages:
        return {"domains": [], "signals": [], "matrix": [], "page_counts": []}

    signals = [
        ("Title", lambda r: bool(r.get("title", "").strip())),
        ("Meta Desc", lambda r: bool(r.get("meta_description", "").strip())),
        ("H1", lambda r: bool(r.get("h1_joined", "").strip())),
        ("Canonical", lambda r: bool(r.get("canonical_url", "").strip())),
        ("Open Graph", lambda r: bool(r.get("og_title", "").strip())),
        ("JSON-LD", lambda r: bool(r.get("json_ld_types", "").strip())),
        ("Alt Text", lambda r: (
            _safe_int(r.get("img_count", "0")) == 0 or
            _safe_int(r.get("img_missing_alt_count", "0")) == 0
        )),
        ("Lang Attr", lambda r: r.get("wcag_lang_valid", "") == "1"),
        ("Headings OK", lambda r: r.get("wcag_heading_order_valid", "") == "1"),
        ("Landmarks", lambda r: r.get("wcag_landmarks_present", "") == "1"),
        ("Privacy Policy", lambda r: bool(r.get("privacy_policy_url", "").strip())),
        ("Dates", lambda r: bool(
            r.get("date_published", "").strip() or
            r.get("date_modified", "").strip()
        )),
        ("Author", lambda r: bool(r.get("author", "").strip())),
        ("Breadcrumbs", lambda r: bool(r.get("breadcrumb_schema", "").strip())),
    ]

    signal_names = [s[0] for s in signals]
    signal_fns = [s[1] for s in signals]

    dom_totals: Counter = Counter()
    dom_signal_hits: Dict[str, List[int]] = {}

    for r in pages:
        dom = r.get("domain", "unknown")
        dom_totals[dom] += 1
        if dom not in dom_signal_hits:
            dom_signal_hits[dom] = [0] * len(signal_fns)
        for i, fn in enumerate(signal_fns):
            if fn(r):
                dom_signal_hits[dom][i] += 1

    top_domains = [d for d, _ in dom_totals.most_common(40)]

    matrix = []
    page_counts = []
    for dom in top_domains:
        pc = dom_totals[dom]
        page_counts.append(pc)
        row = []
        for i in range(len(signal_fns)):
            pct = round(dom_signal_hits[dom][i] / pc * 100, 1) if pc else 0
            row.append(pct)
        matrix.append(row)

    return {
        "domains": top_domains,
        "signals": signal_names,
        "matrix": matrix,
        "page_counts": page_counts,
    }


# -- Content & On-Site Performance Audit -----------------------------------

_THIN_WORD_THRESHOLD = 250

# Default max rows returned in JSON when ``full_lists`` is False (dashboard performance).
_REPORT_DEFAULT_CAP = 80


def _all_page_urls(rows: List[Dict[str, str]]) -> set:
    """All ``final_url`` / ``requested_url`` values from *rows*."""
    urls: set = set()
    for r in rows:
        for key in ("final_url", "requested_url"):
            u = (r.get(key) or "").strip()
            if u:
                urls.add(u)
    return urls


def _normalise_for_keyword_match(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").lower()).strip()


def _tags_all_tokens(tags_all: str) -> List[str]:
    """Split ``tags_all`` into non-empty tokens (pipe-delimited in CSV)."""
    out: List[str] = []
    for part in (tags_all or "").split("|"):
        t = part.strip()
        if len(t) >= 2:
            out.append(t)
    return out


def _keyword_alignment(
    tags_all: str,
    title: str,
    h1: str,
) -> Tuple[bool, List[str]]:
    """Return (aligned, tokens_checked) — aligned if any token appears in title or H1."""
    tokens = _tags_all_tokens(tags_all)
    if not tokens:
        return True, []
    title_n = _normalise_for_keyword_match(title)
    h1_n = _normalise_for_keyword_match(h1)
    blob = title_n + " " + h1_n
    matched = False
    for tok in tokens:
        tl = tok.lower()
        if len(tl) < 2:
            continue
        if tl in blob:
            matched = True
            break
    return matched, tokens


def aggregate_content_performance_audit(
    run_dirs: List[str],
    filters: Optional[Dict[str, Any]] = None,
    full_lists: bool = False,
) -> Dict[str, Any]:
    """Thin/duplicate content, internal links, and keyword–copy alignment.

    Uses ``pages.csv`` and ``edges.csv`` only. Duplicate detection uses
    ``content_hash`` clusters and shared ``canonical_url`` targets. Internal
    links are same-host edges present in the crawl. Keyword mapping uses
    ``tags_all`` (parser-derived tags/keywords) against ``title`` and
    ``h1_joined`` — a heuristic, not a substitute for Search Console data.

    Args:
        run_dirs: Absolute paths to one or more crawl run directories.
        filters: Optional cross-cutting filter dict (see ``filter_pages``).
        full_lists: When True, return complete row lists (no sampling caps).

    Returns:
        Nested dict with ``summary``, ``thin_content``, ``duplicates``,
        ``internal_links``, ``keyword_mapping``, and ``disclaimer``.
    """
    cap = None if full_lists else _REPORT_DEFAULT_CAP
    pages = filter_pages(
        _read_csv_multi(run_dirs, config.PAGES_CSV), filters,
    )
    page_count = len(pages)
    page_urls = _all_page_urls(pages)
    domains_in_scope = {r.get("domain", "unknown") for r in pages}

    # --- Thin content ---
    thin_rows: List[Dict[str, Any]] = []
    for r in pages:
        wc = _safe_int(r.get("word_count", "0"))
        if wc <= _THIN_WORD_THRESHOLD:
            thin_rows.append({
                "url": (r.get("final_url") or r.get("requested_url") or "").strip(),
                "title": (r.get("title") or "")[:120],
                "domain": r.get("domain", "unknown"),
                "word_count": wc,
                "content_kind_guess": (r.get("content_kind_guess") or "").strip(),
            })
    thin_rows.sort(key=lambda x: (x["word_count"], x["url"]))
    thin_sample = thin_rows if cap is None else thin_rows[:cap]

    # --- Duplicate clusters (content hash) ---
    hash_to_urls: Dict[str, List[str]] = defaultdict(list)
    for r in pages:
        h = (r.get("content_hash") or "").strip()
        if not h:
            continue
        u = (r.get("final_url") or r.get("requested_url") or "").strip()
        if u:
            hash_to_urls[h].append(u)
    duplicate_hash_clusters = []
    for h, urls in hash_to_urls.items():
        if len(urls) <= 1:
            continue
        sorted_urls = sorted(set(urls))
        duplicate_hash_clusters.append({
            "content_hash": h,
            "urls": sorted_urls,
            "count": len(sorted_urls),
        })
    duplicate_hash_clusters.sort(key=lambda x: -x["count"])

    # --- Canonical URL duplicates (multiple pages sharing one canonical) ---
    canon_to_pages: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for r in pages:
        canon = (r.get("canonical_url") or "").strip()
        if not canon:
            continue
        u = (r.get("final_url") or r.get("requested_url") or "").strip()
        if not u:
            continue
        if u.rstrip("/") == canon.rstrip("/"):
            continue
        canon_to_pages[canon].append({
            "url": u,
            "title": (r.get("title") or "")[:120],
        })
    canonical_duplicates = [
        {"canonical_url": c, "pages": plist, "count": len(plist)}
        for c, plist in canon_to_pages.items()
        if len(plist) > 1
    ]
    canonical_duplicates.sort(key=lambda x: -x["count"])

    # --- Internal link graph (same host in edges.csv) ---
    inlink: Counter[str] = Counter()
    out_internal: Counter[str] = Counter()
    internal_edge_total = 0
    domain_internal_edges: Counter[str] = Counter()

    for rd in run_dirs:
        edges_path = os.path.join(rd, config.EDGES_CSV)
        if not os.path.isfile(edges_path):
            continue
        try:
            with open(edges_path, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    from_u = (row.get("from_url") or "").strip()
                    to_u = (row.get("to_url") or "").strip()
                    if not from_u or not to_u:
                        continue
                    if from_u not in page_urls:
                        continue
                    src_dom = _extract_domain(from_u)
                    tgt_dom = _extract_domain(to_u)
                    if not src_dom or src_dom != tgt_dom:
                        continue
                    if tgt_dom not in domains_in_scope:
                        continue
                    internal_edge_total += 1
                    domain_internal_edges[src_dom] += 1
                    out_internal[from_u] += 1
                    if to_u in page_urls:
                        inlink[to_u] += 1
        except Exception:
            pass

    url_to_row = {}
    for r in pages:
        fu = (r.get("final_url") or "").strip()
        ru = (r.get("requested_url") or "").strip()
        if fu:
            url_to_row[fu] = r
        if ru:
            url_to_row[ru] = r

    top_inlinked = []
    in_items = inlink.most_common() if cap is None else inlink.most_common(cap)
    for url, cnt in in_items:
        rr = url_to_row.get(url, {})
        top_inlinked.append({
            "url": url,
            "domain": rr.get("domain", _extract_domain(url) or "unknown"),
            "title": (rr.get("title") or "")[:120],
            "inlinks_internal": cnt,
        })

    out_items = out_internal.most_common() if cap is None else out_internal.most_common(cap)
    top_outlinking = [{"url": u, "outlinks_internal": c} for u, c in out_items]

    dom_list = sorted(domains_in_scope)
    by_domain_internal = []
    for dom in dom_list:
        pc = sum(1 for r in pages if r.get("domain") == dom)
        ie = domain_internal_edges.get(dom, 0)
        avg_out = round(ie / pc, 2) if pc else 0.0
        by_domain_internal.append({
            "domain": dom,
            "page_count": pc,
            "internal_edges": ie,
            "avg_internal_edges_per_page": avg_out,
        })
    by_domain_internal.sort(key=lambda x: -x["internal_edges"])
    by_dom_slice = (
        by_domain_internal if cap is None else by_domain_internal[:cap]
    )

    internal_links = {
        "total_internal_edges": internal_edge_total,
        "top_inlinked_pages": top_inlinked,
        "top_outlinking_pages": top_outlinking,
        "by_domain": by_dom_slice,
    }

    # --- Keyword mapping (tags_all vs title / H1) ---
    mapping_rows: List[Dict[str, Any]] = []
    gap_rows: List[Dict[str, Any]] = []
    for r in pages:
        tags_all = r.get("tags_all", "")
        tokens = _tags_all_tokens(tags_all)
        if not tokens:
            continue
        title = r.get("title", "")
        h1 = r.get("h1_joined", "")
        aligned, _ = _keyword_alignment(tags_all, title, h1)
        row_out = {
            "url": (r.get("final_url") or r.get("requested_url") or "").strip(),
            "domain": r.get("domain", "unknown"),
            "title": (title or "")[:120],
            "h1": (h1 or "")[:160],
            "tags_all": tags_all[:300],
            "aligned": aligned,
        }
        mapping_rows.append(row_out)
        if not aligned:
            gap_rows.append(row_out)

    aligned_rows = [m for m in mapping_rows if m["aligned"]]
    if cap is None:
        gap_sample = gap_rows
        aligned_sample = aligned_rows
    else:
        gap_sample = gap_rows[: min(60, cap)]
        aligned_sample = aligned_rows[: min(30, cap)]

    keyword_mapping = {
        "pages_with_tags": len(mapping_rows),
        "aligned_count": sum(1 for m in mapping_rows if m["aligned"]),
        "gap_sample": gap_sample,
        "aligned_sample": aligned_sample,
    }

    summary = {
        "page_count": page_count,
        "thin_word_threshold": _THIN_WORD_THRESHOLD,
        "thin_count": len(thin_rows),
        "thin_pct": (
            round(len(thin_rows) / page_count * 100, 1) if page_count else 0.0
        ),
        "duplicate_hash_cluster_count": len(duplicate_hash_clusters),
        "canonical_duplicate_group_count": len(canonical_duplicates),
        "internal_edge_count": internal_edge_total,
        "pages_with_tags_all": keyword_mapping["pages_with_tags"],
        "keyword_aligned_count": keyword_mapping["aligned_count"],
    }
    if keyword_mapping["pages_with_tags"]:
        summary["keyword_alignment_pct"] = round(
            keyword_mapping["aligned_count"] / keyword_mapping["pages_with_tags"] * 100, 1,
        )
    else:
        summary["keyword_alignment_pct"] = 0.0

    disclaimer = (
        "This audit uses crawl exports only. Thin pages use a fixed word-count "
        f"threshold ({_THIN_WORD_THRESHOLD} words). Duplicate clusters use "
        "``content_hash`` when populated, plus pages that share a canonical URL. "
        "Internal links are counted from ``edges.csv`` on the same host. "
        "Keyword mapping compares ``tags_all`` tokens to title and H1 text — "
        "configure richer tagging in the parser where needed."
    )

    dup_h = duplicate_hash_clusters if cap is None else duplicate_hash_clusters[:cap]
    dup_c = canonical_duplicates if cap is None else canonical_duplicates[:cap]

    return {
        "summary": summary,
        "thin_content": {
            "sample": thin_sample,
            "total_flagged": len(thin_rows),
        },
        "duplicates": {
            "by_content_hash": dup_h,
            "by_canonical_url": dup_c,
        },
        "internal_links": internal_links,
        "keyword_mapping": keyword_mapping,
        "disclaimer": disclaimer,
        "full_lists": bool(full_lists),
    }


# -- Technical Performance (per-domain) ------------------------------------

_SLOW_FETCH_MS = 3000.0
_LARGE_IMAGE_BYTES = 500_000


def _gather_asset_rows(run_dirs: List[str]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for rd in run_dirs:
        if not os.path.isdir(rd):
            continue
        for name in sorted(os.listdir(rd)):
            if name.startswith("assets_") and name.endswith(".csv"):
                rows.extend(_read_csv(os.path.join(rd, name)))
    return rows


def aggregate_technical_performance(
    run_dirs: List[str],
    filters: Optional[Dict[str, Any]] = None,
    full_lists: bool = False,
) -> Dict[str, Any]:
    """Per-domain technical UX signals: fetch time, viewport, asset inventory.

    ``fetch_time_ms`` is recorded at crawl time (full GET including retries).
    Asset rows come from ``assets_*.csv``; large images use ``head_content_length``
    when HEAD metadata was collected.

    Args:
        full_lists: When True, include all slow pages, viewport gaps, large images,
            and full external-script lists per domain (no row caps).

    Returns:
        ``domains`` — list of per-domain summary dicts, sorted by page count.
        ``disclaimer`` — crawl-scope limitations.
    """
    cap = None if full_lists else _REPORT_DEFAULT_CAP
    ext_cap = None if full_lists else 20
    large_cap = None if full_lists else 25
    pages = filter_pages(
        _read_csv_multi(run_dirs, config.PAGES_CSV), filters,
    )
    asset_rows_all = _gather_asset_rows(run_dirs)

    # Restrict assets to referring pages in filtered set
    allowed_ref = _all_page_urls(pages)
    asset_rows = [
        ar for ar in asset_rows_all
        if not pages or (ar.get("referrer_page_url", "").strip() in allowed_ref)
    ]

    by_domain: Dict[str, Dict[str, Any]] = {}

    def _ensure(dom: str) -> Dict[str, Any]:
        if dom not in by_domain:
            by_domain[dom] = {
                "domain": dom,
                "page_count": 0,
                "fetch_times_ms": [],
                "slow_pages": [],
                "no_viewport_pages": [],
                "viewport_ok_count": 0,
                "assets_by_category": Counter(),
                "external_scripts": Counter(),
                "large_images": [],
            }
        return by_domain[dom]

    # Initialise from pages
    for r in pages:
        dom = r.get("domain", "unknown")
        d = _ensure(dom)
        d["page_count"] += 1
        ft = _safe_float(r.get("fetch_time_ms", ""), -1.0)
        if ft >= 0:
            d["fetch_times_ms"].append(ft)
        if r.get("has_viewport_meta", "") == "1":
            d["viewport_ok_count"] += 1
        else:
            u = (r.get("final_url") or r.get("requested_url") or "").strip()
            if cap is None or len(d["no_viewport_pages"]) < cap:
                d["no_viewport_pages"].append({
                    "url": u,
                    "title": (r.get("title") or "")[:100],
                })
        if ft >= _SLOW_FETCH_MS and (cap is None or len(d["slow_pages"]) < cap):
            d["slow_pages"].append({
                "url": (r.get("final_url") or r.get("requested_url") or "").strip(),
                "title": (r.get("title") or "")[:100],
                "fetch_time_ms": round(ft, 0),
            })

    # Assets
    for ar in asset_rows:
        ref = (ar.get("referrer_page_url") or "").strip()
        ref_dom = _extract_domain(ref)
        if not ref_dom:
            continue
        if ref_dom not in by_domain:
            _ensure(ref_dom)
        d = by_domain[ref_dom]
        cat = (ar.get("category") or "other").strip() or "other"
        d["assets_by_category"][cat] += 1

        asset_u = (ar.get("asset_url") or "").strip()
        a_dom = _extract_domain(asset_u)
        if cat == "script" and a_dom and a_dom.lower() != ref_dom.lower():
            d["external_scripts"][asset_u] += 1

        if cat == "image":
            clen = _safe_float(ar.get("head_content_length", ""), 0.0)
            if clen >= _LARGE_IMAGE_BYTES and (
                large_cap is None or len(d["large_images"]) < large_cap
            ):
                d["large_images"].append({
                    "asset_url": asset_u[:300],
                    "bytes": int(clen),
                    "referrer": ref[:300],
                })

    domain_list: List[Dict[str, Any]] = []
    for dom, d in by_domain.items():
        times = sorted(d["fetch_times_ms"])
        n = len(times)
        avg_ft = round(sum(times) / n, 1) if n else 0.0
        p90 = round(times[int(0.9 * (n - 1))], 1) if n else 0.0
        pc = d["page_count"]
        vp_pct = round(d["viewport_ok_count"] / pc * 100, 1) if pc else 0.0
        if ext_cap is None:
            ext_scripts = [
                {"url": u, "count": c}
                for u, c in sorted(
                    d["external_scripts"].items(),
                    key=lambda x: (-x[1], x[0]),
                )
            ]
        else:
            ext_scripts = [
                {"url": u, "count": c}
                for u, c in d["external_scripts"].most_common(ext_cap)
            ]
        domain_list.append({
            "domain": dom,
            "page_count": pc,
            "avg_fetch_time_ms": avg_ft,
            "p90_fetch_time_ms": p90,
            "slow_page_count": len([t for t in times if t >= _SLOW_FETCH_MS]),
            "slow_pages_sample": d["slow_pages"],
            "viewport_meta_pct": vp_pct,
            "no_viewport_sample": d["no_viewport_pages"],
            "assets_by_category": dict(d["assets_by_category"].most_common()),
            "external_scripts_top": ext_scripts,
            "large_images_sample": d["large_images"],
        })

    domain_list.sort(key=lambda x: -x["page_count"])

    disclaimer = (
        "Technical performance uses crawl-time measurements only. "
        f"Fetch time is wall time for the HTTP GET (threshold {int(_SLOW_FETCH_MS)} ms for "
        "\"slow\"). Mobile-friendliness is proxied by a viewport meta tag — "
        "not device rendering. Asset sizes use HEAD metadata when enabled in config; "
        "otherwise only counts are available."
    )

    return {
        "slow_fetch_threshold_ms": int(_SLOW_FETCH_MS),
        "large_image_bytes_threshold": _LARGE_IMAGE_BYTES,
        "domains": domain_list,
        "disclaimer": disclaimer,
        "full_lists": bool(full_lists),
    }


# -- Key metrics snapshot (crawl proxies for traffic / engagement / conversion) ---

_SOCIAL_HOST_FRAGMENTS = (
    "facebook.", "fb.com", "twitter.", "t.co", "linkedin.", "instagram.",
    "youtube.", "youtu.be", "tiktok.", "pinterest.", "reddit.", "snapchat.",
)
_SEARCH_HOST_FRAGMENTS = (
    "google.", "bing.com", "duckduckgo.", "yahoo.", "baidu.", "yandex.",
    "ecosia.", "startpage.",
)


def _classify_discovery_referrer(
    referrer_url: str,
    page_domain: str,
) -> str:
    """Bucket how a page entered the crawl (not real visitor traffic)."""
    ref = (referrer_url or "").strip()
    if not ref:
        return "unknown"
    low = ref.lower()
    if low == "seed":
        return "direct_seed"
    if low.startswith("sitemap:"):
        return "sitemap"

    dom = (page_domain or "").lower().strip()
    try:
        host = (urlparse(ref).hostname or "").lower()
    except Exception:
        host = ""
    if not host:
        return "other"

    if host == dom or host.endswith("." + dom):
        return "internal_discovery"

    h = host + "."
    for frag in _SOCIAL_HOST_FRAGMENTS:
        if frag in h or host == frag.rstrip("."):
            return "social_referrer"
    for frag in _SEARCH_HOST_FRAGMENTS:
        if frag in h:
            return "search_referrer"
    return "external_other"


def aggregate_key_metrics_snapshot(
    run_dirs: List[str],
    filters: Optional[Dict[str, Any]] = None,
    full_lists: bool = False,
) -> Dict[str, Any]:
    """Per-domain proxies for traffic, engagement, and conversion-style signals.

    Real analytics (sessions, time on page, scroll, purchases) are **not** in
    crawl exports. This aggregates **crawl-time** heuristics: how pages were
    discovered, content size and link structure, and schema.org commercial hints.

    Args:
        full_lists: When True, include ``page_breakdown`` with one row per page.

    Returns:
        ``domains`` — list of per-domain dicts; ``disclaimer`` explains limits.
    """
    pages = filter_pages(
        _read_csv_multi(run_dirs, config.PAGES_CSV), filters,
    )

    by_domain: Dict[str, Dict[str, Any]] = {}
    page_breakdown: List[Dict[str, Any]] = []

    def _ensure(dom: str) -> Dict[str, Any]:
        if dom not in by_domain:
            by_domain[dom] = {
                "domain": dom,
                "page_count": 0,
                "discovery_counts": Counter(),
                "word_counts": [],
                "depths": [],
                "link_int": [],
                "link_ext": [],
                "product_pages": 0,
                "in_stock_pages": 0,
                "has_price_pages": 0,
                "form_label_pcts": [],
            }
        return by_domain[dom]

    for r in pages:
        dom = r.get("domain", "unknown")
        d = _ensure(dom)
        d["page_count"] += 1
        bucket = _classify_discovery_referrer(
            r.get("referrer_url", ""),
            dom,
        )
        d["discovery_counts"][bucket] += 1

        if full_lists:
            url = (r.get("final_url") or r.get("requested_url") or "").strip()
            page_breakdown.append({
                "domain": dom,
                "url": url,
                "title": (r.get("title") or "")[:200],
                "discovery_bucket": bucket,
                "referrer_url": (r.get("referrer_url") or "")[:500],
                "word_count": _safe_int(r.get("word_count", "0")),
                "depth": _safe_int(r.get("depth", "0")),
                "link_count_internal": _safe_int(r.get("link_count_internal", "0")),
                "link_count_external": _safe_int(r.get("link_count_external", "0")),
                "has_schema_price": bool((r.get("schema_price") or "").strip()),
                "schema_availability": (r.get("schema_availability") or "")[:120],
            })

        wc = _safe_int(r.get("word_count", "0"))
        if wc > 0:
            d["word_counts"].append(wc)
        d["depths"].append(_safe_int(r.get("depth", "0")))
        d["link_int"].append(_safe_int(r.get("link_count_internal", "0")))
        d["link_ext"].append(_safe_int(r.get("link_count_external", "0")))

        price = (r.get("schema_price") or "").strip()
        if price:
            d["has_price_pages"] += 1
            d["product_pages"] += 1
            avail = (r.get("schema_availability") or "").lower()
            if "instock" in avail or "in stock" in avail:
                d["in_stock_pages"] += 1

        fl = _safe_float(r.get("wcag_form_labels_pct", ""), -1.0)
        if fl >= 0:
            d["form_label_pcts"].append(fl)

    out: List[Dict[str, Any]] = []
    for dom, d in by_domain.items():
        pc = d["page_count"]
        dc = d["discovery_counts"]
        disc_pct = {
            k: round(dc[k] / pc * 100, 1) if pc else 0.0
            for k in sorted(dc.keys())
        }
        wcs = d["word_counts"]
        depths = d["depths"]
        li = d["link_int"]
        le = d["link_ext"]
        fls = d["form_label_pcts"]

        out.append({
            "domain": dom,
            "page_count": pc,
            "discovery_mix_pct": disc_pct,
            "discovery_mix_counts": dict(dc),
            "avg_word_count": round(sum(wcs) / len(wcs)) if wcs else 0,
            "avg_depth": round(sum(depths) / len(depths), 2) if depths else 0.0,
            "avg_internal_links": round(sum(li) / len(li), 1) if li else 0.0,
            "avg_external_links": round(sum(le) / len(le), 1) if le else 0.0,
            "product_pages": d["product_pages"],
            "priced_pages": d["has_price_pages"],
            "in_stock_pages": d["in_stock_pages"],
            "in_stock_pct": round(d["in_stock_pages"] / d["product_pages"] * 100, 1)
            if d["product_pages"] else 0.0,
            "avg_form_label_pct": round(sum(fls) / len(fls) * 100, 1)
            if fls else 0.0,
        })

    out.sort(key=lambda x: -x["page_count"])

    disclaimer = (
        "These figures are crawl snapshots, not Google Analytics or Search Console. "
        "Traffic mix means how each URL entered the crawl (seed, sitemap, "
        "following links from another site, etc.). "
        "Engagement uses page structure (words, depth, links) as a rough proxy; "
        "there is no time on page, scroll depth, or click data in the crawl. "
        "Conversion-style counts use schema.org product fields where present, "
        "not checkout or form submission events."
    )

    result: Dict[str, Any] = {
        "domains": out,
        "disclaimer": disclaimer,
        "full_lists": bool(full_lists),
    }
    if full_lists:
        result["page_breakdown"] = page_breakdown
    return result


# -- Indexability Report ---------------------------------------------------

def aggregate_indexability(
    run_dirs: List[str],
    filters: Optional[Dict[str, Any]] = None,
    full_lists: bool = False,
) -> Dict[str, Any]:
    """Summarise URLs that are not meant to be indexed.

    - **noindex**: pages in ``pages.csv`` whose ``robots_directives`` field
      contains ``noindex`` (from ``<meta name="robots">`` and/or
      ``X-Robots-Tag``).
    - **robots.txt**: rows in ``crawl_errors.csv`` with
      ``error_type == robots_disallowed`` (URLs never fetched).

    Domain filters apply to both lists; other ``filter_pages`` dimensions
    apply only to crawled page rows.

    Args:
        full_lists: When True, return all noindex and robots-block rows (no 500 cap).
    """
    list_cap = None if full_lists else 500
    pages = filter_pages(
        _read_csv_multi(run_dirs, config.PAGES_CSV), filters,
    )
    domain_allow: Optional[set] = None
    if filters and filters.get("domains"):
        domain_allow = {d.lower() for d in filters["domains"]}

    noindex_rows: List[Dict[str, Any]] = []
    noindex_meta_only = 0
    noindex_header_only = 0
    noindex_both = 0

    for r in pages:
        rd = r.get("robots_directives", "")
        if "noindex" not in rd.lower():
            continue
        from_meta, from_header = _robots_noindex_sources(rd)
        if from_meta and from_header:
            noindex_both += 1
        elif from_header:
            noindex_header_only += 1
        else:
            noindex_meta_only += 1
        noindex_rows.append({
            "url": _page_url_for_row(r),
            "requested_url": (r.get("requested_url") or "").strip(),
            "domain": r.get("domain", "unknown"),
            "http_status": r.get("http_status", ""),
            "robots_directives": rd,
            "source_meta": from_meta,
            "source_header": from_header,
        })

    errors_all = _read_csv_multi(run_dirs, config.ERRORS_CSV)
    robots_blocked: List[Dict[str, Any]] = []
    for e in errors_all:
        if e.get("error_type", "").strip() != "robots_disallowed":
            continue
        url = (e.get("url") or "").strip()
        dom = _extract_domain(url)
        if domain_allow is not None and dom.lower() not in domain_allow:
            continue
        robots_blocked.append({
            "url": url,
            "domain": dom or "unknown",
            "message": (e.get("message") or "").strip(),
            "discovered_at": (e.get("discovered_at") or "").strip(),
        })

    noindex_sorted = sorted(
        noindex_rows,
        key=lambda x: (x.get("domain") or "", x.get("url") or ""),
    )
    blocked_sorted = sorted(
        robots_blocked,
        key=lambda x: (x.get("domain") or "", x.get("url") or ""),
    )

    page_count = len(pages)
    ni = len(noindex_rows)
    rb = len(robots_blocked)
    combined = ni + rb
    noindex_out = noindex_sorted if list_cap is None else noindex_sorted[:list_cap]
    blocked_out = blocked_sorted if list_cap is None else blocked_sorted[:list_cap]
    return {
        "summary": {
            "page_count": page_count,
            "noindex_count": ni,
            "noindex_pct": round(ni / page_count * 100, 1) if page_count else 0.0,
            "noindex_meta_only": noindex_meta_only,
            "noindex_header_only": noindex_header_only,
            "noindex_both_sources": noindex_both,
            "robots_txt_blocked_count": rb,
            "non_indexable_total": combined,
            "non_indexable_pct": round(combined / page_count * 100, 1) if page_count else 0.0,
        },
        "noindex_pages": noindex_out,
        "noindex_pages_total": ni,
        "robots_txt_blocked": blocked_out,
        "robots_txt_blocked_total": rb,
        "full_lists": bool(full_lists),
    }


# -- Coverage Metrics / Filter Options -------------------------------------

def get_filter_options(run_dirs: List[str]) -> Dict[str, Any]:
    """Scan pages.csv for the distinct values available in each filter dimension.

    Called once when the dashboard loads to populate dropdowns / multi-selects
    in the global filter bar.  Does not apply any filters itself.

    Returns:
        Dict with ``domains``, ``cms_values``, ``content_kinds``,
        ``schema_types`` (each a sorted list of strings), and
        ``total_pages`` (int).
    """
    pages = _read_csv_multi(run_dirs, config.PAGES_CSV)

    domains: set = set()
    cms_values: set = set()
    content_kinds: set = set()
    schema_types: set = set()

    for r in pages:
        domains.add(r.get("domain", ""))
        cms = r.get("cms_generator", "").strip()
        if cms:
            cms_values.add(cms)
        kind = r.get("content_kind_guess", "").strip()
        if kind:
            content_kinds.add(kind)
        for field in ("json_ld_types", "microdata_types"):
            for t in r.get(field, "").split("|"):
                t = t.strip()
                if t:
                    schema_types.add(t)

    return {
        "domains": sorted(domains),
        "cms_values": sorted(cms_values),
        "content_kinds": sorted(content_kinds),
        "schema_types": sorted(schema_types),
        "total_pages": len(pages),
    }
