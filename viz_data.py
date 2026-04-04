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
        "wcag_form_labels_pct": round(d["wcag_form_labels_sum"] / d["wcag_form_labels_n"] * 100, 1) if d["wcag_form_labels_n"] else 100.0,
        "wcag_landmarks_pct": round(d["wcag_landmarks_present_count"] / pc * 100, 1) if pc else 0,
        "wcag_vague_link_pct": round(d["wcag_vague_link_sum"] / d["wcag_vague_link_n"] * 100, 1) if d["wcag_vague_link_n"] else 0.0,
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
        "avg_extraction_coverage": round(d["extraction_coverage_sum"] / d["extraction_coverage_n"], 1) if d["extraction_coverage_n"] else 0,
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
                        "children": sorted(leaves, key=lambda l: l["name"]),
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
