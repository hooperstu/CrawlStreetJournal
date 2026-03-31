"""
CSV writers for NHS Collector: pages inventory, assets by type, edges, tags, errors.
"""

import csv
import logging
import os
from typing import Any, Dict

import config

logger = logging.getLogger(__name__)

PAGES_FIELDS = (
    "requested_url",
    "final_url",
    "domain",
    "http_status",
    "content_type",
    "title",
    "meta_description",
    "lang",
    "canonical_url",
    "og_title",
    "og_type",
    "og_description",
    "twitter_card",
    "json_ld_types",
    "tags_all",
    "url_content_hint",
    "content_kind_guess",
    "h1_joined",
    "word_count",
    # Phase 1 — freshness / provenance
    "http_last_modified",
    "etag",
    "sitemap_lastmod",
    "referrer_sitemap_url",
    # Phase 2 — on-page quality / trust
    "heading_outline",
    "date_published",
    "date_modified",
    "visible_dates",
    "link_count_internal",
    "link_count_external",
    "link_count_total",
    "img_count",
    "img_missing_alt_count",
    "readability_fk_grade",
    "privacy_policy_url",
    "analytics_signals",
    "training_related_flag",
    # Phase 3 — nav snapshot
    "nav_link_count",
    # common
    "referrer_url",
    "depth",
    "discovered_at",
)

ASSET_FIELDS = (
    "referrer_page_url",
    "asset_url",
    "link_text",
    "category",
    "head_content_type",
    "head_content_length",
    "discovered_at",
)

EDGE_FIELDS = ("from_url", "to_url", "link_text", "discovered_at")

TAG_ROW_FIELDS = ("page_url", "tag_value", "tag_source", "discovered_at")

ERROR_FIELDS = ("url", "error_type", "message", "http_status", "discovered_at")

SITEMAP_URL_FIELDS = (
    "url",
    "lastmod",
    "source_sitemap",
    "discovered_at",
)

NAV_LINK_FIELDS = (
    "page_url",
    "nav_href",
    "nav_text",
    "discovered_at",
)

LINK_CHECK_FIELDS = (
    "from_url",
    "to_url",
    "check_status",
    "check_final_url",
    "discovered_at",
)


def _output_path(name: str) -> str:
    base = config.OUTPUT_DIR.rstrip("/") or "."
    return os.path.join(base, name)


def ensure_output_dir() -> None:
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)


def _sanitise(value: Any) -> str:
    """Coerce a field value to a safe CSV string.

    Strips null bytes (which crash Python's csv module), replaces bare
    carriage returns, and truncates extremely long values to prevent
    memory issues in downstream tooling.
    """
    if value is None:
        return ""
    s = str(value)
    if "\x00" in s:
        s = s.replace("\x00", "")
    if len(s) > 32_000:
        s = s[:32_000] + "…[truncated]"
    return s


def _write_header(path: str, fieldnames: tuple) -> None:
    ensure_output_dir()
    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.DictWriter(
            f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL,
        ).writeheader()


def initialise_outputs() -> None:
    """Create or truncate all output CSVs for a new run."""
    ensure_output_dir()
    _write_header(_output_path(config.PAGES_CSV), PAGES_FIELDS)
    if config.WRITE_EDGES_CSV:
        _write_header(_output_path(config.EDGES_CSV), EDGE_FIELDS)
    if config.WRITE_TAGS_CSV:
        _write_header(_output_path(config.TAGS_CSV), TAG_ROW_FIELDS)
    _write_header(_output_path(config.ERRORS_CSV), ERROR_FIELDS)
    if config.WRITE_SITEMAP_URLS_CSV:
        _write_header(_output_path(config.SITEMAP_URLS_CSV), SITEMAP_URL_FIELDS)
    if config.WRITE_NAV_LINKS_CSV:
        _write_header(_output_path(config.NAV_LINKS_CSV), NAV_LINK_FIELDS)
    if config.CHECK_OUTBOUND_LINKS:
        _write_header(_output_path(config.LINK_CHECKS_CSV), LINK_CHECK_FIELDS)
    # Touch each asset category file we might use
    seen: set[str] = set()
    for cat in set(config.ASSET_CATEGORY_BY_EXT.values()):
        if cat not in seen:
            seen.add(cat)
            _write_header(_assets_path_for_category(cat), ASSET_FIELDS)
    if "other" not in seen:
        _write_header(_assets_path_for_category("other"), ASSET_FIELDS)


def _assets_path_for_category(category: str) -> str:
    fname = f"{config.ASSETS_CSV_PREFIX}{category}.csv"
    return _output_path(fname)


def append_row(path: str, fieldnames: tuple, row: Dict[str, Any]) -> None:
    ensure_output_dir()
    safe = {k: _sanitise(row.get(k, "")) for k in fieldnames}
    try:
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f, fieldnames=fieldnames,
                extrasaction="ignore", quoting=csv.QUOTE_ALL,
            )
            w.writerow(safe)
    except Exception as e:
        url = safe.get("final_url") or safe.get("url") or safe.get("page_url") or "?"
        logger.warning("CSV write failed for %s → %s: %s", url, path, e)


def write_page(row: Dict[str, Any]) -> None:
    append_row(_output_path(config.PAGES_CSV), PAGES_FIELDS, row)


def write_asset(row: Dict[str, Any], category: str) -> None:
    known = set(config.ASSET_CATEGORY_BY_EXT.values()) | {"other"}
    cat = category if category in known else "other"
    append_row(_assets_path_for_category(cat), ASSET_FIELDS, row)


def write_edge(row: Dict[str, Any]) -> None:
    if not config.WRITE_EDGES_CSV:
        return
    append_row(_output_path(config.EDGES_CSV), EDGE_FIELDS, row)


def write_tag_row(row: Dict[str, Any]) -> None:
    if not config.WRITE_TAGS_CSV:
        return
    append_row(_output_path(config.TAGS_CSV), TAG_ROW_FIELDS, row)


def write_error(row: Dict[str, Any]) -> None:
    append_row(_output_path(config.ERRORS_CSV), ERROR_FIELDS, row)


def write_sitemap_url(row: Dict[str, Any]) -> None:
    if not config.WRITE_SITEMAP_URLS_CSV:
        return
    append_row(_output_path(config.SITEMAP_URLS_CSV), SITEMAP_URL_FIELDS, row)


def write_nav_link(row: Dict[str, Any]) -> None:
    if not config.WRITE_NAV_LINKS_CSV:
        return
    append_row(_output_path(config.NAV_LINKS_CSV), NAV_LINK_FIELDS, row)


def write_link_check(row: Dict[str, Any]) -> None:
    if not config.CHECK_OUTBOUND_LINKS:
        return
    append_row(_output_path(config.LINK_CHECKS_CSV), LINK_CHECK_FIELDS, row)
