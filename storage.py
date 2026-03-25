"""
CSV writers for NHS Collector: pages inventory, assets by type, edges, tags, errors.
"""

import csv
import os
from typing import Any, Dict

import config

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


def _output_path(name: str) -> str:
    base = config.OUTPUT_DIR.rstrip("/") or "."
    return os.path.join(base, name)


def ensure_output_dir() -> None:
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)


def _write_header(path: str, fieldnames: tuple) -> None:
    ensure_output_dir()
    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=fieldnames).writeheader()


def initialise_outputs() -> None:
    """Create or truncate all output CSVs for a new run."""
    ensure_output_dir()
    _write_header(_output_path(config.PAGES_CSV), PAGES_FIELDS)
    if config.WRITE_EDGES_CSV:
        _write_header(_output_path(config.EDGES_CSV), EDGE_FIELDS)
    if config.WRITE_TAGS_CSV:
        _write_header(_output_path(config.TAGS_CSV), TAG_ROW_FIELDS)
    _write_header(_output_path(config.ERRORS_CSV), ERROR_FIELDS)
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
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writerow({k: row.get(k, "") for k in fieldnames})


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
