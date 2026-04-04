"""
Shared utilities for The Crawl Street Journal.

This module provides small, stateless helpers that are used by more than one
module.  It must not import from any other CSJ module (``config``, ``parser``,
``scraper``, etc.) so that any module can safely import from here without
creating circular dependencies.
"""

from __future__ import annotations

import csv
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List
from urllib.parse import urlparse


# ── JSON-LD ────────────────────────────────────────────────────────────────


def flatten_json_ld(obj: Any) -> List[dict]:
    """Recursively flatten a JSON-LD object (or ``@graph`` array) into a list of node dicts.

    Handles three cases:
    - A dict with a ``@graph`` key — each graph item is recursed into.
    - A plain dict (a single node) — returned as ``[obj]``.
    - A list — each element is recursed into.

    Any non-dict, non-list value is silently ignored.
    """
    nodes: List[dict] = []
    if isinstance(obj, dict):
        if "@graph" in obj and isinstance(obj["@graph"], list):
            for item in obj["@graph"]:
                nodes.extend(flatten_json_ld(item))
        else:
            nodes.append(obj)
    elif isinstance(obj, list):
        for item in obj:
            nodes.extend(flatten_json_ld(item))
    return nodes


# ── Timestamps ─────────────────────────────────────────────────────────────


def now_iso() -> str:
    """Return the current UTC time as a ``YYYY-MM-DD HH:MM:SS`` string.

    Used for ``discovered_at`` fields written to CSV outputs.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ── CSV helpers ────────────────────────────────────────────────────────────


def sanitise_csv_value(value: Any) -> str:
    """Coerce *value* to a CSV-safe string (strip NUL bytes, truncate at 32 KB).

    Returns an empty string for ``None``.  Strings longer than 32 000 characters
    are truncated with a ``…[truncated]`` suffix to prevent runaway CSV rows.
    """
    if value is None:
        return ""
    s = str(value)
    if "\x00" in s:
        s = s.replace("\x00", "")
    if len(s) > 32_000:
        s = s[:32_000] + "…[truncated]"
    return s


def count_csv_rows(filepath: str) -> int:
    """Return the number of data rows in *filepath* (excluding the header), or 0 on any error.

    Counts by iterating lines rather than loading the full CSV into memory, so
    it is safe to call on large output files.
    """
    import os
    if not os.path.isfile(filepath):
        return 0
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return max(sum(1 for _ in f) - 1, 0)
    except Exception:
        return 0


# ── Domain filtering ───────────────────────────────────────────────────────


def is_allowed_domain(url: str, domains: Iterable[str]) -> bool:
    """Return ``True`` if *url*'s hostname matches any entry in *domains*.

    Matching rules:
    - An empty *domains* list means no restriction — all domains are allowed.
    - Leading dots in domain entries are stripped before comparison.
    - Exact hostname match OR subdomain suffix match (``host.endswith(".domain")``).
    - Case-insensitive on both sides.
    - Any URL-parsing failure returns ``False`` so malformed URLs are excluded.

    Args:
        url:     The URL whose hostname should be checked.
        domains: An iterable of allowed domain strings (e.g. ``["example.com"]``).
                 Pass an empty iterable to allow all domains.
    """
    try:
        normalised = [
            str(d).strip().lower().lstrip(".")
            for d in domains
            if str(d).strip()
        ]
        if not normalised:
            return True
        host = (urlparse(url).hostname or "").lower()
        return any(host == d or host.endswith("." + d) for d in normalised)
    except Exception:
        return False


# ── Robots.txt parsing ─────────────────────────────────────────────────────


def parse_robots_for_sitemaps(text: str) -> List[str]:
    """Extract ``Sitemap:`` directive URLs from a ``robots.txt`` body.

    The prefix is matched case-insensitively.  The leading ``"Sitemap:"`` is
    stripped by its byte length so the full URL (including its own ``https:``
    scheme colon) is preserved.

    Args:
        text: The raw text content of a ``robots.txt`` file.

    Returns:
        A list of sitemap URL strings, in the order they appear in the file.
        Returns an empty list if none are found or *text* is empty.
    """
    found: List[str] = []
    for line in (text or "").splitlines():
        line = line.strip()
        if line.lower().startswith("sitemap:"):
            found.append(line[len("sitemap:"):].strip())
    return found


# ── Shared CSV / numeric helpers ──────────────────────────────────────────

def read_csv(path: str) -> List[Dict[str, str]]:
    """Read a CSV file into a list of dicts, returning empty on any error."""
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def safe_int(val: str, default: int = 0) -> int:
    """Parse an integer from a string, returning *default* on failure."""
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def safe_float(val: str, default: float = 0.0) -> float:
    """Parse a float from a string, returning *default* on failure."""
    try:
        return float(val)
    except (ValueError, TypeError):
        return default
