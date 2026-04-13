"""
HTML error pages with full app chrome (masthead, navigation).

Used anywhere a browser might land on an error so users are never stuck
on a blank body with only plain text.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from flask import render_template

import storage as storage_module


def render_http_error(
    message: str,
    status: int,
    *,
    slug: Optional[str] = None,
    project: Optional[Dict[str, Any]] = None,
    page_title: Optional[str] = None,
) -> Tuple[str, int]:
    """Return ``(html_body, status)`` for a navigable error page."""
    proj: Optional[Dict[str, Any]] = None
    if project is not None:
        proj = dict(project)
        if slug:
            proj.setdefault("slug", slug)
    elif slug:
        loaded = storage_module.load_project(slug)
        if loaded:
            proj = dict(loaded)
            proj["slug"] = slug
    pt = page_title if page_title is not None else f"Error {status}"
    body = render_template(
        "http_error.html",
        message=message,
        status_code=status,
        page_title=pt,
        project=proj,
    )
    return body, status
