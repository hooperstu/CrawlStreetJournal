"""
Optional JavaScript rendering for The Crawl Street Journal.

When ``RENDER_JAVASCRIPT`` is enabled in config, pages are fetched via a
headless Chromium browser (Playwright) so that client-side rendered content
is available to the parser.

Playwright is **not** listed in ``requirements.txt``; install it with::

    pip install playwright
    playwright install chromium

If Playwright is not installed the module gracefully degrades and
:func:`render_page` returns ``None``.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

_PLAYWRIGHT_AVAILABLE = False
_browser = None
_playwright_instance = None

try:
    from playwright.sync_api import sync_playwright  # noqa: F401
    _PLAYWRIGHT_AVAILABLE = True
except ImportError:
    pass


def is_available() -> bool:
    """Return ``True`` if Playwright is installed and usable."""
    return _PLAYWRIGHT_AVAILABLE


def _get_browser():
    """Lazy-initialise a shared headless Chromium browser instance."""
    global _browser, _playwright_instance
    if not _PLAYWRIGHT_AVAILABLE:
        return None
    if _browser is None:
        try:
            _playwright_instance = sync_playwright().start()
            _browser = _playwright_instance.chromium.launch(headless=True)
            logger.info("Playwright headless browser started")
        except Exception as e:
            logger.warning("Could not start Playwright browser: %s", e)
            return None
    return _browser


def render_page(
    url: str,
    timeout_ms: int = 30_000,
    wait_until: str = "networkidle",
    user_agent: str = "",
) -> Optional[Tuple[str, int, str, str, Dict[str, str]]]:
    """Fetch *url* via headless Chromium and return the rendered DOM.

    Returns ``(html, status, final_url, content_type, headers)`` or
    ``None`` when rendering is not available or fails.
    """
    browser = _get_browser()
    if browser is None:
        return None

    page = None
    context = None
    try:
        context = browser.new_context(
            user_agent=user_agent or None,
            ignore_https_errors=True,
        )
        page = context.new_page()

        response = page.goto(url, timeout=timeout_ms, wait_until=wait_until)
        if response is None:
            return None

        html = page.content()
        status = response.status
        final_url = page.url
        ct = response.headers.get("content-type", "").split(";")[0].strip()
        headers = dict(response.headers)

        return html, status, final_url, ct, headers
    except Exception as e:
        logger.warning("Playwright render failed for %s: %s", url, e)
        return None
    finally:
        if page:
            try:
                page.close()
            except Exception:
                pass
        if context:
            try:
                context.close()
            except Exception:
                pass


def close() -> None:
    """Shut down the shared browser instance."""
    global _browser, _playwright_instance
    if _browser is not None:
        try:
            _browser.close()
        except Exception:
            pass
        _browser = None
    if _playwright_instance is not None:
        try:
            _playwright_instance.stop()
        except Exception:
            pass
        _playwright_instance = None
