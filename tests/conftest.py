"""
Shared pytest fixtures. Starts the Flask GUI for Playwright E2E tests when needed.
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Generator, Optional

import pytest
from playwright.sync_api import sync_playwright

_ROOT = Path(__file__).resolve().parent.parent
_GUI_PORT = int(os.environ.get("CSJ_GUI_PORT", "5001"))
_GUI_BIND = os.environ.get("CSJ_GUI_BIND", "127.0.0.1")
_BASE = f"http://{_GUI_BIND}:{_GUI_PORT}"


def _port_accepting(host: str, port: int, timeout_s: float = 60.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2.0):
                return True
        except OSError:
            time.sleep(0.25)
    return False


@pytest.fixture(scope="session")
def csj_e2e_flask_server() -> Generator[None, None, None]:
    """Ensure the GUI is reachable for Playwright E2E tests.

    Used by ``tests/test_playwright.py`` and ``tests/test_real_crawl.py``.

    - If something already listens on the bind/port, reuse it (manual ``gui.py``).
    - Otherwise spawn ``python3 gui.py`` for the session and terminate it after.
    """
    if os.environ.get("CSJ_E2E_NO_SERVER"):
        yield
        return

    if _port_accepting(_GUI_BIND, _GUI_PORT, timeout_s=1.0):
        yield
        return

    env = {**os.environ, "CSJ_GUI_BIND": _GUI_BIND, "CSJ_GUI_PORT": str(_GUI_PORT)}
    proc: Optional[subprocess.Popen] = None
    try:
        proc = subprocess.Popen(
            [sys.executable, str(_ROOT / "gui.py")],
            cwd=str(_ROOT),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if not _port_accepting(_GUI_BIND, _GUI_PORT, timeout_s=90.0):
            pytest.fail(
                f"Flask GUI did not become ready on {_BASE} within 90s "
                f"(exit code {proc.poll()})",
            )
        subprocess.run(
            [sys.executable, str(_ROOT / "tests" / "seed_test_data.py")],
            cwd=str(_ROOT),
            check=False,
            timeout=120,
            capture_output=True,
        )
        yield
    finally:
        if proc is not None and proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()


@pytest.fixture(scope="session")
def csj_sync_playwright_browser(csj_e2e_flask_server):
    """One Chromium instance per test session.

    ``tests/test_playwright.py`` and ``tests/test_real_crawl.py`` both need a
    browser; starting :func:`sync_playwright` twice in one session triggers
    Playwright's "Sync API inside asyncio loop" error.
    """
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    yield browser
    browser.close()
    pw.stop()
