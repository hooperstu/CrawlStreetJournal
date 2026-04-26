"""
Desktop launcher helpers: second-instance hand-off to the default port,
and optional Windows system tray (pystray).

Imported by ``launcher.py`` only.
"""

from __future__ import annotations

import errno
import json
import logging
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Optional

HOST = "127.0.0.1"
DEFAULT_PORT = 5001

_log = logging.getLogger(__name__)


def _gui_icon_path() -> Optional[Path]:
    """PNG for pystray; None if missing."""
    try:
        import config as _cfg

        p = Path(_cfg.BUNDLE_DIR) / "static" / "img" / "icon-192.png"
        if p.is_file():
            return p
    except Exception:
        pass
    here = Path(__file__).resolve().parent / "static" / "img" / "icon-192.png"
    return here if here.is_file() else None


def probe_existing_server(port: int, timeout: float = 0.35) -> bool:
    """True if something on loopback answers like our Flask ``/api/health``."""
    url = f"http://{HOST}:{port}/api/health"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                return False
            body = resp.read().decode("utf-8", errors="replace")
            data = json.loads(body)
            return bool(data.get("ok"))
    except (OSError, urllib.error.URLError, ValueError, json.JSONDecodeError):
        return False


def notify_first_instance(port: int) -> None:
    """Wake the running app (tray handler shows window); ignore failures."""
    url = f"http://{HOST}:{port}/api/raise"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=0.75):
            pass
    except (OSError, urllib.error.URLError):
        pass


def _addr_in_use(err: OSError) -> bool:
    if err.errno in (errno.EADDRINUSE, getattr(errno, "WSAEADDRINUSE", -1)):
        return True
    return getattr(err, "winerror", None) == 10048


def resolve_desktop_listen_port(
    preferred: int,
    *,
    wait_attempts: int = 48,
    wait_interval: float = 0.15,
    fallback_span: int = 10,
) -> int:
    """Pick a port for the GUI server.

    If *preferred* is in use by a running CSJ instance (health check), notify it
    and raise ``SystemExit(0)`` so this second launch exits immediately.

    If *preferred* is busy during server startup, wait briefly for health before
    claiming the next free port (avoids orphaned 5001 + app on 5002).
    """
    for _ in range(wait_attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((HOST, preferred))
        except OSError as e:
            if not _addr_in_use(e):
                raise
            if probe_existing_server(preferred):
                notify_first_instance(preferred)
                raise SystemExit(0)
            time.sleep(wait_interval)
            continue
        return preferred

    for port in range(preferred + 1, preferred + fallback_span):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((HOST, port))
        except OSError as e:
            if not _addr_in_use(e):
                raise
            continue
        _log.warning(
            "Default GUI port %d is in use by another program — binding to %d instead",
            preferred,
            port,
        )
        return port

    raise RuntimeError(
        f"No free GUI port found in range {preferred}\u2013{preferred + fallback_span - 1}"
    )


def run_windows_tray(
    *,
    window_factory: Callable[[], Any],
    on_quit: Callable[[], None],
) -> None:
    """Run pystray on a background thread; ``window_factory`` returns the pywebview Window."""
    if sys.platform != "win32":
        return
    try:
        from PIL import Image
        import pystray
    except ImportError:
        _log.info("pystray/Pillow not installed — tray icon disabled")
        return

    icon_path = _gui_icon_path()
    if icon_path is None:
        _log.warning("Tray icon PNG not found — skipping system tray")
        return

    tray_icon_holder: dict[str, Any] = {}

    def _show_window(_icon: Any = None, _item: Any = None) -> None:
        try:
            w = window_factory()
            if w is not None:
                w.show()
        except Exception:
            _log.exception("Tray: show window failed")

    def _hide_to_tray(_icon: Any = None, _item: Any = None) -> None:
        try:
            import webview

            if webview.windows:
                webview.windows[0].hide()
        except Exception:
            _log.exception("Tray: hide window failed")

    def _quit_app(_icon: Any = None, _item: Any = None) -> None:
        on_quit()
        icon = tray_icon_holder.get("icon")
        if icon is not None:
            try:
                icon.stop()
            except Exception:
                pass

    image = Image.open(icon_path)
    menu = pystray.Menu(
        pystray.MenuItem(
            "Open Crawl Street Journal", _show_window, default=True
        ),
        pystray.MenuItem("Hide to tray", _hide_to_tray),
        pystray.Menu.SEPARATOR,
        pystray.MenuItem("Quit", _quit_app),
    )
    icon = pystray.Icon("csj", image, "The Crawl Street Journal", menu)
    tray_icon_holder["icon"] = icon

    def _run_icon() -> None:
        try:
            icon.run()
        except Exception:
            _log.exception("System tray failed")

    threading.Thread(target=_run_icon, name="csj-tray", daemon=True).start()
