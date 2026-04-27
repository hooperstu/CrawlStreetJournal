"""
The Crawl Street Journal — Android / Briefcase entry point.

This module mirrors the desktop ``launcher.py`` but is tailored for the
Android environment created by BeeWare Briefcase:

  1. Sets ``ANDROID_FILES_DIR`` so that ``config.py`` resolves DATA_DIR
     to the app-private storage directory.
  2. Adds the project root to ``sys.path`` so the existing module layout
     (``gui``, ``scraper``, ``parser``, …) is importable without moving
     files into a Python package.
  3. Starts the Flask server on a free local port in a daemon thread.
  4. Opens the default browser (Chrome on most Android devices) pointing
     at the local server.

On non-Android platforms this module uses the same **single-instance**
port logic and ``run_server`` as ``launcher.py`` / ``python gui.py`` so
``POST /api/quit`` can shut down cleanly and a second launch focuses the
first instead of taking another port.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading
import time
import webbrowser

HOST = "127.0.0.1"
PORT = 5001


def _configure_android_env() -> None:
    """Set up environment variables and sys.path for an Android build.

    Briefcase places the application source inside a predictable directory.
    We detect Android via the ``ANDROID_DATA`` environment variable (set
    by the Android runtime) and point ``ANDROID_FILES_DIR`` at the app's
    private files area so ``config.py`` can resolve ``DATA_DIR``.
    """
    if "ANDROID_DATA" not in os.environ and not hasattr(sys, "getandroidapilevel"):
        return  # Not running on Android — nothing to do.

    # Briefcase + Chaquopy put app code under a known path.  The app's
    # writable files directory is typically accessible via the Activity
    # context, but at Python level we can derive it from __file__.
    app_dir = os.path.dirname(os.path.abspath(__file__))
    files_dir = os.path.join(app_dir, "..", "files")
    files_dir = os.path.normpath(files_dir)
    if os.path.isdir(files_dir):
        os.environ.setdefault("ANDROID_FILES_DIR", files_dir)
    else:
        # Fallback: use the home directory.
        os.environ.setdefault(
            "ANDROID_FILES_DIR",
            os.path.join(os.path.expanduser("~"), "CrawlStreetJournal"),
        )


def _add_project_root_to_path() -> None:
    """Ensure the CSJ project root is on ``sys.path``.

    In a Briefcase build the ``sources`` list places ``src/csjapp`` on
    ``sys.path``.  The rest of the codebase (``gui.py``, ``scraper.py``,
    etc.) lives two directories up from this file.  Add that directory
    so that ``import gui`` and friends resolve correctly.
    """
    project_root = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
    )
    if project_root not in sys.path:
        sys.path.insert(0, project_root)


def _wait_for_server(port: int, retries: int = 60, interval: float = 0.25) -> bool:
    """Block until the Flask server accepts connections."""
    import socket

    for _ in range(retries):
        try:
            with socket.create_connection((HOST, port), timeout=0.25):
                return True
        except OSError:
            time.sleep(interval)
    return False


def main() -> int:
    """Application entry point for Briefcase / ``python -m csjapp``."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger(__name__)

    _configure_android_env()
    _add_project_root_to_path()

    import launcher_desktop as _desk

    import storage as storage_module  # noqa: E402
    from gui import app, ensure_stale_run_states_recovered, run_server  # noqa: E402

    storage_module.migrate_legacy_data()
    ensure_stale_run_states_recovered()

    preferred = int(os.environ.get("CSJ_GUI_PORT", str(PORT)))
    try:
        port = _desk.resolve_desktop_listen_port(preferred)
    except SystemExit as e:
        code = e.code
        return int(code) if isinstance(code, int) else 0

    url = f"http://{HOST}:{port}"

    raise_evt = threading.Event()
    app.config["CSJ_DESKTOP_RAISE_EVENT"] = raise_evt

    def _focus_browser() -> None:
        webbrowser.open(url)

    def _raise_poll() -> None:
        while True:
            if raise_evt.wait(timeout=0.5):
                raise_evt.clear()
                _focus_browser()

    threading.Thread(target=_raise_poll, name="csj-raise", daemon=True).start()

    def _shutdown(*_args: object) -> None:
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _shutdown)

    log.info("The Crawl Street Journal: %s", url)

    flask_thread = threading.Thread(
        target=run_server,
        kwargs={"host": HOST, "port": port, "threaded": True},
        daemon=True,
    )
    flask_thread.start()

    if _wait_for_server(port):
        if sys.platform == "win32" and _desk.try_open_windows_dedicated_browser(url):
            pass
        else:
            webbrowser.open(url)
    else:
        log.warning("Server did not become ready on port %d", port)

    try:
        flask_thread.join()
    except KeyboardInterrupt:
        pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
