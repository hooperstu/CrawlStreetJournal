#!/usr/bin/env python3
"""
The Crawl Street Journal — Desktop Launcher

Opens a native desktop window (via pywebview) containing the Flask GUI.
Falls back to the default browser if pywebview is not available.

The Flask server runs in a background thread; the main thread owns the
native window lifecycle (required by macOS and Windows GUI toolkits).
"""

import logging
import os
import signal
import socket
import sys
import threading
import time
import webbrowser

HOST = "127.0.0.1"
PORT = 5001
MAX_PORT_ATTEMPTS = 10

_WEBVIEW_AVAILABLE = False
try:
    import webview  # pywebview
    _WEBVIEW_AVAILABLE = True
except ImportError:
    pass


def _find_free_port(start: int = PORT, attempts: int = MAX_PORT_ATTEMPTS) -> int:
    for port in range(start, start + attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind((HOST, port))
                return port
            except OSError:
                continue
    raise RuntimeError(
        f"No free port found in range {start}\u2013{start + attempts - 1}"
    )


def _wait_for_server(port: int, retries: int = 60, interval: float = 0.25) -> bool:
    """Block until the Flask server accepts connections."""
    for _ in range(retries):
        try:
            with socket.create_connection((HOST, port), timeout=0.25):
                return True
        except OSError:
            time.sleep(interval)
    return False


def _open_in_browser(port: int) -> None:
    """Fallback: open in the default browser (used when pywebview is missing)."""
    if _wait_for_server(port):
        webbrowser.open(f"http://{HOST}:{port}")
    else:
        logging.getLogger(__name__).warning(
            "Server did not become ready on port %d — skipping browser launch", port
        )


def _crash_log_path() -> str:
    if getattr(sys, "frozen", False):
        import config
        return os.path.join(config.DATA_DIR, "crash.log")
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "crash.log")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    import storage as storage_module
    from gui import run_server

    storage_module.migrate_legacy_data()

    port = _find_free_port()
    url = f"http://{HOST}:{port}"

    def _shutdown(*_args):
        print("\nShutting down\u2026", file=sys.stderr)
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _shutdown)

    print(f"The Crawl Street Journal: {url}")

    # Start Flask in a daemon thread — the main thread drives the UI.
    # ``run_server`` uses Werkzeug so /api/quit can call ``server.shutdown()``.
    flask_thread = threading.Thread(
        target=run_server,
        kwargs={"host": HOST, "port": port, "threaded": True},
        daemon=True,
    )
    flask_thread.start()

    if _WEBVIEW_AVAILABLE:
        _wait_for_server(port)
        try:
            webview.create_window(
                "The Crawl Street Journal",
                url,
                width=1280,
                height=860,
                min_size=(900, 600),
            )
            webview.start()
            return 0
        except Exception as wv_err:
            # GUI backend not available — e.g. missing pythonnet/.NET on
            # Windows, or no GTK/WebKit on Linux.  Log diagnostics so the
            # issue is traceable, then fall through to the browser fallback.
            _log = logging.getLogger(__name__)
            _log.warning(
                "Native window unavailable (%s: %s) — opening in default browser",
                type(wv_err).__name__,
                wv_err,
            )
            if getattr(sys, "frozen", False):
                try:
                    import config as _cfg
                    _diag = os.path.join(_cfg.DATA_DIR, "webview-error.log")
                    import traceback
                    with open(_diag, "w") as _f:
                        _f.write(
                            f"pywebview failed at "
                            f"{time.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                        )
                        traceback.print_exc(file=_f)
                    _log.info("Diagnostic log written to %s", _diag)
                except Exception:
                    pass

    _open_in_browser(port)
    try:
        flask_thread.join()
    except KeyboardInterrupt:
        pass

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        import traceback
        crash_path = _crash_log_path()
        tb = traceback.format_exc()
        try:
            with open(crash_path, "w") as f:
                f.write(f"Crash at {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n{tb}")
            print(f"\nFATAL ERROR — crash log written to: {crash_path}", file=sys.stderr)
        except Exception:
            pass
        print(tb, file=sys.stderr)
        sys.exit(1)
