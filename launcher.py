#!/usr/bin/env python3
"""
The Crawl Street Journal — Desktop Launcher

Entry point for the packaged macOS .app.  Scans for a free TCP port starting
at 5001 (incrementing up to ``MAX_PORT_ATTEMPTS`` times if the default is
occupied), starts the Flask server on that port, then opens the user's default
browser once the server accepts connections.  Handles clean shutdown on
SIGINT / SIGTERM / window close.
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


def _find_free_port(start: int = PORT, attempts: int = MAX_PORT_ATTEMPTS) -> int:
    """Return the first available port in ``[start, start + attempts)``.

    Probes each candidate by attempting to bind a TCP socket; a successful
    bind confirms the port is free.

    Raises:
        RuntimeError: If every port in the range is already occupied.
    """
    for port in range(start, start + attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind((HOST, port))
                return port
            except OSError:
                continue
    raise RuntimeError(
        f"No free port found in range {start}–{start + attempts - 1}"
    )


def _wait_and_open(port: int) -> None:
    """Poll until the Flask server accepts TCP connections, then open the browser.

    Retries up to 40 times at 250 ms intervals (~10 s total).  Runs in a
    daemon thread so the main thread is free to start Flask immediately.
    """
    for _ in range(40):
        try:
            with socket.create_connection((HOST, port), timeout=0.25):
                webbrowser.open(f"http://{HOST}:{port}")
                return
        except OSError:
            time.sleep(0.25)
    logging.getLogger(__name__).warning(
        "Server did not become ready on port %d — skipping browser launch", port
    )


def _crash_log_path() -> str:
    """Return a writable path for the crash log, next to the executable."""
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
    from gui import app

    storage_module.migrate_legacy_data()

    port = _find_free_port()

    threading.Thread(target=_wait_and_open, args=(port,), daemon=True).start()

    def _shutdown(*_args):
        print("\nShutting down…", file=sys.stderr)
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _shutdown)

    print(f"The Crawl Street Journal: http://{HOST}:{port}")
    app.run(host=HOST, port=port, debug=False, threaded=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        import os
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
