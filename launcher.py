#!/usr/bin/env python3
"""
The Crawl Street Journal — Desktop Launcher

Entry point for the packaged macOS .app.  Starts the Flask server, waits
for it to be ready, then opens the default browser.  Handles clean
shutdown on SIGINT / SIGTERM / window close.
"""

import logging
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
    """Poll until the server accepts connections, then open the browser."""
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
    sys.exit(main())
