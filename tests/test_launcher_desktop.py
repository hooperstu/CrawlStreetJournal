"""Tests for ``launcher_desktop`` (single-instance port selection)."""

from __future__ import annotations

import errno
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import mock

import pytest

import launcher_desktop as ld


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path.startswith("/api/health"):
            body = json.dumps({"ok": True}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)

    def log_message(self, *_args) -> None:
        pass


@pytest.fixture
def health_server():
    srv = HTTPServer((ld.HOST, 0), _HealthHandler)
    port = srv.server_address[1]
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    try:
        yield port
    finally:
        srv.shutdown()
        t.join(timeout=2)


def test_probe_existing_server_true(health_server: int) -> None:
    assert ld.probe_existing_server(health_server) is True


def test_probe_existing_server_false() -> None:
    assert ld.probe_existing_server(59987) is False


def test_resolve_exits_when_csj_on_preferred(monkeypatch, health_server: int) -> None:

    def fake_bind(addr):
        raise OSError(errno.EADDRINUSE, "in use")

    with mock.patch("socket.socket.bind", side_effect=fake_bind):
        with pytest.raises(SystemExit) as ei:
            ld.resolve_desktop_listen_port(health_server, wait_attempts=2, wait_interval=0.01)
    assert ei.value.code == 0


def test_resolve_returns_when_port_free() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((ld.HOST, 0))
        free = s.getsockname()[1]
    assert ld.resolve_desktop_listen_port(free) == free
