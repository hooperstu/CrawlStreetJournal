"""Tests for ``POST /api/quit`` (desktop graceful shutdown)."""

import threading

import pytest

import gui


@pytest.fixture
def client():
    gui.app.config["TESTING"] = True
    with gui.app.test_client() as c:
        yield c


def test_quit_forbidden_when_not_loopback(client, monkeypatch):
    monkeypatch.setattr(gui, "_client_is_loopback", lambda: False)
    r = client.post("/api/quit")
    assert r.status_code == 403
    assert r.get_json() == {"ok": False, "error": "forbidden"}


def test_api_health_ok_loopback(client, monkeypatch):
    monkeypatch.setattr(gui, "_client_is_loopback", lambda: True)
    r = client.get("/api/health")
    assert r.status_code == 200
    assert r.get_json() == {"ok": True}


def test_api_raise_forbidden_not_loopback(client, monkeypatch):
    monkeypatch.setattr(gui, "_client_is_loopback", lambda: False)
    r = client.get("/api/raise")
    assert r.status_code == 403


def test_api_raise_sets_config_event(monkeypatch):
    monkeypatch.setattr(gui, "_client_is_loopback", lambda: True)
    ev = threading.Event()
    gui.app.config["CSJ_DESKTOP_RAISE_EVENT"] = ev
    with gui.app.test_client() as c:
        r = c.get("/api/raise")
    assert r.status_code == 200
    assert r.get_json() == {"ok": True}
    assert ev.is_set()


def test_quit_ok_starts_shutdown_worker(client, monkeypatch):
    monkeypatch.setattr(gui, "_client_is_loopback", lambda: True)
    called = {"worker": False}

    def fake_worker():
        called["worker"] = True

    monkeypatch.setattr(gui, "_quit_worker", fake_worker)
    monkeypatch.setattr(gui, "_signal_all_crawls_stop", lambda: None)
    r = client.post("/api/quit")
    assert r.status_code == 200
    assert r.get_json() == {"ok": True}
    assert called["worker"] is True
