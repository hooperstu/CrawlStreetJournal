"""Tests for GUI authentication, CSRF, and reverse-proxy-related behaviour."""

import re
import uuid

import pytest

import gui


@pytest.fixture
def client():
    gui.app.config["TESTING"] = True
    with gui.app.test_client() as c:
        yield c


@pytest.fixture
def client_auth_mode(monkeypatch):
    """Exercise real ``before_request`` auth (``TESTING`` would skip it)."""
    monkeypatch.setenv("CSJ_GUI_PASSWORD", "pw1")
    gui.app.config["TESTING"] = False
    try:
        with gui.app.test_client() as c:
            yield c
    finally:
        gui.app.config["TESTING"] = True
        monkeypatch.delenv("CSJ_GUI_PASSWORD", raising=False)


def test_post_without_csrf_returns_400(client):
    r = client.post("/projects/create", data={"name": "x"})
    assert r.status_code == 400


def test_post_with_csrf_succeeds(client):
    page = client.get("/")
    assert page.status_code == 200
    m = re.search(r'name="csrf_token" value="([^"]+)"', page.get_data(as_text=True))
    assert m, "csrf_token expected on project list forms"
    token = m.group(1)
    name = f"csrf-test-{uuid.uuid4().hex[:8]}"
    r = client.post(
        "/projects/create",
        data={"name": name, "csrf_token": token},
        follow_redirects=False,
    )
    assert r.status_code in (302, 303)


def test_quit_works_without_csrf_token(client, monkeypatch):
    monkeypatch.setattr(gui, "_client_is_loopback", lambda: True)
    monkeypatch.setattr(gui, "_quit_worker", lambda: None)
    monkeypatch.setattr(gui, "_signal_all_crawls_stop", lambda: None)
    r = client.post("/api/quit")
    assert r.status_code == 200
    assert r.get_json() == {"ok": True}


def test_login_redirect_when_password_required(client_auth_mode):
    r = client_auth_mode.get("/", follow_redirects=False)
    assert r.status_code == 302
    assert "/login" in r.headers.get("Location", "")


def test_password_login_sets_session(client_auth_mode):
    page = client_auth_mode.get("/login")
    assert page.status_code == 200
    m = re.search(r'name="csrf_token" value="([^"]+)"', page.get_data(as_text=True))
    assert m, "csrf_token field expected on login page"
    token = m.group(1)
    r = client_auth_mode.post(
        "/login",
        data={"password": "pw1", "csrf_token": token},
        follow_redirects=False,
    )
    assert r.status_code in (302, 303)
    r2 = client_auth_mode.get("/", follow_redirects=False)
    assert r2.status_code == 200


def test_client_is_loopback_addresses():
    with gui.app.test_request_context(environ_overrides={"REMOTE_ADDR": "127.0.0.1"}):
        assert gui._client_is_loopback() is True
    with gui.app.test_request_context(environ_overrides={"REMOTE_ADDR": "10.0.0.1"}):
        assert gui._client_is_loopback() is False
