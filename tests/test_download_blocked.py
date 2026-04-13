"""Tests for HTML 409 responses when downloading during an active crawl."""

import pytest

import gui


@pytest.fixture
def client():
    gui.app.config["TESTING"] = True
    with gui.app.test_client() as c:
        yield c


def test_download_single_returns_html_with_navigation_when_run_active(client, monkeypatch):
    monkeypatch.setattr(gui, "_is_run_active", lambda slug, run_name: True)
    monkeypatch.setattr(
        gui.storage_module,
        "load_project",
        lambda slug: {"name": "Test Project", "slug": slug},
    )
    r = client.get("/p/myproj/runs/run_xyz/download/pages.csv")
    assert r.status_code == 409
    assert "text/html" in (r.content_type or "")
    body = r.get_data(as_text=True)
    assert "Open crawl monitor" in body
    assert "/p/myproj/runs/run_xyz/monitor" in body
    assert "All runs" in body


def test_download_all_returns_html_when_run_active(client, monkeypatch):
    monkeypatch.setattr(gui, "_is_run_active", lambda slug, run_name: True)
    monkeypatch.setattr(
        gui.storage_module,
        "load_project",
        lambda slug: {"name": "Test Project", "slug": slug},
    )
    r = client.get("/p/myproj/runs/run_xyz/download-all")
    assert r.status_code == 409
    assert "text/html" in (r.content_type or "")
    assert "Open crawl monitor" in r.get_data(as_text=True)
