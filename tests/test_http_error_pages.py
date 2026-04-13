"""Navigable HTML for HTTP errors (no bare plain-text trap pages)."""

import gui


def test_unknown_project_settings_is_html_with_home_link():
    gui.app.config["TESTING"] = True
    with gui.app.test_client() as c:
        r = c.get("/p/does-not-exist-xyz/settings")
    assert r.status_code == 404
    assert "text/html" in (r.content_type or "")
    body = r.get_data(as_text=True)
    assert "All projects" in body
    assert "Project not found" in body


def test_reports_dashboard_unknown_project_is_html():
    gui.app.config["TESTING"] = True
    with gui.app.test_client() as c:
        r = c.get("/p/does-not-exist-xyz/reports")
    assert r.status_code == 404
    assert "text/html" in (r.content_type or "")
    assert "All projects" in r.get_data(as_text=True)


def test_unregistered_path_uses_navigable_404_shell():
    gui.app.config["TESTING"] = True
    with gui.app.test_client() as c:
        r = c.get("/this-url-is-not-defined-anywhere-12345")
    assert r.status_code == 404
    assert "text/html" in (r.content_type or "")
    body = r.get_data(as_text=True)
    assert "All projects" in body
    assert "out of date" in body.lower() or "does not exist" in body.lower()
