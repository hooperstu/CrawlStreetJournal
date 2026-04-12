"""Smoke tests for reports blueprint routes in ``viz_api``."""

import gui


def test_api_competitor_intelligence_empty_project():
    """No run directories yield empty-shaped JSON (registers blueprint route)."""
    gui.app.config["TESTING"] = True
    with gui.app.test_client() as c:
        r = c.get("/p/__viz_api_empty__/api/viz/competitor_intelligence")
    assert r.status_code == 200
    data = r.get_json()
    assert data["keyword_content"]["top_tags"] == []
    assert data.get("full_lists") is False


def test_export_competitor_intelligence_zip_no_runs():
    """ZIP export returns 404 when there is no crawl data."""
    gui.app.config["TESTING"] = True
    with gui.app.test_client() as c:
        r = c.get("/p/__viz_api_empty__/export/competitor_intelligence.zip")
    assert r.status_code == 404
