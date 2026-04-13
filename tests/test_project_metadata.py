"""Tests for updating project display name and description in ``_project.json``."""

import config
import gui
import storage


def test_save_project_metadata_updates_file(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "PROJECTS_DIR", str(tmp_path))
    slug = storage.create_project("Original", "Old description")
    storage.save_project_metadata(slug, name="Renamed", description="New description")
    meta = storage.load_project(slug)
    assert meta["name"] == "Renamed"
    assert meta["description"] == "New description"
    assert meta.get("created_at")


def test_save_project_metadata_rejects_empty_name(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "PROJECTS_DIR", str(tmp_path))
    slug = storage.create_project("X", "")
    try:
        storage.save_project_metadata(slug, name="   ", description="")
    except ValueError as e:
        assert "empty" in str(e).lower()
    else:
        raise AssertionError("expected ValueError")


def test_save_project_metadata_route(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "PROJECTS_DIR", str(tmp_path))
    slug = storage.create_project("A", "d")
    gui.app.config["TESTING"] = True
    with gui.app.test_client() as c:
        r = c.post(
            f"/p/{slug}/settings/project",
            data={"project_name": "Better", "project_description": "Desc2"},
            follow_redirects=True,
        )
    assert r.status_code == 200
    meta = storage.load_project(slug)
    assert meta["name"] == "Better"
    assert meta["description"] == "Desc2"


def test_save_project_metadata_route_empty_name(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "PROJECTS_DIR", str(tmp_path))
    slug = storage.create_project("A", "")
    gui.app.config["TESTING"] = True
    with gui.app.test_client() as c:
        r = c.post(
            f"/p/{slug}/settings/project",
            data={"project_name": "", "project_description": ""},
        )
    assert r.status_code == 400
