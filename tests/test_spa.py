from fastapi.testclient import TestClient

import app.main as main_module
from app.main import app

client = TestClient(app)


def test_api_route_still_json() -> None:
    assert client.get("/health").headers["content-type"].startswith("application/json")


def test_docs_route_untouched() -> None:
    assert client.get("/openapi.json").status_code == 200


def test_client_route_returns_index_when_dist_present(tmp_path, monkeypatch) -> None:
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("<!doctype html><title>demo</title>")
    monkeypatch.setattr(main_module, "FRONTEND_DIST", dist)
    r = client.get("/demo")
    assert r.status_code == 200
    assert "<!doctype html>" in r.text.lower()


def test_client_route_returns_hint_when_dist_absent(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(main_module, "FRONTEND_DIST", tmp_path / "missing")
    r = client.get("/architecture")
    assert r.status_code == 200
    assert "build" in r.text.lower()


def test_unknown_api_path_is_404_not_index(tmp_path, monkeypatch) -> None:
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("<!doctype html>")
    monkeypatch.setattr(main_module, "FRONTEND_DIST", dist)
    assert client.get("/api/v1/statements/does-not-exist").status_code == 404
