"""Dashboard media endpoint tests — thumbnail byte-cache and avatar serving.

Covers ISSUES #8/#9: thumbnails fetched from Instagram's public /media/
endpoint and byte-cached to disk (US-06); avatars served from disk with a
DiceBear fallback, no Instagram call at runtime (US-07).

``dashboard/server.py`` lives outside the installed ``datalake`` package, so
it is loaded by file path here.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_SERVER_PATH = Path(__file__).resolve().parents[3] / "dashboard" / "server.py"
_spec = importlib.util.spec_from_file_location("dashboard_server", _SERVER_PATH)
assert _spec and _spec.loader, "dashboard/server.py not found"
server = importlib.util.module_from_spec(_spec)
sys.modules["dashboard_server"] = server
_spec.loader.exec_module(server)


@pytest.fixture
def thumb_dir(tmp_path, monkeypatch):
    d = tmp_path / "thumbnails"
    d.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(server, "thumbnail_path", lambda s: d / f"{s}.jpg")
    return d


@pytest.fixture
def avatar_dir(tmp_path, monkeypatch):
    d = tmp_path / "avatars"
    d.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(server, "avatar_path", lambda u: d / f"{u}.jpg")
    return d


@pytest.fixture
def tmp_ops(tmp_path, monkeypatch):
    monkeypatch.setattr(server, "OPS_PATH", tmp_path / "ops.sqlite")
    server._ensure_media_cache_table()
    return tmp_path / "ops.sqlite"


# ── Thumbnail (US-06) ────────────────────────────────────────────────────


def test_thumbnail_cache_miss(thumb_dir, tmp_ops, monkeypatch):
    """First request fetches bytes, writes to disk, inserts media_cache row."""
    monkeypatch.setattr(
        server, "_fetch_thumbnail_bytes", lambda s: (b"fakejpeg", "image/jpeg")
    )
    client = TestClient(server.app)
    resp = client.get("/api/media/thumbnail/abc123")

    assert resp.status_code == 200
    assert resp.content == b"fakejpeg"
    assert resp.headers["content-type"].startswith("image/jpeg")

    local = thumb_dir / "abc123.jpg"
    assert local.exists()
    assert local.read_bytes() == b"fakejpeg"

    import sqlite3

    con = sqlite3.connect(str(tmp_ops))
    row = con.execute(
        "SELECT local_path, content_type, size_bytes FROM media_cache "
        "WHERE cache_key = 'thumb:abc123'"
    ).fetchone()
    con.close()
    assert row is not None
    assert row[1] == "image/jpeg"
    assert row[2] == len(b"fakejpeg")


def test_thumbnail_cache_hit(thumb_dir, tmp_ops, monkeypatch):
    """Cached file served from disk; Instagram fetch never called."""
    local = thumb_dir / "abc123.jpg"
    local.write_bytes(b"cachedbytes")

    def _boom(_s):
        raise AssertionError("Instagram must not be called on cache hit")

    monkeypatch.setattr(server, "_fetch_thumbnail_bytes", _boom)
    client = TestClient(server.app)
    resp = client.get("/api/media/thumbnail/abc123")

    assert resp.status_code == 200
    assert resp.content == b"cachedbytes"


def test_thumbnail_instagram_404(thumb_dir, tmp_ops, monkeypatch):
    """Instagram failure (None) → HTTP 404, nothing cached."""
    monkeypatch.setattr(server, "_fetch_thumbnail_bytes", lambda s: None)
    client = TestClient(server.app)
    resp = client.get("/api/media/thumbnail/missing")

    assert resp.status_code == 404
    assert not (thumb_dir / "missing.jpg").exists()


def test_thumbnail_non_image_content_type():
    """Non-image content type is not cached."""
    shortcode = "notimage"
    fake_resp = type(
        "Resp",
        (),
        {
            "headers": {"Content-Type": "text/html"},
            "read": lambda self: b"<html></html>",
        },
    )()
    # Patch urlopen at the module level so _fetch_thumbnail_bytes sees it.
    import urllib.request

    real_urlopen = urllib.request.urlopen

    def _fake_urlopen(req, timeout=10):
        return fake_resp

    server.urllib.request.urlopen = _fake_urlopen
    try:
        assert server._fetch_thumbnail_bytes(shortcode) is None
    finally:
        server.urllib.request.urlopen = real_urlopen


def test_thumbnail_empty_body():
    """Empty response body is not cached."""
    fake_resp = type(
        "Resp",
        (),
        {
            "headers": {"Content-Type": "image/jpeg"},
            "read": lambda self: b"",
        },
    )()
    import urllib.request

    real_urlopen = urllib.request.urlopen

    def _fake_urlopen(req, timeout=10):
        return fake_resp

    server.urllib.request.urlopen = _fake_urlopen
    try:
        assert server._fetch_thumbnail_bytes("empty") is None
    finally:
        server.urllib.request.urlopen = real_urlopen


# ── Avatar (US-07) ───────────────────────────────────────────────────────


def test_avatar_from_disk(avatar_dir, tmp_ops):
    """Existing file served from disk."""
    (avatar_dir / "bob.jpg").write_bytes(b"avatarbytes")
    client = TestClient(server.app)
    resp = client.get("/api/media/avatar/bob")

    assert resp.status_code == 200
    assert resp.content == b"avatarbytes"


def test_avatar_empty_file_fallback(avatar_dir, tmp_ops):
    """0-byte file treated as uncached → DiceBear redirect."""
    (avatar_dir / "bob.jpg").write_bytes(b"")
    client = TestClient(server.app)
    resp = client.get("/api/media/avatar/bob", follow_redirects=False)

    assert resp.status_code == 302
    assert "dicebear.com" in resp.headers["location"]


def test_avatar_no_file_dicebear(avatar_dir, tmp_ops):
    """No local file → 302 redirect to DiceBear identicon."""
    client = TestClient(server.app)
    resp = client.get("/api/media/avatar/nobody", follow_redirects=False)

    assert resp.status_code == 302
    assert "dicebear.com" in resp.headers["location"]
