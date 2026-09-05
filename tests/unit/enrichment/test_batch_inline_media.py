"""Unit tests for batch-path inline media (optimization B).

The batch request-building path (and ONLY that path — interactive stays on the
File API) serves small images as inline_data bytes so no File API upload
round-trip happens. Video and oversize files still route to the File API.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from datalake.defs.common.resources import DuckDBResource, GeminiResource, SQLiteResource
from datalake.defs.enrichment import media_cache
from datalake.defs.enrichment.gemini_batch import _build_contents, _to_inlined_request
from datalake.defs.enrichment.media_cache import lookup_or_upload_all, url_hash
from tests.unit.enrichment.test_media_cache import _make_fake_client, _seed_byte_cache

from scripts.enrichment_worker import build_requests_for_items


@pytest.fixture()
def batch_env(tmp_path):
    """Ops db with a claimed gemini-batch + DuckDB silver rows for one post."""
    from datalake.defs.enrichment.batch import _ensure_schema, claim_pending_items, create_batch

    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    _ensure_schema(ops)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    with duckdb.get_connection() as conn:
        conn.execute(
            "CREATE TABLE silver_ig_posts "
            "(post_id VARCHAR, caption VARCHAR, media_files VARCHAR)"
        )
    return ops, duckdb


def _add_silver_post(duckdb, post_id, media_files):
    with duckdb.get_connection() as conn:
        conn.execute(
            "INSERT INTO silver_ig_posts VALUES (?, ?, ?)",
            [post_id, "caption", media_files],
        )


def _claim(ops, post_id):
    from datalake.defs.enrichment.batch import claim_pending_items, create_batch

    payload = json.dumps({"post_id": post_id, "domain": "instagram"})
    job_id = create_batch(ops, [payload], mode="gemini-batch")
    items = claim_pending_items(ops, job_id, limit=10)
    assert items, "claim returned no items"
    return items


def _patch_tier(monkeypatch):
    tier = MagicMock()
    tier.supports_video = True
    tier.supports_batch = True
    monkeypatch.setattr(
        "scripts.enrichment_worker.GeminiTierConfig.detect", classmethod(lambda cls: tier)
    )


def test_lookup_inline_images_returns_bytes_without_upload(tmp_path):
    """(B) A cached small image on the batch path becomes inline bytes — the
    File API client is never touched."""
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    gemini = GeminiResource(api_key="test-key")

    url = "https://cdn.example.com/pic.jpg"
    local = tmp_path / "pic.jpg"
    local.write_bytes(b"fake-image-bytes")
    _seed_byte_cache(ops, url, local)

    client, files = _make_fake_client([])
    with patch("google.genai.Client", return_value=client):
        result = lookup_or_upload_all(ops, gemini, json.dumps([url]), inline_images=True)

    files.upload.assert_not_called()
    files.get.assert_not_called()
    assert result[0]["inline_data"] == b"fake-image-bytes"
    assert result[0]["mime_type"] == "image/jpeg"
    assert "uri" not in result[0]


def test_lookup_inline_images_routes_video_to_file_api(tmp_path):
    """(B) Video is never inlined — it goes through the File API upload."""
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    gemini = GeminiResource(api_key="test-key")

    url = "https://cdn.example.com/clip.mp4"
    local = tmp_path / "clip.mp4"
    local.write_bytes(b"fake-video-bytes")
    _seed_byte_cache(ops, url, local, content_type="video/mp4")

    uri = "https://generativelanguage.googleapis.com/v1beta/files/vid1"
    client, files = _make_fake_client([uri])
    with patch("google.genai.Client", return_value=client):
        result = lookup_or_upload_all(ops, gemini, json.dumps([url]), inline_images=True)

    files.upload.assert_called_once()
    assert result[0]["uri"] == uri
    assert "inline_data" not in result[0]


def test_lookup_inline_images_routes_oversize_to_file_api(tmp_path, monkeypatch):
    """(B) An image over the inline size limit falls back to the File API."""
    monkeypatch.setattr(media_cache, "INLINE_MEDIA_LIMIT_BYTES", 4)

    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    gemini = GeminiResource(api_key="test-key")

    url = "https://cdn.example.com/big.jpg"
    local = tmp_path / "big.jpg"
    local.write_bytes(b"way-more-than-four-bytes")
    _seed_byte_cache(ops, url, local)

    uri = "https://generativelanguage.googleapis.com/v1beta/files/big1"
    client, files = _make_fake_client([uri])
    with patch("google.genai.Client", return_value=client):
        result = lookup_or_upload_all(ops, gemini, json.dumps([url]), inline_images=True)

    files.upload.assert_called_once()
    assert result[0]["uri"] == uri


def test_build_contents_serializes_inline_and_uri_parts():
    """(B) Request serialization carries inline_data parts (base64 bytes via
    the SDK) alongside File API URI parts."""
    from google.genai import types

    media = [
        {"inline_data": b"img-bytes", "mime_type": "image/jpeg", "duration_seconds": None},
        {"uri": "https://generativelanguage.googleapis.com/v1beta/files/vid1",
         "mime_type": "video/mp4", "duration_seconds": 30.0},
    ]
    parts = _build_contents("describe", media)
    assert isinstance(parts, list)
    assert isinstance(parts[0], types.Part)
    assert parts[0].inline_data.data == b"img-bytes"
    assert parts[0].inline_data.mime_type == "image/jpeg"
    assert parts[1].file_data.file_uri == media[1]["uri"]
    assert parts[-1].text == "describe"

    # And the full InlinedRequest shape accepted by batches.create:
    req = {"custom_key": "k1", "prompt": "describe", "media_files": media}
    inlined = _to_inlined_request(req)
    assert inlined.contents[0].inline_data.data == b"img-bytes"


def test_batch_request_building_inline_image_end_to_end(batch_env, tmp_path, monkeypatch):
    """(B) build_requests_for_items for an image-only post yields inline_data
    with no File API upload; the interactive path is untouched."""
    ops, duckdb = batch_env
    _patch_tier(monkeypatch)

    url = "https://cdn.example.com/pic.jpg"
    local = tmp_path / "pic.jpg"
    local.write_bytes(b"fake-image-bytes")
    _seed_byte_cache(ops, url, local)

    client, files = _make_fake_client([])
    _add_silver_post(duckdb, "p_img", json.dumps([url]))
    items = _claim(ops, "p_img")
    with patch("google.genai.Client", return_value=client):
        requests = build_requests_for_items(ops, duckdb, GeminiResource(api_key="k"), items)

    files.upload.assert_not_called()
    req = requests[0]
    assert req["media_files"][0]["inline_data"] == b"fake-image-bytes"

    # The built InlinedRequest really carries an inline_data part.
    inlined = _to_inlined_request(req)
    assert inlined.contents[0].inline_data.mime_type == "image/jpeg"


def test_batch_request_building_video_end_to_end(batch_env, tmp_path, monkeypatch):
    """(B) A video-only post on the batch path still resolves to a File API URI."""
    ops, duckdb = batch_env
    _patch_tier(monkeypatch)

    url = "https://cdn.example.com/clip.mp4"
    local = tmp_path / "clip.mp4"
    local.write_bytes(b"fake-video-bytes")
    _seed_byte_cache(ops, url, local, content_type="video/mp4")

    uri = "https://generativelanguage.googleapis.com/v1beta/files/vid1"
    client, files = _make_fake_client([uri])
    f = MagicMock()
    f.uri = uri
    f.mime_type = "video/mp4"
    f.size_bytes = 123
    f.state.name = "ACTIVE"
    f.video_metadata.duration_seconds = 30.0
    files.get.side_effect = None
    files.get.return_value = f
    _add_silver_post(duckdb, "p_vid", json.dumps([url]))
    items = _claim(ops, "p_vid")
    with patch("google.genai.Client", return_value=client):
        requests = build_requests_for_items(ops, duckdb, GeminiResource(api_key="k"), items)

    files.upload.assert_called_once()
    assert requests[0]["media_files"][0]["uri"] == uri
    assert requests[0]["media_files"][0]["duration_seconds"] == 30.0
