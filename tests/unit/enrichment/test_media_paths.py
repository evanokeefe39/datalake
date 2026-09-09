"""Unit tests for the thin media-path resolver.

media_urls_to_local_paths must only translate cached URLs to existing
absolute local paths (images AND videos), never run ffmpeg, and never
raise on a cache miss.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from datalake.defs.enrichment.media_paths import (
    is_video_path,
    media_urls_to_local_paths,
)


@pytest.fixture
def media_files(tmp_path):
    img = tmp_path / "cached.jpg"
    img.write_bytes(b"jpg-bytes")
    vid = tmp_path / "cached.mp4"
    vid.write_bytes(b"mp4-bytes")
    return img, vid, tmp_path


def _patch_cache(mapping):
    return patch(
        "datalake.defs.enrichment.media_paths.cached_local_path",
        side_effect=lambda ops, url: mapping.get(url),
    )


def test_native_image_passthrough(media_files):
    img, _, _ = media_files
    with _patch_cache({"https://cdn/a.jpg": str(img)}):
        out = media_urls_to_local_paths(None, ["https://cdn/a.jpg"])
    assert out == [str(img.resolve())]


def test_video_passthrough_unmodified(media_files):
    _, vid, _ = media_files
    with _patch_cache({"https://cdn/b.mp4": str(vid)}):
        out = media_urls_to_local_paths(None, ["https://cdn/b.mp4"])
    # Video path is returned as-is — framing happens in qwen-batch, not here.
    assert out == [str(vid.resolve())]
    assert out[0].endswith(".mp4")


def test_missing_cache_skip_never_raises(media_files):
    img, _, _ = media_files
    with _patch_cache({"https://cdn/a.jpg": str(img), "https://cdn/gone.jpg": None}):
        out = media_urls_to_local_paths(
            None, ["https://cdn/gone.jpg", "https://cdn/a.jpg"]
        )
    assert out == [str(img.resolve())]


def test_include_video_false_skips_videos(media_files):
    img, vid, _ = media_files
    with _patch_cache(
        {"https://cdn/a.jpg": str(img), "https://cdn/b.mp4": str(vid)}
    ):
        out = media_urls_to_local_paths(
            None,
            ["https://cdn/b.mp4", "https://cdn/a.jpg"],
            include_video=False,
        )
    assert out == [str(img.resolve())]


def test_nonexistent_file_skipped(media_files):
    _, _, tmp_path = media_files
    ghost = str(tmp_path / "not-on-disk.jpg")
    with _patch_cache({"https://cdn/x.jpg": ghost}):
        out = media_urls_to_local_paths(None, ["https://cdn/x.jpg"])
    assert out == []


def test_deterministic_order_and_dedup(media_files):
    img, vid, _ = media_files
    with _patch_cache(
        {"https://cdn/b.mp4": str(vid), "https://cdn/a.jpg": str(img)}
    ):
        out = media_urls_to_local_paths(
            None, ["https://cdn/b.mp4", "https://cdn/a.jpg", "https://cdn/b.mp4"]
        )
    assert out == [str(vid.resolve()), str(img.resolve())]


def test_accepts_json_string_input(media_files):
    img, _, _ = media_files
    with _patch_cache({"https://cdn/a.jpg": str(img)}):
        out = media_urls_to_local_paths(
            None, '["https://cdn/a.jpg"]'
        )
    assert out == [str(img.resolve())]


def test_is_video_path():
    assert is_video_path("x.MP4") is True
    assert is_video_path("y.mov") is True
    assert is_video_path("y.webm") is True
    assert is_video_path("y.m4v") is True
    assert is_video_path("y.jpg") is False
    assert is_video_path("y.png") is False
    assert is_video_path("y.webp") is False


def test_malformed_json_string_returns_empty_and_warns(caplog):
    """Malformed machine-written media_files_json is logged and skipped."""
    import logging

    with caplog.at_level(logging.WARNING, logger="enrichment.media_paths"):
        out = media_urls_to_local_paths(None, "{not json")
    assert out == []
    assert "not valid JSON" in caplog.text
