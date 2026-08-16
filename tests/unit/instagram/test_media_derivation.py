"""Unit tests for ``_derive_media`` — bronze media columns → media_files JSON.

Media precedence per post: a ``videoUrl`` (rich media) wins; otherwise the
carousel ``images`` list; otherwise the single ``displayUrl`` image.
"""

from __future__ import annotations

import json

import polars as pl

from datalake.defs.instagram.assets import _derive_media


def _derive(df: pl.DataFrame) -> tuple[str, int]:
    out = _derive_media(df)
    return out["media_files"][0], out["media_count"][0]


def test_video_url_wins_over_images_and_display():
    df = pl.DataFrame(
        {
            "videoUrl": ["https://cdn/v.mp4"],
            "images": [["https://cdn/a.jpg", "https://cdn/b.jpg"]],
            "displayUrl": ["https://cdn/thumb.jpg"],
        }
    )
    media_files, count = _derive(df)
    assert json.loads(media_files) == ["https://cdn/v.mp4"]
    assert count == 1


def test_carousel_images_wins_over_display():
    df = pl.DataFrame(
        {
            "videoUrl": [None],
            "images": [["https://cdn/a.jpg", "https://cdn/b.jpg"]],
            "displayUrl": ["https://cdn/first.jpg"],
        }
    )
    media_files, count = _derive(df)
    assert json.loads(media_files) == ["https://cdn/a.jpg", "https://cdn/b.jpg"]
    assert count == 2


def test_empty_images_falls_back_to_display():
    df = pl.DataFrame(
        {
            "videoUrl": [None],
            "images": [[]],
            "displayUrl": ["https://cdn/single.jpg"],
        }
    )
    media_files, count = _derive(df)
    assert json.loads(media_files) == ["https://cdn/single.jpg"]
    assert count == 1


def test_no_media_columns_yields_empty():
    df = pl.DataFrame({"caption": ["text only"]})
    media_files, count = _derive(df)
    assert json.loads(media_files) == []
    assert count == 0


def test_null_media_yields_empty():
    df = pl.DataFrame(
        {
            "videoUrl": [None],
            "images": [None],
            "displayUrl": [None],
        }
    )
    media_files, count = _derive(df)
    assert json.loads(media_files) == []
    assert count == 0
