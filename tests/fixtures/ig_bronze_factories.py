"""Bronze-layer factory helpers for tests.

Produces synthetic rows matching the real Apify Instagram scraper schema
(37 columns, correct types, correct camelCase column names). The full schema
is loaded from a real bronze Parquet file at module-import time; synthetic
rows are cast to match it exactly so that no test silently diverges from
what production ingests.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

# ── Load real schema once at module level ────────────────────────────────────
_LAKE_BRONZE_DIR = Path("data/lake/bronze")
_REAL_FILES = sorted(_LAKE_BRONZE_DIR.glob("*.parquet"))
if _REAL_FILES:
    _BRONZE_SCHEMA = pl.read_parquet(_REAL_FILES[0]).clear().schema
else:
    _BRONZE_SCHEMA = None  # pragma: no cover — CI always has real data


def make_ig_bronze_row(
    post_id: str | None,
    shortcode: str,
    caption: str,
    username: str,
    owner_id: str = "12345",
    likes: int = 10,
    comments: int = 2,
    hashtags: list[str] | None = None,
    timestamp: str | None = "2024-01-01T00:00:00.000Z",
) -> dict:
    """Create a single synthetic row matching the real Apify bronze schema.

    All 37 columns are present. Nested struct/list columns default to empty
    lists or None.  The caller only sets the meaningful fields; the rest are
    filled with sensible production-like defaults.

    The dict is intentionally untyped (values are whatever Polars can infer);
    ``write_bronze`` casts the final DataFrame to the real schema so that every
    Parquet file is bit-for-bit compatible with production.
    """
    return {
        # ── Core identity ──
        "id": post_id,
        "shortCode": shortcode,
        "url": f"https://www.instagram.com/p/{shortcode}/",
        "inputUrl": f"https://www.instagram.com/{username}/",
        # ── Content ──
        "caption": caption,
        "type": "Video",
        "productType": "feed",
        "firstComment": "",
        "alt": None,
        # ── Owner ──
        "ownerId": owner_id,
        "ownerUsername": username,
        "ownerFullName": f"{username} Full Name",
        # ── Engagement ──
        "likesCount": likes,
        "commentsCount": comments,
        "videoViewCount": 0,
        "videoPlayCount": 0,
        # ── Media ──
        "displayUrl": f"https://example.com/{shortcode}.jpg",
        "dimensionsHeight": 1080,
        "dimensionsWidth": 1080,
        "images": [],
        "audioUrl": None,
        "videoUrl": None,
        # ── Lists / nested ──
        "hashtags": hashtags or [],
        "mentions": [],
        "taggedUsers": [],
        "latestComments": [],
        "childPosts": [],
        "coauthorProducers": [],
        "requestErrorMessages": [],
        # ── Metadata ──
        "timestamp": timestamp,
        "username": username,
        "isCommentsDisabled": False,
        "isPinned": False,
        "locationId": None,
        "locationName": None,
        # ── Error fields (success path → None) ──
        "error": None,
        "errorDescription": None,
    }


def write_ig_bronze(path, rows: list[dict]) -> None:
    """Write a bronze Parquet file whose schema exactly matches production.

    Empty *rows* is allowed (writes a 0-row file with the correct schema).
    Every Parquet file produced by this helper is safe to feed into the
    silver asset — column names and types are identical to what Apify emits.
    """
    df = pl.DataFrame(rows)

    if _BRONZE_SCHEMA is not None:
        # Cast every column to the real production type. Columns present in
        # the schema but missing in ``df`` become all-null with the correct
        # type. Extra columns in ``df`` (e.g. test scaffolding) are dropped.
        cast_cols = []
        for col, dtype in _BRONZE_SCHEMA.items():
            if col in df.columns:
                cast_cols.append(pl.col(col).cast(dtype))
            else:
                cast_cols.append(pl.lit(None).cast(dtype).alias(col))
        df = df.select(cast_cols)

    df.write_parquet(path)
