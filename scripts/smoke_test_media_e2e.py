"""External Integration Gate smoke test — media end-to-end.

Proves 1 real image + 1 real video flow bronze → silver → worker → Gemini,
with media bytes cached at scrape time (so the worker never depends on CDN
URLs that expire in ~4-5 days).

Run directly (uses the real GEMINI_API_KEY from .env; makes 2 real Gemini
File API uploads + 2 real analyze calls):

    uv run python scripts/smoke_test_media_e2e.py

Uses temporary DuckDB/SQLite/bronze dirs — zero interference with production
state. Exits non-zero on any gate failure.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import polars as pl
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Real, stable, CC0/public-domain sample media (verified accessible 2026-08-14).
IMAGE_URL = "https://www.gstatic.com/webp/gallery/1.jpg"
VIDEO_URL = "https://interactive-examples.mdn.mozilla.net/media/cc0-videos/flower.mp4"

# Video enrichment requires Tier 1+ (FREE tier skips video in the worker gate).
os.environ["GEMINI_TIER"] = os.environ.get("GEMINI_TIER", "tier1")

from dagster import build_asset_context  # noqa: E402
from dagster_duckdb import DuckDBResource  # noqa: E402

from datalake.defs.common.resources import GeminiResource, SQLiteResource  # noqa: E402
from datalake.defs.instagram.assets import ig_posts_slv  # noqa: E402


def _check(label: str, ok: bool, detail: str = "") -> None:
    mark = "PASS" if ok else "FAIL"
    print(f"[{mark}] {label}" + (f" — {detail}" if detail else ""))
    if not ok:
        sys.exit(1)


def main() -> None:
    if not os.environ.get("GEMINI_API_KEY"):
        _check("GEMINI_API_KEY set", False, "missing from .env — cannot call Gemini")
    print(f"Image URL: {IMAGE_URL}")
    print(f"Video URL: {VIDEO_URL}")

    tmp = Path(tempfile.mkdtemp(prefix="media_e2e_"))
    bronze_dir = tmp / "bronze"
    bronze_dir.mkdir()
    duckdb = DuckDBResource(database=str(tmp / "state.duckdb"))
    ops = SQLiteResource(database=str(tmp / "ops.sqlite"))
    gemini = GeminiResource()

    # ── Gate 1: bronze → silver (media wired + bytes cached at scrape time) ──
    def _post_row(post_id, shortcode, caption, *, video_url, display_url):
        return {
            "id": post_id,
            "shortCode": shortcode,
            "caption": caption,
            "username": "smoke_user",
            "ownerUsername": "smoke_user",
            "ownerId": post_id,
            "likesCount": 10,
            "commentsCount": 2,
            "videoViewCount": 0,
            "videoPlayCount": 0,
            "url": f"https://www.instagram.com/p/{shortcode}/",
            "hashtags": [],
            "videoUrl": video_url,
            "displayUrl": display_url,
            "images": [],
            "timestamp": "2026-08-14T00:00:00.000Z",
        }

    rows = [
        _post_row(
            "img1", "img1sc", "A photo of a red flower", video_url=None, display_url=IMAGE_URL
        ),
        _post_row(
            "vid1", "vid1sc", "A short clip of a flower", video_url=VIDEO_URL, display_url=None
        ),
    ]
    pl.DataFrame(rows).write_parquet(bronze_dir / "smoke.parquet")

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", bronze_dir), patch(
        "datalake.defs.enrichment.media_cache.POST_MEDIA_DIR", tmp / "media"
    ):
        ctx = build_asset_context(resources={"duckdb": duckdb, "ops": ops})
        silver = ig_posts_slv(ctx)

    by_id = {row["post_id"]: row for row in silver.iter_rows(named=True)}
    _check(
        "image post mapped to media_files",
        json.loads(by_id["img1"]["media_files"]) == [IMAGE_URL]
        and by_id["img1"]["media_count"] == 1,
        by_id["img1"]["media_files"],
    )
    _check(
        "video post mapped to media_files",
        json.loads(by_id["vid1"]["media_files"]) == [VIDEO_URL]
        and by_id["vid1"]["media_count"] == 1,
        by_id["vid1"]["media_files"],
    )

    # Byte cache: both URLs must have local files recorded at scrape time.
    conn = ops.get_connection()
    try:
        rows = conn.execute(
            "SELECT source_url, local_path FROM media_cache ORDER BY source_url"
        ).fetchall()
    finally:
        conn.close()
    cached = {r["source_url"]: r["local_path"] for r in rows}
    _check(
        "image bytes cached at scrape time",
        IMAGE_URL in cached and Path(cached[IMAGE_URL]).exists(),
        cached.get(IMAGE_URL, "<missing>"),
    )
    _check(
        "video bytes cached at scrape time",
        VIDEO_URL in cached and Path(cached[VIDEO_URL]).exists(),
        cached.get(VIDEO_URL, "<missing>"),
    )

    # ── Gate 2: worker → Gemini (cached bytes → File API → multimodal) ──────
    from datalake.defs.enrichment.assets import ensure_gold_analyses
    from datalake.defs.enrichment.batch import (
        claim_batch,
        claim_pending_items,
        create_batch,
    )
    from datalake.defs.enrichment.media_cache import lookup_or_upload_all
    from scripts.enrichment_worker import process_item

    ensure_gold_analyses(duckdb)

    create_batch(
        ops,
        [
            json.dumps({"post_id": "img1", "domain": "instagram"}),
            json.dumps({"post_id": "vid1", "domain": "instagram"}),
        ],
    )
    claim_batch(ops)
    items = {json.loads(i["payload"])["post_id"]: i for i in claim_pending_items(ops, 1, limit=10)}

    # Prove File API uploads resolve from cached bytes (not the live CDN).
    for post_id in ("img1", "vid1"):
        mf_json = by_id[post_id]["media_files"]
        uploaded = lookup_or_upload_all(ops, gemini, mf_json)
        _check(
            f"{post_id} uploaded to Gemini File API from cache",
            bool(uploaded) and uploaded[0].get("uri", "").startswith("https://"),
            f"{len(uploaded)} file(s), mime={uploaded[0]['mime_type'] if uploaded else 'n/a'}",
        )

    # Real Gemini analyze calls (multimodal for both posts).
    for post_id in ("img1", "vid1"):
        ok = process_item(ops, duckdb, gemini, items[post_id])
        _check(f"{post_id} analyzed by Gemini", ok)

    with duckdb.get_connection() as conn:
        gold = conn.execute(
            "SELECT post_id, result_json FROM gold_analyses ORDER BY post_id"
        ).fetchall()
    _check("gold_analyses has both posts", {r[0] for r in gold} == {"img1", "vid1"})
    for post_id, result_json in gold:
        try:
            parsed = json.loads(result_json)
        except json.JSONDecodeError:
            _check(f"{post_id} gold result is valid JSON", False, result_json[:120])
        _check(f"{post_id} gold result is valid JSON", isinstance(parsed, dict))

    print(f"\nAll gates passed. Scratch state at {tmp}")


if __name__ == "__main__":
    main()
