"""Tests for the local ad-hoc bronze producer (``ig_posts_local_raw``).

Covers the second bronze producer contract: write-once Parquet + ``.meta``
sidecar (``results_type="posts"``), media-cache seeding from local bytes
(no CDN re-download), nullable-media posts, the ``-1`` ad-hoc depth sentinel,
and silver's cross-producer dedup (``local_*`` vs Apify ``source_dataset``).
"""

from __future__ import annotations

import json
from unittest.mock import patch

import polars as pl
import pytest
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

import importlib
ig_assets = importlib.import_module("datalake.defs.instagram.assets")
media_cache_mod = importlib.import_module("datalake.defs.enrichment.media_cache")
from datalake.defs.enrichment.media_cache import seed_media_from_file, url_hash
from datalake.defs.instagram.assets import ig_posts_local_raw, ig_posts_slv
from datalake.defs.instagram.config import LOCAL_INGEST_DIR
from datalake.defs.instagram.creators import (
    AD_HOC_LIMIT,
    add_profile,
    create_creator,
    edit_depth,
    enabled_profiles,
    is_ad_hoc,
)
from tests.fixtures.ig_bronze_factories import make_ig_bronze_row, write_ig_bronze


# ── Fixtures / helpers ──────────────────────────────────────────────────────


def _post(
    post_id: str,
    shortcode: str,
    *,
    owner: str = "u1",
    video: str | None = None,
    images: list[str] | None = None,
    display: str | None = "https://cdn.example.com/display.jpg",
    likes: int = 5,
) -> dict:
    d = {
        "id": post_id,
        "type": "Video" if video else ("Sidecar" if images else "Image"),
        "shortCode": shortcode,
        "caption": f"caption {shortcode}",
        "ownerUsername": owner,
        "likesCount": likes,
        "commentsCount": 1,
        "url": f"https://www.instagram.com/p/{shortcode}/",
        "hashtags": ["tag"],
        "timestamp": "2026-01-01T00:00:00.000Z",
    }
    if video:
        d["videoUrl"] = video
    if images is not None:
        d["images"] = images
    if display:
        d["displayUrl"] = display
    return d


def _make_dataset(root, ds_id: str, posts: list[dict]) -> None:
    """Lay out ``<root>/<ds_id>/<post_id>/post_metadata.json`` + media files."""
    ds_dir = root / ds_id
    for post in posts:
        pd = ds_dir / post["id"]
        pd.mkdir(parents=True)
        (pd / "post_metadata.json").write_text(json.dumps(post), encoding="utf-8")
        if post.get("videoUrl"):
            (pd / "video.mp4").write_bytes(b"video-bytes-" + post["id"].encode())
        for i in range(len(post.get("images") or [])):
            (pd / f"media_{i:02d}.jpg").write_bytes(
                b"img-bytes-" + post["id"].encode() + f"-{i}".encode()
            )
        if not post.get("images") and post.get("displayUrl") and not post.get("videoUrl"):
            (pd / "media_00.jpg").write_bytes(b"img-bytes-" + post["id"].encode())


@pytest.fixture
def local_env(tmp_path, monkeypatch):
    """Redirect bronze lake, local ingest dir, and POST_MEDIA_DIR to tmp_path."""
    bronze = tmp_path / "bronze"
    ingest = tmp_path / "ingest"
    media = tmp_path / "media" / "posts"
    bronze.mkdir()
    ingest.mkdir()
    media.mkdir(parents=True)
    # NOTE: datalake.defs.__init__ re-exports shadow the submodule attributes,
    # so string-based monkeypatch paths fail — patch module objects directly.
    monkeypatch.setattr(ig_assets, "BRONZE_LAKE", bronze)
    monkeypatch.setattr(ig_assets, "bronze_path", lambda dataset_id: bronze / f"{dataset_id}.parquet")
    monkeypatch.setattr(ig_assets, "LOCAL_INGEST_DIR", ingest)
    monkeypatch.setattr(media_cache_mod, "POST_MEDIA_DIR", media)
    return type("Env", (), {"bronze": bronze, "ingest": ingest, "media": media})()


def _run_local(ops):
    return ig_posts_local_raw(build_asset_context(resources={"ops": ops}))


# ── Bronze materialization ──────────────────────────────────────────────────


def test_local_producer_materializes_datasets(local_env, ops):
    """GIVEN two local datasets
    WHEN ig_posts_local_raw runs
    THEN one local_<id>.parquet + .meta each, results_type='posts', all rows.
    """
    _make_dataset(
        local_env.ingest,
        "aaa",
        [
            _post("p1", "sc1", video="https://cdn.example.com/v1.mp4"),
            _post("p2", "sc2", images=["https://cdn.example.com/i1", "https://cdn.example.com/i2"]),
        ],
    )
    _make_dataset(local_env.ingest, "bbb", [_post("p3", "sc3", owner="u2")])

    result = _run_local(ops)

    assert isinstance(result, pl.DataFrame)
    assert len(result) == 3
    assert set(result["shortCode"].to_list()) == {"sc1", "sc2", "sc3"}

    for ds, n, owner in (("aaa", 2, "u1"), ("bbb", 1, "u2")):
        dest = local_env.bronze / f"local_{ds}.parquet"
        meta_path = dest.with_suffix(".parquet.meta")
        assert dest.exists()
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        assert meta["dataset_id"] == f"local_{ds}"
        assert meta["run_id"] == "local-adhoc"
        assert meta["actor"] == "local-disk"
        assert meta["item_count"] == n
        assert meta["input"]["results_type"] == "posts"
        assert meta["input"]["results_limit"] == AD_HOC_LIMIT
        assert meta["input"]["urls"] == [f"https://www.instagram.com/{owner}/"]
    # ndjson scratch files cleaned up
    assert not list(local_env.bronze.glob("*.jsonl"))


def test_rerun_is_write_once_noop(local_env, ops):
    """GIVEN bronze files already written
    WHEN ig_posts_local_raw runs again
    THEN parquet AND meta bytes are untouched (silver mtime watermark safety).
    """
    _make_dataset(local_env.ingest, "aaa", [_post("p1", "sc1", video="https://cdn.example.com/v1.mp4")])
    _run_local(ops)

    files = list(local_env.bronze.glob("local_*"))
    before = {f: (f.stat().st_mtime_ns, f.read_bytes()) for f in files}

    _run_local(ops)

    for f in files:
        mtime, content = before[f]
        assert f.stat().st_mtime_ns == mtime
        assert f.read_bytes() == content


# ── Media seeding ───────────────────────────────────────────────────────────


def test_media_seeded_from_local_bytes(local_env, ops):
    """GIVEN posts with video + carousel images
    WHEN ig_posts_local_raw runs
    THEN media_cache rows keyed by sha256(url) point at copied local bytes.
    """
    video_url = "https://cdn.example.com/v1.mp4"
    img0, img1 = "https://cdn.example.com/i0", "https://cdn.example.com/i1"
    _make_dataset(
        local_env.ingest,
        "aaa",
        [
            _post("p1", "sc1", video=video_url),
            _post("p2", "sc2", images=[img0, img1]),
        ],
    )

    _run_local(ops)

    expected = [
        (video_url, b"video-bytes-p1", "video/mp4"),
        (img0, b"img-bytes-p2-0", "image/jpeg"),
        (img1, b"img-bytes-p2-1", "image/jpeg"),
    ]
    conn = ops.get_connection()
    try:
        for url, content, ctype in expected:
            row = conn.execute(
                "SELECT local_path, content_type, source_url FROM media_cache WHERE cache_key = ?",
                [url_hash(url)],
            ).fetchone()
            assert row is not None, url
            assert row["content_type"] == ctype
            assert row["source_url"] == url
            # self-contained: bytes copied into POST_MEDIA_DIR, not referenced
            assert str(local_env.media) in row["local_path"]
            assert open(row["local_path"], "rb").read() == content
    finally:
        conn.close()


def test_video_post_display_url_not_seeded(local_env, ops):
    """GIVEN a video post (videoUrl + displayUrl, no images list)
    WHEN ig_posts_local_raw runs
    THEN only video.mp4 is seeded — the displayUrl (poster frame) is NOT
    mapped to a non-existent media_00.jpg (regression: real video posts
    carried displayUrl and logged spurious 'source missing' for media_00.jpg).
    """
    video_url = "https://cdn.example.com/v1.mp4"
    display_url = "https://cdn.example.com/poster.jpg"
    _make_dataset(
        local_env.ingest,
        "aaa",
        [_post("p1", "sc1", video=video_url, display=display_url)],
    )

    _run_local(ops)

    conn = ops.get_connection()
    try:
        # video seeded
        assert conn.execute(
            "SELECT 1 FROM media_cache WHERE cache_key = ?",
            [url_hash(video_url)],
        ).fetchone() is not None
        # displayUrl must NOT be in the cache (no media_00.jpg exists for a video)
        assert conn.execute(
            "SELECT 1 FROM media_cache WHERE cache_key = ?",
            [url_hash(display_url)],
        ).fetchone() is None
    finally:
        conn.close()


def test_media_seeding_idempotent_across_reruns(local_env, ops):
    """GIVEN a new dataset reusing an already-seeded media URL
    WHEN ig_posts_local_raw runs again
    THEN the media_cache row is neither duplicated nor rewritten.
    """
    video_url = "https://cdn.example.com/v1.mp4"
    _make_dataset(local_env.ingest, "aaa", [_post("p1", "sc1", video=video_url)])
    _run_local(ops)

    conn = ops.get_connection()
    try:
        row = conn.execute(
            "SELECT local_path, fetched_at FROM media_cache WHERE cache_key = ?",
            [url_hash(video_url)],
        ).fetchone()
    finally:
        conn.close()

    # same URL reappears in a second dataset
    _make_dataset(local_env.ingest, "bbb", [_post("p9", "sc9", video=video_url)])
    _run_local(ops)

    conn = ops.get_connection()
    try:
        rows = conn.execute(
            "SELECT local_path, fetched_at FROM media_cache WHERE cache_key = ?",
            [url_hash(video_url)],
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) == 1
    assert rows[0]["fetched_at"] == row["fetched_at"]
    assert rows[0]["local_path"] == row["local_path"]

    # direct re-seed is also a no-op
    assert seed_media_from_file(ops, video_url, local_env.ingest / "aaa" / "p1" / "video.mp4")


def test_no_media_posts_skip_cleanly(local_env, ops):
    """GIVEN posts without any media fields (the 52 no-media posts)
    WHEN ig_posts_local_raw runs
    THEN they land in bronze and seeding neither errors nor writes rows for them.
    """
    post = _post("p1", "sc1")
    post.pop("displayUrl", None)
    post.pop("images", None)
    _make_dataset(local_env.ingest, "aaa", [post])
    _make_dataset(local_env.ingest, "bbb", [_post("p2", "sc2", video="https://cdn.example.com/v.mp4")])

    result = _run_local(ops)

    assert len(result) == 2
    conn = ops.get_connection()
    try:
        rows = conn.execute("SELECT cache_key, source_url FROM media_cache").fetchall()
    finally:
        conn.close()
    assert len(rows) == 1  # only the video post seeded
    assert rows[0]["source_url"] == "https://cdn.example.com/v.mp4"


# ── Ad-hoc sentinel ─────────────────────────────────────────────────────────


def test_ad_hoc_sentinel_accepted_and_rejected(ops):
    """GIVEN the -1 ad-hoc sentinel
    WHEN add_profile / edit_depth use it
    THEN -1 is accepted (already-ingested), 0/-2 stay rejected, 1..N unchanged.
    """
    creator = create_creator(ops, "Jane")
    row = add_profile(
        ops, creator_id=creator["id"], platform="instagram", handle="jane",
        results_limit=AD_HOC_LIMIT,
    )
    assert row["results_limit"] == AD_HOC_LIMIT
    assert is_ad_hoc(row["results_limit"])
    updated = edit_depth(ops, platform="instagram", handle="jane", results_limit=AD_HOC_LIMIT)
    assert updated["results_limit"] == AD_HOC_LIMIT

    with pytest.raises(ValueError):
        add_profile(ops, creator_id=creator["id"], platform="instagram", handle="x", results_limit=0)
    with pytest.raises(ValueError):
        edit_depth(ops, platform="instagram", handle="jane", results_limit=-2)
    # continuous depths still work
    assert edit_depth(ops, platform="instagram", handle="jane", results_limit=3)["results_limit"] == 3


def test_enabled_profiles_excludes_ad_hoc(ops):
    """GIVEN an ad-hoc profile alongside a continuous one
    WHEN enabled_profiles runs
    THEN the ad-hoc profile is not scheduled for continuous scraping.
    """
    creator = create_creator(ops, "Jane")
    add_profile(ops, creator_id=creator["id"], platform="instagram", handle="adhoc", results_limit=AD_HOC_LIMIT)
    add_profile(ops, creator_id=creator["id"], platform="instagram", handle="core", results_limit=12)

    handles = [p["handle"] for p in enabled_profiles(ops)]
    assert handles == ["core"]

    full = {p["handle"]: p["results_limit"] for p in enabled_profiles(ops, include_ad_hoc=True)}
    assert full == {"adhoc": AD_HOC_LIMIT, "core": 12}


def test_missing_ingest_dir_returns_empty(local_env, monkeypatch):
    """GIVEN the configured ingest dir absent
    WHEN ig_posts_local_raw runs
    THEN it degrades to an empty DataFrame (no error).
    """
    monkeypatch.setattr(ig_assets, "LOCAL_INGEST_DIR", local_env.ingest / "nope")
    assert _run_local(ops=_DummyOps()).is_empty()


class _DummyOps:
    """Never touched on the empty path — guards against eager connection use."""


# ── Silver cross-producer dedup ─────────────────────────────────────────────


def test_silver_dedup_prefers_newer_scrape_across_producers(tmp_path, ops):
    """GIVEN the same post in an Apify bronze file and a local_ bronze file
    WHEN ig_posts_slv dedups
    THEN the NEWER scrape wins regardless of producer (source_dataset reflects it).
    """
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    apify = [make_ig_bronze_row("1", "abc", "Old", "u1", likes=10)]
    write_ig_bronze(tmp_path / "BxcAvPURKHDxFWzTs.parquet", apify)
    (tmp_path / "BxcAvPURKHDxFWzTs.parquet.meta").write_text(
        json.dumps({"downloaded_at": "2026-01-01T00:00:00+00:00"}), encoding="utf-8"
    )
    local = [make_ig_bronze_row("1", "abc", "LocalNew", "u1", likes=99)]
    write_ig_bronze(tmp_path / "local_BxcAvPURKHDxFWzTs.parquet", local)
    (tmp_path / "local_BxcAvPURKHDxFWzTs.parquet.meta").write_text(
        json.dumps({"downloaded_at": "2026-06-01T00:00:00+00:00"}), encoding="utf-8"
    )

    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        with patch("datalake.defs.instagram.assets.cache_media_bytes", lambda *a, **k: None):
            result = ig_posts_slv(
                build_asset_context(resources={"duckdb": duckdb, "ops": ops})
            )

    assert len(result) == 1
    assert result["likes_count"][0] == 99
    assert result["source_dataset"][0] == "local_BxcAvPURKHDxFWzTs"


def test_local_ingest_dir_constant_default():
    """The knob defaults to the scrape-ig-saved-list checkout and is env-overridable."""
    assert LOCAL_INGEST_DIR.name == "ingest"
    assert "scrape-ig-saved-list" in str(LOCAL_INGEST_DIR)
