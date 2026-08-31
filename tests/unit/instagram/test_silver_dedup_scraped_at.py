"""US-S1 — silver dedup ordered by per-file scraped_at (newest scrape wins).

The old ORDER BY used ``timestamp`` (publication time — constant per post)
then ``source_dataset DESC`` (a random id), so on a re-scrape the newer
likes_count won only ~2/3 of the time. The fix orders by ``scraped_at``
(meta.downloaded_at → file mtime), which is TRANSIENT: it must never become
a silver_ig_posts column.
"""

from __future__ import annotations

import json
from unittest.mock import patch

from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.instagram.assets import ig_posts_slv
from tests.fixtures.ig_bronze_factories import make_ig_bronze_row, write_ig_bronze


def _write_meta(path, downloaded_at: str) -> None:
    path.with_suffix(".parquet.meta").write_text(
        json.dumps({"downloaded_at": downloaded_at}), encoding="utf-8"
    )


def _run(tmp_path, ops):
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb, "ops": ops})
        return ig_posts_slv(context)


def test_newest_scrape_wins_in_silver(tmp_path, ops):
    """Same post in two datasets with IDENTICAL publication timestamps:
    the dataset scraped LATER must win, regardless of source_dataset order.

    Regression guard: the old source_dataset DESC tie-break would pick
    ds_009 (the older scrape) here.
    """
    # Older scrape in a lexicographically LARGER dataset id...
    ds_old = [make_ig_bronze_row("1", "abc", "Old", "u1", likes=10)]
    write_ig_bronze(tmp_path / "ds_009.parquet", ds_old)
    _write_meta(tmp_path / "ds_009.parquet", "2026-01-01T00:00:00+00:00")
    # ...newer scrape in a lexicographically SMALLER dataset id.
    ds_new = [make_ig_bronze_row("1", "abc", "New", "u1", likes=99)]
    write_ig_bronze(tmp_path / "ds_001.parquet", ds_new)
    _write_meta(tmp_path / "ds_001.parquet", "2026-01-02T00:00:00+00:00")

    result = _run(tmp_path, ops)

    assert len(result) == 1
    assert result["likes_count"][0] == 99
    assert result["source_dataset"][0] == "ds_001"


def test_scraped_at_transient_not_a_silver_column(tmp_path, ops):
    """scraped_at drives dedup ordering but never lands in silver."""
    rows = [make_ig_bronze_row("1", "abc", "Post", "u1")]
    write_ig_bronze(tmp_path / "ds_001.parquet", rows)
    _write_meta(tmp_path / "ds_001.parquet", "2026-01-01T00:00:00+00:00")

    result = _run(tmp_path, ops)

    assert "scraped_at" not in result.columns
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"), read_only=True)
    with duckdb.get_connection() as conn:
        cols = [r[0] for r in conn.execute("DESCRIBE silver_ig_posts").fetchall()]
    assert "scraped_at" not in cols


def test_dedup_uses_meta_downloaded_at_over_mtime(tmp_path, ops):
    """Provenance chain: meta.downloaded_at takes priority over file mtime."""
    import os

    # ds_001 has a NEWER file mtime but an OLDER recorded scrape time.
    ds_a = [make_ig_bronze_row("1", "abc", "FromA", "u1", likes=10)]
    write_ig_bronze(tmp_path / "ds_001.parquet", ds_a)
    _write_meta(tmp_path / "ds_001.parquet", "2026-01-01T00:00:00+00:00")
    ds_b = [make_ig_bronze_row("1", "abc", "FromB", "u1", likes=20)]
    write_ig_bronze(tmp_path / "ds_002.parquet", ds_b)
    _write_meta(tmp_path / "ds_002.parquet", "2026-02-01T00:00:00+00:00")
    future = (2027, 1, 1, 0, 0, 0, 0, 0, 0)
    os.utime(tmp_path / "ds_001.parquet", (1_900_000_000, 1_900_000_000))
    os.utime(tmp_path / "ds_002.parquet", (1_900_000_000, 1_900_000_000))
    del future

    result = _run(tmp_path, ops)

    # ds_002 scraped later per its meta → its value wins despite equal mtime.
    assert result["likes_count"][0] == 20
    assert result["source_dataset"][0] == "ds_002"
