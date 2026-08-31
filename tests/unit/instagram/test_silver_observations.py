"""US-S2/S3/S4 — silver post observations: append, idempotency, provenance,
and the refresh non-interaction regression.

Covers:
- one observation row per post per bronze file, before dedup
- observed_at fallback: meta.downloaded_at → bronze mtime (processed_on is a
  last resort only when neither exists, which the mtime-based file discovery
  makes unreachable in the asset path)
- INSERT OR IGNORE on PK (post_id, source_dataset): reprocessing a file is a
  no-op; a re-scrape is a new row
- -1/NULL engagement sentinels kept raw
- US-S4 regression: re-ingesting an existing post must leave processed_on
  and batch_items untouched, append exactly one observation, and update
  silver to the newer scrape value.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from unittest.mock import patch

from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.common.schemas import sqlite_ddl
from datalake.defs.instagram.assets import ig_posts_slv
from tests.fixtures.ig_bronze_factories import make_ig_bronze_row, write_ig_bronze


def _write_meta(path, downloaded_at: str) -> None:
    path.with_suffix(".parquet.meta").write_text(
        json.dumps({"downloaded_at": downloaded_at}), encoding="utf-8"
    )


def _run(tmp_path, ops, duckdb):
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb, "ops": ops})
        return ig_posts_slv(context)


def _obs(duckdb):
    with duckdb.get_connection() as conn:
        return conn.execute(
            "SELECT post_id, observed_at, likes_count, comments_count, "
            "video_view_count, video_play_count, source_dataset "
            "FROM silver_ig_post_observations ORDER BY post_id, source_dataset"
        ).fetchall()


def _silver_post(duckdb, post_id):
    with duckdb.get_connection() as conn:
        return conn.execute(
            "SELECT processed_on, likes_count FROM silver_ig_posts WHERE post_id = ?",
            [post_id],
        ).fetchone()

def _batch_items(ops):
    with ops.get_connection() as conn:
        conn.execute(sqlite_ddl("batch_items"))
        return conn.execute("SELECT COUNT(*) FROM batch_items").fetchone()[0]

def test_observation_appended_per_post_with_meta_provenance(tmp_path, ops):
    rows = [
        make_ig_bronze_row("1", "abc", "Post one", "u1", likes=10),
        make_ig_bronze_row("2", "def", "Post two", "u2", likes=5),
    ]
    write_ig_bronze(tmp_path / "ds_001.parquet", rows)
    _write_meta(tmp_path / "ds_001.parquet", "2026-01-15T10:00:00+00:00")
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    _run(tmp_path, ops, duckdb)

    obs = _obs(duckdb)
    assert len(obs) == 2
    by_id = {r[0]: r for r in obs}
    assert by_id["1"][1] == datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc)
    assert by_id["1"][2] == 10  # likes, raw
    assert by_id["1"][6] == "ds_001"
    assert by_id["2"][1] == datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc)


def test_observed_at_falls_back_to_file_mtime(tmp_path, ops):
    """No meta sidecar → the bronze file mtime is the scrape time."""
    rows = [make_ig_bronze_row("1", "abc", "Post", "u1")]
    write_ig_bronze(tmp_path / "ds_001.parquet", rows)
    stamp = 1_760_000_000.0  # fixed epoch seconds
    os.utime(tmp_path / "ds_001.parquet", (stamp, stamp))
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    _run(tmp_path, ops, duckdb)

    (obs,) = _obs(duckdb)
    assert obs[1] == datetime.fromtimestamp(stamp, tz=timezone.utc)


def test_sentinels_kept_raw(tmp_path, ops):
    """-1 and NULL engagement values flow through untouched."""
    rows = [
        make_ig_bronze_row("1", "abc", "Post", "u1", likes=-1, comments=0),
        make_ig_bronze_row("2", "def", "Post", "u2", likes=3, comments=0),
    ]
    write_ig_bronze(tmp_path / "ds_001.parquet", rows)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    _run(tmp_path, ops, duckdb)

    by_id = {r[0]: r for r in _obs(duckdb)}
    assert by_id["1"][2] == -1  # likes sentinel raw, not coerced


def test_reprocessing_same_file_is_noop(tmp_path, ops):
    """Re-ingesting the SAME dataset (newer mtime) adds zero observations."""
    rows = [make_ig_bronze_row("1", "abc", "Post", "u1", likes=10)]
    fp = tmp_path / "ds_001.parquet"
    write_ig_bronze(fp, rows)
    _write_meta(fp, "2026-01-01T00:00:00+00:00")
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    _run(tmp_path, ops, duckdb)
    assert len(_obs(duckdb)) == 1

    # Bump mtime so the watermark treats the file as new again.
    os.utime(fp, (2_000_000_000, 2_000_000_000))
    _run(tmp_path, ops, duckdb)
    assert len(_obs(duckdb)) == 1  # PK (post_id, source_dataset) ignored the re-insert


def test_rescrape_adds_exactly_one_observation(tmp_path, ops):
    """A re-scrape under a NEW source_dataset is a new observation row; the
    original observation (original provenance) is preserved untouched."""
    write_ig_bronze(
        tmp_path / "ds_001.parquet",
        [make_ig_bronze_row("1", "abc", "Post", "u1", likes=10)],
    )
    _write_meta(tmp_path / "ds_001.parquet", "2026-01-01T00:00:00+00:00")
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    _run(tmp_path, ops, duckdb)
    original = _obs(duckdb)

    write_ig_bronze(
        tmp_path / "ds_002.parquet",
        [make_ig_bronze_row("1", "abc", "Post", "u1", likes=99)],
    )
    _write_meta(tmp_path / "ds_002.parquet", "2026-01-10T00:00:00+00:00")
    _run(tmp_path, ops, duckdb)

    obs = _obs(duckdb)
    assert len(obs) == 2
    assert original[0] in obs  # first observation row unchanged
    assert {(r[0], r[6]) for r in obs} == {("1", "ds_001"), ("1", "ds_002")}


def test_refresh_non_interaction_regression(tmp_path, ops):
    """US-S4 — re-ingesting an existing post_id must do ALL FOUR:
    (a) processed_on unchanged, (b) exactly one new observation appended,
    (c) silver likes_count updated to the newer scrape, (d) zero new
    batch_items.
    """
    write_ig_bronze(
        tmp_path / "ds_001.parquet",
        [make_ig_bronze_row("1", "abc", "Post", "u1", likes=10)],
    )
    _write_meta(tmp_path / "ds_001.parquet", "2026-01-01T00:00:00+00:00")
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    _run(tmp_path, ops, duckdb)

    processed_before, likes_before = _silver_post(duckdb, "1")
    obs_before = len(_obs(duckdb))
    items_before = _batch_items(ops)
    assert processed_before is not None

    # Re-scrape the same post under a new dataset, newer scrape time.
    write_ig_bronze(
        tmp_path / "ds_002.parquet",
        [make_ig_bronze_row("1", "abc", "Post", "u1", likes=99)],
    )
    _write_meta(tmp_path / "ds_002.parquet", "2026-01-10T00:00:00+00:00")
    _run(tmp_path, ops, duckdb)

    processed_after, likes_after = _silver_post(duckdb, "1")
    # (a) first-seen processed_on preserved
    assert processed_after == processed_before
    # (b) exactly one new observation
    assert len(_obs(duckdb)) == obs_before + 1
    # (c) dedup fix: silver holds the NEWER scrape's likes
    assert likes_after == 99
    assert likes_before == 10
    # (d) refresh must not enqueue enrichment
    assert _batch_items(ops) == items_before
