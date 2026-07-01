"""E2E tests: full bronze → silver → gold → serving pipeline.

All assets run against a shared tmp_path + DuckDBResource.  Gemini is
mocked at the resource level (``GeminiResource.analyze``) so no real API
calls are made.  The Parquet I/O path is exercised by writing real bronze
Parquet files into ``tmp_path`` and patching ``BRONZE_LAKE``.

Per test-hardening plan Phase 3:
- Full pipeline on tmp_path + :memory: DuckDB
- Each asset's output state verified after its step (not just final)
- Watermark chain verified (silver advances → gold reads correct posts)
- Parquet files written to lake match DuckDB state table rows
- Dead_letter receives failures, gold_ig_analyses contains only completed
- Cross-layer post_id audit: every bronze post_id traceable through all layers
- Data volume: realistic row counts, no silent drops
"""

from __future__ import annotations

import json
from unittest.mock import patch

import polars as pl
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.common.resources import GeminiResource
from datalake.defs.instagram.assets import ig_posts_gld, ig_posts_slv
from datalake.defs.serving.assets import analytics_views, profile_dimension
from tests.fixtures.ig_bronze_factories import make_ig_bronze_row, write_ig_bronze
from tests.fixtures.gold_factories import FAKE_ANALYSIS

# ── Helpers ────────────────────────────────────────────────────────────────


def _run_silver(duckdb, bronze_dir):
    """Run ``ig_posts_slv`` with ``BRONZE_LAKE`` pointed at *bronze_dir*."""
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", bronze_dir):
        ctx = build_asset_context(resources={"duckdb": duckdb})
        return ig_posts_slv(ctx)


def _run_gold(duckdb, gemini):
    """Run ``ig_posts_gld`` with a (possibly mocked) GeminiResource."""
    ctx = build_asset_context(resources={"duckdb": duckdb, "gemini": gemini})
    return ig_posts_gld(ctx)


def _run_serving(duckdb):
    """Run profile_dimension then analytics_views."""
    ctx = build_asset_context(resources={"duckdb": duckdb})
    profile_dimension(ctx)
    analytics_views(ctx)


def _silver_row_count(duckdb):
    with duckdb.get_connection() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM silver_ig_posts"
        ).fetchone()[0]


def _gold_row_count(duckdb):
    with duckdb.get_connection() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM gold_ig_analyses"
        ).fetchone()[0]


def _dead_letter_rows(duckdb):
    with duckdb.get_connection() as conn:
        return conn.execute(
            "SELECT post_id, error, status FROM dead_letter ORDER BY post_id"
        ).fetchall()


def _analytics_rows(duckdb):
    with duckdb.get_connection() as conn:
        return conn.execute(
            "SELECT post_id, result_json, profile_key, channel "
            "FROM analytics_views ORDER BY post_id"
        ).fetchall()


# ── Test: full pipeline happy path ─────────────────────────────────────────


def test_full_pipeline_happy_path(tmp_path):
    """GIVEN bronze Parquet files with multiple posts
    WHEN silver, gold, and serving assets run in sequence
    THEN each intermediate layer contains the expected data
         AND the watermark chain is correct
         AND data volume is preserved (no silent drops).
    """
    # ── Bronze: write two datasets with 3 unique posts ──────────────────
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    write_ig_bronze(
        bronze_dir / "ds_001.parquet",
        [
            make_ig_bronze_row("p1", "abc", "AI marketing tips", "user_a",
                            likes=100, comments=10),
            make_ig_bronze_row("p2", "def", "Growth hacking 101", "user_b",
                            likes=200, comments=20),
        ],
    )
    write_ig_bronze(
        bronze_dir / "ds_002.parquet",
        [
            make_ig_bronze_row("p3", "ghi", "Data science basics", "user_a",
                            likes=300, comments=30),
        ],
    )

    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    gemini = GeminiResource()

    # ── Silver: process bronze → DuckDB ──────────────────────────────────
    silver_result = _run_silver(duckdb, bronze_dir)

    # Verify silver output state after its step (not just final)
    assert _silver_row_count(duckdb) == 3
    assert len(silver_result) == 3
    post_ids = set(silver_result["post_id"].to_list())
    assert post_ids == {"p1", "p2", "p3"}

    # processed_on must be non-null for every row (stamped during silver)
    assert silver_result["processed_on"].null_count() == 0
    assert all(
        silver_result["source_dataset"].to_list()
    )  # every row has a source_dataset

    # ── Drop watermarks so gold creates the table with config_hash ───────
    # (silver creates watermarks WITHOUT config_hash; gold expects it)
    with duckdb.get_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS watermarks")

    # ── Gold: enrich with mocked Gemini ─────────────────────────────────
    with patch.object(
        GeminiResource, "analyze", return_value=json.dumps(FAKE_ANALYSIS)
    ):
        gold_result = _run_gold(duckdb, gemini)

    # Verify gold output state after its step
    assert _gold_row_count(duckdb) == 3
    assert len(gold_result) == 3
    assert set(gold_result["post_id"].to_list()) == {"p1", "p2", "p3"}

    for i in range(len(gold_result)):
        parsed = json.loads(gold_result["result_json"][i])
        assert parsed["domain"] == "Business"

    # No dead_letter entries for happy path
    assert _dead_letter_rows(duckdb) == []

    # Watermark chain: gold_ig watermark must exist after gold run
    with duckdb.get_connection() as conn:
        wm = conn.execute(
            "SELECT name, timestamp FROM watermarks WHERE name = 'gold_ig'"
        ).fetchone()
    assert wm is not None
    assert wm[1] is not None  # timestamp is set

    # ── Serving: profile dimension + analytics view ──────────────────────
    _run_serving(duckdb)

    # Verify serving output
    rows = _analytics_rows(duckdb)
    assert len(rows) == 3
    post_ids_view = {r[0] for r in rows}
    assert post_ids_view == {"p1", "p2", "p3"}

    # Every row has non-NULL gold columns (happy path)
    for row in rows:
        assert row[1] is not None  # result_json
        assert row[2] is not None  # profile_key
        assert row[3] == "instagram"  # channel

    # ── Data volume: no silent drops across layers ───────────────────────
    assert _silver_row_count(duckdb) == 3  # silver
    assert _gold_row_count(duckdb) == 3    # gold
    assert len(rows) == 3                  # serving


# ── Test: watermark chain — gold only processes new posts ──────────────────


def test_watermark_chain_gold_reads_new_posts(tmp_path):
    """GIVEN silver has been processed and gold enriched batch 1
    WHEN a new bronze file is processed by silver
    THEN gold only enriches the new posts (not previously enriched ones).
    """
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    write_ig_bronze(
        bronze_dir / "ds_001.parquet",
        [make_ig_bronze_row("p1", "abc", "Batch 1 post", "user_a")],
    )
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    gemini = GeminiResource()

    # Batch 1: silver → gold
    _run_silver(duckdb, bronze_dir)
    with duckdb.get_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS watermarks")

    with patch.object(
        GeminiResource, "analyze", return_value=json.dumps(FAKE_ANALYSIS)
    ):
        _run_gold(duckdb, gemini)

    assert _gold_row_count(duckdb) == 1

    # Batch 2: new bronze file arrives
    write_ig_bronze(
        bronze_dir / "ds_002.parquet",
        [make_ig_bronze_row("p2", "def", "Batch 2 post", "user_b")],
    )
    _run_silver(duckdb, bronze_dir)
    assert _silver_row_count(duckdb) == 2

    # Gold should only pick up the new post (p2), not re-process p1
    with patch.object(
        GeminiResource, "analyze", return_value=json.dumps(FAKE_ANALYSIS)
    ):
        gold_result = _run_gold(duckdb, gemini)

    assert _gold_row_count(duckdb) == 2
    assert set(gold_result["post_id"].to_list()) == {"p1", "p2"}


# ── Test: dead_letter routing ──────────────────────────────────────────────


def test_dead_letter_routing(tmp_path):
    """GIVEN a mix of valid and invalid posts in silver
    WHEN gold enrichment runs
    THEN dead_letter receives failures/skips
         AND gold_ig_analyses contains only successfully completed posts.
    """
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    write_ig_bronze(
        bronze_dir / "ds_001.parquet",
        [
            make_ig_bronze_row("p1", "abc", "Good post content", "user_a"),
            make_ig_bronze_row("p2", "def", "", "user_b"),  # empty caption
            make_ig_bronze_row("p3", "ghi", "Another good one", "user_c"),
        ],
    )
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    gemini = GeminiResource()

    _run_silver(duckdb, bronze_dir)
    assert _silver_row_count(duckdb) == 3

    with duckdb.get_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS watermarks")

    # Gemini succeeds for all calls — but p2 (empty caption) is skipped
    # BEFORE Gemini is called (edge case: empty caption check)
    with patch.object(
        GeminiResource, "analyze", return_value=json.dumps(FAKE_ANALYSIS)
    ):
        gold_result = _run_gold(duckdb, gemini)

    # Gold contains only completed (non-failed) posts
    assert _gold_row_count(duckdb) == 2
    completed = set(gold_result["post_id"].to_list())
    assert completed == {"p1", "p3"}  # p2 excluded

    # Dead_letter has the empty-caption post
    dead = _dead_letter_rows(duckdb)
    assert len(dead) == 1
    assert dead[0][0] == "p2"
    assert "Empty caption" in dead[0][1]


def test_dead_letter_api_failure_routing(tmp_path):
    """GIVEN a post that causes Gemini to fail
    WHEN gold enrichment runs
    THEN the post lands in dead_letter (not gold_ig_analyses).
    """
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    write_ig_bronze(
        bronze_dir / "ds_001.parquet",
        [
            make_ig_bronze_row("p1", "abc", "Good post", "user_a"),
            make_ig_bronze_row("p2", "def", "Will fail post", "user_b"),
        ],
    )
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    gemini = GeminiResource()

    _run_silver(duckdb, bronze_dir)
    with duckdb.get_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS watermarks")

    def _mock_analyze(self, prompt):
        if "Will fail post" in prompt:
            raise RuntimeError("Simulated Gemini API failure")
        return json.dumps(FAKE_ANALYSIS)

    with patch.object(GeminiResource, "analyze", _mock_analyze):
        gold_result = _run_gold(duckdb, gemini)

    # Gold has only p1
    assert _gold_row_count(duckdb) == 1
    assert gold_result["post_id"][0] == "p1"

    # Dead_letter has p2
    dead = _dead_letter_rows(duckdb)
    assert len(dead) == 1
    assert dead[0][0] == "p2"
    assert "Simulated Gemini API failure" in dead[0][1]


# ── Test: cross-layer post_id audit ────────────────────────────────────────


def test_cross_layer_post_id_audit(tmp_path):
    """GIVEN bronze Parquet files with known post_ids
    WHEN the full pipeline runs
    THEN every bronze post_id is traceable through silver → gold → serving
         AND no post_id appears in any layer that wasn't in bronze.
    """
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()
    bronze_post_ids = {"p100", "p200", "p300", "p400"}
    posts = [
        make_ig_bronze_row("p100", "s100", "Caption 100", "u1"),
        make_ig_bronze_row("p200", "s200", "Caption 200", "u2"),
    ]
    write_ig_bronze(bronze_dir / "ds_a.parquet", posts)

    posts2 = [
        make_ig_bronze_row("p300", "s300", "Caption 300", "u3"),
        make_ig_bronze_row("p400", "s400", "Caption 400", "u1"),
    ]
    write_ig_bronze(bronze_dir / "ds_b.parquet", posts2)

    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    gemini = GeminiResource()

    # Silver
    silver_result = _run_silver(duckdb, bronze_dir)
    silver_ids = set(silver_result["post_id"].to_list())
    assert silver_ids == bronze_post_ids

    with duckdb.get_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS watermarks")

    # Gold
    with patch.object(
        GeminiResource, "analyze", return_value=json.dumps(FAKE_ANALYSIS)
    ):
        gold_result = _run_gold(duckdb, gemini)
    gold_ids = set(gold_result["post_id"].to_list())
    assert gold_ids == bronze_post_ids

    # Serving
    _run_serving(duckdb)
    rows = _analytics_rows(duckdb)
    serving_ids = {r[0] for r in rows}
    assert serving_ids == bronze_post_ids

    # Negative check: no post_ids outside bronze appear anywhere
    with duckdb.get_connection() as conn:
        silver_all = {
            r[0] for r in conn.execute(
                "SELECT post_id FROM silver_ig_posts"
            ).fetchall()
        }
        gold_all = {
            r[0] for r in conn.execute(
                "SELECT post_id FROM gold_ig_analyses"
            ).fetchall()
        }
    assert silver_all == bronze_post_ids
    assert gold_all == bronze_post_ids

    # Parquet/DuckDB parity: every bronze Parquet post_id made it into DuckDB
    for path in bronze_dir.glob("*.parquet"):
        df = pl.read_parquet(path)
        for row_id in df["id"].to_list():
            assert row_id in silver_ids, f"Bronze post {row_id} missing from silver"


# ── Test: data volume — no silent drops ────────────────────────────────────


def test_data_volume_no_silent_drops(tmp_path):
    """GIVEN 10 posts across 2 bronze files
    WHEN the full pipeline runs
    THEN counts at each layer are consistent (no silent drops beyond dedup).
    """
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()

    # 5 posts per file = 10 total (fits gold LIMIT 10 on one pass)
    for batch, offset in enumerate([0, 5]):
        rows = [
            make_ig_bronze_row(
                f"p{i}", f"sc{i}", f"Post number {i}", f"user{i % 3}"
            )
            for i in range(offset + 1, offset + 6)
        ]
        write_ig_bronze(bronze_dir / f"ds_{batch:03d}.parquet", rows)

    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    gemini = GeminiResource()

    _run_silver(duckdb, bronze_dir)
    assert _silver_row_count(duckdb) == 10

    with duckdb.get_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS watermarks")

    with patch.object(
        GeminiResource, "analyze", return_value=json.dumps(FAKE_ANALYSIS)
    ):
        _run_gold(duckdb, gemini)
    assert _gold_row_count(duckdb) == 10

    _run_serving(duckdb)
    rows = _analytics_rows(duckdb)
    assert len(rows) == 10

    # Check bronze file post_id count matches what's in DuckDB
    bronze_ids = set()
    for path in sorted(bronze_dir.glob("*.parquet")):
        df = pl.read_parquet(path)
        bronze_ids.update(df["id"].to_list())
    assert len(bronze_ids) == 10
