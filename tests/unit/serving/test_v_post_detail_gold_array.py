"""Tests for the v_post_detail gold-extraction array path and its coverage check.

Some model responses are stored as a single-element JSON ARRAY instead of an
object. v_post_detail COALESCEs the ``$[0]`` JSON path so those rows surface
their attributes; ``v_post_detail_gold_attribute_coverage`` warns when stored
attributes are not surfaced and reports (but does not fail on) never-enriched
posts.
"""

from __future__ import annotations

from dagster import (
    AssetCheckSeverity,
    build_asset_check_context,
    build_asset_context,
)
from dagster_duckdb import DuckDBResource

from datalake.defs.serving.asset_checks import _v_post_detail_gold_attribute_coverage
from datalake.defs.serving.assets import (
    dim_date as _dim_date_asset,
)
from datalake.defs.serving.assets import (
    v_post_detail as _v_post_detail_asset,
)
from tests.fixtures.silver_factories import seed_silver_posts

# ── Fixture helpers ─────────────────────────────────────────────────────────


def _ensure_gold_table(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_analyses (
                post_id TEXT NOT NULL,
                domain TEXT NOT NULL DEFAULT 'instagram',
                prompt_hash TEXT,
                result_json TEXT,
                analysed_at TEXT NOT NULL,
                PRIMARY KEY (post_id, domain)
            )
        """)


def _ensure_dim_profile_table(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_profile (
                profile_key INTEGER PRIMARY KEY,
                owner_id TEXT NOT NULL,
                owner_username TEXT,
                channel TEXT NOT NULL DEFAULT 'instagram',
                effective_from TIMESTAMP NOT NULL,
                effective_to TIMESTAMP,
                is_current BOOLEAN NOT NULL DEFAULT TRUE,
                creator_id TEXT,
                creator_name TEXT,
            )
        """)


def _run_v_post_detail(duckdb: DuckDBResource) -> None:
    ctx = build_asset_context(resources={"duckdb": duckdb})
    _dim_date_asset(ctx)
    _v_post_detail_asset(ctx)


# ── v_post_detail array-path extraction ─────────────────────────────────────


def test_v_post_detail_surfaces_array_shaped_result_json(db):
    """GIVEN a gold_analyses row whose result_json is a single-element array
    WHEN v_post_detail is built
    THEN the array attributes are surfaced (not NULL) and object rows are unchanged.
    """
    seed_silver_posts(
        db,
        [("arr1", "Array post"), ("obj1", "Object post")],
        caption_idx=1,
    )
    _ensure_gold_table(db)
    _ensure_dim_profile_table(db)
    with db.get_connection() as conn:
        conn.execute(
            "INSERT INTO gold_analyses VALUES "
            "('arr1', 'instagram', NULL, "
            """'[{"topic":"X","content_type":"tutorial"}]', NOW())"""
        )
        conn.execute(
            "INSERT INTO gold_analyses VALUES "
            "('obj1', 'instagram', NULL, "
            """'{"topic":"Y","content_type":"guide"}', NOW())"""
        )
    _run_v_post_detail(db)

    with db.get_connection() as conn:
        rows = conn.execute(
            "SELECT post_id, gold_topic, content_type, "
            "admiralty, style, format FROM v_post_detail ORDER BY post_id"
        ).fetchall()

    assert rows == [
        ("arr1", "X", "tutorial", None, None, None),  # array path surfaced
        ("obj1", "Y", "guide", None, None, None),  # object path unchanged
    ]


def test_v_post_detail_never_enriched_row_still_nulls(db):
    """GIVEN a post with no gold_analyses row
    WHEN v_post_detail is built
    THEN gold fields stay NULL and the post still appears (LEFT JOIN).
    """
    seed_silver_posts(db, [("p1", "Lonely post")], caption_idx=1)
    _ensure_gold_table(db)
    _ensure_dim_profile_table(db)
    _run_v_post_detail(db)

    with db.get_connection() as conn:
        row = conn.execute(
            "SELECT post_id, gold_topic, content_type, result_json "
            "FROM v_post_detail"
        ).fetchone()
    assert row == ("p1", None, None, None)


# ── v_post_detail_gold_attribute_coverage check ─────────────────────────────


def _seed_check_tables(duckdb: DuckDBResource, result_rows: list[tuple[str, str]]) -> None:
    """Seed minimal source tables + gold rows (post_id, result_json)."""
    seed_silver_posts(
        duckdb,
        [(post_id, post_id) for post_id, _ in result_rows],
    )
    _ensure_gold_table(duckdb)
    _ensure_dim_profile_table(duckdb)
    with duckdb.get_connection() as conn:
        for post_id, result_json in result_rows:
            conn.execute(
                "INSERT INTO gold_analyses VALUES "
                "(?, 'instagram', NULL, ?, NOW())",
                (post_id, result_json),
            )


def test_coverage_check_warns_when_stored_attribute_unsurfaced(db):
    """GIVEN a legacy extraction that only reads the object path
    WHEN an array-shaped result_json stores an attribute the view drops
    THEN the check WARNs.
    """
    _seed_check_tables(db, [("arr1", '[{"topic":"X"}]')])
    with db.get_connection() as conn:
        conn.execute(
            "CREATE OR REPLACE VIEW v_post_detail AS "
            "SELECT sp.post_id, sp.caption, ga.result_json, "
            "ga.result_json->>'$.topic' AS gold_topic "
            "FROM silver_ig_posts sp "
            "LEFT JOIN gold_analyses ga "
            "ON sp.post_id = ga.post_id AND ga.domain = 'instagram'"
        )

    ctx = build_asset_check_context(resources={"duckdb": db})
    result = _v_post_detail_gold_attribute_coverage(ctx)

    assert result.passed is False
    assert result.severity == AssetCheckSeverity.WARN
    assert result.metadata["stored_but_unsurfaced_rows"].value == 1


def test_coverage_check_does_not_fail_on_never_enriched_posts(db):
    """GIVEN posts with no gold_analyses row at all
    WHEN the coverage check runs
    THEN it passes — never-enriched posts are cost-gated, reported only.
    """
    _seed_check_tables(db, [("p1", '{"topic":"Y"}')])
    with db.get_connection() as conn:
        conn.execute(
            "INSERT INTO silver_ig_posts "
            "(post_id, caption, owner_id, owner_username, likes_count, "
            "comments_count, video_play_count, video_view_count, timestamp, "
            "hashtags, meta_data, has_engagement_bait, media_files, "
            "media_count, source_dataset, processed_on) "
            "VALUES ('p2', 'No gold', 'o', 'u', 0, 0, 0, 0, NOW(), "
            "'[]', NULL, FALSE, '[]', 0, 'test', NOW())"
        )
        conn.execute(
            "CREATE OR REPLACE VIEW v_post_detail AS "
            "SELECT sp.post_id, sp.caption, ga.result_json, "
            "COALESCE(ga.result_json->>'$.topic', ga.result_json->>'$[0].topic') "
            "AS gold_topic "
            "FROM silver_ig_posts sp "
            "LEFT JOIN gold_analyses ga "
            "ON sp.post_id = ga.post_id AND ga.domain = 'instagram'"
        )

    ctx = build_asset_check_context(resources={"duckdb": db})
    result = _v_post_detail_gold_attribute_coverage(ctx)

    assert result.passed is True
    assert result.metadata["no_gold_analyses_row"].value == 1
    assert result.metadata["stored_but_unsurfaced_rows"].value == 0


def test_coverage_check_passes_with_object_path_rows_only(db):
    """GIVEN the real v_post_detail and gold rows using the object path
    WHEN the coverage check runs
    THEN it passes — nothing is stored-but-unsurfaced.
    """
    _seed_check_tables(db, [("obj1", '{"topic":"Y"}')])
    _run_v_post_detail(db)

    ctx = build_asset_check_context(resources={"duckdb": db})
    result = _v_post_detail_gold_attribute_coverage(ctx)

    assert result.passed is True
    assert result.metadata["stored_but_unsurfaced_rows"].value == 0


def test_coverage_check_warns_on_real_view_with_unmigrated_array_rows(db):
    """GIVEN the real v_post_detail (array path COALESCEd) but a stored
    array-shaped result_json row
    WHEN the coverage check runs
    THEN it still WARNs — the check is a data-level tripwire on the
    '$.topic' NULL / '$[0].topic' NOT NULL signature, so it keeps flagging
    unmigrated array rows until the data is migrated (extraction is fixed
    in the view, the stored shape is not).
    """
    _seed_check_tables(db, [("arr1", '[{"topic":"X","content_type":"tutorial"}]')])
    _run_v_post_detail(db)

    ctx = build_asset_check_context(resources={"duckdb": db})
    result = _v_post_detail_gold_attribute_coverage(ctx)

    assert result.passed is False
    assert result.severity == AssetCheckSeverity.WARN
    assert result.metadata["stored_but_unsurfaced_rows"].value == 1
    # The view itself DOES surface the array attributes:
    with db.get_connection() as conn:
        row = conn.execute(
            "SELECT gold_topic, content_type FROM v_post_detail"
        ).fetchone()
    assert row == ("X", "tutorial")
