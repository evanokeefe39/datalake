"""Test that ``ensure_state_tables`` never recreates the retired gold tables.

ADR-0011 replaces ``gold_analyses`` with ``silver_content_classification``;
W9 (2026-09-15) retired ``gold_analyses`` and ``gold_growth_facets`` — both
were dropped and archived. The state-table bootstrap must NEVER recreate
either, so a retired table is never silently resurrected.
"""

from __future__ import annotations

from dagster_duckdb import DuckDBResource
from orchestration.defs.ig_core.slv.posts import ensure_state_tables


def testensure_state_tables_does_not_create_retired_gold_tables(tmp_path) -> None:
    """GIVEN an empty DuckDB database
    WHEN ensure_state_tables runs
    THEN the retired gold tables are NOT created, while siblings are.
    """
    db = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    ensure_state_tables(db)

    with db.get_connection() as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }

    assert "gold_analyses" not in tables
    assert "gold_growth_facets" not in tables
    # Siblings are untouched by the fix.
    assert {
        "ig_post_labels",
        "silver_ig_post_observations",
        "silver_ig_profile_observations",
        "watermarks",
        "silver_ig_profiles",
        "silver_ig_comments",
    } <= tables
