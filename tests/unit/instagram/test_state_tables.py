"""Test that ``_ensure_state_tables`` no longer recreates ``gold_analyses``.

ADR-0011 replaces ``gold_analyses`` with ``silver_content_classification``;
a later phase drops ``gold_analyses``. Until then, the state-table bootstrap
must STOP creating it so a retired table is never silently resurrected.
"""

from __future__ import annotations

from dagster_duckdb import DuckDBResource

from datalake.defs.instagram.assets import _ensure_state_tables


def test_ensure_state_tables_does_not_create_gold_analyses(tmp_path) -> None:
    """GIVEN an empty DuckDB database
    WHEN _ensure_state_tables runs
    THEN gold_analyses is NOT created, while its siblings are.
    """
    db = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    _ensure_state_tables(db)

    with db.get_connection() as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }

    assert "gold_analyses" not in tables
    # Siblings are untouched by the fix.
    assert {
        "ig_post_labels",
        "silver_ig_post_observations",
        "silver_ig_profile_observations",
        "watermarks",
        "silver_ig_profiles",
        "silver_ig_comments",
    } <= tables
