"""Central schema catalog — the contract between code and state DB.

Each entry maps table name → {column_name: duckdb_type}. Types match the
actual DuckDB type strings reported by ``information_schema.columns.data_type``.

Every table the pipeline reads or writes must be listed here. The readiness
test (``test_state_compatibility.py``) asserts this catalog matches the
running state DB at ``data/state.duckdb``.

Adding a new column: add it here first, then deploy the pipeline change.
Migration is a separate concern — this test detects drift, it does not repair it.
"""

from __future__ import annotations

EXPECTED_SCHEMA: dict[str, dict[str, str]] = {
    "silver_ig_posts": {
        "post_id": "VARCHAR",
        "shortcode": "VARCHAR",
        "url": "VARCHAR",
        "caption": "VARCHAR",
        "owner_id": "VARCHAR",
        "owner_username": "VARCHAR",
        "likes_count": "INTEGER",
        "comments_count": "INTEGER",
        "video_play_count": "INTEGER",
        "video_view_count": "INTEGER",
        "timestamp": "TIMESTAMP",
        "hashtags": "VARCHAR",
        "meta_data": "VARCHAR",
        "has_engagement_bait": "BOOLEAN",
        "media_files": "VARCHAR",
        "media_count": "INTEGER",
        "source_dataset": "VARCHAR",
        "processed_on": "TIMESTAMP",
    },
    "silver_ig_progress": {
        "source_dataset": "VARCHAR",
        "post_count": "INTEGER",
        "completed_at": "TIMESTAMP",
    },
    "gold_ig_analyses": {
        "post_id": "VARCHAR",
        "schema_version": "INTEGER",
        "result_json": "VARCHAR",
        "analysed_at": "TIMESTAMP",
    },
    "dead_letter": {
        "post_id": "VARCHAR",
        "domain": "VARCHAR",
        "error": "VARCHAR",
        "attempts": "INTEGER",
        "failed_at": "TIMESTAMP",
        "status": "VARCHAR",
    },
    "watermarks": {
        "name": "VARCHAR",
        "timestamp": "TIMESTAMP",
        "config_hash": "VARCHAR",
    },
    "dim_profile": {
        "profile_key": "INTEGER",
        "owner_id": "VARCHAR",
        "owner_username": "VARCHAR",
        "channel": "VARCHAR",
        "effective_from": "TIMESTAMP",
        "effective_to": "TIMESTAMP",
        "is_current": "BOOLEAN",
    },
}

EXPECTED_VIEWS: list[str] = [
    "analytics_views",
]
