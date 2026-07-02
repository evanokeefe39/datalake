"""Canonical schema catalog — single source of truth for all DB tables.

This file DEFINES what tables, columns, and types the pipeline expects
across both DuckDB and SQLite. Every other reference — DDL statements,
AGENTS.md docs, migration scripts — derives from here.

**DuckDB types** match ``information_schema.columns.data_type``
(VARCHAR, INTEGER, TIMESTAMP, BOOLEAN, BIGINT, DOUBLE).

**SQLite types** match ``PRAGMA table_info`` (TEXT, INTEGER, REAL).
"""

from __future__ import annotations

# ── DuckDB (data/state.duckdb) ────────────────────────────────────────────

EXPECTED_DUCKDB: dict[str, dict[str, str]] = {
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
    "gold_analyses": {
        "post_id": "VARCHAR",
        "domain": "VARCHAR",
        "prompt_hash": "VARCHAR",
        "result_json": "VARCHAR",
        "analysed_at": "VARCHAR",
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

EXPECTED_DUCKDB_VIEWS: list[str] = [
    "analytics_views",
]

# ── SQLite (data/ops.sqlite) ─────────────────────────────────────────────

EXPECTED_SQLITE: dict[str, dict[str, str]] = {
    "batch_jobs": {
        "id": "INTEGER",
        "status": "TEXT",
        "created_at": "TEXT",
        "completed_at": "TEXT",
        "total_items": "INTEGER",
        "processed_items": "INTEGER",
        "failed_items": "INTEGER",
    },
    "batch_items": {
        "id": "INTEGER",
        "job_id": "INTEGER",
        "post_id": "TEXT",
        "domain": "TEXT",
        "status": "TEXT",
        "attempts": "INTEGER",
        "error": "TEXT",
        "created_at": "TEXT",
        "updated_at": "TEXT",
    },
    "media_metadata": {
        "media_url_hash": "TEXT",
        "media_url": "TEXT",
        "file_api_uri": "TEXT",
        "mime_type": "TEXT",
        "file_size": "INTEGER",
        "upload_state": "TEXT",
        "created_at": "TEXT",
        "uploaded_at": "TEXT",
    },
    "dead_letter": {
        "post_id": "TEXT",
        "domain": "TEXT",
        "error": "TEXT",
        "attempts": "INTEGER",
        "failed_at": "TEXT",
    },
}

# ── Backward-compat alias ─────────────────────────────────────────────────
# Old code imports EXPECTED_SCHEMA — keep it working during transition.

EXPECTED_SCHEMA = EXPECTED_DUCKDB
EXPECTED_VIEWS = EXPECTED_DUCKDB_VIEWS
