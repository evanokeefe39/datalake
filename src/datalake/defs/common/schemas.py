"""Canonical schema catalog — single source of truth for all DB tables.

This module DEFINES what tables, columns, types, and constraints the pipeline
expects across both DuckDB and SQLite. Every other reference — runtime DDL,
migration scripts, asset column lists — derives from here.

Two dialects are modeled:

**DuckDB types** match ``information_schema.columns.data_type``
(VARCHAR, INTEGER, TIMESTAMP, BOOLEAN, BIGINT, DOUBLE, DATE).

**SQLite types** match ``PRAGMA table_info`` (TEXT, INTEGER, REAL).

Each table is declared once as a :class:`Table` spec (ordered columns +
per-column constraints + table-level constraints). From that single spec we
derive:

* ``DUCKDB_TABLES`` / ``SQLITE_TABLES`` — the type-only maps consumed by the
  state-compatibility test and asset column lists (backward-compatible shape).
* ``duckdb_ddl()`` / ``sqlite_ddl()`` — the ``CREATE TABLE`` statements the
  runtime assets execute, so the DDL can never drift from the catalog.
"""

from __future__ import annotations

from dataclasses import dataclass

# ── Spec model ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Column:
    """One column: its canonical type plus DDL constraints.

    ``sql_type`` is the type name reported by the target DB's introspection.
    ``default`` is a raw SQL literal (e.g. ``"'instagram'"``, ``"0"``,
    ``"FALSE"``, ``"CURRENT_TIMESTAMP"``). ``references`` is the full clause
    after ``REFERENCES`` (e.g. ``"batch_jobs(id)"``,
    ``"creators(id) ON DELETE CASCADE"``).
    """

    sql_type: str
    not_null: bool = False
    default: str | None = None
    primary_key: bool = False
    autoincrement: bool = False
    references: str | None = None


@dataclass(frozen=True)
class Table:
    """A table spec: ordered columns + table-level constraints.

    ``primary_key`` is a composite primary key, used only when no single
    column carries ``primary_key=True``. ``unique`` and ``indexes`` are
    table-level constraints/indexes.
    """

    columns: dict[str, Column]
    primary_key: tuple[str, ...] = ()
    unique: tuple[tuple[str, ...], ...] = ()
    indexes: tuple[tuple[str, str], ...] = ()  # (index_name, "col_a, col_b")


# ── DuckDB (data/state.duckdb) ──────────────────────────────────────────────

_DUCKDB_SPECS: dict[str, Table] = {
    "silver_ig_posts": Table(
        columns={
            "post_id": Column("VARCHAR", primary_key=True),
            "shortcode": Column("VARCHAR"),
            "url": Column("VARCHAR"),
            "caption": Column("VARCHAR"),
            "owner_id": Column("VARCHAR"),
            "owner_username": Column("VARCHAR"),
            "likes_count": Column("INTEGER"),
            "comments_count": Column("INTEGER"),
            "video_play_count": Column("INTEGER"),
            "video_view_count": Column("INTEGER"),
            "timestamp": Column("TIMESTAMP"),
            "hashtags": Column("VARCHAR", not_null=True, default="'[]'"),
            "meta_data": Column("VARCHAR"),
            "has_engagement_bait": Column("BOOLEAN", not_null=True, default="FALSE"),
            "media_files": Column("VARCHAR", not_null=True, default="'[]'"),
            "media_count": Column("INTEGER", not_null=True, default="0"),
            "source_dataset": Column("VARCHAR", not_null=True),
            "processed_on": Column("TIMESTAMP"),
        },
    ),
    "gold_analyses": Table(
        columns={
            "post_id": Column("VARCHAR", not_null=True),
            "domain": Column("VARCHAR", not_null=True, default="'instagram'"),
            "prompt_hash": Column("VARCHAR"),
            "model": Column("VARCHAR"),
            "result_json": Column("VARCHAR"),
            "analysed_at": Column("VARCHAR", not_null=True),
        },
        primary_key=("post_id", "domain"),
    ),
    "watermarks": Table(
        columns={
            "name": Column("VARCHAR", primary_key=True),
            "timestamp": Column("TIMESTAMP", not_null=True),
            "config_hash": Column("VARCHAR"),
        },
    ),
    "silver_ig_profiles": Table(
        columns={
            "owner_id": Column("VARCHAR", primary_key=True),
            "owner_username": Column("VARCHAR"),
            "full_name": Column("VARCHAR"),
            "biography": Column("VARCHAR"),
            "followers_count": Column("INTEGER"),
            "follows_count": Column("INTEGER"),
            "posts_count": Column("INTEGER"),
            "is_business": Column("BOOLEAN"),
            "is_verified": Column("BOOLEAN"),
            "profile_pic_url": Column("VARCHAR"),
            "external_url": Column("VARCHAR"),
            "source_dataset": Column("VARCHAR"),
            "processed_on": Column("TIMESTAMP"),
        },
    ),
    "silver_ig_comments": Table(
        columns={
            "comment_id": Column("VARCHAR", primary_key=True),
            "post_id": Column("VARCHAR"),
            "post_shortcode": Column("VARCHAR"),
            "text": Column("VARCHAR"),
            "owner_username": Column("VARCHAR"),
            "owner_id": Column("VARCHAR"),
            "likes_count": Column("INTEGER"),
            "timestamp": Column("TIMESTAMP"),
            "reply_to_id": Column("VARCHAR"),
            "source_dataset": Column("VARCHAR"),
            "processed_on": Column("TIMESTAMP"),
        },
    ),
    "dim_profile": Table(
        columns={
            "profile_key": Column("INTEGER", primary_key=True),
            "owner_id": Column("VARCHAR", not_null=True),
            "owner_username": Column("VARCHAR"),
            "channel": Column("VARCHAR", not_null=True, default="'instagram'"),
            "effective_from": Column("TIMESTAMP", not_null=True, default="CURRENT_TIMESTAMP"),
            "effective_to": Column("TIMESTAMP"),
            "is_current": Column("BOOLEAN", not_null=True, default="TRUE"),
            "profile_pic_path": Column("VARCHAR"),
            "creator_id": Column("INTEGER"),
            "creator_name": Column("VARCHAR"),
        },
    ),
    "silver_ig_post_observations": Table(
        columns={
            "post_id": Column("VARCHAR", not_null=True),
            "observed_at": Column("TIMESTAMP WITH TIME ZONE", not_null=True),
            "likes_count": Column("INTEGER"),
            "comments_count": Column("INTEGER"),
            "video_view_count": Column("INTEGER"),
            "video_play_count": Column("INTEGER"),
            "source_dataset": Column("VARCHAR", not_null=True),
        },
        primary_key=("post_id", "source_dataset"),
    ),
    "ig_post_labels": Table(
        columns={
            "post_id": Column("VARCHAR", primary_key=True),
            "label": Column("VARCHAR", not_null=True),
            "method": Column("VARCHAR", not_null=True),
            "enrich_decision": Column("VARCHAR", not_null=True),
            "judged_at": Column("TIMESTAMP WITH TIME ZONE", not_null=True),
            "maturity_days": Column("INTEGER"),  # age at day7 judgment; NULL for day0/pending
            "is_provisional": Column("BOOLEAN", not_null=True),
            "label_version": Column("INTEGER", not_null=True),
            "baseline_center": Column("DOUBLE"),
            "baseline_spread": Column("DOUBLE"),
            "baseline_n": Column("INTEGER"),
        },
    ),
}

DUCKDB_VIEWS: list[str] = [
    "v_post_detail",
    "v_post_baselines",
    "v_signal",
    "v_quality_trend",
    "v_creator_quality",
    "v_rising_creators",
    "v_domain_coverage",
    "v_engagement_outliers",
    "v_outlier_posts",
    "v_creator_outlier_rate",
    "v_post_metrics",
    "v_creator_metrics",
    "v_creator_profile",
    "v_creator_topics",
    "v_profile_metrics",
    "v_overview",
    "v_standout_calendar",
    "v_recent_hot_posts",
]

# ── SQLite (data/ops.sqlite) ────────────────────────────────────────────────

_SQLITE_SPECS: dict[str, Table] = {
    "batch_jobs": Table(
        columns={
            "id": Column("INTEGER", primary_key=True, autoincrement=True),
            "consumer": Column("TEXT", not_null=True, default="'gemini'"),
            "mode": Column("TEXT", not_null=True, default="'interactive'"),
            "gemini_batch_name": Column("TEXT"),
            "gemini_batch_status": Column("TEXT"),
            "gemini_batch_error": Column("TEXT"),
            "status": Column("TEXT", not_null=True, default="'pending'"),
            "created_at": Column("TEXT", not_null=True),
            "completed_at": Column("TEXT"),
            "total_items": Column("INTEGER", not_null=True, default="0"),
            "processed_items": Column("INTEGER", not_null=True, default="0"),
            "failed_items": Column("INTEGER", not_null=True, default="0"),
        },
    ),
    "batch_items": Table(
        columns={
            "id": Column("INTEGER", primary_key=True, autoincrement=True),
            "job_id": Column("INTEGER", not_null=True, references="batch_jobs(id)"),
            "payload": Column("TEXT", not_null=True),
            "status": Column("TEXT", not_null=True, default="'pending'"),
            "attempts": Column("INTEGER", not_null=True, default="0"),
            "error": Column("TEXT"),
            "scheduled_for": Column("TEXT"),
            "created_at": Column("TEXT", not_null=True),
            "updated_at": Column("TEXT", not_null=True),
        },
        unique=(("job_id", "payload"),),
        indexes=(("idx_batch_items_job_status", "job_id, status"),),
    ),
    "media_metadata": Table(
        columns={
            "media_url_hash": Column("TEXT", primary_key=True),
            "media_url": Column("TEXT", not_null=True),
            "file_api_uri": Column("TEXT"),
            "mime_type": Column("TEXT"),
            "file_size": Column("INTEGER"),
            "video_duration_seconds": Column("REAL"),
            "upload_state": Column("TEXT", default="'pending'"),
            "expires_at": Column("TEXT"),
            "created_at": Column("TEXT", not_null=True),
            "uploaded_at": Column("TEXT"),
        },
    ),
    "dead_letter": Table(
        columns={
            "post_id": Column("TEXT", not_null=True),
            "domain": Column("TEXT", not_null=True, default="'instagram'"),
            "error": Column("TEXT"),
            "attempts": Column("INTEGER", not_null=True, default="0"),
            "failed_at": Column("TEXT", not_null=True),
        },
        primary_key=("post_id", "domain"),
    ),
    "media_cache": Table(
        columns={
            "cache_key": Column("TEXT", primary_key=True),
            "local_path": Column("TEXT", not_null=True),
            "content_type": Column("TEXT"),
            "size_bytes": Column("INTEGER"),
            "fetched_at": Column("TEXT", not_null=True),
            "source_url": Column("TEXT"),
        },
    ),
    "creators": Table(
        columns={
            "id": Column("INTEGER", primary_key=True),
            "name": Column("TEXT", not_null=True),
            "created_at": Column("TEXT", not_null=True),
            "updated_at": Column("TEXT", not_null=True),
        },
    ),
    "profiles": Table(
        columns={
            "platform": Column("TEXT", not_null=True),
            "handle": Column("TEXT", not_null=True),
            "profile_url": Column("TEXT", not_null=True),
            "results_type": Column("TEXT", not_null=True, default="'details'"),
            "results_limit": Column("INTEGER", not_null=True, default="1"),
            "enabled": Column("INTEGER", not_null=True, default="1"),
            "tier": Column("TEXT", not_null=True, default="'tier1'"),
            "creator_id": Column(
                "INTEGER", not_null=True, references="creators(id) ON DELETE CASCADE"
            ),
            "updated_at": Column("TEXT", not_null=True),
        },
        primary_key=("platform", "handle"),
    ),
    "creator_merges": Table(
        columns={
            "merged_creator_id": Column("INTEGER", primary_key=True),
            "merged_creator_name": Column("TEXT", not_null=True),
            "surviving_creator_id": Column("INTEGER", not_null=True),
            "handle": Column("TEXT", not_null=True),
            "merged_at": Column("TEXT", not_null=True),
            "reversed_at": Column("TEXT"),
        },
    ),
    "prompt_registry": Table(
        columns={
            "prompt_hash": Column("TEXT", primary_key=True),
            "prompt": Column("TEXT", not_null=True),
            "model": Column("TEXT", not_null=True),
            "recorded_at": Column("TEXT", not_null=True),
        },
    ),
}

# ── Derived type-only maps (backward-compatible shape) ──────────────────────

DUCKDB_TABLES: dict[str, dict[str, str]] = {
    name: {col: spec.sql_type for col, spec in table.columns.items()}
    for name, table in _DUCKDB_SPECS.items()
}

SQLITE_TABLES: dict[str, dict[str, str]] = {
    name: {col: spec.sql_type for col, spec in table.columns.items()}
    for name, table in _SQLITE_SPECS.items()
}

# ── Convenience: column-name-only lists for asset code ──────────────────────

SILVER_COLUMNS: list[str] = list(DUCKDB_TABLES["silver_ig_posts"].keys())

# ── DDL builder ─────────────────────────────────────────────────────────────


def _column_def(col: Column) -> str:
    """Render a single column's DDL fragment (type + constraints)."""
    parts = [col.sql_type]
    if col.primary_key:
        parts.append("PRIMARY KEY")
        if col.autoincrement:
            parts.append("AUTOINCREMENT")
    if col.not_null:
        parts.append("NOT NULL")
    if col.default is not None:
        parts.append(f"DEFAULT {col.default}")
    if col.references is not None:
        parts.append(f"REFERENCES {col.references}")
    return " ".join(parts)


def _render_table(name: str, table: Table) -> str:
    """Render a ``CREATE TABLE IF NOT EXISTS`` statement from a Table spec."""
    clauses = [f"    {cname} {_column_def(col)}" for cname, col in table.columns.items()]
    if table.primary_key:
        clauses.append(f"    PRIMARY KEY ({', '.join(table.primary_key)})")
    for uniq in table.unique:
        clauses.append(f"    UNIQUE({', '.join(uniq)})")
    return f"CREATE TABLE IF NOT EXISTS {name} (\n" + ",\n".join(clauses) + "\n)"


def _render_indexes(name: str, table: Table) -> list[str]:
    """Render ``CREATE INDEX IF NOT EXISTS`` statements for a table."""
    return [
        f"CREATE INDEX IF NOT EXISTS {idx_name} ON {name}({cols})"
        for idx_name, cols in table.indexes
    ]


def duckdb_ddl(name: str) -> str:
    """Return the ``CREATE TABLE IF NOT EXISTS`` statement for a DuckDB table."""
    return _render_table(name, _DUCKDB_SPECS[name])


def sqlite_ddl(name: str) -> str:
    """Return the ``CREATE TABLE IF NOT EXISTS`` statement for a SQLite table."""
    return _render_table(name, _SQLITE_SPECS[name])


def sqlite_index_ddl(name: str) -> list[str]:
    """Return any ``CREATE INDEX IF NOT EXISTS`` statements for a SQLite table."""
    return _render_indexes(name, _SQLITE_SPECS[name])

def sqlite_ddl_for(*names: str) -> str:
    """Return DDL (tables + their indexes) for the given SQLite tables.

    Statements are ``;``-terminated so the result can be passed to
    ``sqlite3.Connection.executescript``.
    """
    statements: list[str] = []
    for name in names:
        statements.append(sqlite_ddl(name))
        statements.extend(sqlite_index_ddl(name))
    return ";\n".join(statements) + ";" if statements else ""


def duckdb_all_ddl() -> str:
    """Return DDL for every DuckDB table in the catalog."""
    return ";\n".join(duckdb_ddl(name) for name in _DUCKDB_SPECS) + ";"


def sqlite_all_ddl() -> str:
    """Return DDL (tables + indexes) for every SQLite table in the catalog."""
    return sqlite_ddl_for(*_SQLITE_SPECS)
