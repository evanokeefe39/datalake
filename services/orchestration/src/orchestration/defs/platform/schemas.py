"""DuckDB half of the canonical schema catalog — the analytical contract.

`state.duckdb` holds analytical state: the normalized silver tables, the six
enrichment silver tables, and the serving dimensions. The operational catalog
(`ops.sqlite`) is the other half, in `opsdb.schema`.

The spec model (`Column`, `Table`) is defined once, in `opsdb.schema`, and
imported here — the two halves are one catalog split by database, not two
catalogs.
"""

from __future__ import annotations

from opsdb.schema import Column, Table

MODEL_LEGACY_NULL = "unrecorded-legacy-null"
"""Sentinel for silver enrichment rows whose producing model was never
recorded (ADR-0014 D5): an honest "this result is verified, but the model
name was never written" — NOT a NULL and NOT a fabricated model name.

Single definition shared by both classification silver producers
(`classification.py`, `conform.py`); never redefine it locally."""

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
    "silver_ig_profile_observations": Table(
        columns={
            "owner_id": Column("VARCHAR", not_null=True),
            "owner_username": Column("VARCHAR"),
            "observed_at": Column("TIMESTAMP WITH TIME ZONE", not_null=True),
            "followers_count": Column("INTEGER"),
            "follows_count": Column("INTEGER"),
            "posts_count": Column("INTEGER"),
            "is_verified": Column("BOOLEAN"),
            "source_dataset": Column("VARCHAR", not_null=True),
        },
        primary_key=("owner_id", "observed_at", "source_dataset"),
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
    # NOTE: ``bronze_enrichment_raw`` is deliberately NOT in this catalog.
    # It is a bronze LAKE table (Parquet at data/lake/bronze/bronze_enrichment_raw.parquet,
    # written/read via polars in defs/enrichment/landing.py — see DATASET_ID and
    # read_responses), never a table registered in data/state.duckdb. The live
    # database has never contained it (verified read-only 2026-09-15); landing and
    # conform access it exclusively through Parquet, so it has no DuckDB DDL and
    # no entry in DUCKDB_TABLES.
    "silver_visual_annotations": Table(
        columns={
            "post_id": Column("VARCHAR", not_null=True),
            "platform": Column("VARCHAR", not_null=True),
            "provider": Column("VARCHAR"),
            "model": Column("VARCHAR"),
            "prompt_hash": Column("VARCHAR"),
            "schema_version": Column("VARCHAR"),
            "input_modality": Column("VARCHAR"),
            "content_mime_type": Column("VARCHAR"),
            "sampling_params_json": Column("VARCHAR"),
            "run_id": Column("VARCHAR"),
            "analysed_at": Column("TIMESTAMP WITH TIME ZONE"),
            "face_present": Column("BOOLEAN"),
            "value_medium": Column("VARCHAR"),
            "brand_logos_json": Column("VARCHAR"),
            "text_overlay_present": Column("BOOLEAN"),
            "on_screen_claim": Column("BOOLEAN"),
        },
        primary_key=("post_id", "platform"),
    ),
    "silver_visual_summaries": Table(
        columns={
            "post_id": Column("VARCHAR", not_null=True),
            "platform": Column("VARCHAR", not_null=True),
            "provider": Column("VARCHAR"),
            "model": Column("VARCHAR"),
            "prompt_hash": Column("VARCHAR"),
            "schema_version": Column("VARCHAR"),
            "input_modality": Column("VARCHAR"),
            "content_mime_type": Column("VARCHAR"),
            "sampling_params_json": Column("VARCHAR"),
            "run_id": Column("VARCHAR"),
            "analysed_at": Column("TIMESTAMP WITH TIME ZONE"),
            "content_summary": Column("VARCHAR"),
            "image_summaries_json": Column("VARCHAR"),
        },
        primary_key=("post_id", "platform"),
    ),
    "silver_audio_transcripts": Table(
        columns={
            "post_id": Column("VARCHAR", not_null=True),
            "platform": Column("VARCHAR", not_null=True),
            "provider": Column("VARCHAR"),
            "model": Column("VARCHAR"),
            "prompt_hash": Column("VARCHAR"),
            "schema_version": Column("VARCHAR"),
            "input_modality": Column("VARCHAR"),
            "content_mime_type": Column("VARCHAR"),
            "sampling_params_json": Column("VARCHAR"),
            "run_id": Column("VARCHAR"),
            "analysed_at": Column("TIMESTAMP WITH TIME ZONE"),
            "transcript": Column("VARCHAR"),
            "transcript_status": Column("VARCHAR"),
            "audio_present": Column("BOOLEAN"),
            "asr_model": Column("VARCHAR"),
            "language": Column("VARCHAR"),
        },
        primary_key=("post_id", "platform"),
    ),
    "silver_text_annotations": Table(
        columns={
            "post_id": Column("VARCHAR", not_null=True),
            "platform": Column("VARCHAR", not_null=True),
            "provider": Column("VARCHAR"),
            "model": Column("VARCHAR"),
            "prompt_hash": Column("VARCHAR"),
            "schema_version": Column("VARCHAR"),
            "input_modality": Column("VARCHAR"),
            "content_mime_type": Column("VARCHAR"),
            "sampling_params_json": Column("VARCHAR"),
            "run_id": Column("VARCHAR"),
            "analysed_at": Column("TIMESTAMP WITH TIME ZONE"),
            "hook_content": Column("VARCHAR"),
            "hook_type": Column("VARCHAR"),
            "is_sponsored": Column("BOOLEAN"),
            "sponsorship_signal": Column("VARCHAR"),
            "claimed_results": Column("BOOLEAN"),
            "cta_type": Column("VARCHAR"),
            "audience_named": Column("BOOLEAN"),
            "value_depth": Column("VARCHAR"),
            "replicable_tactic": Column("VARCHAR"),
            "hashtag_strategy": Column("VARCHAR"),
            "evidence": Column("VARCHAR"),
            "brand_safety_json": Column("VARCHAR"),
        },
        primary_key=("post_id", "platform"),
    ),
    "silver_text_summaries": Table(
        columns={
            "post_id": Column("VARCHAR", not_null=True),
            "platform": Column("VARCHAR", not_null=True),
            "provider": Column("VARCHAR"),
            "model": Column("VARCHAR"),
            "prompt_hash": Column("VARCHAR"),
            "schema_version": Column("VARCHAR"),
            "input_modality": Column("VARCHAR"),
            "content_mime_type": Column("VARCHAR"),
            "sampling_params_json": Column("VARCHAR"),
            "run_id": Column("VARCHAR"),
            "analysed_at": Column("TIMESTAMP WITH TIME ZONE"),
            "transcript_summary": Column("VARCHAR"),
        },
        primary_key=("post_id", "platform"),
    ),
    "silver_content_classification": Table(
        columns={
            "post_id": Column("VARCHAR", not_null=True),
            "platform": Column("VARCHAR", not_null=True),
            "provider": Column("VARCHAR"),
            "model": Column("VARCHAR"),
            "prompt_hash": Column("VARCHAR"),
            "schema_version": Column("VARCHAR"),
            "input_modality": Column("VARCHAR"),
            "content_mime_type": Column("VARCHAR"),
            "sampling_params_json": Column("VARCHAR"),
            "run_id": Column("VARCHAR"),
            "analysed_at": Column("TIMESTAMP WITH TIME ZONE"),
            "domain": Column("VARCHAR"),
            "subdomain": Column("VARCHAR"),
            "topic": Column("VARCHAR"),
            "subtopic": Column("VARCHAR"),
            "is_educational": Column("BOOLEAN"),
            "is_actionable": Column("BOOLEAN"),
            "admiralty": Column("VARCHAR"),
            "content_type": Column("VARCHAR"),
            "style": Column("VARCHAR"),
            "format": Column("VARCHAR"),
            # Verbatim bronze passthrough (US-ESA-2 AC6 byte parity):
            # NULL for bronze-conformed rows, populated by the legacy
            # gold_analyses backfill only.
            "result_json": Column("VARCHAR"),
        },
        primary_key=("post_id", "platform"),
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
    "v_underperformer_posts",
    "v_creator_underperformer_rate",
    "v_post_metrics",
    "v_creator_metrics",
    "v_creator_profile",
    "v_creator_topics",
    "v_profile_metrics",
    "v_overview",
    "v_standout_calendar",
    "v_recent_hot_posts",
    "v_post_follower_context",
    "gold_post_enrichment",
    "gold_creator_performance",
    "gold_content_shape_performance",
    "gold_top_posts",
]


# ── Derived type-only maps (backward-compatible shape) ──────────────────────

DUCKDB_TABLES: dict[str, dict[str, str]] = {
    name: {col: spec.sql_type for col, spec in table.columns.items()}
    for name, table in _DUCKDB_SPECS.items()
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


def duckdb_all_ddl() -> str:
    """Return DDL for every DuckDB table in the catalog."""
    return ";\n".join(duckdb_ddl(name) for name in _DUCKDB_SPECS) + ";"


