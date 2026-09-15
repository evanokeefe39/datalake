"""The conform layer HAS a production caller (audit-finding regression guard).

Phase 4 shipped ``conform.conform()`` with nothing in ``src/`` or
``scripts/`` invoking it: the five non-classification silver tables never
materialized and ``gold_content_shape_performance``'s view SQL could not
compile. These tests pin the caller end-to-end on a scratch lake + scratch
state DB: plan mode writes nothing; an apply run registers all six silver
tables (typed-empty where a workload has no bronze rows) and the serving
mart's view SQL compiles against them; a second run changes 0 rows.
"""

from __future__ import annotations

import inspect
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl
import pytest
from dagster_duckdb import DuckDBResource

import orchestration.defs.engine.silver_rt as conform
from orchestration.defs.engine.silver_rt import (
    SILVER_AUDIO_TRANSCRIPTS,
    SILVER_CONTENT_CLASSIFICATION,
    SILVER_TABLES,
    SILVER_TEXT_ANNOTATIONS,
    SILVER_VISUAL_ANNOTATIONS,
    SILVER_VISUAL_SUMMARIES,
)
from orchestration.defs.engine.landing import (
    WORKLOAD_CONTENT_CLASSIFICATION,
    WORKLOAD_GROWTH_FACETS_TEXT,
    WORKLOAD_GROWTH_FACETS_VISUAL,
    land_response,
)
from orchestration.defs.serving.marts import gold_content_shape_performance
from orchestration.defs.engine.silver_rt import conform as run_conform

NOW = datetime(2026, 9, 14, 12, 0, 0, tzinfo=UTC)


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def bronze_root(tmp_path) -> Path:
    return tmp_path / "lake" / "bronze"


@pytest.fixture
def silver_root(tmp_path) -> Path:
    return tmp_path / "lake" / "silver"


@pytest.fixture
def state_db(tmp_path) -> str:
    return str(tmp_path / "state.duckdb")


def _land(**kwargs) -> None:
    kwargs.setdefault("provider", "qwen")
    kwargs.setdefault("model", "qwen3.7-flash")
    kwargs.setdefault("prompt_hash", "ph")
    kwargs.setdefault("schema_version", "1")
    kwargs.setdefault("run_id", "job-1")
    kwargs.setdefault("ok", True)
    land_response(**kwargs)


def _land_visual(bronze_root: Path, post_id: str = "p1") -> None:
    payload = {
        "visual_facets": {
            "face_present": False,
            "value_medium": "demo",
            "brand_logos": ["Acme"],
            "text_overlay_present": True,
            "on_screen_claim": False,
        },
        "content_summary": "A demo video.",
        "image_summaries": [
            {"index": 0, "summary": "first"},
            {"index": 1, "summary": "second"},
        ],
    }
    _land(
        post_id=post_id,
        platform="instagram",
        workload=WORKLOAD_GROWTH_FACETS_VISUAL,
        response_text=json.dumps(payload),
        root=bronze_root,
    )


def _land_text(bronze_root: Path, post_id: str = "p2") -> None:
    payload = {
        "hook_content": "Stop doing X",
        "hook_type": "bold_claim",
        "is_sponsored": False,
        "sponsorship_signal": "",
        "claimed_results": True,
        "cta_type": "save",
        "audience_named": False,
        "value_depth": "practical",
        "replicable_tactic": "Open with the claim, then demo.",
        "evidence": "caption+transcript",
        "brand_safety": {
            "profanity": False,
            "sexualized_content": False,
            "political": False,
            "medical_claims": False,
            "financial_guarantees": False,
            "violence_trauma": False,
        },
    }
    _land(
        post_id=post_id,
        platform="instagram",
        workload=WORKLOAD_GROWTH_FACETS_TEXT,
        response_text=json.dumps(payload),
        root=bronze_root,
    )


def _land_classification(bronze_root: Path, post_id: str = "p3") -> None:
    payload = {
        "domain": "Dev",
        "subdomain": "Data",
        "topic": "Analytics",
        "subtopic": "Dashboards",
        "is_educational": True,
        "is_actionable": True,
        "admiralty": "C2",
        "content_type": "educational",
        "style": "casual",
        "format": "video",
    }
    _land(
        post_id=post_id,
        platform="instagram",
        workload=WORKLOAD_CONTENT_CLASSIFICATION,
        response_text=json.dumps(payload),
        root=bronze_root,
    )


# ── Import-level: the caller exists ────────────────────────────────────────


def test_conform_has_production_caller():
    """The audit finding, pinned at import level: both the CLI script and
    the Dagster asset actually invoke conform.conform()."""
    import orchestration.defs.engine.silver_rt as enrichment_assets
    from scripts import conform_silver

    script_src = inspect.getsource(conform_silver)
    asset_src = inspect.getsource(enrichment_assets)
    assert "conform.conform(" in script_src
    assert "conform.conform(" in asset_src
    # And the asset is exported for orchestration.
    from datalake import definitions

    names = {
        getattr(a, "name", getattr(getattr(a, "key", None), "to_user_string", lambda: str(a))())
        for a in definitions.all_assets
    }
    assert "silver_enrichment" in names


# ── Plan mode writes nothing ───────────────────────────────────────────────


def test_plan_mode_writes_nothing(bronze_root, silver_root, state_db):
    _land_visual(bronze_root)
    report = run_conform(
        state_db, bronze_root=str(bronze_root), silver_root=str(silver_root)
    )
    assert report["mode"] == "plan"
    assert report["bronze_rows"] == 1
    assert not silver_root.exists() or not any(silver_root.rglob("*.parquet"))
    assert not Path(state_db).exists()


# ── Apply: all six tables registered + queryable ───────────────────────────


def test_apply_registers_all_six_tables(bronze_root, silver_root, state_db):
    _land_visual(bronze_root)
    _land_text(bronze_root)
    _land_classification(bronze_root)
    report = run_conform(
        state_db,
        bronze_root=str(bronze_root),
        silver_root=str(silver_root),
        apply=True,
        now=NOW,
    )
    assert report["counts"] == {"conformed": 3, "quarantined": 0}
    assert report["silver_table_rows"] == {
        SILVER_VISUAL_ANNOTATIONS: 1,
        SILVER_VISUAL_SUMMARIES: 1,
        SILVER_AUDIO_TRANSCRIPTS: 0,
        SILVER_TEXT_ANNOTATIONS: 1,
        SILVER_CONTENT_CLASSIFICATION: 1,
        "silver_text_summaries": 0,
    }

    con = duckdb.connect(state_db)
    try:
        for tid in SILVER_TABLES:
            # Queryable by bare name — this is what unblocks the marts.
            cols = [
                r[0]
                for r in con.execute(
                    f"SELECT column_name FROM information_schema.columns "
                    f"WHERE table_name = '{tid}'"
                ).fetchall()
            ]
            assert cols[:2] == ["post_id", "platform"], tid
        assert con.execute(
            f"SELECT post_id, platform FROM {SILVER_CONTENT_CLASSIFICATION}"
        ).fetchall() == [("p3", "instagram")]
    finally:
        con.close()


def test_empty_workloads_still_create_typed_empty_tables(bronze_root, silver_root, state_db):
    """Only classification has bronze rows: the other five tables must still
    EXIST (typed-empty), so the gold marts compile regardless."""
    _land_classification(bronze_root)
    report = run_conform(
        state_db,
        bronze_root=str(bronze_root),
        silver_root=str(silver_root),
        apply=True,
        now=NOW,
    )
    assert report["silver_table_rows"][SILVER_VISUAL_ANNOTATIONS] == 0
    con = duckdb.connect(state_db)
    try:
        for tid in SILVER_TABLES:
            assert con.execute(f"SELECT COUNT(*) FROM {tid}").fetchone()[0] == (
                1 if tid == SILVER_CONTENT_CLASSIFICATION else 0
            )
    finally:
        con.close()

# ── The mart's view SQL compiles against the registered tables ─────────────


def test_gold_content_shape_performance_view_compiles(
    bronze_root, silver_root, state_db
):
    """The live observed failure: CREATE OR REPLACE VIEW failed because
    silver_visual_annotations / silver_text_annotations did not exist. After
    an apply run, the mart's ACTUAL view SQL (serving/assets.py, unmodified)
    compiles and the view is queryable."""
    _land_visual(bronze_root, post_id="p1")
    _land_text(bronze_root, post_id="p2")
    run_conform(
        state_db,
        bronze_root=str(bronze_root),
        silver_root=str(silver_root),
        apply=True,
        now=NOW,
    )
    # The mart's view SQL also joins the canonical serving views; stub the
    # three upstream views' minimal grain (the point under test is the SILVER
    # half of the join, produced by conform).
    con = duckdb.connect(state_db)
    try:
        con.execute(
            "CREATE TABLE v_post_detail AS SELECT CAST(NULL AS VARCHAR) AS post_id, "
            "CAST(NULL AS VARCHAR) AS channel, CAST(NULL AS VARCHAR) AS gold_domain, "
            "CAST(NULL AS VARCHAR) AS gold_topic, CAST(NULL AS VARCHAR) AS content_type, "
            "CAST(NULL AS VARCHAR) AS format, CAST(NULL AS VARCHAR) AS style, "
            "CAST(NULL AS VARCHAR) AS admiralty, CAST(NULL AS BOOLEAN) AS is_educational"
        )
        con.execute(
            "CREATE TABLE v_post_metrics AS SELECT CAST(NULL AS VARCHAR) AS post_id, "
            "CAST(NULL AS DOUBLE) AS engagement_score, CAST(NULL AS INTEGER) AS is_standout"
        )
        con.execute(
            "CREATE TABLE v_post_follower_context AS SELECT "
            "CAST(NULL AS VARCHAR) AS post_id, CAST(NULL AS VARCHAR) AS follower_tier"
        )
    finally:
        con.close()

    gold_content_shape_performance(DuckDBResource(database=state_db))

    con = duckdb.connect(state_db)
    try:
        rows = con.execute(
            "SELECT COUNT(*) FROM gold_content_shape_performance"
        ).fetchone()[0]
        assert rows == 0  # stub views are empty; compiling + querying is the proof
    finally:
        con.close()


# ── Idempotency: second run changes 0 rows ─────────────────────────────────


def test_second_run_changes_zero_rows(bronze_root, silver_root, state_db):
    _land_visual(bronze_root, post_id="p1")
    _land_text(bronze_root, post_id="p2")
    _land_classification(bronze_root, post_id="p3")
    for _ in range(2):
        run_conform(
            state_db,
            bronze_root=str(bronze_root),
            silver_root=str(silver_root),
            apply=True,
            now=NOW,
        )
    con = duckdb.connect(state_db)
    try:
        for tid in SILVER_TABLES:
            n, distinct = con.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT (post_id, platform)) FROM {tid}"
            ).fetchone()
            assert n == distinct, f"{tid}: duplicate keys after re-run"
        # Key sets identical to the silver snapshots on disk (replay, not growth).
        for tid in SILVER_TABLES:
            df = pl.read_parquet(conform_mod.table_path(tid, silver_root))
            keys = set(
                zip(df["post_id"].to_list(), df["platform"].to_list())
            )
            db_keys = set(
                con.execute(f"SELECT post_id, platform FROM {tid}").fetchall()
            )
            assert keys == db_keys, tid
    finally:
        con.close()
