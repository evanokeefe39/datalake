"""View-definition baseline: lock the 25 serving view definitions (US-ESA-2).

The old readiness hole: ``test_state_compatibility.py`` only checked that
views were SELECT-able — it never looked at their DEFINITIONS, so an entire
migration (gold_analyses → silver_content_classification + the ADR-0011
marts) could pass invisibly. This module closes that hole from the other
side: it materializes every catalog view (``DUCKDB_VIEWS``, incl. the four
gold marts) into a scratch DuckDB from the CURRENT asset code and asserts
each normalized definition is byte-for-byte the committed baseline.

- Any accidental edit to a view definition fails here.
- The intended rebind is asserted positively: no definition references
  ``gold_analyses`` / ``gold_growth_facets``.
- The marts are grepped for restated constants (tier buckets, momentum
  windows/gates) — they must compose canonical views, never re-derive
  (WATCHDOG metrics-centralization).

Old-world detectability (the readiness checks FAIL when the views still
read ``gold_analyses``) lives in
``tests/operational/test_state_compatibility.py::TestRebindDetectable``.

Regenerate the snapshot after an INTENDED definition change:

    UPDATE_VIEW_BASELINE=1 uv run pytest tests/unit/serving/test_view_definition_baseline.py
"""

from __future__ import annotations

import importlib
import json
import os
import re
from pathlib import Path
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource
import duckdb
import pytest
from dagster import build_asset_context

from datalake.defs.common.schemas import duckdb_ddl
serving = importlib.import_module("datalake.defs.serving.assets")
from tests.operational.expected_schema import EXPECTED_DUCKDB_VIEWS
from tests.operational.test_state_compatibility import (
    _BANNED_VIEW_SOURCES_RE,
    _REQUIRED_VIEW_SOURCES,
)

SNAPSHOT_PATH = Path(__file__).parent / "view_definition_baseline.json"

# Materialization order: each CREATE VIEW only needs its sources to exist.
_ASSET_ORDER = [
    "dim_date",
    "v_post_detail",
    "v_signal",
    "v_quality_trend",
    "v_creator_quality",
    "v_post_follower_context",
    "v_domain_coverage",
    "v_engagement_outliers",
    "v_post_baselines",
    "v_outlier_posts",
    "v_creator_outlier_rate",
    "v_underperformer_posts",
    "v_creator_underperformer_rate",
    "v_post_metrics",
    "v_creator_metrics",
    "v_profile_metrics",
    "v_creator_profile",
    "v_rising_creators",
    "v_overview",
    "v_standout_calendar",
    "v_recent_hot_posts",
    "gold_post_enrichment",
    "v_creator_topics",
    "gold_creator_performance",
    "gold_content_shape_performance",
    "gold_top_posts",
]
# Base tables the views bind against but that no serving asset creates here
# (they belong to silver/label assets). Created straight from the catalog.
# The two RETIRED tables (``gold_analyses`` / ``gold_growth_facets``) are
# deliberately INCLUDED — with minimal hand-written DDL, since W9 removed
# them from the catalog — so the no-banned-reference assertion is proven
# against a world where they exist to be bound against.
_BASE_TABLES = [
    "silver_ig_posts",
    "silver_content_classification",
    "silver_visual_annotations",
    "silver_visual_summaries",
    "silver_audio_transcripts",
    "silver_text_annotations",
    "silver_text_summaries",
    "silver_ig_profile_observations",
    "ig_post_labels",
    "dim_profile",
]
_RETIRED_TABLE_DDL = [
    """CREATE TABLE gold_analyses (
           post_id VARCHAR NOT NULL, domain VARCHAR NOT NULL,
           prompt_hash VARCHAR, result_json VARCHAR,
           analysed_at VARCHAR, PRIMARY KEY (post_id, domain))""",
    """CREATE TABLE gold_growth_facets (
           post_id VARCHAR NOT NULL, domain VARCHAR NOT NULL,
           prompt_hash VARCHAR NOT NULL, schema_version VARCHAR NOT NULL,
           growth_facets_json VARCHAR NOT NULL, content_summary VARCHAR,
           image_summaries_json VARCHAR, model VARCHAR,
           analysed_at VARCHAR, PRIMARY KEY (post_id, domain))""",
]


def _build_view_world(tmp_path: Path) -> str:
    """Materialize every serving view into a scratch DuckDB from asset code."""
    db_path = tmp_path / "state.duckdb"
    con = duckdb.connect(str(db_path))
    for table in _BASE_TABLES:
        con.execute(duckdb_ddl(table))
    for ddl in _RETIRED_TABLE_DDL:
        con.execute(ddl)
    con.close()

    resource = DuckDBResource(database=str(db_path))
    ctx = build_asset_context(resources={"duckdb": resource})
    for name in _ASSET_ORDER:
        getattr(serving, name)(ctx)
    return str(db_path)


def _view_definitions(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    rows = con.execute(
        "SELECT view_name, sql FROM duckdb_views() WHERE internal IS FALSE"
    ).fetchall()
    return {name: re.sub(r"\s+", " ", (sql or "")).strip() for name, sql in rows}


@pytest.fixture(scope="module")
def built_views(tmp_path_factory):
    db_path = _build_view_world(tmp_path_factory.mktemp("baseline"))
    con = duckdb.connect(db_path)
    yield _view_definitions(con), con
    con.close()


def _load_baseline() -> dict[str, str]:
    return json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))


class TestViewDefinitionBaseline:
    """Every catalog view definition is exactly the committed baseline."""

    def test_view_set_is_exactly_the_catalog(self, built_views):
        defs, _ = built_views
        actual = set(defs)
        assert actual == set(EXPECTED_DUCKDB_VIEWS), (
            f"missing={sorted(set(EXPECTED_DUCKDB_VIEWS) - actual)} "
            f"extra={sorted(actual - set(EXPECTED_DUCKDB_VIEWS))}"
        )

    def test_definitions_match_baseline_snapshot(self, built_views):
        defs, _ = built_views
        baseline = _load_baseline()
        assert set(defs) == set(baseline)
        drifted = {v for v in defs if defs[v] != baseline[v]}
        assert not drifted, (
            "View definition(s) drifted from the committed baseline:\n"
            + "\n".join(f"  {v}" for v in sorted(drifted))
            + "\nIf the change is INTENDED, regenerate with "
            "UPDATE_VIEW_BASELINE=1 and review the diff in git."
        )

    def test_no_retired_enrichment_table_in_any_definition(self, built_views):
        defs, _ = built_views
        offenders = {
            v: _BANNED_VIEW_SOURCES_RE.search(sql)
            for v, sql in defs.items()
            if _BANNED_VIEW_SOURCES_RE.search(sql)
        }
        assert not offenders, (
            f"Definitions still reference gold_analyses / gold_growth_facets: "
            f"{sorted(offenders)}"
        )

    def test_rebind_views_reference_target_sources(self, built_views):
        defs, _ = built_views
        missing = []
        for view, sources in _REQUIRED_VIEW_SOURCES.items():
            for source in sources:
                if not re.search(rf"\b{re.escape(source)}\b", defs[view]):
                    missing.append(f"{view} -> {source}")
        assert not missing, (
            f"Rebind incomplete; required target sources absent: {missing}"
        )

    def test_every_view_is_selectable(self, built_views):
        _, con = built_views
        for view in sorted(EXPECTED_DUCKDB_VIEWS):
            con.execute(f"SELECT * FROM {view} LIMIT 1")


class TestMartsDoNotRestateConstants:
    """Grep-shaped guard: marts compose canonical views, never re-derive.

    WATCHDOG metrics-centralization: tier buckets (growth tiers, hot/sigma
    thresholds) and momentum constants (28d/84d windows, 1.25 ratio, 5.0
    likes gate) each have exactly ONE definition — in the canonical views.
    The marts may SELECT those columns but must not restate a constant.
    """

    _MARTS = [
        "gold_post_enrichment",
        "gold_creator_performance",
        "gold_content_shape_performance",
        "gold_top_posts",
    ]

    # Restated follower-growth-tier buckets (canonical: v_post_follower_context).
    _TIER_BUCKET_RE = re.compile(r"'0-100'|'100-1k'|'1k-10k'|'10k+'")
    # Restated bucket arithmetic on a raw follower count.
    _BUCKET_ARITH_RE = re.compile(r"followers_count\s*<")
    # Restated momentum constants (canonical: v_creator_profile).
    _MOMENTUM_RE = re.compile(
        r"INTERVAL\s+'28'\s+DAY|INTERVAL\s+'84'\s+DAY"
        r"|\b1\.25\b|>=\s*5\.0"
    )

    def test_no_constants_restate_in_marts(self, built_views):
        defs, _ = built_views
        offenders: list[str] = []
        for mart in self._MARTS:
            sql = defs[mart]
            for label, rx in (
                ("tier bucket", self._TIER_BUCKET_RE),
                ("bucket arithmetic", self._BUCKET_ARITH_RE),
                ("momentum constant", self._MOMENTUM_RE),
            ):
                m = rx.search(sql)
                if m:
                    offenders.append(f"{mart}: restated {label} ({m.group(0)!r})")
        assert not offenders, (
            "Mart restates a canonical constant (metrics-centralization):\n"
            + "\n".join(f"  {o}" for o in offenders)
        )


def _maybe_update_snapshot(defs: dict[str, str]) -> None:
    SNAPSHOT_PATH.write_text(
        json.dumps(defs, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


@pytest.fixture(scope="module", autouse=True)
def _update_snapshot_gate(request, tmp_path_factory):
    """When UPDATE_VIEW_BASELINE=1, regenerate the snapshot instead of
    asserting it (explicit, reviewed regeneration — never silent)."""
    if os.environ.get("UPDATE_VIEW_BASELINE") == "1":
        db_path = _build_view_world(tmp_path_factory.mktemp("baseline_update"))
        con = duckdb.connect(db_path)
        defs = _view_definitions(con)
        con.close()
        _maybe_update_snapshot(defs)
        pytest.skip("snapshot regenerated (UPDATE_VIEW_BASELINE=1)")
    yield
