"""Readiness tests: validate code expectations against live databases.

- Table existence (both expected tables present, and no unexpected extra tables)
- Column types (extra columns tolerated, missing/mismatched columns fail)
- View queryability — against the TARGET world (all DUCKDB_VIEWS must exist)
- View DEFINITION purity: no serving view may reference the retired
  enrichment tables (``gold_analyses`` / ``gold_growth_facets``); the
  classification views must read ``silver_content_classification`` and the
  marts must compose their canonical upstreams (ADR-0011, US-ESA-2).
- Stale name detection (tables AND views, e.g. gold_ig_analyses,
  analytics_views)
"""


import re
import sqlite3
import warnings
from pathlib import Path

import duckdb
import pytest

from tests.operational.expected_schema import (
    EXPECTED_DUCKDB,
    EXPECTED_DUCKDB_VIEWS,
    EXPECTED_SQLITE,
)

# ── Known stale table names ──────────────────────────────────────────────
# Table names that existed in older schema versions but have been
# renamed or dropped. If any of these appear in the DB, the test fails
# with a migration hint.

_STALE_DUCKDB_TABLES: dict[str, str] = {
    "gold_ig_analyses": "Rename to 'gold_analyses' — run migrations/migrate_schema_drift.py",
    "silver_ig_progress": (
        "Drop — vestigial table replaced by watermarks. "
        "Run migrations/migrate_schema_drift.py"
    ),
}

_STALE_SQLITE_TABLES: dict[str, str] = {
    "instagram_media_cache": (
        "Drop — replaced by 'media_cache' (byte cache). "
        "Run migrations/migrate_schema_drift.py"
    ),
    "scrape_targets": (
        "Replace with 'creators' + 'profiles'. "
        "Run migrations/migrate_creators_profiles.py"
    ),
}

# ── Tables that exist in the DB but are not in the catalog ───────────────
_STALE_DUCKDB_VIEWS: dict[str, str] = {
    "analytics_views": (
        "Retired — the serving layer was split into per-view assets "
        "(v_post_detail + downstream views). The v_post_detail serving asset "
        "drops this view; re-run the serving assets."
    ),
}

# Retired enrichment tables — no serving view may reference them (ADR-0011:
# gold_analyses → silver_content_classification; gold_growth_facets → the
# four visual/text silver tables).
_BANNED_VIEW_SOURCES_RE = re.compile(r"\b(gold_analyses|gold_growth_facets)\b")

# Views that MUST read the target world, and the canonical sources their
# definition must reference. This is what makes the rebind detectable: an
# old-world definition (reading gold_analyses) cannot satisfy it.
_REQUIRED_VIEW_SOURCES: dict[str, list[str]] = {
    "v_post_detail": ["silver_content_classification"],
    "v_overview": ["silver_content_classification"],
    "gold_post_enrichment": ["v_post_metrics", "silver_visual_annotations"],
    "gold_creator_performance": ["v_creator_profile"],
    "gold_content_shape_performance": [
        "silver_visual_annotations",
        "silver_text_annotations",
    ],
    "gold_top_posts": ["gold_post_enrichment"],
}

# ── Tables that exist in the DB but are not in the catalog ───────────────
# Extra tables are detected and reported as warnings. They might be
# legitimate (user-created) or forgotten migrations. Either way, surface them.

# ---------------------------------------------------------------------------
# DuckDB helpers
# ---------------------------------------------------------------------------


def _duckdb_connect(path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(path, read_only=True)


def _duckdb_list_tables(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    ).fetchall()
    return {r[0] for r in rows}


def _duckdb_list_views(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_type = 'VIEW'
        ORDER BY table_name
        """
    ).fetchall()
    return {r[0] for r in rows}


def _duckdb_get_columns(con: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    rows = con.execute(
        f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = '{table}'
        ORDER BY ordinal_position
        """
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _duckdb_view_definitions(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Every non-internal view name -> its stored definition SQL."""
    rows = con.execute(
        "SELECT view_name, sql FROM duckdb_views() WHERE internal IS FALSE"
    ).fetchall()
    return {r[0]: (r[1] or "") for r in rows}


def view_definition_violations(
    con: duckdb.DuckDBPyConnection,
    check_required: bool = True,
) -> list[str]:
    """Target-world violations of the stored view definitions.

    - No view definition may reference a retired enrichment table.
    - The retired ``analytics_views`` view must not exist.
    - Views in ``_REQUIRED_VIEW_SOURCES`` must exist and reference their
      canonical target-world sources.
    - Every catalog view (DUCKDB_VIEWS) must have a stored definition.

    ``check_required=False`` limits the check to the banned-source/stale-view
    rules — used by the old-world detectability fixtures, which do not
    materialize the whole target world.
    """
    defs = _duckdb_view_definitions(con)
    violations: list[str] = []
    for name, sql in defs.items():
        if _BANNED_VIEW_SOURCES_RE.search(sql):
            violations.append(
                f"view '{name}' references a retired enrichment table "
                f"(gold_analyses / gold_growth_facets) — rebind to "
                f"silver_content_classification / the silver visual+text tables"
            )
    actual_views = _duckdb_list_views(con)
    for stale in sorted(set(_STALE_DUCKDB_VIEWS) & actual_views):
        violations.append(
            f"stale view '{stale}' still exists — {_STALE_DUCKDB_VIEWS[stale]}"
        )
    if check_required:
        for view in EXPECTED_DUCKDB_VIEWS:
            if view not in defs:
                violations.append(
                    f"catalog view '{view}' has no stored definition — "
                    f"run the serving assets"
                )
        for view, sources in _REQUIRED_VIEW_SOURCES.items():
            sql = defs.get(view)
            if sql is None:
                continue  # already reported above
            for source in sources:
                if not re.search(rf"\b{re.escape(source)}\b", sql):
                    violations.append(
                        f"view '{view}' does not reference required target "
                        f"source '{source}' — the rebind did not happen"
                    )
    return violations


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------


def _sqlite_connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _sqlite_list_tables(con: sqlite3.Connection) -> set[str]:
    rows = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return {r["name"] for r in rows}


def _sqlite_get_columns(con: sqlite3.Connection, table: str) -> dict[str, str]:
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return {r["name"]: r["type"].upper() for r in rows}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def state_db():
    """Skip all DuckDB tests when the file does not exist."""
    path = Path("data/state.duckdb")
    if not path.exists():
        pytest.skip("data/state.duckdb does not exist")
    return path


@pytest.fixture
def ops_db():
    """Skip all SQLite tests when the file does not exist."""
    path = Path("data/ops.sqlite")
    if not path.exists():
        pytest.skip("data/ops.sqlite does not exist")
    return path


# ---------------------------------------------------------------------------
# DuckDB: table existence
# ---------------------------------------------------------------------------


class TestDuckDBTablesExist:
    """Every expected table must exist; no stale table names; warn on extras."""

    def test_all_expected_tables_exist(self, state_db):
        con = _duckdb_connect(str(state_db))
        actual = _duckdb_list_tables(con)
        con.close()

        expected = set(EXPECTED_DUCKDB)
        missing = expected - actual
        assert not missing, (
            f"State DB is missing expected table(s): "
            f"{', '.join(sorted(missing))}\n"
            f"Run the pipeline or migration to create them."
        )

    def test_no_stale_table_names(self, state_db):
        con = _duckdb_connect(str(state_db))
        actual = _duckdb_list_tables(con)
        con.close()

        stale = {t for t in actual if t in _STALE_DUCKDB_TABLES}
        if stale:
            lines = [f"  {t} — {_STALE_DUCKDB_TABLES[t]}" for t in sorted(stale)]
            pytest.fail(
                "State DB has stale table(s) from a previous schema:\n"
                + "\n".join(lines)
            )

    def test_no_unexpected_tables(self, state_db):
        con = _duckdb_connect(str(state_db))
        actual = _duckdb_list_tables(con)
        con.close()

        expected = set(EXPECTED_DUCKDB) | set(_STALE_DUCKDB_TABLES)
        extra = actual - expected
        if extra:
            # Warn but do not fail — extra tables may be legitimate.

            warnings.warn(
                f"State DB has unexpected table(s) not in the catalog: "
                f"{', '.join(sorted(extra))}. "
                f"Add them to expected_schema.py if they are intentional."
            )


# ---------------------------------------------------------------------------
# DuckDB: column types
# ---------------------------------------------------------------------------


class TestDuckDBColumnsMatch:
    """Every expected column must exist with the correct type.

    Extra columns in the DB are tolerated (forward-compatible).
    """

    @pytest.mark.parametrize("table", sorted(EXPECTED_DUCKDB))
    def test_columns_match(self, state_db, table):
        con = _duckdb_connect(str(state_db))
        actual_tables = _duckdb_list_tables(con)

        if table not in actual_tables:
            pytest.skip(f"Table '{table}' does not exist — cannot check columns")

        actual_cols = _duckdb_get_columns(con, table)
        expected_cols = EXPECTED_DUCKDB[table]
        con.close()

        missing: list[str] = []
        type_mismatches: list[str] = []

        for col, dtype in expected_cols.items():
            if col not in actual_cols:
                missing.append(f"  {col} ({dtype})")
            elif actual_cols[col].upper() != dtype.upper():
                type_mismatches.append(
                    f"  {col}: expected {dtype}, got {actual_cols[col]}"
                )

        msg_parts: list[str] = []
        if missing:
            msg_parts.append(
                f"Missing column(s) in '{table}':\n" + "\n".join(missing)
            )
        if type_mismatches:
            msg_parts.append(
                f"Type mismatch(es) in '{table}':\n" + "\n".join(type_mismatches)
            )

        assert not msg_parts, "\n\n".join(msg_parts)


# ---------------------------------------------------------------------------
# DuckDB: views
# ---------------------------------------------------------------------------


class TestDuckDBViewsQueryable:
    """Every expected view must be SELECT-able without error."""

    @pytest.mark.parametrize("view", sorted(EXPECTED_DUCKDB_VIEWS))
    def test_view_is_queryable(self, state_db, view):
        con = _duckdb_connect(str(state_db))
        actual_views = _duckdb_list_views(con)

        if view not in actual_views:
            pytest.fail(
                f"View '{view}' does not exist — the target world (DUCKDB_VIEWS, "
                f"incl. the four ADR-0011 gold marts) requires it. Run the "
                f"serving assets; a missing view is migration drift, not a skip."
            )

        try:
            con.execute(f"SELECT * FROM {view} LIMIT 1")
        except Exception as exc:
            pytest.fail(
                f"Failed to query view '{view}': {exc}\n"
                f"This may indicate a broken view definition or "
                f"missing underlying table."
            )


# ── DuckDB: view DEFINITIONS (target world) ──────────────────────────────


class TestDuckDBViewDefinitionsTargetWorld:
    """The stored view DEFINITIONS must be the target world (US-ESA-2).

    The old readiness hole: views were only ever SELECT-able — a view
    reading the retired ``gold_analyses`` still passed. These tests read
    each definition's SQL out of the DB and assert the ADR-0011 rebind.
    """

    def test_no_view_references_retired_enrichment_tables(self, state_db):
        con = _duckdb_connect(str(state_db))
        violations = view_definition_violations(con, check_required=False)
        con.close()
        assert not violations, (
            "Serving views still reference the OLD enrichment world:\n"
            + "\n".join(f"  {v}" for v in violations)
            + "\nRun the serving assets to rebind the views."
        )

    def test_definitions_are_target_world(self, state_db):
        con = _duckdb_connect(str(state_db))
        violations = view_definition_violations(con)
        con.close()
        assert not violations, (
            "Serving view definitions are not the target world:\n"
            + "\n".join(f"  {v}" for v in violations)
            + "\nRun the serving assets to materialize the ADR-0011 rebind."
        )


# ── Old-world detectability (self-contained fixtures) ───────────────────

# Verbatim-shaped PRE-migration definitions: what the views looked like
# while they still read gold_analyses (the status quo the old readiness
# test blessed). These fixtures prove the rewritten checks FAIL on them.
_OLD_WORLD_DDL = [
    """CREATE TABLE silver_ig_posts (post_id VARCHAR PRIMARY KEY)""",
    """CREATE TABLE gold_analyses (
           post_id VARCHAR NOT NULL, domain VARCHAR NOT NULL,
           prompt_hash VARCHAR, result_json VARCHAR, admiralty VARCHAR,
           analysed_at VARCHAR, PRIMARY KEY (post_id, domain))""",
    """CREATE VIEW v_post_detail AS
           SELECT sp.post_id, g.result_json, g.prompt_hash,
                  g.analysed_at AS gold_analysed_at, g.admiralty
           FROM silver_ig_posts sp
           LEFT JOIN gold_analyses g
               ON sp.post_id = g.post_id AND g.domain = 'instagram'""",
    """CREATE VIEW v_overview AS
           SELECT (SELECT COUNT(*) FROM silver_ig_posts) AS total_posts,
                  (SELECT COUNT(*) FROM gold_analyses)   AS total_enriched""",
    """CREATE VIEW analytics_views AS
           SELECT sp.post_id FROM silver_ig_posts sp
           LEFT JOIN gold_analyses g
               ON sp.post_id = g.post_id AND g.domain = 'instagram'""",
]
_TARGET_WORLD_DDL = [
    """CREATE TABLE silver_ig_posts (post_id VARCHAR PRIMARY KEY)""",
    """CREATE TABLE silver_content_classification (
           post_id VARCHAR NOT NULL, platform VARCHAR NOT NULL,
           prompt_hash VARCHAR, result_json VARCHAR, admiralty VARCHAR,
           PRIMARY KEY (post_id, platform))""",
    """CREATE VIEW v_post_detail AS
           SELECT sp.post_id, scc.result_json, scc.prompt_hash,
                  scc.admiralty
           FROM silver_ig_posts sp
           LEFT JOIN silver_content_classification scc
               ON sp.post_id = scc.post_id AND scc.platform = 'instagram'""",
    """CREATE VIEW v_overview AS
           SELECT (SELECT COUNT(*) FROM silver_ig_posts) AS total_posts,
                  (SELECT COUNT(*) FROM silver_content_classification
                   WHERE platform = 'instagram')        AS total_enriched""",
]


def _build_world(tmp_path, ddl):
    path = str(tmp_path / "world.duckdb")
    con = duckdb.connect(path)
    for stmt in ddl:
        con.execute(stmt)
    return con


class TestRebindDetectable:
    """The rewritten readiness checks FAIL on the old world, PASS on the new."""

    def test_old_world_flags_every_retired_reference(self, tmp_path):
        con = _build_world(tmp_path, _OLD_WORLD_DDL)
        violations = view_definition_violations(con, check_required=False)
        con.close()
        flagged = {v.split("'")[1] for v in violations}
        # v_post_detail + v_overview: banned-table references in definitions;
        # analytics_views: stale view exists.
        assert {"v_post_detail", "v_overview", "analytics_views"} <= flagged
        assert any("gold_analyses" in v for v in violations)

    def test_target_world_same_grain_has_no_violations(self, tmp_path):
        con = _build_world(tmp_path, _TARGET_WORLD_DDL)
        violations = view_definition_violations(con, check_required=False)
        con.close()
        assert violations == []


# ---------------------------------------------------------------------------
# SQLite: table existence
# ---------------------------------------------------------------------------


class TestSQLiteTablesExist:
    """Every expected ops.sqlite table must exist; no stale names; warn on extras."""

    def test_all_expected_tables_exist(self, ops_db):
        con = _sqlite_connect(str(ops_db))
        actual = _sqlite_list_tables(con)
        con.close()

        expected = set(EXPECTED_SQLITE)
        missing = expected - actual
        assert not missing, (
            f"Ops DB is missing expected table(s): "
            f"{', '.join(sorted(missing))}\n"
            f"Run the pipeline or migration to create them."
        )

    def test_no_stale_table_names(self, ops_db):
        con = _sqlite_connect(str(ops_db))
        actual = _sqlite_list_tables(con)
        con.close()

        stale = {t for t in actual if t in _STALE_SQLITE_TABLES}
        if stale:
            lines = [f"  {t} — {_STALE_SQLITE_TABLES[t]}" for t in sorted(stale)]
            pytest.fail(
                "Ops DB has stale table(s) from a previous schema:\n"
                + "\n".join(lines)
            )

    def test_no_unexpected_tables(self, ops_db):
        con = _sqlite_connect(str(ops_db))
        actual = _sqlite_list_tables(con)
        con.close()

        expected = set(EXPECTED_SQLITE) | set(_STALE_SQLITE_TABLES)
        extra = actual - expected
        if extra:

            warnings.warn(
                f"Ops DB has unexpected table(s) not in the catalog: "
                f"{', '.join(sorted(extra))}. "
                f"Add them to expected_schema.py if they are intentional."
            )


# ---------------------------------------------------------------------------
# SQLite: column types
# ---------------------------------------------------------------------------


class TestSQLiteColumnsMatch:
    """Every expected column in ops.sqlite must exist with the correct type."""

    @pytest.mark.parametrize("table", sorted(EXPECTED_SQLITE))
    def test_columns_match(self, ops_db, table):
        con = _sqlite_connect(str(ops_db))
        actual_tables = _sqlite_list_tables(con)

        if table not in actual_tables:
            pytest.skip(f"Table '{table}' does not exist — cannot check columns")

        actual_cols = _sqlite_get_columns(con, table)
        expected_cols = EXPECTED_SQLITE[table]
        con.close()

        missing: list[str] = []
        type_mismatches: list[str] = []

        for col, dtype in expected_cols.items():
            if col not in actual_cols:
                missing.append(f"  {col} ({dtype})")
            elif actual_cols[col].upper() != dtype.upper():
                type_mismatches.append(
                    f"  {col}: expected {dtype}, got {actual_cols[col]}"
                )

        msg_parts: list[str] = []
        if missing:
            msg_parts.append(
                f"Missing column(s) in ops.sqlite '{table}':\n" + "\n".join(missing)
            )
        if type_mismatches:
            msg_parts.append(
                f"Type mismatch(es) in ops.sqlite '{table}':\n"
                + "\n".join(type_mismatches)
            )

        assert not msg_parts, "\n\n".join(msg_parts)


# ── Backward-compat aliases (old test code) ──────────────────────────────

# These exist so existing test classes bound to the old names still resolve.
TestStateTablesExist = TestDuckDBTablesExist
TestStateColumnsMatch = TestDuckDBColumnsMatch
TestViewsQueryable = TestDuckDBViewsQueryable
