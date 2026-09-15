"""W8 blocking DQ gates — each check proven to FIRE on the malformed
condition and PASS on the clean one (ADR-0012 anti-join, quarantine
growth, snapshot freshness) plus the ``v_quarantine_triage`` consumer.

A check that only ever passes is the defect this unit exists to fix: every
test here asserts the FAILING side (``passed is False``) on injected bad
state, not just the green side. Tests use tmp roots and in-memory/tmp DuckDB
— never the live lake or live state.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import polars as pl
import pytest
from dagster_duckdb import DuckDBResource

from datalake.defs.common import lake
from datalake.defs.enrichment import checks as checks_mod
from datalake.defs.enrichment import conform as conform_mod
from datalake.defs.enrichment import landing as landing_mod
from datalake.defs.enrichment.checks import (
    QUARANTINE_BASELINE_TABLE,
    anti_join_losses,
    quarantine_growth,
    stale_snapshot_files,
)
from datalake.defs.enrichment.conform import SILVER_QUARANTINE
from datalake.defs.enrichment.growth_facets_schema import (
    GROWTH_FACETS_SCHEMA_VERSION,
)
from datalake.defs.enrichment.landing import WORKLOAD_GROWTH_FACETS_VISUAL

# Import the module under test for the payload builders (mirror the
# validated shapes; keeps this file free of duplicated fixtures).
from tests.unit.enrichment.test_conform import (
    text_payload,  # noqa: F401  (payload builders)
    visual_payload,  # noqa: F401
    _land_visual,
)

NOW = datetime(2026, 9, 10, 12, 0, 0, tzinfo=timezone.utc)


# ── Fixtures / helpers ─────────────────────────────────────────────────────


@pytest.fixture
def roots(tmp_path, monkeypatch):
    """Point the checks' lake roots at tmp dirs (module attrs are read at
    call time), and give every test a fresh bronze + silver root."""
    bronze = tmp_path / "lake" / "bronze"
    silver = tmp_path / "lake" / "silver"
    monkeypatch.setattr(lake, "BRONZE_LAKE", bronze)
    monkeypatch.setattr(lake, "SILVER_LAKE", silver)
    return bronze, silver


def _conform(bronze: Path, silver: Path) -> duckdb.DuckDBPyConnection:
    """Land one clean visual response, conform it, register into :memory:."""
    _land_visual(bronze)
    conn = duckdb.connect(":memory:")
    conform_mod.conform(root=bronze, silver_root=silver, conn=conn, now=NOW)
    return conn


def _duckdb_resource(conn: duckdb.DuckDBPyConnection) -> DuckDBResource:
    """Stand-in resource exposing the check functions' get_connection()."""
    import contextlib

    class _Conn:
        @contextlib.contextmanager
        def get_connection(self):
            yield conn

    return _Conn()  # type: ignore[return-value]


def _conformed_keys(conn, table: str) -> set[tuple[str, str]]:
    return {
        (r[0], r[1])
        for r in conn.execute(f"SELECT post_id, platform FROM {table}").fetchall()
    }


def _quarantined_keys(conn) -> set[tuple[str, str, str]]:
    return {
        (r[0], r[1], r[2])
        for r in conn.execute(
            f"SELECT post_id, platform, workload FROM {SILVER_QUARANTINE}"
        ).fetchall()
    }


# ── 1. Anti-join: landed(bronze) \ conformed(silver) ───────────────────────


def test_anti_join_passes_on_clean_conform(roots):
    """Clean world: bronze's every key is conformed → no losses."""
    bronze, silver = roots
    conn = _conform(bronze, silver)
    bronze = landing_mod.read_responses(bronze)
    losses = anti_join_losses(
        conform_mod._latest_per_key(bronze),
        {t: _conformed_keys(conn, t) for t in conform_mod.SILVER_TABLES},
        _quarantined_keys(conn),
    )
    assert losses == []


def test_anti_join_fires_on_silent_loss(roots):
    """THE W8 condition: a bronze key with NEITHER a conformed row NOR a
    quarantine row — the ISSUES.md #26 shape. Firing must be observable as
    passed=False on the actual asset check, not just a helper list."""
    bronze, silver = roots
    conn = _conform(bronze, silver)
    bronze_df = landing_mod.read_responses(bronze)
    quarantined = _quarantined_keys(conn)

    # Inject the silent loss: drop BOTH visual counterparts without adding
    # a quarantine row (what a wrong-table done-guard effectively produced).
    ann, summ = (
        conform_mod.SILVER_VISUAL_ANNOTATIONS,
        conform_mod.SILVER_VISUAL_SUMMARIES,
    )
    conn.execute(f"DELETE FROM {ann} WHERE post_id = 'p1'")
    conn.execute(f"DELETE FROM {summ} WHERE post_id = 'p1'")
    conformed = {t: _conformed_keys(conn, t) for t in conform_mod.SILVER_TABLES}

    losses = anti_join_losses(
        conform_mod._latest_per_key(bronze_df), conformed, quarantined
    )
    assert losses and losses[0]["post_id"] == "p1"

    # The real check FIRES on that state and PASSES on the clean state.
    result = checks_mod.check_no_silent_loss(_duckdb_resource(conn))
    assert result.passed is False
    assert result.metadata["silent_losses"].value == 1


def test_anti_join_tolerates_quarantined_rows(roots):
    """A quarantined key is NOT a loss — quarantine is an accounted outcome."""
    bronze, silver = roots
    # Land an ok=False response → conform quarantines it (provider_error).
    _land_visual(bronze, payload=None, ok=False, error_message="boom")
    conn = _conform(bronze, silver)
    bronze_df = landing_mod.read_responses(bronze)
    losses = anti_join_losses(
        conform_mod._latest_per_key(bronze_df),
        {t: _conformed_keys(conn, t) for t in conform_mod.SILVER_TABLES},
        _quarantined_keys(conn),
    )
    assert losses == []


# ── 2. Quarantine growth ────────────────────────────────────────────────────


def test_quarantine_growth_fires_on_spike_and_stays_failing():
    conn = duckdb.connect(":memory:")
    # First observation: baseline recorded, passes.
    fired, meta = quarantine_growth(conn, 10)
    assert fired is False and meta["previous_baseline"] is None
    # Flat: passes.
    fired, meta = quarantine_growth(conn, 10)
    assert fired is False and meta["previous_baseline"] == 10
    # Shrink: passes, baseline follows down.
    fired, _ = quarantine_growth(conn, 5)
    assert fired is False
    # THE CONDITION: growth past the baseline FIRES…
    fired, meta = quarantine_growth(conn, 6)
    assert fired is True
    # …and the baseline is NOT advanced, so it keeps firing on re-runs
    # (a spike cannot be silently absorbed by re-running the check).
    fired, meta = quarantine_growth(conn, 6)
    assert fired is True and meta["previous_baseline"] == 5


def test_quarantine_growth_check_end_to_end(roots):
    """The asset check fires when the quarantine snapshot grows vs baseline."""
    bronze, silver = roots
    _land_visual(bronze, payload=None, ok=False, error_message="boom")
    conn = _conform(bronze, silver)
    resource = _duckdb_resource(conn)

    first = checks_mod.check_quarantine_growth(resource)
    assert first.passed is True  # baseline recorded

    # Grow the quarantine: land a second failed response under a new key and
    # re-conform (snapshot now has 2 quarantine rows).
    _land_visual(bronze, post_id="p2", payload=None, ok=False, error_message="boom2")
    conform_mod.conform(root=bronze, silver_root=silver, conn=conn, now=NOW)

    second = checks_mod.check_quarantine_growth(resource)
    assert second.passed is False
    assert second.metadata["quarantined"].value == 2

    # Clean again: shrink the snapshot back → passes, baseline follows.
    conn.execute(f"DELETE FROM {SILVER_QUARANTINE} WHERE post_id = 'p2'")
    assert checks_mod.check_quarantine_growth(resource).passed is True


# ── 3. Freshness / volume expectations ─────────────────────────────────────


def test_freshness_passes_when_snapshots_are_current(roots):
    bronze, silver = roots
    conn = _conform(bronze, silver)
    conn.close()  # ensure all snapshot writes are flushed before mtime math
    files = {
        tid: conform_mod.table_path(tid, silver) for tid in conform_mod.SILVER_TABLES
    }
    files[SILVER_QUARANTINE] = conform_mod.table_path(SILVER_QUARANTINE, silver)
    assert stale_snapshot_files(
        landing_mod.response_path(bronze), files
    ) == []


def test_freshness_fires_on_stale_and_missing_snapshots(roots):
    bronze, silver = roots
    _conform(bronze, silver)
    # Backdate every silver snapshot: bronze is now newer than silver.
    old = (datetime.now(timezone.utc) - timedelta(hours=1)).timestamp()
    files = {
        tid: conform_mod.table_path(tid, silver) for tid in conform_mod.SILVER_TABLES
    }
    files[SILVER_QUARANTINE] = conform_mod.table_path(SILVER_QUARANTINE, silver)
    for p in files.values():
        os.utime(p, (old, old))
    stale = stale_snapshot_files(landing_mod.response_path(bronze), files)
    assert stale  # FIRES
    assert set(stale) == set(files)

    # A MISSING snapshot while bronze exists also fires (volume violation).
    files[SILVER_QUARANTINE].unlink()
    assert SILVER_QUARANTINE in stale_snapshot_files(
        landing_mod.response_path(bronze), files
    )

    # A missing BRONZE file is a dormant source — healthy, passes.
    assert (
        stale_snapshot_files(bronze / "nonexistent.parquet", files) == []
    )


def test_freshness_check_end_to_end(roots):
    """The asset check itself fires on the stale state."""
    bronze, silver = roots
    _conform(bronze, silver)
    old = (datetime.now(timezone.utc) - timedelta(hours=1)).timestamp()
    for tid in (*conform_mod.SILVER_TABLES, SILVER_QUARANTINE):
        os.utime(conform_mod.table_path(tid, silver), (old, old))
    resource = _duckdb_resource(duckdb.connect(":memory:"))
    result = checks_mod.check_silver_snapshot_freshness(resource)
    assert result.passed is False
    assert result.metadata["stale_snapshots"]


# ── 4. v_quarantine_triage — the quarantine table's named consumer ─────────


def test_v_quarantine_triage_joins_bronze_and_post_context(roots):
    from datalake.defs.serving.assets import v_quarantine_triage as view_asset

    bronze, silver = roots
    # One quarantined row (provider failure) under a post with context.
    _land_visual(
        bronze, post_id="p1", payload=None, ok=False, error_message="boom"
    )
    conn = _conform(bronze, silver)

    # Post context: silver_ig_posts row joined on post_id.
    conn.execute(
        "CREATE OR REPLACE TABLE silver_ig_posts AS "
        "SELECT 'p1' AS post_id, 'demo_author' AS owner_username, "
        "'demo caption' AS caption, TIMESTAMP '2026-09-01' AS timestamp"
    )

    view_asset(_duckdb_resource(conn))

    rows = conn.execute(
        "SELECT post_id, reason_code, bronze_error_message, "
        "bronze_response_text, owner_username, caption "
        "FROM v_quarantine_triage WHERE post_id = 'p1'"
    ).fetchall()
    assert rows, "triage view must surface the quarantined row"
    post_id, reason, bronze_err, bronze_text, owner, caption = rows[0]
    assert reason == conform_mod.REASON_PROVIDER_ERROR
    assert bronze_err == "boom"
    assert owner == "demo_author"
    assert caption == "demo caption"
    # LEFT JOIN contract: even with an EMPTY bronze file the row survives
    # (bronze_* columns NULL; the quarantine row never disappears).
    pl.DataFrame(schema=landing_mod.SCHEMA).write_parquet(
        bronze / "bronze_enrichment_raw.parquet"
    )
    view_asset(_duckdb_resource(conn))
    rows = conn.execute(
        "SELECT reason_code, bronze_error_message FROM v_quarantine_triage"
    ).fetchall()
    assert rows[0][0] == conform_mod.REASON_PROVIDER_ERROR
    assert rows[0][1] is None
