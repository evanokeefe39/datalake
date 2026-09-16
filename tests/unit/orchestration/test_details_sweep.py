"""The roster-driven details sweep: reconciliation, not a request side effect.

The dashboard used to fire a details scrape from the HTTP request that added a
profile. The sweep replaces it, and the load-bearing difference is RETRYABILITY:
a request-triggered scrape that fails is gone, while a swept one stays due.

The property that makes that true is where the watermark moves. It advances when
`ig_profile_details_raw` SUCCEEDS — never at schedule-evaluation time. If it
advanced at evaluation, a tick that emitted ten runs and had three fail would
leave those three permanently skipped (the watermark would already be past
them), which is silent loss of paid intent. These tests pin that ordering.
"""

from __future__ import annotations

import pytest
from dagster import RunRequest, SkipReason
from dagster_duckdb import DuckDBResource
from orchestration.defs.platform import details_sweep as ds
from orchestration.defs.platform.schemas import duckdb_ddl

_EPOCH = "1970-01-01 00:00:00"


@pytest.fixture
def roster_db(tmp_path) -> DuckDBResource:
    db = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    with db.get_connection() as conn:
        conn.execute(duckdb_ddl("silver_ig_roster"))
        conn.execute(duckdb_ddl("watermarks"))
    return db


def _profile(
    db: DuckDBResource,
    handle: str,
    *,
    updated_at: str,
    results_type: str = "details",
    enabled: bool = True,
) -> None:
    with db.get_connection() as conn:
        conn.execute(
            "INSERT INTO silver_ig_roster VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                "instagram",
                handle,
                f"https://www.instagram.com/{handle}/",
                results_type,
                1,
                enabled,
                "tier1",
                1,
                handle,
                updated_at,
                "2026-01-01T00:00:00+00:00",
                None,
            ],
        )


# ── What is due ─────────────────────────────────────────────────────────────


def test_details_typed_profiles_are_due(roster_db):
    _profile(roster_db, "a", updated_at="2026-01-01 00:00:00")
    with roster_db.get_connection() as conn:
        assert [p["handle"] for p in ds.profiles_needing_details(conn)] == ["a"]


def test_posts_typed_profiles_are_not_due(roster_db):
    """Only `details` rows describe a profile scrape; posts are core refresh."""
    _profile(roster_db, "a", updated_at="2026-01-01 00:00:00", results_type="posts")
    with roster_db.get_connection() as conn:
        assert ds.profiles_needing_details(conn) == []


def test_disabled_profiles_are_not_due(roster_db):
    _profile(roster_db, "a", updated_at="2026-01-01 00:00:00", enabled=False)
    with roster_db.get_connection() as conn:
        assert ds.profiles_needing_details(conn) == []


def test_oldest_unswept_rows_go_first(roster_db):
    """A capped tick drains a backlog in order instead of starving a row."""
    for h, ua in (
        ("newest", "2026-03-01 00:00:00"),
        ("oldest", "2026-01-01 00:00:00"),
        ("middle", "2026-02-01 00:00:00"),
    ):
        _profile(roster_db, h, updated_at=ua)
    with roster_db.get_connection() as conn:
        got = [p["handle"] for p in ds.profiles_needing_details(conn, max_profiles=2)]
    assert got == ["oldest", "middle"]


# ── The watermark's ordering (the retryability property) ────────────────────


def test_evaluation_does_not_advance_the_watermark(roster_db):
    """Emitting a run is not doing it: until it executes, the row stays due.

    This is what makes a failed scrape retryable. If evaluation advanced the
    watermark, every emitted run would be marked done before it ran.
    """
    _profile(roster_db, "a", updated_at="2026-01-01 00:00:00")
    out = ds.details_sweep_run_requests(roster_db)
    assert isinstance(out, list) and len(out) == 1

    with roster_db.get_connection() as conn:
        assert ds._watermark(conn).strftime("%Y-%m-%d %H:%M:%S") == _EPOCH
        assert [p["handle"] for p in ds.profiles_needing_details(conn)] == ["a"]


def test_success_advances_and_retires_the_row(roster_db):
    _profile(roster_db, "a", updated_at="2026-01-01 00:00:00")
    with roster_db.get_connection() as conn:
        ds.advance_watermark(conn, through="2026-01-01 00:00:00")
    with roster_db.get_connection() as conn:
        assert ds.profiles_needing_details(conn) == []


def test_watermark_never_moves_backwards(roster_db):
    """A slow run finishing after a newer one must not re-open swept profiles."""
    with roster_db.get_connection() as conn:
        ds.advance_watermark(conn, through="2026-05-01 00:00:00")
        ds.advance_watermark(conn, through="2026-01-01 00:00:00")
        assert ds._watermark(conn).strftime("%Y-%m-%d") == "2026-05-01"


def test_capped_sweep_leaves_the_backlog_due(roster_db):
    for h, ua in (("a", "2026-01-01 00:00:00"), ("b", "2026-01-02 00:00:00")):
        _profile(roster_db, h, updated_at=ua)
    out = ds.details_sweep_run_requests(roster_db, max_profiles=1)
    assert isinstance(out, list) and len(out) == 1
    with roster_db.get_connection() as conn:
        # Nothing was executed, so nothing is retired.
        assert len(ds.profiles_needing_details(conn, max_profiles=10)) == 2


# ── Run requests ────────────────────────────────────────────────────────────


def test_idle_tick_returns_a_skip_reason(roster_db):
    out = ds.details_sweep_run_requests(roster_db)
    assert isinstance(out, SkipReason)


def test_run_request_carries_the_roster_identity(roster_db):
    """The run needs platform/handle/updated_at to record its own completion."""
    _profile(roster_db, "a", updated_at="2026-01-01 00:00:00")
    req = ds.details_sweep_run_requests(roster_db)[0]
    assert isinstance(req, RunRequest)
    cfg = req.run_config["ops"]["ig_profile_details_raw"]["config"]
    assert cfg["profile_url"] == "https://www.instagram.com/a/"
    assert cfg["roster_updated_at"] == "2026-01-01 00:00:00"
    assert cfg["max_charge_usd"] == ds.DETAILS_CHARGE_CAP_USD


def test_run_key_is_stable_for_an_unswept_profile(roster_db):
    """Two ticks before either run lands must dedupe, not double-charge."""
    _profile(roster_db, "a", updated_at="2026-01-01 00:00:00")
    first = ds.details_sweep_run_requests(roster_db)[0]
    second = ds.details_sweep_run_requests(roster_db)[0]
    assert first.run_key == second.run_key


def test_run_key_changes_when_the_roster_row_changes(roster_db):
    _profile(roster_db, "a", updated_at="2026-01-01 00:00:00")
    first = ds.details_sweep_run_requests(roster_db)[0].run_key

    with roster_db.get_connection() as conn:
        conn.execute(
            "UPDATE silver_ig_roster SET updated_at = '2026-06-01 00:00:00' "
            "WHERE handle = 'a'"
        )
    second = ds.details_sweep_run_requests(roster_db)[0].run_key
    assert first != second


def test_sweep_ships_stopped(roster_db):
    """It spends money — enablement is the owner's deliberate act."""
    from dagster import DefaultScheduleStatus

    assert ds.details_sweep.default_status == DefaultScheduleStatus.STOPPED
