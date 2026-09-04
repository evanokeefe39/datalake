"""US-A1/A2/A3 — ``silver_ig_profile_observations``: append gate, idempotency,
provenance chain, and the fabricated-0 regression guard.

Mirrors ``tests/unit/instagram/test_silver_observations.py``: real DuckDB +
Parquet over tmp fixtures with ``BRONZE_LAKE`` patched to ``tmp_path``. The
real ``data/state.duckdb`` is NEVER touched.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from unittest.mock import patch

import polars as pl
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource

from datalake.defs.common.schemas import duckdb_ddl
from datalake.defs.instagram.assets import _profile_observations, ig_profiles_slv
from scripts.migrate_backfill_profile_observations import (
    apply_backfill,
    collect_observations,
)

# ── Helpers ───────────────────────────────────────────────────────────────


def _run_profiles(tmp_path, ops, duckdb):
    with patch("datalake.defs.instagram.assets.BRONZE_LAKE", tmp_path):
        context = build_asset_context(resources={"duckdb": duckdb, "ops": ops})
        return ig_profiles_slv(context)


def _write_details(fp, rows, downloaded_at: str | None = None) -> None:
    """Write a details-type bronze file (profile id lives in ``id``)."""
    pl.DataFrame(rows).write_parquet(fp)
    if downloaded_at:
        fp.with_suffix(".parquet.meta").write_text(
            json.dumps({"downloaded_at": downloaded_at}), encoding="utf-8"
        )


def _write_posts(fp, rows, downloaded_at: str | None = None) -> None:
    """Write a posts-type bronze file (owner id lives in ``ownerId``)."""
    pl.DataFrame(rows).write_parquet(fp)
    if downloaded_at:
        fp.with_suffix(".parquet.meta").write_text(
            json.dumps({"downloaded_at": downloaded_at}), encoding="utf-8"
        )


def _profile_obs(duckdb):
    with duckdb.get_connection() as conn:
        return conn.execute(
            "SELECT owner_id, owner_username, observed_at, followers_count, "
            "follows_count, posts_count, is_verified, source_dataset "
            "FROM silver_ig_profile_observations "
            "ORDER BY owner_id, source_dataset"
        ).fetchall()


def _pk_cols(con, table):
    return [
        r[1]
        for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()
        if r[5]  # pk flag
    ]


def test_ddl_creates_table_with_pk(tmp_path):
    """``duckdb_ddl`` emits CREATE TABLE IF NOT EXISTS with the full PK."""
    import duckdb as duckdb_mod

    from datalake.defs.common.schemas import DUCKDB_TABLES

    ddl = duckdb_ddl("silver_ig_profile_observations")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS silver_ig_profile_observations")

    con = duckdb_mod.connect(str(tmp_path / "ddl.duckdb"))
    try:
        con.execute(ddl)
        cols = dict(
            con.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = 'silver_ig_profile_observations' "
                "ORDER BY ordinal_position"
            ).fetchall()
        )
        assert list(cols) == list(DUCKDB_TABLES["silver_ig_profile_observations"])
        pk_cols = _pk_cols(con, "silver_ig_profile_observations")
        assert pk_cols == ["owner_id", "observed_at", "source_dataset"]
    finally:
        con.close()


# ── Append gate + provenance (US-A2.1) ────────────────────────────────────


def test_details_file_appends_observation_with_meta_provenance(tmp_path, ops):
    """A details bronze file appends one observation per profile; observed_at
    comes from the meta sidecar downloaded_at."""
    fp = tmp_path / "ds_001.parquet"
    _write_details(
        fp,
        [
            {
                "id": "111",
                "username": "alice",
                "followersCount": 1500,
                "followsCount": 300,
                "postsCount": 42,
                "verified": False,
            },
            {
                "id": "222",
                "username": "bob",
                "followersCount": 90,
                "followsCount": 100,
                "postsCount": 7,
                "verified": True,
            },
        ],
        downloaded_at="2026-03-01T09:30:00+00:00",
    )
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    _run_profiles(tmp_path, ops, duckdb)

    obs = _profile_obs(duckdb)
    assert len(obs) == 2
    by_owner = {r[0]: r for r in obs}
    assert by_owner["111"][1] == "alice"
    assert by_owner["111"][2] == datetime(2026, 3, 1, 9, 30, tzinfo=timezone.utc)
    assert by_owner["111"][3] == 1500
    assert by_owner["111"][4] == 300
    assert by_owner["111"][5] == 42
    assert by_owner["111"][6] is False
    assert by_owner["111"][7] == "ds_001"


def test_observed_at_falls_back_to_file_mtime(tmp_path, ops):
    """No meta sidecar → the bronze file mtime is the observation time."""
    fp = tmp_path / "ds_001.parquet"
    _write_details(
        fp, [{"id": "111", "username": "alice", "followersCount": 1500}]
    )
    stamp = 1_800_000_000
    os.utime(fp, (stamp, stamp))
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    _run_profiles(tmp_path, ops, duckdb)

    obs = _profile_obs(duckdb)
    assert len(obs) == 1
    assert obs[0][2] == datetime.fromtimestamp(stamp, tz=timezone.utc)


def test_posts_file_without_followers_count_emits_nothing(tmp_path, ops):
    """FABRICATED-0 REGRESSION: a posts file that lacks followersCount (only
    ownerId/ownerUsername) must emit ZERO observation rows — the writer must
    not record the defaulted followers_count=0 as a real observation."""
    fp = tmp_path / "ds_posts_nofol.parquet"
    _write_posts(
        fp,
        [
            {"id": "p1", "shortCode": "abc", "ownerId": "333", "username": "carol"},
            {"id": "p2", "shortCode": "def", "ownerId": "333", "username": "carol"},
        ],
        downloaded_at="2026-03-02T10:00:00+00:00",
    )
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    result = _run_profiles(tmp_path, ops, duckdb)

    assert _profile_obs(duckdb) == []
    # The profile row may still be upserted (existing behavior) but must not
    # produce an observation.
    assert result is not None


def test_posts_file_with_followers_count_appends(tmp_path, ops):
    """A posts file where the actor embedded the owner object DOES observe."""
    fp = tmp_path / "ds_posts_fol.parquet"
    _write_posts(
        fp,
        [
            {
                "id": "p1",
                "shortCode": "abc",
                "ownerId": "444",
                "username": "dave",
                "followersCount": 187137,
                "followsCount": 292,
                "postsCount": 179,
                "verified": False,
            },
            {
                "id": "p2",
                "shortCode": "def",
                "ownerId": "444",
                "username": "dave",
                "followersCount": 187137,
                "followsCount": 292,
                "postsCount": 179,
                "verified": False,
            },
        ],
        downloaded_at="2026-03-03T08:00:00+00:00",
    )
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    _run_profiles(tmp_path, ops, duckdb)

    obs = _profile_obs(duckdb)
    # Multiple post rows from the same owner collapse to ONE observation.
    assert len(obs) == 1
    assert obs[0][0] == "444"
    assert obs[0][3] == 187137


def test_row_with_null_followers_count_excluded(tmp_path, ops):
    """A row whose followersCount is null (e.g. an Apify error row) is not
    observed even in an otherwise-valid details file."""
    fp = tmp_path / "ds_err.parquet"
    _write_details(
        fp,
        [
            {"id": "111", "username": "alice", "followersCount": None},
            {"id": "222", "username": "bob", "followersCount": 90},
        ],
        downloaded_at="2026-03-01T09:30:00+00:00",
    )
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    _run_profiles(tmp_path, ops, duckdb)

    obs = _profile_obs(duckdb)
    assert [r[0] for r in obs] == ["222"]


def test_gate_unit_details_vs_posts(tmp_path):
    """``_profile_observations`` gate, unit-level: entity-type drives the
    owner-id column and a missing followersCount short-circuits to None."""
    details = pl.DataFrame(
        {"id": ["111"], "username": ["alice"], "followersCount": [1500]}
    )
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    obs = _profile_observations(details, "details", "ds", ts)
    assert obs is not None and obs["owner_id"][0] == "111"

    posts = pl.DataFrame(
        {"id": ["p1"], "shortCode": ["abc"], "ownerId": ["333"], "username": ["carol"]}
    )
    assert _profile_observations(posts, "posts", "ds", ts) is None

    # Details without the id column (degenerate) → gated off.
    broken = pl.DataFrame({"username": ["alice"], "followersCount": [1500]})
    assert _profile_observations(broken, "details", "ds", ts) is None


# ── Idempotency (US-A2.1) ─────────────────────────────────────────────────


def test_reprocessing_same_file_is_noop(tmp_path, ops):
    """Re-ingesting the SAME dataset (mtime bumped past the watermark) adds
    zero observation rows."""
    fp = tmp_path / "ds_001.parquet"
    _write_details(
        fp,
        [{"id": "111", "username": "alice", "followersCount": 1500}],
        downloaded_at="2026-03-01T09:30:00+00:00",
    )
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    _run_profiles(tmp_path, ops, duckdb)
    assert len(_profile_obs(duckdb)) == 1

    os.utime(fp, (2_000_000_000, 2_000_000_000))
    _run_profiles(tmp_path, ops, duckdb)
    assert len(_profile_obs(duckdb)) == 1


def test_rescrape_new_dataset_adds_exactly_one_observation(tmp_path, ops):
    """A re-scrape under a NEW source_dataset appends one row; the original
    observation (original provenance) is preserved untouched."""
    fp1 = tmp_path / "ds_001.parquet"
    _write_details(
        fp1,
        [{"id": "111", "username": "alice", "followersCount": 1500}],
        downloaded_at="2026-03-01T09:30:00+00:00",
    )
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))

    _run_profiles(tmp_path, ops, duckdb)
    original = _profile_obs(duckdb)

    fp2 = tmp_path / "ds_002.parquet"
    _write_details(
        fp2,
        [{"id": "111", "username": "alice", "followersCount": 1520}],
        downloaded_at="2026-03-10T09:30:00+00:00",
    )
    _run_profiles(tmp_path, ops, duckdb)

    obs = _profile_obs(duckdb)
    assert len(obs) == 2
    assert original[0] in obs
    assert {(r[0], r[7]) for r in obs} == {("111", "ds_001"), ("111", "ds_002")}
    assert obs[1][3] == 1520  # the newer count is the newer observation


# ── Backfill script (US-A3.1) ─────────────────────────────────────────────


def test_backfill_idempotent_and_provenance_correct(tmp_path):
    """collect → apply twice: second pass adds zero; observed_at is the
    ORIGINAL scrape time from the meta sidecar, never the run time."""
    bronze = tmp_path / "bronze"
    bronze.mkdir()
    fp = bronze / "ds_backfill.parquet"
    _write_details(
        fp,
        [
            {"id": "111", "username": "alice", "followersCount": 1500},
            {"id": "222", "username": "bob", "followersCount": 90},
        ],
        downloaded_at="2026-02-15T12:00:00+00:00",
    )
    # A posts file without followersCount must be gated out of the backfill.
    _write_posts(
        bronze / "ds_backfill_posts.parquet",
        [{"id": "p1", "shortCode": "abc", "ownerId": "333", "username": "carol"}],
        downloaded_at="2026-02-16T12:00:00+00:00",
    )

    db_path = str(tmp_path / "state.duckdb")
    import duckdb as duckdb_mod

    db = duckdb_mod.connect(db_path)
    try:
        db.execute(duckdb_ddl("silver_ig_profile_observations"))

        rows = collect_observations(bronze_dir=bronze)
        assert len(rows) == 2
        assert {r[0] for r in rows} == {"111", "222"}
        assert all(
            r[2] == datetime(2026, 2, 15, 12, 0, tzinfo=timezone.utc) for r in rows
        )

        first = apply_backfill(db, rows)
        assert first == 2
        second = apply_backfill(db, rows)  # idempotent re-run
        assert second == 0

        total = db.execute(
            "SELECT COUNT(*) FROM silver_ig_profile_observations"
        ).fetchone()[0]
        assert total == 2
    finally:
        db.close()


def test_backfill_mtime_fallback(tmp_path):
    """No meta sidecar → backfill uses the file mtime as observed_at."""
    bronze = tmp_path / "bronze"
    bronze.mkdir()
    fp = bronze / "ds_mtime.parquet"
    _write_details(fp, [{"id": "111", "username": "alice", "followersCount": 1500}])
    stamp = 1_750_000_000
    os.utime(fp, (stamp, stamp))

    rows = collect_observations(bronze_dir=bronze)
    assert len(rows) == 1
    assert rows[0][2] == datetime.fromtimestamp(stamp, tz=timezone.utc)
