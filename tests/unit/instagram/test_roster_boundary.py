"""The roster boundary: bronze landing and the published silver table.

The contract this defends is the OWNERSHIP boundary. The dashboard owns
`creators`/`profiles` and serves them; the pipeline lands that list as bronze and
publishes a DuckDB copy its assets read. Two properties matter and are asserted
here rather than assumed:

1. **The fetch fails loudly.** An unreachable dashboard must not read downstream
   as "no creators configured" — that would silently stop all scraping.
2. **The published readers agree with the source implementation.** The roster the
   pipeline acts on must be the same list the dashboard would have given it,
   including the ad-hoc exclusion, or the cutover quietly changed behaviour.
"""

from __future__ import annotations

import polars as pl
import pytest
from dagster_duckdb import DuckDBResource
from orchestration.defs.ig_core.bnz import roster as roster_mod
from orchestration.defs.ig_core.slv.roster import creator_map, enabled_profiles
from orchestration.defs.platform.schemas import duckdb_ddl


@pytest.fixture
def bronze_root(tmp_path, monkeypatch):
    """Isolated roster snapshot root — never the live lake."""
    root = tmp_path / "ig_roster_raw"
    monkeypatch.setattr(roster_mod, "ROSTER_BRONZE_DIR", root)
    return root


def _payload(fetched_at: str = "2026-01-01T00:00:00+00:00") -> dict:
    return {
        "fetched_at": fetched_at,
        "creators": [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}],
        "profiles": [
            {
                "platform": "instagram",
                "handle": "alpha",
                "profile_url": "https://www.instagram.com/alpha/",
                "results_type": "details",
                "results_limit": 12,
                "enabled": True,
                "tier": "tier1",
                "creator_id": 1,
                "updated_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "platform": "instagram",
                "handle": "beta",
                "profile_url": "https://www.instagram.com/beta/",
                "results_type": "posts",
                "results_limit": -1,  # AD_HOC_LIMIT: excluded from scraping
                "enabled": True,
                "tier": "tier1",
                "creator_id": 2,
                "updated_at": "2026-01-01T00:00:00+00:00",
            },
        ],
    }


class _Resp:
    def __init__(self, payload, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.text = str(payload)

    def json(self):
        return self._payload


# ── Bronze landing ──────────────────────────────────────────────────────────


def test_lands_one_snapshot_with_creator_name_joined(bronze_root, monkeypatch):
    monkeypatch.setattr(
        roster_mod.httpx, "get", lambda url, timeout: _Resp(_payload())
    )
    df = roster_mod.ig_roster_raw()

    assert df.height == 2
    names = dict(zip(df["handle"].to_list(), df["creator_name"].to_list()))
    assert names == {"alpha": "alpha", "beta": "beta"}
    assert len(list(bronze_root.glob("*.parquet"))) == 1


def test_same_fetched_at_writes_no_second_snapshot(bronze_root, monkeypatch):
    """Re-fetching an unchanged roster must be a no-op, not a duplicate."""
    monkeypatch.setattr(
        roster_mod.httpx, "get", lambda url, timeout: _Resp(_payload())
    )
    roster_mod.ig_roster_raw()
    roster_mod.ig_roster_raw()

    assert len(list(bronze_root.glob("*.parquet"))) == 1


def test_unreachable_dashboard_raises_loudly(bronze_root, monkeypatch):
    """An empty roster must never be invented from a transport failure."""

    def boom(url, timeout):
        raise roster_mod.httpx.ConnectError("refused")

    monkeypatch.setattr(roster_mod.httpx, "get", boom)
    with pytest.raises(RuntimeError, match="unreachable"):
        roster_mod.ig_roster_raw()


def test_non_200_raises_loudly(bronze_root, monkeypatch):
    monkeypatch.setattr(
        roster_mod.httpx, "get", lambda url, timeout: _Resp({"detail": "no"}, 503)
    )
    with pytest.raises(RuntimeError, match="HTTP 503"):
        roster_mod.ig_roster_raw()


def test_latest_snapshot_is_the_newest_by_name(bronze_root, monkeypatch):
    """Ordering is by name (the API timestamp), not mtime."""
    for i in (1, 2):
        monkeypatch.setattr(
            roster_mod.httpx,
            "get",
            lambda url, timeout, i=i: _Resp(_payload(f"2026-01-0{i}T00:00:00+00:00")),
        )
        roster_mod.ig_roster_raw()
    latest = roster_mod.latest_roster_path()
    assert latest is not None
    assert "2026-01-02" in latest.name


# ── Readers agree with the source implementation ────────────────────────────


@pytest.fixture
def published(tmp_path, monkeypatch, bronze_root) -> DuckDBResource:
    """A DuckDB with the roster published from a landed snapshot."""
    monkeypatch.setattr(
        roster_mod.httpx, "get", lambda url, timeout: _Resp(_payload())
    )
    roster_mod.ig_roster_raw()

    from orchestration.defs.ig_core.slv.roster import ig_roster_slv

    db = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    with db.get_connection() as conn:
        conn.execute(duckdb_ddl("silver_ig_roster"))
    ig_roster_slv(db)
    return db


def test_enabled_profiles_excludes_ad_hoc(published):
    """beta carries AD_HOC_LIMIT and must not be scheduled for a scrape."""
    with published.get_connection() as conn:
        handles = [p["handle"] for p in enabled_profiles(conn)]
    assert handles == ["alpha"]


def test_creator_map_resolves_handles(published):
    with published.get_connection() as conn:
        cm = creator_map(conn)
    assert cm["alpha"] == {"creator_id": 1, "creator_name": "alpha"}


def test_republishing_replaces_rather_than_unions(published, bronze_root, monkeypatch):
    """A snapshot is a MIRROR of one fetch — a union would keep disabled rows."""
    from orchestration.defs.ig_core.slv.roster import ig_roster_slv

    updated = _payload("2026-02-01T00:00:00+00:00")
    updated["profiles"] = [updated["profiles"][0]]  # beta removed from the roster
    updated["creators"] = [updated["creators"][0]]
    monkeypatch.setattr(roster_mod.httpx, "get", lambda url, timeout: _Resp(updated))
    roster_mod.ig_roster_raw()
    ig_roster_slv(published)

    with published.get_connection() as conn:
        handles = sorted(
            r[0] for r in conn.execute("SELECT handle FROM silver_ig_roster").fetchall()
        )
    assert handles == ["alpha"], "the retired profile must not survive a republish"


def test_no_snapshot_leaves_table_unchanged(published, bronze_root):
    """Dashboard down + nothing landed: stale, never emptied."""
    for f in bronze_root.glob("*.parquet"):
        f.unlink()
    for f in bronze_root.glob("*.meta"):
        f.unlink()

    from orchestration.defs.ig_core.slv.roster import ig_roster_slv

    before = None
    with published.get_connection() as conn:
        before = conn.execute("SELECT count(*) FROM silver_ig_roster").fetchone()[0]
    out = ig_roster_slv(published)
    with published.get_connection() as conn:
        after = conn.execute("SELECT count(*) FROM silver_ig_roster").fetchone()[0]

    assert isinstance(out, pl.DataFrame) and out.height == 0
    assert after == before > 0, "an absent snapshot must not clear the table"
