"""Tests for US-C1 (max_charge_usd → maxTotalChargeUsd) and US-C2 (core_refresh schedule)."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from dagster import DefaultScheduleStatus, build_asset_context
from dagster_duckdb import DuckDBResource
from opsdb.roster import ensure_schema
from orchestration.defs.ig_core.bnz.scrape import ScrapeConfig, ig_posts_raw
from orchestration.defs.integration.apify_client import trigger_run
from orchestration.defs.platform.resources import SQLiteResource
from orchestration.defs.platform.schedules import (
    CORE_REFRESH_CHARGE_CAP_USD,
    core_refresh,
)
from orchestration.defs.platform.schedules import (
    core_refresh_run_requests as run_requests,
)

# ── Fixtures ───────────────────────────────────────────────────────────────


class _FakeRunInfo:
    def __init__(self):
        self.run_id = "run_1"
        self.dataset_id = "ds_1"
        self.actor = "apify~instagram-scraper"
        self.estimated_cost_usd = 0.0


class _FakeApifyResource:
    def __init__(self, token: str = "tok"):
        self.token = token


@pytest.fixture
def fake_post():
    """Patch apify._post; yields the call recorder."""
    with patch(
        "orchestration.defs.integration.apify_client._post",
        return_value={"id": "run_1", "defaultDatasetId": "ds_1", "stats": {}},
    ) as post:
        yield post


@pytest.fixture
def ops_db(tmp_path) -> SQLiteResource:
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    ensure_schema(ops)
    return ops


@pytest.fixture
def roster_db(tmp_path) -> DuckDBResource:
    """A DuckDB holding ``silver_ig_roster``, empty until a profile is added.

    The schedule reads the PUBLISHED roster (the dashboard owns the original and
    serves it over the API), so these tests publish what they would otherwise
    have written to ops.sqlite. The intent of each test — "this roster produces
    these run requests" — is unchanged.
    """
    db = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    with db.get_connection() as conn:
        from orchestration.defs.platform.schemas import duckdb_ddl

        conn.execute(duckdb_ddl("silver_ig_roster"))
    return db


def _publish(
    db: DuckDBResource,
    handle: str,
    *,
    enabled: int = 1,
    tier: str = "tier1",
    results_limit: int = 7,
    platform: str = "instagram",
) -> None:
    """Publish one profile row into ``silver_ig_roster``."""
    with db.get_connection() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO silver_ig_roster
               (platform, handle, profile_url, results_type, results_limit,
                enabled, tier, creator_id, creator_name, updated_at,
                source_fetched_at, processed_on)
               VALUES (?, ?, ?, 'details', ?, ?, ?, 1, ?, '2026-01-01T00:00:00',
                       '2026-01-01T00:00:00', NULL)""",
            [
                platform,
                handle,
                f"https://www.instagram.com/{handle}/",
                results_limit,
                bool(enabled),
                tier,
                handle,
            ],
        )


def _add_profile(ops: SQLiteResource, handle: str, *, enabled: int = 1, tier: str = "tier1"):
    conn = ops.get_connection()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO creators (name, created_at, updated_at) VALUES (?, ?, ?)",
            [handle, "2026-01-01T00:00:00", "2026-01-01T00:00:00"],
        )
        creator_id = conn.execute(
            "SELECT id FROM creators WHERE name = ?", [handle]
        ).fetchone()[0]
        conn.execute(
            """INSERT OR REPLACE INTO profiles
               (platform, handle, profile_url, results_type, results_limit,
                enabled, tier, creator_id, updated_at)
               VALUES ('instagram', ?, ?, 'details', 7, ?, ?, ?, '2026-01-01T00:00:00')""",
            [handle, f"https://www.instagram.com/{handle}/", enabled, tier, creator_id],
        )
        conn.commit()
    finally:
        conn.close()


def _invoke_bronze(tmp_path, config) -> None:
    with (
        patch("orchestration.defs.ig_core.bnz.scrape.trigger_run",
              return_value=_FakeRunInfo()) as trigger,
        patch("orchestration.defs.ig_core.bnz.scrape.poll_run", return_value="ds_fwd"),
        patch("orchestration.defs.platform.paths.BRONZE_LAKE", tmp_path),
        patch("orchestration.defs.ig_core.bnz.scrape.stream_dataset", return_value=0),
    ):
        ig_posts_raw(
            build_asset_context(),
            config=config,
            apify=_FakeApifyResource(),
            ops=SQLiteResource(database=str(tmp_path / "ops.sqlite")),
        )
    assert trigger.call_args.kwargs["max_charge_usd"] == config.max_charge_usd


# ── US-C1: ScrapeConfig + trigger_run query param ──────────────────────────


def test_scrape_config_default_charge_cap_none():
    cfg = ScrapeConfig(urls=["https://instagram.com/x"])
    assert cfg.max_charge_usd is None


def test_trigger_run_passes_charge_cap_as_query_param(fake_post):
    trigger_run("apify~instagram-scraper", ["https://instagram.com/x"],
                token="tok", max_charge_usd=0.5)
    kwargs = fake_post.call_args.kwargs
    assert kwargs["maxTotalChargeUsd"] == 0.5


def test_trigger_run_omits_charge_cap_when_none(fake_post):
    trigger_run("apify~instagram-scraper", ["https://instagram.com/x"], token="tok")
    kwargs = fake_post.call_args.kwargs
    assert "maxTotalChargeUsd" not in kwargs


def test_trigger_run_body_unchanged_by_charge_cap(fake_post):
    body = {
        "directUrls": ["https://instagram.com/x"],
        "resultsType": "posts",
        "resultsLimit": 1,
        "proxy": {"useApifyProxy": True},
    }
    trigger_run("apify~instagram-scraper", ["https://instagram.com/x"],
                token="tok", max_charge_usd=0.5)
    assert fake_post.call_args.kwargs["body"] == body
    assert fake_post.call_args.args[0] == "acts/apify~instagram-scraper/runs"


# ── US-C1: ig_posts_raw forwards the cap ───────────────────────────────────


def test_ig_posts_raw_forwards_max_charge_usd(tmp_path):
    _invoke_bronze(
        tmp_path,
        ScrapeConfig(urls=["https://instagram.com/x"], max_charge_usd=0.5),
    )


def test_ig_posts_raw_forwards_none_by_default(tmp_path):
    _invoke_bronze(tmp_path, ScrapeConfig(urls=["https://instagram.com/x"]))


# ── US-C2: core_refresh schedule ───────────────────────────────────────────


def test_core_refresh_stopped_monthly():
    assert core_refresh.name == "core_refresh"
    assert core_refresh.default_status == DefaultScheduleStatus.STOPPED
    assert core_refresh.cron_schedule == "0 4 2 * *"


def test_core_refresh_one_run_request_per_enabled_tier1_profile(roster_db):
    _publish(roster_db, "alpha")                    # enabled tier1 -> included
    _publish(roster_db, "beta", enabled=0)          # disabled -> excluded
    _publish(roster_db, "gamma", tier="tier2")      # tier2 -> excluded

    requests = run_requests(roster_db)
    assert isinstance(requests, list)
    assert len(requests) == 1
    req = requests[0]
    assert req.run_key == "core_refresh:instagram:alpha"
    cfg = req.run_config["ops"]["ig_posts_raw"]["config"]
    assert cfg == {
        "urls": ["https://www.instagram.com/alpha/"],
        "results_limit": 7,
        "results_type": "details",
        "max_charge_usd": CORE_REFRESH_CHARGE_CAP_USD,
    }


def test_core_refresh_multiple_enabled_profiles_get_separate_requests(roster_db):
    _publish(roster_db, "alpha")
    _publish(roster_db, "beta")
    requests = run_requests(roster_db)
    assert [r.run_key for r in requests] == [
        "core_refresh:instagram:alpha",
        "core_refresh:instagram:beta",
    ]


def test_core_refresh_skips_when_roster_empty(roster_db):
    result = run_requests(roster_db)
    assert not isinstance(result, list)
    assert "No enabled tier1" in str(result)


def test_core_refresh_run_config_validates_against_scrape_config(roster_db):
    """The emitted run_config must parse as a valid ScrapeConfig."""
    _publish(roster_db, "alpha")
    req = run_requests(roster_db)[0]
    cfg = req.run_config["ops"]["ig_posts_raw"]["config"]
    parsed = ScrapeConfig(**cfg)
    assert parsed.max_charge_usd == 0.50
    assert parsed.results_limit == 7
