"""Tests for US-DISC-7 (per-creator watermark sync) and US-DISC-8 (SDK transport)."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import pytest
from dagster import DefaultScheduleStatus, build_asset_context
from dagster_duckdb import DuckDBResource
from opsdb.roster import AD_HOC_LIMIT, ensure_schema
from orchestration.defs.ig_core.bnz.scrape import ScrapeConfig, bronze_ig_posts
from orchestration.defs.integration.apify_runs import RunOutcome, trigger_run
from orchestration.defs.platform.core_refresh import (
    CORE_REFRESH_CHARGE_CAP_USD,
    DEFAULT_MAX_PARALLEL_SCRAPE_RUNS,
    SCRAPE_RUN_MEMORY_MB,
    core_refresh,
)
from orchestration.defs.platform.core_refresh import (
    core_refresh_run_requests as run_requests,
)
from orchestration.defs.platform.resources import SQLiteResource

# ── Fixtures ───────────────────────────────────────────────────────────────


class _FakeRunInfo:
    def __init__(self):
        self.run_id = "run_1"
        self.dataset_id = "ds_1"
        self.actor = "apify~instagram-scraper"
        self.estimated_cost_usd = 0.0


class _FakeRun:
    """Stand-in for the SDK's ``Run`` model."""

    def __init__(self, run_id, default_dataset_id, usage_total_usd=0.0):
        self.id = run_id
        self.default_dataset_id = default_dataset_id
        self.status = "SUCCEEDED"
        self.status_message = None
        self.usage_total_usd = usage_total_usd


class _FakeActorClient:
    """Records every ``start()`` call; returns a finished-looking run."""

    def __init__(self, recorder):
        self._recorder = recorder

    def start(self, **kwargs):
        self._recorder.append(kwargs)
        return _FakeRun("run_1", "ds_1", usage_total_usd=0.0023)


class _FakeApifyClient:
    def __init__(self, recorder):
        self._recorder = recorder

    def actor(self, actor_id):
        self._recorder.append({"actor": actor_id})
        return _FakeActorClient(self._recorder)


class _FakeApifyResource:
    def __init__(self, token: str = "tok"):
        self.token = token


@pytest.fixture
def fake_client():
    """Patch ``apify_runs._client``; yields the call recorder.

    The recorder is a list of the kwargs passed to ``actor().start()`` — the
    payload and run options the SDK would have sent — interleaved with
    ``{"actor": <id>}`` entries recording which actor was addressed.
    """
    recorder: list[dict] = []
    with patch(
        "orchestration.defs.integration.apify_runs._client",
        return_value=_FakeApifyClient(recorder),
    ):
        yield recorder


def _start_kwargs(recorder) -> dict:
    """The ``start()`` kwargs from a recorder (skips the actor-id entries)."""
    starts = [entry for entry in recorder if "actor" not in entry]
    assert len(starts) == 1, f"expected one start() call, got {len(starts)}"
    return starts[0]


@pytest.fixture
def ops_db(tmp_path) -> SQLiteResource:
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    ensure_schema(ops)
    return ops


@pytest.fixture
def roster_db(tmp_path) -> DuckDBResource:
    """A DuckDB holding ``silver_ig_roster`` and ``silver_ig_posts``.

    The schedule reads the PUBLISHED roster (the dashboard owns the original and
    serves it over the API), so these tests publish what they would otherwise
    have written to ops.sqlite. ``silver_ig_posts`` is here because the
    per-profile watermark is derived from it — the schedule's date boundary is a
    query over silver, not a stored table.
    """
    db = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    with db.get_connection() as conn:
        from orchestration.defs.platform.schemas import duckdb_ddl

        for table in ("silver_ig_roster", "silver_ig_posts"):
            conn.execute(duckdb_ddl(table))
    return db


def _publish(
    db: DuckDBResource,
    handle: str,
    *,
    enabled: int = 1,
    tier: str = "tier1",
    results_limit: int = 7,
    results_type: str = "details",
    platform: str = "instagram",
) -> None:
    """Publish one profile row into ``silver_ig_roster``."""
    with db.get_connection() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO silver_ig_roster
               (platform, handle, profile_url, results_type, results_limit,
                enabled, tier, creator_id, creator_name, updated_at,
                source_fetched_at, processed_on)
               VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, '2026-01-01T00:00:00',
                       '2026-01-01T00:00:00', NULL)""",
            [
                platform,
                handle,
                f"https://www.instagram.com/{handle}/",
                results_type,
                results_limit,
                bool(enabled),
                tier,
                handle,
            ],
        )


def _observe(
    db: DuckDBResource, handle: str, timestamp: str | None
) -> None:
    """Give `handle` a post in silver at `timestamp` (None = no posts)."""
    if timestamp is None:
        return
    with db.get_connection() as conn:
        conn.execute(
            """INSERT INTO silver_ig_posts
               (post_id, url, owner_username, timestamp, source_dataset)
               VALUES (?, ?, ?, ?, ?)""",
            [
                f"{handle}-{timestamp}",
                f"https://www.instagram.com/p/{handle}-{timestamp}/",
                handle,
                timestamp,
                f"ds_{handle}",
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
        patch("orchestration.defs.ig_core.bnz.scrape.poll_run",
              return_value=RunOutcome(dataset_id="ds_fwd")),
        patch("orchestration.defs.ig_core.bnz.scrape.BRONZE_LAKE", tmp_path),
        patch("orchestration.defs.ig_core.bnz.scrape.stream_dataset", return_value=0),
    ):
        bronze_ig_posts(
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


def test_trigger_run_passes_charge_cap_as_run_option(fake_client):
    trigger_run("apify~instagram-scraper", ["https://instagram.com/x"],
                token="tok", max_charge_usd=0.5)
    assert _start_kwargs(fake_client)["max_total_charge_usd"] == Decimal("0.5")


def test_trigger_run_omits_charge_cap_when_none(fake_client):
    trigger_run("apify~instagram-scraper", ["https://instagram.com/x"], token="tok")
    assert _start_kwargs(fake_client)["max_total_charge_usd"] is None


def test_trigger_run_body_unchanged_by_charge_cap(fake_client):
    body = {
        "directUrls": ["https://instagram.com/x"],
        "resultsType": "posts",
        "resultsLimit": 1,
        "proxy": {"useApifyProxy": True},
    }
    trigger_run("apify~instagram-scraper", ["https://instagram.com/x"],
                token="tok", max_charge_usd=0.5)
    assert _start_kwargs(fake_client)["run_input"] == body
    assert {"actor": "apify~instagram-scraper"} in fake_client


# ── US-C1: bronze_ig_posts forwards the cap ───────────────────────────────────


def test_ig_posts_raw_forwards_max_charge_usd(tmp_path):
    _invoke_bronze(
        tmp_path,
        ScrapeConfig(urls=["https://instagram.com/x"], max_charge_usd=0.5),
    )


def test_ig_posts_raw_forwards_none_by_default(tmp_path):
    _invoke_bronze(tmp_path, ScrapeConfig(urls=["https://instagram.com/x"]))


# ── US-DISC-7: core_refresh schedule ───────────────────────────────────────


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
    # The key is deterministic and names the run's boundary (or backfill).
    assert requests[0].run_key == "core_refresh:instagram:00-backfill"
    cfg = requests[0].run_config["ops"]["bronze_ig_posts"]["config"]
    assert cfg == {
        "urls": ["https://www.instagram.com/alpha/"],
        "results_limit": 7,
        "results_type": "details",
        "max_charge_usd": CORE_REFRESH_CHARGE_CAP_USD,
        "only_posts_newer_than": None,
        "memory_mbytes": SCRAPE_RUN_MEMORY_MB,
    }


def test_core_refresh_skips_when_roster_empty(roster_db):
    result = run_requests(roster_db)
    assert not isinstance(result, list)
    assert "No enabled tier1" in str(result)


def test_core_refresh_run_config_validates_against_scrape_config(roster_db):
    """The emitted run_config must parse as a valid ScrapeConfig."""
    _publish(roster_db, "alpha")
    req = run_requests(roster_db)[0]
    cfg = req.run_config["ops"]["bronze_ig_posts"]["config"]
    parsed = ScrapeConfig(**cfg)
    assert parsed.max_charge_usd == 0.50
    assert parsed.results_limit == 7
    assert parsed.memory_mbytes == SCRAPE_RUN_MEMORY_MB


# ── US-DISC-7 AC 14: same-depth profiles batch into ONE run ────────────────


def test_same_depth_profiles_share_one_run_carrying_both_urls(roster_db):
    _publish(roster_db, "alpha")
    _publish(roster_db, "beta")
    requests = run_requests(roster_db)
    assert isinstance(requests, list)
    assert len(requests) == 1
    cfg = requests[0].run_config["ops"]["bronze_ig_posts"]["config"]
    assert sorted(cfg["urls"]) == [
        "https://www.instagram.com/alpha/",
        "https://www.instagram.com/beta/",
    ]
    # The cap scales with the run's URL count, not a flat per-profile figure.
    assert cfg["max_charge_usd"] == 2 * CORE_REFRESH_CHARGE_CAP_USD


def test_null_results_type_does_not_crash_the_planner(roster_db):
    """A NULL `results_type` must not take the whole tick down.

    `results_type` is a nullable VARCHAR in the roster schema and the selection
    applies no COALESCE, so NULL reaches the planner. Sorting a tuple holding
    None raises TypeError, which would abort `core_refresh_run_requests` and
    emit no runs at all — a silent-style outage where the schedule looks
    healthy and does nothing.
    """
    _publish(roster_db, "null_type", results_type=None, results_limit=7)
    _publish(roster_db, "posts_type", results_type="posts", results_limit=7)

    runs = run_requests(roster_db)
    assert isinstance(runs, list)
    # Normalised to the actor's own default rather than dropped or fatal.
    types = {
        r.run_config["ops"]["bronze_ig_posts"]["config"]["results_type"]
        for r in runs
    }
    assert types == {"posts"}
    urls = {
        u
        for r in runs
        for u in r.run_config["ops"]["bronze_ig_posts"]["config"]["urls"]
    }
    assert urls == {
        "https://www.instagram.com/null_type/",
        "https://www.instagram.com/posts_type/",
    }


def test_same_depth_different_results_type_never_share_a_run(roster_db):
    """`resultsType` is one input field per run, like `resultsLimit`.

    Two profiles at the same depth but different types must not merge: the run
    would send one type for both URLs, and the mis-typed scrape would land the
    wrong shape under the wrong `results_type` in its sidecar. Live data
    currently avoids this only because the one `details` profile sits at a
    different depth — that is luck, not a guarantee.
    """
    _publish(roster_db, "posts_profile", results_type="posts", results_limit=7)
    _publish(roster_db, "details_profile", results_type="details", results_limit=7)

    runs = run_requests(roster_db)
    by_url = {
        u: r.run_config["ops"]["bronze_ig_posts"]["config"]["results_type"]
        for r in runs
        for u in r.run_config["ops"]["bronze_ig_posts"]["config"]["urls"]
    }
    assert by_url["https://www.instagram.com/posts_profile/"] == "posts"
    assert by_url["https://www.instagram.com/details_profile/"] == "details"


def test_run_order_is_deterministic_across_rebuilds(roster_db):
    """Two groups sharing a depth must order by type, not by insertion.

    Without `results_type` in the sort key, the relative order of same-depth
    groups depends on dict insertion and the run_key position index shifts
    between evaluations — the same roster would produce different run keys.
    """
    _publish(roster_db, "posts_a", results_type="posts", results_limit=7)
    _publish(roster_db, "details_a", results_type="details", results_limit=7)
    _publish(roster_db, "posts_b", results_type="posts", results_limit=7)

    first = [r.run_key for r in run_requests(roster_db)]
    second = [r.run_key for r in run_requests(roster_db)]
    assert first == second
    # details sorts before posts at equal depth.
    types = [
        r.run_config["ops"]["bronze_ig_posts"]["config"]["results_type"]
        for r in run_requests(roster_db)
    ]
    assert types == sorted(types)


# ── US-DISC-7 AC 15: the ad-hoc sentinel is excluded ───────────────────────


def test_ad_hoc_profiles_are_excluded(roster_db):
    _publish(roster_db, "alpha")
    _publish(roster_db, "ad_hoc", results_limit=AD_HOC_LIMIT)
    requests = run_requests(roster_db)
    urls = [
        u
        for r in requests
        for u in r.run_config["ops"]["bronze_ig_posts"]["config"]["urls"]
    ]
    assert urls == ["https://www.instagram.com/alpha/"]


# ── US-DISC-7 AC 10: never-scraped selects a full backfill, never a drop ───


def test_never_scraped_profile_gets_full_backfill(roster_db):
    _publish(roster_db, "alpha")  # no silver posts -> never scraped
    cfg = run_requests(roster_db)[0].run_config["ops"]["bronze_ig_posts"]["config"]
    assert [u for u in cfg["urls"]] == ["https://www.instagram.com/alpha/"]
    assert cfg["only_posts_newer_than"] is None


def test_never_scraped_profiles_never_land_in_a_dated_run(roster_db):
    """AC 10: every never-scraped profile is in a full-backfill run.

    Fixed-size chunking of a mixed group must not sweep an unscraped profile
    into a dated chunk — it would inherit that chunk's boundary and skip the
    history it has never fetched. Order the roster so an unscraped profile sits
    past a chunk boundary of dated ones.
    """
    for i in range(3):
        _publish(roster_db, f"dated{i}", results_limit=30)
        _observe(roster_db, f"dated{i}", f"2026-0{i + 1}-01 10:00:00")
    for i in range(12):
        _publish(roster_db, f"fresh{i}", results_limit=30)  # never scraped

    runs = run_requests(roster_db)
    placed = {
        u: r.run_config["ops"]["bronze_ig_posts"]["config"]["only_posts_newer_than"]
        for r in runs
        for u in r.run_config["ops"]["bronze_ig_posts"]["config"]["urls"]
    }
    fresh_urls = [u for u in placed if "/fresh" in u]
    assert len(fresh_urls) == 12, "a never-scraped profile went missing"
    assert all(
        placed[u] is None for u in fresh_urls
    ), "a never-scraped profile inherited a dated run's boundary"


# ── US-DISC-7 AC 2: a run transmits its OLDEST member's boundary ───────────


def test_run_boundary_is_the_oldest_member_watermark(roster_db):
    _publish(roster_db, "older")
    _publish(roster_db, "newer")
    _observe(roster_db, "older", "2026-03-01 10:00:00")
    _observe(roster_db, "newer", "2026-08-01 10:00:00")

    cfg = run_requests(roster_db)[0].run_config["ops"]["bronze_ig_posts"]["config"]
    # One date per run, and it must be the earliest — a newer member's date
    # would silently exclude the older member's posts.
    assert cfg["only_posts_newer_than"] == "2026-03-01"


def test_unscraped_and_dated_profiles_split_into_separate_runs(
    roster_db,
):
    """An unscraped profile beside a dated one splits into separate runs.

    This is also where the two timezone frames meet: silver timestamps come back
    naive from DuckDB while the never-scraped sentinel is UTC-aware. That mix
    must resolve into a total order rather than raising, and it must not merge
    the two profiles — the dated one keeps its real boundary, the unscraped one
    gets the full backfill it needs.
    """
    _publish(roster_db, "dated")
    _publish(roster_db, "fresh")
    _observe(roster_db, "dated", "2026-06-15 10:00:00")

    runs = run_requests(roster_db)
    by_url = {
        u: r.run_config["ops"]["bronze_ig_posts"]["config"]["only_posts_newer_than"]
        for r in runs
        for u in r.run_config["ops"]["bronze_ig_posts"]["config"]["urls"]
    }
    assert by_url["https://www.instagram.com/dated/"] == "2026-06-15"
    assert by_url["https://www.instagram.com/fresh/"] is None


# ── US-DISC-7 AC 6: the fan-out is capped ──────────────────────────────────


def test_fanout_is_capped_at_max_parallel_runs(roster_db):
    # 200 profiles at one per run would be 200 runs without the cap.
    for i in range(200):
        _publish(roster_db, f"p{i:03d}", results_limit=1)
    requests = run_requests(roster_db)
    assert len(requests) == DEFAULT_MAX_PARALLEL_SCRAPE_RUNS


# ── US-DISC-7 AC 7 / AC 5: the pre-flight guard names the limit it hit ─────


def test_fanout_guard_rejects_too_many_parallel_runs(roster_db):
    _publish(roster_db, "alpha")
    with pytest.raises(RuntimeError, match="32"):
        run_requests(roster_db, max_parallel_runs=33)


def test_fanout_guard_rejects_exceeding_account_memory(monkeypatch):
    from orchestration.defs.platform import core_refresh as cr

    monkeypatch.setattr(cr, "ACCOUNT_MAX_MEMORY_MB", 2048)
    with pytest.raises(RuntimeError, match="combined-memory ceiling"):
        cr.validate_fanout(4, memory_mb=1024, max_parallel_runs=16)


# ── US-DISC-7 AC 8: no profile is scraped twice in one tick ────────────────


def test_no_url_appears_in_two_runs(roster_db):
    for i in range(25):
        _publish(roster_db, f"p{i:03d}", results_limit=(12 if i % 2 else 30))
    urls = [
        u
        for r in run_requests(roster_db)
        for u in r.run_config["ops"]["bronze_ig_posts"]["config"]["urls"]
    ]
    assert len(urls) == len(set(urls)) == 25


# ── US-DISC-7 AC 4: the boundary is a top-level input, memory a run option ─


def test_payload_carries_boundary_and_memory(fake_client):
    trigger_run(
        "apify~instagram-scraper",
        ["https://instagram.com/x"],
        token="tok",
        results_limit=12,
        only_posts_newer_than="2026-03-01",
        memory_mbytes=2048,
    )
    kwargs = _start_kwargs(fake_client)
    assert kwargs["run_input"]["onlyPostsNewerThan"] == "2026-03-01"
    assert "memoryMbytes" not in kwargs["run_input"]
    assert kwargs["memory_mbytes"] == 2048


def test_boundary_is_omitted_entirely_when_none(fake_client):
    trigger_run("apify~instagram-scraper", ["https://instagram.com/x"], token="tok")
    assert "onlyPostsNewerThan" not in _start_kwargs(fake_client)["run_input"]
