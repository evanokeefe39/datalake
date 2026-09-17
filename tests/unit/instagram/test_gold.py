"""Tests for the drain-based enrichment architecture.

Verifies:
- Retired queue primitives refuse a queueless ops.sqlite — the retirement
  stands (ADR-0012)
- ig_posts_gen_batches asset behaviour — the enqueue is Dagster-native: one
  ``enrichment_submitted`` partition per post on the INJECTED instance
- SQLiteResource integration
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from dagster import AssetKey, AssetMaterialization, build_asset_context

from orchestration.defs.platform.resources import DuckDBResource, SQLiteResource
import orchestration.defs.ig_enriched.slv.classification as classification
from orchestration.defs.engine.partitions import (
    HARVESTED_ASSET_NAME,
    SUBMITTED_ASSET_NAME,
    partition_key,
)
from orchestration.defs.ig_core.slv import posts as ig_assets_mod
from orchestration.defs.ig_enriched.slv.workloads import ig_posts_gen_batches
from orchestration.defs.ig_enriched.slv.workloads import DRAIN_WORKLOAD


def _run_drain(instance, *args, **kwargs):
    """Run the drain asset with a fake PartitionSnapshot injected via the
    module-level test-only override (the asset reads context.instance in
    production; fakes cannot ride build_asset_context)."""
    ig_assets_mod._drain_instance = instance
    try:
        return ig_posts_gen_batches(*args, **kwargs)
    finally:
        ig_assets_mod._drain_instance = None


_ATTEMPT_ROUND = 0  # first attempts enqueue at round 0 (ADR-0014 D1 grammar)
# ── Helpers ──────────────────────────────────────────────────────────────────

def _pd(post_id: str, domain: str = "instagram") -> str:
    """Build a Gemini-consumer payload string."""
    return json.dumps({"post_id": post_id, "domain": domain})


def _make_ops_db(tmp_path):
    return SQLiteResource(database=str(tmp_path / "ops.sqlite"))


def _make_duckdb(tmp_path):
    return DuckDBResource(database=str(tmp_path / "state.duckdb"))


def _seed_silver(db, rows):
    """Seed silver_ig_posts with (post_id, caption, processed_on) tuples."""
    with db.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_ig_posts (
                post_id TEXT PRIMARY KEY, caption TEXT,
                processed_on TIMESTAMP, timestamp TIMESTAMP,
                source_dataset TEXT NOT NULL DEFAULT '',
                url TEXT, shortcode TEXT, owner_id TEXT, owner_username TEXT,
                likes_count INTEGER, comments_count INTEGER,
                video_play_count INTEGER, video_view_count INTEGER,
                hashtags TEXT, meta_data TEXT,
                has_engagement_bait BOOLEAN, media_files TEXT, media_count INTEGER
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ig_post_labels (
                post_id VARCHAR PRIMARY KEY,
                label VARCHAR NOT NULL,
                method VARCHAR NOT NULL,
                enrich_decision VARCHAR NOT NULL,
                judged_at TIMESTAMP WITH TIME ZONE NOT NULL,
                is_provisional BOOLEAN NOT NULL,
                label_version INTEGER NOT NULL,
                baseline_center DOUBLE,
                baseline_spread DOUBLE,
                baseline_n INTEGER
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS watermarks (
                name TEXT PRIMARY KEY, timestamp TIMESTAMP NOT NULL, config_hash TEXT
            )
        """)
    for post_id, caption, ts in rows:
        with db.get_connection() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO silver_ig_posts "
                "(post_id, caption, processed_on, timestamp, source_dataset) "
                "VALUES (?, ?, ?, ?, 'test')",
                [post_id, caption, ts, ts],
            )




class FakeInstance:
    """Minimal DagsterInstance stand-in.

    Mirrors tests/unit/instagram/test_drain_inflight_guard.py::FakeInstance.
    In-memory submitted/harvested partition sets; ``submit``/``harvest``
    simulate the two enrichment stages materializing one per-post partition
    (i.e. work that happened OUTSIDE the drain, before the test); the
    ``add_dynamic_partitions``/``report_runless_asset_event`` surface is
    what the drain's enqueue writes at run time.
    """

    def __init__(self, submitted=(), harvested=()):
        self._submitted = set(submitted)
        self._harvested = set(harvested)
        self._dynamic_registered: set[str] = set()

    def get_materialized_partitions(self, asset_key):
        """STRICT: the real DagsterInstance takes an AssetKey on Dagster
        1.13.x — reject anything else instead of silently returning empty."""
        if asset_key == AssetKey(SUBMITTED_ASSET_NAME):
            return set(self._submitted)
        if asset_key == AssetKey(HARVESTED_ASSET_NAME):
            return set(self._harvested)
        raise TypeError(
            f"get_materialized_partitions expects an AssetKey, "
            f"got {type(asset_key).__name__}"
        )

    def add_dynamic_partitions(self, partitions_def_name, partition_keys):
        """Runless partition registration. STRICT: the drain only ever
        registers the ``enrichment_submitted`` dynamic space."""
        if partitions_def_name != SUBMITTED_ASSET_NAME:
            raise TypeError(
                f"add_dynamic_partitions expects {SUBMITTED_ASSET_NAME!r}, "
                f"got {partitions_def_name!r}"
            )
        self._dynamic_registered.update(partition_keys)

    def report_runless_asset_event(self, event):
        """Runless event-log write. STRICT: only a partition-scoped
        AssetMaterialization for the two enrichment spaces lands here."""
        if not isinstance(event, AssetMaterialization):
            raise TypeError(
                f"report_runless_asset_event expects an AssetMaterialization, "
                f"got {type(event).__name__}"
            )
        if event.partition is None:
            raise ValueError("runless enrichment events must carry a partition")
        if event.asset_key == AssetKey(SUBMITTED_ASSET_NAME):
            self._submitted.add(event.partition)
        elif event.asset_key == AssetKey(HARVESTED_ASSET_NAME):
            self._harvested.add(event.partition)
        else:
            raise TypeError(f"unexpected asset key {event.asset_key}")

    def submitted_partitions(self):
        return set(self._submitted)

    def harvested_partitions(self):
        return set(self._harvested)

    def dynamic_partitions(self):
        return set(self._dynamic_registered)

    def submit(self, post_ids):
        for pid in post_ids:
            self._submitted.add(
                partition_key(DRAIN_WORKLOAD, _ATTEMPT_ROUND, [pid])
            )

    def harvest(self, post_ids):
        for pid in post_ids:
            self._harvested.add(
                partition_key(DRAIN_WORKLOAD, _ATTEMPT_ROUND, [pid])
            )


def _seed_classification(db, rows):
    """Seed silver_content_classification (the new completion-guard source)
    with (post_id, prompt_hash) tuples at platform='instagram', via the
    canonical CLASSIFICATION_DDL — never a hand-rolled schema."""
    with db.get_connection() as conn:
        conn.execute(classification_mod.CLASSIFICATION_DDL)
        for post_id, prompt_hash in rows:
            conn.execute(
                "INSERT OR REPLACE INTO silver_content_classification "
                "(post_id, platform, prompt_hash) VALUES (?, 'instagram', ?)",
                [post_id, prompt_hash],
            )


def _seed_labels(db, rows):
    """Seed ig_post_labels with (post_id, decision, method, version) tuples."""
    from datetime import timezone as _tz

    from orchestration.defs.ig_core.slv.labels import LABEL_VERSION

    with db.get_connection() as conn:
        for post_id, decision, method, version in rows:
            conn.execute(
                "INSERT OR REPLACE INTO ig_post_labels "
                "(post_id, label, method, enrich_decision, judged_at, "
                " is_provisional, label_version) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    post_id, "standout" if decision == "standout" else "average",
                    method, decision,
                    datetime.now(_tz.utc), method != "day7_matched",
                    version if version is not None else LABEL_VERSION,
                ],
            )


# ── Enqueue asset tests ─────────────────────────────────────────────────────


def test_enqueue_asset_writes_batch(tmp_path):
    """GIVEN label-approved posts in silver
    WHEN ig_posts_gen_batches runs with an INJECTED instance
    THEN one enrichment_submitted partition per approved post is materialized
    on that instance (the which-posts observable — the retired queue stored
    the same intent as batch_items rows).
    """
    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)
    instance = FakeInstance()

    now = datetime.now(timezone.utc)
    _seed_silver(db, [("p1", "Test caption", now), ("p2", "Another caption", now)])
    _seed_labels(db, [("p1", "standout", "day7_matched", None),
                      ("p2", "control", "day0_heuristic", None)])

    result = _run_drain(instance, build_asset_context(), duckdb=db, ops=ops)

    assert result["enqueued"][0] == 2
    assert result["candidates_seen"][0] == 2

    assert instance.submitted_partitions() == {
        partition_key(DRAIN_WORKLOAD, _ATTEMPT_ROUND, [pid])
        for pid in ("p1", "p2")
    }


def _run_enqueue(tmp_path, config=None):
    """Seed one label-approved post and run ig_posts_gen_batches with an
    INJECTED FakeInstance. Returns (result, instance, surfaced_mode) — the
    mode the drain SURFACES in the result frame. No tier faking: the drain
    no longer consults GeminiTierConfig (ADR-0009/0012 retirement) — the
    submit stage owns the execution mode and the provider readiness gate.
    """
    from orchestration.defs.ig_core.bnz.scrape import GoldConfig

    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)
    instance = FakeInstance()

    now = datetime.now(timezone.utc)
    _seed_silver(db, [("p1", "Test caption", now)])
    _seed_labels(db, [("p1", "standout", "day7_matched", None)])

    result = _run_drain(
        instance,
        build_asset_context(),
        duckdb=db, ops=ops, config=config or GoldConfig()
    )
    return result, instance, result["mode"][0]


def test_enqueue_surfaces_seam_mode_for_curated_selection(tmp_path):
    """GIVEN a curated (non-whole-corpus) label-approved selection
    WHEN ig_posts_gen_batches runs
    THEN the drain enqueues each approved post and surfaces ``seam`` mode
    (the submit stage owns execution through the seam; ADR-0012).
    """
    result, _instance, mode = _run_enqueue(tmp_path)
    assert result["enqueued"][0] == 1
    assert mode == "seam"


def test_enqueue_surfaces_seam_mode_whole_corpus(tmp_path):
    """GIVEN whole_corpus admission
    WHEN ig_posts_gen_batches runs
    THEN the drain still surfaces ``seam`` mode.
    """
    from orchestration.defs.ig_core.bnz.scrape import GoldConfig

    _result, _instance, mode = _run_enqueue(
        tmp_path, config=GoldConfig(whole_corpus=True)
    )
    assert mode == "seam"


def test_enqueue_mode_is_independent_of_retired_tier_selection(tmp_path):
    """GIVEN the drain runs with default GoldConfig (no tier consulted)
    WHEN ig_posts_gen_batches runs
    THEN the surfaced mode is ``seam`` — the retired gemini-batch/interactive
    tier split no longer exists at the drain (ADR-0009/0012 retirement);
    GeminiTierConfig.detect() is only consumed by the submit-stage adapters.
    """
    _result, _instance, mode = _run_enqueue(tmp_path)
    assert mode == "seam"


def test_enqueue_tolerates_prefer_interactive_opt_out(tmp_path):
    """GIVEN an operator sets GoldConfig(prefer_interactive=True)
    WHEN ig_posts_gen_batches runs
    THEN the drain still enqueues and surfaces ``seam`` mode — the retired
    interactive opt-out is inert at the drain; execution mode is decided by
    the submit stage through the seam.
    """
    from orchestration.defs.ig_core.bnz.scrape import GoldConfig

    _result, _instance, mode = _run_enqueue(
        tmp_path, config=GoldConfig(prefer_interactive=True)
    )
    assert mode == "seam"


def test_enqueue_skips_current_prompt_enriched(tmp_path):
    """GIVEN a label-approved post with a CURRENT-prompt conformed
    classification in silver_content_classification (the new completion-guard
    source, post-ADR-0011)
    WHEN ig_posts_gen_batches runs
    THEN that post is not re-batched (only stale-prompt rows re-enqueue, US-L5).
    """
    from orchestration.defs.ig_enriched.slv.prompts import CURRENT_PROMPT_HASH

    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)
    instance = FakeInstance()

    now = datetime.now(timezone.utc)
    _seed_silver(db, [("p1", "Test caption", now), ("p2", "Already done", now)])
    _seed_labels(db, [("p1", "standout", "day7_matched", None),
                      ("p2", "standout", "day7_matched", None)])
    _seed_classification(db, [("p2", CURRENT_PROMPT_HASH)])

    result = _run_drain(instance, build_asset_context(), duckdb=db, ops=ops)

    assert result["enqueued"][0] == 1
    # Exactly p1 was enqueued (was: the claimed batch's payload post_ids).
    assert instance.submitted_partitions() == {
        partition_key(DRAIN_WORKLOAD, _ATTEMPT_ROUND, ["p1"])
    }


def test_enqueue_reenqueues_stale_prompt_gold(tmp_path):
    """GIVEN a label-approved post whose conformed classification was written
    under a stale (pre-multimodal) prompt_hash in silver_content_classification
    WHEN ig_posts_gen_batches runs
    THEN the post IS re-enqueued — no permanent orphaning (US-L5).
    """
    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)
    instance = FakeInstance()

    now = datetime.now(timezone.utc)
    _seed_silver(db, [("p1", "Test caption", now)])
    _seed_labels(db, [("p1", "standout", "day7_matched", None)])
    _seed_classification(db, [("p1", "stale-pre-multimodal-hash")])

    result = _run_drain(instance, build_asset_context(), duckdb=db, ops=ops)
    assert result["enqueued"][0] == 1
    assert instance.submitted_partitions() == {
        partition_key(DRAIN_WORKLOAD, _ATTEMPT_ROUND, ["p1"])
    }


def test_enqueue_skips_skip_decision(tmp_path):
    """GIVEN a post whose label decision is 'skip' (e.g. empty caption —
    the label pass owns the skip, US-L6)
    WHEN ig_posts_gen_batches runs
    THEN the post is not batched.
    """
    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)
    instance = FakeInstance()

    now = datetime.now(timezone.utc)
    _seed_silver(db, [("p1", "   ", now)])
    _seed_labels(db, [("p1", "skip", "day0_heuristic", None)])

    result = _run_drain(instance, build_asset_context(), duckdb=db, ops=ops)
    assert result["enqueued"][0] == 0

    # Nothing was enqueued: no partition materialized (was: claim_batch is
    # None — the queue observable).
    assert instance.submitted_partitions() == set()


def test_enqueue_skips_open_batch_items(tmp_path):
    """GIVEN a label-approved post whose partition is in flight on the Dagster
    instance (submitted, not yet harvested)
    WHEN ig_posts_gen_batches runs
    THEN the post is not re-enqueued while its work is in flight (US-EENG-4).
    """
    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)
    now = datetime.now(timezone.utc)

    _seed_silver(db, [("p1", "Caption", now)])
    _seed_labels(db, [("p1", "standout", "day7_matched", None)])
    instance = FakeInstance()
    instance.submit(["p1"])  # partition materialized by the submit stage

    result = _run_drain(instance, build_asset_context(), duckdb=db, ops=ops)
    assert result["enqueued"][0] == 0
    assert result["in_flight_suppressed"][0] == 1


def test_enqueue_no_pending_posts(tmp_path):
    """GIVEN no label-approved posts
    WHEN ig_posts_gen_batches runs
    THEN it enqueues nothing.
    """
    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)
    instance = FakeInstance()

    _seed_silver(db, [])

    result = _run_drain(instance, build_asset_context(), duckdb=db, ops=ops)
    assert result["enqueued"][0] == 0
    assert result["candidates_seen"][0] == 0
    assert instance.submitted_partitions() == set()


def test_enqueue_post_ids_bypasses_guards(tmp_path):
    """GIVEN posts with CURRENT-prompt conformed classifications in
    silver_content_classification and no labels
    WHEN ig_posts_gen_batches runs with post_ids
    THEN the requested posts are batched regardless (explicit bypass).
    """
    from orchestration.defs.ig_enriched.slv.prompts import CURRENT_PROMPT_HASH
    from orchestration.defs.ig_core.bnz.scrape import GoldConfig

    db = _make_duckdb(tmp_path)
    ops = _make_ops_db(tmp_path)
    instance = FakeInstance()

    now = datetime.now(timezone.utc)
    _seed_silver(db, [
        ("p1", "Caption one", now),
        ("p2", "Caption two", now),
        ("p3", "Caption three", now),
    ])

    _seed_classification(
        db, [(pid, CURRENT_PROMPT_HASH) for pid in ("p2", "p3")]
    )

    result = _run_drain(
        instance,
        build_asset_context(),
        config=GoldConfig(post_ids=["p2", "p3"]), duckdb=db, ops=ops,
    )

    assert result["enqueued"][0] == 2
    # Exactly p2+p3 were enqueued (was: the claimed batch's payload post_ids).
    assert instance.submitted_partitions() == {
        partition_key(DRAIN_WORKLOAD, _ATTEMPT_ROUND, [pid])
        for pid in ("p2", "p3")
    }
