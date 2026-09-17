"""US-EENG-4 — the discovery drain derives in-flight state from Dagster.

Proves that ``ig_posts_gen_batches``:

* never double-submits: two consecutive runs over the same corpus enqueue no
  post twice (AC1) — in-flight suppression is instance-derived (AC2);
* suppresses a post whose partition is submitted-but-not-harvested, and does
  NOT suppress once that partition is harvested;
* still honours the candidate filters (``enrich_decision``, ``label_version``)
  and the explicit-``post_ids`` bypass (drain contract preserved);
* names in-flight work in its skip path (AC4) — never a bare "no run";
* agrees with the accounting identity on the definition of "in flight" —
  both go through ``partitions.in_flight_partitions`` (AC5); and
* guards completion against ``silver_content_classification``, not
  ``gold_analyses`` (AC6, post-ADR-0011).

The instance is INJECTED (a fake ``PartitionSnapshot``); no test touches a
real Dagster instance or the real ``data/state.duckdb``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from dagster import AssetKey, AssetMaterialization, build_asset_context

from orchestration.defs.platform.resources import DuckDBResource, SQLiteResource
from orchestration.defs.platform.schemas import duckdb_ddl
import orchestration.defs.ig_enriched.slv.classification as classification
from orchestration.defs.engine.partitions import (
    HARVESTED_ASSET_NAME,
    SUBMITTED_ASSET_NAME,
    in_flight_partitions,
    partition_key,
)
from orchestration.defs.ig_core.slv import posts as ig_assets
from orchestration.defs.ig_enriched.slv.workloads import drain_in_flight_keys, drain_suppressed_post_ids, ig_posts_gen_batches
from orchestration.defs.ig_enriched.slv.workloads import DRAIN_WORKLOAD
from orchestration.defs.ig_enriched.slv.workloads import GoldConfig
from orchestration.defs.ig_core.slv.labels import LABEL_VERSION

NOW = datetime(2026, 9, 1, tzinfo=timezone.utc)


class FakeInstance:
    """Minimal DagsterInstance stand-in: in-memory submitted/harvested
    partition sets plus the runless-materialization surface the drain's
    enqueue writes (``add_dynamic_partitions`` +
    ``report_runless_asset_event``)."""

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
        """Simulate an EXTERNAL submit (a submit that happened before this
        test, outside the drain) materializing one partition per post."""
        for pid in post_ids:
            self._submitted.add(partition_key(DRAIN_WORKLOAD, 0, [pid]))

    def harvest(self, post_ids):
        """Simulate the harvest stage materializing those same partitions."""
        for pid in post_ids:
            self._harvested.add(partition_key(DRAIN_WORKLOAD, 0, [pid]))


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture()
def db(tmp_path):
    return DuckDBResource(database=str(tmp_path / "state.duckdb"))


@pytest.fixture()
def conn(db):
    with db.get_connection() as c:
        for t in ("silver_ig_posts", "ig_post_labels"):
            c.execute(duckdb_ddl(t))
        c.execute(classification.CLASSIFICATION_DDL)
        yield c


@pytest.fixture()
def ops(tmp_path):
    return SQLiteResource(database=str(tmp_path / "ops.sqlite"))


@pytest.fixture()
def drain(conn, db, ops):
    """Call the asset directly.

    No tier patch: the drain no longer consults GeminiTierConfig — the
    execution mode is surfaced as ``seam`` and the submit stage owns the
    provider readiness gate (ADR-0012 retirement cleanup).
    """

    def run(config=None, instance=None):
        ig_assets._drain_instance = instance
        try:
            return ig_posts_gen_batches(
                build_asset_context(),
                config=config or GoldConfig(prefer_interactive=True),
                duckdb=db,
                ops=ops,
            )
        finally:
            ig_assets._drain_instance = None

    return run


def _post(conn, post_id):
    ts = NOW - timedelta(days=60)
    conn.execute(
        "INSERT INTO silver_ig_posts (post_id, owner_id, owner_username, caption, "
        "likes_count, timestamp, processed_on, source_dataset) "
        "VALUES (?, 'u1', 'u1', 'caption', 10, ?, ?, 'test')",
        [post_id, ts, ts],
    )


def _label(conn, post_id, decision="standout", version=LABEL_VERSION):
    conn.execute(
        "INSERT INTO ig_post_labels (post_id, label, method, enrich_decision, "
        "is_provisional, label_version, baseline_center, baseline_spread, "
        "baseline_n, judged_at) "
        "VALUES (?, 'standout', 'rule', ?, FALSE, ?, 0, 1, 3, ?)",
        [post_id, decision, version, NOW],
    )


def _conform(conn, post_id, prompt_hash):
    conn.execute(
        "INSERT INTO silver_content_classification (post_id, platform, prompt_hash) "
        "VALUES (?, 'instagram', ?)",
        [post_id, prompt_hash],
    )


def _enqueued(frame):
    return frame["enqueued"][0]


# ── AC1: two consecutive runs never double-submit ───────────────────────────


def test_two_consecutive_runs_enqueue_no_post_twice(drain, conn):
    for pid in ("p1", "p2", "p3"):
        _post(conn, pid)
        _label(conn, pid)
    fake = FakeInstance()

    first = drain(instance=fake)
    assert _enqueued(first) == 3
    # THE DRAIN materialized the three per-post submitted partitions — no
    # test-side submit simulation. The second run therefore reads an
    # in-flight state written by the production write path.
    assert fake.submitted_partitions() == {
        partition_key(DRAIN_WORKLOAD, 0, [pid])
        for pid in ("p1", "p2", "p3")
    }

    # Second run over the SAME corpus against the SAME instance: every post
    # is still in flight (first run's materializations are visible).
    second = drain(instance=fake)
    assert _enqueued(second) == 0
    assert second["in_flight_suppressed"][0] == 3
    # The second run enqueued nothing: no new partitions materialized.
    assert fake.submitted_partitions() == {
        partition_key(DRAIN_WORKLOAD, 0, [pid])
        for pid in ("p1", "p2", "p3")
    }


def test_one_post_done_others_in_flight_still_no_double_submit(drain, conn):
    from orchestration.defs.ig_enriched.slv.prompts import CURRENT_PROMPT_HASH

    for pid in ("p1", "p2", "p3"):
        _post(conn, pid)
        _label(conn, pid)
    fake = FakeInstance()
    assert _enqueued(drain(instance=fake)) == 3
    assert fake.submitted_partitions() == {
        partition_key(DRAIN_WORKLOAD, 0, [pid])
        for pid in ("p1", "p2", "p3")
    }

    _conform(conn, "p2", CURRENT_PROMPT_HASH)  # p2 completes while 1,3 fly

    second = drain(instance=fake)
    # p2 blocked by the completion guard; p1/p3 by the in-flight guard —
    # zero re-enqueues either way.
    assert _enqueued(second) == 0
    assert second["candidates_seen"][0] == 2
    assert second["in_flight_suppressed"][0] == 2


def test_enqueue_partition_keys_equal_guard_derived_keys(drain, conn):
    """THE contract-agreement property (US-EENG-4/ADR-0012): the keys the
    drain materializes at enqueue are EXACTLY the keys the in-flight guard
    derives for the same posts — per post, same workload, same round.

    The guard suppresses a post by deriving
    ``partition_key(WORKLOAD, ROUND, [pid])`` and testing membership in
    ``in_flight_partitions``. If the enqueue wrote any other key shape
    (e.g. a batch-level key over the whole candidate set), the two sets
    would never intersect and the drain would silently double-submit.
    """
    for pid in ("a1", "a2", "a3"):
        _post(conn, pid)
        _label(conn, pid)
    fake = FakeInstance()

    result = drain(instance=fake)
    assert _enqueued(result) == 3

    enqueued_keys = fake.get_materialized_partitions(
        AssetKey(SUBMITTED_ASSET_NAME)
    )
    guard_derived = {
        partition_key(DRAIN_WORKLOAD, 0, [pid])
        for pid in ("a1", "a2", "a3")
    }
    # The enqueue wrote exactly the keys the guard derives — no more, no less.
    assert enqueued_keys == guard_derived
    # Registered in the dynamic partition space too (the real-instance
    # sequence: add_dynamic_partitions, then the runless materialization).
    assert fake.dynamic_partitions() == guard_derived
    # Therefore the guard suppresses every enqueued post on read:
    assert drain_suppressed_post_ids(["a1", "a2", "a3"], fake) == [
        "a1", "a2", "a3"
    ]
    # Negative control pinning the historical failure mode: a per-BATCH key
    # over the whole candidate set is no longer a legal key at all — the D1
    # grammar is strictly per-post (exactly one post id).
    with pytest.raises(ValueError):
        partition_key(DRAIN_WORKLOAD, 0, ["a1", "a2", "a3"])


def test_two_consecutive_runs_real_instance_no_double_submit(drain, conn):
    """AC1 against a REAL DagsterInstance (ephemeral, in-memory): the drain's
    runless materializations must be visible to the second run's guard. The
    FakeInstance tests prove the drain's suppress logic; this one proves the
    write path (``add_dynamic_partitions`` + ``report_runless_asset_event``)
    lands in the instance's materialization registry the guard actually
    reads — the API shape pinned in
    ``tests/unit/enrichment/test_partitions.py``.
    """
    from dagster import DagsterInstance

    for pid in ("r1", "r2"):
        _post(conn, pid)
        _label(conn, pid)
    real = DagsterInstance.ephemeral()

    first = drain(instance=real)
    assert _enqueued(first) == 2
    assert in_flight_partitions(real) == {
        partition_key(DRAIN_WORKLOAD, 0, [pid])
        for pid in ("r1", "r2")
    }

    second = drain(instance=real)
    assert _enqueued(second) == 0
    assert second["in_flight_suppressed"][0] == 2


# ── submitted-not-harvested suppresses; harvested does not ──────────────────


def test_submitted_not_harvested_suppresses(drain, conn):
    _post(conn, "p1")
    _label(conn, "p1")
    fake = FakeInstance()
    fake.submit(["p1"])

    result = drain(instance=fake)
    assert _enqueued(result) == 0
    assert result["in_flight_suppressed"][0] == 1


def test_harvested_partition_does_not_suppress(drain, conn):
    _post(conn, "p1")
    _label(conn, "p1")
    fake = FakeInstance()
    fake.submit(["p1"])
    fake.harvest(["p1"])  # responses landed: no longer in flight

    result = drain(instance=fake)
    assert _enqueued(result) == 1  # re-eligible (completion guard governs next)
    # Re-eligible means the drain re-enqueued at the NEXT round (ADR-0014 D3):
    # the round-0 key stays harvested and is never re-materialized.
    # Round 0 (run 1) + round 1 (run 2's re-enqueue) are both materialized;
    # the run-2 enqueue ADDED r1 rather than re-materializing r0.
    assert fake.submitted_partitions() == {
        partition_key(DRAIN_WORKLOAD, 0, ["p1"]),
        partition_key(DRAIN_WORKLOAD, 1, ["p1"]),
    }


# ── drain contract preserved: filters + post_ids bypass ────────────────────


def test_candidate_filters_still_honoured(drain, conn):
    _post(conn, "p_ok")
    _label(conn, "p_ok", decision="standout")
    _post(conn, "p_skip")
    _label(conn, "p_skip", decision="skip")
    _post(conn, "p_stale")
    _label(conn, "p_stale", version=LABEL_VERSION - 1)
    _post(conn, "p_empty")
    _label(conn, "p_empty", decision="floor_filler", version=LABEL_VERSION)

    result = drain(instance=FakeInstance())
    # p_ok + p_empty pass; skip and stale-version labels are excluded.
    assert result["candidates_seen"][0] == 2
    assert _enqueued(result) == 2


def test_post_ids_bypass_bypasses_all_guards(drain, conn):
    from orchestration.defs.ig_enriched.slv.prompts import CURRENT_PROMPT_HASH

    _post(conn, "p1")
    _label(conn, "p1")
    _conform(conn, "p1", CURRENT_PROMPT_HASH)  # completion guard would block
    fake = FakeInstance()
    fake.submit(["p1"])  # in-flight guard would block

    result = drain(
        config=GoldConfig(post_ids=["p1"], prefer_interactive=True), instance=fake
    )
    assert _enqueued(result) == 1
    # The bypass skips the GUARDS, not the materialization: the partition is
    # (re)materialized so the NEXT normal run's guard sees the work.
    assert fake.submitted_partitions() == {
        partition_key(DRAIN_WORKLOAD, 0, ["p1"])
    }


# ── AC4: the skip path NAMES the in-flight work ────────────────────────────


def test_skip_message_names_in_flight_work(drain, conn, caplog):
    for pid in ("p1", "p2"):
        _post(conn, pid)
        _label(conn, pid)
    fake = FakeInstance()
    fake.submit(["p1", "p2"])

    with caplog.at_level("WARNING", logger="orchestration.defs.ig_core.slv.posts"):
        result = drain(instance=fake)

    assert _enqueued(result) == 0
    messages = [r.getMessage() for r in caplog.records]
    skip = [m for m in messages if "IN FLIGHT" in m]
    assert skip, f"no in-flight skip message; got {messages}"
    assert "2 post(s)" in skip[0]  # the count, not a bare "no run"
    assert "p1" in skip[0] and "p2" in skip[0]  # the keys
    assert "nothing to do" not in skip[0]


def test_empty_corpus_says_nothing_to_do(drain, conn, caplog):
    with caplog.at_level("INFO", logger="orchestration.defs.ig_core.slv.posts"):
        result = drain(instance=FakeInstance())
    assert _enqueued(result) == 0
    assert any("nothing to do" in r.getMessage() for r in caplog.records)


# ── AC5: the drain and the accounting identity agree on "in flight" ────────


def test_drain_in_flight_definition_matches_accounting_identity():
    fake = FakeInstance(submitted=["k1", "k2"], harvested=["k2"])
    assert drain_in_flight_keys(fake) == frozenset(in_flight_partitions(fake)) == {"k1"}


def test_suppression_uses_in_flight_partitions_keys(drain, conn):
    _post(conn, "p1")
    _post(conn, "p2")
    fake = FakeInstance()
    fake.submit(["p1"])

    candidates = ["p1", "p2"]
    suppressed = drain_suppressed_post_ids(candidates, fake)
    # The suppression is exactly: partition_key(post) ∈ in_flight_partitions.
    expected = {
        pid
        for pid in candidates
        if partition_key(DRAIN_WORKLOAD, 0, [pid])
        in in_flight_partitions(fake)
    }
    assert set(suppressed) == expected == {"p1"}
    # And the keys the drain saw ARE the identity's in-flight set.
    assert {partition_key(DRAIN_WORKLOAD, 0, [pid]) for pid in suppressed} == (
        in_flight_partitions(fake) & {
            partition_key(DRAIN_WORKLOAD, 0, [pid]) for pid in candidates
        }
    )


# ── AC6: completion guard reads silver, never gold ─────────────────────────


def test_completion_guard_reads_silver_classification(drain, conn):
    from orchestration.defs.ig_enriched.slv.prompts import CURRENT_PROMPT_HASH

    _post(conn, "p_done")
    _label(conn, "p_done")
    _post(conn, "p_fresh")
    _label(conn, "p_fresh")
    _conform(conn, "p_done", CURRENT_PROMPT_HASH)
    # A stale (different-prompt-hash) silver row does NOT block: re-enqueue-eligible.
    _post(conn, "p_stale_row")
    _label(conn, "p_stale_row")
    _conform(conn, "p_stale_row", "deadbeef" * 4)

    result = drain(instance=FakeInstance())
    assert result["candidates_seen"][0] == 2  # p_done blocked, others eligible
    assert _enqueued(result) == 2
