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

from datalake.defs.common.resources import DuckDBResource, SQLiteResource
from datalake.defs.common.schemas import duckdb_ddl
from datalake.defs.enrichment import classification, landing
from datalake.defs.enrichment.partitions import (
    HARVESTED_PARTITIONS,
    SUBMITTED_PARTITIONS,
    in_flight_partitions,
    partition_key,
)
from datalake.defs.instagram import assets as ig_assets
from datalake.defs.instagram.assets import (
    DRAIN_ATTEMPT_ROUND,
    DRAIN_WORKLOAD,
    drain_in_flight_keys,
    drain_suppressed_post_ids,
    ig_posts_gen_batches,
)
from datalake.defs.instagram.config import GoldConfig
from datalake.defs.instagram.labels import LABEL_VERSION

NOW = datetime(2026, 9, 1, tzinfo=timezone.utc)


class FakeInstance:
    """Minimal PartitionSnapshot: in-memory submitted/harvested sets."""

    def __init__(self, submitted=(), harvested=()):
        self._submitted = set(submitted)
        self._harvested = set(harvested)

    def get_materialized_partitions(self, partitions_def):
        if partitions_def is SUBMITTED_PARTITIONS:
            return set(self._submitted)
        if partitions_def is HARVESTED_PARTITIONS:
            return set(self._harvested)
        return set()

    def submit(self, post_ids):
        """Simulate the submit stage materializing one partition per post."""
        for pid in post_ids:
            self._submitted.add(partition_key(DRAIN_WORKLOAD, DRAIN_ATTEMPT_ROUND, [pid]))

    def harvest(self, post_ids):
        """Simulate the harvest stage materializing those same partitions."""
        for pid in post_ids:
            self._harvested.add(partition_key(DRAIN_WORKLOAD, DRAIN_ATTEMPT_ROUND, [pid]))


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
def drain(conn, db, ops, monkeypatch):
    """Call the asset directly; batch mode pinned to interactive."""

    class _Tier:
        supports_batch = True

    monkeypatch.setattr(ig_assets.GeminiTierConfig, "detect", lambda: _Tier())

    def run(config=None, instance=None):
        return ig_posts_gen_batches(
            config=config or GoldConfig(prefer_interactive=True),
            duckdb=db,
            ops=ops,
            instance=instance,
        )

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
    fake.submit(["p1", "p2", "p3"])  # submit stage materializes the partitions

    # Second run over the SAME corpus: every post is still in flight.
    second = drain(instance=fake)
    assert _enqueued(second) == 0
    assert second["in_flight_suppressed"][0] == 3


def test_one_post_done_others_in_flight_still_no_double_submit(drain, conn):
    from datalake.defs.enrichment.prompts import CURRENT_PROMPT_HASH

    for pid in ("p1", "p2", "p3"):
        _post(conn, pid)
        _label(conn, pid)
    fake = FakeInstance()
    assert _enqueued(drain(instance=fake)) == 3
    fake.submit(["p1", "p2", "p3"])

    _conform(conn, "p2", CURRENT_PROMPT_HASH)  # p2 completes while 1,3 fly

    second = drain(instance=fake)
    # p2 blocked by the completion guard; p1/p3 by the in-flight guard —
    # zero re-enqueues either way.
    assert _enqueued(second) == 0
    assert second["candidates_seen"][0] == 2
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
    from datalake.defs.enrichment.prompts import CURRENT_PROMPT_HASH

    _post(conn, "p1")
    _label(conn, "p1")
    _conform(conn, "p1", CURRENT_PROMPT_HASH)  # completion guard would block
    fake = FakeInstance()
    fake.submit(["p1"])  # in-flight guard would block

    result = drain(
        config=GoldConfig(post_ids=["p1"], prefer_interactive=True), instance=fake
    )
    assert _enqueued(result) == 1


# ── AC4: the skip path NAMES the in-flight work ────────────────────────────


def test_skip_message_names_in_flight_work(drain, conn, caplog):
    for pid in ("p1", "p2"):
        _post(conn, pid)
        _label(conn, pid)
    fake = FakeInstance()
    fake.submit(["p1", "p2"])

    with caplog.at_level("WARNING", logger="datalake.defs.instagram.assets"):
        result = drain(instance=fake)

    assert _enqueued(result) == 0
    messages = [r.getMessage() for r in caplog.records]
    skip = [m for m in messages if "IN FLIGHT" in m]
    assert skip, f"no in-flight skip message; got {messages}"
    assert "2 post(s)" in skip[0]  # the count, not a bare "no run"
    assert "p1" in skip[0] and "p2" in skip[0]  # the keys
    assert "nothing to do" not in skip[0]


def test_empty_corpus_says_nothing_to_do(drain, conn, caplog):
    with caplog.at_level("INFO", logger="datalake.defs.instagram.assets"):
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
        if partition_key(DRAIN_WORKLOAD, DRAIN_ATTEMPT_ROUND, [pid])
        in in_flight_partitions(fake)
    }
    assert set(suppressed) == expected == {"p1"}
    # And the keys the drain saw ARE the identity's in-flight set.
    assert {partition_key(DRAIN_WORKLOAD, DRAIN_ATTEMPT_ROUND, [pid]) for pid in suppressed} == (
        in_flight_partitions(fake) & {
            partition_key(DRAIN_WORKLOAD, DRAIN_ATTEMPT_ROUND, [pid]) for pid in candidates
        }
    )


# ── AC6: completion guard reads silver, never gold ─────────────────────────


def test_completion_guard_reads_silver_classification(drain, conn):
    from datalake.defs.enrichment.prompts import CURRENT_PROMPT_HASH

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
