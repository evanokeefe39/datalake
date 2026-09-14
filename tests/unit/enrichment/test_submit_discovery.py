"""W3/W4 round-trip: drain → submit → harvest over ONE shared instance.

These tests join the real halves on a real (ephemeral) Dagster instance and
a fake seam adapter — the acceptance contract is behavioral:

* the harvested producer exists and SHIFTS the in-flight set: delete
  ``report_harvested`` and the release/suppression assertions below fail
  (in-flight would never shrink — the stall defect ADR-0014 exists to kill);
* drain run 2 re-enqueues the failed post AND suppresses the in-flight one
  — BOTH asserted, so total suppression fails;
* the accounting identity holds over the corpus;
* the whole loop runs with ZERO queue-table reads.
"""

import pytest
from dagster import AssetKey, AssetMaterialization, DagsterInstance

from datalake.defs.common.resources import DuckDBResource, SQLiteResource
from datalake.defs.enrichment import harvest, submit
from datalake.defs.enrichment.landing import WORKLOAD_CONTENT_CLASSIFICATION, read_responses
from datalake.defs.enrichment.partitions import (
    MAX_ROUNDS,
    account,
    failure_set,
    in_flight_partitions,
    post_partition_state,
)
from datalake.defs.enrichment.seam import DEFAULT_JOBSPEC, Result
from datalake.defs.instagram import assets as ig_assets


WORKLOAD = WORKLOAD_CONTENT_CLASSIFICATION
SUBMITTED = AssetKey("enrichment_submitted")
HARVESTED = AssetKey("enrichment_harvested")


def rkey(pid: str, round_n: int = 0) -> str:
    return f"{WORKLOAD}\x00r{round_n}\x00{pid}"


class FakeAdapter:
    """Seam adapter double: one job per submit call, scripted results."""

    name = "fake"

    def __init__(
        self,
        results_by_handle: dict[str, list[Result]] | None = None,
        terminal: bool = True,
        fail_poll: bool = False,
    ) -> None:
        self.results_by_handle = results_by_handle or {}
        self.terminal = terminal
        self.fail_poll = fail_poll
        self.submitted: list[list] = []

    def health(self) -> bool:
        return True

    def submit(self, items, *, job_spec=DEFAULT_JOBSPEC) -> str:
        self.submitted.append(list(items))
        return f"job{len(self.submitted)}"

    def poll(self, handle: str) -> dict:
        if self.fail_poll:
            raise RuntimeError(f"transport down for {handle}")
        return {"state": "completed" if self.terminal else "processing"}

    def normalize_state(self, raw) -> str:
        return raw["state"]

    def is_terminal(self, state: str) -> bool:
        return state in {"completed", "failed"}

    def retrieve(self, handle: str) -> list[Result]:
        return self.results_by_handle.get(handle, [])


@pytest.fixture()
def instance():
    return DagsterInstance.ephemeral()

def drain_enqueue(inst, posts: dict[str, int]) -> None:
    """The drain's enqueue: one submitted partition per post at its round."""
    ig_assets._materialize_submitted_partitions(inst, posts)


def record_handle(inst, key: str, handle: str) -> None:
    """What the submit stage does after a provider POST."""
    inst.report_runless_asset_event(
        AssetMaterialization(
            asset_key=SUBMITTED, partition=key, metadata={"handle": handle}
        )
    )


def make_result(pid: str, ok: bool, round_n: int = 0) -> Result:
    return Result(
        custom_key=rkey(pid, round_n),
        ok=ok,
        response_text='{"classification": "tech"}' if ok else "",
        error=None if ok else "boom",
        model="fake-model",
        provider="fake",
    )


# ── The harvested producer (D2): completion genuinely releases work ─────────


def test_harvest_releases_only_its_own_partitions(instance):
    """P terminates (failed → retry), Q stays in flight.

    Fails if the harvested producer is deleted: P's key would stay in the
    in-flight set forever and the release assert below breaks.
    """
    drain_enqueue(instance, {"P": 0, "Q": 0})
    record_handle(instance, rkey("P"), "job1")
    record_handle(instance, rkey("Q"), "job1")
    assert in_flight_partitions(instance) == {rkey("P"), rkey("Q")}

    adapter = FakeAdapter(results_by_handle={"job1": [make_result("P", ok=False)]})
    outcome = harvest.harvest_pending(instance, adapter)

    # D2: the round-0 key moved OUT of in-flight — Q's did not.
    assert in_flight_partitions(instance) == {rkey("Q")}
    assert outcome["harvested"] == 1
    # D3: a retry key was minted for the failed post.
    assert outcome["retried"] == 1


def test_drain_run_two_reenqueues_failed_and_suppresses_in_flight(instance):
    """The W4 acceptance: run 2 re-enqueues the eligible failed post AND
    suppresses the in-flight one — BOTH, so total suppression fails."""
    drain_enqueue(instance, {"P": 0, "Q": 0})
    record_handle(instance, rkey("P"), "job1")
    record_handle(instance, rkey("Q"), "job1")
    adapter = FakeAdapter(results_by_handle={"job1": [make_result("P", ok=False)]})
    harvest.harvest_pending(instance, adapter)

    candidates = ["P", "Q"]
    suppressed = ig_assets.drain_suppressed_post_ids(candidates, instance)
    assert "Q" in suppressed          # in flight → suppressed
    assert "P" not in suppressed      # harvested+failed → released

    # Drain run 2's enqueue state: P re-enqueues at its retry round.
    p_state = post_partition_state(instance, WORKLOAD, "P")
    assert p_state.next_round == 1
    drain_enqueue(instance, {"P": p_state.next_round})
    assert rkey("P", 1) in in_flight_partitions(instance)
    # Round 0 was NEVER re-materialized: P is in flight only at r1.
    assert rkey("P", 0) not in in_flight_partitions(instance)


def test_in_flight_empty_after_harvest_lands(instance):
    drain_enqueue(instance, {"P": 0})
    record_handle(instance, rkey("P"), "job1")
    adapter = FakeAdapter(results_by_handle={"job1": [make_result("P", ok=True)]})
    harvest.harvest_pending(instance, adapter)
    assert in_flight_partitions(instance) == set()


def test_submit_discovers_retry_round_after_drain_reenqueue(instance):
    drain_enqueue(instance, {"P": 0})
    record_handle(instance, rkey("P"), "job1")
    adapter = FakeAdapter(results_by_handle={"job1": [make_result("P", ok=False)]})
    harvest.harvest_pending(instance, adapter)
    drain_enqueue(instance, {"P": 1})

    pending = submit.discover_pending(instance)
    assert [p.round for p in pending] == [1]
    assert [p.post_id for p in pending] == ["P"]


# ── The accounting identity over the corpus ─────────────────────────────────


def test_accounting_identity_holds_over_corpus(instance):
    """done + failed + in_flight + backlog == candidates, over the corpus.

    Lake-derived counts: done = conformed silver rows (none here — no
    conform ran); failed = the landed∖conformed failure set
    (``partitions.failure_set``); in_flight = instance-derived; backlog =
    candidates with no materialized key at all.
    """
    corpus = [f"P{i}" for i in range(8)]
    drain_enqueue(instance, {pid: 0 for pid in corpus[:6]})
    for pid in corpus[:6]:
        record_handle(instance, rkey(pid), "job1")
    adapter = FakeAdapter(
        results_by_handle={
            "job1": [make_result("P0", ok=True), make_result("P1", ok=False)],
        }
    )
    harvest.harvest_pending(instance, adapter)

    harvested = instance.get_materialized_partitions(HARVESTED)
    in_flight = in_flight_partitions(instance)
    failed = len(failure_set(landed=set(harvested), conformed=set()))
    backlog = sum(
        1 for pid in corpus
        if rkey(pid) not in harvested and rkey(pid) not in in_flight
    )
    a = account(
        instance,
        done=0,
        failed=failed,
        backlog=backlog,
        total_candidates=len(corpus),
    )
    assert a.holds, f"identity broke: deficit={a.deficit}"
    assert failed == 2 and backlog == 2 and len(in_flight) == 4


# ── Submit: discovery, build failures, handle recording ─────────────────────


@pytest.fixture()
def dbs(tmp_path):
    from datalake.defs.common.schemas import duckdb_ddl, sqlite_ddl

    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    conn = ops.get_connection()
    conn.executescript(sqlite_ddl("media_cache"))
    conn.commit()
    conn.close()
    with duckdb.get_connection() as conn:
        conn.execute(duckdb_ddl("silver_ig_posts"))
        conn.execute(
            "INSERT INTO silver_ig_posts (post_id, caption, source_dataset) VALUES"
            " ('P1', 'caption one', 'test'), ('P2', 'caption two', 'test')"
        )
    return ops, duckdb


def test_submit_roundtrip_records_handle_and_discovery(instance, dbs, tmp_path):
    ops, duckdb = dbs
    drain_enqueue(instance, {"P1": 0, "P2": 0})
    adapter = FakeAdapter()
    result = submit.submit_pending(
        instance, ops, duckdb, adapter, root=str(tmp_path / "lake")
    )

    assert result["submitted"] == 2
    assert len(adapter.submitted) == 1  # ONE provider job per run
    items = adapter.submitted[0]
    assert {i.custom_key for i in items} == {rkey("P1"), rkey("P2")}
    # The handle is recorded Dagster-natively on the submitted partitions.
    handles = harvest.discover_handles(instance)
    assert set(handles.values()) == {result["handle"]}


def test_empty_caption_is_terminal_without_retry(instance, dbs, tmp_path):
    ops, duckdb = dbs
    with duckdb.get_connection() as conn:
        conn.execute("UPDATE silver_ig_posts SET caption = '' WHERE post_id = 'P1'")
    drain_enqueue(instance, {"P1": 0})
    adapter = FakeAdapter()
    result = submit.submit_pending(
        instance, ops, duckdb, adapter, root=str(tmp_path / "lake")
    )

    assert result["failed"] == 1
    assert result["submitted"] == 0
    # In-flight shrinks: the partition is terminal, minted nothing.
    assert in_flight_partitions(instance) == set()
    # The failure LANDED in bronze (loud, not dropped).
    rows = read_responses(root=tmp_path / "lake")
    assert len(rows) == 1 and not rows["ok"][0]


def test_discover_pending_raises_on_budget_exhausted_in_flight(instance):
    drain_enqueue(instance, {"P": MAX_ROUNDS})
    with pytest.raises(RuntimeError, match="MAX_ROUNDS"):
        submit.discover_pending(instance)


def test_mint_retries_stops_at_budget(instance):
    minted = harvest.mint_retries(
        instance, {rkey("P", MAX_ROUNDS): "still failing"}
    )
    assert minted == []


# ── Harvest: bounded loops, fail-loudly polls ────────────────────────────────


def test_poll_failure_raises_instead_of_warning_forever(instance):
    drain_enqueue(instance, {"P": 0})
    handle = FakeAdapter().submit([], job_spec=DEFAULT_JOBSPEC)
    record_handle(instance, rkey("P"), handle)
    failer = FakeAdapter(fail_poll=True)
    with pytest.raises(RuntimeError, match="transport down"):
        harvest.harvest_pending(instance, failer)


def test_non_terminal_handle_is_skipped_not_polled_forever(instance):
    drain_enqueue(instance, {"P": 0})
    handle = FakeAdapter().submit([], job_spec=DEFAULT_JOBSPEC)
    record_handle(instance, rkey("P"), handle)
    pending_adapter = FakeAdapter(terminal=False)
    outcome = harvest.harvest_pending(instance, pending_adapter)
    assert outcome["harvested"] == 0
    assert in_flight_partitions(instance) == {rkey("P")}  # still in flight


def test_no_queue_read_anywhere_on_target_path():
    import inspect

    from datalake.defs.enrichment import batch as batch_mod

    for mod in (submit, harvest, ig_assets):
        src = inspect.getsource(mod)
        for pattern in ("FROM batch_jobs", "FROM batch_items", "FROM dead_letter",
                        "batch_items bi", "INSERT INTO batch"):
            assert pattern not in src, f"{mod.__name__} still reads {pattern}"
    # The queue primitives themselves are gone.
    assert not hasattr(batch_mod, "claim_pending_items")
    assert not hasattr(batch_mod, "create_batch")
    assert not hasattr(batch_mod, "_require_legacy_queue_tables")
