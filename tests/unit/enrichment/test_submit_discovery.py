"""Submit owns discovery: the round-trip over ONE shared instance.

ADR-0016 made the submit stage the sole discovery actor — it derives
candidates, guards against double-submission, materializes its own
``enrichment_submitted`` placeholder, and submits. There is no drain.

These tests join the real halves on a real (ephemeral) Dagster instance and a
fake seam adapter — the acceptance contract is behavioral:

* the harvested producer exists and SHIFTS the in-flight set: delete
  ``report_harvested`` and the release assertions fail (in-flight would never
  shrink — the stall defect ADR-0014 exists to kill);
* discovery reads the SAME ``in_flight_partitions`` derivation the guard uses,
  so a second run suppresses what the first submitted and submits ONLY new work;
* a build failure is terminal-loud (an ``ok=False`` bronze row + harvested
  partition), so a partition cannot be stranded in flight;
* the accounting identity holds over the corpus;
* the whole loop runs with ZERO queue-table reads.
"""

import orchestration.defs.engine.harvest as harvest
import orchestration.defs.engine.submit as submit
import pytest
from dagster import AssetKey, AssetMaterialization, DagsterInstance
from orchestration.defs.engine.landing import (
    WORKLOAD_CONTENT_CLASSIFICATION,
    read_responses,
)
from orchestration.defs.engine.partitions import (
    MAX_ROUNDS,
    SUBMITTED_ASSET_NAME,
    account,
    failure_set,
    in_flight_partitions,
    parse_partition_key,
    partition_key,
    post_partition_state,
)
from orchestration.defs.engine.provider import DEFAULT_JOBSPEC, Result
from orchestration.defs.ig_enriched.slv.workloads import SubmitConfig
from orchestration.defs.platform.resources import DuckDBResource, SQLiteResource

WORKLOAD = WORKLOAD_CONTENT_CLASSIFICATION
SUBMITTED = AssetKey(SUBMITTED_ASSET_NAME)
HARVESTED = AssetKey("enrichment_harvested")


def rkey(pid: str, round_n: int = 0) -> str:
    return partition_key(WORKLOAD, round_n, [pid])


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
        self.calls: list[tuple] = []

    def health(self) -> bool:
        return True

    def submit(self, items, *, job_spec=DEFAULT_JOBSPEC) -> str:
        self.submitted.append(list(items))
        # Record the job-level options alongside the batch: the spec is what
        # a misattributed batch would be submitted UNDER, so a test that pins
        # workload correctness needs to see both.
        self.calls.append((job_spec, list(items)))
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


def enqueue(inst, posts: dict[str, int]) -> None:
    """Materialize the submitted placeholder, as submit does pre-POST.

    Direct materialization, not a drain: ADR-0016 deleted the drain, and these
    tests need a partition in the submitted space to observe guard behaviour.
    """
    keys = [rkey(pid, round_n) for pid, round_n in posts.items()]
    inst.add_dynamic_partitions(SUBMITTED_ASSET_NAME, keys)
    for key in keys:
        inst.report_runless_asset_event(
            AssetMaterialization(asset_key=SUBMITTED, partition=key)
        )


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


def approve(duckdb, post_ids: list[str]) -> None:
    """Label-approve posts, which is what makes them discovery-eligible."""
    from orchestration.defs.ig_core.slv.labels import LABEL_VERSION

    with duckdb.get_connection() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ig_post_labels ("
            "post_id VARCHAR, enrich_decision VARCHAR, label_version INTEGER)"
        )
        for pid in post_ids:
            conn.execute(
                "INSERT INTO ig_post_labels VALUES (?, 'standout', ?)",
                [pid, LABEL_VERSION],
            )


# ── The harvested producer (D2): completion genuinely releases work ─────────


def test_harvest_releases_only_its_own_partitions(instance, tmp_path):
    """P terminates (failed → retry), Q stays in flight.

    Fails if the harvested producer is deleted: P's key would stay in the
    in-flight set forever and the release assert below breaks.
    """
    enqueue(instance, {"P": 0, "Q": 0})
    record_handle(instance, rkey("P"), "job1")
    record_handle(instance, rkey("Q"), "job1")
    assert in_flight_partitions(instance) == {rkey("P"), rkey("Q")}

    adapter = FakeAdapter(results_by_handle={"job1": [make_result("P", ok=False)]})
    outcome = harvest.harvest_pending(instance, adapter, root=str(tmp_path / "lake"))

    # D2: the round-0 key moved OUT of in-flight — Q's did not.
    assert in_flight_partitions(instance) == {rkey("Q")}
    assert outcome["harvested"] == 1
    # D3: a retry key was minted for the failed post.
    assert outcome["retried"] == 1


def test_in_flight_empty_after_harvest_lands(instance, tmp_path):
    enqueue(instance, {"P": 0})
    record_handle(instance, rkey("P"), "job1")
    adapter = FakeAdapter(results_by_handle={"job1": [make_result("P", ok=True)]})
    harvest.harvest_pending(instance, adapter, root=str(tmp_path / "lake"))
    assert in_flight_partitions(instance) == set()


def test_a_failed_post_is_released_and_its_retry_round_derives(instance, tmp_path):
    """The W4 acceptance, restated for the submit-owned world: a harvested
    failure is RELEASED (not suppressed) and its next round derives to 1."""
    enqueue(instance, {"P": 0, "Q": 0})
    record_handle(instance, rkey("P"), "job1")
    record_handle(instance, rkey("Q"), "job1")
    adapter = FakeAdapter(results_by_handle={"job1": [make_result("P", ok=False)]})
    harvest.harvest_pending(instance, adapter, root=str(tmp_path / "lake"))

    # Q is still in flight; P has been released and re-derives at round 1.
    assert rkey("Q") in in_flight_partitions(instance)
    assert rkey("P") not in in_flight_partitions(instance)
    assert post_partition_state(instance, WORKLOAD, "P").next_round == 1

    # A retry round is a NEW key; round 0 is never re-materialized.
    enqueue(instance, {"P": 1})
    assert rkey("P", 1) in in_flight_partitions(instance)
    assert rkey("P", 0) not in in_flight_partitions(instance)


# ── The accounting identity over the corpus ─────────────────────────────────


def test_accounting_identity_holds_over_corpus(instance, tmp_path):
    """done + failed + in_flight + backlog == candidates, over the corpus.

    Lake-derived counts: done = conformed silver rows (none here — no conform
    ran); failed = the landed∖conformed failure set
    (``partitions.failure_set``); in_flight = instance-derived; backlog =
    candidates with no materialized key at all.
    """
    corpus = [f"P{i}" for i in range(8)]
    enqueue(instance, {pid: 0 for pid in corpus[:6]})
    for pid in corpus[:6]:
        record_handle(instance, rkey(pid), "job1")
    adapter = FakeAdapter(
        results_by_handle={
            "job1": [make_result("P0", ok=True), make_result("P1", ok=False)],
        }
    )
    harvest.harvest_pending(instance, adapter, root=str(tmp_path / "lake"))

    harvested = instance.get_materialized_partitions(HARVESTED)
    in_flight = in_flight_partitions(instance)
    failed = len(failure_set(landed=set(harvested), conformed=set()))
    backlog = sum(
        1
        for pid in corpus
        if rkey(pid) not in harvested and rkey(pid) not in in_flight
    )
    a = account(
        instance, done=0, failed=failed, backlog=backlog, total_candidates=len(corpus)
    )
    assert a.holds, f"identity broke: deficit={a.deficit}"
    assert failed == 2 and backlog == 2 and len(in_flight) == 4


# ── Submit: discovery, build failures, handle recording ─────────────────────


@pytest.fixture()
def dbs(tmp_path):
    from opsdb.schema import sqlite_ddl
    from orchestration.defs.platform.schemas import duckdb_ddl

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
        # The classification workload reads labels and guards on the conformed
        # table; create both so a whole-registry submit is runnable in tests.
        conn.execute(
            "CREATE TABLE ig_post_labels ("
            "post_id VARCHAR, enrich_decision VARCHAR, label_version INTEGER)"
        )
        conn.execute(
            "CREATE TABLE silver_content_classification ("
            "post_id VARCHAR, platform VARCHAR, prompt_hash VARCHAR)"
        )
    return ops, duckdb


def test_submit_discovers_builds_and_records_one_handle(instance, dbs, tmp_path):
    """Submit discovers its own candidates — nothing pre-enqueued it."""
    ops, duckdb = dbs
    adapter = FakeAdapter()
    result = submit.submit_pending(
        instance,
        ops,
        duckdb,
        adapter,
        config=SubmitConfig(post_ids=["P1", "P2"], workload=WORKLOAD),
        root=str(tmp_path / "lake"),
    )

    assert result["submitted"] == 2
    assert len(adapter.submitted) == 1  # ONE provider job per workload per run
    # The placeholder exists for both posts, and both carry the handle.
    assert in_flight_partitions(instance) == {rkey("P1"), rkey("P2")}
    assert harvest.discover_handles(instance) == {
        rkey("P1"): "job1",
        rkey("P2"): "job1",
    }


def test_second_submit_run_suppresses_what_the_first_submitted(instance, dbs, tmp_path):
    """The guard and discovery read ONE derivation (ADR-0016).

    GIVEN a label-approved post submitted by the DISCOVERY path
    WHEN a second run discovers the same corpus
    THEN it submits NOTHING — the post is in flight, not eligible again.

    The discovery path is the one under test: ``post_ids`` deliberately
    bypasses the guard, so using it here would prove nothing.
    """
    ops, duckdb = dbs
    approve(duckdb, ["P1"])
    adapter = FakeAdapter()

    first = submit.submit_pending(
        instance, ops, duckdb, adapter,
        config=SubmitConfig(workload=WORKLOAD), root=str(tmp_path / "lake"),
    )
    assert first["submitted"] == 1

    second = submit.submit_pending(
        instance, ops, duckdb, adapter,
        config=SubmitConfig(workload=WORKLOAD), root=str(tmp_path / "lake"),
    )
    assert second["submitted"] == 0
    assert second["in_flight"] == 1  # reported as suppressed, not silent
    assert len(adapter.submitted) == 1  # ONE provider call in total


def test_empty_caption_is_terminal_without_retry(instance, dbs, tmp_path):
    """An unbuildable post lands a FAILED row and is harvested — never
    stranded in flight, and never retried (the cause is deterministic)."""
    ops, duckdb = dbs
    with duckdb.get_connection() as conn:
        conn.execute(
            "INSERT INTO silver_ig_posts (post_id, caption, source_dataset)"
            " VALUES ('P3', '   ', 'test')"
        )
    adapter = FakeAdapter()
    result = submit.submit_pending(
        instance,
        ops,
        duckdb,
        adapter,
        config=SubmitConfig(post_ids=["P3"], workload=WORKLOAD),
        root=str(tmp_path / "lake"),
    )

    assert result["submitted"] == 0
    assert result["failed"] == 1
    assert len(adapter.submitted) == 0  # no provider call for unbuildable work
    # Terminal, not stranded: the key is no longer in flight.
    assert rkey("P3") not in in_flight_partitions(instance)
    landed = read_responses(str(tmp_path / "lake"))
    assert landed.height >= 1
    assert any(
        (not row["ok"]) and row["workload"] == WORKLOAD
        for row in landed.iter_rows(named=True)
    )


def test_a_build_failure_does_not_misattribute_another_workloads_batch(
    instance, dbs, tmp_path
):
    """A failing candidate must not hand its workload to another workload's item.

    Regression: ``build_items`` routed unbuildable candidates into its failure
    list while the caller zipped the surviving items against the CANDIDATE
    list positionally. One failure shifts every later item by one, so a
    caption-only workload's item gets dispatched as media-bearing — under the
    wrong job spec, parsed by the wrong parser, landed with the wrong prompt
    hash (which is what would break the bronze→silver replay).

    Two workloads in one run are REQUIRED to observe this: with a single
    workload both assignments coincide, which is why a single-workload test
    (and the smoke slice, where every candidate builds) does not catch it.
    """
    ops, duckdb = dbs
    with duckdb.get_connection() as conn:
        # P1 is a candidate for BOTH passes: a caption (text) and a media URL
        # (visual). P2 is caption-only, so the text pass alone covers it. The
        # two facet workloads are the case that exposes it — the classification
        # pass is a single workload and cannot misattribute.
        # P1: caption + an UNCACHED media URL, so it is a candidate for both
        # facet passes and builds for text only.
        conn.execute(
            "UPDATE silver_ig_posts SET caption = 'caption one', media_files ="
            " '[\"https://cdn.example/never-cached.jpg\"]' WHERE post_id = 'P1'"
        )
        # P2, P3: caption-only, so the text pass alone covers them.
        conn.execute(
            "UPDATE silver_ig_posts SET media_files = '[]' WHERE post_id = 'P2'"
        )
        conn.execute(
            "INSERT INTO silver_ig_posts (post_id, caption, media_files, source_dataset)"
            " VALUES ('P3', 'caption three', '[]', 'test')"
        )

    # No media is cached, so the VISUAL pass fails on P1 while the TEXT pass
    # builds everything. The registry lists visual before text, so the failure
    # precedes the text items in discovery order — the shift this pins.
    adapter = FakeAdapter()
    result = submit.submit_pending(
        instance,
        ops,
        duckdb,
        adapter,
        config=SubmitConfig(),
        root=str(tmp_path / "lake"),
    )

    assert result["failed"] >= 1, "the visual pass must fail on uncached media"

    # Each provider call's items must declare the workload it was made for:
    # a partition key names its own workload, so the two must agree.
    for spec, batch in adapter.calls:
        declared = {parse_partition_key(i.custom_key).workload for i in batch}
        assert len(declared) == 1, (
            f"one provider call carried partitions from {sorted(declared)} — "
            "a build failure shifted the workload assignment"
        )
        if spec is not None and spec.mode is not None:
            # A media-bearing call submits under mode='visual'; a caption-only
            # call under mode='text'. They must not be swapped.
            (workload_name,) = declared
            expected = "growth-facets-visual" if spec.mode == "visual" else "growth-facets-text"
            assert workload_name == expected, (
                f"call made with mode={spec.mode!r} carried "
                f"{workload_name!r} partitions"
            )

    # Nothing is in flight without a handle to resolve it.
    assert set(harvest.discover_handles(instance)) == set(in_flight_partitions(instance))


def test_explicit_post_ids_bypass_the_in_flight_guard(instance, dbs, tmp_path):
    """Re-enrichment at will: an explicit post_id is submitted even though
    the same post is already in flight."""
    ops, duckdb = dbs
    adapter = FakeAdapter()
    submit.submit_pending(
        instance, ops, duckdb, adapter,
        config=SubmitConfig(post_ids=["P1"], workload=WORKLOAD),
        root=str(tmp_path / "lake"),
    )
    again = submit.submit_pending(
        instance, ops, duckdb, adapter,
        config=SubmitConfig(post_ids=["P1"], workload=WORKLOAD),
        root=str(tmp_path / "lake"),
    )
    assert again["submitted"] == 1
    assert len(adapter.submitted) == 2


    """Re-enrichment at will: an explicit post_id is submitted even though
    the same post is already in flight."""
    ops, duckdb = dbs
    adapter = FakeAdapter()
    submit.submit_pending(
        instance, ops, duckdb, adapter,
        config=SubmitConfig(post_ids=["P1"], workload=WORKLOAD), root=str(tmp_path / "lake"),
    )
    again = submit.submit_pending(
        instance, ops, duckdb, adapter,
        config=SubmitConfig(post_ids=["P1"], workload=WORKLOAD), root=str(tmp_path / "lake"),
    )
    assert again["submitted"] == 1
    assert len(adapter.submitted) == 2



def test_dry_run_writes_nothing_to_the_instance(instance, dbs, tmp_path):
    """A dry run projects cost and materializes NO placeholder, so it cannot
    change what the next real run sees."""
    ops, duckdb = dbs
    adapter = FakeAdapter()
    result = submit.submit_pending(
        instance,
        ops,
        duckdb,
        adapter,
        config=SubmitConfig(post_ids=["P1", "P2"], workload=WORKLOAD, dry_run=True),
        root=str(tmp_path / "lake"),
    )

    assert result["dry_run"] is True
    assert result["submitted"] == 0
    assert result["candidates"] == 2
    assert result["estimate_tokens"] > 0
    assert len(adapter.submitted) == 0
    assert in_flight_partitions(instance) == set()

    # And the next real run still sees the work.
    real = submit.submit_pending(
        instance,
        ops,
        duckdb,
        adapter,
        config=SubmitConfig(post_ids=["P1", "P2"], workload=WORKLOAD),
        root=str(tmp_path / "lake"),
    )
    assert real["submitted"] == 2


def test_unregistered_workload_raises(instance, dbs, tmp_path):
    """A workload-name typo must not look like a successful empty run."""
    ops, duckdb = dbs
    with pytest.raises(ValueError, match="unknown workload"):
        submit.submit_pending(
            instance,
            ops,
            duckdb,
            FakeAdapter(),
            config=SubmitConfig(workload="no-such-workload"),
            root=str(tmp_path / "lake"),
        )


def test_health_gate_fails_loudly(instance, dbs, tmp_path):
    """US-EENG-2: a down provider raises — never a quiet 'nothing to do'."""
    ops, duckdb = dbs

    class Down(FakeAdapter):
        def health(self) -> bool:
            return False

    with pytest.raises(RuntimeError, match="readiness gate"):
        submit.submit_pending(
            instance, ops, duckdb, Down(), config=SubmitConfig(), root=str(tmp_path)
        )


def test_poll_failure_raises_instead_of_warning_forever(instance, tmp_path):
    enqueue(instance, {"P": 0})
    record_handle(instance, rkey("P"), "job1")
    adapter = FakeAdapter(fail_poll=True)
    with pytest.raises(RuntimeError, match="transport down"):
        harvest.harvest_pending(instance, adapter, root=str(tmp_path / "lake"))


def test_non_terminal_handle_is_skipped_not_polled_forever(instance, tmp_path):
    enqueue(instance, {"P": 0})
    record_handle(instance, rkey("P"), "job1")
    adapter = FakeAdapter(terminal=False)
    outcome = harvest.harvest_pending(instance, adapter, root=str(tmp_path / "lake"))
    assert outcome["harvested"] == 0
    # Still in flight — skipped, not landed, not retried.
    assert in_flight_partitions(instance) == {rkey("P")}


def test_mint_retries_stops_at_budget(instance):
    enqueue(instance, {"P": MAX_ROUNDS - 1})
    record_handle(instance, rkey("P", MAX_ROUNDS - 1), "job1")
    minted = harvest.mint_retries(instance, {rkey("P", MAX_ROUNDS - 1): "boom"})
    assert not minted  # budget spent; no round MAX_ROUNDS key was minted


# ── The queue is gone (ADR-0012) ────────────────────────────────────────────


def test_no_queue_read_anywhere_on_target_path():
    import inspect

    import orchestration.defs.engine.provider as batch_mod

    for mod in (submit, harvest):
        src = inspect.getsource(mod)
        for pattern in (
            "FROM batch_jobs",
            "FROM batch_items",
            "FROM dead_letter",
            "batch_items bi",
            "INSERT INTO batch",
        ):
            assert pattern not in src, f"{mod.__name__} still reads {pattern}"
    # The queue primitives themselves are gone.
    assert not hasattr(batch_mod, "claim_pending_items")
    assert not hasattr(batch_mod, "create_batch")
    assert not hasattr(batch_mod, "_require_legacy_queue_tables")
