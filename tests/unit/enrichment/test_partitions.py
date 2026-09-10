"""Unit tests for Dagster-native enrichment orchestration state (ADR-0012/0013).

No live Dagster instance: a local fake implements the PartitionSnapshot
surface (get_materialized_partitions) that the real DagsterInstance provides.
"""

import pytest
from dagster import DynamicPartitionsDefinition

from datalake.defs.enrichment.partitions import (
    HARVESTED_PARTITIONS,
    SUBMITTED_PARTITIONS,
    Account,
    account,
    failure_set,
    in_flight_partitions,
    partition_key,
)


class FakeInstance:
    """Test double implementing the PartitionSnapshot Protocol.

    Backed by plain dicts; never touches the filesystem or a real instance.
    """

    def __init__(self) -> None:
        self._materialized: dict[DynamicPartitionsDefinition, set[str]] = {}

    def materialize(
        self, partitions_def: DynamicPartitionsDefinition, keys: set[str]
    ) -> None:
        self._materialized.setdefault(partitions_def, set()).update(keys)

    def get_materialized_partitions(
        self, partitions_def: DynamicPartitionsDefinition
    ) -> set[str]:
        return set(self._materialized.get(partitions_def, set()))


def _batch(i: int) -> str:
    return partition_key("qwen-vision", 0, [f"post_{i}"])


# ------------------------------------------------------- partition key design


class TestPartitionKey:
    def test_deterministic_and_order_insensitive(self) -> None:
        a = partition_key("qwen-vision", 0, ["b", "a", "c"])
        b = partition_key("qwen-vision", 0, ["c", "b", "a"])
        assert a == b
        assert partition_key("qwen-vision", 0, ["a", "b", "c"]) == a

    def test_inputs_distinguish_keys(self) -> None:
        base = partition_key("qwen-vision", 0, ["a", "b"])
        assert partition_key("text-LLM", 0, ["a", "b"]) != base  # workload
        assert partition_key("qwen-vision", 1, ["a", "b"]) != base  # retry round
        assert partition_key("qwen-vision", 0, ["a", "b", "c"]) != base  # membership
        # Subset membership does NOT collide (a shared-prefix batch differs).
        assert partition_key("qwen-vision", 0, ["a"]) != base

    def test_retry_is_a_new_key(self) -> None:
        # ADR-0012 decision 5: retry round N mints a fresh partition.
        assert partition_key("qwen-vision", 0, ["a"]) != partition_key(
            "qwen-vision", 1, ["a"]
        )

    def test_same_work_is_stable_across_re_runs(self) -> None:
        # Same inputs -> same key, so a re-run of the same batch re-materializes
        # the SAME submitted partition (idempotent), not a new one.
        assert partition_key("qwen-vision", 0, ["x", "y"]) == partition_key(
            "qwen-vision", 0, ["y", "x"]
        )

    def test_error_paths(self) -> None:
        with pytest.raises(ValueError):
            partition_key("", 0, ["a"])
        with pytest.raises(ValueError):
            partition_key("qwen-vision", -1, ["a"])
        with pytest.raises(ValueError):
            partition_key("qwen-vision", 0, [])
        with pytest.raises(ValueError):
            partition_key("qwen-vision", 0, ["a", ""])


# ------------------------------------------------------------- in-flight set


class TestInFlight:
    def test_in_flight_is_exactly_submitted_minus_harvested(self) -> None:
        inst = FakeInstance()
        k1, k2, k3 = _batch(1), _batch(2), _batch(3)
        inst.materialize(SUBMITTED_PARTITIONS, {k1, k2, k3})
        inst.materialize(HARVESTED_PARTITIONS, {k2})
        assert in_flight_partitions(inst) == {k1, k3}

    def test_empty_submitted_gives_empty_in_flight(self) -> None:
        assert in_flight_partitions(FakeInstance()) == set()

    def test_harvesting_everything_gives_empty_in_flight(self) -> None:
        inst = FakeInstance()
        keys = {_batch(i) for i in range(4)}
        inst.materialize(SUBMITTED_PARTITIONS, keys)
        inst.materialize(HARVESTED_PARTITIONS, keys)
        assert in_flight_partitions(inst) == set()

    def test_submitted_not_harvested_appears_core_double_submit_guard(self) -> None:
        inst = FakeInstance()
        key = _batch(7)
        inst.materialize(SUBMITTED_PARTITIONS, {key})
        # Not harvested yet: the batch is in flight, so a second submit of the
        # same work must be refused — the key is visible to the derivation.
        assert key in in_flight_partitions(inst)
        # Harvesting removes it, allowing a (retry-round) resubmission space.
        inst.materialize(HARVESTED_PARTITIONS, {key})
        assert key not in in_flight_partitions(inst)

    def test_harvested_without_submitted_never_appears(self) -> None:
        inst = FakeInstance()
        inst.materialize(HARVESTED_PARTITIONS, {_batch(1)})
        assert in_flight_partitions(inst) == set()

    def test_result_is_a_copy(self) -> None:
        inst = FakeInstance()
        key = _batch(1)
        inst.materialize(SUBMITTED_PARTITIONS, {key})
        snapshot = in_flight_partitions(inst)
        snapshot.clear()
        assert in_flight_partitions(inst) == {key}

    def test_rejects_object_without_snapshot_surface(self) -> None:
        class NotAnInstance:
            pass

        with pytest.raises(TypeError):
            in_flight_partitions(NotAnInstance())  # type: ignore[arg-type]

    def test_identity_holds_for_constructed_example(self) -> None:
        # 4 candidates: 2 done, 1 failed, 1 in flight (one submitted batch of
        # 1 post), 0 backlog.
        inst = FakeInstance()
        key = partition_key("qwen-vision", 0, ["p3"])
        inst.materialize(SUBMITTED_PARTITIONS, {key})
        result: Account = account(
            inst, done=2, failed=1, backlog=0, total_candidates=4
        )
        assert (result.done, result.failed, result.in_flight, result.backlog) == (
            2,
            1,
            1,
            0,
        )
        assert result.holds
        assert result.deficit == 0

    def test_identity_violated_when_submitted_partition_dropped(self) -> None:
        # The lake says: 2 batches of 2 posts outstanding, p5 still backlog
        # (total 5). But one submitted materialization was lost — in_flight
        # deflates to 1 and the identity breaks. This is the guardrail
        # actually firing.
        inst = FakeInstance()
        k1 = partition_key("qwen-vision", 0, ["p1", "p2"])
        k2 = partition_key("qwen-vision", 0, ["p3", "p4"])
        inst.materialize(SUBMITTED_PARTITIONS, {k1})  # k2's materialization LOST
        # the loss is real, not merely commented: k2 is absent from the snapshot
        assert k2 not in inst.get_materialized_partitions(SUBMITTED_PARTITIONS)
        result = account(inst, done=0, failed=0, backlog=1, total_candidates=5)
        assert result.in_flight == 1
        assert not result.holds
        assert result.deficit == 3

    def test_identity_violated_when_harvested_partition_dropped(self) -> None:
        # Opposite direction: batch 1 harvested AND conformed (done=1); batch 2
        # harvested but not yet conformed (backlog=1, lake-side). Its harvest
        # materialization is LOST from the instance, so in_flight overcounts
        # by 1 and the identity breaks on the other side.
        inst = FakeInstance()
        keys = {_batch(1), _batch(2)}
        inst.materialize(SUBMITTED_PARTITIONS, keys)
        inst.materialize(HARVESTED_PARTITIONS, {_batch(1)})  # _batch(2) harvest LOST
        result = account(inst, done=1, failed=0, backlog=1, total_candidates=2)
        assert result.in_flight == 1
        assert not result.holds
        assert result.deficit == -1

    def test_negative_counts_rejected(self) -> None:
        inst = FakeInstance()
        with pytest.raises(ValueError):
            account(inst, done=-1, failed=0, backlog=0, total_candidates=0)
        with pytest.raises(ValueError):
            account(inst, done=0, failed=0, backlog=0, total_candidates=-5)


# --------------------------------------------------------- failure surfacing


class TestFailureSet:
    def test_anti_join_returns_landed_not_conformed(self) -> None:
        landed = {"p1@instagram", "p2@instagram", "p3@instagram", "p4@instagram"}
        conformed = {"p1@instagram", "p3@instagram"}
        assert failure_set(landed, conformed) == frozenset(
            {"p2@instagram", "p4@instagram"}
        )

    def test_fully_conformed_landing_is_empty_failure_set(self) -> None:
        landed = {"p1@instagram", "p2@instagram"}
        assert failure_set(landed, landed.copy()) == frozenset()

    def test_conformed_without_landing_is_ignored(self) -> None:
        # The anti-join only surfaces landed-but-stuck work.
        assert failure_set(set(), {"p1@instagram"}) == frozenset()

    def test_replaces_dead_letter_as_the_failure_surface(self) -> None:
        # A terminal failure is visible purely from absence: the response was
        # landed verbatim, but no conformed row exists — no dead_letter table.
        landed = {"p_dead@instagram"}
        conformed: set[str] = set()
        assert "p_dead@instagram" in failure_set(landed, conformed)

    def test_error_paths(self) -> None:
        with pytest.raises(TypeError):
            failure_set(["p1"], set())  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            failure_set(set(), "p1")  # type: ignore[arg-type]
