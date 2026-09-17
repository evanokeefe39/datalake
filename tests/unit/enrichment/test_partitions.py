"""Unit tests for Dagster-native enrichment orchestration state (ADR-0012/0013).

No live filesystem instance: a STRICT fake implements the PartitionSnapshot
surface — the real ``DagsterInstance.get_materialized_partitions`` takes an
``AssetKey`` on Dagster 1.13.x (a PartitionsDefinition argument raises
``AttributeError ... no attribute 'to_string'``), so the fake rejects any
non-AssetKey argument. ``TestInstanceContract`` additionally exercises the
REAL ephemeral instance so signature drift fails here, not in production.
"""

import inspect

import pytest
from dagster import (
    AssetKey,
    AssetMaterialization,
    DagsterInstance,
    DynamicPartitionsDefinition,
)

from orchestration.defs.engine.partitions import (
    HARVESTED_ASSET_NAME,
    HARVESTED_PARTITIONS,
    SUBMITTED_ASSET_NAME,
    SUBMITTED_PARTITIONS,
    Account,
    account,
    failure_set,
    in_flight_partitions,
    parse_partition_key,
    partition_key,
)


class FakeInstance:
    """Test double implementing the PartitionSnapshot Protocol.

    STRICT: mirrors the real API shape — the argument to
    ``get_materialized_partitions`` MUST be an ``AssetKey``. A call with a
    ``DynamicPartitionsDefinition`` (the defect this suite guards against)
    raises TypeError instead of silently returning an empty set, which is
    what a permissive fake let through before.
    """

    def __init__(self) -> None:
        self._materialized: dict[AssetKey, set[str]] = {}
        self.calls: list[AssetKey] = []

    @staticmethod
    def _as_asset_key(space: object) -> AssetKey:
        if isinstance(space, AssetKey):
            return space
        if isinstance(space, DynamicPartitionsDefinition):
            if space.name is None:
                raise TypeError("unnamed dynamic partitions definition")
            return AssetKey(space.name)
        raise TypeError(
            f"get_materialized_partitions expects an AssetKey, "
            f"got {type(space).__name__}"
        )

    def materialize(self, space: object, keys: set[str]) -> None:
        self._materialized.setdefault(self._as_asset_key(space), set()).update(
            keys
        )

    def get_materialized_partitions(self, asset_key: AssetKey) -> set[str]:
        if not isinstance(asset_key, AssetKey):
            raise TypeError(
                f"get_materialized_partitions expects an AssetKey, "
                f"got {type(asset_key).__name__}"
            )
        self.calls.append(asset_key)
        return set(self._materialized.get(asset_key, set()))


SUBMITTED_KEY = AssetKey(SUBMITTED_ASSET_NAME)
HARVESTED_KEY = AssetKey(HARVESTED_ASSET_NAME)



def _batch(i: int) -> str:
    return partition_key("qwen-vision", 0, [f"post_{i}"])


# ------------------------------------------------------- partition key design


class TestPartitionKey:
    def test_key_is_the_self_describing_composite(self) -> None:
        # ADR-0014 D1: the key IS the record — readable, reversible.
        key = partition_key("qwen-vision", 0, ["p1"])
        assert key == "qwen-vision\x00r0\x00p1"
        parsed = parse_partition_key(key)
        assert (parsed.workload, parsed.attempt_round, parsed.post_id) == (
            "qwen-vision", 0, "p1",
        )

    def test_inputs_distinguish_keys(self) -> None:
        base = partition_key("qwen-vision", 0, ["a"])
        assert partition_key("text-LLM", 0, ["a"]) != base  # workload
        assert partition_key("qwen-vision", 1, ["a"]) != base  # retry round
        assert partition_key("qwen-vision", 0, ["b"]) != base  # post

    def test_retry_is_a_new_key(self) -> None:
        # ADR-0012 decision 5: retry round N mints a fresh partition.
        assert partition_key("qwen-vision", 0, ["a"]) != partition_key(
            "qwen-vision", 1, ["a"]
        )

    def test_same_work_is_stable_across_re_runs(self) -> None:
        # Same inputs -> same key, so a re-run of the same post re-materializes
        # the SAME submitted partition (idempotent), not a new one.
        assert partition_key("qwen-vision", 0, ["x"]) == partition_key(
            "qwen-vision", 0, ["x"]
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
        k1 = partition_key("qwen-vision", 0, ["p1"])
        k2 = partition_key("qwen-vision", 0, ["p3"])
        inst.materialize(SUBMITTED_PARTITIONS, {k1})  # k2's materialization LOST
        # the loss is real, not merely commented: k2 is absent from the snapshot
        assert k2 not in inst.get_materialized_partitions(SUBMITTED_KEY)
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



# ------------------------------------------------- instance API contract


class TestInstanceContract:
    """Guards the defect this suite once missed: the fake accepted any
    argument type, so ``in_flight_partitions`` passing a
    ``DynamicPartitionsDefinition`` to the real instance (which needs an
    ``AssetKey``) surfaced only as an AttributeError in production. These
    tests fail on the wrong argument type / wrong API shape.
    """

    def test_snapshot_called_with_the_two_enrichment_asset_keys(self) -> None:
        inst = FakeInstance()
        inst.materialize(SUBMITTED_PARTITIONS, {_batch(1)})
        in_flight_partitions(inst)
        assert inst.calls == [SUBMITTED_KEY, HARVESTED_KEY]

    def test_fake_rejects_partitions_definition_argument(self) -> None:
        # The exact wrong call the module used to make: passing the
        # DynamicPartitionsDefinition instead of an AssetKey.
        inst = FakeInstance()
        with pytest.raises(TypeError):
            inst.get_materialized_partitions(SUBMITTED_PARTITIONS)

    def test_real_instance_signature_takes_asset_key(self) -> None:
        # Empirically pinned on Dagster 1.13.11: the real method's first
        # parameter (after self) is named asset_key. If dagster changes the
        # API, this fails here instead of at runtime.
        sig = inspect.signature(DagsterInstance.get_materialized_partitions)
        params = [p for name, p in sig.parameters.items() if name != "self"]
        assert params[0].name == "asset_key", (
            f"DagsterInstance.get_materialized_partitions signature "
            f"changed: {sig}"
        )

    def test_real_ephemeral_instance_roundtrip(self) -> None:
        # End-to-end against a REAL DagsterInstance: materialize the
        # submitted partition, observe it in flight, then harvest and
        # observe it leave. This is the test that would have caught the
        # AttributeError from calling the API with a PartitionsDefinition.
        inst = DagsterInstance.ephemeral()
        key = _batch(42)
        inst.add_dynamic_partitions(SUBMITTED_ASSET_NAME, [key])
        assert in_flight_partitions(inst) == set()
        inst.report_runless_asset_event(
            AssetMaterialization(
                asset_key=SUBMITTED_KEY, partition=key
            )
        )
        assert in_flight_partitions(inst) == {key}
        inst.report_runless_asset_event(
            AssetMaterialization(asset_key=HARVESTED_KEY, partition=key)
        )
        assert in_flight_partitions(inst) == set()


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
