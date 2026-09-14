"""ADR-0014 D1/D3: the composite partition key and the retry-budget math.

The key is a readable composite ``<workload>\\x00r<N>\\x00<post_id>`` —
derivable in BOTH directions, no one-way digest (the digest that fused the
round away is the defect this grammar replaces). ``MAX_ROUNDS`` replaces the
retired queue's ``MAX_ATTEMPTS``.
"""

import pytest

from datalake.defs.enrichment.partitions import (
    HARVESTED_ASSET_NAME,
    MAX_ROUNDS,
    SUBMITTED_ASSET_NAME,
    ParsedKey,
    parse_partition_key,
    post_partition_state,
)


WORKLOAD = "content-classification"


class FakeInstance:
    """Minimal PartitionSnapshot double over two materialization spaces."""

    def __init__(self) -> None:
        self._materialized: dict[str, set[str]] = {
            SUBMITTED_ASSET_NAME: set(),
            HARVESTED_ASSET_NAME: set(),
        }

    def materialize(self, space: str, keys: set[str]) -> None:
        self._materialized[space].update(keys)

    def get_materialized_partitions(self, asset_key) -> set[str]:
        return set(self._materialized[asset_key.to_user_string()])


# ── D1: the key grammar ─────────────────────────────────────────────────────


def test_partition_key_is_the_read_composite():
    key = f"{WORKLOAD}\x00r0\x00Cabc123"
    from datalake.defs.enrichment.partitions import partition_key

    assert partition_key(WORKLOAD, 0, ["Cabc123"]) == key


def test_round_zero_uses_the_same_grammar():
    from datalake.defs.enrichment.partitions import partition_key

    assert "\x00r0\x00" in partition_key(WORKLOAD, 0, ["P1"])
    assert "\x00r3\x00" in partition_key(WORKLOAD, 3, ["P1"])


def test_parse_roundtrips_the_round():
    from datalake.defs.enrichment.partitions import partition_key

    for round_n in (0, 1, 4):
        key = partition_key(WORKLOAD, round_n, ["P1"])
        parsed = parse_partition_key(key)
        assert parsed == ParsedKey(
            workload=WORKLOAD, attempt_round=round_n, post_id="P1"
        )


@pytest.mark.parametrize(
    "bad",
    ["no-delimiters", "a\x00b", "w\x00rN\x00p", "w\x00r-1\x00p", "w\x00r0\x00p\x00extra"],
)
def test_parse_rejects_malformed_keys(bad):
    with pytest.raises(ValueError):
        parse_partition_key(bad)


def test_partition_key_requires_exactly_one_post_id():
    from datalake.defs.enrichment.partitions import partition_key

    with pytest.raises(ValueError):
        partition_key(WORKLOAD, 0, [])
    with pytest.raises(ValueError):
        partition_key(WORKLOAD, 0, ["a", "b"])


def test_max_rounds_replaces_max_attempts():
    # The retired queue's MAX_ATTEMPTS=5 is gone from batch.py; the budget
    # is this constant, read from the key itself.
    assert MAX_ROUNDS >= 1
    with pytest.raises(ImportError):
        from datalake.defs.enrichment.batch import MAX_ATTEMPTS  # noqa: F401


# ── D3: the state math the drain guard and retry driver share ───────────────


def test_first_attempt_suppresses_and_enqueues_at_round_zero():
    inst = FakeInstance()
    inst.materialize(SUBMITTED_ASSET_NAME, {f"{WORKLOAD}\x00r0\x00P1"})
    state = post_partition_state(inst, WORKLOAD, "P1")
    assert state.suppressed is True
    assert state.next_round == 0


def test_harvested_round_releases_the_post():
    inst = FakeInstance()
    inst.materialize(SUBMITTED_ASSET_NAME, {f"{WORKLOAD}\x00r0\x00P1"})
    inst.materialize(HARVESTED_ASSET_NAME, {f"{WORKLOAD}\x00r0\x00P1"})
    state = post_partition_state(inst, WORKLOAD, "P1")
    assert state.suppressed is False


def test_failed_round_advances_next_round():
    inst = FakeInstance()
    inst.materialize(SUBMITTED_ASSET_NAME, {f"{WORKLOAD}\x00r0\x00P1"})
    inst.materialize(HARVESTED_ASSET_NAME, {f"{WORKLOAD}\x00r0\x00P1"})
    state = post_partition_state(inst, WORKLOAD, "P1")
    assert state.suppressed is False
    assert state.next_round == 1


def test_retry_round_in_flight_suppresses_whole_post():
    inst = FakeInstance()
    inst.materialize(SUBMITTED_ASSET_NAME, {f"{WORKLOAD}\x00r0\x00P1"})
    inst.materialize(HARVESTED_ASSET_NAME, {f"{WORKLOAD}\x00r0\x00P1"})
    inst.materialize(SUBMITTED_ASSET_NAME, {f"{WORKLOAD}\x00r1\x00P1"})
    state = post_partition_state(inst, WORKLOAD, "P1")
    assert state.suppressed is True


def test_other_workloads_and_posts_do_not_interfere():
    inst = FakeInstance()
    inst.materialize(SUBMITTED_ASSET_NAME, {"other\x00r0\x00P1", f"{WORKLOAD}\x00r0\x00OTHER"})
    assert post_partition_state(inst, WORKLOAD, "P1").suppressed is False
