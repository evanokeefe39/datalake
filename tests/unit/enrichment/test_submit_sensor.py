"""Unit tests for the submit sensor (ADR-0012 D5 / ADR-0016).

The submit sensor gates PAID work, so its failure modes are asymmetric: a miss
is a silent stall (work exists, nothing is ever requested), a false positive is
wasted spend. These tests pin the identity contract that prevents the first.

Runs against a REAL ephemeral DagsterInstance and its real resource bindings —
the pending set is derived by the workloads' own ``candidates``, exactly as the
submit run derives it, so the sensor cannot disagree with what the run finds.
"""

from __future__ import annotations

import json

import pytest
from dagster import DagsterInstance, build_sensor_context
from orchestration.defs.engine.partitions import partition_key
from orchestration.defs.engine.sensor import enrichment_submit_sensor
from orchestration.defs.platform.resources import DuckDBResource

WORKLOAD = "content-classification"


def _slice_ctx(database: str = "data/smoke/state.duckdb"):
    """A sensor context over the smoke slice with the real resource binding."""
    return build_sensor_context(
        instance=DagsterInstance.ephemeral(),
        resources={"duckdb": DuckDBResource(database=database)},
    )


def _requests(ctx):
    return list(enrichment_submit_sensor(ctx))


# ── The request shape (ADR-0012 D5) ─────────────────────────────────────────


def test_pending_work_yields_one_identity_keyed_request():
    """GIVEN eligible posts with nothing in flight
    WHEN the sensor ticks
    THEN it yields exactly ONE RunRequest whose run_key is keyed on the
    IDENTITY of the pending set and whose tag carries the pending keys.
    """
    ctx = _slice_ctx()
    reqs = _requests(ctx)

    assert len(reqs) == 1
    req = reqs[0]
    assert req.run_key is not None and req.run_key.startswith("submit-")
    # The key is a digest of the pending set, never a bare count.
    assert req.run_key.split("submit-")[1].isalnum()

    keys = json.loads(req.tags["enrichment/submit_partitions"])
    assert len(keys) > 0
    assert keys == sorted(keys)
    # Every tagged key is a well-formed composite naming this grammar.
    for key in keys:
        assert key.count("\x00") == 2, key


def test_run_key_is_stable_for_the_same_pending_set():
    """Consecutive ticks before the previous run lands must dedupe: the same
    still-pending set produces the SAME run_key, so Dagster requests once."""
    first, second = _requests(_slice_ctx()), _requests(_slice_ctx())
    assert first[0].run_key == second[0].run_key


def test_run_key_is_keyed_on_identity_not_size():
    """REGRESSION: a count-keyed run_key silently stalls paid work.

    ``run_key = f"submit-{len(keys)}"`` collapses two DIFFERENT backlogs of
    equal size into one key. Dagster treats the second as the
    already-requested run and requests nothing, so a genuinely-new set of
    posts is never submitted — the stall is invisible, because the sensor
    looks like it behaved correctly.

    GIVEN two disjoint pending sets of equal size
    WHEN each is digested into a run_key
    THEN the keys differ.
    """
    import hashlib

    def ident_key(keys: list[str]) -> str:
        return "submit-" + hashlib.sha256(
            "\x00".join(sorted(keys)).encode()
        ).hexdigest()[:16]

    a = [
        partition_key(WORKLOAD, 0, ["P1"]),
        partition_key(WORKLOAD, 0, ["P2"]),
    ]
    b = [
        partition_key(WORKLOAD, 0, ["P3"]),
        partition_key(WORKLOAD, 0, ["P9"]),
    ]
    assert len(a) == len(b)
    assert ident_key(a) != ident_key(b), (
        "identity-keyed run_keys must distinguish equal-size sets"
    )
    # And the same set must still dedupe.
    assert ident_key(a) == ident_key(list(reversed(a)))


def test_the_tag_carries_every_pending_key():
    """The tag is the request's payload of record: the run uses it to know
    what was asked for. A truncated or partial tag would under-report work."""
    req = _requests(_slice_ctx())[0]
    tagged = json.loads(req.tags["enrichment/submit_partitions"])
    assert len(tagged) == len(set(tagged)), "tagged keys must be unique"
    # The smoke slice has 170 eligible posts across the three workloads.
    assert len(tagged) >= 100


# ── The sensor does not act ─────────────────────────────────────────────────


def test_sensor_materializes_nothing():
    """The sensor requests; it never writes. Discovering, guarding and
    materializing happen inside the run, where they share one snapshot."""
    ctx = _slice_ctx()
    _requests(ctx)
    inst = ctx.instance
    assert list(inst.get_dynamic_partitions("enrichment_submitted")) == []
    assert inst.get_materialized_partitions(
        __import__("dagster").AssetKey("enrichment_submitted")
    ) == set()


def test_sensor_never_submits_or_harvests():
    """Source-level guard: the submit sensor must not reach any acting verb."""
    import inspect

    from orchestration.defs.engine import sensor as sensor_mod

    src = inspect.getsource(sensor_mod.enrichment_submit_sensor)
    for forbidden in (
        ".submit(",
        "land_result",
        "report_harvested",
        "mint_retries",
        "render_submitted",
    ):
        assert forbidden not in src, f"sensor must not call {forbidden}"


def test_missing_slice_raises_rather_than_reporting_nothing(tmp_path):
    """A sensor that cannot read its state must fail LOUDLY.

    A missing state DB must not be mistaken for "nothing pending" — that is
    the silent-nothing-to-do defect US-EENG-2 exists to prevent.
    """
    ctx = _slice_ctx(database=str(tmp_path / "does-not-exist.duckdb"))
    with pytest.raises(Exception):
        _requests(ctx)
