"""The asset graph's internal references must resolve.

`dagster definitions validate` checks that a code location LOADS; it does not
check that the keys inside it name anything. A check attached to a key no asset
produces, or an asset depending on a key nothing produces, both load cleanly and
fail only at run time — or, worse, silently never run. That is ISSUES #35's
class: a dead `silver_enrichment_conform` key sat in `@asset_check(asset=...)`
and in a view's `deps=` while the registered producer was `silver_enrichment`.

Nothing else loads the composed graph (ISSUES #30), so this module is the only
place the reference integrity of `orchestration.definitions` is asserted.

Two clauses:

1. **Every asset check targets a registered asset.** A check on a missing key
   never executes and never reports — the exact failure mode a "blocking" DQ
   gate exists to prevent.
2. **Every asset dependency names a registered asset, or is a declared
   external input.** Some keys in `deps=` deliberately name a thing produced
   outside the graph — `bronze_enrichment_raw` is written by the harvest landing
   code, not a Dagster-managed asset. Those are declared below. An
   UNDECLARED missing key is a real dangling reference and fails — that is what
   keeps this clause from being vacuous.
"""

from __future__ import annotations

from dagster import AssetKey
from orchestration import definitions as defs

#: Dependency keys that legitimately name a thing other than a registered
#: asset. Each entry states WHY it is not an `@asset`:
#:
#: - ``bronze_enrichment_raw``: verbatim Parquet landed by the harvest path,
#:   not a Dagster-managed asset. ``silver_enrichment`` declares it as its dep.
#:   (Unit 2 emits a materialization EVENT for it, but an event does not
#:   register an asset — so it stays declared here.)
#:
#: The six ``silver_*`` tables used to be listed here because the single
#: ``silver_enrichment`` asset published them as a side effect. That producer is
#: now a ``@multi_asset`` whose seven outputs ARE registered assets, so they are
#: removed — see ``test_declared_external_deps_are_not_registered``.
DECLARED_EXTERNAL_DEPS: frozenset[str] = frozenset(
    {
        "bronze_enrichment_raw",
    }
)

#: The two partition spaces the enrichment lifecycle is defined over. These are
#: NOT assets: `enrichment_submitted`/`enrichment_harvested` are dynamic
#: partition spaces materialized by the jobs via `report_runless_asset_event`
#: (ADR-0013/0016 — submit discovers its own partitions; there is no drain
#: asset). Named here because the lifecycle's correctness ("in-flight =
#: submitted minus harvested") is meaningless if either space is absent.
LIFECYCLE_PARTITION_SPACES: tuple[str, ...] = ("enrichment_submitted", "enrichment_harvested")


def _registered_keys() -> set[AssetKey]:
    """Every asset key the graph registers.

    A `@multi_asset` carries SEVERAL keys and has no single `.key` — asking for
    `.key` on one raises. `keys` returns them all, so this collects every key
    from every definition, single- or multi-asset.
    """
    return {key for a in defs.defs.assets for key in a.keys}


def test_graph_is_populated() -> None:
    """Guard the guard: an empty graph would make every case below vacuous."""
    keys = _registered_keys()
    assert len(keys) > 20, f"only {len(keys)} assets registered; expected the full graph"


def test_every_check_targets_a_registered_asset() -> None:
    """Clause 1 — a check on a missing key never runs."""
    registered = _registered_keys()
    dangling: list[str] = []
    for check in defs.defs.asset_checks or []:
        for check_key in check.check_keys:
            if check_key.asset_key not in registered:
                dangling.append(f"{check_key.name} -> {check_key.asset_key.to_user_string()}")
    assert not dangling, (
        "asset checks target keys no registered asset produces; these checks "
        f"can never execute: {sorted(dangling)}"
    )


def test_every_asset_dependency_resolves() -> None:
    """Clause 2 — no dangling dep keys beyond the declared external inputs.

    Iterates the GRAPH, not the definitions: a `@multi_asset` is one definition
    with several keys and no single `.key`, so per-key deps must come from the
    resolved graph (`get(key).parent_entity_keys`).
    """
    registered = _registered_keys()
    graph = defs.defs.resolve_asset_graph()
    dangling: list[str] = []
    for key in registered:
        for parent in graph.get(key).parent_entity_keys:
            if parent in registered:
                continue
            if parent.to_user_string() in DECLARED_EXTERNAL_DEPS:
                continue
            dangling.append(f"{key.to_user_string()} -> {parent.to_user_string()}")
    assert not dangling, (
        "assets depend on keys that are neither registered assets nor declared "
        f"external inputs: {sorted(dangling)}"
    )


def test_declared_external_deps_are_not_registered() -> None:
    """A stale entry here would MASK a future deregistration.

    Clause 2 resolves a registered key before consulting the declared set, so a
    stale entry is harmless TODAY — but it would silently absorb the key if it
    were ever deregistered, turning a dangling reference into a quiet pass. That
    is precisely the class the declared set exists to prevent, so assert the two
    sets are disjoint.
    """
    overlap = sorted(set(DECLARED_EXTERNAL_DEPS) & {k.to_user_string() for k in _registered_keys()})
    assert not overlap, (
        "these keys are declared external AND registered as assets — once "
        "registered they must be removed from DECLARED_EXTERNAL_DEPS or they "
        f"mask a future deregistration: {overlap}"
    )


def test_lifecycle_partition_spaces_exist() -> None:
    """The two halves of the enrichment lifecycle must both be defined."""
    from orchestration.defs.engine import partitions

    defined = {
        partitions.SUBMITTED_PARTITIONS.name,
        partitions.HARVESTED_PARTITIONS.name,
    }
    missing = [name for name in LIFECYCLE_PARTITION_SPACES if name not in defined]
    assert not missing, (
        f"enrichment lifecycle partition space(s) missing: {missing}; "
        "in-flight is derived as materialized(enrichment_submitted) minus "
        "materialized(enrichment_harvested) and needs both"
    )
    # The names are the contract: the jobs materialize partitions under these
    # exact strings and the in-flight guard reads them back by name.
    assert partitions.SUBMITTED_ASSET_NAME == "enrichment_submitted"
    assert partitions.HARVESTED_ASSET_NAME == "enrichment_harvested"


def test_every_schedule_target_is_a_registered_asset() -> None:
    """A schedule targeting a key nothing supplies fails to build its job.

    This is the same dangling-reference class as the checks and deps above, and
    it presents identically: `definitions validate` is the only thing that
    notices, and only because it eagerly builds each schedule's asset job. A
    target written from a function's name instead of its registered key is the
    way it happens in practice.
    """
    registered = _registered_keys()
    dangling: list[str] = []
    for schedule in defs.defs.schedules or []:
        # An asset schedule compiles to an anonymous asset job; its selection is
        # where the target keys live.
        job = getattr(schedule, "job", None)
        selection = getattr(job, "selection", None)
        if selection is None:
            continue
        for key in _selected_keys(selection):
            if key not in registered:
                dangling.append(f"{schedule.name} -> {key.to_user_string()}")
    assert not dangling, (
        f"schedule(s) target keys no registered asset supplies: {sorted(dangling)}"
    )


def _selected_keys(selection) -> set[AssetKey]:
    """Every AssetKey a selection names, walking nested operands."""
    found: set[AssetKey] = set()
    for key in getattr(selection, "selected_keys", None) or ():
        found.add(key)
    for operand in getattr(selection, "operands", None) or ():
        found |= _selected_keys(operand)
    return found
