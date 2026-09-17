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


def test_blocking_checks_are_ancestors_of_a_mart() -> None:
    """A BLOCKING check that gates nothing looks like a gate but is not one.

    Found by the multi_asset refactor: `check_no_silent_loss` was BLOCKING on
    the single `silver_enrichment` asset, so it gated the whole publish. After
    the producer split into seven outputs it landed on the quarantine output —
    which no mart depends on (quarantine's only consumer is
    `v_quarantine_triage`) — so the check blocked nothing while still
    advertising `blocking=True`. That is worse than a non-blocking check,
    because it reads as protection.

    Rule: every blocking check must be an ancestor of at least one mart, so a
    failure actually stops downstream materialization.
    """
    graph = defs.defs.resolve_asset_graph()
    marts = [k for k in _registered_keys() if k.to_user_string().startswith("gold_")]

    def ancestors(key: AssetKey) -> set[AssetKey]:
        seen: set[AssetKey] = set()
        stack = [key]
        while stack:
            for parent in graph.get(stack.pop()).parent_entity_keys:
                if parent not in seen:
                    seen.add(parent)
                    stack.append(parent)
        return seen

    mart_ancestors: set[AssetKey] = set()
    for mart in marts:
        mart_ancestors |= ancestors(mart)

    inert: list[str] = []
    for check in defs.defs.asset_checks or []:
        for spec in check.check_specs:
            if spec.blocking and spec.asset_key not in mart_ancestors:
                inert.append(f"{spec.name} -> {spec.asset_key.to_user_string()}")
    assert not inert, (
        "BLOCKING check(s) gate no mart — they cannot block any downstream "
        f"materialization, so they are gates in name only: {sorted(inert)}"
    )


# ── ADR-0018: the automation wall ───────────────────────────────────────────

#: The paid edge. Auto-materialization must NEVER reach it (ADR-0018 decision 4):
#: `silver_*` has to stay a pure deterministic replay of `bronze_enrichment_raw`
#: (ADR-0011), so if a `materialize` on silver could transitively call the
#: provider, re-publishing silver would produce different rows on different runs
#: and "a schema change is a replay, not a re-bill" would stop being true.
#: This is NOT a cost measure — cost sits on the API credential as
#: defense-in-depth, and the wall does not become removable when cost is handled.
_SUBMIT_KEYS = frozenset({"enrichment_submit", "enrichment_submitted"})


def test_no_automation_path_reaches_submit() -> None:
    """The determinism wall, asserted on a surface that can actually break.

    The paid edge is NOT an asset: `enrichment_submit` is an op-JOB with no
    asset layer and no `asset_selection` (it discovers its own work), and the
    in-flight `enrichment_submitted` is a dynamic partition space reported via
    runless events. A first version of this guard walked the asset graph
    downstream looking for a submit KEY, could therefore never fail, and was
    proven non-discriminating by injecting a `deps=` on `enrichment_submit`
    into the silver producer — the guard still passed. A wall guard that cannot
    fail is worse than none: it reads as ADR-0018 compliance.

    So assert the three things that would actually make silver provider-
    dependent, each independently checkable:

    1. **No asset depends on a submit key.** The only way to wire provider work
       into the asset graph is a `deps=`, and that is exactly what would make a
       `materialize` on silver transitively call the provider.
    2. **No automated asset's dep chain reaches one either** (transitive form of
       1, so a chain through an intermediate node cannot hide it).
    3. **The submit job consumes no asset selection**, so no asset materialization
       (automatic or otherwise) can be the thing that feeds it.
    """
    graph = defs.defs.resolve_asset_graph()
    automated = {
        s.key
        for a in defs.defs.assets
        for s in getattr(a, "specs", [])
        if s.automation_condition is not None
    }
    assert automated, "no asset carries an automation condition; the guard would be vacuous"

    def ancestors(key: AssetKey) -> set[AssetKey]:
        seen: set[AssetKey] = set()
        stack = [key]
        while stack:
            for parent in graph.get(stack.pop()).parent_entity_keys:
                if parent not in seen:
                    seen.add(parent)
                    stack.append(parent)
        return seen

    # 1 + 2: no automated asset (nor any asset at all) sits on a submit key.
    breaches: list[str] = []
    for key in _registered_keys():
        for parent in ancestors(key):
            if parent.to_user_string() in _SUBMIT_KEYS:
                breaches.append(f"{key.to_user_string()} -> {parent.to_user_string()}")
    assert not breaches, (
        "an asset depends (transitively) on a submit key — a materialize on it "
        "would reach the provider, so silver is no longer a pure replay of "
        f"bronze (ADR-0018 decision 4): {sorted(breaches)}"
    )

    # No automated asset may be upstream of the paid edge either.
    reach = sorted(
        f"{k.to_user_string()} -> {d.to_user_string()}"
        for k in automated
        for d in ancestors(k)
        if d.to_user_string() in _SUBMIT_KEYS
    )
    assert not reach, f"auto-materialized asset feeds submit: {reach}"

    # 3: the submit job must consume no assets at all.
    for job in defs.defs.jobs or []:
        if job.name != "enrichment_submit":
            continue
        selected = set()
        try:
            selected = {k.to_user_string() for k in job.asset_layer.asset_keys}
        except Exception:
            selected = set()
        assert not selected, (
            "the submit job now consumes assets — an asset materialization "
            f"could feed the paid edge: {sorted(selected)}"
        )


def test_only_the_replay_path_is_automated() -> None:
    """The automated set is exactly the free replay path — and submit is NOT in it.

    Two failure modes this catches: (1) an asset is made automatic that should
    not be (the paid edge being the catastrophic case), and (2) the automation
    silently vanishes in a refactor, taking the pipeline back to "nothing acts
    on correct lineage" — the ADR-0018 gap this work exists to close.
    """
    expected = {
        "silver_visual_annotations", "silver_visual_summaries", "silver_audio_transcripts",
        "silver_text_annotations", "silver_text_summaries", "silver_content_classification",
        "silver_enrichment_quarantine",
        "gold_post_enrichment", "gold_creator_performance",
        "gold_content_shape_performance", "gold_top_posts",
    }
    actual = {
        s.key.to_user_string()
        for a in defs.defs.assets
        for s in getattr(a, "specs", [])
        if s.automation_condition is not None
    }
    assert actual == expected, (
        "the automated set drifted from the declared replay path; "
        f"missing={sorted(expected - actual)} unexpected={sorted(actual - expected)}"
    )
    # The paid edge must never carry a condition.
    paid = actual & _SUBMIT_KEYS
    assert not paid, f"submit carries an automation condition: {sorted(paid)}"


def test_automation_sensor_ships_stopped() -> None:
    """Declarative automation ships INERT until the owner enables it (ADR-0018).

    A new condition does not start spending effort on its own. The sensor is
    synthesized by the repository (not declared in `Definitions`), so this
    asserts it exists AND that its declared default is STOPPED — if it ever
    defaults RUNNING, the owner has lost the deliberate-enable boundary.
    """
    from dagster._core.definitions.utils import get_default_automation_condition_sensor

    sensor = get_default_automation_condition_sensor({}, defs.defs.resolve_asset_graph())
    assert sensor is not None, (
        "no automation-condition sensor was synthesized — assets carry "
        "conditions but nothing would ever evaluate them, so automation is "
        "decoration"
    )
    assert sensor.name == "default_automation_condition_sensor"
    assert sensor.default_status.value == "STOPPED", (
        "the automation sensor no longer ships STOPPED — a new schedule or "
        f"condition must not start running on its own (got {sensor.default_status})"
    )

# ── Unit 3: the layer-prefix naming convention ──────────────────────────────

#: Deliberate exceptions, each for a stated reason — NOT oversights:
#:
#: - ``ig_roster_raw``: a source IDENTITY, not a graph convenience. It is a
#:   directory of append-only snapshots read via ``ROSTER_BRONZE_DIR``; renaming
#:   it orphans the snapshots and leaves the roster reading empty until the next
#:   fetch (and the miss is SILENT — ``latest_roster_path()`` returns None and
#:   the sweep simply finds no profiles).
#: - ``ig_post_labels``: labels are a distinct ARTIFACT, not a medallion layer.
#:   There is no ``bronze_``/``silver_``/``gold_`` stage that describes it.
_NAMING_EXCEPTIONS = frozenset({"ig_roster_raw", "ig_post_labels"})

#: The layer prefixes an asset key may carry. Serving keys are ``dim_*``/``v_*``
#: and match their table exactly; the medallion layers match theirs.
_LAYER_PREFIXES = ("bronze_", "silver_", "gold_", "dim_", "v_")


def test_asset_keys_follow_the_layer_prefix_convention() -> None:
    """Every asset key is layer-prefixed, or a declared exception.

    The convention (AGENTS.md "Table naming convention"): a key starts with its
    medallion layer, so the layer is never ambiguous and multi-source expansion
    does not need a new naming scheme. Bronze is the documented carve-out on two
    counts: bronze keys name Parquet DATASETS rather than DuckDB tables, and two
    bronze names are on-disk identities.

    Without this guard the convention drifts back one key at a time — the state
    it was in before: three `*_slv` keys, five `*_raw` keys and the newer
    `silver_*`/`gold_*` set all coexisting.
    """
    offenders: list[str] = []
    for key in sorted(_registered_keys()):
        name = key.to_user_string()
        if name in _NAMING_EXCEPTIONS:
            continue
        if not name.startswith(_LAYER_PREFIXES):
            offenders.append(name)
    assert not offenders, (
        "asset key(s) violate the layer-prefix convention — every key must start "
        f"with one of {_LAYER_PREFIXES}, or be a declared exception "
        f"({sorted(_NAMING_EXCEPTIONS)}): {offenders}"
    )


def test_naming_exceptions_are_still_real_keys() -> None:
    """A stale exception would silently excuse a new non-conforming key.

    Same failure class as `DECLARED_EXTERNAL_DEPS`: an allowlist entry that no
    longer names a real key widens the exemption without anyone noticing.
    """
    registered = {k.to_user_string() for k in _registered_keys()}
    stale = sorted(_NAMING_EXCEPTIONS - registered)
    assert not stale, (
        f"naming exception(s) no longer name a registered asset: {stale} — "
        "remove them, or the exemption quietly excuses a future bad key"
    )
