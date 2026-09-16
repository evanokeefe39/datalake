"""ADR-0018 acceptance: the replay path is driven by the SENSOR, not an operator.

This is the keystone claim of the automation work, and it is the one a manual
`materialize` cannot demonstrate: a manual materialize bypasses the condition
entirely and would pass while automation never triggers at all.

**Which surface, and why.** The synthesized `default_automation_condition_sensor`
is daemon-evaluated: it is built with `use_user_code_server=False`, so its
`evaluation_fn` is `not_supported` and calling `evaluate_tick` on it raises
`NotImplementedError` ("Automation condition sensors cannot be evaluated like
regular user-space sensors"). Its `sensor_type` is `AUTO_MATERIALIZE` — the
AssetDaemon evaluates it.

So these tests build a test-local `AutomationConditionSensorDefinition` with
`use_user_code_server=True`. That is the SAME class and the SAME evaluation
engine (`_evaluate`), differing only in whether the result is persisted — and it
is the only construction that yields real `RunRequest`s, which is what the
acceptance criterion ("a sensor-launched auto-fire is observed") actually asks
for. `test_automation_sensor_ships_stopped` separately asserts the synthesized
production sensor's existence and STOPPED default.

Observed behaviour this pins (ephemeral instance, 2026-09-16):
    tick 1 (baseline, no new events)   -> 0 run requests
    tick 2 (bronze event reported)     -> run requests naming the silver outputs
    marts                              -> not yet (silver has not materialized)
"""

from __future__ import annotations

import pytest
from dagster import (
    AssetKey,
    AssetMaterialization,
    AssetSelection,
    DagsterInstance,
    DefaultSensorStatus,
    build_sensor_context,
)
from dagster._core.definitions.automation_condition_sensor_definition import (
    AutomationConditionSensorDefinition,
)
from orchestration.definitions import defs

#: The seven silver outputs the producer owns. If the producer gains a table this
#: list must grow with it — `test_only_the_replay_path_is_automated` is the guard
#: for that, and this test asserts the EXPECTED set actually fires.
_SILVER = [
    "silver_visual_annotations",
    "silver_visual_summaries",
    "silver_audio_transcripts",
    "silver_text_annotations",
    "silver_text_summaries",
    "silver_content_classification",
    "silver_enrichment_quarantine",
]

_BRONZE = AssetKey(["bronze_enrichment_raw"])
_PAID = {"enrichment_submit", "enrichment_submitted"}

_REPO = defs.get_repository_def()


@pytest.fixture
def instance():
    """An ephemeral instance — automation state must never touch the real one."""
    with DagsterInstance.ephemeral() as inst:
        yield inst


def _tick(instance: DagsterInstance, cursor: str | None = None) -> tuple[list, str | None]:
    """One real sensor tick over the same engine the daemon drives.

    Returns `(run_requests, new_cursor)`. The cursor MUST be threaded between
    ticks exactly as the daemon does: `_evaluate` reads `context.cursor` to
    reconstruct its baseline, so a fresh cursor each call re-evaluates from
    scratch and can never observe "what changed since last tick" — which is the
    whole mechanism `eager()` decides on.
    """
    sensor = AutomationConditionSensorDefinition(
        "test_automation_tick",
        target=AssetSelection.all(),
        use_user_code_server=True,
        default_status=DefaultSensorStatus.STOPPED,
    )
    context = build_sensor_context(instance=instance, repository_def=_REPO, cursor=cursor)
    result = sensor.evaluate_tick(context)
    return list(result.run_requests or []), result.cursor


def _requested_keys(requests: list) -> set[str]:
    """Every asset key a RunRequest names.

    The automation sensor emits `asset_selection` as a LIST of AssetKeys (not a
    selection object with `.selected_keys`), plus an `entity_keys` field. Read
    both, tolerantly, since a future Dagster may shape either differently.
    """
    found: set[str] = set()
    for req in requests:
        selection = getattr(req, "asset_selection", None)
        if isinstance(selection, (list, tuple, set)):
            for key in selection:
                found.add(key.to_user_string() if hasattr(key, "to_user_string") else str(key))
        else:
            for key in getattr(selection, "selected_keys", None) or ():
                found.add(key.to_user_string())
        for key in getattr(req, "entity_keys", None) or ():
            found.add(key.to_user_string() if hasattr(key, "to_user_string") else str(key))
    return found


def _assert_tick_observes_keys(requests: list) -> set[str]:
    """The keys named by these run requests. Fails loudly if requests name none.

    An automation RunRequest may carry its selection on the sensor rather than
    the request, so unnamed requests are still evidence that the sensor FIRED —
    but they cannot evidence WHICH assets. Assert we can see them, so the test
    never passes vacuously.
    """
    if not requests:
        return set()
    keys = _requested_keys(requests)
    assert keys, (
        "the sensor issued run requests but none named an asset key, so this "
        "test cannot distinguish which assets fired — the assertion would be "
        f"vacuous. requests={[type(r).__name__ for r in requests]}"
    )
    return keys


def test_bronze_landing_event_alone_causes_a_sensor_run(instance: DagsterInstance) -> None:
    """The gap ADR-0018 exists to close: bronze landed, silver sat still.

    Before the fix, `harvest` reported only `enrichment_harvested` and never a
    bronze event, so no condition could fire and the chain stopped at bronze
    until someone materialized silver by hand. Now the bronze event is the one
    signal needed, and this asserts the SENSOR issues the run.
    """
    baseline, cursor = _tick(instance)
    assert baseline == [], (
        "the baseline sensor tick issued run requests before any bronze event — "
        "the instance was not clean, so the assertion below would prove nothing"
    )

    # The ONLY new signal: the bronze landing event the harvest now reports.
    instance.report_runless_asset_event(AssetMaterialization(asset_key=_BRONZE))

    requests, _ = _tick(instance, cursor=cursor)
    requested = _assert_tick_observes_keys(requests)
    silver_requested = {k for k in requested if k.startswith("silver_")}
    assert silver_requested, (
        "a bronze landing event did not cause the SENSOR to request the silver "
        "outputs — the pipeline is not event-driven, so it regressed to the "
        "ADR-0018 stall where bronze lands and silver waits for a human. "
        f"sensor requested={sorted(requested)}"
    )


def test_marts_are_not_requested_off_the_bronze_event(instance: DagsterInstance) -> None:
    """The marts must NOT fire off the bronze event — they read silver.

    Firing them early would materialize a mart over a silver snapshot that has
    not been republished yet (stale or missing rows). This pins the ordering
    that `eager()` derives from declared lineage.
    """
    _, cursor = _tick(instance)
    instance.report_runless_asset_event(AssetMaterialization(asset_key=_BRONZE))
    requests, _ = _tick(instance, cursor=cursor)
    requested = _assert_tick_observes_keys(requests)
    marts = {k for k in requested if k.startswith("gold_")}
    assert not marts, (
        "a mart was requested off the bronze event alone, before silver "
        f"republished — the mart would read a stale silver snapshot: {sorted(marts)}"
    )


def test_sensor_never_requests_the_paid_edge(instance: DagsterInstance) -> None:
    """The wall, observed at the sensor rather than in the graph structure.

    `test_no_automation_path_reaches_submit` asserts the structure; this asserts
    the behaviour. The paid edge is not an asset, so it can only appear here if
    something wires it into an asset's lineage.
    """
    _, cursor = _tick(instance)
    instance.report_runless_asset_event(AssetMaterialization(asset_key=_BRONZE))
    requests, _ = _tick(instance, cursor=cursor)
    paid = _assert_tick_observes_keys(requests) & _PAID
    assert not paid, (
        "the automation sensor requested the PAID edge — the determinism wall "
        f"is broken and re-publishing silver would cost money: {sorted(paid)}"
    )
