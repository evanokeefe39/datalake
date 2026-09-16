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
    """Every ASSET key a RunRequest names.

    The automation sensor emits `asset_selection` as a LIST of AssetKeys (not a
    selection object with `.selected_keys`). `entity_keys` is deliberately NOT
    consulted: it is `asset_selection + asset_check_keys`, so reading it would
    pull check keys such as `check_no_silent_loss` into a set this module
    compares against asset keys.
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
    return found


def _tick_after_bronze(instance: DagsterInstance) -> set[str]:
    """Baseline tick, bronze event, second tick — return the keys requested.

    Asserts the requests are non-empty and NAME keys, so a negative assertion
    built on this cannot pass vacuously when the sensor fired nothing.
    """
    _, cursor = _tick(instance)
    instance.report_runless_asset_event(AssetMaterialization(asset_key=_BRONZE))
    requests, _ = _tick(instance, cursor=cursor)
    keys = _requested_keys(requests)
    assert keys, (
        "the sensor issued no named run requests after a bronze landing event, "
        "so any negative assertion below ('no marts', 'no paid edge') would be "
        "vacuously true. The positive test is the one that must fail here; this "
        "guard stops the negatives from passing for the wrong reason."
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
    requested = _requested_keys(requests)
    assert requested, (
        "the sensor issued run requests but none named an asset key, so this "
        "assertion would be vacuous — the key extraction is wrong, not the "
        f"pipeline. requests={[type(r).__name__ for r in requests]}"
    )
    silver_requested = {k for k in requested if k.startswith("silver_")}
    # NOTE on what this proves, and what it does NOT: the sensor evaluates
    # `AssetSelection.all()` and falls back to `default_condition` when an asset
    # carries none (`automation_context.py:76` — `.automation_condition or
    # evaluator.default_condition`). So this asserts the SENSOR requests the full
    # replay path after a bronze landing event; it does NOT prove each output
    # carries `eager()`. That property is asserted structurally in
    # `test_asset_graph_integrity.py::test_only_the_replay_path_is_automated`,
    # which reads the specs directly. Verified by injection: dropping one
    # output's condition leaves this test green (the default covers it) while the
    # structural guard fails.
    assert silver_requested == set(_SILVER), (
        "a bronze landing event did not fire the FULL replay path — the missing "
        "outputs mean that table will not republish when bronze lands, which is "
        "the ADR-0018 stall in miniature. missing="
        f"{sorted(set(_SILVER) - silver_requested)} unexpected="
        f"{sorted(silver_requested - set(_SILVER))}"
    )


def test_marts_are_not_requested_off_the_bronze_event(instance: DagsterInstance) -> None:
    """The marts must NOT fire off the bronze event — they read silver.

    Firing them early would materialize a mart over a silver snapshot that has
    not been republished yet (stale or missing rows). This pins the ordering
    that `eager()` derives from declared lineage.
    """
    requested = _tick_after_bronze(instance)
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
    paid = _tick_after_bronze(instance) & _PAID
    assert not paid, (
        "the automation sensor requested the PAID edge — the determinism wall "
        f"is broken and re-publishing silver would cost money: {sorted(paid)}"
    )
