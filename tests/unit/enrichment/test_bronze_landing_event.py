"""`report_harvested` also reports the bronze landing event (ADR-0018).

The event is what un-gates the `deps=`-declared consumers of
`bronze_enrichment_raw` (the seven silver outputs). Measured with
`dg.evaluate_automation_conditions`: a spec-declared key DOES un-gate its
downstream once a materialization event is recorded for it, so the missing
event — not an unproducible asset — is what stalled the chain at bronze.

Contract: the event fires on the path that actually wrote bytes, and does NOT
fire when nothing landed. A false bronze event would make the graph lie and fire
downstream automation on a no-op.
"""

from __future__ import annotations

from dagster import AssetKey
from orchestration.defs.engine import harvest


class _CapturingInstance:
    """Minimal instance stub: records dynamic partitions and reported events."""

    def __init__(self) -> None:
        self.dynamic: dict[str, list[str]] = {}
        self.events: list = []

    def add_dynamic_partitions(self, name: str, partition_keys) -> None:
        self.dynamic.setdefault(name, []).extend(partition_keys)

    def report_runless_asset_event(self, event) -> None:
        self.events.append(event)


def _reported_keys(instance: _CapturingInstance) -> list[str]:
    return [e.asset_key.to_user_string() for e in instance.events]


def test_empty_harvest_reports_nothing() -> None:
    """No keys landed → no bronze event. The 'did not write' direction."""
    instance = _CapturingInstance()
    harvest.report_harvested(instance, [])
    assert instance.events == [], (
        "report_harvested emitted an event for an empty harvest — a bronze "
        "materialization on a path that wrote nothing is a false event, and "
        "downstream eager() would fire on a no-op"
    )


def test_bronze_event_fires_alongside_harvested() -> None:
    """Keys landed → the bronze dataset event fires exactly once."""
    instance = _CapturingInstance()
    keys = ["ig/classification/0/post-a", "ig/classification/0/post-b"]
    harvest.report_harvested(instance, keys)

    reported = _reported_keys(instance)
    bronze = [k for k in reported if k == "bronze_enrichment_raw"]
    assert len(bronze) == 1, (
        "the bronze landing event must be reported exactly once per harvest "
        f"cycle that landed bytes; got {len(bronze)} in {reported}"
    )
    # The per-partition harvested events still fire, one per key.
    harvested = [k for k in reported if k == harvest.HARVESTED_ASSET_NAME]
    assert len(harvested) == len(keys), (
        f"expected one harvested event per partition; got {len(harvested)} "
        f"for {len(keys)} keys"
    )


def test_bronze_event_is_not_partitioned() -> None:
    """The bronze event is about the FILE, not a partition key.

    `bronze_enrichment_raw` is one append-only Parquet dataset; the partition
    spaces (`enrichment_submitted`/`enrichment_harvested`) are separate concerns
    with their own dynamic definitions. A partitioned bronze event would need a
    partition definition it does not have and would fail to record.
    """
    instance = _CapturingInstance()
    harvest.report_harvested(instance, ["ig/classification/0/post-a"])
    bronze = [e for e in instance.events if e.asset_key == AssetKey(["bronze_enrichment_raw"])]
    assert bronze, "no bronze event reported"
    assert bronze[0].partition is None, (
        "the bronze event carries a partition; the dataset is unpartitioned and "
        "the event is about the Parquet file's contents"
    )
