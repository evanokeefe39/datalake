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

import duckdb
import pytest
from dagster import DagsterInstance, build_sensor_context
from orchestration.defs.engine.sensor import enrichment_submit_sensor
from orchestration.defs.platform.resources import DuckDBResource

WORKLOAD = "content-classification"


@pytest.fixture
def labels_db(tmp_path):
    """A self-contained DuckDB carrying the three tables the workloads read.

    This test used to point at `data/smoke/state.duckdb`, which is GITIGNORED —
    so it passed only on a machine where someone had built the smoke slice, and
    failed on a bare checkout. DDL comes from the catalog, so the fixture cannot
    drift from the real tables.
    """
    from orchestration.defs.platform.schemas import duckdb_ddl

    path = tmp_path / "state.duckdb"
    conn = duckdb.connect(str(path))
    for table in ("silver_ig_posts", "ig_post_labels", "silver_content_classification"):
        conn.execute(duckdb_ddl(table))

    conn.executemany(
        "INSERT INTO silver_ig_posts (post_id, caption, owner_id, owner_username, "
        "media_files, media_count, source_dataset, processed_on) "
        "VALUES (?, ?, 'owner_a', 'user_a', '[]', 0, 'test', current_timestamp)",
        [(f"p{i:03d}", f"Caption {i}") for i in range(12)],
    )
    conn.executemany(
        "INSERT INTO ig_post_labels (post_id, label, method, enrich_decision, "
        "judged_at, is_provisional, label_version) "
        "VALUES (?, 'standard', 'test', 'standout', current_timestamp, FALSE, 1)",
        [(f"p{i:03d}",) for i in range(12)],
    )
    conn.close()
    return str(path)


def _slice_ctx(database: str | None = None):
    """A sensor context over a DuckDB with real resource binding."""
    if database is None:
        database = "data/smoke/state.duckdb"
    return build_sensor_context(
        instance=DagsterInstance.ephemeral(),
        resources={"duckdb": DuckDBResource(database=database)},
    )


def _requests(ctx):
    return list(enrichment_submit_sensor(ctx))


# ── The request shape (ADR-0012 D5) ─────────────────────────────────────────


def test_pending_work_yields_one_identity_keyed_request(labels_db):
    """GIVEN eligible posts with nothing in flight
    WHEN the sensor ticks
    THEN it yields exactly ONE RunRequest whose run_key is keyed on the
    IDENTITY of the pending set and whose tag carries the pending keys.
    """
    ctx = _slice_ctx(labels_db)
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


def test_run_key_is_stable_for_the_same_pending_set(labels_db):
    """Consecutive ticks before the previous run lands must dedupe: the same
    still-pending set produces the SAME run_key, so Dagster requests once."""
    first, second = _requests(_slice_ctx(labels_db)), _requests(_slice_ctx(labels_db))
    assert first[0].run_key == second[0].run_key


def test_run_key_is_keyed_on_identity_not_size(labels_db, monkeypatch):
    """REGRESSION: a count-keyed run_key silently stalls paid work.

    ``run_key = f"submit-{len(keys)}"`` collapses two DIFFERENT backlogs of
    equal size into one key. Dagster treats the second as the already-requested
    run and requests nothing, so a genuinely-new set of posts is never
    submitted — invisible, because the sensor looks like it behaved correctly.

    Drives the REAL sensor over two different pending sets of equal size by
    swapping the workload's ``candidates``. Asserting on a locally
    re-implemented hash would prove nothing: such a test passes unchanged
    against the count-keyed defect.
    """
    from orchestration.defs.ig_enriched.slv import workloads as wl_mod

    # Captured ONCE, before any patching: the helper below replaces WORKLOADS,
    # so reading WORKLOADS[0] inside it would derive the second fake from the
    # first — order-dependent, and right only while the fake faithfully copies
    # the template it was built from.
    template = wl_mod.WORKLOADS[0]

    def sensor_key_for(post_ids: list[str]) -> str:
        """The run_key the real sensor yields for exactly these candidates."""
        fake = wl_mod.Workload(
            name=template.name,
            silver_table=template.silver_table,
            candidates=lambda conn, cfg: [
                {"post_id": pid, "caption": "cap", "media_files": None}
                for pid in post_ids
            ],
            build_item=template.build_item,
            estimate=template.estimate,
            media_bearing=template.media_bearing,
            job_spec=template.job_spec,
            prompt_hash=template.prompt_hash,
            schema_version=template.schema_version,
            parse=template.parse,
        )
        # Patch the LOOKUP the sensor performs, not just the tuple: the sensor
        # resolves workloads through workloads_for(cfg) inside its body.
        monkeypatch.setattr(wl_mod, "WORKLOADS", (fake,))
        monkeypatch.setattr(wl_mod, "WORKLOAD_BY_NAME", {fake.name: fake})
        monkeypatch.setattr(wl_mod, "workloads_for", lambda cfg: (fake,))
        reqs = _requests(_slice_ctx(labels_db))
        assert len(reqs) == 1, reqs
        return reqs[0].run_key

    set_a = ["P1", "P2"]
    set_b = ["P3", "P9"]
    assert len(set_a) == len(set_b)

    key_a = sensor_key_for(set_a)
    key_b = sensor_key_for(set_b)
    assert key_a != key_b, (
        "two different pending sets of equal size produced the SAME run_key "
        f"({key_a!r}) — Dagster would treat the second as already requested "
        "and never submit it"
    )

    # And the same set observed twice still dedupes.
    assert sensor_key_for(set_a) == key_a


def test_the_tag_carries_every_pending_key(labels_db):
    """The tag is the request's payload of record: the run uses it to know
    what was asked for. A truncated or partial tag would under-report work."""
    req = _requests(_slice_ctx(labels_db))[0]
    tagged = json.loads(req.tags["enrichment/submit_partitions"])
    assert len(tagged) == len(set(tagged)), "tagged keys must be unique"
    # One key per (workload, eligible post) pair, and nothing else. With 12
    # eligible posts and two applicable workloads (content-classification +
    # growth-facets-text; growth-facets-visual needs media), that is 24. A
    # partial tag would under-report the work the run is asked to do.
    assert len(tagged) == 24


# ── The sensor does not act ─────────────────────────────────────────────────


def test_sensor_materializes_nothing(labels_db):
    """The sensor requests; it never writes. Discovering, guarding and
    materializing happen inside the run, where they share one snapshot."""
    ctx = _slice_ctx(labels_db)
    _requests(ctx)
    inst = ctx.instance
    assert list(inst.get_dynamic_partitions("enrichment_submitted")) == []
    assert inst.get_materialized_partitions(
        __import__("dagster").AssetKey("enrichment_submitted")
    ) == set()


def test_sensor_never_submits_or_harvests(labels_db):
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


# ── Registry ↔ anti-join consistency ────────────────────────────────────────


def test_every_workload_is_visible_to_the_anti_join_check():
    """A workload whose silver tables are absent from WORKLOAD_SILVER_TABLES is
    invisible to check_no_silent_loss — which is exactly how the facet passes
    went unchecked while their silver tables sat empty.

    This pins the two definitions together: the registry's ``silver_table``
    must be the FIRST entry of the check's tuple for that workload, so a human
    reading the registry and the check agree about where responses land.
    """
    from orchestration.defs.ig_enriched.slv.checks import WORKLOAD_SILVER_TABLES
    from orchestration.defs.ig_enriched.slv.workloads import WORKLOADS

    assert WORKLOADS, "the registry must not be empty"
    for workload in WORKLOADS:
        assert workload.name in WORKLOAD_SILVER_TABLES, (
            f"{workload.name} has no WORKLOAD_SILVER_TABLES entry — it would "
            "be invisible to check_no_silent_loss"
        )
        tables = WORKLOAD_SILVER_TABLES[workload.name]
        assert tables, f"{workload.name} maps to an empty table tuple"
        assert workload.silver_table == tables[0], (
            f"{workload.name}: registry says silver_table="
            f"{workload.silver_table!r} but the anti-join maps it to "
            f"{tables[0]!r} — the two definitions have drifted"
        )
