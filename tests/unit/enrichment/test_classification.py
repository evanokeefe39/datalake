"""Unit tests for the classification conform + migration (US-ESA-2 Phase 5).

Covers: `result_json` round-trips BYTE-identically from `gold_analyses` to
`silver_content_classification` (newlines, unicode, escaped quotes); the key
is ``(post_id, platform)`` — same post_id on different platforms are distinct
rows; the three named key-deviant rows are handled explicitly and appear in
the reconciliation output with reasons — never silently dropped; the
reconciliation identity ``total == conformed + quarantined`` holds and the
gate provably FAILS when a row is silently dropped; the migration is
idempotent (re-run appends nothing new, no duplicates); plan mode writes
nothing. Tests use tmp DuckDB/Parquet roots — never the real
``data/state.duckdb`` or lake — and make zero network/API calls (the conform
is a pure function of bronze).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import orchestration.defs.ig_enriched.slv.classification as classification
import polars as pl
import pytest
from orchestration.defs.engine import landing
from orchestration.defs.platform import paths as lake
from orchestration.defs.platform.schemas import MODEL_LEGACY_NULL as SHARED_MODEL_LEGACY_NULL

from migrations.migrate_classification_to_silver import (
    MIGRATION_RUN_ID,
    backfill_bronze,
    run_migration,
)

NOW = datetime(2026, 9, 10, 12, 0, 0, tzinfo=UTC)

ADVISORY_ID = "3960884543625651437"
SUBSUBTOPIC_ID = "3917136874405145474"
NO_SUBDOMAIN_ID = "3863648947509006965"
DEVIANT_IDS = (ADVISORY_ID, SUBSUBTOPIC_ID, NO_SUBDOMAIN_ID)

GOLD_DDL = """
CREATE TABLE gold_analyses (
    post_id VARCHAR NOT NULL,
    domain VARCHAR NOT NULL,
    prompt_hash VARCHAR,
    result_json VARCHAR,
    analysed_at VARCHAR,
    model VARCHAR,
    PRIMARY KEY (post_id, domain)
)
"""


# ── Fixture builders ───────────────────────────────────────────────────────


def gold_payload(**overrides) -> dict:
    payload = {
        "is_educational": False,
        "is_actionable": False,
        "admiralty": "C2",
        "domain": "Lifestyle",
        "subdomain": "Music",
        "topic": "Live Performance",
        "subtopic": "Band",
        "content_type": "other",
        "style": "casual",
        "format": "other",
    }
    payload.update(overrides)
    return payload


def gold_payload_no_key(**overrides) -> dict:
    """Like gold_payload, but keys passed as ``True`` are REMOVED entirely —
    mirroring the real key-deviant bodies (the key is absent, not NULL)."""
    payload = gold_payload()
    for k, v in overrides.items():
        if v is True:
            payload.pop(k, None)
        else:
            payload[k] = v
    return payload


def gold_rows() -> list[dict]:
    """A representative corpus: normal, byte-hostile, array-form, the 3
    named key-deviant rows, and a NULL-model provenance-gap row."""
    byte_hostile = (
        '{\n  "is_educational": true,\n  "is_actionable": true,\n'
        '  "admiralty": "C4",\n  "domain": "Téchnölogy",\n  "subdomain": "AI 🚀",\n'
        '  "topic": " quo\\"ted ",\n  "subtopic": "line\\nbreak",\n'
        '  "content_type": "demo",\n  "style": "casual",\n  "format": "other"\n}'
    )
    return [
        {
            "post_id": "9001",
            "domain": "instagram",
            "prompt_hash": "24c8e291fdfc28ed",
            "result_json": json.dumps(gold_payload(), indent=2),
            "analysed_at": "2026-09-05T08:27:12.730183+00:00",
            "model": "gemini-3.5-flash-lite",
        },
        {
            "post_id": "9002",  # byte-hostile payload: newlines, unicode, escapes
            "domain": "instagram",
            "prompt_hash": "24c8e291fdfc28ed",
            "result_json": byte_hostile,
            "analysed_at": "2026-09-05T08:27:13.000000+00:00",
            "model": "gemini-3.5-flash-lite",
        },
        {
            "post_id": "9003",  # array form (dual shape the views COALESCE)
            "domain": "instagram",
            "prompt_hash": "24c8e291fdfc28ed",
            "result_json": json.dumps([gold_payload(topic="Array Topic")]),
            "analysed_at": "2026-09-05T08:27:14.000000+00:00",
            "model": "gemini-3.5-flash-lite",
        },
        {
            "post_id": ADVISORY_ID,  # key-deviant: advisory, NO admiralty key
            "domain": "instagram",
            "prompt_hash": "24c8e291fdfc28ed",
            "result_json": json.dumps(gold_payload_no_key(admiralty=True, advisory="C3")),
            "analysed_at": "2026-09-05T08:27:15.000000+00:00",
            "model": "gemini-3.5-flash-lite",
        },
        {
            "post_id": SUBSUBTOPIC_ID,  # key-deviant: subsubtopic, NO subtopic
            "domain": "instagram",
            "prompt_hash": "24c8e291fdfc28ed",
            "result_json": json.dumps(gold_payload_no_key(subtopic=True, subsubtopic="Deep")),
            "analysed_at": "2026-09-05T08:27:16.000000+00:00",
            "model": "gemini-3.5-flash-lite",
        },
        {
            "post_id": NO_SUBDOMAIN_ID,  # key-deviant: no subdomain, no topic
            "domain": "instagram",
            "prompt_hash": "24c8e291fdfc28ed",
            "result_json": json.dumps(
                gold_payload_no_key(subdomain=True, topic=True, domain="Dev")
            ),
            "analysed_at": "2026-09-05T08:27:17.000000+00:00",
            "model": None,  # provenance gap (AC5)
        },
    ]


def make_gold_db(tmp_path: Path, rows: list[dict]) -> str:
    db_path = str(tmp_path / "state.duckdb")
    con = duckdb.connect(db_path)
    con.execute(GOLD_DDL)
    for r in rows:
        con.execute(
            "INSERT INTO gold_analyses VALUES (?, ?, ?, ?, ?, ?)",
            [r["post_id"], r["domain"], r["prompt_hash"], r["result_json"],
             r["analysed_at"], r["model"]],
        )
    con.close()
    return db_path


def read_gold_frame(db_path: str) -> pl.DataFrame:
    con = duckdb.connect(db_path, read_only=True)
    frame = pl.from_arrow(
        con.execute(
            "SELECT post_id, domain, prompt_hash, model, result_json, analysed_at "
            "FROM gold_analyses"
        ).fetch_arrow_table()
    )
    con.close()
    return frame


def bronze_row(post_id: str, platform: str, response_text: str) -> dict:
    return {
        "post_id": post_id,
        "platform": platform,
        "workload": classification.WORKLOAD,
        "prompt_hash": "ph",
        "run_id": "r1",
        "provider": "gemini",
        "model": "m",
        "schema_version": "1",
        "landing_at": NOW,
        "analysed_at": NOW,
        "ok": True,
        "error_message": None,
        "response_text": response_text,
        "request_echo_json": None,
        "input_modality": None,
        "sampling_params_json": None,
    }


@pytest.fixture
def roots(tmp_path):
    return {
        "db": make_gold_db(tmp_path, gold_rows()),
        "bronze_root": str(tmp_path / "lake" / "bronze"),
        "silver_root": str(tmp_path / "lake" / "silver"),
        "report": str(tmp_path / "reconciliation.json"),
    }


# ── result_json byte identity ──────────────────────────────────────────────


def test_result_json_roundtrips_byte_identically(roots):
    """A payload with newlines, unicode, and escaped quotes round-trips
    EXACTLY — gold result_json → bronze response_text → silver result_json →
    the registered DuckDB table, byte for byte (US-ESA-2 AC6: consumers and
    asset_checks assert on this passthrough column)."""
    gold = read_gold_frame(roots["db"])
    assert any("\n" in v for v in gold["result_json"].to_list())
    assert any("🚀" in v for v in gold["result_json"].to_list())
    assert any('\\"' in v for v in gold["result_json"].to_list())

    bronze, _ = backfill_bronze(gold, root=roots["bronze_root"])
    result = classification.conform_classification(
        bronze.filter(pl.col("workload") == classification.WORKLOAD),
        deviance_notes=classification.LEGACY_DEVIANCE_NOTES,
    )
    served = {
        r["post_id"]: r["result_json"] for r in result.silver.iter_rows(named=True)
    }
    for post_id, verbatim in zip(gold["post_id"], gold["result_json"]):
        assert served[post_id] == verbatim, (
            f"result_json not byte-identical for {post_id}"
        )

    # And it survives registration into DuckDB unchanged.
    classification.publish_silver(result.silver, roots["silver_root"])
    dcon = duckdb.connect(":memory:")
    classification.register_silver(dcon, roots["silver_root"])
    registered = dict(
        dcon.execute(
            f"SELECT post_id, result_json FROM {classification.TABLE_ID}"
        ).fetchall()
    )
    for post_id, verbatim in zip(gold["post_id"], gold["result_json"]):
        assert registered[post_id] == verbatim
    dcon.close()


# ── Key: (post_id, platform) — never the overloaded domain ─────────────────


def test_same_post_id_different_platforms_are_distinct_rows():
    body = json.dumps(gold_payload())
    bronze = pl.DataFrame(
        [
            bronze_row("p1", "instagram", body),
            bronze_row("p1", "tiktok", json.dumps(gold_payload(topic="TikTok Topic"))),
        ],
        schema=landing.SCHEMA,
    )
    result = classification.conform_classification(bronze)
    assert result.silver.height == 2
    assert set(result.silver["platform"]) == {"instagram", "tiktok"}
    rows = {
        (r["post_id"], r["platform"]): r["topic"]
        for r in result.silver.iter_rows(named=True)
    }
    assert rows[("p1", "instagram")] == "Live Performance"
    assert rows[("p1", "tiktok")] == "TikTok Topic"


# ── The 3 named key-deviant rows: mapped or quarantined LOUDLY ─────────────


def test_named_deviant_rows_conform_with_recorded_reasons(roots):
    gold = read_gold_frame(roots["db"])
    bronze, _ = backfill_bronze(gold, root=roots["bronze_root"])
    result = classification.conform_classification(
        bronze.filter(pl.col("workload") == classification.WORKLOAD),
        deviance_notes=classification.LEGACY_DEVIANCE_NOTES,
    )
    silver_by_id = {r["post_id"]: r for r in result.silver.iter_rows(named=True)}
    deviant_by_id = {d["post_id"]: d for d in result.deviance}

    # All three LAND (mapped, never dropped) and each is NAMED with a reason.
    for post_id in DEVIANT_IDS:
        assert post_id in silver_by_id, f"deviant {post_id} silently dropped"
        assert post_id in deviant_by_id, (
            f"deviant {post_id} not named in the reconciliation output"
        )
        assert "key_deviant" in deviant_by_id[post_id]["reason"]

    # advisory-not-admiralty: typed admiralty stays NULL (what legacy served);
    # the surrogate value survives verbatim in result_json.
    adv = silver_by_id[ADVISORY_ID]
    assert adv["admiralty"] is None
    assert '"advisory"' in adv["result_json"]

    # subsubtopic-not-subtopic: typed subtopic stays NULL.
    sub = silver_by_id[SUBSUBTOPIC_ID]
    assert sub["subtopic"] is None
    assert '"subsubtopic"' in sub["result_json"]

    # no subdomain/topic: both typed columns NULL, the rest conforms normally.
    nd = silver_by_id[NO_SUBDOMAIN_ID]
    assert nd["subdomain"] is None and nd["topic"] is None
    assert nd["admiralty"] == "C2"

    # Provenance gap is explicit, never silent (AC5): exactly 1 NULL model.
    assert (
        silver_by_id[NO_SUBDOMAIN_ID]["model"]
        == classification.MODEL_LEGACY_NULL
        == SHARED_MODEL_LEGACY_NULL
    )
    assert result.counts["model_gap"] == 1

    # A body carrying NONE of the 10 keys is quarantined loudly with a reason.
    empty = pl.DataFrame(
        [bronze_row("p-empty", "instagram", '{"something": "else"}')],
        schema=landing.SCHEMA,
    )
    empty_result = classification.conform_classification(empty)
    assert empty_result.silver.height == 0
    assert empty_result.quarantine.height == 1
    q = empty_result.quarantine.row(0, named=True)
    assert q["post_id"] == "p-empty"
    assert q["reason_code"] == classification.REASON_EMPTY_BODY


# ── Reconciliation gate: holds on truth, FAILS on a dropped row ────────────


def test_reconciliation_identity_holds_and_can_fail(roots):
    gold = read_gold_frame(roots["db"])
    bronze, _ = backfill_bronze(gold, root=roots["bronze_root"])
    # Add one genuinely unparseable row so the conform produces a REAL
    # quarantine — the gate must be exercised on the class it guards.
    bad = bronze_row("p-bad", "instagram", "{not json at all")
    result = classification.conform_classification(
        pl.concat(
            [bronze.filter(pl.col("workload") == classification.WORKLOAD),
             pl.DataFrame([bad], schema=landing.SCHEMA)]
        ),
        deviance_notes=classification.LEGACY_DEVIANCE_NOTES,
    )
    counts = result.counts
    assert counts["quarantined"] == 1, counts
    total = counts["conformed"] + counts["quarantined"]
    # On the true counts the identity holds.
    assert classification.reconcile(total, counts) == counts

    # A silently DROPPED row violates the identity: remove the quarantined
    # row from the accounting exactly as a buggy migration would.
    dropped = dict(counts)
    dropped["quarantined"] -= 1
    with pytest.raises(classification.ReconciliationError, match="unaccounted"):
        classification.reconcile(total, dropped)

    # And a wholesale drop of the quarantined class fails just as loudly.
    wholesale = dict(counts, quarantined=0)
    with pytest.raises(classification.ReconciliationError, match="unaccounted"):
        classification.reconcile(total, wholesale)


# ── Migration end-to-end: plan writes nothing; apply is idempotent ─────────


def test_migration_plan_writes_nothing_and_apply_is_idempotent(roots):
    db, bronze_root, silver_root = (
        roots["db"], roots["bronze_root"], roots["silver_root"]
    )
    bronze_path = Path(bronze_root) / f"{landing.DATASET_ID}.parquet"
    silver_path = Path(silver_root) / f"{classification.TABLE_ID}.parquet"

    # PLAN (default): the full reconciliation is reported, nothing is written.
    plan = run_migration(
        db_path=db, bronze_root=bronze_root, silver_root=silver_root,
        report_path=roots["report"], apply=False,
    )
    assert plan["mode"] == "plan"
    assert not bronze_path.exists() and not silver_path.exists()
    rec = plan["reconciliation"]
    assert rec["total"] == rec["conformed"] + rec["quarantined"], rec
    assert plan["unaccounted_rows"] == 0
    # Every deviant row is named in the plan report.
    named = {d["post_id"] for d in plan["deviant_rows"]}
    assert set(DEVIANT_IDS) <= named

    # APPLY: reconciliation identity, byte identity, parity all hold.
    first = run_migration(
        db_path=db, bronze_root=bronze_root, silver_root=silver_root,
        report_path=roots["report"], apply=True,
    )
    rec = first["reconciliation"]
    assert rec["total"] == rec["conformed"] + rec["quarantined"]
    assert first["result_json_byte_mismatches"] == 0
    assert first["duplicate_keys"] == 0
    assert all(v == 0 for v in first["typed_parity_mismatches"].values())
    assert first["bronze_rows_appended"] == rec["total"]

    con = duckdb.connect(db, read_only=True)
    n_after_first = con.execute(
        f"SELECT COUNT(*) FROM {classification.TABLE_ID}"
    ).fetchone()[0]
    con.close()
    assert n_after_first == rec["conformed"]
    bronze_n_first = landing.read_responses(bronze_root).height

    # IDEMPOTENT re-apply: nothing new lands, nothing duplicates or corrupts.
    second = run_migration(
        db_path=db, bronze_root=bronze_root, silver_root=silver_root,
        report_path=roots["report"], apply=True,
    )
    assert second["bronze_rows_appended"] == 0
    assert second["bronze_rows_duplicate_natural_keys"] == 0
    assert second["result_json_byte_mismatches"] == 0
    con = duckdb.connect(db, read_only=True)
    assert con.execute(
        f"SELECT COUNT(*) FROM {classification.TABLE_ID}"
    ).fetchone()[0] == n_after_first
    con.close()
    assert landing.read_responses(bronze_root).height == bronze_n_first
    assert second["run_id"] == MIGRATION_RUN_ID


# ── Defect guards: default-root symmetry + live-lake provider guard ────────


@pytest.fixture
def default_root_redirect(tmp_path, monkeypatch):
    """Point the lake module's default bronze root at tmp_path. A test that
    exercises `root=None` must NEVER touch the real `data/lake/bronze` —
    bronze is write-once and discovery keys on file mtime; a stray fixture
    write there re-triggers full silver processing of the live file."""
    root = tmp_path / "lake" / "bronze"
    monkeypatch.setattr(lake, "BRONZE_LAKE", root)
    return root


def test_default_root_apply_materializes_bronze_readable_from_disk(
    roots, default_root_redirect
):
    """Apply with `bronze_root=None` (the CLI default) MUST land the bronze
    Parquet at the DEFAULT lake root and conform from DISK — the live run's
    defect was a memory-only landing (conformed 9576, appended 0, silver 0
    rows). `landing.read_responses(None)` and the backfill write path name
    the same file for the same root value; under the old asymmetric branch
    the file would not exist and every assertion below fails."""
    db, silver_root = roots["db"], roots["silver_root"]
    report = run_migration(
        db_path=db, bronze_root=None, silver_root=silver_root, apply=True
    )
    rec = report["reconciliation"]
    n = rec["total"]
    assert n > 0

    # Same root value (None): the write path and the read path agree.
    disk = landing.read_responses(None)
    assert disk.height == n
    assert (default_root_redirect / f"{landing.DATASET_ID}.parquet").exists()

    # Conform reads the LANDED file, not memory: same counts from disk.
    from_disk = classification.conform_classification(
        disk, deviance_notes=classification.LEGACY_DEVIANCE_NOTES
    )
    assert from_disk.counts["conformed"] == rec["conformed"]
    assert from_disk.counts["quarantined"] == rec["quarantined"]
    assert from_disk.counts["classification_rows"] == n

    # The silver table registered in state was materialized from that file.
    con = duckdb.connect(db, read_only=True)
    silver_rows = con.execute(
        f"SELECT COUNT(*) FROM {classification.TABLE_ID}"
    ).fetchone()[0]
    con.close()
    assert silver_rows == rec["conformed"]
    assert report["bronze_rows_appended"] == n


def test_synthetic_provider_cannot_land_into_default_root(
    tmp_path, default_root_redirect
):
    """The guard that makes the observed fixture leak structurally
    impossible: `provider='fake'` (any non-KNOWN_PROVIDERS value) is refused
    outright at the live default root, while an explicit test root still
    accepts it."""
    kwargs = dict(
        post_id="P0",
        platform="instagram",
        workload=landing.WORKLOAD_CONTENT_CLASSIFICATION,
        provider="fake",
        model="fake-model",
        prompt_hash="ph",
        schema_version="1",
        run_id="job1",
        response_text="{}",
        ok=True,
    )
    with pytest.raises(ValueError, match="LIVE default lake root"):
        landing.land_response(**kwargs, root=None)
    assert not (default_root_redirect / f"{landing.DATASET_ID}.parquet").exists()
    # An explicit tmp root is the sanctioned way to land synthetic fixtures.
    tmp_root = tmp_path / "fixtures"
    assert landing.land_response(**kwargs, root=str(tmp_root)) == 1
