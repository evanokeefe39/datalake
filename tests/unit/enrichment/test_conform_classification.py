"""Tests for the classification conform path + legacy gold migration.

Covers (AC):
- the platform/domain mapping: the legacy `domain` KEY value ('instagram')
  populates the silver `platform` KEY column; the niche `domain` BODY column
  is never the platform string;
- `model IS NULL` → the ADR-0014 D5 `unrecorded-legacy-null` sentinel, never NULL;
- a malformed payload (trailing-comma JSON) QUARANTINES with `parse_error`
  rather than disappearing;
- the bronze conform path conforms the classification workload (no skip);
- the migration script is idempotent (second run changes 0 rows) and
  reconciles `silver + quarantine == gold` in the same run.

Tests use tmp_path copies — never the live `data/state.duckdb`.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import duckdb
import orchestration.defs.engine.silver_rt as conform_mod
import orchestration.defs.ig_enriched.slv.classification as classification
import pytest
from orchestration.defs.engine.landing import (
    WORKLOAD_CONTENT_CLASSIFICATION,
    land_response,
)
from orchestration.defs.engine.silver_rt import (
    SILVER_CONTENT_CLASSIFICATION,
    TABLE_SCHEMAS,
    conform,
)
from orchestration.defs.ig_enriched.slv.quarantine import REASON_PARSE_ERROR
from orchestration.defs.platform.schemas import MODEL_LEGACY_NULL

NOW = __import__("datetime").datetime(2026, 9, 14, 12, 0, 0)

CLS_PAYLOAD = {
    "domain": "Lifestyle",
    "subdomain": "Music",
    "topic": "Live Performance",
    "subtopic": "Band",
    "is_educational": False,
    "is_actionable": False,
    "admiralty": "C2",
    "content_type": "other",
    "style": "casual",
    "format": "other",
}


@pytest.fixture
def bronze_root(tmp_path) -> Path:
    return tmp_path / "lake" / "bronze"


@pytest.fixture
def silver_root(tmp_path) -> Path:
    return tmp_path / "lake" / "silver"


def _land_classification(bronze_root, post_id="p1", response_text=None, **overrides):
    kwargs = {
        "post_id": post_id,
        "platform": "instagram",
        "workload": WORKLOAD_CONTENT_CLASSIFICATION,
        "provider": "gemini",
        "model": "gemini-3.5-flash-lite",
        "prompt_hash": "ph-cls",
        "schema_version": "1",
        "run_id": "job-1",
        "response_text": (
            json.dumps(CLS_PAYLOAD) if response_text is None else response_text
        ),
        "ok": True,
        "root": bronze_root,
    }
    kwargs.update(overrides)
    return land_response(**kwargs)


# ── Platform / domain mapping (the ADR-0011 load-bearing fix) ──────────────


def test_platform_is_key_and_domain_is_the_niche(bronze_root, silver_root):
    _land_classification(bronze_root)
    result = conform(root=bronze_root, silver_root=silver_root, now=NOW)
    assert result.counts["conformed"] == 1
    row = conform_mod.read_table(SILVER_CONTENT_CLASSIFICATION, silver_root).row(
        0, named=True
    )
    # The platform value is the KEY column…
    assert row["platform"] == "instagram"
    # …and the niche body column carries the payload's domain, NEVER the
    # platform string.
    assert row["domain"] == CLS_PAYLOAD["domain"]
    assert row["domain"] != "instagram"


def test_classification_table_body_columns_exactly(bronze_root, silver_root):
    expected_body = {
        "domain",
        "subdomain",
        "topic",
        "subtopic",
        "is_educational",
        "is_actionable",
        "admiralty",
        "content_type",
        "style",
        "format",
    }
    schema = TABLE_SCHEMAS[SILVER_CONTENT_CLASSIFICATION]
    assert len(schema) == 2 + 11 + len(expected_body) + 1  # +1: result_json

# ── Provenance ──────────────────────────────────────────────────────────────


def test_provenance_is_carried_not_invented(bronze_root, silver_root):
    _land_classification(bronze_root)
    conform(root=bronze_root, silver_root=silver_root, now=NOW)
    row = conform_mod.read_table(SILVER_CONTENT_CLASSIFICATION, silver_root).row(
        0, named=True
    )
    assert row["provider"] == "gemini"
    assert row["prompt_hash"] == "ph-cls"
    assert row["model"] == "gemini-3.5-flash-lite"
    assert row["schema_version"] == "1"
    assert row["run_id"] == "job-1"
    assert row["analysed_at"] is not None
    for col in ("provider", "model", "prompt_hash", "schema_version", "run_id"):
        assert row[col] is not None, col


def test_model_null_carries_legacy_unknown_sentinel(bronze_root, silver_root):
    _land_classification(bronze_root, model=None)
    conform(root=bronze_root, silver_root=silver_root, now=NOW)
    row = conform_mod.read_table(SILVER_CONTENT_CLASSIFICATION, silver_root).row(
        0, named=True
    )
    assert row["model"] == MODEL_LEGACY_NULL == "unrecorded-legacy-null"
    assert row["model"] is not None


# ── Malformed payload quarantines loudly ───────────────────────────────────


def test_malformed_payload_quarantines_never_vanishes(bronze_root, silver_root):
    _land_classification(bronze_root, post_id="ok1")
    # The known trailing-comma case (~1/95 legacy rows): strict JSON fails.
    _land_classification(
        bronze_root, post_id="bad1", response_text=json.dumps(CLS_PAYLOAD)[:-1] + ",}"
    )
    _land_classification(
        bronze_root, post_id="bad2", response_text='{"domain": "Tech"'  # truncated
    )
    result = conform(root=bronze_root, silver_root=silver_root, now=NOW)
    assert result.counts == {"conformed": 1, "quarantined": 2}
    q = result.quarantine
    assert q.height == 2
    assert set(q["reason_code"].to_list()) == {REASON_PARSE_ERROR}
    # the malformed post_ids are named, not lost
    assert set(q["post_id"].to_list()) == {"bad1", "bad2"}
    assert q["response_excerpt"].str.len_bytes().min() > 0
    # and no silver row exists for them
    silver = conform_mod.read_table(SILVER_CONTENT_CLASSIFICATION, silver_root)
    assert set(silver["post_id"].to_list()) == {"ok1"}


def test_array_form_conforms_like_the_legacy_fallback(bronze_root, silver_root):
    _land_classification(bronze_root, response_text=json.dumps([CLS_PAYLOAD]))
    result = conform(root=bronze_root, silver_root=silver_root, now=NOW)
    assert result.counts["conformed"] == 1
    row = conform_mod.read_table(SILVER_CONTENT_CLASSIFICATION, silver_root).row(
        0, named=True
    )
    assert row["domain"] == CLS_PAYLOAD["domain"]



def test_result_json_byte_identical_to_bronze(bronze_root, silver_root):
    """US-ESA-2 AC6: the conform-path silver row carries the bronze
    ``response_text`` VERBATIM in ``result_json`` — byte-identical
    (newlines, unicode, escaped quotes), never re-serialized."""
    byte_hostile = (
        '{\n  "domain": "Dev",\n  "topic": "émoji 🚀",'
        '\n  "note": "quote \\" and backslash \\\\"\n}'
    )
    _land_classification(bronze_root, post_id="p1", response_text=byte_hostile)
    result = conform(root=bronze_root, silver_root=silver_root, now=NOW)
    assert result.counts["conformed"] == 1
    row = conform_mod.read_table(SILVER_CONTENT_CLASSIFICATION, silver_root).row(
        0, named=True
    )
    assert row["result_json"] == byte_hostile
    # and it is not a re-serialization of the parsed body
    assert row["result_json"] != json.dumps(json.loads(byte_hostile))

# ── Legacy gold_analyses backfill (migration script) ───────────────────────

_GOLD_DDL = """
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

# 8 model-NULL rows (the ADR-0014 D5 audited count the script asserts) + 4
# non-null rows, one of which is malformed → quarantined.
_GOOD_PAYLOAD = CLS_PAYLOAD
_MALFORMED = '{"domain": "Tech", "admiralty": "A1",}'  # trailing comma


def _make_gold_fixture(db_path: Path, *, model_null_rows: int = 8) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(_GOLD_DDL)
        rows = []
        for i in range(model_null_rows):
            rows.append(
                (f"null-{i}", "instagram", "24c8e291", json.dumps(_GOOD_PAYLOAD),
                 "2026-09-05T08:27:12.730183+00:00", None)
            )
        for i in range(3):
            rows.append(
                (f"ok-{i}", "instagram", "abc123", json.dumps(_GOOD_PAYLOAD),
                 "2026-09-06T10:00:00+00:00", "gemini-3.5-flash-lite")
            )
        rows.append(
            ("bad-0", "instagram", "abc123", _MALFORMED,
             "2026-09-06T10:00:00+00:00", "gemini-3.5-flash-lite")
        )
        conn.executemany(
            "INSERT INTO gold_analyses VALUES (?, ?, ?, ?, ?, ?)", rows
        )
    finally:
        conn.close()


@pytest.fixture
def state_copy(tmp_path) -> Path:
    db = tmp_path / "state.duckdb"
    _make_gold_fixture(db)
    return db


def _run_migration(
    state_db: Path, bronze_root: Path, silver_root: Path
) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            sys.executable,
            str(Path(__file__).parents[3] / "migrations" / "migrate_classification_to_silver.py"),
            "--state-db",
            str(state_db),
            "--bronze-root",
            str(bronze_root),
            "--silver-root",
            str(silver_root),
            "--apply",
            "--run-id",
            "test-migration",
        ],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parents[3],
    )


def test_migration_conforms_reconciles_and_is_idempotent(
    state_copy, tmp_path
):
    bronze_root = tmp_path / "lake" / "bronze"
    silver_root = tmp_path / "lake" / "silver"
    r1 = _run_migration(state_copy, bronze_root, silver_root)
    assert r1.returncode == 0, r1.stderr
    report = json.loads(r1.stdout.split("counts:", 1)[1])
    assert report["gold_rows"] == 12
    rec = report["reconciliation"]
    assert rec["conformed"] == 11
    assert rec["quarantined"] == 1

    conn = duckdb.connect(str(state_copy), read_only=True)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM silver_content_classification"
        ).fetchone()[0] == 11
        # sentinel: exactly the observed 8 model-NULL rows, carried as the
        # classification module's explicit gap marker (never a silent NULL)
        assert conn.execute(
            f"SELECT COUNT(*) FROM silver_content_classification "
            f"WHERE model = '{classification.MODEL_LEGACY_NULL}'"
        ).fetchone()[0] == 8
        # reconciliation identity holds on the report...
        assert rec["total"] == rec["conformed"] + rec["quarantined"] == 12
        # ...and every quarantined row is named loudly, never dropped
        qrows = report["quarantined_rows"]
        assert [q["post_id"] for q in qrows] == ["bad-0"]
        assert {q["reason_code"] for q in qrows} == {"parse_error"}
        # platform/domain mapping + provenance parity on every row
        assert conn.execute(
            "SELECT COUNT(*) FROM silver_content_classification s "
            "JOIN gold_analyses g USING (post_id) "
            "WHERE s.prompt_hash != g.prompt_hash"
        ).fetchone()[0] == 0
        before = conn.execute(
            "SELECT * FROM silver_content_classification ORDER BY post_id"
        ).fetchall()
    finally:
        conn.close()

    # second run changes 0 rows
    r2 = _run_migration(state_copy, bronze_root, silver_root)
    assert r2.returncode == 0, r2.stderr
    conn = duckdb.connect(str(state_copy), read_only=True)
    try:
        after = conn.execute(
            "SELECT * FROM silver_content_classification ORDER BY post_id"
        ).fetchall()
    finally:
        conn.close()
    assert after == before


def test_migration_fails_loudly_on_unexpected_sentinel_count(tmp_path):
    db = tmp_path / "state.duckdb"
    _make_gold_fixture(db, model_null_rows=7)  # != the audited 8
    r = _run_migration(db, tmp_path / "lake" / "bronze", tmp_path / "lake" / "silver")
    assert r.returncode != 0
    assert "observed 7" in (r.stderr + r.stdout)
    assert "expected 8" in (r.stderr + r.stdout)
    # and nothing was published
    conn = duckdb.connect(str(db), read_only=True)
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='main'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert "silver_content_classification" not in tables
