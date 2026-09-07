"""Tests for the out-of-band interactive enrichment tool (scripts/enrich_interactive.py).

Phase 4 of the batch-native enrichment migration: a manual, non-orchestrated
tool that synchronously enriches a chosen subset and idempotently upserts
``gold_analyses``. These tests pin the observable contract:

- subset selection is read-only (no batch_items mutation, no claims)
- gold upsert is idempotent on (post_id, domain) with the current prompt hash
- the worker's 429 taxonomy is reused: quota exhaustion halts the run
- skipped posts (missing/empty caption) never touch Gemini or gold

No live Gemini calls — gemini is a duck-typed fake. Databases are temp files
per repo conventions (tests call ``ensure_gold_analyses`` when touching gold).
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import duckdb
import pytest

from datalake.defs.common.resources import DuckDBResource, SQLiteResource
from datalake.defs.enrichment.assets import ensure_gold_analyses
from datalake.defs.enrichment.prompts import CURRENT_PROMPT_HASH
from datalake.defs.instagram.labels import LABEL_VERSION

SCRIPTS = Path(__file__).resolve().parents[3] / "scripts"


@pytest.fixture
def tool():
    """Load enrich_interactive (which itself imports the worker module)."""
    spec = importlib.util.spec_from_file_location("enrich_interactive", SCRIPTS / "enrich_interactive.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["enrich_interactive"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def state_db(tmp_path):
    """Temp DuckDB with silver_ig_posts + gold_analyses via a real resource."""
    path = tmp_path / "state.duckdb"
    res = DuckDBResource(database=str(path))
    with res.get_connection() as conn:
        conn.execute(
            "CREATE TABLE silver_ig_posts (post_id VARCHAR, caption VARCHAR, media_files VARCHAR)"
        )
        conn.execute(
            "INSERT INTO silver_ig_posts VALUES "
            "('p1', 'caption one', '[]'), ('p2', '', '[]'), ('p3', 'caption three', '[]')"
        )
    ensure_gold_analyses(res)
    return res


@pytest.fixture
def ops_db(tmp_path):
    """Temp ops.sqlite with a minimal batch_items table."""
    path = tmp_path / "ops.sqlite"
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE batch_items (id INTEGER PRIMARY KEY, job_id INTEGER, "
        "payload TEXT, status TEXT)"
    )
    conn.execute(
        "INSERT INTO batch_items (job_id, payload, status) VALUES "
        "(1, ?, 'pending'), (1, ?, 'complete'), (1, ?, 'failed')",
        [
            json.dumps({"post_id": "b1", "domain": "instagram"}),
            json.dumps({"post_id": "b2", "domain": "instagram"}),
            json.dumps({"post_id": "b3", "domain": "instagram"}),
        ],
    )
    conn.commit()
    conn.close()
    return SQLiteResource(database=str(path))


class FakeGemini:
    """Duck-typed GeminiResource stand-in — no live API."""

    def __init__(self, result: str = '{"topic": "test"}', error: Exception | None = None):
        self.result = result
        self.error = error
        self.calls: list[str] = []

    def analyze(self, prompt_text, **kwargs):
        self.calls.append(prompt_text)
        if self.error:
            raise self.error
        return self.result


# ── Subset selection: read-only, drain semantics ────────────────────────────


def test_batch_id_selection_is_read_only_pending_failed_only(tool, ops_db):
    """--batch-id picks pending+failed payloads only, mutating nothing."""
    picked = tool.select_from_batch(ops_db, 1)
    assert picked == ["b1", "b3"]

    conn = ops_db.get_connection()
    with conn:
        statuses = [
            tuple(r)
            for r in conn.execute(
                "SELECT payload, status FROM batch_items ORDER BY id"
            ).fetchall()
        ]
    assert statuses == [
        (json.dumps({"post_id": "b1", "domain": "instagram"}), "pending"),
        (json.dumps({"post_id": "b2", "domain": "instagram"}), "complete"),
        (json.dumps({"post_id": "b3", "domain": "instagram"}), "failed"),
    ]


# ── Subset selection: read-only, drain semantics ────────────────────────────




def test_count_discovery_excludes_gold_and_open_batch_items(tool, state_db, ops_db):
    """Drain-style pick skips gold-covered posts and posts with open batch items."""
    with state_db.get_connection() as conn:
        conn.execute(
            "CREATE TABLE ig_post_labels (post_id VARCHAR, enrich_decision VARCHAR, "
            "label_version INTEGER)"
        )
        conn.execute(
            "INSERT INTO ig_post_labels VALUES "
            "('p1', 'standout', ?), ('p2', 'control', ?), ('p3', 'floor_filler', ?)",
            [LABEL_VERSION, LABEL_VERSION, LABEL_VERSION],
        )
        # p1 already enriched at the current prompt
        conn.execute(
            "INSERT INTO gold_analyses (post_id, domain, prompt_hash, model, "
            "result_json, analysed_at) VALUES ('p1', 'instagram', ?, 'm', '{}', '2026-01-01')",
            [CURRENT_PROMPT_HASH],
        )
    # p3 has an open batch item
    c = ops_db.get_connection()
    try:
        c.execute(
            "INSERT INTO batch_items (job_id, payload, status) VALUES (2, ?, 'pending')",
            [json.dumps({"post_id": "p3", "domain": "instagram"})],
        )
        c.commit()
    finally:
        c.close()

    assert tool.select_from_labels(state_db, ops_db, 10) == ["p2"]
    # count caps the pick
    assert tool.select_from_labels(state_db, ops_db, 0) == []


# ── Gold upsert path with a mocked Gemini ───────────────────────────────────


def test_enrich_posts_upserts_gold_and_is_idempotent(tool, state_db, ops_db):
    gemini = FakeGemini(result='{"topic": "food"}')
    first = tool.enrich_posts(ops_db, state_db, gemini, ["p1"])
    assert first["enriched"] == ["p1"]
    assert first["quota_exhausted"] is False

    with state_db.get_connection() as conn:
        rows = conn.execute(
            "SELECT post_id, domain, prompt_hash, result_json FROM gold_analyses"
        ).fetchall()
    assert rows == [("p1", "instagram", CURRENT_PROMPT_HASH, '{"topic": "food"}')]

    # Re-run: upsert contract — still exactly one row, no duplicates
    gemini2 = FakeGemini(result='{"topic": "food-v2"}')
    second = tool.enrich_posts(ops_db, state_db, gemini2, ["p1"])
    assert second["enriched"] == ["p1"]
    with state_db.get_connection() as conn:
        count, result = conn.execute(
            "SELECT COUNT(*), MAX(result_json) FROM gold_analyses"
        ).fetchone()
    assert count == 1
    assert result in ('{"topic": "food"}', '{"topic": "food-v2"}')

    # The prompt sent to Gemini carried the gold prompt + caption
    assert gemini.calls[0].endswith("caption one")
    assert gemini.calls[0].split("\n")[0] != "caption one"  # prompt preamble present


def test_missing_and_empty_caption_posts_are_skipped_without_gemini(tool, state_db, ops_db):
    gemini = FakeGemini()
    summary = tool.enrich_posts(ops_db, state_db, gemini, ["p2", "ghost-post"])
    assert summary["skipped"] == ["p2", "ghost-post"]
    assert summary["enriched"] == []
    assert gemini.calls == []  # no API calls for skipped posts

    with state_db.get_connection() as conn:
        assert conn.execute("SELECT COUNT(*) FROM gold_analyses").fetchone()[0] == 0


def test_quota_exhaustion_halts_run_without_gold_writes(tool, state_db, ops_db):
    gemini = FakeGemini(error=Exception("429 RESOURCE_EXHAUSTED daily quota limit reached"))
    summary = tool.enrich_posts(ops_db, state_db, gemini, ["p1", "p3"])
    assert summary["quota_exhausted"] is True
    assert summary["enriched"] == []
    # halted before consuming the rest of the subset
    # quota classified on the FIRST attempt — run halts before consuming the subset
    assert len(gemini.calls) == 1

    with state_db.get_connection() as conn:
        assert conn.execute("SELECT COUNT(*) FROM gold_analyses").fetchone()[0] == 0
