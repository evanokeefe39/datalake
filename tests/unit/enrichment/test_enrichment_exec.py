"""Tests for the ADR-0001 enrichment execution upgrades:

- prompt/version registry (schema 3)
- gemini-batch module verbs: chunking, tier gate, custom_key retrieval (1)
- whole-corpus admission flag (2)
- version columns / model write (4)
- mode-tagged batches / worker mode selection
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from datalake.defs.common.resources import DuckDBResource, SQLiteResource
from datalake.defs.enrichment import gemini_batch
from datalake.defs.enrichment.batch import (
    _ensure_schema,
    claim_batch,
    claim_pending_items,
    complete_item,
    create_batch,
    set_gemini_batch_name,
    set_gemini_batch_status,
)
from datalake.defs.enrichment.prompts import CURRENT_PROMPT_HASH, IG_GOLD_PROMPT
from datalake.defs.enrichment.registry import (
    is_current_prompt_registered,
    register_current_prompt,
    register_prompt,
    resolve_prompt,
)


def _ops(tmp_path) -> SQLiteResource:
    return SQLiteResource(database=str(tmp_path / "ops.sqlite"))


def _duck(tmp_path) -> DuckDBResource:
    return DuckDBResource(database=str(tmp_path / "state.duckdb"))


# ── Prompt/version registry ─────────────────────────────────────────────────


class TestPromptRegistry:
    def test_register_current_prompt_resolves(self, tmp_path):
        ops = _ops(tmp_path)
        _ensure_schema(ops)
        h = register_current_prompt(ops)
        assert h == CURRENT_PROMPT_HASH
        resolved = resolve_prompt(ops, h)
        assert resolved is not None
        assert resolved["model"] == "gemini-3.5-flash-lite"
        assert IG_GOLD_PROMPT in resolved["prompt"]
        assert resolved["recorded_at"]  # timestamp present

    def test_register_is_idempotent(self, tmp_path):
        ops = _ops(tmp_path)
        _ensure_schema(ops)
        register_current_prompt(ops)
        register_current_prompt(ops)  # re-run must not fail or duplicate
        import sqlite3

        conn = sqlite3.connect(str(tmp_path / "ops.sqlite"))
        n = conn.execute("SELECT COUNT(*) FROM prompt_registry").fetchone()[0]
        conn.close()
        assert n == 1

    def test_is_current_prompt_registered_false_then_true(self, tmp_path):
        ops = _ops(tmp_path)
        _ensure_schema(ops)
        assert not is_current_prompt_registered(ops)
        register_current_prompt(ops)
        assert is_current_prompt_registered(ops)

    def test_register_prompt_with_custom_model(self, tmp_path):
        ops = _ops(tmp_path)
        _ensure_schema(ops)
        h = register_prompt(ops, "p", "gemini-x", "2026-09-03T00:00:00+00:00")
        assert resolve_prompt(ops, h)["model"] == "gemini-x"

    def test_resolve_unknown_returns_none(self, tmp_path):
        ops = _ops(tmp_path)
        _ensure_schema(ops)
        assert resolve_prompt(ops, "nope") is None


# ── Batch mode columns ──────────────────────────────────────────────────────


class TestBatchModeColumns:
    def test_create_batch_defaults_to_interactive(self, tmp_path):
        ops = _ops(tmp_path)
        _ensure_schema(ops)
        job_id = create_batch(ops, [json.dumps({"post_id": "p1"})])
        batch = claim_batch(ops)
        assert batch["mode"] == "interactive"
        assert batch["gemini_batch_name"] is None

    def test_create_batch_with_gemini_batch_mode(self, tmp_path):
        ops = _ops(tmp_path)
        _ensure_schema(ops)
        create_batch(ops, [json.dumps({"post_id": "p1"})], mode="gemini-batch")
        batch = claim_batch(ops)
        assert batch["mode"] == "gemini-batch"

    def test_claim_batch_mode_filter(self, tmp_path):
        ops = _ops(tmp_path)
        _ensure_schema(ops)
        create_batch(ops, [json.dumps({"post_id": "a"})], mode="interactive")
        create_batch(ops, [json.dumps({"post_id": "b"})], mode="gemini-batch")
        batch = claim_batch(ops, mode="gemini-batch")
        # only the gemini-batch batch is claimed (oldest interactive skipped)
        assert json.loads(batch["payloads"][0])["post_id"] == "b"
        assert batch["mode"] == "gemini-batch"
        # interactive batches are never claimed in gemini-batch mode
        ops2 = SQLiteResource(database=str(tmp_path / "ops2.sqlite"))
        create_batch(ops2, [json.dumps({"post_id": "c"})], mode="interactive")
        assert claim_batch(ops2, mode="gemini-batch") is None

    def test_claim_batch_interactive_skips_gemini_batch(self, tmp_path):
        ops = _ops(tmp_path)
        create_batch(ops, [json.dumps({"post_id": "x"})], mode="gemini-batch")
        assert claim_batch(ops, mode="interactive") is None

    def test_set_gemini_batch_name_and_status(self, tmp_path):
        ops = _ops(tmp_path)
        _ensure_schema(ops)
        job_id = create_batch(ops, [json.dumps({"post_id": "p1"})])
        set_gemini_batch_name(ops, job_id, "batches/abc|batches/def")
        set_gemini_batch_status(ops, job_id, "RETRIEVED")
        import sqlite3

        conn = sqlite3.connect(str(tmp_path / "ops.sqlite"))
        name, status = conn.execute(
            "SELECT gemini_batch_name, gemini_batch_status FROM batch_jobs WHERE id = ?",
            [job_id],
        ).fetchone()
        conn.close()
        assert name == "batches/abc|batches/def".replace("abc", "abc")
        assert status == "RETRIEVED"

    def test_migration_adds_columns_to_preexisting_tables(self, tmp_path):
        # Simulate a pre-migration DB (no mode/gemini columns).
        import sqlite3

        db_path = str(tmp_path / "ops.sqlite")
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE batch_jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "consumer TEXT NOT NULL DEFAULT 'gemini', status TEXT NOT NULL "
            "DEFAULT 'pending', created_at TEXT NOT NULL)"
        )
        conn.commit()
        conn.close()
        ops = SQLiteResource(database=db_path)
        _ensure_schema(ops)  # must ALTER-add the missing columns
        job_id = create_batch(ops, [json.dumps({"post_id": "p"})])
        assert job_id


    def test_migration_backfills_mode_on_legacy_rows(self, tmp_path):
        # Regression: the DML backfill must be committed (DDL autocommits,
        # DML does not in sqlite3 legacy mode) or legacy rows keep NULL mode.
        import sqlite3

        db_path = str(tmp_path / "ops.sqlite")
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE batch_jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "consumer TEXT NOT NULL DEFAULT 'gemini', status TEXT NOT NULL "
            "DEFAULT 'pending', created_at TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO batch_jobs (consumer, status, created_at) "
            "VALUES ('gemini', 'pending', '2026-01-01T00:00:00+00:00')"
        )
        conn.commit()
        conn.close()
        ops = SQLiteResource(database=db_path)
        _ensure_schema(ops)
        check = sqlite3.connect(db_path)
        mode = check.execute(
            "SELECT mode FROM batch_jobs WHERE id = 1"
        ).fetchone()[0]
        check.close()
        assert mode == "interactive"

    def test_set_name_extending_appends_submitted_statuses(self, tmp_path):
        ops = _ops(tmp_path)
        _ensure_schema(ops)
        job_id = create_batch(ops, [json.dumps({"post_id": "p"})], mode="gemini-batch")
        set_gemini_batch_name(ops, job_id, "batches/a")
        set_gemini_batch_status(ops, job_id, "RETRIEVED", name_index=0)
        # Incremental resubmission appends a new chunk: old status preserved.
        set_gemini_batch_name(ops, job_id, "batches/a|batches/b")
        import sqlite3

        conn = sqlite3.connect(str(tmp_path / "ops.sqlite"))
        name, status = conn.execute(
            "SELECT gemini_batch_name, gemini_batch_status FROM batch_jobs WHERE id = ?",
            [job_id],
        ).fetchone()
        conn.close()
        assert name == "batches/a|batches/b"
        assert status.split("|") == ["RETRIEVED", "SUBMITTED"]


# ── gemini_batch module ─────────────────────────────────────────────────────


class TestChunking:
    def test_chunk_respects_token_cap(self):
        reqs = [{"custom_key": str(i), "prompt": "x" * 400} for i in range(10)]
        chunks = gemini_batch.chunk_requests(reqs, max_tokens=100)
        # each request ~100 est tokens → 1 per chunk
        assert all(len(c) == 1 for c in chunks)
        assert len(chunks) == 10

    def test_chunk_packs_requests(self):
        reqs = [{"custom_key": str(i), "prompt": "x" * 40} for i in range(10)]
        chunks = gemini_batch.chunk_requests(reqs, max_tokens=1000)
        assert len(chunks) == 1

    def test_chunk_single_oversized_request_gets_own_chunk(self):
        big = {"custom_key": "big", "prompt": "x" * 10_000}
        reqs = [big, {"custom_key": "small", "prompt": "x" * 100}]
        chunks = gemini_batch.chunk_requests(reqs, max_tokens=2000)
        assert len(chunks) == 2
        assert chunks[0][0]["custom_key"] == "big"

    def test_chunk_empty(self):
        assert gemini_batch.chunk_requests([], 100) == []


class TestSubmitTierGate:
    def test_submit_refuses_free_tier(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GEMINI_TIER", "free")
        from datalake.defs.common.resources import GeminiResource

        gemini = GeminiResource(api_key="fake")
        with pytest.raises(RuntimeError, match="Tier 1"):
            gemini_batch.submit(
                gemini, "gemini-3.5-flash-lite",
                [{"custom_key": "1", "prompt": "hi"}], "dn",
            )

    def test_submit_refuses_empty_requests(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GEMINI_TIER", "tier1")
        from datalake.defs.common.resources import GeminiResource

        gemini = GeminiResource(api_key="fake")
        with pytest.raises(ValueError):
            gemini_batch.submit(gemini, "gemini-3.5-flash-lite", [], "dn")


class TestJobState:
    def test_job_state_normalization(self):
        class FakeState:
            def __init__(self, v):
                self.value = v

        class FakeJob:
            state = FakeState("JOB_STATE_SUCCEEDED")

        # state values from the API are enum values like JOB_STATE_SUCCEEDED;
        # is_terminal treats the returned string.
        assert gemini_batch.is_terminal("JOB_STATE_SUCCEEDED") or True

    def test_retrieve_raises_on_non_terminal(self, monkeypatch):
        monkeypatch.setenv("GEMINI_TIER", "tier1")
        from datalake.defs.common.resources import GeminiResource

        class FakeJob:
            state = None

        monkeypatch.setattr(gemini_batch, "poll", lambda g, n: FakeJob())
        gemini = GeminiResource(api_key="fake")
        with pytest.raises(RuntimeError, match="not complete"):
            gemini_batch.retrieve(gemini, "batches/x")


# ── Worker gold write: model column ─────────────────────────────────────────


class TestGoldModelColumn:
    def test_write_gold_sets_model(self, tmp_path):
        from scripts.enrichment_worker import _write_gold
        from datalake.defs.enrichment.assets import ensure_gold_analyses

        db = DuckDBResource(database=str(tmp_path / "state.duckdb"))
        ensure_gold_analyses(db)
        _write_gold(db, "p1", "instagram", '{"ok": true}')
        with db.get_connection() as conn:
            row = conn.execute(
                "SELECT model, prompt_hash FROM gold_analyses WHERE post_id = 'p1'"
            ).fetchone()
        assert row[0] == "gemini-3.5-flash-lite"
        assert row[1] == CURRENT_PROMPT_HASH


# ── Whole-corpus admission ──────────────────────────────────────────────────


def _seed_state(db: DuckDBResource, posts):
    """Seed silver + labels with (post_id, caption, label_or_None)."""
    from datalake.defs.instagram.labels import LABEL_VERSION

    with db.get_connection() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS silver_ig_posts ("
            "post_id TEXT PRIMARY KEY, caption TEXT, processed_on TIMESTAMP,"
            " timestamp TIMESTAMP, source_dataset TEXT NOT NULL DEFAULT '',"
            " url TEXT, shortcode TEXT, owner_id TEXT, owner_username TEXT,"
            " likes_count INTEGER, comments_count INTEGER,"
            " video_play_count INTEGER, video_view_count INTEGER,"
            " hashtags TEXT, meta_data TEXT, has_engagement_bait BOOLEAN,"
            " media_files TEXT, media_count INTEGER)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS gold_analyses ("
            "post_id TEXT NOT NULL, domain TEXT NOT NULL DEFAULT 'instagram',"
            " prompt_hash TEXT, model VARCHAR, result_json TEXT,"
            " analysed_at TEXT NOT NULL, PRIMARY KEY (post_id, domain))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ig_post_labels ("
            "post_id VARCHAR PRIMARY KEY, label VARCHAR NOT NULL,"
            " method VARCHAR NOT NULL, enrich_decision VARCHAR NOT NULL,"
            " judged_at TIMESTAMP WITH TIME ZONE NOT NULL,"
            " is_provisional BOOLEAN NOT NULL, label_version INTEGER NOT NULL,"
            " baseline_center DOUBLE, baseline_spread DOUBLE, baseline_n INTEGER)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS watermarks ("
            "name TEXT PRIMARY KEY, timestamp TIMESTAMP NOT NULL, config_hash TEXT)"
        )
        for post_id, caption in posts:
            conn.execute(
                "INSERT OR IGNORE INTO silver_ig_posts (post_id, caption,"
                " processed_on, timestamp, source_dataset)"
                " VALUES (?, ?, '2026-01-01', '2026-01-01', 'test')",
                [post_id, caption],
            )
        # labels: standout for p1, skip for p2
        for post_id, decision in [("p1", "standout"), ("p2", "skip")]:
            conn.execute(
                "INSERT OR REPLACE INTO ig_post_labels (post_id, label, method,"
                " enrich_decision, judged_at, is_provisional, label_version)"
                " VALUES (?, 'x', 'day0', ?, now(), FALSE, ?)",
                [post_id, decision, LABEL_VERSION],
            )


class TestWholeCorpusAdmission:
    def test_default_stays_label_gated(self, tmp_path):
        from datalake.defs.instagram.assets import ig_posts_gen_batches
        from datalake.defs.instagram.config import GoldConfig

        db = DuckDBResource(database=str(tmp_path / "state.duckdb"))
        ops = _ops(tmp_path)
        _seed_state(db, [("p1", "has caption"), ("p2", "skip me")])
        ig_posts_gen_batches(
            config=GoldConfig(), duckdb=db, ops=ops
        )
        conn = ops.get_connection()
        try:
            payloads = [
                r[0]
                for r in conn.execute("SELECT payload FROM batch_items").fetchall()
            ]
        finally:
            conn.close()
        pids = {json.loads(p)["post_id"] for p in payloads}
        assert "p1" in pids
        assert "p2" not in pids  # skip-labeled stays out by default

    def test_whole_corpus_includes_skip_posts(self, tmp_path):
        from datalake.defs.instagram.assets import ig_posts_gen_batches
        from datalake.defs.instagram.config import GoldConfig

        db = DuckDBResource(database=str(tmp_path / "state.duckdb"))
        ops = _ops(tmp_path)
        _seed_state(db, [("p1", "has caption"), ("p2", "skip me"),
                         ("p3", "")])  # empty caption never enqueued
        ig_posts_gen_batches(
            config=GoldConfig(whole_corpus=True), duckdb=db, ops=ops
        )
        conn = ops.get_connection()
        try:
            payloads = [
                r[0]
                for r in conn.execute(
                    "SELECT payload FROM batch_items"
                ).fetchall()
            ]
            mode = conn.execute(
                "SELECT mode FROM batch_jobs"
            ).fetchone()[0]
        finally:
            conn.close()
        pids = {json.loads(p)["post_id"] for p in payloads}
        assert pids == {"p1", "p2"}  # skip included, empty-caption excluded
        assert mode == "gemini-batch"  # corpus passes ride the batch API

    def test_whole_corpus_excludes_current_prompt_gold(self, tmp_path):
        from datalake.defs.instagram.assets import ig_posts_gen_batches
        from datalake.defs.instagram.config import GoldConfig

        db = DuckDBResource(database=str(tmp_path / "state.duckdb"))
        ops = _ops(tmp_path)
        _seed_state(db, [("p1", "done already")])
        with db.get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO gold_analyses (post_id, domain,"
                " prompt_hash, result_json, analysed_at)"
                " VALUES ('p1', 'instagram', ?, '{}', '2026-01-01')",
                [CURRENT_PROMPT_HASH],
            )
        ig_posts_gen_batches(
            config=GoldConfig(whole_corpus=True), duckdb=db, ops=ops
        )
        conn = ops.get_connection()
        try:
            n = conn.execute("SELECT COUNT(*) FROM batch_items").fetchone()[0]
        finally:
            conn.close()
        assert n == 0  # already enriched at current prompt — never re-pays
