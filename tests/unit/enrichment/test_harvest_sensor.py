"""Unit tests for the Dagster-native gemini-batch harvest (Phase 1, ADR-0007).

Proves the #24 cure end-to-end without any live API:

- the sensor discovers a batch job whose remote state is terminal and issues
  a RunRequest for the harvest job (cursor makes repeat ticks cheap);
- an active (non-terminal) job is NOT harvested;
- job discovery survives a "restart" (fresh cursor; the job name persists in
  ops.sqlite);
- the harvest run applies retrieved results to gold_analyses with the same
  idempotent upsert contract as the worker, closes items, and marks the
  batch complete;
- a FAILED job resubmits its items without burning attempts;
- per-item errors route through fail_item exactly like the worker.

No network: ``gemini_batch.poll``/``retrieve`` are monkeypatched.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from dagster import build_sensor_context

from datalake.defs.common.resources import DuckDBResource, SQLiteResource
from datalake.defs.enrichment import gemini_batch
from datalake.defs.enrichment.batch import (
    _ensure_schema,
    claim_pending_items,
    create_batch,
    set_gemini_batch_name,
    set_gemini_batch_status,
)
from datalake.defs.enrichment.harvest import (
    gemini_batch_harvest_sensor,
    harvest_gemini_batches,
)
from datalake.defs.enrichment.prompts import _DEFAULT_GEMINI_MODEL, CURRENT_PROMPT_HASH

JOB_NAME = "batches/abc123"


@pytest.fixture()
def env(tmp_path):
    """Ops db with a submitted gemini-batch job + a temp DuckDB gold table."""
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    _ensure_schema(ops)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    payloads = [
        json.dumps({"post_id": pid, "domain": "instagram"}) for pid in ("p1", "p2")
    ]
    job_id = create_batch(ops, payloads, mode="gemini-batch")
    items = claim_pending_items(ops, job_id, limit=100)
    assert len(items) == 2
    set_gemini_batch_name(ops, job_id, JOB_NAME)
    return ops, duckdb, job_id, {str(it["id"]): it for it in items}


def _job(state: str, error: str | None = None):
    """Fake google.genai BatchJob: job_state reads ``job.state.value``."""
    return SimpleNamespace(
        state=SimpleNamespace(value=f"JOB_STATE_{state}"), error=error
    )


def _patch_remote(monkeypatch, states: dict[str, str], results: dict | None = None):
    jobs = {
        name: _job(state, error=None if state == "SUCCEEDED" else "terminated")
        for name, state in states.items()
    }
    calls = {"poll": 0}

    def fake_poll(gemini, job_name):
        calls["poll"] += 1
        return jobs[job_name]

    def fake_retrieve(gemini, job_name):
        assert results is not None
        return results

    monkeypatch.setattr(gemini_batch, "poll", fake_poll)
    monkeypatch.setattr(gemini_batch, "retrieve", fake_retrieve)
    return calls


def _evaluate_sensor(ops, cursor: str | None = None):
    context = build_sensor_context(
        resources={"ops": ops, "gemini": SimpleNamespace()},
        cursor=cursor,
    )
    return gemini_batch_harvest_sensor(context), context


class TestHarvestSensor:
    def test_terminal_job_issues_run_request(self, env, monkeypatch):
        ops, _duckdb, job_id, _ = env
        calls = _patch_remote(monkeypatch, {JOB_NAME: "SUCCEEDED"})
        result, context = _evaluate_sensor(ops)
        assert len(result.run_requests) == 1
        req = result.run_requests[0]
        assert req.run_key.startswith(f"harvest-{job_id}-")
        assert req.tags["batch_job_id"] == str(job_id)
        assert calls["poll"] == 1
        # Cursor cached the terminal state.
        assert json.loads(context.cursor)[JOB_NAME] == "SUCCEEDED"

        # Second tick with the cursor: no re-poll (cached terminal), still
        # one request (same run_key — Dagster dedups; the harvest run
        # advances status).
        result2, _ = _evaluate_sensor(ops, cursor=context.cursor)
        assert len(result2.run_requests) == 1
        assert result2.run_requests[0].run_key == req.run_key
        assert calls["poll"] == 1

    def test_active_job_not_harvested(self, env, monkeypatch):
        ops, _duckdb, _job_id, _ = env
        _patch_remote(monkeypatch, {JOB_NAME: "RUNNING"})
        result, context = _evaluate_sensor(ops)
        assert result.run_requests == []
        assert json.loads(context.cursor)[JOB_NAME] == "RUNNING"

    def test_job_survives_restart(self, env, monkeypatch):
        """Fresh cursor (daemon restart): job re-discovered from ops.sqlite."""
        ops, _duckdb, job_id, _ = env
        _patch_remote(monkeypatch, {JOB_NAME: "SUCCEEDED"})
        result, _ = _evaluate_sensor(ops)
        assert len(result.run_requests) == 1
        # Restart: brand-new cursor, same persisted ops row.
        result2, _ = _evaluate_sensor(ops)
        assert len(result2.run_requests) == 1
        assert result2.run_requests[0].run_key.startswith(f"harvest-{job_id}-")

    def test_handled_chunks_not_requested(self, env, monkeypatch):
        ops, _duckdb, job_id, _ = env
        calls = _patch_remote(monkeypatch, {JOB_NAME: "SUCCEEDED"})
        set_gemini_batch_status(ops, job_id, "RETRIEVED")
        result, _ = _evaluate_sensor(ops)
        assert result.run_requests == []
        assert calls["poll"] == 0


class TestHarvestRun:
    def test_terminal_job_harvested_to_gold(self, env, monkeypatch):
        ops, duckdb, job_id, items = env
        ids = sorted(items)
        results = {
            ids[0]: {"ok": True, "text": '{"topic": "a"}', "error": None},
            ids[1]: {"ok": True, "text": '{"topic": "b"}', "error": None},
        }
        _patch_remote(monkeypatch, {JOB_NAME: "SUCCEEDED"}, results)
        summary = harvest_gemini_batches(ops, duckdb, SimpleNamespace())
        assert summary == {"completed": 2, "failed": 0}

        with duckdb.get_connection() as conn:
            rows = conn.execute(
                "SELECT post_id, prompt_hash, model, result_json FROM gold_analyses "
                "ORDER BY post_id"
            ).fetchall()
        assert [(r[0], r[1], r[2]) for r in rows] == [
            ("p1", CURRENT_PROMPT_HASH, _DEFAULT_GEMINI_MODEL),
            ("p2", CURRENT_PROMPT_HASH, _DEFAULT_GEMINI_MODEL),
        ]
        assert json.loads(rows[0][3]) == {"topic": "a"}

        # Items closed; batch marked complete; chunk status RETRIEVED.
        conn = ops.get_connection()
        try:
            item_statuses = [r[0] for r in conn.execute(
                "SELECT status FROM batch_items ORDER BY id"
            ).fetchall()]
            job_status, chunk_status = conn.execute(
                "SELECT status, gemini_batch_status FROM batch_jobs WHERE id = ?",
                [job_id],
            ).fetchone()
        finally:
            conn.close()
        assert item_statuses == ["complete", "complete"]
        assert job_status == "complete"
        assert chunk_status == "RETRIEVED"

    def test_harvest_idempotent(self, env, monkeypatch):
        ops, duckdb, _job_id, items = env
        ids = sorted(items)
        results = {
            ids[0]: {"ok": True, "text": '{"topic": "a"}', "error": None},
            ids[1]: {"ok": True, "text": '{"topic": "b"}', "error": None},
        }
        _patch_remote(monkeypatch, {JOB_NAME: "SUCCEEDED"}, results)
        harvest_gemini_batches(ops, duckdb, SimpleNamespace())
        # Re-run: chunk is RETRIEVED → no poll, no second write.
        summary2 = harvest_gemini_batches(ops, duckdb, SimpleNamespace())
        assert summary2 == {"completed": 0, "failed": 0}
        with duckdb.get_connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM gold_analyses").fetchone()[0]
        assert count == 2

    def test_failed_job_resubmits_items(self, env, monkeypatch):
        ops, duckdb, job_id, _ = env
        _patch_remote(monkeypatch, {JOB_NAME: "FAILED"}, {})
        summary = harvest_gemini_batches(ops, duckdb, SimpleNamespace())
        assert summary == {"completed": 0, "failed": 0}
        conn = ops.get_connection()
        try:
            item_rows = conn.execute(
                "SELECT status, attempts FROM batch_items ORDER BY id"
            ).fetchall()
            chunk_status, error = conn.execute(
                "SELECT gemini_batch_status, gemini_batch_error FROM batch_jobs "
                "WHERE id = ?",
                [job_id],
            ).fetchone()
        finally:
            conn.close()
        assert all(status == "pending" and attempts == 0 for status, attempts in item_rows)
        assert chunk_status == "JOB_FAILED"
        assert error  # job error recorded for resubmission bookkeeping

    def test_per_item_error_routes_to_fail_item(self, env, monkeypatch):
        ops, duckdb, _job_id, items = env
        ids = sorted(items)
        results = {
            ids[0]: {"ok": True, "text": '{"topic": "a"}', "error": None},
            ids[1]: {"ok": False, "error": "blocked", "text": None},
        }
        _patch_remote(monkeypatch, {JOB_NAME: "SUCCEEDED"}, results)
        summary = harvest_gemini_batches(ops, duckdb, SimpleNamespace())
        assert summary == {"completed": 1, "failed": 1}
        with duckdb.get_connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM gold_analyses").fetchone()[0]
        assert count == 1
        conn = ops.get_connection()
        try:
            status, attempts = conn.execute(
                "SELECT status, attempts FROM batch_items WHERE id = ?", [ids[1]]
            ).fetchone()
        finally:
            conn.close()
        assert status == "pending" and attempts == 1
