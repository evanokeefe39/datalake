"""Wave-2 tests: the Gemini harvest path lands responses verbatim to bronze.

Proves the ADR-0011 seam behaviour for ``harvest.py`` — harvest = poll +
retrieve + idempotent verbatim landing, BEFORE any parsing:

- a harvested response lands byte-identically (newlines, unicode, quotes
  round-trip exactly — no trimming, no re-serialization);
- landing is idempotent: re-running harvest over the same batch lands ZERO
  new rows (natural key ``(post_id, platform, workload, prompt_hash,
  run_id)`` already present);
- a terminal per-item provider failure lands an ``ok=False`` row with a
  non-empty ``error_message`` (so ``landed ∖ conformed`` distinguishes
  "ran and failed" from "never ran");
- a chunk-level job failure lands ``ok=False`` rows for its items;
- the landing write happens BEFORE any parse call (ordering spy);
- the pre-existing gold write is preserved (both happen, landing first).

No network and no live service: ``gemini_batch.poll``/``retrieve`` are
monkeypatched, ops.sqlite + DuckDB + the bronze root live in tmp_path.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from datalake.defs.common import lake
from datalake.defs.common.resources import DuckDBResource, SQLiteResource
from datalake.defs.enrichment import gemini_batch
from datalake.defs.enrichment import harvest as harvest_module
from datalake.defs.enrichment.batch import (
    _ensure_schema,
    claim_pending_items,
    create_batch,
    set_gemini_batch_name,
)
from datalake.defs.enrichment.harvest import apply_retrieved, harvest_gemini_batches
from datalake.defs.enrichment.landing import (
    DATASET_ID,
    KEY_COLUMNS,
    WORKLOAD_CONTENT_CLASSIFICATION,
    read_responses,
)
from datalake.defs.enrichment.prompts import _DEFAULT_GEMINI_MODEL

JOB_NAME = "batches/def456"

# A provider payload that would be corrupted by any trimming, re-serialization
# or field extraction: newlines, unicode, quotes, trailing whitespace.
VERBATIM_TEXT = '{"topic":"AI","note":"line1\\nline2 éü😀","q":"\\"quoted\\""}  \n'


@pytest.fixture()
def env(tmp_path, monkeypatch):
    """Ops db + DuckDB + a bronze landing root, all inside tmp_path."""
    monkeypatch.setattr(lake, "BRONZE_LAKE", tmp_path / "bronze")
    root = tmp_path / "bronze"
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
    return ops, duckdb, job_id, {str(it["id"]): it for it in items}, root


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

    def fake_poll(gemini, job_name):
        return jobs[job_name]

    def fake_retrieve(gemini, job_name):
        assert results is not None
        return results

    monkeypatch.setattr(gemini_batch, "poll", fake_poll)
    monkeypatch.setattr(gemini_batch, "retrieve", fake_retrieve)


class TestVerbatimLanding:
    def test_success_lands_byte_identically(self, env, monkeypatch):
        ops, duckdb, _job_id, items, root = env
        ids = sorted(items)
        results = {ids[0]: {"ok": True, "text": VERBATIM_TEXT, "error": None}}
        _patch_remote(monkeypatch, {JOB_NAME: "SUCCEEDED"}, results)

        summary = harvest_gemini_batches(ops, duckdb, SimpleNamespace(), root=root)
        assert summary == {"completed": 1, "failed": 0}

        df = read_responses(root)
        assert df.height == 1
        row = df.row(0, named=True)
        # Verbatim: the provider body reaches bronze unmodified.
        assert row["response_text"] == VERBATIM_TEXT
        assert row["ok"] is True
        assert row["error_message"] is None
        assert row["workload"] == WORKLOAD_CONTENT_CLASSIFICATION
        assert row["model"] == _DEFAULT_GEMINI_MODEL
        assert row["run_id"] == JOB_NAME
        assert row["post_id"] == "p1" and row["platform"] == "instagram"

    def test_natural_key_columns_present(self, env, monkeypatch):
        ops, _duckdb, _job_id, items, root = env
        ids = sorted(items)
        results = {ids[0]: {"ok": True, "text": VERBATIM_TEXT, "error": None}}
        _patch_remote(monkeypatch, {JOB_NAME: "SUCCEEDED"}, results)
        harvest_gemini_batches(ops, _duckdb, SimpleNamespace(), root=root)
        df = read_responses(root)
        assert all(col in df.columns for col in KEY_COLUMNS)
        assert DATASET_ID == "bronze_enrichment_raw"

    def test_landing_before_parse(self, env, monkeypatch):
        """The provider-body landing is attempted BEFORE the body is parsed."""
        ops, duckdb, _job_id, items, root = env
        ids = sorted(items)
        results = {ids[0]: {"ok": True, "text": VERBATIM_TEXT, "error": None}}
        _patch_remote(monkeypatch, {JOB_NAME: "SUCCEEDED"}, results)

        order: list[str] = []
        real_land = harvest_module.land_response

        def spy_land(**kwargs):
            order.append("land")
            return real_land(**kwargs)

        class JsonSpy:
            @staticmethod
            def loads(s, *a, **kw):
                order.append("parse")
                return json.loads(s, *a, **kw)

            JSONDecodeError = json.JSONDecodeError

        monkeypatch.setattr(harvest_module, "land_response", spy_land)
        monkeypatch.setattr(harvest_module, "json", JsonSpy())

        harvest_gemini_batches(ops, duckdb, SimpleNamespace(), root=root)
        # apply_retrieved: payload lookup parse → LANDING → body parse.
        assert order == ["parse", "land", "parse"]
        # And the landed text is the verbatim body, not a parsed/normalized
        # re-serialization of it.
        row = read_responses(root).row(0, named=True)
        assert row["response_text"] == VERBATIM_TEXT

    def test_reharvest_lands_zero_new_rows(self, env, monkeypatch):
        """Idempotency proven by running the same batch twice: row count is
        unchanged and the second landing is a pure no-op."""
        ops, duckdb, _job_id, items, root = env
        ids = sorted(items)
        results = {
            ids[0]: {"ok": True, "text": VERBATIM_TEXT, "error": None},
            ids[1]: {"ok": True, "text": '{"topic": "b"}', "error": None},
        }
        _patch_remote(monkeypatch, {JOB_NAME: "SUCCEEDED"}, results)

        # First pass: direct apply — the same run_id/chunk name.
        apply_retrieved(ops, duckdb, results, run_id=JOB_NAME, root=root)
        assert read_responses(root).height == 2

        # Second pass over the SAME batch: zero new rows, no raise.
        apply_retrieved(ops, duckdb, results, run_id=JOB_NAME, root=root)
        df = read_responses(root)
        assert df.height == 2
        # And the verbatim text was not duplicated or clobbered.
        assert sorted(r["response_text"] for r in df.to_dicts()) == sorted(
            [VERBATIM_TEXT, '{"topic": "b"}']
        )


class TestFailureLanding:
    def test_per_item_failure_lands_ok_false_row(self, env, monkeypatch):
        ops, duckdb, _job_id, items, root = env
        ids = sorted(items)
        results = {
            ids[0]: {"ok": True, "text": VERBATIM_TEXT, "error": None},
            ids[1]: {"ok": False, "error": "blocked by provider", "text": None},
        }
        _patch_remote(monkeypatch, {JOB_NAME: "SUCCEEDED"}, results)

        summary = harvest_gemini_batches(ops, duckdb, SimpleNamespace(), root=root)
        assert summary == {"completed": 1, "failed": 1}

        rows = {row["post_id"]: row for row in read_responses(root).to_dicts()}
        assert len(rows) == 2
        failed_row = rows["p2"]
        assert failed_row["ok"] is False
        assert failed_row["error_message"] == "blocked by provider"
        # Success row landed too.
        assert rows["p1"]["ok"] is True
        # Gold kept pure: only the successful item.
        with duckdb.get_connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM gold_analyses").fetchone()[0]
        assert count == 1

    def test_job_failure_lands_failure_rows(self, env, monkeypatch):
        """A terminally failed chunk lands ok=False rows for its items —
        "ran and failed", not "never ran"."""
        ops, duckdb, _job_id, _items, root = env
        _patch_remote(monkeypatch, {JOB_NAME: "FAILED"}, {})

        summary = harvest_gemini_batches(ops, duckdb, SimpleNamespace(), root=root)
        assert summary == {"completed": 0, "failed": 0}

        df = read_responses(root)
        assert df.height == 2
        for row in df.to_dicts():
            assert row["ok"] is False
            assert row["error_message"]  # non-empty
            assert row["run_id"] == JOB_NAME

    def test_failure_landing_idempotent(self, env, monkeypatch):
        ops, _duckdb, _job_id, _items, root = env
        _patch_remote(monkeypatch, {JOB_NAME: "FAILED"}, {})
        harvest_gemini_batches(ops, _duckdb, SimpleNamespace(), root=root)
        assert read_responses(root).height == 2
        # Chunk is now JOB_FAILED (in _HANDLED) — a re-run touches nothing.
        summary2 = harvest_gemini_batches(ops, _duckdb, SimpleNamespace(), root=root)
        assert summary2 == {"completed": 0, "failed": 0}
        assert read_responses(root).height == 2


class TestGoldPreserved:
    def test_gold_write_still_happens_after_landing(self, env, monkeypatch):
        ops, duckdb, _job_id, items, root = env
        ids = sorted(items)
        results = {
            ids[0]: {"ok": True, "text": VERBATIM_TEXT, "error": None},
            ids[1]: {"ok": True, "text": '{"topic": "b"}', "error": None},
        }
        _patch_remote(monkeypatch, {JOB_NAME: "SUCCEEDED"}, results)

        harvest_gemini_batches(ops, duckdb, SimpleNamespace(), root=root)

        # Both happened: landing first, gold second.
        assert read_responses(root).height == 2
        with duckdb.get_connection() as conn:
            gold = conn.execute(
                "SELECT post_id, result_json FROM gold_analyses ORDER BY post_id"
            ).fetchall()
        assert [g[0] for g in gold] == ["p1", "p2"]
        assert json.loads(gold[0][1]) == json.loads(VERBATIM_TEXT)
