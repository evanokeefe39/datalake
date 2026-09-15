"""Bronze landing on the qwen growth-facets path (US-EENG-3 / ADR-0011/0013).

Every retrieved provider response lands VERBATIM into
``bronze_enrichment_raw`` BEFORE any parsing — visual responses under
``WORKLOAD_GROWTH_FACETS_VISUAL``, text responses under
``WORKLOAD_GROWTH_FACETS_TEXT`` (two different workloads, one submit per
pass). The landing is idempotent by the natural key
``(post_id, platform, workload, prompt_hash, run_id)`` with ``run_id`` =
the service job id, so re-harvesting the same job lands zero new rows.
Failures land ``ok=False`` with a populated ``error_message``.

NO network, NO live service, NO ledger: job state comes from a seam-shaped
fake adapter; the module must not even mention ``facets_batch_jobs`` (the
spike-S5 negative assertion, by name).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datalake.defs.common.schemas import duckdb_ddl  # noqa: E402
from datalake.defs.enrichment import facets_batch  # noqa: E402
from datalake.defs.enrichment.growth_facets_schema import (  # noqa: E402
    GROWTH_FACETS_SCHEMA_VERSION,
)
from datalake.defs.enrichment.landing import (  # noqa: E402
    KEY_COLUMNS,
    WORKLOAD_GROWTH_FACETS_TEXT,
    WORKLOAD_GROWTH_FACETS_VISUAL,
    read_responses,
)
from datalake.defs.enrichment.prompts import (  # noqa: E402
    _DEFAULT_QWEN_MODEL,
    CURRENT_FACETS_PROMPT_HASH,
    CURRENT_TEXT_FACETS_PROMPT_HASH,
)
from datalake.defs.enrichment.qwen_client import QwenServiceError  # noqa: E402
from datalake.defs.enrichment.seam import Result  # noqa: E402

# ── Fixtures ────────────────────────────────────────────────────────────────


class _FakeAdapter:
    """Seam-shaped double for the service-backed adapter (NO network)."""

    name = "service_backed"
    model = _DEFAULT_QWEN_MODEL

    def __init__(self):
        self.docs: dict[str, dict] = {}
        self.results: dict[str, list[Result]] = {}

    def poll(self, handle):
        doc = self.docs.get(handle)
        if doc is None:
            raise QwenServiceError(
                f"unknown job {handle!r}", base_url="fake://service"
            )
        return doc

    def normalize_state(self, raw):
        return str(raw.get("state", ""))

    def is_terminal(self, state):
        return state in {"completed", "failed"}

    def retrieve(self, handle):
        return self.results.get(handle, [])


@pytest.fixture()
def state_conn(tmp_path):
    """Temp duckdb with silver_ig_posts (NEVER data/state.duckdb)."""
    con = duckdb.connect(str(tmp_path / "test_state.duckdb"))
    con.execute(duckdb_ddl("silver_ig_posts"))
    con.execute(
        "INSERT INTO silver_ig_posts (post_id, caption, media_files, "
        "source_dataset, hashtags) VALUES "
        "('p_img', 'see pic', '[\"https://cdn/b.jpg\"]', 'test', '[]'), "
        "('p_caption', 'caption only post', '[]', 'test', '[]')"
    )
    yield con
    con.close()


@pytest.fixture()
def bronze_root(tmp_path) -> str:
    """A throwaway bronze landing root (NEVER the real lake)."""
    return str(tmp_path / "bronze")


def _harvest(monkeypatch, state_conn, bronze_root, job_id, mode, results):
    """Drive one harvest against a fake service job; return its summary."""
    adapter = _FakeAdapter()
    adapter.docs[job_id] = {"state": "completed"}
    adapter.results[job_id] = results
    monkeypatch.setattr(
        facets_batch, "_service_adapter", lambda base_url, model: adapter
    )
    return facets_batch.harvest_facets_batches(
        state_conn, [job_id], mode, root=bronze_root
    )


def _result(custom_key, ok=True, output=None, error=None) -> Result:
    return Result(
        custom_key=custom_key, ok=ok, response_text=output, error=error,
        model=_DEFAULT_QWEN_MODEL, provider="qwen",
    )


# ── Workload separation: one submit per pass, two distinct workloads ───────


def test_visual_and_text_land_under_distinct_workloads(
    state_conn, bronze_root, monkeypatch
):
    visual_text = json.dumps({"visual_facets": {"face_present": False}})
    text_text = json.dumps({"hook_type": "bold_claim"})
    _harvest(monkeypatch, state_conn, bronze_root, "job-visual", "visual",
             [_result("p_img", output=visual_text)])
    _harvest(monkeypatch, state_conn, bronze_root, "job-text", "text",
             [_result("p_caption", output=text_text)])

    df = read_responses(bronze_root)
    assert df.height == 2  # two rows, one per pass
    workloads = set(df["workload"].to_list())
    assert workloads == {WORKLOAD_GROWTH_FACETS_VISUAL,
                         WORKLOAD_GROWTH_FACETS_TEXT}
    visual_row = df.filter(
        df["workload"] == WORKLOAD_GROWTH_FACETS_VISUAL
    )
    text_row = df.filter(df["workload"] == WORKLOAD_GROWTH_FACETS_TEXT)
    assert visual_row["response_text"].to_list() == [visual_text]
    assert text_row["response_text"].to_list() == [text_text]
    # full provenance on the visual row (the text row mirrors it)
    assert visual_row["provider"].to_list() == ["qwen"]
    assert visual_row["model"].to_list() == [_DEFAULT_QWEN_MODEL]
    assert visual_row["prompt_hash"].to_list() == [CURRENT_FACETS_PROMPT_HASH]
    assert text_row["prompt_hash"].to_list() == [
        CURRENT_TEXT_FACETS_PROMPT_HASH
    ]
    assert visual_row["schema_version"].to_list() == [GROWTH_FACETS_SCHEMA_VERSION]
    assert visual_row["run_id"].to_list() == ["job-visual"]
    assert text_row["run_id"].to_list() == ["job-text"]


# ── Verbatim round-trip ────────────────────────────────────────────────────


def test_raw_text_round_trips_byte_identically(
    state_conn, bronze_root, monkeypatch
):
    raw = (
        'line one\n"quoted" \\ backslash \t tab \u00fcnicode \u2713 '
        '{"json": [1, 2]}  \n  trailing spaces'
    )
    _harvest(monkeypatch, state_conn, bronze_root, "job1", "visual",
             [_result("p_img", output=raw)])

    df = read_responses(bronze_root)
    assert df.height == 1
    # byte-identical: no trimming, no re-serialization, no normalization
    assert df["response_text"][0] == raw


# ── Idempotency: re-harvesting the same job lands zero new rows ────────────


def test_reharvest_lands_no_duplicates(state_conn, bronze_root, monkeypatch):
    payload = json.dumps({"visual_facets": {"face_present": False}})
    _harvest(monkeypatch, state_conn, bronze_root, "job1", "visual",
             [_result("p_img", output=payload)])
    assert read_responses(bronze_root).height == 1

    out2 = _harvest(monkeypatch, state_conn, bronze_root, "job1", "visual",
                    [_result("p_img", output=payload)])
    assert out2["landed"] == 0
    df = read_responses(bronze_root)
    assert df.height == 1  # idempotent on (post_id, platform, workload,
    # prompt_hash, run_id) — never a duplicate
    assert df["response_text"].to_list() == [payload]


# ── Failures land ok=False with an error message ──────────────────────────


def test_failed_item_lands_ok_false_with_error(
    state_conn, bronze_root, monkeypatch
):
    _harvest(monkeypatch, state_conn, bronze_root, "job1", "visual",
             [_result("p_img", ok=False, output=None,
                      error="unreadable file")])

    df = read_responses(bronze_root)
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["ok"] is False
    assert row["error_message"] == "unreadable file"
    assert row["workload"] == WORKLOAD_GROWTH_FACETS_VISUAL


def test_null_body_ok_item_lands_as_failure(
    state_conn, bronze_root, monkeypatch
):
    """ok=True with NO body cannot satisfy the verbatim contract — it lands
    as an explicit failure, never as a silently empty success row."""
    _harvest(monkeypatch, state_conn, bronze_root, "job1", "text",
             [_result("p_caption", ok=True, output=None, error=None)])

    df = read_responses(bronze_root)
    assert df["ok"].to_list() == [False]
    assert "no output body" in (df["error_message"][0] or "")


# ── Landing precedes parsing ───────────────────────────────────────────────


def test_landing_precedes_parsing_invalid_json_still_landed(
    state_conn, bronze_root, monkeypatch
):
    out = _harvest(monkeypatch, state_conn, bronze_root, "job1", "visual",
                   [_result("p_img", output="this is not json")])
    assert out["invalid"] == 1 and out["written"] == 0
    df = read_responses(bronze_root)
    assert df.height == 1
    assert df["response_text"].to_list() == ["this is not json"]
    assert df["ok"].to_list() == [True]


# ── ADR-0013: no ledger, by name (spike-S5 negative assertion) ─────────────


def test_no_facets_batch_jobs_anywhere(tmp_path, state_conn, bronze_root,
                                       monkeypatch):
    # the module source never mentions the retired ledger table
    source = Path(facets_batch.__file__).read_text(encoding="utf-8")
    assert "facets_batch_jobs" not in source
    # a submit + harvest cycle creates/consults NO such table either
    import sqlite3

    ops_db = tmp_path / "test_ops.sqlite"
    con = sqlite3.connect(str(ops_db))
    con.execute("CREATE TABLE other_table (id INTEGER PRIMARY KEY)")
    con.commit()
    con.close()
    # submit goes through the seam adapter (patched to a stub returning svc-1)
    submitted: list = []

    class _StubAdapter:
        def submit(self, items, *, job_spec=None):
            submitted.append(list(items))
            return "svc-1"

    monkeypatch.setattr(
        facets_batch, "_service_adapter", lambda base_url, model: _StubAdapter()
    )
    facets_batch.submit_facets_batch(
        [{"custom_key": "p1", "prompt": "p", "images": []}], "text"
    )
    assert submitted, "submit must reach the service adapter"
    _harvest(monkeypatch, state_conn, bronze_root, "svc-1", "text",
             [_result("p_caption", output=json.dumps({"hook_type": "x"}))])
    con = sqlite3.connect(str(ops_db))
    tables = {
        r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    con.close()
    assert "facets_batch_jobs" not in tables
    assert tables == {"other_table"}
    assert "facets_batch_jobs" not in " ".join(KEY_COLUMNS)
