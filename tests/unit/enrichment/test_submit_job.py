"""Unit tests for the Dagster-native gemini-batch submit job (Phase 2).

Proves, with no network (mocked ``gemini_batch.submit`` + a cache-faithful
fake ``lookup_or_upload_all``; temp ops.sqlite + DuckDB per repo conventions):

- submit claims a pending, unsubmitted ``gemini-batch`` batch, pre-uploads
  media, builds requests via the relocated builder
  (``analysis.build_requests_for_items``), submits via ``gemini_batch.submit``
  and persists the returned chunk name on the batch job (items in-flight);
- idempotent: a batch that already carries a ``gemini_batch_name`` is
  skipped — re-discovery finds nothing and a direct ``submit_batch`` never
  calls the API;
- failure: a submission failure reschedules the claimed items as pending
  with attempts preserved (backoff ~300s) — nothing stranded in
  'processing', the batch stays discoverable;
- the tier gate raises on FREE tier BEFORE any queue mutation;
- the seam-tagged op drives the same core under a real op context
  (ADR-0008).
"""

from __future__ import annotations

import json
import sqlite3

import pytest
from dagster import build_op_context
from dagster_duckdb import DuckDBResource

from datalake.defs.common.resources import SQLiteResource
from datalake.defs.enrichment import analysis as analysis_module
from datalake.defs.enrichment import gemini_batch
from datalake.defs.enrichment import media_upload as media_upload_module
from datalake.defs.enrichment.batch import (
    _ensure_schema,
    create_batch,
    set_gemini_batch_name,
)
from datalake.defs.enrichment.media_cache import (
    _ensure_schema as _ensure_media_schema,
)
from datalake.defs.enrichment.media_cache import (
    url_hash,
)
from datalake.defs.enrichment.submit import (
    submit_batch,
    submit_gemini_batches_op,
    submit_pending_gemini_batches,
)

MEDIA = [{"uri": "files/abc", "mime_type": "image/jpeg"}]


@pytest.fixture()
def env(tmp_path, monkeypatch):
    """Pending unsubmitted gemini-batch batch + DuckDB silver rows p1/p2/p3.

    p1 is media-bearing, p2/p3 text-only. ``GEMINI_TIER=tier1`` so the
    submit tier gate passes without a paid key.
    """
    monkeypatch.setenv("GEMINI_TIER", "tier1")
    ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
    _ensure_schema(ops)
    _ensure_media_schema(ops)
    duckdb = DuckDBResource(database=str(tmp_path / "state.duckdb"))
    with duckdb.get_connection() as conn:
        conn.execute(
            "CREATE TABLE silver_ig_posts "
            "(post_id VARCHAR, caption VARCHAR, media_files VARCHAR)"
        )
        conn.execute(
            "INSERT INTO silver_ig_posts VALUES "
            "('p1', 'caption one', '[\"https://cdn/img1.jpg\"]'), "
            "('p2', 'caption two', NULL), "
            "('p3', 'caption three', NULL)"
        )
    payloads = [
        json.dumps({"post_id": pid, "domain": "instagram"})
        for pid in ("p1", "p2", "p3")
    ]
    job_id = create_batch(ops, payloads, mode="gemini-batch")
    return ops, duckdb, job_id


def _fake_lookup(ops: SQLiteResource, calls: list[str]):
    """Cache-contract-faithful fake of ``lookup_or_upload_all``.

    First call records the File API URI in ``media_metadata`` (mirrors the
    real upload persistence); later calls are cache hits. No network.
    """

    def fake(ops_, gemini, media_files_json, inline_images=False):
        urls = json.loads(media_files_json)
        calls.extend(urls)
        for url in urls:
            conn = ops_.get_connection()
            try:
                row = conn.execute(
                    "SELECT upload_state FROM media_metadata "
                    "WHERE media_url_hash = ?",
                    [url_hash(url)],
                ).fetchone()
                if not row or row[0] != "uploaded":
                    conn.execute(
                        "INSERT OR REPLACE INTO media_metadata "
                        "(media_url_hash, media_url, file_api_uri, mime_type, "
                        " upload_state, expires_at, created_at, uploaded_at) "
                        "VALUES (?, ?, 'files/abc', 'image/jpeg', 'uploaded', "
                        "'9999-12-31T00:00:00+00:00', '2026-01-01', '2026-01-01')",
                        [url_hash(url), url],
                    )
                    conn.commit()
            finally:
                conn.close()
        return [dict(m) for m in MEDIA]

    return fake


def _patch_media_fakes(env, monkeypatch) -> list[str]:
    """Patch the media fake into both consumers of the real uploader:
    the pre-warm core (media_upload's binding) and the request builder
    (analysis's binding).
    Returns the shared call log."""
    ops, _, _ = env
    calls: list[str] = []
    fake = _fake_lookup(ops, calls)
    monkeypatch.setattr(media_upload_module, "lookup_or_upload_all", fake)
    monkeypatch.setattr(analysis_module, "lookup_or_upload_all", fake)
    return calls


def _job_row(ops: SQLiteResource, job_id: int) -> tuple:
    conn = ops.get_connection()
    try:
        row = conn.execute(
            "SELECT status, gemini_batch_name FROM batch_jobs WHERE id = ?",
            [job_id],
        ).fetchone()
        return (row["status"], row["gemini_batch_name"])
    finally:
        conn.close()


def _item_rows(ops: SQLiteResource, job_id: int) -> list[tuple]:
    conn = ops.get_connection()
    try:
        return conn.execute(
            "SELECT id, status, attempts, scheduled_for, payload "
            "FROM batch_items WHERE job_id = ? ORDER BY id",
            [job_id],
        ).fetchall()
    finally:
        conn.close()


def _set_attempts(ops: SQLiteResource, item_id: int, attempts: int) -> None:
    conn = sqlite3.connect(str(ops.database))
    conn.execute(
        "UPDATE batch_items SET attempts = ? WHERE id = ?", [attempts, item_id]
    )
    conn.commit()
    conn.close()


class TestSubmitClaimsSubmitsPersists:
    def test_one_pass_media_and_text_only(self, env, monkeypatch):
        """Pre-warm → claim → build → submit → persist chunk name."""
        ops, duckdb, job_id = env
        calls = _patch_media_fakes(env, monkeypatch)

        captured_requests: list[dict] = []
        submit_calls: list[str] = []

        def fake_submit(gemini, model, requests, display_name=""):
            submit_calls.append(display_name)
            captured_requests.extend(requests)
            return ["batches/xyz"]

        monkeypatch.setattr(gemini_batch, "submit", fake_submit)

        result = submit_pending_gemini_batches(ops, duckdb, None, limit=1)

        assert result["submitted"] == 1
        assert result["batches"] == [job_id]
        assert submit_calls == [f"enrich-job{job_id}"]

        # Media pre-warm AND builder both resolved p1's URL (fake, no network)
        assert calls == ["https://cdn/img1.jpg", "https://cdn/img1.jpg"]

        # Batch row: claimed + chunk name persisted
        status, name = _job_row(ops, job_id)
        assert status == "processing"
        assert name == "batches/xyz"

        # Items: all in-flight (claimed), none stranded pending
        items = _item_rows(ops, job_id)
        assert [it[1] for it in items] == ["processing"] * 3
        # Requests: p1 media-bearing; p2/p3 text-only (non-empty captions).
        # All three submittable → 3 requests, only p1 carries media_files
        by_post = {r["post_id"]: r for r in captured_requests}
        assert len(captured_requests) == 3
        assert by_post["p1"]["media_files"] == MEDIA
        assert "media_files" not in by_post["p2"]
        assert by_post["p2"]["prompt"].endswith("caption two")


class TestSubmitIdempotentSkip:
    def test_rediscovery_finds_nothing_after_submit(self, env, monkeypatch):
        """A submitted batch (name set) is never re-submitted by discovery."""
        ops, duckdb, job_id = env
        _patch_media_fakes(env, monkeypatch)

        submit_calls: list[int] = []

        def fake_submit(gemini, model, requests, display_name=""):
            submit_calls.append(1)
            return ["batches/xyz"]

        monkeypatch.setattr(gemini_batch, "submit", fake_submit)

        first = submit_pending_gemini_batches(ops, duckdb, None, limit=1)
        assert first["submitted"] == 1
        assert len(submit_calls) == 1

        second = submit_pending_gemini_batches(ops, duckdb, None, limit=1)
        assert second == {"submitted": 0, "batches": []}
        assert len(submit_calls) == 1  # API never called again

    def test_submit_batch_skips_already_named_batch(self, env, monkeypatch):
        """Direct ``submit_batch`` on a named batch is a defensive no-op."""
        ops, duckdb, job_id = env
        set_gemini_batch_name(ops, job_id, "batches/xyz")
        items_before = _item_rows(ops, job_id)

        monkeypatch.setattr(
            gemini_batch, "submit",
            lambda *a, **k: pytest.fail("submit must not be called on a named batch"),
        )

        result = submit_batch(ops, duckdb, None, job_id)
        assert result == {"submitted": 0, "skipped": True}
        # No queue mutation either
        assert _item_rows(ops, job_id) == items_before


class TestSubmitFailureReschedule:
    def test_submit_failure_reschedules_items_attempts_preserved(self, env, monkeypatch):
        """API failure → items pending again, attempts untouched, backoff set."""
        ops, duckdb, job_id = env
        _patch_media_fakes(env, monkeypatch)

        # Burn attempts first so "preserved" is observable (not just zero)
        items_before = _item_rows(ops, job_id)
        for item_id, *_ in items_before:
            _set_attempts(ops, item_id, 2)

        def raising_submit(*args, **kwargs):
            raise RuntimeError("simulated Gemini API outage")

        monkeypatch.setattr(gemini_batch, "submit", raising_submit)

        result = submit_pending_gemini_batches(ops, duckdb, None, limit=1)

        assert result["submitted"] == 0
        assert result["batches"] == [job_id]  # batch stays discoverable

        # No stranding: every item back to pending, attempts preserved,
        # backoff scheduled (~300s — not None), error recorded
        items = _item_rows(ops, job_id)
        assert len(items) == 3
        for _, status, attempts, scheduled_for, _payload in items:
            assert status == "pending"
            assert attempts == 2
            assert scheduled_for is not None

        # Batch name still NULL — the failure never persisted a chunk
        status, name = _job_row(ops, job_id)
        assert name is None

    def test_items_recover_after_backoff_passes(self, env, monkeypatch):
        """Once scheduled_for passes, discovery + claim pick the batch up."""
        ops, duckdb, job_id = env
        _patch_media_fakes(env, monkeypatch)

        monkeypatch.setattr(
            gemini_batch, "submit",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("outage")),
        )
        submit_pending_gemini_batches(ops, duckdb, None, limit=1)

        # Simulate backoff expiry
        conn = sqlite3.connect(str(ops.database))
        conn.execute("UPDATE batch_items SET scheduled_for = NULL")
        conn.commit()
        conn.close()

        monkeypatch.setattr(gemini_batch, "submit", lambda *a, **k: ["batches/xyz"])
        result = submit_pending_gemini_batches(ops, duckdb, None, limit=1)
        assert result["submitted"] == 1
        assert _job_row(ops, job_id) == ("processing", "batches/xyz")


class TestSubmitTierGate:
    def test_free_tier_raises_before_mutation(self, tmp_path, monkeypatch):
        """FREE tier raises RuntimeError before touching queue or API."""
        monkeypatch.delenv("GEMINI_TIER", raising=False)
        ops = SQLiteResource(database=str(tmp_path / "ops.sqlite"))
        job_id = create_batch(
            ops, [json.dumps({"post_id": "p", "domain": "instagram"})],
            mode="gemini-batch",
        )

        monkeypatch.setattr(
            gemini_batch, "submit",
            lambda *a, **k: pytest.fail("submit must not be called on FREE tier"),
        )

        with pytest.raises(RuntimeError, match="Tier 1"):
            submit_pending_gemini_batches(ops, None, None, limit=1)

        items = _item_rows(ops, job_id)
        assert [it[1] for it in items] == ["pending"]
        assert [it[2] for it in items] == [0]


class TestSubmitOp:
    def test_seam_tagged_op_drives_core(self, env, monkeypatch):
        """The ADR-0008 seam-tagged op drives the same core end-to-end."""
        ops, duckdb, job_id = env
        _patch_media_fakes(env, monkeypatch)
        monkeypatch.setattr(gemini_batch, "submit", lambda *a, **k: ["batches/xyz"])

        context = build_op_context(
            resources={"ops": ops, "duckdb": duckdb, "gemini": None}
        )
        result = submit_gemini_batches_op(context, ops, duckdb, None)

        assert result["submitted"] == 1
        assert result["batches"] == [job_id]
        assert _job_row(ops, job_id) == ("processing", "batches/xyz")
