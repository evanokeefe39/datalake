"""Tests for the ADR-0001 enrichment execution upgrades:

- prompt/version registry (schema 3)
- gemini-batch module verbs: chunking, tier gate, custom_key retrieval (1)
- whole-corpus admission flag (2)
- version columns / model write (4)
- mode-tagged batches / worker mode selection
"""

from __future__ import annotations

import pytest

from datalake.defs.common.resources import DuckDBResource, SQLiteResource
from datalake.defs.enrichment import gemini_batch
from datalake.defs.enrichment.batch import _ensure_schema
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
            def __init__(self, v):
                self.state = FakeState(v)

        # The proto enum strings carry the JOB_STATE_ prefix; job_state must
        # strip it so terminal checks match _TERMINAL_OK ({"SUCCEEDED"}) etc.
        assert gemini_batch.job_state(FakeJob("JOB_STATE_SUCCEEDED")) == "SUCCEEDED"
        assert gemini_batch.is_terminal("SUCCEEDED") is True
        assert gemini_batch.is_terminal(
            gemini_batch.job_state(FakeJob("JOB_STATE_RUNNING"))
        ) is False
        assert gemini_batch.job_state(FakeJob("JOB_STATE_FAILED")) == "FAILED"
        assert gemini_batch.is_terminal("FAILED") is True

    def test_retrieve_returns_inline_results_on_terminal(self, monkeypatch):
        monkeypatch.setenv("GEMINI_TIER", "tier1")
        from datalake.defs.common.resources import GeminiResource

        class FakeState:
            def __init__(self, v):
                self.value = v

        class FakeResp:
            metadata = {"custom_key": "k1"}
            error = None
            response = type("R", (), {"text": '{"domain": "Business"}'})()

        class FakeDest:
            inlined_responses = [FakeResp()]
            file_name = None

        class FakeJob:
            state = FakeState("JOB_STATE_SUCCEEDED")
            dest = FakeDest()

        monkeypatch.setattr(gemini_batch, "poll", lambda g, n: FakeJob())
        gemini = GeminiResource(api_key="fake")
        out = gemini_batch.retrieve(gemini, "batches/x")
        assert out == {"k1": {"ok": True, "text": '{"domain": "Business"}', "error": None}}

    def test_retrieve_raises_on_non_terminal(self, monkeypatch):
        monkeypatch.setenv("GEMINI_TIER", "tier1")
        from datalake.defs.common.resources import GeminiResource

        class FakeJob:
            state = None

        monkeypatch.setattr(gemini_batch, "poll", lambda g, n: FakeJob())
        gemini = GeminiResource(api_key="fake")
        with pytest.raises(RuntimeError, match="not complete"):
            gemini_batch.retrieve(gemini, "batches/x")


class TestBatchMultimodal:
    """Media wiring in the gemini-batch path: token accounting + file Parts."""

    def test_media_input_tokens_counts_images_and_video(self):
        from datalake.defs.enrichment.gemini_batch import _media_input_tokens

        assert _media_input_tokens(None) == 0
        assert _media_input_tokens([]) == 0
        # Two images @ 258 each
        assert _media_input_tokens(
            [{"mime_type": "image/jpeg"}, {"mime_type": "image/png"}]
        ) == 516
        # A 10s video at low-res ~98 tok/s
        assert _media_input_tokens(
            [{"mime_type": "video/mp4", "duration_seconds": 10}]
        ) == 980
        # Unknown video duration defaults to 60s
        assert _media_input_tokens([{"mime_type": "video/mp4"}]) == 60 * 98

    def test_request_estimate_tokens_includes_media(self):
        from datalake.defs.enrichment.gemini_batch import request_estimate_tokens

        text_only = request_estimate_tokens({"prompt": "a" * 40})
        with_media = request_estimate_tokens(
            {"prompt": "a" * 40, "media_files": [{"mime_type": "image/jpeg"}]}
        )
        # text (~10) + one image (258)
        assert with_media == text_only + 258

    def test_chunk_requests_accounts_for_media_tokens(self):
        # A single heavy video request should not share a chunk with text that
        # together would exceed the cap.
        img = {"prompt": "p", "media_files": [{"mime_type": "image/jpeg"}]}  # ~258
        big_vid = {
            "prompt": "p",
            "media_files": [{"mime_type": "video/mp4", "duration_seconds": 20}],
        }  # ~1960
        chunks = gemini_batch.chunk_requests([img, big_vid, img], max_tokens=1500)
        # img(258)+big_vid(1960) > 1500 -> split; img+big_vid separate chunks
        assert len(chunks) == 3

    def test_to_inlined_request_text_only_unchanged(self):
        req = gemini_batch._to_inlined_request(
            {"custom_key": "1", "prompt": "hello", "media_files": None}
        )
        assert req.contents == "hello"
        assert req.config.media_resolution is None

    def test_to_inlined_request_builds_file_parts_when_media(self):
        media = [
            {"uri": "https://g/v1beta/files/i1", "mime_type": "image/jpeg"},
            {"uri": "https://g/v1beta/files/v1", "mime_type": "video/mp4"},
        ]
        req = gemini_batch._to_inlined_request(
            {"custom_key": "7", "prompt": "describe", "media_files": media}
        )
        # contents is a list of Parts; first two are file URIs, last is text.
        assert isinstance(req.contents, list)
        assert len(req.contents) == 3
        assert req.contents[0].file_data.file_uri == media[0]["uri"]
        assert req.contents[1].file_data.file_uri == media[1]["uri"]
        assert req.contents[2].text == "describe"
        # Low-res video resolution is applied on the batch config.
        assert req.config.media_resolution == "MEDIA_RESOLUTION_LOW"
