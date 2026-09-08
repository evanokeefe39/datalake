"""Unit tests for the batch-native growth-facets path (facets_batch).

Covers:
- request builder: media Parts present for visual, absent for text
- media-aware token/cost estimation (batch 50% discount)
- custom_key (post_id) → response harvest mapping
- parse + validate (visual + text) incl. rejection cases
- write/merge semantics into gold_growth_facets on a TEMP duckdb
  (never data/state.duckdb)
- driver --plan offline mode (no spend, no prod writes)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datalake.defs.common.schemas import duckdb_ddl  # noqa: E402
from datalake.defs.enrichment import facets_batch  # noqa: E402
from datalake.defs.enrichment.facets import (  # noqa: E402
    parse_text_response,
    parse_universal_response,
    write_gold_facets_pass_conn,
)
from datalake.defs.enrichment.prompts import (  # noqa: E402
    CURRENT_TEXT_FACETS_PROMPT_HASH,
    build_text_facets_prompt,
)


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture()
def state_conn(tmp_path):
    """Temp duckdb with silver_ig_posts + gold_growth_facets (NEVER prod)."""
    con = duckdb.connect(str(tmp_path / "test_state.duckdb"))
    con.execute(duckdb_ddl("silver_ig_posts"))
    con.execute(duckdb_ddl("gold_growth_facets"))
    con.execute(
        "INSERT INTO silver_ig_posts (post_id, caption, media_files, "
        "source_dataset, hashtags) VALUES "
        "('p_video', 'watch this', '[\"https://cdn/a.mp4\"]', 'test', '[]'), "
        "('p_img', 'see pic', '[\"https://cdn/b.jpg\"]', 'test', '[]'), "
        "('p_caption', 'caption only post', '[]', 'test', '[]')"
    )
    yield con
    con.close()


def _sqlite(tmp_path):
    """A throwaway ops.sqlite for the ledger (never data/ops.sqlite)."""
    from datalake.defs.common.resources import SQLiteResource

    return SQLiteResource(database=str(tmp_path / "test_ops.sqlite"))


def _tier():
    return MagicMock(supports_video=True)


# ── Request builder ─────────────────────────────────────────────────────────


class TestBuildRequests:
    def test_visual_requests_carry_media(self, state_conn, tmp_path):
        ops = _sqlite(tmp_path)
        gemini = MagicMock()
        targets = facets_batch.enumerate_targets(state_conn, "visual")
        assert {t["post_id"] for t in targets} == {"p_video", "p_img"}
        with patch(
            "datalake.defs.enrichment.analysis.lookup_or_upload_all",
            return_value=[{"uri": "https://files/a", "mime_type": "video/mp4",
                           "duration_seconds": 30}],
        ), patch(
            "datalake.defs.enrichment.analysis.GeminiTierConfig.detect",
            return_value=_tier(),
        ):
            reqs = facets_batch.build_facets_batch_requests(
                ops, gemini, state_conn, targets, "visual"
            )
        assert len(reqs) == 2
        for r in reqs:
            assert r["custom_key"] == r["post_id"]
            assert r["media_files"][0]["uri"] == "https://files/a"
            assert "visual_facets" in r["prompt"]

    def test_text_requests_have_no_media(self, state_conn, tmp_path):
        ops = _sqlite(tmp_path)
        gemini = MagicMock()  # text path must NEVER touch media resolution
        targets = facets_batch.enumerate_targets(state_conn, "text")
        assert {t["post_id"] for t in targets} == {
            "p_video", "p_img", "p_caption",
        }
        reqs = facets_batch.build_facets_batch_requests(
            ops, gemini, state_conn, targets, "text"
        )
        assert len(reqs) == 3
        assert all("media_files" not in r for r in reqs)
        assert all("hook_type" in r["prompt"] for r in reqs)
        gemini.assert_not_called()

    def test_custom_key_is_post_id(self, state_conn, tmp_path):
        ops = _sqlite(tmp_path)
        gemini = MagicMock()
        with patch(
            "datalake.defs.enrichment.analysis.lookup_or_upload_all",
            return_value=[{"uri": "u", "mime_type": "image/jpeg"}],
        ), patch(
            "datalake.defs.enrichment.analysis.GeminiTierConfig.detect",
            return_value=_tier(),
        ):
            reqs = facets_batch.build_facets_batch_requests(
                ops, gemini, state_conn,
                facets_batch.enumerate_targets(state_conn, "visual"),
                "visual",
            )
        assert {r["custom_key"] for r in reqs} == {"p_video", "p_img"}


# ── Token/cost estimation ───────────────────────────────────────────────────


class TestEstimation:
    def test_media_tokens_included_and_discounted(self):
        text_req = {"custom_key": "t", "prompt": "x" * 4000}
        media_req = {
            "custom_key": "m",
            "prompt": "x" * 4000,
            "media_files": [{"uri": "u", "mime_type": "video/mp4",
                             "duration_seconds": 60}],
        }
        in_t, cost = facets_batch.estimate_facets_cost([text_req, media_req])
        # media request adds ~60s * 98 tok/s over the text one
        assert in_t > 2 * (4000 // 4)
        assert cost > 0
        # discount: cost < full list price on input + output shares
        full_price = (
            in_t / 1_000_000 * 0.10
            + (4096 + 1024) / 1_000_000 * 0.40
        )
        assert cost < full_price


# ── Parse + validate ────────────────────────────────────────────────────────


def _visual_payload():
    return {
        "visual_facets": {
            "face_present": False, "value_medium": "demo",
            "brand_logos": [], "text_overlay_present": True,
            "on_screen_claim": False,
        },
        "content_summary": "a demo",
    }


def _text_payload():
    return {
        "hook_content": "stop doing x", "hook_type": "bold_claim",
        "is_sponsored": False, "sponsorship_signal": "",
        "claimed_results": True, "cta_type": "comment",
        "audience_named": True, "value_depth": "practical",
        "replicable_tactic": "", "evidence": "opening line",
        "brand_safety": {f: False for f in (
            "profanity", "sexualized_content", "political",
            "medical_claims", "financial_guarantees", "violence_trauma",
        )},
    }


class TestParseValidate:
    def test_visual_ok(self):
        parsed = parse_universal_response(json.dumps(_visual_payload()), 1)
        assert parsed["errors"] == []
        assert parsed["visual_facets"]["value_medium"] == "demo"
        assert parsed["content_summary"] == "a demo"

    def test_visual_rejects_text_layer_bleed(self):
        payload = _visual_payload()
        payload["visual_facets"]["hook_type"] = "question"
        parsed = parse_universal_response(json.dumps(payload), 1)
        assert parsed["visual_facets"] is None
        assert parsed["errors"]

    def test_text_ok(self):
        parsed = parse_text_response(json.dumps(_text_payload()))
        assert parsed["errors"] == []
        assert parsed["text_facets"]["hook_type"] == "bold_claim"

    def test_text_rejects_visual_bleed(self):
        parsed = parse_text_response(
            json.dumps({"hook_type": "question", "face_present": True})
        )
        assert parsed["text_facets"] is None

    def test_text_prompt_enforces_split(self):
        prompt = build_text_facets_prompt("cap")
        assert "hook_type" in prompt
        assert "face_present" in prompt  # forbids visual fields explicitly
        assert CURRENT_TEXT_FACETS_PROMPT_HASH


# ── Harvest mapping + write/merge on temp duckdb ────────────────────────────


def _terminal_job():
    job = MagicMock()
    job.state.value = "JOB_STATE_SUCCEEDED"
    return job


class TestHarvestAndMerge:
    def test_harvest_maps_custom_key_and_merges(self, state_conn, tmp_path):
        ops = _sqlite(tmp_path)
        gemini = MagicMock()
        # pre-seed a text-only row (text pass ran first)
        write_gold_facets_pass_conn(
            state_conn, "p_img", "instagram",
            {"hook_type": "question", "is_sponsored": False},
            prompt_hash=CURRENT_TEXT_FACETS_PROMPT_HASH,
        )
        results = {
            "p_img": {"ok": True, "text": json.dumps(_visual_payload()),
                      "error": None},
        }
        with patch.object(facets_batch.gemini_batch, "poll",
                          return_value=_terminal_job()), \
             patch.object(facets_batch.gemini_batch, "retrieve",
                          return_value=results):
            out = facets_batch.harvest_facets_batches(
                ops, gemini, state_conn, names=["job1"]
            )
        assert out["written"] == 1
        row = state_conn.execute(
            "SELECT growth_facets_json, content_summary "
            "FROM gold_growth_facets WHERE post_id = 'p_img'"
        ).fetchone()
        stored = json.loads(row[0])
        # UNION: the earlier text pass's sub-fields survive the visual merge
        assert stored["hook_type"] == "question"
        assert stored["face_present"] is False
        assert row[1] == "a demo"

    def test_harvest_invalid_response_not_written(self, state_conn, tmp_path):
        ops = _sqlite(tmp_path)
        gemini = MagicMock()
        results = {"p_img": {"ok": True, "text": "not json", "error": None}}
        with patch.object(facets_batch.gemini_batch, "poll",
                          return_value=_terminal_job()), \
             patch.object(facets_batch.gemini_batch, "retrieve",
                          return_value=results):
            out = facets_batch.harvest_facets_batches(
                ops, gemini, state_conn, names=["job1"]
            )
        assert out["written"] == 0 and out["invalid"] == 1
        assert state_conn.execute(
            "SELECT count(*) FROM gold_growth_facets"
        ).fetchone()[0] == 0

    def test_text_harvest_writes_text_pass(self, state_conn, tmp_path):
        ops = _sqlite(tmp_path)
        gemini = MagicMock()
        facets_batch._record_jobs(
            ops, "text", "gemini-3.5-flash-lite", ["job2"], 1, 0, {}
        )
        results = {
            "p_caption": {"ok": True, "text": json.dumps(_text_payload()),
                          "error": None},
        }
        with patch.object(facets_batch.gemini_batch, "poll",
                          return_value=_terminal_job()), \
             patch.object(facets_batch.gemini_batch, "retrieve",
                          return_value=results):
            out = facets_batch.harvest_facets_batches(
                ops, gemini, state_conn
            )
        assert out["written"] == 1
        stored = json.loads(state_conn.execute(
            "SELECT growth_facets_json FROM gold_growth_facets "
            "WHERE post_id = 'p_caption'"
        ).fetchone()[0])
        assert stored["cta_type"] == "comment"


# ── Ledger (resume-safe state) ──────────────────────────────────────────────


class TestLedger:
    def test_record_and_pending(self, state_conn, tmp_path):
        ops = _sqlite(tmp_path)
        facets_batch._record_jobs(
            ops, "visual", "m", ["jobA", "jobB"], 2, 100,
            {"n_media": {"p": 1}},
        )
        pending = facets_batch.pending_ledger_jobs(ops)
        assert [p["name"] for p in pending] == ["jobA", "jobB"]
        assert pending[0]["meta"]["n_media"] == {"p": 1}
        facets_batch._set_ledger_status(ops, "jobA", "RETRIEVED")
        assert [p["name"] for p in facets_batch.pending_ledger_jobs(ops)] == [
            "jobB"
        ]


# ── Driver plan mode (offline) ──────────────────────────────────────────────


class TestDriverPlan:
    def test_plan_offline_counts_and_cost(self, tmp_path, capsys):
        state = tmp_path / "plan_state.duckdb"
        con = duckdb.connect(str(state))
        con.execute(duckdb_ddl("silver_ig_posts"))
        con.execute(
            "INSERT INTO silver_ig_posts (post_id, caption, media_files, "
            "source_dataset, hashtags) VALUES "
            "('p1', 'cap one', '[\"https://cdn/1.mp4\"]', 'test', '[]'), "
            "('p2', 'cap two', '[]', 'test', '[]')"
        )
        con.close()
        sys.path.insert(0, str(REPO_ROOT / "scripts"))
        import enrich_facets_batch as drv

        rc = drv.main(["--plan", "--state-db", str(state)])
        out = capsys.readouterr().out
        assert rc == 0
        assert "[visual]" in out and "[text]" in out
        assert "targets=1" in out  # p1 only for visual (media-bearing)
        assert "targets=2" in out  # both captions for text
        assert "50% discount" in out
