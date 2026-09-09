"""Unit tests for the qwen-batch growth-facets path (facets_batch).

Covers:
- request builder: cached local image paths for visual, images=[] for text,
  deterministic skip of visual targets with no resolvable media
- media-aware token/cost estimation at qwen list prices
- custom_key (post_id) → response harvest mapping
- parse + validate (visual + text) incl. rejection cases
- write/merge semantics into gold_growth_facets on a TEMP duckdb
  (never data/state.duckdb)
- ledger resume state (job_id keyed, SUBMITTED → RETRIEVED/JOB_FAILED)
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
from datalake.defs.common.resources import SQLiteResource  # noqa: E402
from datalake.defs.enrichment import facets_batch  # noqa: E402
from datalake.defs.enrichment.facets import (  # noqa: E402
    parse_text_response,
    parse_universal_response,
    write_gold_facets_pass_conn,
)
from datalake.defs.enrichment.prompts import (  # noqa: E402
    CURRENT_TEXT_FACETS_PROMPT_HASH,
    _DEFAULT_QWEN_MODEL,
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
    return SQLiteResource(database=str(tmp_path / "test_ops.sqlite"))


@pytest.fixture()
def cached_media(tmp_path):
    """Patch media_cache.cached_local_path to return real temp files."""
    files = {}
    for url in ("https://cdn/a.mp4", "https://cdn/b.jpg"):
        p = tmp_path / f"cached_{url.split('/')[-1]}"
        p.write_bytes(b"\x00")
        files[url] = str(p)

    def _fake(ops, url):
        return files.get(url)

    with patch(
        "datalake.defs.enrichment.media_paths.cached_local_path",
        side_effect=_fake,
    ):
        yield files

# ── Request builder ─────────────────────────────────────────────────────────


class TestBuildRequests:
    def test_visual_requests_carry_cached_paths(self, state_conn, tmp_path,
                                                cached_media):
        ops = _sqlite(tmp_path)
        targets = facets_batch.enumerate_targets(state_conn, "visual")
        assert {t["post_id"] for t in targets} == {"p_video", "p_img"}
        reqs = facets_batch.build_facets_batch_requests(
            ops, state_conn, targets, "visual"
        )
        assert len(reqs) == 2
        for r in reqs:
            assert r["custom_key"] in {"p_video", "p_img"}
            assert len(r["images"]) == 1
            assert Path(r["images"][0]).exists()
            assert "visual_facets" in r["prompt"]

    def test_text_requests_have_no_media(self, state_conn, tmp_path):
        ops = _sqlite(tmp_path)
        targets = facets_batch.enumerate_targets(state_conn, "text")
        assert {t["post_id"] for t in targets} == {
            "p_video", "p_img", "p_caption",
        }
        reqs = facets_batch.build_facets_batch_requests(
            ops, state_conn, targets, "text"
        )
        assert len(reqs) == 3
        assert all(r["images"] == [] for r in reqs)
        assert all("hook_type" in r["prompt"] for r in reqs)

    def test_visual_skips_unresolvable_media(self, state_conn, tmp_path,
                                             caplog):
        """No cached bytes → deterministic skip with a logged reason."""
        ops = _sqlite(tmp_path)
        targets = facets_batch.enumerate_targets(state_conn, "visual")
        with caplog.at_level("WARNING", logger="enrichment.facets_batch"):
            reqs = facets_batch.build_facets_batch_requests(
                ops, state_conn, targets, "visual"
            )
        assert reqs == []  # both URLs uncached in this fixture
        assert "no cached media paths" in caplog.text


# ── Token/cost estimation ───────────────────────────────────────────────────


class TestEstimation:
    def test_media_tokens_included_at_qwen_prices(self):
        text_req = {"custom_key": "t", "prompt": "x" * 4000, "images": []}
        img_req = {
            "custom_key": "i",
            "prompt": "x" * 4000,
            "images": ["/tmp/a.jpg"],
        }
        vid_req = {
            "custom_key": "v",
            "prompt": "x" * 4000,
            "images": ["/tmp/b.mp4"],
        }
        in_t, cost = facets_batch.estimate_facets_cost(
            [text_req, img_req, vid_req]
        )
        # video counts as its 8-frame server-side sample
        assert in_t > 3 * (4000 // 4) + facets_batch._TOKENS_PER_IMAGE
        assert cost > 0
        # qwen list prices: cost == tokens/1M * price exactly
        expected = (
            in_t / 1_000_000 * facets_batch.QWEN_INPUT_PRICE_PER_M
            + (1024 + 2 * facets_batch._EST_OUTPUT_TOKENS_VISUAL)
            / 1_000_000
            * facets_batch.QWEN_OUTPUT_PRICE_PER_M
        )
        assert cost == pytest.approx(expected)


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


# ── Submit / wait / harvest on the qwen service ─────────────────────────────


def _completed_job():
    return {"job_id": "job1", "state": "completed", "total": 1,
            "completed": 1, "failed": 0, "error": None}


def _failed_job():
    return {"job_id": "job1", "state": "failed", "total": 1,
            "completed": 0, "failed": 1, "error": "boom"}


class TestSubmitWait:
    def test_submit_health_checks_then_posts_one_job(self, tmp_path):
        ops = _sqlite(tmp_path)
        items = [{"custom_key": "p1", "prompt": "p", "images": []}]
        with patch.object(
            facets_batch.qwen_client, "check_health", return_value={}
        ) as health, patch.object(
            facets_batch.qwen_client, "submit_job", return_value="svc-1"
        ) as sub:
            job_id = facets_batch.submit_facets_batch(ops, items, "text")
        assert job_id == "svc-1"
        health.assert_called_once()
        sub.assert_called_once()
        assert sub.call_args.kwargs["model"] == _DEFAULT_QWEN_MODEL
        assert sub.call_args.args[1] == items
        # ledger row recorded SUBMITTED
        assert [p["job_id"] for p in facets_batch.pending_ledger_jobs(ops)] == [
            "svc-1"
        ]

    def test_submit_empty_items_raises(self, tmp_path):
        with pytest.raises(ValueError):
            facets_batch.submit_facets_batch(_sqlite(tmp_path), [], "text")

    def test_wait_polls_to_terminal(self):
        with patch.object(
            facets_batch.qwen_client, "get_job", return_value=_completed_job()
        ):
            out = facets_batch.wait_for_facets_batches(
                ["job1"], poll_seconds=0, timeout_seconds=5
            )
        assert out == {"job1": "completed"}

    def test_wait_raises_on_timeout(self):
        with patch.object(
            facets_batch.qwen_client, "get_job",
            return_value={"state": "processing"},
        ), pytest.raises(RuntimeError, match="Timed out"):
            facets_batch.wait_for_facets_batches(
                ["job1"], poll_seconds=0, timeout_seconds=0
            )


class TestHarvestAndMerge:
    def test_harvest_maps_custom_key_and_merges(self, state_conn, tmp_path):
        ops = _sqlite(tmp_path)
        # pre-seed a text-only row (text pass ran first)
        write_gold_facets_pass_conn(
            state_conn, "p_img", "instagram",
            {"hook_type": "question", "is_sponsored": False},
            prompt_hash=CURRENT_TEXT_FACETS_PROMPT_HASH,
        )
        facets_batch._record_jobs(
            ops, "visual", _DEFAULT_QWEN_MODEL, ["job1"], 1, 0,
            {"n_media": {"p_img": 1}},
        )
        results = [
            {"custom_key": "p_img", "ok": True,
             "output": json.dumps(_visual_payload()), "error": None},
        ]
        with patch.object(facets_batch.qwen_client, "get_job",
                          return_value=_completed_job()), \
             patch.object(facets_batch.qwen_client, "get_results",
                          return_value=results):
            out = facets_batch.harvest_facets_batches(ops, state_conn)
        assert out["written"] == 1
        row = state_conn.execute(
            "SELECT growth_facets_json, content_summary, model "
            "FROM gold_growth_facets WHERE post_id = 'p_img'"
        ).fetchone()
        stored = json.loads(row[0])
        # UNION: the earlier text pass's sub-fields survive the visual merge
        assert stored["hook_type"] == "question"
        assert stored["face_present"] is False
        assert row[1] == "a demo"
        assert row[2] == _DEFAULT_QWEN_MODEL

    def test_harvest_invalid_response_not_written(self, state_conn, tmp_path):
        ops = _sqlite(tmp_path)
        facets_batch._record_jobs(
            ops, "visual", _DEFAULT_QWEN_MODEL, ["job1"], 1, 0,
            {"n_media": {"p_img": 1}},
        )
        results = [
            {"custom_key": "p_img", "ok": True, "output": "not json",
             "error": None},
        ]
        with patch.object(facets_batch.qwen_client, "get_job",
                          return_value=_completed_job()), \
             patch.object(facets_batch.qwen_client, "get_results",
                          return_value=results):
            out = facets_batch.harvest_facets_batches(ops, state_conn)
        assert out["written"] == 0 and out["invalid"] == 1
        assert state_conn.execute(
            "SELECT count(*) FROM gold_growth_facets"
        ).fetchone()[0] == 0

    def test_text_harvest_writes_text_pass(self, state_conn, tmp_path):
        ops = _sqlite(tmp_path)
        facets_batch._record_jobs(
            ops, "text", _DEFAULT_QWEN_MODEL, ["job2"], 1, 0, {}
        )
        results = [
            {"custom_key": "p_caption", "ok": True,
             "output": json.dumps(_text_payload()), "error": None},
        ]
        with patch.object(facets_batch.qwen_client, "get_job",
                          return_value=_completed_job()), \
             patch.object(facets_batch.qwen_client, "get_results",
                          return_value=results):
            out = facets_batch.harvest_facets_batches(ops, state_conn)
        assert out["written"] == 1
        stored = json.loads(state_conn.execute(
            "SELECT growth_facets_json FROM gold_growth_facets "
            "WHERE post_id = 'p_caption'"
        ).fetchone()[0])
        assert stored["cta_type"] == "comment"

    def test_failed_job_flips_ledger_no_writes(self, state_conn, tmp_path):
        ops = _sqlite(tmp_path)
        facets_batch._record_jobs(
            ops, "visual", _DEFAULT_QWEN_MODEL, ["job3"], 1, 0, {}
        )
        with patch.object(facets_batch.qwen_client, "get_job",
                          return_value=_failed_job()):
            out = facets_batch.harvest_facets_batches(ops, state_conn)
        assert out["written"] == 0
        assert facets_batch.pending_ledger_jobs(ops) == []

    def test_failed_item_surfaced_not_written(self, state_conn, tmp_path):
        ops = _sqlite(tmp_path)
        facets_batch._record_jobs(
            ops, "visual", _DEFAULT_QWEN_MODEL, ["job1"], 1, 0,
            {"n_media": {"p_img": 1}},
        )
        results = [
            {"custom_key": "p_img", "ok": False, "output": None,
             "error": "unreadable file"},
        ]
        with patch.object(facets_batch.qwen_client, "get_job",
                          return_value=_completed_job()), \
             patch.object(facets_batch.qwen_client, "get_results",
                          return_value=results):
            out = facets_batch.harvest_facets_batches(ops, state_conn)
        assert out["written"] == 0 and out["failed_items"] == 1
        assert state_conn.execute(
            "SELECT count(*) FROM gold_growth_facets"
        ).fetchone()[0] == 0

    def test_non_terminal_job_skipped(self, state_conn, tmp_path):
        ops = _sqlite(tmp_path)
        facets_batch._record_jobs(
            ops, "visual", _DEFAULT_QWEN_MODEL, ["job4"], 1, 0, {}
        )
        with patch.object(
            facets_batch.qwen_client, "get_job",
            return_value={"state": "processing"},
        ):
            out = facets_batch.harvest_facets_batches(ops, state_conn)
        assert out["written"] == 0
        assert facets_batch.pending_ledger_jobs(ops)[0]["job_id"] == "job4"


# ── Ledger (resume-safe state) ──────────────────────────────────────────────


class TestLedger:
    def test_record_and_pending(self, state_conn, tmp_path):
        ops = _sqlite(tmp_path)
        facets_batch._record_jobs(
            ops, "visual", "m", ["jobA", "jobB"], 2, 100,
            {"n_media": {"p": 1}},
        )
        pending = facets_batch.pending_ledger_jobs(ops)
        assert [p["job_id"] for p in pending] == ["jobA", "jobB"]
        assert pending[0]["meta"]["n_media"] == {"p": 1}
        facets_batch._set_ledger_status(ops, "jobA", "RETRIEVED")
        assert [p["job_id"] for p in facets_batch.pending_ledger_jobs(ops)] == [
            "jobB"
        ]

    def test_gemini_column_migrated(self, tmp_path):
        """A pre-existing Gemini-era ledger migrates gemini_batch_name → job_id."""
        import sqlite3

        db = tmp_path / "legacy_ops.sqlite"
        con = sqlite3.connect(str(db))
        con.execute(
            "CREATE TABLE facets_batch_jobs (id INTEGER PRIMARY KEY, "
            "mode VARCHAR NOT NULL, model VARCHAR NOT NULL, "
            "gemini_batch_name VARCHAR NOT NULL, status VARCHAR NOT NULL, "
            "n_requests INTEGER NOT NULL, est_tokens INTEGER NOT NULL, "
            "meta_json VARCHAR NOT NULL DEFAULT '{}', "
            "created_at VARCHAR NOT NULL, updated_at VARCHAR NOT NULL)"
        )
        con.execute(
            "INSERT INTO facets_batch_jobs (mode, model, gemini_batch_name, "
            "status, n_requests, est_tokens, created_at, updated_at) "
            "VALUES ('visual', 'gemini-3.5-flash-lite', 'old-chunk', "
            "'SUBMITTED', 1, 0, 't', 't')"
        )
        con.commit()
        con.close()
        ops = SQLiteResource(database=str(db))
        pending = facets_batch.pending_ledger_jobs(ops)
        assert [p["job_id"] for p in pending] == ["old-chunk"]


class TestVisualDoneDetection:
    """Visual mode must key done-ness on facet JSON content, not prompt_hash
    (the text pass overwrites prompt_hash on the same row)."""

    def test_visual_done_row_with_text_hash_not_reenqueued(
        self, state_conn
    ):
        # Both passes landed; the text pass ran LAST so prompt_hash is the
        # TEXT hash even though all visual fields are present.
        write_gold_facets_pass_conn(
            state_conn, "p_img", "instagram",
            _visual_payload()["visual_facets"],
        )
        write_gold_facets_pass_conn(
            state_conn, "p_img", "instagram",
            {"hook_type": "question", "is_sponsored": False},
            prompt_hash=CURRENT_TEXT_FACETS_PROMPT_HASH,
        )
        targets = facets_batch.enumerate_targets(state_conn, "visual")
        assert "p_img" not in {t["post_id"] for t in targets}

    def test_text_only_row_still_targeted_by_visual(self, state_conn):
        write_gold_facets_pass_conn(
            state_conn, "p_img", "instagram",
            {"hook_type": "question", "is_sponsored": False},
            prompt_hash=CURRENT_TEXT_FACETS_PROMPT_HASH,
        )
        targets = facets_batch.enumerate_targets(state_conn, "visual")
        assert "p_img" in {t["post_id"] for t in targets}


class TestHarvestLedgerGuards:
    def test_unknown_explicit_job_id_raises(self, state_conn, tmp_path):
        ops = _sqlite(tmp_path)
        with pytest.raises(ValueError, match="not in the facets ledger"):
            facets_batch.harvest_facets_batches(
                ops, state_conn, job_ids=["never-submitted"]
            )

    def test_ledger_model_stamps_gold_rows(self, state_conn, tmp_path):
        ops = _sqlite(tmp_path)
        facets_batch._record_jobs(
            ops, "text", "m-submit-time", ["jobM"], 1, 0, {}
        )
        results = [
            {"custom_key": "p_caption", "ok": True,
             "output": json.dumps(_text_payload()), "error": None},
        ]
        with patch.object(facets_batch.qwen_client, "get_job",
                          return_value=_completed_job()), \
             patch.object(facets_batch.qwen_client, "get_results",
                          return_value=results):
            out = facets_batch.harvest_facets_batches(
                ops, state_conn, model=_DEFAULT_QWEN_MODEL
            )
        assert out["written"] == 1
        assert state_conn.execute(
            "SELECT model FROM gold_growth_facets WHERE post_id = 'p_caption'"
        ).fetchone()[0] == "m-submit-time"


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
        assert "qwen_cost_usd" in out
