"""Unit tests for the qwen-batch growth-facets path (facets_batch).

- request builder: cached local image paths for visual, images=[] for text,
  deterministic skip of visual targets with no resolvable media
- media-aware token/cost estimation at qwen list prices
- custom_key (post_id) → response harvest mapping
- parse + validate (visual + text) incl. rejection cases
- harvest lands responses VERBATIM in bronze and counts invalid ones
  loudly — nothing is written to DuckDB (the ``gold_growth_facets`` write
  path was retired W9; conform publishes the typed silver tables)
- driver --plan offline mode (no spend, no prod writes)

Job state comes from a seam-shaped fake adapter (ADR-0013: NO ledger — the
service owns its job store); nothing here touches a network or ops.sqlite.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from datalake.defs.common.resources import SQLiteResource  # noqa: E402
from datalake.defs.common.schemas import duckdb_ddl  # noqa: E402
from datalake.defs.enrichment import facets_batch  # noqa: E402
from datalake.defs.enrichment.growth_facets_schema import (  # noqa: E402
    GROWTH_FACETS_SCHEMA_VERSION,
)
from datalake.defs.enrichment.facets import (  # noqa: E402
    parse_text_response,
    parse_universal_response,
)
from datalake.defs.enrichment.prompts import (  # noqa: E402
    _DEFAULT_QWEN_MODEL,
    CURRENT_TEXT_FACETS_PROMPT_HASH,
    build_text_facets_prompt,
)
from datalake.defs.enrichment.qwen_client import QwenServiceError  # noqa: E402
from datalake.defs.enrichment.seam import Result  # noqa: E402

# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture()
def state_conn(tmp_path):
    """Temp duckdb with silver_ig_posts (NEVER prod; no gold table)."""
    con = duckdb.connect(str(tmp_path / "test_state.duckdb"))
    con.execute(duckdb_ddl("silver_ig_posts"))
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
    """A throwaway ops.sqlite for media-cache lookups (never data/ops.sqlite)."""
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


class _FakeAdapter:
    """Seam-shaped double for the service-backed adapter (NO network).

    Job docs/results are dicts keyed by job id — the service's own job
    store, exactly what ADR-0013 says we poll instead of a ledger.
    """
    name = "service_backed"
    model = _DEFAULT_QWEN_MODEL

    def __init__(self):
        self.docs: dict[str, dict] = {}
        self.results: dict[str, list[Result]] = {}
        self.submitted: list = []

    def submit(self, items, *, job_spec=None):
        self.submitted.append(list(items))
        return f"svc-{len(self.submitted)}"

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


def _fake_service(monkeypatch, adapter) -> None:
    monkeypatch.setattr(
        facets_batch, "_service_adapter", lambda base_url, model: adapter
    )


def _seed_silver_visual(con, post_id, model=_DEFAULT_QWEN_MODEL,
                        schema_version=GROWTH_FACETS_SCHEMA_VERSION):
    """Minimal silver_visual_annotations row — exactly the columns
    _done_post_ids reads. Simulates the conform step out of band."""
    con.execute(
        "CREATE TABLE IF NOT EXISTS silver_visual_annotations "
        "(post_id VARCHAR, platform VARCHAR, model VARCHAR, "
        "schema_version VARCHAR)"
    )
    con.execute(
        "INSERT INTO silver_visual_annotations VALUES (?, 'instagram', ?, ?)",
        [post_id, model, schema_version],
    )


def _seed_silver_text(con, post_id, model=_DEFAULT_QWEN_MODEL):
    """Minimal silver_text_annotations row (same contract as visual)."""
    con.execute(
        "CREATE TABLE IF NOT EXISTS silver_text_annotations "
        "(post_id VARCHAR, platform VARCHAR, model VARCHAR, "
        "schema_version VARCHAR)"
    )
    con.execute(
        "INSERT INTO silver_text_annotations VALUES (?, 'instagram', ?, ?)",
        [post_id, model, GROWTH_FACETS_SCHEMA_VERSION],
    )


def _result(custom_key, ok=True, output=None, error=None,
            model=_DEFAULT_QWEN_MODEL) -> Result:
    return Result(
        custom_key=custom_key, ok=ok, response_text=output, error=error,
        model=model, provider="qwen",
    )


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
        img_req = {"custom_key": "i", "prompt": "x" * 4000,
                   "images": ["/tmp/a.jpg"]}
        vid_req = {"custom_key": "v", "prompt": "x" * 4000,
                   "images": ["/tmp/a.mp4"]}
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

    def test_submit_posts_one_job_via_seam(self, monkeypatch):
        adapter = _FakeAdapter()
        _fake_service(monkeypatch, adapter)
        items = [{"custom_key": "p1", "prompt": "p", "images": []}]
        job_id = facets_batch.submit_facets_batch(items, "text")
        assert job_id == "svc-1"
        assert len(adapter.submitted) == 1
        assert [it.custom_key for it in adapter.submitted[0]] == ["p1"]
        # ADR-0013: NO ledger row — the returned job id IS the in-flight
        # record; the service owns the job store.

    def test_submit_empty_items_raises(self):
        with pytest.raises(ValueError):
            facets_batch.submit_facets_batch([], "text")

    def test_submit_unknown_mode_raises(self):
        with pytest.raises(ValueError, match="unknown mode"):
            facets_batch.submit_facets_batch(
                [{"custom_key": "p1", "prompt": "p", "images": []}], "nope"
            )

    def test_wait_polls_to_terminal(self, monkeypatch):
        adapter = _FakeAdapter()
        adapter.docs["job1"] = {"state": "completed"}
        _fake_service(monkeypatch, adapter)
        out = facets_batch.wait_for_facets_batches(
            ["job1"], poll_seconds=0, timeout_seconds=5
        )
        assert out == {"job1": "completed"}

    def test_wait_raises_on_timeout(self, monkeypatch):
        adapter = _FakeAdapter()
        adapter.docs["job1"] = {"state": "processing"}
        _fake_service(monkeypatch, adapter)
        with pytest.raises(RuntimeError, match="Timed out"):
            facets_batch.wait_for_facets_batches(
                ["job1"], poll_seconds=0, timeout_seconds=0
            )

class TestHarvestAndLand:
    """Harvest lands VERBATIM in bronze and counts validation outcomes;
    nothing is written to DuckDB (W9: the gold write path is retired and
    conform publishes the typed silver tables from bronze)."""

    def test_harvest_maps_custom_key_and_lands(self, state_conn, tmp_path,
                                               monkeypatch):
        adapter = _FakeAdapter()
        adapter.docs["job1"] = {"state": "completed"}
        adapter.results["job1"] = [
            _result("p_img", output=json.dumps(_visual_payload())),
        ]
        _fake_service(monkeypatch, adapter)
        root = str(tmp_path / "bronze")
        out = facets_batch.harvest_facets_batches(
            state_conn, ["job1"], "visual", root=root
        )
        assert out["written"] == 1 and out["landed"] == 1 and out["invalid"] == 0
        from datalake.defs.enrichment.landing import (
            WORKLOAD_GROWTH_FACETS_VISUAL,
            read_responses,
        )

        df = read_responses(root)
        assert df.height == 1
        assert df["workload"][0] == WORKLOAD_GROWTH_FACETS_VISUAL
        assert df["run_id"][0] == "job1"
        assert df["response_text"][0] == json.dumps(_visual_payload())
        assert df["model"][0] == _DEFAULT_QWEN_MODEL

    def test_harvest_invalid_response_counted_not_gated(self, state_conn,
                                                        tmp_path,
                                                        monkeypatch):
        adapter = _FakeAdapter()
        adapter.docs["job1"] = {"state": "completed"}
        adapter.results["job1"] = [_result("p_img", output="not json")]
        _fake_service(monkeypatch, adapter)
        root = str(tmp_path / "bronze")
        out = facets_batch.harvest_facets_batches(
            state_conn, ["job1"], "visual", root=root
        )
        assert out["written"] == 0 and out["invalid"] == 1
        # the verbatim body is IN bronze — parsing never gates landing
        from datalake.defs.enrichment.landing import read_responses

        assert read_responses(root)["response_text"].to_list() == ["not json"]

    def test_text_harvest_lands_text_pass(self, state_conn, tmp_path,
                                          monkeypatch):
        adapter = _FakeAdapter()
        adapter.docs["job2"] = {"state": "completed"}
        adapter.results["job2"] = [
            _result("p_caption", output=json.dumps(_text_payload())),
        ]
        _fake_service(monkeypatch, adapter)
        root = str(tmp_path / "bronze")
        out = facets_batch.harvest_facets_batches(
            state_conn, ["job2"], "text", root=root
        )
        assert out["written"] == 1 and out["invalid"] == 0
        from datalake.defs.enrichment.landing import (
            WORKLOAD_GROWTH_FACETS_TEXT,
            read_responses,
        )

        df = read_responses(root)
        assert df["workload"][0] == WORKLOAD_GROWTH_FACETS_TEXT
        assert df["response_text"][0] == json.dumps(_text_payload())

    def test_failed_job_lands_nothing(self, state_conn, tmp_path, monkeypatch):
        adapter = _FakeAdapter()
        adapter.docs["job3"] = {"state": "failed"}
        _fake_service(monkeypatch, adapter)
        root = str(tmp_path / "bronze")
        out = facets_batch.harvest_facets_batches(
            state_conn, ["job3"], "visual", root=root
        )
        assert out["written"] == 0 and out["failed_jobs"] == 1
        from datalake.defs.enrichment.landing import read_responses

        assert read_responses(root).height == 0

    def test_failed_item_surfaced_not_written(self, state_conn, tmp_path,
                                              monkeypatch):
        adapter = _FakeAdapter()
        adapter.docs["job1"] = {"state": "completed"}
        adapter.results["job1"] = [
            _result("p_img", ok=False, output=None, error="unreadable file"),
        ]
        _fake_service(monkeypatch, adapter)
        root = str(tmp_path / "bronze")
        out = facets_batch.harvest_facets_batches(
            state_conn, ["job1"], "visual", root=root
        )
        assert out["written"] == 0 and out["failed_items"] == 1
        # failure is READ from a bronze column, never inferred from a
        # missing conformed row (ADR-0013)
        from datalake.defs.enrichment.landing import read_responses

        df = read_responses(root)
        assert df["ok"].to_list() == [False]
        assert df["error_message"][0] == "unreadable file"

    def test_non_terminal_job_skipped(self, state_conn, tmp_path, monkeypatch):
        adapter = _FakeAdapter()
        adapter.docs["job4"] = {"state": "processing"}
        _fake_service(monkeypatch, adapter)
        out = facets_batch.harvest_facets_batches(
            state_conn, ["job4"], "visual", root=str(tmp_path / "bronze")
        )
        assert out["written"] == 0 and out["skipped"] == 1



class TestVisualDoneDetection:
    """Done-ness = a CONFORMED silver row under the current engine + schema
    (ADR-0012 D4). Each pass owns its own typed table, so the old
    prompt_hash-overwrite hazard is structurally dissolved."""

    def test_conformed_visual_row_not_reenqueued(self, state_conn):
        # Both passes conformed: each pass sees its own table.
        _seed_silver_visual(state_conn, "p_img")
        _seed_silver_text(state_conn, "p_img")
        targets = facets_batch.enumerate_targets(state_conn, "visual")
        assert "p_img" not in {t["post_id"] for t in targets}

    def test_text_only_row_still_targeted_by_visual(self, state_conn):
        _seed_silver_text(state_conn, "p_img")
        targets = facets_batch.enumerate_targets(state_conn, "visual")
        assert "p_img" in {t["post_id"] for t in targets}

    def test_superseded_engine_row_is_reenqueued(self, state_conn):
        # gemini-era silver row must NOT count as done under qwen — it is
        # re-enqueued so the corpus re-enriches under the current engine.
        _seed_silver_visual(state_conn, "p_video",
                            model="gemini-3.5-flash-lite")
        targets = facets_batch.enumerate_targets(state_conn, "visual")
        assert "p_video" in {t["post_id"] for t in targets}

    def test_visual_table_absent_does_not_block_text(self, state_conn):
        # Regression (2026-09-15): the table-existence guard was hardcoded to
        # silver_visual_annotations for BOTH modes, so a text-only state DB
        # re-billed every text post on each run. Each mode guards its own table.
        _seed_silver_text(state_conn, "p_caption")
        assert "p_caption" not in {
            t["post_id"]
            for t in facets_batch.enumerate_targets(state_conn, "text")
        }
        # ...and the missing visual table never masks visual work
        assert "p_img" in {
            t["post_id"]
            for t in facets_batch.enumerate_targets(state_conn, "visual")
        }

    def test_superseded_schema_version_reenqueued(self, state_conn):
        _seed_silver_visual(state_conn, "p_img", schema_version="2")
        targets = facets_batch.enumerate_targets(state_conn, "visual")
        assert "p_img" in {t["post_id"] for t in targets}


class TestHarvestGuards:
    def test_empty_job_ids_raises(self, state_conn, tmp_path):
        """No ledger to consult (ADR-0013) — the caller MUST supply ids."""
        with pytest.raises(ValueError, match="no job ids"):
            facets_batch.harvest_facets_batches(
                state_conn, [], "visual", root=str(tmp_path / "bronze")
            )

    def test_unknown_job_id_raises_loudly(self, state_conn, tmp_path,
                                          monkeypatch):
        """An unknown job id surfaces the service's error — never a quiet
        skip, never a ledger lookup."""
        adapter = _FakeAdapter()  # no docs at all
        _fake_service(monkeypatch, adapter)
        with pytest.raises(QwenServiceError):
            facets_batch.harvest_facets_batches(
                state_conn, ["never-submitted"], "visual",
                root=str(tmp_path / "bronze"),
            )

    def test_model_param_lands_in_bronze(self, state_conn, tmp_path,
                                         monkeypatch):
        """Model provenance is stored on the VERBATIM bronze row."""
        adapter = _FakeAdapter()
        adapter.docs["jobM"] = {"state": "completed"}
        adapter.results["jobM"] = [
            _result("p_caption", output=json.dumps(_text_payload()),
                    model="m-submit-time"),
        ]
        _fake_service(monkeypatch, adapter)
        root = str(tmp_path / "bronze")
        out = facets_batch.harvest_facets_batches(
            state_conn, ["jobM"], "text", root=root
        )
        assert out["written"] == 1
        from datalake.defs.enrichment.landing import read_responses

        assert read_responses(root)["model"][0] == "m-submit-time"


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


class TestResumeAfterMidRunFailure:
    """US-EENG-1 AC7: a second discovery→submit→harvest cycle resubmits
    ONLY posts lacking a current CONFORMED silver row (ADR-0012 D4);
    failed posts are retried — the credits-outage resume guarantee.

    Scenario: p_img succeeds in cycle 1; p_video's item fails server-side
    in cycle 1 (ok=False). Cycle 2 must skip p_img once it is conformed
    and retry p_video.
    """

    def test_rerun_skips_gold_and_retries_failed(self, state_conn, tmp_path,
                                                 cached_media, monkeypatch):
        ops = _sqlite(tmp_path)
        adapter = _FakeAdapter()
        _fake_service(monkeypatch, adapter)
        root = str(tmp_path / "bronze")

        def run_cycle(results_payload):
            targets = facets_batch.enumerate_targets(state_conn, "visual")
            items = facets_batch.build_facets_batch_requests(
                ops, state_conn, targets, "visual"
            )
            if not items:
                return None
            job_id = facets_batch.submit_facets_batch(items, "visual")
            adapter.docs[job_id] = {"state": "completed"}
            adapter.results[job_id] = [
                _result(r["custom_key"], ok=r["ok"], output=r["output"],
                        error=r["error"])
                for r in results_payload
            ]
            return facets_batch.harvest_facets_batches(
                state_conn, [job_id], "visual", root=root
            )

        # ── Cycle 1 ──
        cycle1_results = [
            {"custom_key": "p_img", "ok": True,
             "output": json.dumps(_visual_payload()), "error": None},
            {"custom_key": "p_video", "ok": False, "output": None,
             "error": "out of credits"},
        ]
        out1 = run_cycle(cycle1_results)
        assert out1["written"] == 1 and out1["failed_items"] == 1
        assert [it.custom_key for it in adapter.submitted[-1]] == [
            "p_img", "p_video"
        ]
        # nothing was written to DuckDB — bronze holds both responses verbatim
        from datalake.defs.enrichment.landing import read_responses

        df1 = read_responses(root)
        assert sorted(df1["post_id"].to_list()) == ["p_img", "p_video"]
        assert df1["ok"].to_list() == [True, False]

        # Conform ran out of band: p_img's payload is CONFORMED into silver —
        # ADR-0012 D4 makes that the completion signal the guard reads.
        _seed_silver_visual(state_conn, "p_img")
        # (a) cycle-2 discovery excludes the conformed post, includes the failed one
        targets2 = facets_batch.enumerate_targets(state_conn, "visual")
        assert "p_img" not in {t["post_id"] for t in targets2}
        assert "p_video" in {t["post_id"] for t in targets2}

        # Cycle 2: both items now succeed; the resubmitted job is harvested
        # from its service-owned id (no ledger involved).
        cycle2_results = [
            dict(r, ok=True, output=json.dumps(_visual_payload()),
                 error=None)
            for r in cycle1_results
        ]
        out2 = run_cycle(cycle2_results)
        # ONLY the failed post retried — p_img is done (conformed)
        assert [it.custom_key for it in adapter.submitted[-1]] == ["p_video"]
        # the double replays BOTH results for the single re-submitted job, so
        # both land — the load-bearing claim is the submit list above
        assert out2["written"] == 2

        # (b) bronze is append-only: p_img's cycle-2 response landed again
        # under the NEW job id (different run_id — no silent dedup), while
        # discovery/submit-level idempotency (the load-bearing claim above)
        df2 = read_responses(root)
        p_video_rows = [r for r in df2["post_id"].to_list() if r == "p_video"]
        assert p_video_rows == ["p_video", "p_video"]
        assert df2["post_id"].to_list().count("p_img") == 2
