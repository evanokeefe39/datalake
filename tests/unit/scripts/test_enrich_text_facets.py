"""Tests for the text-layer facets driver (scripts/enrich_text_facets.py).

Pins the observable contract without any Gemini call or spend:

- enumeration = caption-bearing silver posts (media-free pass; empty captions
  skipped); --post-ids/--owners subset-targetable;
- --plan is offline and projects counts + cost with no network;
- resume: posts already materialized under the current TEXT prompt hash are
  skipped;
- merge semantics: a visual-only row + a text write assembles BOTH passes'
  fields without clobbering the visual fields or the per-pass summary columns,
  and the assembled union satisfies validate_growth_facets;
- the text upsert is idempotent on (post_id, domain) with the ordering guard.

Databases are temp files per repo conventions — data/state.duckdb is never
opened.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import duckdb
import pytest

from datalake.defs.common.schemas import DUCKDB_TABLES, duckdb_ddl, sqlite_ddl
from datalake.defs.enrichment.growth_facets_schema import (
    BRAND_SAFETY_FLAGS,
    validate_growth_facets,
)
from datalake.defs.enrichment.prompts import CURRENT_TEXT_FACETS_PROMPT_HASH
from datalake.defs.enrichment.prompts import CURRENT_FACETS_PROMPT_HASH

SCRIPTS = Path(__file__).resolve().parents[3] / "scripts"


@pytest.fixture
def driver():
    spec = importlib.util.spec_from_file_location(
        "enrich_text_facets", str(SCRIPTS / "enrich_text_facets.py")
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["enrich_text_facets"] = mod
    spec.loader.exec_module(mod)
    return mod


def _seed_state(path: Path, posts: list[tuple]) -> str:
    con = duckdb.connect(str(path))
    con.execute(duckdb_ddl("silver_ig_posts"))
    for row in posts:
        con.execute(
            "INSERT INTO silver_ig_posts "
            "(post_id, caption, media_files, media_count, owner_username, "
            " source_dataset, hashtags) VALUES (?, ?, ?, ?, ?, 'test', '[]')",
            list(row),
        )
    con.close()
    return str(path)


def _seed_ops(path: Path) -> str:
    conn = sqlite3.connect(str(path))
    conn.executescript(sqlite_ddl("dead_letter"))
    conn.commit()
    conn.close()
    return str(path)


VISUAL_FACETS = {
    "face_present": False,
    "value_medium": "screenshare",
    "brand_logos": [],
    "text_overlay_present": True,
    "on_screen_claim": False,
}

TEXT_FACETS = {
    "hook_content": "Stop doing this",
    "hook_type": "pattern_interrupt",
    "is_sponsored": False,
    "sponsorship_signal": "",
    "claimed_results": False,
    "cta_type": "save",
    "audience_named": True,
    "value_depth": "practical",
    "replicable_tactic": "screen-record the workflow",
    "evidence": "caption addresses 'data engineers'",
    "brand_safety": {f: False for f in BRAND_SAFETY_FLAGS},
}


@pytest.fixture
def dbs(tmp_path):
    state = _seed_state(tmp_path / "s.duckdb", [
        ("p1", "caption one", "[]", 1, "alice"),
        ("p2", "", "[]", 0, "alice"),          # empty caption: skipped
        ("p3", "caption three", "[]", 2, "bob"),
    ])
    ops = _seed_ops(tmp_path / "o.sqlite")
    return state, ops


class TestCatalog:
    def test_gold_growth_facets_in_catalog(self):
        assert "gold_growth_facets" in DUCKDB_TABLES


class TestEnumerateTargets:
    def test_captions_only(self, driver, dbs):
        state, ops = dbs
        eligible = driver.enumerate_targets(state)
        assert [p["post_id"] for p in eligible] == ["p1", "p3"]

    def test_post_ids_subset(self, driver, dbs):
        state, _ = dbs
        eligible = driver.enumerate_targets(state, post_ids=["p3"])
        assert [p["post_id"] for p in eligible] == ["p3"]

    def test_owners_subset(self, driver, dbs):
        state, _ = dbs
        eligible = driver.enumerate_targets(state, owners=["bob"])
        assert [p["post_id"] for p in eligible] == ["p3"]


class TestPlanMode:
    def test_plan_offline_no_network(self, driver, dbs, capsys, monkeypatch):
        monkeypatch.delenv("GEMINI_API_KEY", raising=False)
        state, ops = dbs
        rc = driver.main(["--plan", "--state-db", state, "--ops-db", ops])
        assert rc == 0
        out = capsys.readouterr().out
        assert "remaining to process: 2" in out
        assert "0.0002/post" in out
        assert "dry-run" in out
        assert "media=NONE" in out

    def test_plan_respects_resume(self, driver, dbs, capsys):
        state, ops = dbs
        con = duckdb.connect(state)
        con.execute(duckdb_ddl("gold_growth_facets"))
        con.execute(
            "INSERT INTO gold_growth_facets VALUES "
            "('p1', 'instagram', ?, '3', '{}', NULL, NULL, 'm', '2026-09-08')",
            [CURRENT_TEXT_FACETS_PROMPT_HASH],
        )
        con.close()
        driver.main(["--plan", "--state-db", state, "--ops-db", ops])
        assert "remaining to process: 1" in capsys.readouterr().out

    def test_missing_table_plan_ok(self, driver, dbs, capsys):
        """No gold_growth_facets yet → nothing done, plan still works."""
        state, ops = dbs
        rc = driver.main(["--plan", "--state-db", state, "--ops-db", ops])
        assert rc == 0
        assert "remaining to process: 2" in capsys.readouterr().out


class TestMergeSemantics:
    def test_visual_row_plus_text_assembles_without_clobbering(
        self, driver, dbs,
    ):
        state, ops = dbs
        from datalake.defs.enrichment.facets import write_gold_facets_conn

        con = duckdb.connect(state)
        con.execute(duckdb_ddl("gold_growth_facets"))
        write_gold_facets_conn(
            con, "p1", "instagram", VISUAL_FACETS,
            "a summary", [{"index": 1, "summary": "s1"}],
            prompt_hash=CURRENT_FACETS_PROMPT_HASH,
        )
        con.close()

        driver._ensure_gold_facets(state)
        con = duckdb.connect(state)
        from datalake.defs.enrichment.text_facets import write_text_facets_conn
        write_text_facets_conn(con, "p1", "instagram", TEXT_FACETS,
                               prompt_hash=CURRENT_TEXT_FACETS_PROMPT_HASH)
        row = con.execute(
            "SELECT growth_facets_json, content_summary, image_summaries_json, "
            "prompt_hash FROM gold_growth_facets WHERE post_id='p1'"
        ).fetchone()
        con.close()

        assembled = json.loads(row[0])
        # text fields landed
        assert assembled["hook_type"] == "pattern_interrupt"
        assert assembled["brand_safety"] == TEXT_FACETS["brand_safety"]
        # visual fields preserved untouched
        for k, v in VISUAL_FACETS.items():
            assert assembled[k] == v, f"visual field clobbered: {k}"
        # per-pass summary columns preserved
        assert row[1] == "a summary"
        assert row[2] == json.dumps([{"index": 1, "summary": "s1"}])
        # prompt_hash reflects the last (text) pass
        assert row[3] == CURRENT_TEXT_FACETS_PROMPT_HASH
        # the assembled union satisfies the FULL locked V3 schema
        assert validate_growth_facets(assembled) == []

    def test_text_only_row_is_partial_and_valid_per_pass(self, driver, dbs):
        state, _ = dbs
        from datalake.defs.enrichment.text_facets import write_text_facets_conn

        driver._ensure_gold_facets(state)
        con = duckdb.connect(state)
        write_text_facets_conn(con, "p1", "instagram", TEXT_FACETS)
        (stored,) = con.execute(
            "SELECT growth_facets_json FROM gold_growth_facets"
        ).fetchone()
        con.close()
        assembled = json.loads(stored)
        assert validate_growth_facets(assembled) != []  # partial: no visual yet
        from datalake.defs.enrichment.growth_facets_schema import (
            validate_text_facets,
        )
        assert validate_text_facets(assembled) == []

    def test_unparseable_existing_json_replaced(self, driver, tmp_path):
        from datalake.defs.enrichment.text_facets import merge_text_into_row

        merged = json.loads(merge_text_into_row("{broken", dict(TEXT_FACETS)))
        assert merged["hook_content"] == TEXT_FACETS["hook_content"]


class TestUpsertIdempotency:
    def test_write_twice_single_row(self, driver, dbs):
        state, _ = dbs
        from datalake.defs.enrichment.text_facets import write_text_facets_conn

        driver._ensure_gold_facets(state)
        con = duckdb.connect(state)
        write_text_facets_conn(con, "p1", "instagram", TEXT_FACETS)
        write_text_facets_conn(con, "p1", "instagram", TEXT_FACETS)
        n, = con.execute(
            "SELECT count(*) FROM gold_growth_facets").fetchone()
        con.close()
        assert n == 1

    def test_ordering_guard_keeps_newer(self, driver, dbs):
        state, _ = dbs
        from datalake.defs.enrichment.text_facets import write_text_facets_conn

        driver._ensure_gold_facets(state)
        con = duckdb.connect(state)
        con.execute(
            "INSERT INTO gold_growth_facets VALUES "
            "('p1', 'instagram', 'old', '3', '{}', NULL, NULL, 'm', "
            "'2999-01-01')")
        write_text_facets_conn(con, "p1", "instagram", TEXT_FACETS,
                               prompt_hash=CURRENT_TEXT_FACETS_PROMPT_HASH)
        (ph,) = con.execute(
            "SELECT prompt_hash FROM gold_growth_facets").fetchone()
        con.close()
        assert ph == "old"  # stale write must not clobber the newer row


class TestProcessWithBackpressure:
    def test_writes_merged_facets_and_returns_rec(self, driver, dbs):
        state, ops = dbs
        from datalake.defs.enrichment.text_facets import run_text_call

        calls = {"n": 0}

        class FakeGemini:
            def analyze_with_usage(self, prompt, **kw):
                calls["n"] += 1
                return json.dumps({"text_facets": TEXT_FACETS}), {
                    "prompt_token_count": 100, "candidates_token_count": 200}

        driver._ensure_gold_facets(state)
        con = duckdb.connect(state)
        rec = driver._process_with_backpressure(
            FakeGemini(), {"post_id": "p1", "caption": "cap"}, con, ops)
        con.close()
        assert rec["ok"] is True
        assert calls["n"] == 1
        con = duckdb.connect(state)
        stored = json.loads(con.execute(
            "SELECT growth_facets_json FROM gold_growth_facets"
        ).fetchone()[0])
        con.close()
        assert stored["hook_type"] == "pattern_interrupt"

    def test_quota_exhausted_stops(self, driver, dbs):
        state, ops = dbs

        class QuotaError(RuntimeError):
            pass

        class FakeGemini:
            def analyze_with_usage(self, prompt, **kw):
                raise QuotaError("quota exceeded for this project")

        driver._ensure_gold_facets(state)
        con = duckdb.connect(state)
        monkey_ew = driver.ew
        orig = monkey_ew._is_quota_exhausted
        monkey_ew._is_quota_exhausted = (
            lambda exc, text: isinstance(exc, QuotaError))
        try:
            with pytest.raises(driver.QuotaExhausted):
                driver._process_with_backpressure(
                    FakeGemini(), {"post_id": "p1", "caption": "cap"}, con, ops)
        finally:
            monkey_ew._is_quota_exhausted = orig
        con.close()
