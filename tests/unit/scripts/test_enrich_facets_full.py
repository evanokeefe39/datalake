"""Tests for the full-corpus facets materialization driver
(scripts/enrich_facets_full.py).

Pins the observable contract without any Gemini call or spend:

- ``gold_growth_facets`` is in the schema catalog and its rendered DDL is the
  single source for facets.py (no duplicated CREATE TABLE drift);
- target enumeration splits media-bearing posts into eligible
  (fully byte-cached) vs terminal media-unavailable, skipping empty captions;
- plan mode is offline and projects counts + both cost estimates;
- resume: posts already materialized under the current prompt hash are skipped;
- the gold upsert is idempotent on (post_id, domain) with the ordering guard;
- media-unavailable dead-letters once (terminal, not retried).

Databases are temp files per repo conventions.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import duckdb
import pytest

from datalake.defs.common.schemas import (
    DUCKDB_TABLES,
    duckdb_ddl,
    sqlite_ddl,
)
from datalake.defs.enrichment.media_cache import url_hash
from datalake.defs.enrichment.prompts import CURRENT_FACETS_PROMPT_HASH

SCRIPTS = Path(__file__).resolve().parents[3] / "scripts"


@pytest.fixture
def driver():
    spec = importlib.util.spec_from_file_location(
        "enrich_facets_full", SCRIPTS / "enrich_facets_full.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["enrich_facets_full"] = mod
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


def _seed_ops(path: Path, cached_urls: set[str]) -> str:
    conn = sqlite3.connect(str(path))
    conn.execute(sqlite_ddl("media_cache"))
    conn.execute(sqlite_ddl("dead_letter"))
    for u in cached_urls:
        conn.execute(
            "INSERT INTO media_cache (cache_key, local_path, fetched_at) "
            "VALUES (?, ?, '2026-09-08T00:00:00Z')",
            [url_hash(u), str(path.parent / f"{url_hash(u)}.bin")],
        )
    conn.commit()
    conn.close()
    return str(path)


def _mk_cached_file(ops_path: str, url: str) -> None:
    conn = sqlite3.connect(ops_path)
    (row,) = conn.execute(
        "SELECT local_path FROM media_cache WHERE cache_key = ?",
        [url_hash(url)],
    ).fetchone()
    Path(row).write_bytes(b"fake")
    conn.close()


VIDEO_URL = "https://cdn.example.com/v1.mp4"
IMAGE_URL = "https://cdn.example.com/i1.jpg"


@pytest.fixture
def dbs(tmp_path):
    """Two posts: one fully cached (eligible), one with an uncached URL."""
    posts = [
        ("p_video", "a video post", json.dumps([VIDEO_URL]), 1, "alice"),
        ("p_missing", "uncached media", json.dumps([IMAGE_URL]), 1, "alice"),
        ("p_empty", "", json.dumps([VIDEO_URL]), 1, "alice"),
    ]
    state = _seed_state(tmp_path / "state.duckdb", posts)
    ops = _seed_ops(tmp_path / "ops.sqlite", {VIDEO_URL})
    _mk_cached_file(ops, VIDEO_URL)
    return state, ops


class TestCatalog:
    def test_in_catalog(self):
        assert "gold_growth_facets" in DUCKDB_TABLES
        cols = DUCKDB_TABLES["gold_growth_facets"]
        assert list(cols) == [
            "post_id", "domain", "prompt_hash", "schema_version",
            "growth_facets_json", "content_summary", "image_summaries_json",
            "model", "analysed_at",
        ]
        assert cols["post_id"] == "VARCHAR"
        assert cols["domain"] == "VARCHAR"

    def test_ddl_single_source(self, driver):
        from datalake.defs.enrichment import facets
        assert facets._GOLD_FACETS_DDL == duckdb_ddl("gold_growth_facets")
        assert "PRIMARY KEY (post_id, domain)" in facets._GOLD_FACETS_DDL

    def test_ddl_creates_idempotently(self, tmp_path):
        path = tmp_path / "s.duckdb"
        for _ in range(2):
            con = duckdb.connect(str(path))
            con.execute(duckdb_ddl("gold_growth_facets"))
            con.close()
        con = duckdb.connect(str(path), read_only=True)
        assert con.execute(
            "SELECT count(*) FROM gold_growth_facets").fetchone()[0] == 0
        con.close()


class TestEnumerateTargets:
    def test_split(self, driver, dbs):
        state, ops = dbs
        eligible, unavailable = driver.enumerate_targets(state, ops)
        assert {p["post_id"] for p in eligible} == {"p_video"}
        assert {p["post_id"] for p in unavailable} == {"p_missing"}

    def test_subset_filters(self, driver, dbs):
        state, ops = dbs
        eligible, _ = driver.enumerate_targets(
            state, ops, post_ids=["p_video"])
        assert [p["post_id"] for p in eligible] == ["p_video"]
        eligible, _ = driver.enumerate_targets(state, ops, owners=["nobody"])
        assert eligible == []


class TestPlanMode:
    def test_plan_offline_counts_and_costs(self, driver, dbs, capsys, monkeypatch):
        monkeypatch.delenv("GEMINI_API_KEY", raising=False)
        state, ops = dbs
        rc = driver.main(["--plan", "--state-db", state, "--ops-db", ops])
        assert rc == 0
        out = capsys.readouterr().out
        assert "remaining to process: 1" in out
        assert "media-unavailable" in out
        assert "$0.002" in out or "0.002/post" in out      # design §7
        assert "0.00092" in out                            # pilot measured
        assert "dry-run" in out

    def test_plan_respects_resume(self, driver, dbs, capsys):
        state, ops = dbs
        con = duckdb.connect(state)
        con.execute(duckdb_ddl("gold_growth_facets"))
        con.execute(
            "INSERT INTO gold_growth_facets VALUES "
            "('p_video', 'instagram', ?, '3', '{}', 's', '[]', 'm', '2026-09-08')",
            [CURRENT_FACETS_PROMPT_HASH],
        )
        con.close()
        driver.main(["--plan", "--state-db", state, "--ops-db", ops])
        out = capsys.readouterr().out
        assert "remaining to process: 0" in out


class TestUpsertIdempotency:
    def test_write_twice_single_row(self, driver, tmp_path):
        from datalake.defs.enrichment.facets import write_gold_facets_conn
        path = tmp_path / "s.duckdb"
        con = duckdb.connect(str(path))
        facets_payload = {"face_present": False, "value_medium": "demo"}
        write_gold_facets_conn(con, "p1", "instagram", facets_payload,
                               "summary", None)
        write_gold_facets_conn(con, "p1", "instagram", facets_payload,
                               "summary2", None)
        rows = con.execute(
            "SELECT post_id, domain, content_summary, prompt_hash, "
            "schema_version FROM gold_growth_facets").fetchall()
        con.close()
        assert len(rows) == 1
        assert rows[0][2] == "summary2"
        assert rows[0][3] == CURRENT_FACETS_PROMPT_HASH

    def test_ordering_guard_keeps_newer(self, driver, tmp_path):
        from datalake.defs.enrichment import facets
        path = tmp_path / "s.duckdb"
        con = duckdb.connect(str(path))
        con.execute(duckdb_ddl("gold_growth_facets"))
        con.execute(
            "INSERT INTO gold_growth_facets VALUES "
            "('p1', 'instagram', 'old', '2', '{}', 'old', '[]', 'm', '2999-01-01')")
        facets.write_gold_facets_conn(con, "p1", "instagram",
                                      {"face_present": False}, "newer",
                                      None, prompt_hash=CURRENT_FACETS_PROMPT_HASH)
        # stale write must not clobber the newer analysed_at row
        (summary,) = con.execute(
            "SELECT content_summary FROM gold_growth_facets").fetchone()
        con.close()
        assert summary == "old"


class TestDeadLetter:
    def test_media_unavailable_terminal(self, driver, dbs):
        state, ops = dbs
        driver._dead_letter(ops, "p_missing",
                            "media unavailable: not in byte cache", 1)
        conn = sqlite3.connect(ops)
        rows = conn.execute(
            "SELECT post_id, error, attempts FROM dead_letter").fetchall()
        conn.close()
        assert rows == [("p_missing",
                         "media unavailable: not in byte cache", 1)]
