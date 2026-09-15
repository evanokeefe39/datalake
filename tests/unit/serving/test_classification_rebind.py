"""US-ESA-2 Phase 5 part 2 — the serving rebind regression test.

Locks the rebinding of ``v_post_detail`` / ``v_overview`` from
``gold_analyses`` to ``silver_content_classification``:

- the served column SET of ``v_post_detail`` is unchanged from the
  pre-rebind definition (consumers depend on names, not provenance);
- ``result_json`` is byte-identical between what the legacy gold table
  held and what the rebound view serves (newlines / unicode / escaped
  quotes round-trip exactly);
- two classification rows sharing a ``post_id`` on different platforms
  stay distinct in the silver table and do not fan out ``v_post_detail``
  (the join is keyed on ``platform = 'instagram'``);
- ``v_overview`` resolves with its single-row shape unchanged, counting
  enrichment from the classification table on ``platform``.
"""

from __future__ import annotations

import importlib

import duckdb
import pytest

from datalake.defs.enrichment.classification import CLASSIFICATION_DDL

serving_assets = importlib.import_module("datalake.defs.serving.assets")

# ── The pre-rebind column set of v_post_detail (verbatim, frozen) ──────────
# Copied from the gold_analyses-era SELECT list before the rebind. If a
# column is added or dropped (or renamed) by a future change, this test
# fails and forces a conscious consumer-facing decision.
_POST_DETAIL_COLUMNS = [
    "post_id",
    "shortcode",
    "url",
    "caption",
    "owner_id",
    "owner_username",
    "likes_count",
    "comments_count",
    "video_play_count",
    "video_view_count",
    "timestamp",
    "hashtags",
    "meta_data",
    "has_engagement_bait",
    "media_files",
    "media_count",
    "source_dataset",
    "processed_on",
    "result_json",
    "gold_analysed_at",
    "prompt_hash",
    "admiralty",
    "gold_domain",
    "gold_subdomain",
    "gold_topic",
    "gold_subtopic",
    "content_type",
    "style",
    "format",
    "is_educational",
    "is_actionable",
    "profile_key",
    "channel",
    "effective_from",
    "effective_to",
    "is_current",
    "creator_id",
    "creator_name",
    "dim_date",
    "year",
    "quarter",
    "month_number",
    "month_name",
    "week_number",
    "day_number",
    "day_of_week",
    "is_weekend",
    "financial_year",
]

_OVERVIEW_COLUMNS = [
    "total_posts",
    "total_enriched",
    "total_profiles",
    "enrichment_pct",
    "avg_admiralty_score",
    "high_signal_count",
]

# Payload exercising the byte-identity contract: newlines, unicode,
# escaped quotes, backslashes — anything a naive re-serialization would
# corrupt.
_TRICKY_JSON = (
    '{"admiralty":"B","domain":"dev","subdomain":"tooling",'
    '"topic":"build systems","subtopic":"caching",'
    '"content_type":"tutorial","style":"conversational",'
    '"format":"longform","is_educational":true,"is_actionable":false,'
    '"note":"line1\\nline2 — ünïcödé ✓ \\"quoted\\" \\\\ backslash",'
    '"emoji":"🚀 café"}'
)

_TRICKY_JSON_ARRAY_FORM = (
    '[{"admiralty":"A","domain":"dev","subdomain":"api",'
    '"topic":"api design","subtopic":"versioning",'
    '"content_type":"reference","style":"formal",'
    '"format":"snippet","is_educational":true,"is_actionable":false,'
    '"note":"tab\\tseparated \\"q\\" — 日本語"}]'
)


class _DuckDBResourceShim:
    """Stand-in for the DuckDBResource the asset bodies need.

    Asset bodies use ``with duckdb.get_connection() as conn:`` — the real
    resource returns a connection proxy whose ``__exit__`` does not close
    the underlying database. Mirror that: returning the raw connection
    would have the ``with`` close it mid-fixture.
    """

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self._con = con

    def get_connection(self) -> _ConnectionProxy:
        return _ConnectionProxy(self._con)


class _ConnectionProxy:
    """Context-manager wrapper that keeps the connection open on exit."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self._con = con

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        return self._con

    def __exit__(self, *exc) -> None:
        return None

    def execute(self, sql, params=None):  # pragma: no cover — defensive
        return self._con.execute(sql, params or [])


@pytest.fixture
def con(tmp_path):
    """Isolated DuckDB with the serving fixtures the rebind reads."""
    connection = duckdb.connect(str(tmp_path / "serving_rebind_test.duckdb"))
    connection.execute("""
        CREATE TABLE silver_ig_posts (
            post_id VARCHAR PRIMARY KEY,
            shortcode VARCHAR,
            url VARCHAR,
            caption VARCHAR,
            owner_id VARCHAR,
            owner_username VARCHAR,
            likes_count BIGINT,
            comments_count BIGINT,
            video_play_count BIGINT,
            video_view_count BIGINT,
            timestamp TIMESTAMP,
            hashtags VARCHAR,
            meta_data VARCHAR,
            has_engagement_bait BOOLEAN,
            media_files VARCHAR,
            media_count BIGINT,
            source_dataset VARCHAR,
            processed_on TIMESTAMP
        )
    """)
    connection.execute("""
        CREATE TABLE dim_profile (
            profile_key VARCHAR,
            channel VARCHAR,
            effective_from TIMESTAMP,
            effective_to TIMESTAMP,
            is_current BOOLEAN,
            owner_id VARCHAR,
            creator_id BIGINT,
            creator_name VARCHAR
        )
    """)
    connection.execute("""
        CREATE TABLE dim_date (
            date DATE PRIMARY KEY,
            year INTEGER,
            quarter INTEGER,
            month_number INTEGER,
            month_name VARCHAR,
            week_number INTEGER,
            day_number INTEGER,
            day_of_week INTEGER,
            is_weekend BOOLEAN,
            financial_year VARCHAR
        )
    """)
    # (The legacy ``gold_analyses`` table is retired (W9) — no scratch copy
    # is created; the byte-identity reference below is held in Python.)
    connection.execute(CLASSIFICATION_DDL)
    _materialize_v_post_detail(connection)
    # stub upstream views v_overview aggregates (after v_post_detail exists)
    connection.execute(
        "CREATE VIEW v_creator_quality AS SELECT NULL::DOUBLE AS admiralty_score, "
        "0 AS enriched_posts WHERE 1=0"
    )
    connection.execute(
        "CREATE VIEW v_signal AS SELECT * FROM v_post_detail WHERE 1=0"
    )
    yield connection
    connection.close()


def _materialize_v_post_detail(con: duckdb.DuckDBPyConnection) -> None:
    serving_assets.v_post_detail(_DuckDBResourceShim(con))


def _seed_baseline(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        "INSERT INTO dim_date VALUES ('2024-06-01', 2024, 2, 6, 'June', 22, 1, 6, TRUE, 'FY24')"
    )
    con.execute(
        "INSERT INTO dim_profile VALUES "
        "('pk1', 'instagram', NULL, NULL, TRUE, 'o1', 1, 'Creator One')"
    )


class TestPostDetailRebind:
    def test_column_set_unchanged(self, con):
        _seed_baseline(con)
        con.execute(
            "INSERT INTO silver_ig_posts VALUES ('p1','abc','u','cap','o1','user',"
            "1,2,3,4,'2024-06-01','#tag','{}',FALSE,'[]',1,'src','2024-06-01')"
        )
        _materialize_v_post_detail(con)
        cols = [r[0] for r in con.execute(
            "SELECT * FROM v_post_detail LIMIT 0").description]
        assert sorted(cols) == sorted(_POST_DETAIL_COLUMNS)
        assert cols == _POST_DETAIL_COLUMNS  # order too — consumers index positionally

    def test_result_json_byte_identical_to_legacy_gold(self, con):
        legacy_gold: dict[str, str] = {}
        for post_id, payload in (("p1", _TRICKY_JSON), ("p2", _TRICKY_JSON_ARRAY_FORM)):
            con.execute(
                "INSERT INTO silver_ig_posts VALUES (?, 'sc', 'u', 'c', 'o1', 'user',"
                " 1, 1, NULL, NULL, '2024-06-01', NULL, NULL, FALSE, NULL, 0, 'src', NULL)",
                [post_id],
            )
            legacy_gold[post_id] = payload
            con.execute(
                "INSERT INTO silver_content_classification VALUES "
                "(?, 'instagram', 'dev', 'tooling', 'topic', 'subtopic', TRUE, FALSE, "
                "'B', 'tutorial', 'style', 'format', ?, 'hash1', 'v1', 'run1', "
                "'direct_batch', 'model', '2024-06-01T00:00:00Z')",
                [post_id, payload],
            )
        _materialize_v_post_detail(con)
        # Reference: exactly what the legacy gold table held (in-memory).
        gold_rows = legacy_gold
        served = dict(con.execute(
            "SELECT post_id, result_json FROM v_post_detail").fetchall())
        assert set(served) == {"p1", "p2"}
        for post_id in ("p1", "p2"):
            # byte-identity on the exact string: length, codepoints, round-trip
            assert served[post_id] == gold_rows[post_id]
            assert len(served[post_id]) == len(gold_rows[post_id])
            assert served[post_id].encode("utf-8") == gold_rows[post_id].encode("utf-8")
        # and the tricky content survived verbatim
        assert "\\n" in served["p1"]
        assert "ünïcödé ✓" in served["p1"]
        assert "🚀 café" in served["p1"]
        assert "日本語" in served["p2"]

    def test_same_post_id_two_platforms_stay_distinct_no_fanout(self, con):
        _seed_baseline(con)
        con.execute(
            "INSERT INTO silver_ig_posts VALUES ('dup','d','u','c','o1','user',"
            "5,5,NULL,NULL,'2024-06-01',NULL,NULL,FALSE,NULL,0,'src',NULL)"
        )
        body = '{"admiralty":"C","domain":"x"}'
        for platform in ("instagram", "tiktok"):
            con.execute(
                "INSERT INTO silver_content_classification VALUES "
                "(?, ?, 'x', NULL, 't', NULL, FALSE, FALSE, 'C', NULL, NULL, NULL, "
                "?, 'h', 'v1', 'r', 'p', 'm', '2024-06-01T00:00:00Z')",
                ["dup", platform, body],
            )
        _materialize_v_post_detail(con)
        # No fan-out: one post → one serving row.
        assert con.execute("SELECT COUNT(*) FROM v_post_detail").fetchone()[0] == 1
        # In the classification table itself they are distinct rows.
        assert con.execute(
            "SELECT COUNT(*) FROM silver_content_classification "
            "WHERE post_id = 'dup'").fetchone()[0] == 2

    def test_unenriched_post_still_appears(self, con):
        _seed_baseline(con)
        con.execute(
            "INSERT INTO silver_ig_posts VALUES ('bare','b','u','c','o1','user',"
            "1,1,NULL,NULL,'2024-06-01',NULL,NULL,FALSE,NULL,0,'src',NULL)"
        )
        row = con.execute(
            "SELECT result_json, gold_analysed_at, gold_topic, is_educational "
            "FROM v_post_detail WHERE post_id = 'bare'").fetchone()
        assert row == (None, None, None, None)


class TestOverviewRebind:
    @pytest.fixture(autouse=True)
    def _overview_env(self, con):
        _seed_baseline(con)

    def test_single_row_shape_unchanged(self, con):
        _materialize_v_post_detail(con)
        serving_assets.v_overview(_DuckDBResourceShim(con))
        cols = [r[0] for r in con.execute(
            "SELECT * FROM v_overview LIMIT 0").description]
        assert cols == _OVERVIEW_COLUMNS
        rows = con.execute("SELECT * FROM v_overview").fetchall()
        assert len(rows) == 1
        total_posts, total_enriched, total_profiles, pct = rows[0][:4]
        assert (total_posts, total_enriched, total_profiles, pct) == (0, 0, 0, None)

    def test_counts_come_from_classification_on_platform(self, con):
        con.execute(
            "INSERT INTO silver_ig_posts VALUES ('p1','a','u','c','o1','user',"
            "1,1,NULL,NULL,'2024-06-01',NULL,NULL,FALSE,NULL,0,'src',NULL)"
        )
        con.execute(
            "INSERT INTO silver_ig_posts VALUES ('p2','b','u','c','o2','user2',"
            "1,1,NULL,NULL,'2024-06-01',NULL,NULL,FALSE,NULL,0,'src',NULL)"
        )
        con.execute(
            "INSERT INTO dim_profile VALUES ('pk2','instagram',NULL,NULL,TRUE,'o2',2,'Two')"
        )
        body = '{"admiralty":"A","domain":"dev"}'
        con.execute(
            "INSERT INTO silver_content_classification VALUES "
            "('p1','instagram','dev',NULL,'t','s',TRUE,FALSE,'A','x','y','z',"
            "?, 'h','v1','r','p','m','2024-06-01T00:00:00Z')",
            [body],
        )
        # a second platform for the same post must NOT inflate the count
        con.execute(
            "INSERT INTO silver_content_classification VALUES "
            "('p1','tiktok','dev',NULL,'t','s',TRUE,FALSE,'A','x','y','z',"
            "?, 'h','v1','r','p','m','2024-06-01T00:00:00Z')",
            [body],
        )
        _materialize_v_post_detail(con)
        serving_assets.v_overview(_DuckDBResourceShim(con))
        total_posts, total_enriched, total_profiles, pct = con.execute(
            "SELECT total_posts, total_enriched, total_profiles, enrichment_pct "
            "FROM v_overview").fetchone()
        assert total_posts == 2
        assert total_enriched == 1  # only platform='instagram', not the tiktok twin
        assert total_profiles == 2
        assert pct == 50.0
