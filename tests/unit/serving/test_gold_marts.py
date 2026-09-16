"""US-ESA-1 Phase 6 — tests for the four gold marts.

The marts are built against a fixture DuckDB where the REAL canonical view
chain is materialized (``v_post_detail`` table stub → ``v_engagement_outliers``
→ ``v_post_baselines`` → ``v_post_metrics`` → ``v_post_follower_context`` →
``v_creator_profile``), so every mart value can be asserted EQUAL to the
canonical view's value for the same key — the structural proof that no metric
is re-derived. Tier buckets and momentum constants are checked to be absent
from the mart SQL entirely.
"""

from __future__ import annotations

import importlib
import inspect

import pytest
from dagster import build_asset_context
from dagster_duckdb import DuckDBResource
from orchestration.defs.platform.schemas import (
    DUCKDB_TABLES,
    DUCKDB_VIEWS,
    duckdb_ddl,
)

serving_assets = importlib.import_module("orchestration.defs.serving.views")
marts = importlib.import_module("orchestration.defs.serving.marts")
metrics = importlib.import_module("orchestration.defs.serving.metrics")
views = importlib.import_module("orchestration.defs.serving.views")

_MART_FNS = [
    marts.gold_post_enrichment,
    marts.gold_creator_performance,
    marts.gold_content_shape_performance,
    marts.gold_top_posts,
]

_CANON_FNS = [
    views.v_engagement_outliers,
    metrics.v_post_baselines,
    metrics.v_post_metrics,
    views.v_post_follower_context,
    metrics.v_creator_profile,
]


# ── Fixture ────────────────────────────────────────────────────────────────


@pytest.fixture
def db(tmp_path) -> DuckDBResource:
    """Isolated DuckDB with the real canonical chain + silver seeds.

    Baselines: (likes - center) / spread. All posts are in the past, so
    momentum windows are empty (is_rising FALSE) deterministically.
    """
    resource = DuckDBResource(database=str(tmp_path / "marts.duckdb"))
    with resource.get_connection() as con:
        # v_post_detail stub (the columns the chain + marts read).
        con.execute(
            """
            CREATE TABLE v_post_detail (
                post_id TEXT, shortcode TEXT, caption TEXT,
                owner_id TEXT, owner_username TEXT,
                creator_id INTEGER, creator_name TEXT, channel TEXT,
                likes_count BIGINT, comments_count BIGINT,
                video_view_count BIGINT, timestamp TIMESTAMP,
                source_dataset TEXT,
                gold_domain TEXT, gold_subdomain TEXT,
                gold_topic TEXT, gold_subtopic TEXT,
                content_type TEXT, format TEXT, style TEXT,
                admiralty TEXT, is_educational BOOLEAN,
                is_actionable BOOLEAN, result_json TEXT, prompt_hash TEXT
            )
            """
        )
        con.execute(
            """
            INSERT INTO v_post_detail
                (post_id, owner_id, owner_username, creator_id, creator_name,
                 channel, likes_count, timestamp, gold_domain, gold_topic,
                 content_type, format, style, admiralty, is_educational)
            VALUES
            ('p1', 'o1', 'jane', 1, 'Jane', 'instagram', 900, '2026-01-10',
             'dev', 'cli', 'tutorial', 'carousel', 'terse', 'B', TRUE),
            ('p2', 'o1', 'jane', 1, 'Jane', 'instagram', 300, '2026-01-05',
             'dev', 'cli', NULL, NULL, NULL, NULL, NULL),
            ('p3', 'o1', 'jane', 1, 'Jane', 'instagram', 100, '2026-01-01',
             'dev', 'cli', NULL, NULL, NULL, NULL, NULL),
            ('p4', 'o2', 'bob', 2, 'Bob', 'instagram', 500, '2026-01-08',
             'ai', 'agents', NULL, NULL, NULL, NULL, NULL),
            ('p5', 'o2', 'bob', 2, 'Bob', 'instagram', 50, '2026-01-03',
             'ai', 'agents', NULL, NULL, NULL, NULL, NULL)
            """
        )
        con.execute(
            """
            CREATE TABLE ig_post_labels (
                post_id TEXT PRIMARY KEY, label TEXT, method TEXT,
                is_provisional BOOLEAN, baseline_center DOUBLE,
                baseline_spread DOUBLE
            )
            """
        )
        con.execute(
            """
            INSERT INTO ig_post_labels VALUES
            ('p1', 'standout', 'day7_matched', FALSE, 100, 50),
            ('p2', 'standout', 'day7_matched', FALSE, 100, 50),
            ('p3', 'average',  'day7_matched', FALSE, 100, 50),
            ('p4', 'standout', 'day7_matched', FALSE, 100, 50),
            ('p5', 'average',  'day7_matched', FALSE, 100, 50)
            """
        )
        con.execute(
            """
            CREATE TABLE silver_ig_profile_observations (
                owner_id TEXT, owner_username TEXT,
                followers_count BIGINT, observed_at TIMESTAMP,
                source_dataset TEXT
            )
            """
        )
        con.execute(
            """
            INSERT INTO silver_ig_profile_observations VALUES
            ('o1', 'jane', 500,   '2026-01-15', 'ds'),
            ('o2', 'bob',  15000, '2026-01-20', 'ds')
            """
        )
        # Five conform silver tables (typed per the conform contract).
        con.execute(
            """
            CREATE TABLE silver_visual_annotations (
                post_id TEXT, platform TEXT, provider TEXT, model TEXT,
                prompt_hash TEXT, schema_version TEXT, input_modality TEXT,
                content_mime_type TEXT, sampling_params_json TEXT,
                run_id TEXT, analysed_at TEXT,
                face_present BOOLEAN, value_medium TEXT,
                brand_logos_json TEXT, text_overlay_present BOOLEAN,
                on_screen_claim BOOLEAN
            )
            """
        )
        con.execute(
            """
            INSERT INTO silver_visual_annotations
                (post_id, platform, provider, model, run_id,
                 schema_version, analysed_at, face_present, value_medium)
            VALUES
            ('p1', 'instagram', 'qwen', 'qwen-vl', 'r1', '3', '2026-01-11',
             FALSE, 'screenshot'),
            ('p4', 'instagram', 'qwen', 'qwen-vl', 'r1', '3', '2026-01-11',
             TRUE, NULL)
            """
        )
        con.execute(
            """
            CREATE TABLE silver_visual_summaries (
                post_id TEXT, platform TEXT, provider TEXT, model TEXT,
                prompt_hash TEXT, schema_version TEXT, input_modality TEXT,
                content_mime_type TEXT, sampling_params_json TEXT,
                run_id TEXT, analysed_at TEXT,
                content_summary TEXT, image_summaries_json TEXT
            )
            """
        )
        con.execute(
            """
            INSERT INTO silver_visual_summaries
                (post_id, platform, provider, model, run_id, analysed_at,
                 content_summary)
            VALUES
            ('p1', 'instagram', 'qwen', 'qwen-vl', 'r1', '2026-01-11',
             'code walkthrough screenshot')
            """
        )
        con.execute(
            """
            CREATE TABLE silver_audio_transcripts (
                post_id TEXT, platform TEXT, provider TEXT, model TEXT,
                prompt_hash TEXT, schema_version TEXT, input_modality TEXT,
                content_mime_type TEXT, sampling_params_json TEXT,
                run_id TEXT, analysed_at TEXT,
                transcript TEXT, transcript_status TEXT, audio_present BOOLEAN,
                asr_model TEXT, language TEXT
            )
            """
        )
        con.execute(
            """
            INSERT INTO silver_audio_transcripts
                (post_id, platform, provider, model, run_id, analysed_at,
                 transcript_status, audio_present)
            VALUES
            ('p4', 'instagram', 'whisper', 'whisper-1', 'r2', '2026-01-11',
             'no_audio_source', FALSE)
            """
        )
        con.execute(
            """
            CREATE TABLE silver_text_annotations (
                post_id TEXT, platform TEXT, provider TEXT, model TEXT,
                prompt_hash TEXT, schema_version TEXT, input_modality TEXT,
                content_mime_type TEXT, sampling_params_json TEXT,
                run_id TEXT, analysed_at TEXT,
                hook_content TEXT, hook_type TEXT, is_sponsored BOOLEAN,
                sponsorship_signal TEXT, claimed_results BOOLEAN,
                cta_type TEXT, audience_named BOOLEAN, value_depth TEXT,
                replicable_tactic TEXT, hashtag_strategy TEXT,
                evidence TEXT, brand_safety_json TEXT
            )
            """
        )
        con.execute(
            """
            INSERT INTO silver_text_annotations
                (post_id, platform, provider, model, run_id,
                 schema_version, analysed_at, hook_type, is_sponsored)
            VALUES
            ('p1', 'instagram', 'qwen', 'qwen-text', 'r3', '3', '2026-01-11',
             'question', FALSE),
            ('p2', 'instagram', 'qwen', 'qwen-text', 'r3', '3', '2026-01-11',
             'listicle', FALSE),
            ('p3', 'instagram', 'qwen', 'qwen-text', 'r3', '3', '2026-01-11',
             'question', FALSE),
            ('p4', 'instagram', 'qwen', 'qwen-text', 'r3', '3', '2026-01-11',
             'question', FALSE)
            """
        )
        con.execute(
            """
            CREATE TABLE silver_text_summaries (
                post_id TEXT, platform TEXT, provider TEXT, model TEXT,
                prompt_hash TEXT, schema_version TEXT, input_modality TEXT,
                content_mime_type TEXT, sampling_params_json TEXT,
                run_id TEXT, analysed_at TEXT,
                transcript_summary TEXT
            )
            """
        )
    return resource


def _run(db: DuckDBResource, fns) -> None:
    ctx = build_asset_context(resources={"duckdb": db})
    for fn in fns:
        fn(ctx)


# ── Existence + resolution ─────────────────────────────────────────────────


class TestMartsResolve:
    def test_all_four_marts_resolve(self, db):
        _run(db, _CANON_FNS + _MART_FNS)
        with db.get_connection() as con:
            for mart in (
                "gold_post_enrichment",
                "gold_creator_performance",
                "gold_content_shape_performance",
                "gold_top_posts",
            ):
                n = con.execute(f"SELECT COUNT(*) FROM {mart}").fetchone()[0]
                assert n is not None


# ── gold_post_enrichment ───────────────────────────────────────────────────


class TestPostEnrichment:
    def test_row_reconciliation_no_silent_drops(self, db):
        """Every post in the canonical domain appears in the mart."""
        _run(db, _CANON_FNS + _MART_FNS)
        with db.get_connection() as con:
            n_detail = con.execute(
                "SELECT COUNT(*) FROM v_post_detail"
            ).fetchone()[0]
            n_mart = con.execute(
                "SELECT COUNT(*) FROM gold_post_enrichment"
            ).fetchone()[0]
            assert n_mart == n_detail == 5

    def test_engagement_score_flows_from_canonical_view(self, db):
        """Per-key equality: the mart's score IS the canonical score."""
        _run(db, _CANON_FNS + _MART_FNS)
        with db.get_connection() as con:
            rows = con.execute(
                """
                SELECT pm.post_id, pm.engagement_score, gpe.engagement_score
                FROM v_post_metrics pm
                JOIN gold_post_enrichment gpe ON gpe.post_id = pm.post_id
                """
            ).fetchall()
        assert len(rows) == 5
        assert all(pm == gpe for _, pm, gpe in rows)

    def test_joins_all_five_silver_outputs(self, db):
        _run(db, _CANON_FNS + _MART_FNS)
        with db.get_connection() as con:
            cols = {
                r[0]
                for r in con.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'gold_post_enrichment'"
                ).fetchall()
            }
        for expected in (
            "value_medium", "content_summary", "transcript_status",
            "hook_type", "transcript_summary", "engagement_score",
            "gold_domain", "visual_provider", "text_provider",
        ):
            assert expected in cols


# ── gold_creator_performance ───────────────────────────────────────────────


class TestCreatorPerformance:
    def test_one_row_per_creator_no_inflation(self, db):
        """Creator with 3 posts appears ONCE; count == v_creator_profile."""
        _run(db, _CANON_FNS + _MART_FNS)
        with db.get_connection() as con:
            n_mart = con.execute(
                "SELECT COUNT(*) FROM gold_creator_performance"
            ).fetchone()[0]
            n_canon = con.execute(
                "SELECT COUNT(*) FROM v_creator_profile"
            ).fetchone()[0]
            n_jane = con.execute(
                "SELECT COUNT(*) FROM gold_creator_performance "
                "WHERE creator_name = 'Jane'"
            ).fetchone()[0]
        assert n_mart == n_canon == 2
        assert n_jane == 1

    def test_canonical_metrics_equal_creator_profile(self, db):
        _run(db, _CANON_FNS + _MART_FNS)
        with db.get_connection() as con:
            rows = con.execute(
                """
                SELECT
                    cp.creator_id, cp.total_posts, gcp.post_count,
                    cp.avg_engagement_score, gcp.avg_engagement_score,
                    cp.momentum_ratio, gcp.momentum_ratio,
                    cp.is_rising, gcp.is_rising,
                    cp.dominant_domain, gcp.dominant_domain
                FROM v_creator_profile cp
                JOIN gold_creator_performance gcp
                    ON gcp.creator_id = cp.creator_id
                """
            ).fetchall()
        for r in rows:
            cid, tp, mtp, avg, mavg, mom, mmom, ris, mris, dom, mdom = r
            assert tp == mtp and avg == mavg and mom == mmom
            assert ris == mris and dom == mdom

    def test_mart_only_metrics(self, db):
        """Median/standout_rate/follower tier come from canonical inputs."""
        _run(db, _CANON_FNS + _MART_FNS)
        with db.get_connection() as con:
            rows = {
                r[0]: r[1:]
                for r in con.execute(
                    """
                    SELECT creator_id, median_engagement_score, standout_rate,
                           follower_tier, platform
                    FROM gold_creator_performance ORDER BY creator_id
                    """
                ).fetchall()
            }
        # Jane: scores 8.0 / 2.0 / 0.0 → median 2.0, rate 2/3; tier 100-1k.
        assert rows[1] == (2.0, 2 / 3, "100-1k", "instagram")
        # Bob: scores 4.0 / -0.5 → median 1.75, rate 1/2; tier 10k+.
        assert rows[2] == (1.75, 0.5, "10k+", "instagram")

    def test_median_matches_canonical_per_post_scores(self, db):
        _run(db, _CANON_FNS + _MART_FNS)
        with db.get_connection() as con:
            rows = con.execute(
                """
                SELECT
                    gcp.creator_id, gcp.median_engagement_score,
                    (SELECT median(pm.engagement_score)
                     FROM v_post_metrics pm
                     WHERE pm.creator_id = gcp.creator_id
                       AND pm.engagement_score IS NOT NULL)
                FROM gold_creator_performance gcp
                """
            ).fetchall()
        assert all(m == e for _, m, e in rows)


# ── gold_content_shape_performance ─────────────────────────────────────────


class TestContentShape:
    def test_cells_reconcile_no_double_counting(self, db):
        """Per slice, facet values partition the posts: sum(n_posts) equals
        the number of facet-bearing posts — never inflated."""
        _run(db, _CANON_FNS + _MART_FNS)
        with db.get_connection() as con:
            rows = con.execute(
                """
                SELECT gold_domain, gold_topic, follower_tier, facet_name,
                       SUM(n_posts) AS summed
                FROM gold_content_shape_performance
                WHERE facet_name = 'hook_type'
                GROUP BY 1, 2, 3, 4
                """
            ).fetchall()
            seeded = con.execute(
                """
                SELECT COUNT(*) FROM silver_text_annotations
                WHERE hook_type IS NOT NULL
                """
            ).fetchone()[0]
        by_slice = {(r[0], r[1]): r[4] for r in rows}
        assert by_slice == {("dev", "cli"): 3, ("ai", "agents"): 1}
        assert sum(r[4] for r in rows) == seeded == 4

    def test_cell_values_measured_from_canonical_views(self, db):
        """avg_engagement_z / standout_rate equal the canonical aggregates."""
        _run(db, _CANON_FNS + _MART_FNS)
        with db.get_connection() as con:
            rows = con.execute(
                """
                SELECT facet_value, n_posts, avg_engagement_z, standout_rate,
                       lift_vs_slice_baseline
                FROM gold_content_shape_performance
                WHERE facet_name = 'hook_type'
                  AND gold_domain = 'dev' AND gold_topic = 'cli'
                """
            ).fetchall()
        cells = {r[0]: r[1:] for r in rows}
        # question = p1 (z 8.0) + p3 (z 0.0) → n 2, avg 4.0, rate 0.5;
        # slice avg over hook_type dev/cli posts (p1,p2,p3) = 10/3.
        assert cells["question"] == (2, 4.0, 0.5, 4.0 / (10 / 3))
        assert cells["listicle"] == (1, 2.0, 1.0, 2.0 / (10 / 3))

    def test_long_form_pk_is_unique(self, db):
        _run(db, _CANON_FNS + _MART_FNS)
        with db.get_connection() as con:
            dupes = con.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT gold_domain, gold_topic, follower_tier,
                           facet_name, facet_value,
                           COUNT(*) AS c
                    FROM gold_content_shape_performance
                    GROUP BY 1, 2, 3, 4, 5 HAVING c > 1
                )
                """
            ).fetchone()[0]
        assert dupes == 0


# ── gold_top_posts ─────────────────────────────────────────────────────────


class TestTopPosts:
    def test_ranks_across_all_domains(self, db):
        _run(db, _CANON_FNS + _MART_FNS)
        with db.get_connection() as con:
            rows = con.execute(
                """
                SELECT post_id, overall_rank, engagement_score, gold_domain
                FROM gold_top_posts ORDER BY overall_rank
                """
            ).fetchall()
        assert [r[0] for r in rows] == ["p1", "p4", "p2", "p3", "p5"]
        assert rows[0][2] == 8.0 and rows[0][3] == "dev"
        assert rows[1][3] == "ai"  # cross-domain: ai's best outranks dev's #2

    def test_score_flows_from_canonical_view(self, db):
        _run(db, _CANON_FNS + _MART_FNS)
        with db.get_connection() as con:
            rows = con.execute(
                """
                SELECT pm.post_id, pm.engagement_score, gtp.engagement_score
                FROM v_post_metrics pm
                JOIN gold_top_posts gtp ON gtp.post_id = pm.post_id
                """
            ).fetchall()
        assert len(rows) == 5
        assert all(pm == gtp for _, pm, gtp in rows)

    def test_row_count_matches_scored_population(self, db):
        _run(db, _CANON_FNS + _MART_FNS)
        with db.get_connection() as con:
            n_mart = con.execute(
                "SELECT COUNT(*) FROM gold_top_posts"
            ).fetchone()[0]
            n_canon = con.execute(
                "SELECT COUNT(*) FROM v_post_metrics "
                "WHERE engagement_score IS NOT NULL"
            ).fetchone()[0]
        assert n_mart == n_canon == 5


# ── Idempotency ────────────────────────────────────────────────────────────


def test_mart_reruns_are_idempotent(db):
    """Re-materializing the marts yields identical row counts (AC5)."""
    _run(db, _CANON_FNS + _MART_FNS)
    with db.get_connection() as con:
        first = {
            m: con.execute(f"SELECT COUNT(*) FROM {m}").fetchone()[0]
            for m in (
                "gold_post_enrichment",
                "gold_creator_performance",
                "gold_content_shape_performance",
                "gold_top_posts",
            )
        }
    _run(db, _MART_FNS)
    with db.get_connection() as con:
        second = {
            m: con.execute(f"SELECT COUNT(*) FROM {m}").fetchone()[0]
            for m in (
                "gold_post_enrichment",
                "gold_creator_performance",
                "gold_content_shape_performance",
                "gold_top_posts",
            )
        }
    assert first == second


 # ── Structural: no canonical metric/constant restated ─────────────────────


class TestNoRestatedConstants:
    def test_mart_sql_carries_no_canonical_constants(self):
        """Tier buckets, momentum gates/windows, baseline arithmetic and the
        engagement_score weights each live in exactly one canonical view —
        the mart SQL must not contain any of them."""
        forbidden = [
            "1.25",            # momentum gate
            "INTERVAL",        # momentum windows
            "< 100", "< 1000", "< 10000", ">= 2", ">= 3", "<= -2",  # tiers/σ
            "quantile_cont",   # baseline estimator
            "baseline_center", "baseline_iqr",  # baseline arithmetic
            "0.5 *", "0.3 *", "0.2 *",          # engagement_score weights
            "recent_posts", "baseline_avg >= ",  # rising gates
        ]
        module_src = inspect.getsource(
            importlib.import_module("orchestration.defs.serving.marts")
        )
        for tok in forbidden:
            assert tok not in module_src, (
                f"marts restate canonical constant {tok!r}"
            )


# ── Schema catalog reconciliation ──────────────────────────────────────────


class TestSchemaCatalog:
    def test_silver_classification_selectable_from_catalog_ddl(self, tmp_path):
        """The catalog (schemas.py) — not hand-written DDL — must produce a
        working ``silver_content_classification``: create it from
        ``duckdb_ddl``, insert a contract-shaped row, SELECT it back.

        Residual, declared: this exercises the SILVER side from the catalog
        only; the canonical views in this module's fixture are the real
        asset code but the ``v_post_detail`` stub is hand-written (the
        serving views are asset-defined, not catalog-defined).
        """
        resource = DuckDBResource(database=str(tmp_path / "cat.duckdb"))
        with resource.get_connection() as con:
            con.execute(duckdb_ddl("silver_content_classification"))
            con.execute(
                """
                INSERT INTO silver_content_classification (
                    post_id, platform, provider, model, prompt_hash,
                    schema_version, input_modality, content_mime_type,
                    sampling_params_json, run_id, analysed_at,
                    domain, subdomain, topic, subtopic,
                    is_educational, is_actionable, admiralty,
                    content_type, style, format, result_json
                ) VALUES (
                    'p1', 'instagram', 'qwen', 'qwen-text', 'ph', '3',
                    'text', 'text/plain', NULL, 'r1', '2026-01-11',
                    'dev', 'software', 'cli', 'tui',
                    TRUE, FALSE, 'B', 'tutorial', 'terse', 'carousel',
                    '{"topic":"cli"}'
                )
                """
            )
            row = con.execute(
                """
                SELECT post_id, platform, domain, topic, is_educational,
                       result_json
                FROM silver_content_classification
                """
            ).fetchone()
        assert row == ("p1", "instagram", "dev", "cli", True, '{"topic":"cli"}')

    def test_target_objects_in_catalog_maps(self):
        # bronze_enrichment_raw is deliberately NOT asserted here: it is a
        # bronze LAKE table (Parquet at data/lake/bronze/), never a registered
        # DuckDB table. schemas.py documents that exclusion explicitly, and the
        # live DB confirms its absence — asserting it belongs to DUCKDB_TABLES
        # encoded the opposite of the design.
        for name in (
            "silver_visual_annotations",
            "silver_visual_summaries",
            "silver_audio_transcripts",
            "silver_text_annotations",
            "silver_text_summaries",
            "silver_content_classification",
        ):
            assert name in DUCKDB_TABLES, f"{name} missing from catalog"
        for mart in (
            "gold_post_enrichment",
            "gold_creator_performance",
            "gold_content_shape_performance",
            "gold_top_posts",
        ):
            assert mart in DUCKDB_VIEWS, f"{mart} missing from DUCKDB_VIEWS"


# ── Grain: platform (contract deviation from enrichment.md §5.3) ───────────


class TestPlatformGrain:
    @pytest.fixture
    def multi_platform_db(self, tmp_path) -> DuckDBResource:
        """Same dev/cli slice on two platforms — cells must NOT merge."""
        resource = DuckDBResource(database=str(tmp_path / "grain.duckdb"))
        with resource.get_connection() as con:
            con.execute(
                """
                CREATE TABLE v_post_detail (
                    post_id TEXT, shortcode TEXT, caption TEXT,
                    owner_id TEXT, owner_username TEXT,
                    creator_id INTEGER, creator_name TEXT, channel TEXT,
                    likes_count BIGINT, comments_count BIGINT,
                    video_view_count BIGINT, timestamp TIMESTAMP,
                    source_dataset TEXT,
                    gold_domain TEXT, gold_subdomain TEXT,
                    gold_topic TEXT, gold_subtopic TEXT,
                    content_type TEXT, format TEXT, style TEXT,
                    admiralty TEXT, is_educational BOOLEAN,
                    is_actionable BOOLEAN, result_json TEXT, prompt_hash TEXT
                )
                """
            )
            con.execute(
                """
                INSERT INTO v_post_detail
                    (post_id, owner_id, owner_username, creator_id,
                     creator_name, channel, likes_count, comments_count,
                     video_view_count, timestamp, source_dataset,
                     gold_domain, gold_topic)
                VALUES
                ('p1', 'o1', 'jane', 1, 'Jane', 'instagram', 900, 5, 1000,
                 '2026-01-10', 'ds', 'dev', 'cli'),
                ('p2', 'o1', 'jane', 1, 'Jane', 'instagram', 300, 2, 400,
                 '2026-01-05', 'ds', 'dev', 'cli'),
                ('p3', 'o2', 'bob',  2, 'Bob',  'tiktok',    500, 3, 800,
                 '2026-01-08', 'ds', 'dev', 'cli')
                """
            )
            con.execute(
                """
                CREATE TABLE silver_text_annotations (
                    post_id TEXT, platform TEXT, hook_type TEXT,
                    is_sponsored BOOLEAN, cta_type TEXT, value_depth TEXT,
                    replicable_tactic TEXT, audience_named BOOLEAN,
                    claimed_results BOOLEAN
                )
                """
            )
            con.execute(
                """
                CREATE TABLE silver_visual_annotations (
                    post_id TEXT, platform TEXT, face_present BOOLEAN,
                    value_medium TEXT, text_overlay_present BOOLEAN,
                    on_screen_claim BOOLEAN
                )
                """
            )
            con.execute(
                """
                CREATE TABLE ig_post_labels (
                    post_id TEXT PRIMARY KEY, label TEXT, method TEXT,
                    is_provisional BOOLEAN, baseline_center DOUBLE,
                    baseline_spread DOUBLE
                )
                """
            )
            con.execute(
                """
                INSERT INTO ig_post_labels VALUES
                ('p1', 'standout', 'day7_matched', FALSE, 100, 50),
                ('p2', 'standout', 'day7_matched', FALSE, 100, 50),
                ('p3', 'standout', 'day7_matched', FALSE, 100, 50)
                """
            )
            con.execute(
                """
                CREATE TABLE silver_ig_profile_observations (
                    owner_id TEXT, owner_username TEXT,
                    followers_count BIGINT, observed_at TIMESTAMP,
                    source_dataset TEXT
                )
                """
            )
            con.execute(
                """
                INSERT INTO silver_ig_profile_observations VALUES
                ('o1', 'jane', 500, '2026-01-15', 'ds'),
                ('o2', 'bob',  900, '2026-01-15', 'ds')
                """
            )
            con.execute(
                """
                INSERT INTO silver_text_annotations
                    (post_id, platform, hook_type) VALUES
                ('p1', 'instagram', 'question'),
                ('p2', 'instagram', 'question'),
                ('p3', 'tiktok',    'question')
                """
            )
        return resource

    def test_same_facet_on_two_platforms_stays_distinct(self, multi_platform_db):
        """`platform` is part of the grain: identical facet values for the
        same (domain, topic, tier) on different platforms are separate
        cells, never merged."""
        _run(multi_platform_db, _CANON_FNS + [marts.gold_content_shape_performance])
        with multi_platform_db.get_connection() as con:
            rows = con.execute(
                """
                SELECT platform, gold_domain, gold_topic, facet_name,
                       facet_value, n_posts
                FROM gold_content_shape_performance
                WHERE facet_name = 'hook_type'
                ORDER BY platform
                """
            ).fetchall()
        cells = {(r[0], r[4]): r[5] for r in rows}
        assert cells == {("instagram", "question"): 2, ("tiktok", "question"): 1}
        assert len(rows) == 2  # a platform-less grain would collapse to 1

    def test_full_grain_is_unique(self, multi_platform_db):
        _run(multi_platform_db, _CANON_FNS + [marts.gold_content_shape_performance])
        with multi_platform_db.get_connection() as con:
            dupes = con.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT platform, gold_domain, gold_topic, follower_tier,
                           facet_name, facet_value, COUNT(*) AS c
                    FROM gold_content_shape_performance
                    GROUP BY 1, 2, 3, 4, 5, 6 HAVING c > 1
                )
                """
            ).fetchone()[0]
        assert dupes == 0
